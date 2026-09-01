package verification

import (
	"fmt"
	"strings"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/securityaudit"
)

const maxSecurityFindings = 128

func securityCheck(request Request) CheckResult {
	started := time.Now().UTC()
	before := securityaudit.NewScanner()
	before.ScanDiff(request.Change.BeforeDiff, request.ChangedPaths)
	after := securityaudit.NewScanner()
	after.ScanDiff(request.Change.AfterDiff, request.ChangedPaths)
	findings := securityaudit.Difference(before.Findings(), after.Findings())

	files := securityaudit.NewScanner()
	fileSummary := files.ScanFiles(request.Workspace, request.Change.CurrentFilePaths)
	findings = append(findings, files.Findings()...)
	if request.Change.DiffTruncated {
		findings = append(findings, securityaudit.Finding{
			RuleID: "audit.diff_truncated", Severity: securityaudit.SeverityMedium, Category: "audit_coverage",
			Scope: securityaudit.ScopeDiffAdded, Source: "workspace snapshot",
			Evidence: "task diff exceeded the stored audit bound", Detail: "Review the full Git diff because automatic audit coverage was truncated.", Count: 1,
		})
	}
	for _, path := range fileSummary.Skipped {
		findings = append(findings, securityaudit.Finding{
			RuleID: "audit.file_unscanned", Severity: securityaudit.SeverityMedium, Category: "audit_coverage",
			Scope: securityaudit.ScopeCurrentFile, Source: "current file " + path, Path: path,
			Evidence: "changed untracked file was not scanned", Detail: fmt.Sprintf("Only regular files up to %d bytes are scanned automatically.", securityaudit.MaxFileBytes), Count: 1,
		})
	}
	securityaudit.Sort(findings)

	high, medium, low := securityaudit.Counts(findings)
	status := CheckPassed
	switch {
	case high > 0:
		status = CheckFailed
	case medium > 0 || low > 0:
		status = CheckWarning
	}

	var output strings.Builder
	fmt.Fprintf(&output, "Rule findings: high=%d medium=%d low=%d. Reviewed added diff lines for %d authoritative changed path(s)",
		high, medium, low, len(request.ChangedPaths))
	if len(fileSummary.Scanned) > 0 {
		fmt.Fprintf(&output, "; scanned %d changed untracked file(s) as current-file evidence", len(fileSummary.Scanned))
	}
	output.WriteString(".\n")
	for index, finding := range findings {
		if index >= 12 {
			fmt.Fprintf(&output, "... %d additional finding(s); inspect structured task evidence for the complete bounded set.\n", len(findings)-index)
			break
		}
		output.WriteString(securityaudit.FormatFinding(finding))
		output.WriteByte('\n')
	}
	if len(fileSummary.Scanned) > 0 {
		output.WriteString("Current-file findings cover changed untracked content and may include material that predates this turn.\n")
	}
	output.WriteString("Policy: high severity blocks completion; medium and low severity are non-blocking review warnings.")

	if len(findings) > maxSecurityFindings {
		omitted := len(findings) - (maxSecurityFindings - 1)
		findings = append(findings[:maxSecurityFindings-1], securityaudit.Finding{
			RuleID: "audit.findings_truncated", Severity: securityaudit.SeverityMedium, Category: "audit_coverage",
			Scope: securityaudit.ScopeDiffAdded, Source: "security audit",
			Evidence: fmt.Sprintf("%d lower-priority finding(s) omitted", omitted),
			Detail:   "The durable finding set is bounded; inspect the full diff for additional matches.", Count: 1,
		})
	}
	return CheckResult{
		Name: "Security audit", Kind: "security", CWD: ".", Status: status, Output: output.String(),
		StartedAt: started, CompletedAt: time.Now().UTC(), Findings: findings,
	}
}
