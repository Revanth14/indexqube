package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/redact"
)

const (
	defaultAuditScannerMax = 64 << 20
)

type auditDumpRecord struct {
	Timestamp   string          `json:"ts"`
	RequestID   string          `json:"request_id"`
	BeforeBytes int             `json:"before_bytes"`
	AfterBytes  int             `json:"after_bytes"`
	SavedBytes  int             `json:"saved_bytes"`
	Before      json.RawMessage `json:"before"`
	After       json.RawMessage `json:"after"`
	Response    struct {
		Text                     string `json:"text"`
		OutputTokens             int    `json:"output_tokens"`
		Status                   string `json:"status"`
		InputTokens              int    `json:"input_tokens"`
		CacheReadInputTokens     int    `json:"cache_read_input_tokens"`
		CacheCreationInputTokens int    `json:"cache_creation_input_tokens"`
	} `json:"response"`
	Optimizer *struct {
		BlocksPruned         int `json:"blocks_pruned"`
		BlocksKnown          int `json:"blocks_known"`
		BlocksKnownProtected int `json:"blocks_known_protected"`
		BytesPruned          int `json:"bytes_pruned"`
		ProtectedBytes       int `json:"protected_bytes"`
		KnownBytes           int `json:"known_bytes"`
		TrueCacheHitBytes    int `json:"true_cache_hit_bytes"`
	} `json:"optimizer,omitempty"`
}

type auditFinding struct {
	Severity string
	Category string
	Source   string
	Evidence string
	Detail   string
	// Count is how many times this exact finding (same severity, category, and
	// evidence) recurred across the session. Claude Code resends the same
	// context every turn, so one benign pattern in a CLAUDE.md/README would
	// otherwise explode into a separate finding per request.
	Count int
}

type auditReport struct {
	GeneratedAt          time.Time
	RepoRoot             string
	DumpPath             string
	Requests             int
	InputTokens          int
	CacheReadTokens      int
	CacheCreationTokens  int
	OutputTokens         int
	BeforeBytes          int
	AfterBytes           int
	Findings             []auditFinding
	DependencyLinesAdded int
	DiffBytes            int
}

type auditRule struct {
	severity string
	category string
	detail   string
	re       *regexp.Regexp
}

var securityRules = []auditRule{
	{
		severity: "high",
		category: "secret_exposure",
		detail:   "Secret-like value was present in agent-visible content.",
		re:       regexp.MustCompile(`(?i)(\[redacted(?:-[a-z-]+)?\]|sk-[a-z0-9_-]{8,}|github_pat_[a-z0-9_]{8,}|(?:ghp|gho|ghu|ghs|ghr)_[a-z0-9_]{8,}|AKIA[0-9A-Z]{16}|xox[baprs]-[a-z0-9-]{20,}|-----BEGIN (?:[A-Z ]+ )?PRIVATE KEY-----)`),
	},
	{
		severity: "medium",
		category: "secret_reference",
		detail:   "Credential-related key or header appeared in agent-visible content.",
		re:       regexp.MustCompile(`(?i)\b(authorization|bearer|api[-_ ]?key|x-anthropic-api-key|secret[-_ ]?access[-_ ]?key|provider[-_ ]?key)\b`),
	},
	{
		severity: "high",
		category: "sensitive_file",
		detail:   "Agent-visible content referenced a high-risk credential file or directory.",
		re:       regexp.MustCompile(`(?i)(^|[\s"'=/\\])(\.ssh|id_rsa|id_ed25519|\.aws[/\\]credentials|\.config[/\\]gcloud|kube[/\\]config)([/\\]|\b)`),
	},
	{
		severity: "medium",
		category: "sensitive_file",
		detail:   "Agent-visible content referenced a sensitive configuration or secret file.",
		re:       regexp.MustCompile(`(?i)(^|[\s"'=/\\])(\.env(?:\.[a-z0-9_.-]+)?|\.npmrc|\.pypirc|\.netrc|secrets?\.ya?ml|credentials?\.json)(\b|[/\\])`),
	},
	{
		severity: "high",
		category: "dangerous_command",
		detail:   "Potentially destructive shell command appeared in the session.",
		re:       regexp.MustCompile(`(?i)\b(rm\s+-rf\s+/(?:\s|$)|sudo\s+rm\s+-rf|dd\s+if=|mkfs\.|chmod\s+-R?\s+777|git\s+push\s+--force|kill\s+-9)\b`),
	},
	{
		severity: "high",
		category: "dangerous_command",
		detail:   "Shelling remote content directly into an interpreter is supply-chain sensitive.",
		re:       regexp.MustCompile(`(?i)\b(curl|wget)\b[^\n|;]{0,200}\|\s*(bash|sh|zsh|python|ruby|perl)\b`),
	},
	{
		severity: "high",
		category: "dangerous_command",
		detail:   "Cloud or cluster deletion command appeared in the session.",
		re:       regexp.MustCompile(`(?i)\b(aws|gcloud|az|kubectl)\b[^\n]{0,120}\b(delete|destroy|terminate|remove)\b`),
	},
	{
		severity: "medium",
		category: "prompt_injection",
		detail:   "Text resembles an instruction override or data-exfiltration prompt.",
		re:       regexp.MustCompile(`(?i)\b(ignore|disregard|override)\b.{0,80}\b(previous|prior|system|developer|instructions?)\b|\b(exfiltrate|leak|send|upload)\b.{0,80}\b(secret|token|credential|\.env|ssh key)\b`),
	},
	{
		severity: "high",
		category: "generated_code_risk",
		detail:   "Generated diff appears to execute user-controlled shell text.",
		re:       regexp.MustCompile(`(?i)(exec\.Command\(\s*"sh"\s*,\s*"-c"|child_process\.exec\(|subprocess\.(Popen|run|call)\([^)\n]*shell\s*=\s*true)`),
	},
	{
		severity: "medium",
		category: "generated_code_risk",
		detail:   "Generated diff appears to disable TLS or certificate verification.",
		re:       regexp.MustCompile(`(?i)(InsecureSkipVerify\s*:\s*true|rejectUnauthorized\s*:\s*false|verify\s*=\s*False)`),
	},
	{
		severity: "medium",
		category: "generated_code_risk",
		detail:   "Generated diff may build SQL with string formatting or concatenation.",
		re:       regexp.MustCompile(`(?i)(fmt\.Sprintf\([^)\n]*(SELECT|INSERT|UPDATE|DELETE)|\b(SELECT|INSERT|UPDATE|DELETE)\b[^\n]{0,120}(\+|%s|\$\{))`),
	},
	{
		severity: "medium",
		category: "generated_code_risk",
		detail:   "Generated diff appears to bypass or weaken authentication checks.",
		re:       regexp.MustCompile(`(?i)(jwt\.decode\(|verify_signature\s*=\s*False|skipAuth|disableAuth|auth\s*=\s*false)`),
	},
	{
		severity: "high",
		category: "dependency_risk",
		detail:   "Dependency lifecycle script changed; review for install-time execution.",
		re:       regexp.MustCompile(`(?i)"(preinstall|install|postinstall|prepare)"\s*:`),
	},
}

func runAudit(args []string) {
	fs := flag.NewFlagSet("audit", flag.ExitOnError)
	sessionPath := fs.String("session", "", "path to an iq-session-*.jsonl dump")
	repoRoot := fs.String("repo", "", "repository root to audit")
	outPath := fs.String("out", "", "report output path")
	parseArgs := stripAuditLatestArg(args)
	if err := fs.Parse(parseArgs); err != nil {
		os.Exit(2)
	}

	rest := fs.Args()
	if len(rest) > 0 {
		switch rest[0] {
		case "latest":
			// default
		default:
			if *sessionPath == "" {
				*sessionPath = rest[0]
			}
		}
	}

	root := strings.TrimSpace(*repoRoot)
	if root == "" {
		root = findRepoRoot()
	}
	if root == "" {
		cwd, err := os.Getwd()
		if err != nil {
			fmt.Fprintf(os.Stderr, "iq: audit failed: %v\n", err)
			os.Exit(1)
		}
		root = cwd
	}

	dumpPath := strings.TrimSpace(*sessionPath)
	if dumpPath == "" {
		var err error
		dumpPath, err = latestAuditDump(root)
		if err != nil {
			fmt.Fprintf(os.Stderr, "iq: audit failed: %v\n", err)
			os.Exit(1)
		}
	}

	records, err := readAuditDump(dumpPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "iq: audit failed: %v\n", err)
		os.Exit(1)
	}
	diff := gitDiff(root)
	report := buildSecurityAuditReport(root, dumpPath, records, diff, time.Now())

	dest := strings.TrimSpace(*outPath)
	if dest == "" {
		dest = defaultAuditReportPath(root, dumpPath)
	}
	if err := writeSecurityAuditReport(dest, report); err != nil {
		fmt.Fprintf(os.Stderr, "iq: audit failed: %v\n", err)
		os.Exit(1)
	}

	high, medium, low := auditSeverityCounts(report.Findings)
	fmt.Fprintf(os.Stderr, "  [iq] security report: %s\n", dest)
	fmt.Fprintf(os.Stderr, "  [iq] risk events: %d (high=%d medium=%d low=%d)\n", len(report.Findings), high, medium, low)
}

func stripAuditLatestArg(args []string) []string {
	out := make([]string, 0, len(args))
	for _, arg := range args {
		if arg == "latest" {
			continue
		}
		out = append(out, arg)
	}
	return out
}

func latestAuditDump(repoRoot string) (string, error) {
	var candidates []string
	for _, dir := range auditDumpSearchDirs(repoRoot) {
		matches, err := filepath.Glob(filepath.Join(dir, "iq-session-*.jsonl"))
		if err != nil {
			continue
		}
		candidates = append(candidates, matches...)
	}
	if len(candidates) == 0 {
		return "", errors.New("no iq-session dump found; run `./iq claude --dev --dump-payloads` first")
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		ai, aerr := os.Stat(candidates[i])
		bi, berr := os.Stat(candidates[j])
		if aerr != nil || berr != nil {
			return candidates[i] > candidates[j]
		}
		return ai.ModTime().After(bi.ModTime())
	})
	return candidates[0], nil
}

func auditDumpSearchDirs(repoRoot string) []string {
	var dirs []string
	if repoRoot != "" {
		dirs = append(dirs,
			filepath.Join(repoRoot, ".indexqube", "dumps"),
			filepath.Join(repoRoot, "gateway", ".indexqube", "dumps"),
		)
	}
	if home := strings.TrimSpace(os.Getenv("INDEXQUBE_HOME")); home != "" {
		dirs = append(dirs, filepath.Join(home, "dumps"))
	} else if userHome, err := os.UserHomeDir(); err == nil {
		dirs = append(dirs, filepath.Join(userHome, ".indexqube", "dumps"))
	}
	return uniqueStrings(dirs)
}

func readAuditDump(path string) ([]auditDumpRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return decodeAuditDump(f)
}

func decodeAuditDump(r io.Reader) ([]auditDumpRecord, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), defaultAuditScannerMax)
	var records []auditDumpRecord
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var rec auditDumpRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return nil, fmt.Errorf("decode dump line %d: %w", lineNo, err)
		}
		records = append(records, rec)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, errors.New("dump is empty")
	}
	return records, nil
}

func gitDiff(repoRoot string) string {
	var b strings.Builder
	for _, args := range [][]string{{"diff", "--"}, {"diff", "--cached", "--"}} {
		cmd := exec.Command("git", args...) //nolint:gosec
		cmd.Dir = repoRoot
		out, err := cmd.Output()
		if err != nil {
			continue
		}
		if len(out) == 0 {
			continue
		}
		if b.Len() > 0 {
			b.WriteString("\n")
		}
		b.Write(out)
	}
	return b.String()
}

func buildSecurityAuditReport(repoRoot, dumpPath string, records []auditDumpRecord, diff string, now time.Time) auditReport {
	report := auditReport{
		GeneratedAt: now,
		RepoRoot:    repoRoot,
		DumpPath:    dumpPath,
		Requests:    len(records),
		DiffBytes:   len(diff),
	}
	collector := newAuditFindingCollector()
	for _, rec := range records {
		report.InputTokens += rec.Response.InputTokens
		report.CacheReadTokens += rec.Response.CacheReadInputTokens
		report.CacheCreationTokens += rec.Response.CacheCreationInputTokens
		report.OutputTokens += rec.Response.OutputTokens
		report.BeforeBytes += rec.BeforeBytes
		report.AfterBytes += rec.AfterBytes

		req := rec.RequestID
		if req == "" {
			req = "unknown-request"
		}
		scanJSONPayload(collector, "dump before "+req, rec.Before, false)
		scanJSONPayload(collector, "dump after "+req, rec.After, false)
		if rec.Response.Text != "" {
			scanSecurityText(collector, "response "+req, rec.Response.Text, false)
		}
	}
	report.DependencyLinesAdded = scanGitDiff(collector, diff)
	report.Findings = collector.findings()
	return report
}

type auditFindingCollector struct {
	index map[string]int // dedup key -> position in items
	items []auditFinding
}

func newAuditFindingCollector() *auditFindingCollector {
	return &auditFindingCollector{index: map[string]int{}}
}

func (c *auditFindingCollector) add(f auditFinding) {
	f.Evidence = cleanEvidence(f.Evidence)
	if f.Evidence == "" {
		return
	}
	// Dedup on the finding itself (severity + category + evidence), not on the
	// per-request source, so the same pattern resent every turn collapses into a
	// single finding with an occurrence count instead of one row per request.
	key := f.Severity + "\x00" + f.Category + "\x00" + f.Evidence
	if pos, ok := c.index[key]; ok {
		c.items[pos].Count++
		return
	}
	f.Count = 1
	c.index[key] = len(c.items)
	c.items = append(c.items, f)
}

func (c *auditFindingCollector) findings() []auditFinding {
	out := append([]auditFinding(nil), c.items...)
	sort.SliceStable(out, func(i, j int) bool {
		ri, rj := severityRank(out[i].Severity), severityRank(out[j].Severity)
		if ri != rj {
			return ri < rj
		}
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		if out[i].Category != out[j].Category {
			return out[i].Category < out[j].Category
		}
		return out[i].Source < out[j].Source
	})
	return out
}

func scanJSONPayload(collector *auditFindingCollector, source string, raw json.RawMessage, diffOnly bool) {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		scanSecurityText(collector, source, string(raw), diffOnly)
		return
	}
	for _, text := range jsonStrings(value) {
		scanSecurityText(collector, source, text, diffOnly)
	}
}

func jsonStrings(value any) []string {
	var out []string
	var walk func(any)
	walk = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			for key, child := range x {
				if redact.SensitiveKey(key) {
					out = append(out, key+"=[redacted]")
				}
				walk(child)
			}
		case []any:
			for _, child := range x {
				walk(child)
			}
		case string:
			out = append(out, x)
		}
	}
	walk(value)
	return out
}

func scanSecurityText(collector *auditFindingCollector, source, text string, diffOnly bool) {
	for _, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		for _, rule := range securityRules {
			if diffOnly && rule.category != "generated_code_risk" && rule.category != "dependency_risk" && rule.category != "sensitive_file" && rule.category != "secret_exposure" && rule.category != "secret_reference" {
				continue
			}
			if !rule.re.MatchString(trimmed) {
				continue
			}
			collector.add(auditFinding{
				Severity: rule.severity,
				Category: rule.category,
				Source:   source,
				Evidence: rule.re.FindString(trimmed),
				Detail:   rule.detail,
			})
		}
	}
}

func scanGitDiff(collector *auditFindingCollector, diff string) int {
	if strings.TrimSpace(diff) == "" {
		return 0
	}
	currentFile := ""
	dependencyLines := 0
	for _, line := range strings.Split(diff, "\n") {
		if strings.HasPrefix(line, "diff --git ") {
			fields := strings.Fields(line)
			if len(fields) >= 4 {
				currentFile = strings.TrimPrefix(fields[3], "b/")
				scanSecurityText(collector, "git diff "+currentFile, currentFile, true)
			}
			continue
		}
		if !strings.HasPrefix(line, "+") || strings.HasPrefix(line, "+++") {
			continue
		}
		added := strings.TrimPrefix(line, "+")
		source := "git diff"
		if currentFile != "" {
			source += " " + currentFile
		}
		if isDependencyManifest(currentFile) && strings.TrimSpace(added) != "" {
			dependencyLines++
			if dependencyLines <= 8 {
				collector.add(auditFinding{
					Severity: "low",
					Category: "dependency_change",
					Source:   source,
					Evidence: added,
					Detail:   "Dependency manifest changed; review new package trust and lifecycle scripts.",
				})
			}
		}
		scanSecurityText(collector, source, added, true)
	}
	return dependencyLines
}

func isDependencyManifest(path string) bool {
	base := filepath.Base(path)
	switch base {
	case "package.json", "package-lock.json", "pnpm-lock.yaml", "yarn.lock",
		"go.mod", "go.sum", "requirements.txt", "pyproject.toml", "poetry.lock",
		"Gemfile", "Gemfile.lock", "Cargo.toml", "Cargo.lock":
		return true
	default:
		return false
	}
}

func defaultAuditReportPath(repoRoot, dumpPath string) string {
	base := strings.TrimSuffix(filepath.Base(dumpPath), filepath.Ext(dumpPath))
	if base == "" || base == "." {
		base = "latest"
	}
	return filepath.Join(repoRoot, ".indexqube", "reports", "security-"+base+".md")
}

func writeSecurityAuditReport(path string, report auditReport) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(renderSecurityAuditReport(report)), 0o600)
}

func renderSecurityAuditReport(report auditReport) string {
	high, medium, low := auditSeverityCounts(report.Findings)
	var b strings.Builder
	b.WriteString("# IndexQube Agent Security Report\n\n")
	b.WriteString(fmt.Sprintf("Generated: %s\n\n", report.GeneratedAt.Format(time.RFC3339)))
	b.WriteString("## Summary\n\n")
	b.WriteString(fmt.Sprintf("- Risk events: %d distinct (%d occurrences)\n", len(report.Findings), auditOccurrences(report.Findings)))
	b.WriteString(fmt.Sprintf("- High: %d\n", high))
	b.WriteString(fmt.Sprintf("- Medium: %d\n", medium))
	b.WriteString(fmt.Sprintf("- Low: %d\n", low))
	b.WriteString(fmt.Sprintf("- Requests: %d\n", report.Requests))
	b.WriteString(fmt.Sprintf("- Dump: `%s`\n", report.DumpPath))
	b.WriteString(fmt.Sprintf("- Repository: `%s`\n", report.RepoRoot))
	b.WriteString(fmt.Sprintf("- Git diff bytes: %d\n", report.DiffBytes))
	b.WriteString(fmt.Sprintf("- Dependency lines added: %d\n", report.DependencyLinesAdded))
	b.WriteString("\n## Session Signals\n\n")
	b.WriteString(fmt.Sprintf("- Upstream input tokens: %d\n", report.InputTokens))
	b.WriteString(fmt.Sprintf("- Prompt-cache read tokens: %d\n", report.CacheReadTokens))
	b.WriteString(fmt.Sprintf("- Prompt-cache write tokens: %d\n", report.CacheCreationTokens))
	b.WriteString(fmt.Sprintf("- Output tokens: %d\n", report.OutputTokens))
	b.WriteString(fmt.Sprintf("- Payload bytes before: %d\n", report.BeforeBytes))
	b.WriteString(fmt.Sprintf("- Payload bytes after: %d\n", report.AfterBytes))
	b.WriteString("\n## Findings\n\n")
	if len(report.Findings) == 0 {
		b.WriteString("No rule-based security findings were detected in the session dump or git diff.\n\n")
	} else {
		b.WriteString("| Severity | Category | Count | Sample source | Evidence | Detail |\n")
		b.WriteString("| --- | --- | --- | --- | --- | --- |\n")
		for _, f := range report.Findings {
			b.WriteString(fmt.Sprintf("| %s | %s | %d | %s | `%s` | %s |\n",
				tableCell(f.Severity),
				tableCell(f.Category),
				f.Count,
				tableCell(f.Source),
				tableCell(f.Evidence),
				tableCell(f.Detail),
			))
		}
		b.WriteString("\n")
	}
	b.WriteString("## Notes\n\n")
	b.WriteString("- This is a local, rule-based triage report. It favors surfacing suspicious agent activity over proving exploitability.\n")
	b.WriteString("- Payload dumps are redacted before audit; a redacted secret marker means secret-like material reached agent-visible content before redaction.\n")
	b.WriteString("- Review high-severity findings before committing or sharing generated code.\n")
	return b.String()
}

func auditOccurrences(findings []auditFinding) int {
	total := 0
	for _, f := range findings {
		if f.Count <= 0 {
			total++
			continue
		}
		total += f.Count
	}
	return total
}

func auditSeverityCounts(findings []auditFinding) (high, medium, low int) {
	for _, f := range findings {
		switch f.Severity {
		case "high":
			high++
		case "medium":
			medium++
		default:
			low++
		}
	}
	return high, medium, low
}

func severityRank(sev string) int {
	switch sev {
	case "high":
		return 0
	case "medium":
		return 1
	default:
		return 2
	}
}

func cleanEvidence(s string) string {
	s = strings.TrimSpace(redact.String(s))
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 140 {
		s = s[:137] + "..."
	}
	return s
}

func tableCell(s string) string {
	s = strings.ReplaceAll(s, "|", "\\|")
	s = strings.ReplaceAll(s, "`", "'")
	s = strings.ReplaceAll(s, "\n", " ")
	return s
}

func uniqueStrings(in []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}
