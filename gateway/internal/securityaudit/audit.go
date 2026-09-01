// Package securityaudit provides the rule-based scanner shared by the
// session audit command and automatic post-task verification.
package securityaudit

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/Revanth14/indexqube/gateway/internal/redact"
)

const MaxFileBytes = 1 << 20

type Severity string

const (
	SeverityHigh   Severity = "high"
	SeverityMedium Severity = "medium"
	SeverityLow    Severity = "low"
)

type Scope string

const (
	ScopeSession     Scope = "session"
	ScopeDiffAdded   Scope = "diff_added"
	ScopeCurrentFile Scope = "current_file"
)

// Finding is one redacted, bounded rule match. RuleID is stable for clients;
// Category is the broader grouping intended for human summaries.
type Finding struct {
	RuleID   string   `json:"rule_id"`
	Severity Severity `json:"severity"`
	Category string   `json:"category"`
	Scope    Scope    `json:"scope"`
	Source   string   `json:"source"`
	Path     string   `json:"path,omitempty"`
	Line     int      `json:"line,omitempty"`
	Evidence string   `json:"evidence"`
	Detail   string   `json:"detail"`
	Count    int      `json:"count"`
}

type rule struct {
	id       string
	severity Severity
	category string
	detail   string
	diff     bool
	re       *regexp.Regexp
}

var rules = []rule{
	{
		id: "secret.exposed_value", severity: SeverityHigh, category: "secret_exposure", diff: true,
		detail: "Secret-like value was present in agent-visible content.",
		re:     regexp.MustCompile(`(?i)(\[redacted(?:-[a-z-]+)?\]|sk-[a-z0-9_-]{8,}|github_pat_[a-z0-9_]{8,}|(?:ghp|gho|ghu|ghs|ghr)_[a-z0-9_]{8,}|AKIA[0-9A-Z]{16}|xox[baprs]-[a-z0-9-]{20,}|-----BEGIN (?:[A-Z ]+ )?PRIVATE KEY-----)`),
	},
	{
		id: "secret.credential_reference", severity: SeverityMedium, category: "secret_reference", diff: true,
		detail: "Credential-related key or header appeared in agent-visible content.",
		re:     regexp.MustCompile(`(?i)\b(authorization|bearer|api[-_ ]?key|x-anthropic-api-key|secret[-_ ]?access[-_ ]?key|provider[-_ ]?key)\b`),
	},
	{
		id: "file.high_risk_credential", severity: SeverityHigh, category: "sensitive_file", diff: true,
		detail: "Agent-visible content referenced a high-risk credential file or directory.",
		re:     regexp.MustCompile(`(?i)(^|[\s"'=/\\])(\.ssh|id_rsa|id_ed25519|\.aws[/\\]credentials|\.config[/\\]gcloud|kube[/\\]config)([/\\]|\b)`),
	},
	{
		id: "file.sensitive_configuration", severity: SeverityMedium, category: "sensitive_file", diff: true,
		detail: "Agent-visible content referenced a sensitive configuration or secret file.",
		re:     regexp.MustCompile(`(?i)(^|[\s"'=/\\])(\.env(?:\.[a-z0-9_.-]+)?|\.npmrc|\.pypirc|\.netrc|secrets?\.ya?ml|credentials?\.json)(\b|[/\\])`),
	},
	{
		id: "command.destructive", severity: SeverityHigh, category: "dangerous_command",
		detail: "Potentially destructive shell command appeared in the session.",
		re:     regexp.MustCompile(`(?i)\b(rm\s+-rf\s+/(?:\s|$)|sudo\s+rm\s+-rf|dd\s+if=|mkfs\.|chmod\s+-R?\s+777|git\s+push\s+--force|kill\s+-9)\b`),
	},
	{
		id: "command.remote_pipe", severity: SeverityHigh, category: "dangerous_command",
		detail: "Shelling remote content directly into an interpreter is supply-chain sensitive.",
		re:     regexp.MustCompile(`(?i)\b(curl|wget)\b[^\n|;]{0,200}\|\s*(bash|sh|zsh|python|ruby|perl)\b`),
	},
	{
		id: "command.cloud_delete", severity: SeverityHigh, category: "dangerous_command",
		detail: "Cloud or cluster deletion command appeared in the session.",
		re:     regexp.MustCompile(`(?i)\b(aws|gcloud|az|kubectl)\b[^\n]{0,120}\b(delete|destroy|terminate|remove)\b`),
	},
	{
		id: "prompt.instruction_override", severity: SeverityMedium, category: "prompt_injection",
		detail: "Text resembles an instruction override or data-exfiltration prompt.",
		re:     regexp.MustCompile(`(?i)\b(ignore|disregard|override)\b.{0,80}\b(previous|prior|system|developer|instructions?)\b|\b(exfiltrate|leak|send|upload)\b.{0,80}\b(secret|token|credential|\.env|ssh key)\b`),
	},
	{
		id: "code.shell_injection", severity: SeverityHigh, category: "generated_code_risk", diff: true,
		detail: "Generated diff appears to execute user-controlled shell text.",
		re:     regexp.MustCompile(`(?i)(exec\.Command\(\s*"sh"\s*,\s*"-c"|child_process\.exec\(|subprocess\.(Popen|run|call)\([^)\n]*shell\s*=\s*true)`),
	},
	{
		id: "code.tls_verification_disabled", severity: SeverityMedium, category: "generated_code_risk", diff: true,
		detail: "Generated diff appears to disable TLS or certificate verification.",
		re:     regexp.MustCompile(`(?i)(InsecureSkipVerify\s*:\s*true|rejectUnauthorized\s*:\s*false|verify\s*=\s*False)`),
	},
	{
		id: "code.sql_concatenation", severity: SeverityMedium, category: "generated_code_risk", diff: true,
		detail: "Generated diff may build SQL with string formatting or concatenation.",
		re:     regexp.MustCompile(`(?i)(fmt\.Sprintf\([^)\n]*(SELECT|INSERT|UPDATE|DELETE)|\b(SELECT|INSERT|UPDATE|DELETE)\b[^\n]{0,120}(\+|%s|\$\{))`),
	},
	{
		id: "code.authentication_bypass", severity: SeverityMedium, category: "generated_code_risk", diff: true,
		detail: "Generated diff appears to bypass or weaken authentication checks.",
		re:     regexp.MustCompile(`(?i)(jwt\.decode\(|verify_signature\s*=\s*False|skipAuth|disableAuth|auth\s*=\s*false)`),
	},
	{
		id: "dependency.lifecycle_script", severity: SeverityHigh, category: "dependency_risk", diff: true,
		detail: "Dependency lifecycle script changed; review for install-time execution.",
		re:     regexp.MustCompile(`(?i)"(preinstall|install|postinstall|prepare)"\s*:`),
	},
}

type Scanner struct {
	index map[string]int
	items []Finding
}

func NewScanner() *Scanner {
	return &Scanner{index: map[string]int{}}
}

// ScanText applies session rules to arbitrary agent-visible text.
func (s *Scanner) ScanText(source, text string) {
	s.scanText(source, "", 0, ScopeSession, text, false)
}

// ScanDiff examines added lines only. allowedPaths limits results to the
// authoritative paths changed by the current turn; nil permits every path.
func (s *Scanner) ScanDiff(diff string, allowedPaths []string) int {
	allowed := pathSet(allowedPaths)
	currentFile := ""
	newLine := 0
	dependencyLines := 0
	for _, line := range strings.Split(diff, "\n") {
		if strings.HasPrefix(line, "diff --git ") {
			currentFile = diffPath(line)
			newLine = 0
			if currentFile != "" && pathAllowed(currentFile, allowed) {
				s.scanText("git diff "+currentFile, currentFile, 0, ScopeDiffAdded, currentFile, true)
			}
			continue
		}
		if match := hunkHeader.FindStringSubmatch(line); len(match) == 2 {
			newLine, _ = strconv.Atoi(match[1])
			continue
		}
		if currentFile == "" || !pathAllowed(currentFile, allowed) || strings.HasPrefix(line, "+++") {
			continue
		}
		switch {
		case strings.HasPrefix(line, "+"):
			added := strings.TrimPrefix(line, "+")
			if isDependencyManifest(currentFile) && strings.TrimSpace(added) != "" {
				dependencyLines++
				if dependencyLines <= 8 {
					s.add(Finding{
						RuleID: "dependency.manifest_change", Severity: SeverityLow, Category: "dependency_change",
						Scope: ScopeDiffAdded, Source: "git diff " + currentFile, Path: currentFile, Line: newLine,
						Evidence: added, Detail: "Dependency manifest changed; review new package trust and lifecycle scripts.",
					})
				}
			}
			s.scanText("git diff "+currentFile, currentFile, newLine, ScopeDiffAdded, added, true)
			newLine++
		case strings.HasPrefix(line, "-"):
			// Deleted lines do not advance the new-file line counter.
		default:
			if newLine > 0 {
				newLine++
			}
		}
	}
	return dependencyLines
}

type FileScanSummary struct {
	Scanned []string
	Skipped []string
}

// ScanFiles reviews the current contents of changed untracked files, which
// are absent from Git diffs. Files are bounded and symlinks are not followed.
func (s *Scanner) ScanFiles(root string, paths []string) FileScanSummary {
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return FileScanSummary{Skipped: append([]string(nil), paths...)}
	}
	rootAbs, err = filepath.EvalSymlinks(rootAbs)
	if err != nil {
		return FileScanSummary{Skipped: append([]string(nil), paths...)}
	}
	seen := map[string]bool{}
	var summary FileScanSummary
	for _, rel := range paths {
		rel = filepath.ToSlash(filepath.Clean(rel))
		if rel == "." || rel == "" || seen[rel] {
			continue
		}
		seen[rel] = true
		path, ok := safePath(rootAbs, rel)
		if !ok {
			summary.Skipped = append(summary.Skipped, rel)
			continue
		}
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > MaxFileBytes {
			summary.Skipped = append(summary.Skipped, rel)
			continue
		}
		resolved, err := filepath.EvalSymlinks(path)
		if err != nil || !withinRoot(rootAbs, resolved) {
			summary.Skipped = append(summary.Skipped, rel)
			continue
		}
		f, err := os.Open(path)
		if err != nil {
			summary.Skipped = append(summary.Skipped, rel)
			continue
		}
		s.scanText("current file "+rel, rel, 1, ScopeCurrentFile, readBounded(f, MaxFileBytes), true)
		_ = f.Close()
		summary.Scanned = append(summary.Scanned, rel)
	}
	sort.Strings(summary.Scanned)
	sort.Strings(summary.Skipped)
	return summary
}

func (s *Scanner) scanText(source, path string, startingLine int, scope Scope, text string, diffOnly bool) {
	lineNumber := startingLine
	for _, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			for _, candidate := range rules {
				if diffOnly && !candidate.diff {
					continue
				}
				if !candidate.re.MatchString(trimmed) {
					continue
				}
				s.add(Finding{
					RuleID: candidate.id, Severity: candidate.severity, Category: candidate.category,
					Scope: scope, Source: source, Path: path, Line: lineNumber,
					Evidence: candidate.re.FindString(trimmed), Detail: candidate.detail,
				})
			}
		}
		if lineNumber > 0 {
			lineNumber++
		}
	}
}

func (s *Scanner) add(f Finding) {
	f.Evidence = cleanEvidence(f.Evidence)
	if f.Evidence == "" {
		return
	}
	key := findingKey(f)
	if position, ok := s.index[key]; ok {
		s.items[position].Count++
		return
	}
	f.Count = 1
	s.index[key] = len(s.items)
	s.items = append(s.items, f)
}

func (s *Scanner) Findings() []Finding {
	out := append([]Finding(nil), s.items...)
	sortFindings(out)
	return out
}

// Difference removes findings already visible at the pre-turn boundary. It
// compares stable rule/path/evidence identities rather than line numbers so a
// harmless line shift does not become a new risk event.
func Difference(before, after []Finding) []Finding {
	baseline := map[string]int{}
	for _, finding := range before {
		baseline[findingKey(finding)] += normalizedCount(finding.Count)
	}
	result := make([]Finding, 0, len(after))
	for _, finding := range after {
		count := normalizedCount(finding.Count) - baseline[findingKey(finding)]
		if count <= 0 {
			continue
		}
		finding.Count = count
		result = append(result, finding)
	}
	sortFindings(result)
	return result
}

func Counts(findings []Finding) (high, medium, low int) {
	for _, finding := range findings {
		switch finding.Severity {
		case SeverityHigh:
			high++
		case SeverityMedium:
			medium++
		default:
			low++
		}
	}
	return high, medium, low
}

func Sort(findings []Finding) {
	sortFindings(findings)
}

func Occurrences(findings []Finding) int {
	total := 0
	for _, finding := range findings {
		total += normalizedCount(finding.Count)
	}
	return total
}

func findingKey(f Finding) string {
	return f.RuleID + "\x00" + f.Path + "\x00" + f.Evidence
}

func normalizedCount(count int) int {
	if count <= 0 {
		return 1
	}
	return count
}

func sortFindings(findings []Finding) {
	sort.SliceStable(findings, func(i, j int) bool {
		ri, rj := severityRank(findings[i].Severity), severityRank(findings[j].Severity)
		if ri != rj {
			return ri < rj
		}
		if findings[i].Path != findings[j].Path {
			return findings[i].Path < findings[j].Path
		}
		if findings[i].Line != findings[j].Line {
			return findings[i].Line < findings[j].Line
		}
		return findings[i].RuleID < findings[j].RuleID
	})
}

func severityRank(severity Severity) int {
	switch severity {
	case SeverityHigh:
		return 0
	case SeverityMedium:
		return 1
	default:
		return 2
	}
}

func cleanEvidence(value string) string {
	value = strings.TrimSpace(redact.String(value))
	value = strings.Join(strings.Fields(value), " ")
	if len(value) > 140 {
		value = value[:137] + "..."
	}
	return value
}

func pathSet(paths []string) map[string]bool {
	if paths == nil {
		return nil
	}
	out := make(map[string]bool, len(paths))
	for _, path := range paths {
		out[filepath.ToSlash(filepath.Clean(path))] = true
	}
	return out
}

func pathAllowed(path string, allowed map[string]bool) bool {
	return allowed == nil || allowed[filepath.ToSlash(filepath.Clean(path))]
}

func diffPath(header string) string {
	fields := quotedFields(strings.TrimPrefix(header, "diff --git "))
	if len(fields) < 2 {
		return ""
	}
	value := fields[1]
	value = strings.TrimPrefix(value, "b/")
	return filepath.ToSlash(filepath.Clean(value))
}

func quotedFields(value string) []string {
	fields := make([]string, 0, 2)
	for index := 0; index < len(value); {
		for index < len(value) && (value[index] == ' ' || value[index] == '\t') {
			index++
		}
		if index >= len(value) {
			break
		}
		start := index
		if value[index] == '"' {
			index++
			escaped := false
			for index < len(value) {
				character := value[index]
				index++
				if character == '"' && !escaped {
					break
				}
				if character == '\\' && !escaped {
					escaped = true
				} else {
					escaped = false
				}
			}
		} else {
			for index < len(value) && value[index] != ' ' && value[index] != '\t' {
				index++
			}
		}
		field := value[start:index]
		if unquoted, err := strconv.Unquote(field); err == nil {
			field = unquoted
		}
		fields = append(fields, field)
	}
	return fields
}

var hunkHeader = regexp.MustCompile(`^@@ -[0-9]+(?:,[0-9]+)? \+([0-9]+)(?:,[0-9]+)? @@`)

func isDependencyManifest(path string) bool {
	switch filepath.Base(path) {
	case "package.json", "package-lock.json", "pnpm-lock.yaml", "yarn.lock",
		"go.mod", "go.sum", "requirements.txt", "pyproject.toml", "poetry.lock",
		"Gemfile", "Gemfile.lock", "Cargo.toml", "Cargo.lock":
		return true
	default:
		return false
	}
}

func safePath(root, rel string) (string, bool) {
	clean := filepath.Clean(filepath.FromSlash(rel))
	if filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", false
	}
	path := filepath.Join(root, clean)
	if !withinRoot(root, path) {
		return "", false
	}
	return path, true
}

func withinRoot(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func readBounded(r io.Reader, limit int64) string {
	data, _ := io.ReadAll(io.LimitReader(r, limit))
	return string(data)
}

func FormatFinding(f Finding) string {
	location := f.Path
	if location != "" && f.Line > 0 {
		location += fmt.Sprintf(":%d", f.Line)
	}
	if location == "" {
		location = f.Source
	}
	return fmt.Sprintf("%s %s at %s: %s", f.Severity, f.RuleID, location, f.Evidence)
}
