package governor

import (
	"strings"
)

// CodeBlock is one fenced markdown ``` region inside a message body.
type CodeBlock struct {
	Path     string // logical path key for History (may be synthetic)
	Lang     string // syntax tag from fence opener (informational)
	Inner    string // body between fences (no trailing newline before closing ```)
	RawOuter string // full fenced region including ``` lines (for replacement)
	Start    int    // byte offset of RawOuter in original message content
	End      int    // exclusive byte offset end of RawOuter
}

// ExtractCodeBlocks finds markdown fenced code regions and tries to infer a
// stable file path for each:
//
//  1. Opening fence ` ```lang path/to/file.go `
//  2. Opening fence ` ```path/to/file.go `
//  3. First lines inside the fence matching `// file: path` or `# file: path`
//
// Blocks without a resolvable path are omitted — the pruner leaves them untouched.
func ExtractCodeBlocks(content string) []CodeBlock {
	// Line-based scanner with byte offsets. This fixes a correctness bug in
	// naive "\n```" searching: a code block may contain the literal substring
	// "\n```" as part of its content (e.g. markdown examples). We only treat a
	// closing fence as a LINE whose trimmed content is exactly "```".
	type lineInfo struct {
		start int
		end   int // excludes newline bytes
		nlLen int // 0, 1, or 2
	}
	lines := make([]lineInfo, 0, 128)
	for i := 0; i < len(content); {
		start := i
		for i < len(content) && content[i] != '\n' && content[i] != '\r' {
			i++
		}
		end := i
		nlLen := 0
		if i < len(content) {
			if content[i] == '\r' {
				nlLen = 1
				i++
				if i < len(content) && content[i] == '\n' {
					nlLen = 2
					i++
				}
			} else if content[i] == '\n' {
				nlLen = 1
				i++
			}
		}
		lines = append(lines, lineInfo{start: start, end: end, nlLen: nlLen})
	}

	var blocks []CodeBlock
	for i := 0; i < len(lines); i++ {
		line := content[lines[i].start:lines[i].end]
		if !strings.HasPrefix(line, "```") {
			continue
		}
		header := strings.TrimSpace(strings.TrimPrefix(line, "```"))
		lang, pathGuess := parseFenceHeader(header)
		openStart := lines[i].start
		bodyStart := lines[i].end + lines[i].nlLen

		// Find closing fence line.
		closeLineIdx := -1
		for j := i + 1; j < len(lines); j++ {
			t := strings.TrimSpace(content[lines[j].start:lines[j].end])
			if t == "```" {
				closeLineIdx = j
				break
			}
		}
		if closeLineIdx < 0 {
			break // unclosed fence
		}
		innerEnd := lines[closeLineIdx].start
		rawOuterEnd := lines[closeLineIdx].end + lines[closeLineIdx].nlLen

		inner := content[bodyStart:innerEnd]
		bodyLines := strings.Split(inner, "\n")
		path := pathGuess
		if path == "" {
			path = inferPathFromInner(lang, bodyLines)
		}
		if path == "" {
			i = closeLineIdx
			continue
		}

		rawOuter := content[openStart:rawOuterEnd]
		blocks = append(blocks, CodeBlock{
			Path:     path,
			Lang:     lang,
			Inner:    inner,
			RawOuter: rawOuter,
			Start:    openStart,
			End:      rawOuterEnd,
		})

		// Skip to line after the close fence.
		i = closeLineIdx
	}
	return blocks
}

func parseFenceHeader(rest string) (lang, path string) {
	if rest == "" {
		return "", ""
	}
	fields := strings.Fields(rest)
	if len(fields) >= 2 {
		// Accept "go path/to/file.go", and "go file=path/to/file.go"
		if strings.HasPrefix(fields[1], "file=") {
			return fields[0], strings.TrimPrefix(fields[1], "file=")
		}
		return fields[0], fields[1]
	}
	tok := fields[0]
	if strings.HasPrefix(tok, "file=") {
		return "", strings.TrimPrefix(tok, "file=")
	}
	if looksLikePath(tok) {
		return "", tok
	}
	return tok, ""
}

func looksLikePath(s string) bool {
	if strings.Contains(s, "/") {
		return true
	}
	if strings.Contains(s, ".") {
		if idx := strings.LastIndex(s, "."); idx > 0 {
			ext := strings.ToLower(s[idx+1:])
			switch ext {
			case "go", "ts", "tsx", "js", "jsx", "py", "rs", "java", "kt", "swift", "rb", "cs", "cpp", "h", "hpp", "c", "sql", "yaml", "yml", "json", "md", "txt":
				return true
			}
		}
	}
	return false
}

func inferPathFromInner(lang string, bodyLines []string) string {
	const maxScan = 5
	n := len(bodyLines)
	if n > maxScan {
		n = maxScan
	}
	for i := 0; i < n; i++ {
		t := strings.TrimSpace(bodyLines[i])
		if strings.HasPrefix(t, "// file:") {
			return strings.TrimSpace(strings.TrimPrefix(t, "// file:"))
		}
		if strings.HasPrefix(t, "# file:") {
			return strings.TrimSpace(strings.TrimPrefix(t, "# file:"))
		}
	}
	_ = lang
	return ""
}
