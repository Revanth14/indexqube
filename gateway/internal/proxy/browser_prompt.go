package proxy

import "strings"

type browserCodeRegion struct {
	prefixEnd int
	codeStart int
	codeEnd   int
	path      string
}

func normalizeBrowserPromptCode(content string) (string, bool) {
	lines := strings.Split(content, "\n")
	return normalizeBrowserPromptSegments(lines)
}

func findBrowserCodeRegion(lines []string) (browserCodeRegion, bool) {
	return findNextBrowserCodeRegion(lines, 0)
}

func normalizeBrowserPromptSegments(lines []string) (string, bool) {
	parts := make([]string, 0, 8)
	textStart := 0
	scan := 0
	found := false
	for scan < len(lines) {
		region, ok := findNextBrowserCodeRegion(lines, scan)
		if !ok {
			break
		}
		found = true
		appendBrowserTextPart(&parts, lines[textStart:region.prefixEnd])
		code := strings.TrimSpace(strings.Join(lines[region.codeStart:region.codeEnd], "\n"))
		if code != "" {
			parts = append(parts, ensureFencedContext(code, region.path, ""))
		}
		textStart = region.codeEnd
		scan = region.codeEnd
	}
	if !found {
		return "", false
	}
	appendBrowserTextPart(&parts, lines[textStart:])
	return strings.Join(parts, "\n\n"), true
}

func appendBrowserTextPart(parts *[]string, lines []string) {
	text := strings.TrimSpace(strings.Join(lines, "\n"))
	if text != "" {
		*parts = append(*parts, text)
	}
}

func findNextBrowserCodeRegion(lines []string, start int) (browserCodeRegion, bool) {
	for i := start; i < len(lines); i++ {
		if path := parseBrowserPathHint(lines[i]); path != "" {
			next := nextNonBlankLine(lines, i+1)
			if next >= 0 && lineLooksLikeCodeStart(lines[next]) {
				return browserCodeRegion{
					prefixEnd: i,
					codeStart: next,
					codeEnd:   findBrowserCodeEnd(lines, next),
					path:      path,
				}, true
			}
		}
		if lineLooksLikeCodeStart(lines[i]) {
			return browserCodeRegion{
				prefixEnd: i,
				codeStart: i,
				codeEnd:   findBrowserCodeEnd(lines, i),
			}, true
		}
	}
	return browserCodeRegion{}, false
}

func findBrowserCodeEnd(lines []string, start int) int {
	for i := start + 1; i < len(lines); i++ {
		if isNextBrowserFileStart(lines, i) {
			return trimTrailingBlankLines(lines, start, i)
		}
		if !isBlankLine(lines[i]) {
			continue
		}
		next := nextNonBlankLine(lines, i+1)
		if next < 0 {
			return len(lines)
		}
		if lineLooksNaturalLanguage(lines[next]) && !lineLooksLikeCodeContinuation(lines[next]) {
			return i
		}
	}
	return len(lines)
}

func isNextBrowserFileStart(lines []string, idx int) bool {
	if parseBrowserPathHint(lines[idx]) == "" {
		return false
	}
	next := nextNonBlankLine(lines, idx+1)
	if next < 0 || !lineLooksLikeCodeStart(lines[next]) {
		return false
	}
	prev := previousNonBlankLine(lines, idx-1)
	return prev < 0 || isBlankLine(lines[idx-1]) || lineLooksLikeCodeBoundary(lines[prev])
}

func previousNonBlankLine(lines []string, start int) int {
	for i := start; i >= 0; i-- {
		if !isBlankLine(lines[i]) {
			return i
		}
	}
	return -1
}

func trimTrailingBlankLines(lines []string, start, end int) int {
	for end > start && isBlankLine(lines[end-1]) {
		end--
	}
	return end
}

func nextNonBlankLine(lines []string, start int) int {
	for i := start; i < len(lines); i++ {
		if !isBlankLine(lines[i]) {
			return i
		}
	}
	return -1
}

func isBlankLine(line string) bool {
	return strings.TrimSpace(line) == ""
}

func lineLooksLikeCodeStart(line string) bool {
	t := strings.TrimSpace(line)
	if t == "" {
		return false
	}
	lower := strings.ToLower(t)
	prefixes := []string{
		"package ", "func ", "type ", "const ", "var ", "import ",
		"class ", "function ", "async function ", "export ", "def ",
		"from ", "select ", "with ", "create ", "insert ", "update ", "delete ",
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return strings.Contains(t, " := ") ||
		strings.Contains(t, "=>") ||
		strings.HasPrefix(t, "{") && strings.Contains(t, ":") ||
		strings.HasPrefix(t, "[") && strings.Contains(t, ":") ||
		strings.HasSuffix(t, "{") && (strings.Contains(t, ")") || strings.Contains(t, "="))
}

func lineLooksLikeCodeContinuation(line string) bool {
	t := strings.TrimSpace(line)
	if t == "" {
		return true
	}
	lower := strings.ToLower(t)
	if strings.HasPrefix(line, " ") || strings.HasPrefix(line, "\t") {
		return true
	}
	if lineLooksLikeCodeStart(line) {
		return true
	}
	prefixes := []string{
		"return ", "if ", "for ", "switch ", "case ", "else", "try", "catch ",
		"//", "#", "/*", "*", "}", ")", "]",
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(lower, prefix) || strings.HasPrefix(t, prefix) {
			return true
		}
	}
	return strings.Contains(t, ";") ||
		strings.Contains(t, "{") ||
		strings.Contains(t, "}") ||
		strings.Contains(t, " := ") ||
		strings.Contains(t, "=>")
}

func lineLooksLikeCodeBoundary(line string) bool {
	t := strings.TrimSpace(line)
	return t == "}" ||
		t == "};" ||
		t == ")" ||
		t == ")," ||
		t == "]" ||
		t == "]," ||
		strings.HasPrefix(t, "}") ||
		strings.HasSuffix(t, "}") ||
		strings.HasSuffix(t, "};")
}

func lineLooksNaturalLanguage(line string) bool {
	t := strings.TrimSpace(line)
	if t == "" {
		return false
	}
	if strings.ContainsAny(t, "{};") {
		return false
	}
	lower := strings.ToLower(t)
	prefixes := []string{
		"what ", "why ", "how ", "can ", "could ", "please ", "fix ",
		"explain ", "is ", "are ", "do ", "does ", "tell ", "where ",
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return strings.Contains(t, "?")
}

func parseBrowserPathHint(line string) string {
	t := strings.TrimSpace(line)
	if t == "" {
		return ""
	}
	t = strings.TrimSpace(strings.TrimLeft(t, "#"))
	t = strings.TrimSpace(strings.TrimPrefix(t, "//"))
	t = strings.TrimSpace(strings.TrimPrefix(t, "--"))
	t = strings.TrimSpace(strings.TrimPrefix(t, "-"))
	t = strings.TrimSpace(strings.TrimPrefix(t, "*"))

	lower := strings.ToLower(t)
	for _, prefix := range []string{"file:", "filename:", "path:", "source:"} {
		if strings.HasPrefix(lower, prefix) {
			return cleanBrowserPathHint(t[len(prefix):])
		}
	}
	fields := strings.Fields(t)
	if len(fields) != 1 {
		return ""
	}
	return cleanBrowserPathHint(fields[0])
}

func cleanBrowserPathHint(path string) string {
	path = strings.TrimSpace(path)
	path = strings.Trim(path, "`'\"<>")
	path = strings.TrimSuffix(path, ":")
	path = strings.TrimPrefix(path, "file=")
	if !looksLikeBrowserPath(path) {
		return ""
	}
	return path
}

func looksLikeBrowserPath(path string) bool {
	if path == "" || strings.Contains(path, "```") || strings.ContainsAny(path, "\r\n\t ") {
		return false
	}
	if strings.Contains(path, "/") {
		return true
	}
	idx := strings.LastIndex(path, ".")
	if idx <= 0 || idx == len(path)-1 {
		return false
	}
	switch strings.ToLower(path[idx+1:]) {
	case "go", "ts", "tsx", "js", "jsx", "py", "rs", "java", "kt", "swift", "rb", "cs", "cpp", "h", "hpp", "c", "sql", "yaml", "yml", "json", "md", "txt":
		return true
	default:
		return false
	}
}
