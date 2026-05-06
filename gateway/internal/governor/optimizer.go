package governor

import (
	"fmt"
	"strings"
)

const (
	// defaultMaxExactDiffCells bounds exact LCS memory. The optimizer trims
	// common prefix/suffix before applying this limit, so small edits in large
	// files still take the exact path.
	defaultMaxExactDiffCells int64 = 500_000
)

// LineOptimizerConfig controls the dependency-free line diff engine.
type LineOptimizerConfig struct {
	// MaxLines is a caller-side circuit breaker. If either input exceeds it,
	// the optimizer returns SkipReason "too_many_lines".
	MaxLines int
	// ContextLines controls unchanged context around each emitted hunk.
	ContextLines int
	// MaxExactCells caps exact LCS work for the changed middle window. Larger
	// windows fall back to one compact replacement hunk instead of the full file.
	MaxExactCells int64
}

// LineOptimization is the result of a bounded line-diff optimization.
type LineOptimization struct {
	Diff       string
	SkipReason string
	Exact      bool
}

type diffOp struct {
	tag  byte // ' ' equal, '-' delete from old, '+' insert from new
	text string
}

// OptimizeLineDiff renders a compact unified diff between oldText and newText.
func OptimizeLineDiff(path, oldText, newText string, cfg LineOptimizerConfig) LineOptimization {
	return OptimizeLineSlices(path, splitLines(oldText), splitLines(newText), cfg)
}

// OptimizeLineSlices renders a compact unified diff between two line slices.
// It is fast for common code-edit cases because it first trims identical edges,
// then only runs exact LCS on the changed middle window. When that middle
// window is too large, it emits a compact replacement hunk rather than risking
// unbounded memory.
func OptimizeLineSlices(path string, oldLines, newLines []string, cfg LineOptimizerConfig) LineOptimization {
	cfg = normalizeLineOptimizerConfig(cfg)
	n, m := len(oldLines), len(newLines)
	if cfg.MaxLines > 0 && (n > cfg.MaxLines || m > cfg.MaxLines) {
		return LineOptimization{SkipReason: "too_many_lines"}
	}
	if n == 0 && m == 0 {
		return LineOptimization{SkipReason: "empty"}
	}

	prefix := commonPrefixLen(oldLines, newLines)
	suffix := commonSuffixLen(oldLines[prefix:], newLines[prefix:])
	oldMid := oldLines[prefix : n-suffix]
	newMid := newLines[prefix : m-suffix]
	if len(oldMid) == 0 && len(newMid) == 0 {
		return LineOptimization{SkipReason: "empty"}
	}

	ops := make([]diffOp, 0, prefix+len(oldMid)+len(newMid)+suffix)
	for _, line := range oldLines[:prefix] {
		ops = append(ops, diffOp{' ', line})
	}

	exact := true
	if int64(len(oldMid))*int64(len(newMid)) > cfg.MaxExactCells {
		exact = false
		// The changed window is too tangled for exact LCS. Still return a
		// compact, valid replacement hunk so token reduction survives.
		for _, line := range oldMid {
			ops = append(ops, diffOp{'-', line})
		}
		for _, line := range newMid {
			ops = append(ops, diffOp{'+', line})
		}
	} else {
		ops = append(ops, lcsDiffOps(oldMid, newMid)...)
	}

	for _, line := range oldLines[n-suffix:] {
		ops = append(ops, diffOp{' ', line})
	}

	diff := formatUnified(path, ops, cfg.ContextLines)
	if diff == "" {
		return LineOptimization{SkipReason: "empty", Exact: exact}
	}
	return LineOptimization{Diff: diff, Exact: exact}
}

func normalizeLineOptimizerConfig(cfg LineOptimizerConfig) LineOptimizerConfig {
	if cfg.ContextLines < 0 {
		cfg.ContextLines = 0
	}
	if cfg.MaxExactCells <= 0 {
		cfg.MaxExactCells = defaultMaxExactDiffCells
	}
	return cfg
}

func lcsDiffOps(oldLines, newLines []string) []diffOp {
	n, m := len(oldLines), len(newLines)
	dp := make([][]int, n+1)
	for i := range dp {
		dp[i] = make([]int, m+1)
	}
	for i := 1; i <= n; i++ {
		for j := 1; j <= m; j++ {
			if oldLines[i-1] == newLines[j-1] {
				dp[i][j] = dp[i-1][j-1] + 1
			} else {
				dp[i][j] = maxInt(dp[i-1][j], dp[i][j-1])
			}
		}
	}

	var ops []diffOp
	i, j := n, m
	for i > 0 || j > 0 {
		if i > 0 && j > 0 && oldLines[i-1] == newLines[j-1] && dp[i][j] == dp[i-1][j-1]+1 {
			ops = append(ops, diffOp{' ', oldLines[i-1]})
			i--
			j--
		} else if j > 0 && (i == 0 || dp[i][j] == dp[i][j-1]) {
			ops = append(ops, diffOp{'+', newLines[j-1]})
			j--
		} else if i > 0 {
			ops = append(ops, diffOp{'-', oldLines[i-1]})
			i--
		}
	}
	for k, l := 0, len(ops)-1; k < l; k, l = k+1, l-1 {
		ops[k], ops[l] = ops[l], ops[k]
	}
	return ops
}

func formatUnified(path string, ops []diffOp, contextLines int) string {
	if len(ops) == 0 {
		return ""
	}

	var ranges [][2]int
	for i, op := range ops {
		if op.tag == ' ' {
			continue
		}
		start := i - contextLines
		if start < 0 {
			start = 0
		}
		end := i + contextLines + 1
		if end > len(ops) {
			end = len(ops)
		}
		if len(ranges) > 0 && start <= ranges[len(ranges)-1][1] {
			if end > ranges[len(ranges)-1][1] {
				ranges[len(ranges)-1][1] = end
			}
			continue
		}
		ranges = append(ranges, [2]int{start, end})
	}
	if len(ranges) == 0 {
		return ""
	}

	oldBefore := make([]int, len(ops)+1)
	newBefore := make([]int, len(ops)+1)
	for i, op := range ops {
		oldBefore[i+1] = oldBefore[i]
		newBefore[i+1] = newBefore[i]
		if op.tag != '+' {
			oldBefore[i+1]++
		}
		if op.tag != '-' {
			newBefore[i+1]++
		}
	}

	var b strings.Builder
	fmt.Fprintf(&b, "--- a/%s\n", path)
	fmt.Fprintf(&b, "+++ b/%s\n", path)
	for _, r := range ranges {
		start, end := r[0], r[1]
		oldStart := oldBefore[start] + 1
		newStart := newBefore[start] + 1
		oldCnt := oldBefore[end] - oldBefore[start]
		newCnt := newBefore[end] - newBefore[start]
		if oldCnt == 0 {
			oldStart--
			if oldStart < 0 {
				oldStart = 0
			}
		}
		if newCnt == 0 {
			newStart--
			if newStart < 0 {
				newStart = 0
			}
		}
		fmt.Fprintf(&b, "@@ -%d,%d +%d,%d @@\n", oldStart, maxInt(oldCnt, 1), newStart, maxInt(newCnt, 1))
		for _, op := range ops[start:end] {
			b.WriteByte(op.tag)
			b.WriteByte(' ')
			b.WriteString(op.text)
			b.WriteByte('\n')
		}
	}
	return b.String()
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func commonPrefixLen(a, b []string) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

func commonSuffixLen(a, b []string) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[len(a)-1-i] != b[len(b)-1-i] {
			return i
		}
	}
	return n
}

func splitLines(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}
