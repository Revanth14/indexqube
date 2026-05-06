package governor

import (
	"fmt"
	"strings"
	"testing"
)

func TestOptimizeLineSlices_ExactCompactHunk(t *testing.T) {
	t.Parallel()
	result := OptimizeLineSlices("src/app.go",
		[]string{"package main", "", "func main() {", "\tprintln(\"old\")", "}"},
		[]string{"package main", "", "func main() {", "\tprintln(\"new\")", "}"},
		LineOptimizerConfig{ContextLines: 1, MaxLines: 100},
	)
	if result.Diff == "" {
		t.Fatal("expected diff")
	}
	if !result.Exact {
		t.Fatal("expected exact LCS path")
	}
	if result.SkipReason != "" {
		t.Fatalf("skip reason=%q, want empty", result.SkipReason)
	}
	for _, want := range []string{"--- a/src/app.go", "+++ b/src/app.go", "- \tprintln(\"old\")", "+ \tprintln(\"new\")"} {
		if !strings.Contains(result.Diff, want) {
			t.Fatalf("diff missing %q:\n%s", want, result.Diff)
		}
	}
}

func TestOptimizeLineSlices_TrimsTwoThousandLineEdges(t *testing.T) {
	t.Parallel()
	oldLines := makeOptimizerLines(2000)
	newLines := append([]string(nil), oldLines...)
	for i := 1000; i < 1015; i++ {
		newLines[i] = fmt.Sprintf("line %04d changed", i+1)
	}

	result := OptimizeLineSlices("src/huge.go", oldLines, newLines, LineOptimizerConfig{
		ContextLines: 3,
		MaxLines:     8000,
	})
	if result.Diff == "" {
		t.Fatal("expected diff")
	}
	if !result.Exact {
		t.Fatal("small middle window should use exact path")
	}
	if strings.Contains(result.Diff, "line 0001 original") || strings.Contains(result.Diff, "line 2000 original") {
		t.Fatalf("diff leaked far-away lines:\n%s", result.Diff)
	}
	if got := countDiffLinesWithPrefix(result.Diff, "  "); got > 6 {
		t.Fatalf("too many context lines: %d\n%s", got, result.Diff)
	}
}

func TestOptimizeLineSlices_FallbackReplacementHunkWhenMiddleTooLarge(t *testing.T) {
	t.Parallel()
	oldLines := []string{"prefix", "old-a", "old-b", "old-c", "suffix"}
	newLines := []string{"prefix", "new-a", "new-b", "new-c", "suffix"}

	result := OptimizeLineSlices("src/app.go", oldLines, newLines, LineOptimizerConfig{
		ContextLines:  1,
		MaxLines:      100,
		MaxExactCells: 1,
	})
	if result.Diff == "" {
		t.Fatal("expected fallback diff")
	}
	if result.Exact {
		t.Fatal("expected non-exact fallback path")
	}
	for _, want := range []string{"- old-a", "- old-b", "- old-c", "+ new-a", "+ new-b", "+ new-c"} {
		if !strings.Contains(result.Diff, want) {
			t.Fatalf("fallback diff missing %q:\n%s", want, result.Diff)
		}
	}
}

func TestOptimizeLineSlices_TooManyLinesSkips(t *testing.T) {
	t.Parallel()
	result := OptimizeLineSlices("src/app.go",
		[]string{"a", "b"},
		[]string{"a", "B"},
		LineOptimizerConfig{MaxLines: 1, ContextLines: 1},
	)
	if result.Diff != "" {
		t.Fatalf("diff=%q, want empty", result.Diff)
	}
	if result.SkipReason != "too_many_lines" {
		t.Fatalf("skip=%q, want too_many_lines", result.SkipReason)
	}
}

func makeOptimizerLines(n int) []string {
	lines := make([]string, n)
	for i := range lines {
		lines[i] = fmt.Sprintf("line %04d original", i+1)
	}
	return lines
}
