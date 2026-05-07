package governor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/domain"
)

func TestExtractCodeBlocks_GoPathFence(t *testing.T) {
	t.Parallel()
	content := "intro\n```go src/foo.go\nalpha\nbeta\n```\ntrailer"
	blocks := ExtractCodeBlocks(content)
	if len(blocks) != 1 {
		t.Fatalf("blocks=%d want 1", len(blocks))
	}
	if blocks[0].Path != "src/foo.go" {
		t.Errorf("path=%q", blocks[0].Path)
	}
	if blocks[0].Lang != "go" {
		t.Errorf("lang=%q", blocks[0].Lang)
	}
	if !strings.Contains(blocks[0].Inner, "alpha") {
		t.Errorf("inner=%q", blocks[0].Inner)
	}
}

func TestExtractCodeBlocks_FileHintComment(t *testing.T) {
	t.Parallel()
	content := "```go\n// file: pkg/bar.go\none\n```"
	blocks := ExtractCodeBlocks(content)
	if len(blocks) != 1 || blocks[0].Path != "pkg/bar.go" {
		t.Fatalf("blocks=%v", blocks)
	}
}

func TestExtractCodeBlocks_LangOnlyFenceWithFileHint(t *testing.T) {
	t.Parallel()
	content := "```go\n// file: a/b/c.go\nx\n```"
	blocks := ExtractCodeBlocks(content)
	if len(blocks) != 1 {
		t.Fatalf("blocks=%d want 1", len(blocks))
	}
	if blocks[0].Lang != "go" {
		t.Errorf("lang=%q", blocks[0].Lang)
	}
	if blocks[0].Path != "a/b/c.go" {
		t.Errorf("path=%q", blocks[0].Path)
	}
}

func TestExtractCodeBlocks_WindowsNewlines(t *testing.T) {
	t.Parallel()
	content := "intro\r\n```go file=src/win.go\r\none\r\ntwo\r\n```\r\noutro\r\n"
	blocks := ExtractCodeBlocks(content)
	if len(blocks) != 1 {
		t.Fatalf("blocks=%d want 1", len(blocks))
	}
	if blocks[0].Path != "src/win.go" {
		t.Errorf("path=%q", blocks[0].Path)
	}
	// Ensure fence was captured as one region.
	if blocks[0].Start < 0 || blocks[0].End <= blocks[0].Start {
		t.Fatalf("bad offsets: start=%d end=%d", blocks[0].Start, blocks[0].End)
	}
}

func TestExtractCodeBlocks_DoesNotCloseOnInlineBackticksInBody(t *testing.T) {
	t.Parallel()
	content := "```md file=docs/example.md\nline1\nhere is a literal ```not a fence\nline3\n```\n"
	blocks := ExtractCodeBlocks(content)
	if len(blocks) != 1 {
		t.Fatalf("blocks=%d want 1", len(blocks))
	}
	if !strings.Contains(blocks[0].Inner, "line3") {
		t.Fatalf("block closed early, inner=%q", blocks[0].Inner)
	}
}

func TestUnifiedLineDiff_SimpleChange(t *testing.T) {
	t.Parallel()
	oldL := []string{"a", "b", "c"}
	newL := []string{"a", "x", "c"}
	u := UnifiedLineDiff("f.go", oldL, newL, 100)
	if u == "" {
		t.Fatal("empty diff")
	}
	if !strings.Contains(u, "- b") || !strings.Contains(u, "+ x") {
		t.Fatalf("missing change lines:\n%s", u)
	}
}

func TestPruneMessages_SecondRoundIdentical(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	h := NewMemoryHistory()
	tenant := "t1"
	body := "```go src/x.go\n" + strings.Join(makeNumberedLines(80), "\n") + "\n```"
	msgs := []domain.Message{{Role: "user", Content: body}}
	out1, st1 := PruneMessages(ctx, h, tenant, msgs, 8000, nil)
	if st1.BlocksSeen != 1 || st1.BlocksPruned != 0 {
		t.Fatalf("first round: %+v", st1)
	}
	if out1[0].Content != body {
		t.Error("first round should keep body verbatim")
	}

	out2, st2 := PruneMessages(ctx, h, tenant, out1, 8000, nil)
	if st2.BlocksPruned != 1 {
		t.Fatalf("second round pruned=%d want 1", st2.BlocksPruned)
	}
	if strings.Contains(out2[0].Content, "```go src/x.go") {
		t.Error("expected fence replaced with short marker")
	}
	if !strings.Contains(out2[0].Content, "No changes") {
		t.Errorf("got %q", out2[0].Content)
	}
}

func TestPruneMessages_SecondRoundChangeBecomesDiff(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	h := NewMemoryHistory()
	tenant := "t1"

	oldLines := makeNumberedLines(80)
	newLines := append([]string(nil), oldLines...)
	newLines[40] = "line 0041 changed"

	body1 := "```go src/x.go\n" + strings.Join(oldLines, "\n") + "\n```"
	body2 := "```go src/x.go\n" + strings.Join(newLines, "\n") + "\n```"

	msg1 := []domain.Message{{Role: "user", Content: body1}}
	out1, _ := PruneMessages(ctx, h, tenant, msg1, 8000, nil)
	msg2 := []domain.Message{{Role: "user", Content: body2}}
	out2, st2 := PruneMessages(ctx, h, tenant, msg2, 8000, nil)
	if st2.BlocksPruned != 1 {
		t.Fatalf("pruned=%d want 1", st2.BlocksPruned)
	}
	if !strings.Contains(out2[0].Content, "```diff") {
		t.Fatalf("expected diff fence, got:\n%s", out2[0].Content)
	}
	if !strings.Contains(out2[0].Content, "- line 0041 original") || !strings.Contains(out2[0].Content, "+ line 0041 changed") {
		t.Fatalf("diff missing change lines:\n%s", out2[0].Content)
	}
	_ = out1
}

func TestPruneMessages_SkipsReplacementWhenDiffIsNotSmaller(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	h := NewMemoryHistory()
	tenant := "t1"

	body1 := "```go src/x.go\nhello\nworld\n```"
	body2 := "```go src/x.go\nhello\nWORLD\n```"
	_, _ = PruneMessages(ctx, h, tenant, []domain.Message{{Role: "user", Content: body1}}, 8000, nil)
	out, st := PruneMessages(ctx, h, tenant, []domain.Message{{Role: "user", Content: body2}}, 8000, nil)
	if st.BlocksPruned != 0 || st.BlocksSkipped != 1 || st.SkipReasons["not_smaller"] != 1 {
		t.Fatalf("expected not_smaller skip, stats=%+v", st)
	}
	if out[0].Content != body2 {
		t.Fatalf("not-smaller replacement should leave prompt verbatim, got:\n%s", out[0].Content)
	}
}

func TestPruneMessages_ChangedLargeFileUsesCompactHunk(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	h := NewMemoryHistory()
	tenant := "t1"

	oldLines := make([]string, 80)
	newLines := make([]string, 80)
	for i := range oldLines {
		oldLines[i] = "line same"
		newLines[i] = "line same"
	}
	oldLines[40] = "old interesting line"
	newLines[40] = "new interesting line"

	body1 := "```go src/large.go\n" + strings.Join(oldLines, "\n") + "\n```"
	body2 := "```go src/large.go\n" + strings.Join(newLines, "\n") + "\n```"
	_, _ = PruneMessages(ctx, h, tenant, []domain.Message{{Role: "user", Content: body1}}, 8000, nil)
	out, st := PruneMessages(ctx, h, tenant, []domain.Message{{Role: "user", Content: body2}}, 8000, nil)
	if st.BlocksPruned != 1 {
		t.Fatalf("blocks_pruned=%d want 1; stats=%+v", st.BlocksPruned, st)
	}
	if !strings.Contains(out[0].Content, "- old interesting line") || !strings.Contains(out[0].Content, "+ new interesting line") {
		t.Fatalf("missing changed lines:\n%s", out[0].Content)
	}
	if strings.Count(out[0].Content, "line same") > 8 {
		t.Fatalf("diff is not compact enough, same-line count=%d:\n%s", strings.Count(out[0].Content, "line same"), out[0].Content)
	}
	if st.ReductionRatio <= 0 {
		t.Fatalf("expected positive reduction ratio, stats=%+v", st)
	}
}

func TestPruneMessages_TwoThousandLineFileReturnsOnlyChangedHunk(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	h := NewMemoryHistory()
	tenant := "t1"

	oldLines := makeNumberedLines(2000)
	newLines := append([]string(nil), oldLines...)
	for i := 1000; i < 1015; i++ {
		newLines[i] = fmt.Sprintf("line %04d CHANGED", i+1)
	}

	body1 := "```go src/two_thousand.go\n" + strings.Join(oldLines, "\n") + "\n```"
	body2 := "```go src/two_thousand.go\n" + strings.Join(newLines, "\n") + "\n```"
	_, _ = PruneMessages(ctx, h, tenant, []domain.Message{{Role: "user", Content: body1}}, 8000, nil)
	out, st := PruneMessages(ctx, h, tenant, []domain.Message{{Role: "user", Content: body2}}, 8000, nil)

	if st.BlocksPruned != 1 || st.BlocksSkipped != 0 {
		t.Fatalf("expected one pruned block and no skips, stats=%+v", st)
	}
	diff := out[0].Content
	for i := 1000; i < 1015; i++ {
		if !strings.Contains(diff, "+ "+newLines[i]) {
			t.Fatalf("missing changed line %q in diff:\n%s", newLines[i], diff)
		}
	}
	if strings.Contains(diff, "line 0001 original") || strings.Contains(diff, "line 2000 original") {
		t.Fatalf("diff leaked far-away unchanged lines:\n%s", diff)
	}
	if contextLines := countDiffLinesWithPrefix(diff, "  "); contextLines > 6 {
		t.Fatalf("expected only a few context lines, got %d unchanged lines:\n%s", contextLines, diff)
	}
	if len(diff) >= len(body2)/10 {
		t.Fatalf("diff too large: diff bytes=%d original bytes=%d", len(diff), len(body2))
	}
}

func TestPruneMessages_TooLargeDiffRecordsSkip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	h := NewMemoryHistory()
	tenant := "t1"

	body1 := "```go src/x.go\na\nb\nc\n```"
	body2 := "```go src/x.go\na\nB\nc\n```"
	_, _ = PruneMessages(ctx, h, tenant, []domain.Message{{Role: "user", Content: body1}}, 1, nil)
	out, st := PruneMessages(ctx, h, tenant, []domain.Message{{Role: "user", Content: body2}}, 1, nil)
	if st.BlocksSkipped != 1 || st.SkipReasons["too_many_lines"] != 1 {
		t.Fatalf("expected too_many_lines skip, stats=%+v", st)
	}
	if out[0].Content != body2 {
		t.Fatalf("skipped diff should leave body verbatim, got %q", out[0].Content)
	}
}

func TestGovernor_Optimize_InjectsMemory(t *testing.T) {
	t.Parallel()
	g := New(
		WithHistory(NewMemoryHistory()),
		WithPruning(false, 8000),
	)
	msgs, stats, err := g.Optimize(t.Context(), "u1", []domain.Message{{Role: "user", Content: "hi"}}, "# rules")
	if err != nil {
		t.Fatal(err)
	}
	if stats.BlocksSeen != 0 {
		t.Fatalf("stats %+v", stats)
	}
	if len(msgs) != 2 || msgs[0].Role != "system" || !strings.Contains(msgs[0].Content, "rules") {
		t.Fatalf("msgs=%+v", msgs)
	}
}

func TestLoadProjectMemory_ReadsIndexQubeContext(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "indexqube_context.md")
	if err := os.WriteFile(path, []byte("# Rules\n\nPrefer compact diffs.\n"), 0o600); err != nil {
		t.Fatalf("write context: %v", err)
	}
	mem, err := LoadProjectMemory(path)
	if err != nil {
		t.Fatalf("LoadProjectMemory: %v", err)
	}
	if mem != "# Rules\n\nPrefer compact diffs." {
		t.Fatalf("memory=%q", mem)
	}
}

func TestLoadProjectMemory_MissingFileIsEmpty(t *testing.T) {
	t.Parallel()
	mem, err := LoadProjectMemory(filepath.Join(t.TempDir(), "indexqube_context.md"))
	if err != nil {
		t.Fatalf("LoadProjectMemory missing file: %v", err)
	}
	if mem != "" {
		t.Fatalf("memory=%q, want empty", mem)
	}
}

func TestLoadProjectMemory_Directory(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Create a hierarchy
	if err := os.WriteFile(filepath.Join(dir, "01_root.md"), []byte("Root instructions."), 0o600); err != nil {
		t.Fatal(err)
	}
	sub := filepath.Join(dir, "sub")
	if err := os.Mkdir(sub, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "02_nested.md"), []byte("Sub instructions."), 0o600); err != nil {
		t.Fatal(err)
	}

	mem, err := LoadProjectMemory(dir)
	if err != nil {
		t.Fatalf("LoadProjectMemory: %v", err)
	}

	if !strings.Contains(mem, "## File: 01_root.md") {
		t.Errorf("missing root file header:\n%s", mem)
	}
	if !strings.Contains(mem, "Root instructions.") {
		t.Errorf("missing root content:\n%s", mem)
	}
	if !strings.Contains(mem, "## File: sub/02_nested.md") {
		t.Errorf("missing sub file header:\n%s", mem)
	}
	if !strings.Contains(mem, "Sub instructions.") {
		t.Errorf("missing sub content:\n%s", mem)
	}
}

func TestGovernor_Optimize_MergesStaticAndRequestMemoryWithPrunedPrompt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g := New(
		WithHistory(NewMemoryHistory()),
		WithPruning(true, 8000),
		WithProjectMemory("Always answer with repository-specific context."),
	)
	tenant := "t1"
	oldLines := makeNumberedLines(80)
	newLines := append([]string(nil), oldLines...)
	newLines[40] = "\tprintln(\"hello indexqube\")"
	body1 := "```go src/x.go\n" + strings.Join(oldLines, "\n") + "\n```"
	body2 := "```go src/x.go\n" + strings.Join(newLines, "\n") + "\n```"

	if _, _, err := g.Optimize(ctx, tenant, []domain.Message{{Role: "user", Content: body1}}, "Use short replies."); err != nil {
		t.Fatal(err)
	}
	msgs, stats, err := g.Optimize(ctx, tenant, []domain.Message{{Role: "user", Content: body2}}, "Use short replies.")
	if err != nil {
		t.Fatal(err)
	}
	if stats.BlocksPruned != 1 {
		t.Fatalf("expected pruned prompt, stats=%+v", stats)
	}
	if len(msgs) != 2 {
		t.Fatalf("msgs len=%d want 2: %+v", len(msgs), msgs)
	}
	if msgs[0].Role != "system" {
		t.Fatalf("first role=%q want system", msgs[0].Role)
	}
	for _, want := range []string{
		projectMemoryTitle,
		"## Static project rules",
		"Always answer with repository-specific context.",
		"## Request memory",
		"Use short replies.",
	} {
		if !strings.Contains(msgs[0].Content, want) {
			t.Fatalf("system memory missing %q:\n%s", want, msgs[0].Content)
		}
	}
	if !strings.Contains(msgs[1].Content, "```diff") || !strings.Contains(msgs[1].Content, "+ \tprintln(\"hello indexqube\")") {
		t.Fatalf("user prompt was not pruned/merged correctly:\n%s", msgs[1].Content)
	}
}

func TestMemoryHistory_EvictsByTTL(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
	h := NewMemoryHistoryWithConfig(MemoryHistoryConfig{TTL: time.Minute})
	h.nowFn = func() time.Time { return now }
	h.Put(context.Background(), "t1", "a.go", "body")
	now = now.Add(2 * time.Minute)
	if _, ok := h.Get(context.Background(), "t1", "a.go"); ok {
		t.Fatal("expected expired history miss")
	}
	if stats := h.Stats(); stats.Entries != 0 || stats.Bytes != 0 {
		t.Fatalf("expected empty stats after TTL eviction, got %+v", stats)
	}
}

func TestMemoryHistory_StoresContentHash(t *testing.T) {
	t.Parallel()
	h := NewMemoryHistory()
	h.Put(context.Background(), "t1", "a.go", "package main")
	snap, ok := h.Get(context.Background(), "t1", "a.go")
	if !ok {
		t.Fatal("expected snapshot")
	}
	if snap.Hash == "" {
		t.Fatal("expected non-empty hash")
	}
	if snap.Hash != ContentHash("package main") {
		t.Fatalf("hash=%q want %q", snap.Hash, ContentHash("package main"))
	}
}

func TestMemoryHistory_EvictsByFileAndTenantLimits(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
	h := NewMemoryHistoryWithConfig(MemoryHistoryConfig{MaxTenants: 1, MaxFilesPerTenant: 1})
	h.nowFn = func() time.Time { return now }
	h.Put(context.Background(), "t1", "a.go", "a")
	now = now.Add(time.Second)
	h.Put(context.Background(), "t1", "b.go", "b")
	if _, ok := h.Get(context.Background(), "t1", "a.go"); ok {
		t.Fatal("expected oldest file to be evicted")
	}
	if _, ok := h.Get(context.Background(), "t1", "b.go"); !ok {
		t.Fatal("expected newest file to remain")
	}

	now = now.Add(time.Second)
	h.Put(context.Background(), "t2", "c.go", "c")
	if _, ok := h.Get(context.Background(), "t1", "b.go"); ok {
		t.Fatal("expected oldest tenant to be evicted")
	}
	if _, ok := h.Get(context.Background(), "t2", "c.go"); !ok {
		t.Fatal("expected newest tenant to remain")
	}
}

func TestMemoryHistory_EvictsByByteLimits(t *testing.T) {
	t.Parallel()
	h := NewMemoryHistoryWithConfig(MemoryHistoryConfig{MaxFileBytes: 3, MaxBytes: 5})
	h.Put(context.Background(), "t1", "large.go", "toolarge")
	if _, ok := h.Get(context.Background(), "t1", "large.go"); ok {
		t.Fatal("expected oversized file to be rejected")
	}
	h.Put(context.Background(), "t1", "a.go", "abc")
	h.Put(context.Background(), "t1", "b.go", "def")
	stats := h.Stats()
	if stats.Bytes > 5 || stats.Entries != 1 {
		t.Fatalf("expected byte-budget eviction, got %+v", stats)
	}
}

func makeNumberedLines(n int) []string {
	lines := make([]string, n)
	for i := range lines {
		lines[i] = fmt.Sprintf("line %04d original", i+1)
	}
	return lines
}

func countDiffLinesWithPrefix(diff, prefix string) int {
	count := 0
	for _, line := range strings.Split(diff, "\n") {
		if strings.HasPrefix(line, prefix) {
			count++
		}
	}
	return count
}
