package proxy

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	govpkg "github.com/Revanth14/indexqube/gateway/internal/governor"
)

func TestBrowserPromptFixtures_NormalizeRawText(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		paths     []string
		wantPlain []string
	}{
		{
			name:      "natural_language_only",
			wantPlain: []string{"Can you explain how the optimizer works", "Please do not change anything yet."},
		},
		{
			name:      "single_file_question_before",
			paths:     []string{"src/main.go"},
			wantPlain: []string{"Can you review this bug?", "What is wrong here?"},
		},
		{
			name:      "multi_file_question_before_after",
			paths:     []string{"src/a.go", "src/b.go"},
			wantPlain: []string{"Find the bug across these files.", "What should change?"},
		},
		{
			name:      "markdown_headings",
			paths:     []string{"gateway/internal/proxy/handlers.go", "gateway/internal/proxy/browser_prompt.go"},
			wantPlain: []string{"Review these parser files."},
		},
		{
			name:  "tiny_code_not_smaller",
			paths: []string{"src/tiny.go"},
		},
		{
			name:      "react_component",
			paths:     []string{"components/UserCard.tsx"},
			wantPlain: []string{"Why is the email not rendering?"},
		},
		{
			name:      "nextjs_api_route",
			paths:     []string{"pages/api/users.ts"},
			wantPlain: []string{"Getting 404 on POST requests to this route. What am I missing?"},
		},
		{
			name:      "python_script",
			paths:     []string{"scripts/process.py"},
			wantPlain: []string{"This crashes on empty input. How do I fix it?"},
		},
		{
			name:      "large_go_file",
			paths:     []string{"internal/gateway/server.go"},
			wantPlain: []string{"Can you review this service implementation?"},
		},
		{
			name:      "markdown_heavy_with_code",
			paths:     []string{"internal/config/loader.go"},
			wantPlain: []string{"Here is what I'm seeing:", "Does Load() look thread-safe to you?"},
		},
		{
			name:      "multi_file_frontend",
			paths:     []string{"src/components/Parent.tsx", "src/components/Child.tsx"},
			wantPlain: []string{"I have two components that share state.", "Why is the parent not re-rendering when count changes?"},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			input := readBrowserPromptFixture(t, tc.name+".txt")
			want := readBrowserPromptFixture(t, tc.name+".golden")

			got := normalizeRawOptimizeText(input, "", "")
			if got != want {
				t.Fatalf("normalized fixture mismatch\n--- got ---\n%s\n--- want ---\n%s", got, want)
			}

			blocks := govpkg.ExtractCodeBlocks(got)
			if len(blocks) != len(tc.paths) {
				t.Fatalf("blocks=%d want %d; normalized:\n%s", len(blocks), len(tc.paths), got)
			}
			for i, path := range tc.paths {
				if blocks[i].Path != path {
					t.Fatalf("block[%d].Path=%q want %q; normalized:\n%s", i, blocks[i].Path, path, got)
				}
			}
			for _, plain := range tc.wantPlain {
				if !strings.Contains(got, plain) {
					t.Fatalf("plain prompt text %q missing from normalized fixture:\n%s", plain, got)
				}
			}
		})
	}
}

func TestBrowserPromptFixtures_TinyCodeNotSmallerRecordsSkip(t *testing.T) {
	t.Parallel()
	gov := govpkg.New(
		govpkg.WithHistory(govpkg.NewMemoryHistory()),
		govpkg.WithPruning(true, 8000),
	)
	srv := newTestServer(t, gov)
	input := readBrowserPromptFixture(t, "tiny_code_not_smaller.txt")
	want := readBrowserPromptFixture(t, "tiny_code_not_smaller.golden")

	first := postOptimizeText(t, srv.URL, "fixture-not-smaller", "", input)
	if first.status != http.StatusOK {
		t.Fatalf("first status=%d body=%s", first.status, first.body)
	}
	if first.body != want {
		t.Fatalf("first normalized body mismatch\n--- got ---\n%s\n--- want ---\n%s", first.body, want)
	}

	second := postOptimizeText(t, srv.URL, "fixture-not-smaller", "", input)
	if second.status != http.StatusOK {
		t.Fatalf("second status=%d body=%s", second.status, second.body)
	}
	if got := second.header.Get("X-IQ-Blocks-Skipped"); got != "1" {
		t.Fatalf("X-IQ-Blocks-Skipped=%q want 1; body=%s", got, second.body)
	}
	if got := second.header.Get("X-IQ-Skip-Reasons"); got != "not_smaller=1" {
		t.Fatalf("X-IQ-Skip-Reasons=%q want not_smaller=1", got)
	}
	if second.body != want {
		t.Fatalf("second body should remain normalized original\n--- got ---\n%s\n--- want ---\n%s", second.body, want)
	}
}

func readBrowserPromptFixture(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "browser_prompts", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return strings.TrimSpace(string(b))
}
