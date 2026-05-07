package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Revanth14/indexqube/gateway/internal/domain"
	govpkg "github.com/Revanth14/indexqube/gateway/internal/governor"
)

// fakeGovernor lets tests script the governor's streaming behavior.
type fakeGovernor struct {
	streamFunc   func(ctx context.Context, req *domain.InferenceRequest, tw domain.TokenWriter) error
	optimizeFunc func(ctx context.Context, tenant string, messages []domain.Message, projectMemory string) ([]domain.Message, domain.PruneStats, error)
	readyErr     error
	gotReq       *domain.InferenceRequest
	gotTenant    string
}

func (f *fakeGovernor) Optimize(ctx context.Context, tenant string, messages []domain.Message, projectMemory string) ([]domain.Message, domain.PruneStats, error) {
	f.gotTenant = tenant
	if f.optimizeFunc != nil {
		return f.optimizeFunc(ctx, tenant, messages, projectMemory)
	}
	_ = projectMemory
	return messages, domain.PruneStats{}, nil
}

func (f *fakeGovernor) Stream(ctx context.Context, req *domain.InferenceRequest, tw domain.TokenWriter) error {
	f.gotReq = req
	if f.streamFunc != nil {
		return f.streamFunc(ctx, req, tw)
	}
	return nil
}

func (f *fakeGovernor) Ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return f.readyErr
}

func newTestServer(t *testing.T, gov Governor) *httptest.Server {
	t.Helper()
	p := New(gov)
	srv := httptest.NewServer(p.Handler())
	t.Cleanup(srv.Close)
	return srv
}

func validBody(t *testing.T) []byte {
	t.Helper()
	b, err := json.Marshal(domain.InferenceRequest{
		Model:    "claude-3-5-sonnet",
		Messages: []domain.Message{{Role: "user", Content: "hello"}},
		Stream:   true,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func TestExtractCredential(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		provider string
		key      string
		wantErr  error
	}{
		{"valid anthropic", "anthropic", "sk-ant-xyz", nil},
		{"valid uppercased", "OPENAI", "sk-xyz", nil},
		{"missing provider", "", "sk-xyz", errMissingProvider},
		{"unknown provider", "cohere", "sk-xyz", errUnknownProvider},
		{"missing key", "anthropic", "", errMissingKey},
		{"whitespace key", "anthropic", "   ", errMissingKey},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
			if tc.provider != "" {
				r.Header.Set(headerProvider, tc.provider)
			}
			if tc.key != "" {
				r.Header.Set(headerKey, tc.key)
			}
			_, err := extractCredential(r)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("got err=%v, want=%v", err, tc.wantErr)
			}
		})
	}
}

func TestSSEWriter_FramesAndFlushes(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	sw, err := newSSEWriter(rec)
	if err != nil {
		t.Fatalf("newSSEWriter: %v", err)
	}
	if err := sw.WriteData([]byte(`{"delta":"hi"}`)); err != nil {
		t.Fatalf("WriteData: %v", err)
	}
	if err := sw.WriteEvent("error", []byte(`{"x":1}`)); err != nil {
		t.Fatalf("WriteEvent: %v", err)
	}
	if err := sw.WriteDone(); err != nil {
		t.Fatalf("WriteDone: %v", err)
	}

	got := rec.Body.String()
	if !strings.Contains(got, "data: {\"delta\":\"hi\"}\n\n") {
		t.Errorf("missing data frame, body=%q", got)
	}
	if !strings.Contains(got, "event: error\ndata: {\"x\":1}\n\n") {
		t.Errorf("missing event frame, body=%q", got)
	}
	if !strings.Contains(got, "data: [DONE]\n\n") {
		t.Errorf("missing done sentinel, body=%q", got)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type=%q, want text/event-stream", ct)
	}
	if cc := rec.Header().Get("Cache-Control"); !strings.Contains(cc, "no-cache") {
		t.Errorf("Cache-Control=%q missing no-cache", cc)
	}
	if ab := rec.Header().Get("X-Accel-Buffering"); ab != "no" {
		t.Errorf("X-Accel-Buffering=%q, want no", ab)
	}
}

func TestHealthEndpoints(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t, &fakeGovernor{})

	for _, path := range []string{"/healthz", "/readyz"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Errorf("%s status=%d, want 200", path, resp.StatusCode)
		}
		_ = resp.Body.Close()
	}
}

func TestReadyz_GovernorNotReady(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t, &fakeGovernor{readyErr: errors.New("adapter warming up")})

	resp, err := http.Get(srv.URL + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503", resp.StatusCode)
	}
	var env errorEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.Error.Code != "not_ready" {
		t.Fatalf("code=%q, want not_ready", env.Error.Code)
	}
}

func TestChatCompletions_MissingProvider(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t, &fakeGovernor{})
	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json", bytes.NewReader(validBody(t)))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d, want 400", resp.StatusCode)
	}
	var env errorEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatalf("decode err envelope: %v", err)
	}
	if env.Error.Code != "missing_provider" {
		t.Errorf("error.code=%q, want missing_provider", env.Error.Code)
	}
}

func TestChatCompletions_NonStreamingRejected(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t, &fakeGovernor{})
	body, _ := json.Marshal(domain.InferenceRequest{
		Model:    "claude-3-5-sonnet",
		Messages: []domain.Message{{Role: "user", Content: "hello"}},
		Stream:   false,
	})
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions", bytes.NewReader(body))
	req.Header.Set(headerProvider, "anthropic")
	req.Header.Set(headerKey, "sk-ant-xyz")
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d, want 400", resp.StatusCode)
	}
	var env errorEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.Error.Code != "stream_required" {
		t.Errorf("error.code=%q, want stream_required", env.Error.Code)
	}
}

func TestChatCompletions_HappyPathStreaming(t *testing.T) {
	t.Parallel()

	gov := &fakeGovernor{
		streamFunc: func(_ context.Context, _ *domain.InferenceRequest, tw domain.TokenWriter) error {
			if err := tw.WriteData([]byte(`{"delta":"he"}`)); err != nil {
				return err
			}
			if err := tw.WriteData([]byte(`{"delta":"llo"}`)); err != nil {
				return err
			}
			return nil
		},
	}
	srv := newTestServer(t, gov)

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions", bytes.NewReader(validBody(t)))
	req.Header.Set(headerProvider, "anthropic")
	req.Header.Set(headerKey, "sk-ant-xyz")
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type=%q, want text/event-stream", ct)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	bodyStr := string(body)

	wantFrames := []string{
		"data: {\"delta\":\"he\"}\n\n",
		"data: {\"delta\":\"llo\"}\n\n",
		"data: [DONE]\n\n",
	}
	for _, frame := range wantFrames {
		if !strings.Contains(bodyStr, frame) {
			t.Errorf("missing frame %q in body=%q", frame, bodyStr)
		}
	}

	if gov.gotReq == nil {
		t.Fatal("governor did not receive a request")
	}
	if gov.gotReq.Credential.Provider != domain.ProviderAnthropic {
		t.Errorf("got provider=%q, want anthropic", gov.gotReq.Credential.Provider)
	}
	if gov.gotReq.Credential.APIKey != "sk-ant-xyz" {
		t.Errorf("got key=%q, want sk-ant-xyz", gov.gotReq.Credential.APIKey)
	}
}

func TestChatCompletions_GovernorErrorEmitsErrorEvent(t *testing.T) {
	t.Parallel()

	gov := &fakeGovernor{
		streamFunc: func(_ context.Context, _ *domain.InferenceRequest, tw domain.TokenWriter) error {
			_ = tw.WriteData([]byte(`{"delta":"partial"}`))
			return errors.New("upstream provider exploded")
		},
	}
	srv := newTestServer(t, gov)

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions", bytes.NewReader(validBody(t)))
	req.Header.Set(headerProvider, "anthropic")
	req.Header.Set(headerKey, "sk-ant-xyz")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	var sawError bool
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "event: error") {
			sawError = true
			break
		}
	}
	if !sawError {
		t.Error("expected an SSE `event: error` frame on governor failure")
	}
}

func TestOptimize_WithoutSessionKeyIsStateless(t *testing.T) {
	t.Parallel()
	gov := govpkg.New(
		govpkg.WithHistory(govpkg.NewMemoryHistory()),
		govpkg.WithPruning(true, 8000),
	)
	srv := newTestServer(t, gov)

	body := testFencedGo("src/x.go", "hello")
	first := postOptimize(t, srv.URL, "", body)
	second := postOptimize(t, srv.URL, "", body)
	if first.Stats.BlocksPruned != 0 || second.Stats.BlocksPruned != 0 {
		t.Fatalf("anonymous optimize must be stateless, first=%+v second=%+v", first.Stats, second.Stats)
	}
	if second.Messages[0].Content != body {
		t.Fatalf("anonymous optimize changed body: %q", second.Messages[0].Content)
	}
}

func TestOptimize_SameSessionPrunesSecondRequest(t *testing.T) {
	t.Parallel()
	gov := govpkg.New(
		govpkg.WithHistory(govpkg.NewMemoryHistory()),
		govpkg.WithPruning(true, 8000),
	)
	srv := newTestServer(t, gov)

	body := testFencedGo("src/x.go", "hello")
	_ = postOptimize(t, srv.URL, "session-a", body)
	second := postOptimize(t, srv.URL, "session-a", body)
	if second.Stats.BlocksPruned != 1 {
		t.Fatalf("blocks_pruned=%d, want 1; stats=%+v", second.Stats.BlocksPruned, second.Stats)
	}
	if !strings.Contains(second.Messages[0].Content, "No changes") {
		t.Fatalf("expected no-change marker, got %q", second.Messages[0].Content)
	}
}

func TestOptimize_DifferentSessionsDoNotShareHistory(t *testing.T) {
	t.Parallel()
	gov := govpkg.New(
		govpkg.WithHistory(govpkg.NewMemoryHistory()),
		govpkg.WithPruning(true, 8000),
	)
	srv := newTestServer(t, gov)

	body := "```go src/x.go\nhello\nworld\n```"
	_ = postOptimize(t, srv.URL, "session-a", body)
	second := postOptimize(t, srv.URL, "session-b", body)
	if second.Stats.BlocksPruned != 0 {
		t.Fatalf("different sessions must not share history, stats=%+v", second.Stats)
	}
	if second.Messages[0].Content != body {
		t.Fatalf("unexpected optimize output: %q", second.Messages[0].Content)
	}
}

func TestOptimize_JSONPromptContextShape(t *testing.T) {
	t.Parallel()
	gov := &fakeGovernor{}
	srv := newTestServer(t, gov)

	body := []byte(`{
		"session_id": "test-session-123",
		"prompt": "Can you fix the bug in this function?",
		"context_text": "func calculate() {\n return 1 + 1\n }"
	}`)
	resp, err := http.Post(srv.URL+"/v1/optimize", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/optimize: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s", resp.StatusCode, payload)
	}
	if gov.gotTenant == "" {
		t.Fatal("expected session_id to produce a tenant key")
	}
	var out optimizeResponseBody
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out.Messages) != 1 {
		t.Fatalf("messages=%d want 1", len(out.Messages))
	}
	content := out.Messages[0].Content
	if !strings.Contains(content, "Can you fix the bug") || !strings.Contains(content, "func calculate") {
		t.Fatalf("prompt/context not merged into user message: %q", content)
	}
}

func TestOptimize_JSONPromptContextAutoWrapsAndPrunes(t *testing.T) {
	t.Parallel()
	gov := govpkg.New(
		govpkg.WithHistory(govpkg.NewMemoryHistory()),
		govpkg.WithPruning(true, 8000),
	)
	srv := newTestServer(t, gov)

	first := postOptimizePromptContext(t, srv.URL, map[string]string{
		"session_id":   "json-auto-wrap",
		"prompt":       "Can you fix the bug?",
		"context_path": "src/calc.go",
		"context_text": testGoInner("hello"),
	})
	if first.Stats.BlocksSeen != 1 || first.Stats.BlocksPruned != 0 {
		t.Fatalf("first stats=%+v", first.Stats)
	}

	second := postOptimizePromptContext(t, srv.URL, map[string]string{
		"session_id":   "json-auto-wrap",
		"prompt":       "Can you fix the bug?",
		"context_path": "src/calc.go",
		"context_text": testGoInner("hello indexqube"),
	})
	if second.Stats.BlocksSeen != 1 || second.Stats.BlocksPruned != 1 {
		t.Fatalf("second stats=%+v", second.Stats)
	}
	content := second.Messages[0].Content
	if !strings.Contains(content, "Can you fix the bug?") {
		t.Fatalf("prompt missing from optimized content:\n%s", content)
	}
	if !strings.Contains(content, "```diff") || !strings.Contains(content, "+++ b/src/calc.go") {
		t.Fatalf("expected compact diff for auto-wrapped context:\n%s", content)
	}
	if strings.Contains(content, "```go src/calc.go") {
		t.Fatalf("expected raw fence to be replaced by diff:\n%s", content)
	}
}

func TestOptimize_JSONResponseIncludesStatsHeaders(t *testing.T) {
	t.Parallel()
	gov := govpkg.New(
		govpkg.WithHistory(govpkg.NewMemoryHistory()),
		govpkg.WithPruning(true, 8000),
	)
	srv := newTestServer(t, gov)

	_ = postOptimizePromptContext(t, srv.URL, map[string]string{
		"session_id":   "json-stats-headers",
		"context_path": "src/calc.go",
		"context_text": testGoInner("hello"),
	})
	body, err := json.Marshal(map[string]string{
		"session_id":   "json-stats-headers",
		"context_path": "src/calc.go",
		"context_text": testGoInner("hello indexqube"),
	})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(srv.URL+"/v1/optimize", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/optimize: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s", resp.StatusCode, payload)
	}
	for _, h := range []string{
		"X-IQ-Blocks-Seen",
		"X-IQ-Blocks-Pruned",
		"X-IQ-Bytes-Before",
		"X-IQ-Bytes-After",
		"X-IQ-Tokens-Before",
		"X-IQ-Tokens-After",
		"X-IQ-Reduction-Ratio",
		"X-IQ-Diff-Exact",
	} {
		if resp.Header.Get(h) == "" {
			t.Fatalf("missing %s header; headers=%v", h, resp.Header)
		}
	}
	if got := resp.Header.Get("X-IQ-Blocks-Pruned"); got != "1" {
		t.Fatalf("X-IQ-Blocks-Pruned=%q want 1", got)
	}
	if got := resp.Header.Get("X-IQ-Diff-Exact"); got != "1" {
		t.Fatalf("X-IQ-Diff-Exact=%q want 1", got)
	}
}

func TestOptimize_JSONContextTextUsesDefaultSyntheticPath(t *testing.T) {
	t.Parallel()
	gov := govpkg.New(
		govpkg.WithHistory(govpkg.NewMemoryHistory()),
		govpkg.WithPruning(true, 8000),
	)
	srv := newTestServer(t, gov)

	_ = postOptimizePromptContext(t, srv.URL, map[string]string{
		"session_id":   "json-default-path",
		"context_text": testGoInner("hello"),
	})
	second := postOptimizePromptContext(t, srv.URL, map[string]string{
		"session_id":   "json-default-path",
		"context_text": testGoInner("hello indexqube"),
	})
	if second.Stats.BlocksPruned != 1 {
		t.Fatalf("stats=%+v", second.Stats)
	}
	if !strings.Contains(second.Messages[0].Content, "+++ b/"+defaultRawContextPath) {
		t.Fatalf("expected synthetic path in diff:\n%s", second.Messages[0].Content)
	}
}

func TestOptimize_TextPlainReturnsCompressedPayload(t *testing.T) {
	t.Parallel()
	gov := govpkg.New(
		govpkg.WithHistory(govpkg.NewMemoryHistory()),
		govpkg.WithPruning(true, 8000),
	)
	srv := newTestServer(t, gov)

	body1 := testFencedGo("src/x.go", "hello")
	body2 := testFencedGo("src/x.go", "hello indexqube")
	_ = postOptimizeText(t, srv.URL, "session-a", "", body1)
	resp := postOptimizeText(t, srv.URL, "session-a", "", body2)

	if resp.status != http.StatusOK {
		t.Fatalf("status=%d body=%s", resp.status, resp.body)
	}
	if ct := resp.header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("Content-Type=%q, want text/plain", ct)
	}
	if got := resp.header.Get("X-IQ-Blocks-Pruned"); got != "1" {
		t.Fatalf("X-IQ-Blocks-Pruned=%q want 1; body=%s", got, resp.body)
	}
	if got := resp.header.Get("X-IQ-Tokens-Before"); got == "" {
		t.Fatalf("missing X-IQ-Tokens-Before header; headers=%v", resp.header)
	}
	if got := resp.header.Get("X-IQ-Diff-Exact"); got != "1" {
		t.Fatalf("X-IQ-Diff-Exact=%q want 1; body=%s", got, resp.body)
	}
	if !strings.Contains(resp.body, "```diff") {
		t.Fatalf("expected compressed diff payload, got:\n%s", resp.body)
	}
	if !strings.Contains(resp.body, "+ \tprintln(\"hello indexqube\")") {
		t.Fatalf("missing changed line:\n%s", resp.body)
	}
	if strings.Contains(resp.body, "```go src/x.go") {
		t.Fatalf("raw full code fence should be replaced, got:\n%s", resp.body)
	}
}

func TestOptimize_TextPlainAutoWrapsRawCode(t *testing.T) {
	t.Parallel()
	gov := govpkg.New(
		govpkg.WithHistory(govpkg.NewMemoryHistory()),
		govpkg.WithPruning(true, 8000),
	)
	srv := newTestServer(t, gov)

	_ = postOptimizeTextWithHeaders(t, srv.URL, map[string]string{
		headerSessionKey:  "raw-auto-wrap",
		headerContextPath: "src/raw.go",
	}, testGoInner("hello"))
	resp := postOptimizeTextWithHeaders(t, srv.URL, map[string]string{
		headerSessionKey:  "raw-auto-wrap",
		headerContextPath: "src/raw.go",
	}, testGoInner("hello indexqube"))

	if resp.status != http.StatusOK {
		t.Fatalf("status=%d body=%s", resp.status, resp.body)
	}
	if got := resp.header.Get("X-IQ-Blocks-Pruned"); got != "1" {
		t.Fatalf("X-IQ-Blocks-Pruned=%q want 1; body=%s", got, resp.body)
	}
	if !strings.Contains(resp.body, "```diff") || !strings.Contains(resp.body, "+++ b/src/raw.go") {
		t.Fatalf("expected diff for raw code:\n%s", resp.body)
	}
}

func TestOptimize_TextPlainMixedPromptWrapsOnlyCode(t *testing.T) {
	t.Parallel()
	gov := govpkg.New(
		govpkg.WithHistory(govpkg.NewMemoryHistory()),
		govpkg.WithPruning(true, 8000),
	)
	srv := newTestServer(t, gov)

	body1 := "what is wrong here?\n\n" + testGoInner("hello")
	body2 := "what is wrong here?\n\n" + testGoInner("hello indexqube")
	first := postOptimizeText(t, srv.URL, "raw-mixed-prompt", "", body1)
	if first.status != http.StatusOK {
		t.Fatalf("status=%d body=%s", first.status, first.body)
	}
	if got := first.header.Get("X-IQ-Blocks-Seen"); got != "1" {
		t.Fatalf("first X-IQ-Blocks-Seen=%q want 1; body=%s", got, first.body)
	}
	if !strings.HasPrefix(first.body, "what is wrong here?\n\n```go "+defaultRawContextPath) {
		t.Fatalf("question should stay outside fenced code:\n%s", first.body)
	}

	second := postOptimizeText(t, srv.URL, "raw-mixed-prompt", "", body2)
	if got := second.header.Get("X-IQ-Blocks-Pruned"); got != "1" {
		t.Fatalf("second X-IQ-Blocks-Pruned=%q want 1; body=%s", got, second.body)
	}
	if !strings.Contains(second.body, "what is wrong here?") {
		t.Fatalf("question missing from optimized payload:\n%s", second.body)
	}
	if !strings.Contains(second.body, "```diff") || !strings.Contains(second.body, "+ \tprintln(\"hello indexqube\")") {
		t.Fatalf("expected compact diff for mixed prompt:\n%s", second.body)
	}
	if strings.Contains(second.body, "```go "+defaultRawContextPath) {
		t.Fatalf("raw code fence should be replaced by diff on second request:\n%s", second.body)
	}
}

func TestOptimize_TextPlainMixedPromptUsesFileHint(t *testing.T) {
	t.Parallel()
	gov := govpkg.New(
		govpkg.WithHistory(govpkg.NewMemoryHistory()),
		govpkg.WithPruning(true, 8000),
	)
	srv := newTestServer(t, gov)

	body1 := "review this\n\nsrc/calc.go\n" + testGoInner("hello")
	body2 := "review this\n\nsrc/calc.go\n" + testGoInner("hello indexqube")
	first := postOptimizeText(t, srv.URL, "raw-mixed-file-hint", "", body1)
	if got := first.header.Get("X-IQ-Blocks-Seen"); got != "1" {
		t.Fatalf("first X-IQ-Blocks-Seen=%q want 1; body=%s", got, first.body)
	}
	if !strings.Contains(first.body, "```go src/calc.go\nfunc calculate") {
		t.Fatalf("expected file hint to become fenced path:\n%s", first.body)
	}
	if strings.Contains(first.body, "\nsrc/calc.go\nfunc calculate") {
		t.Fatalf("path hint should not remain as plain prompt text:\n%s", first.body)
	}

	second := postOptimizeText(t, srv.URL, "raw-mixed-file-hint", "", body2)
	if got := second.header.Get("X-IQ-Blocks-Pruned"); got != "1" {
		t.Fatalf("second X-IQ-Blocks-Pruned=%q want 1; body=%s", got, second.body)
	}
	if !strings.Contains(second.body, "+++ b/src/calc.go") {
		t.Fatalf("expected diff to use file hint path:\n%s", second.body)
	}
}

func TestOptimize_TextPlainMultiFilePromptPrunesEachFile(t *testing.T) {
	t.Parallel()
	gov := govpkg.New(
		govpkg.WithHistory(govpkg.NewMemoryHistory()),
		govpkg.WithPruning(true, 8000),
	)
	srv := newTestServer(t, gov)

	body1 := strings.Join([]string{
		"Find the bug across these files.",
		"",
		"src/a.go",
		testGoInner("file a"),
		"",
		"src/b.go",
		testGoInner("file b"),
		"",
		"What should change?",
	}, "\n")
	body2 := strings.Join([]string{
		"Find the bug across these files.",
		"",
		"src/a.go",
		testGoInner("file a changed"),
		"",
		"src/b.go",
		testGoInner("file b changed"),
		"",
		"What should change?",
	}, "\n")

	first := postOptimizeText(t, srv.URL, "raw-multi-file", "", body1)
	if got := first.header.Get("X-IQ-Blocks-Seen"); got != "2" {
		t.Fatalf("first X-IQ-Blocks-Seen=%q want 2; body=%s", got, first.body)
	}
	for _, want := range []string{
		"Find the bug across these files.",
		"```go src/a.go\nfunc calculate",
		"```go src/b.go\nfunc calculate",
		"What should change?",
	} {
		if !strings.Contains(first.body, want) {
			t.Fatalf("first response missing %q:\n%s", want, first.body)
		}
	}
	if strings.Contains(first.body, "\nsrc/a.go\nfunc calculate") || strings.Contains(first.body, "\nsrc/b.go\nfunc calculate") {
		t.Fatalf("file path hints should become fence headers:\n%s", first.body)
	}

	second := postOptimizeText(t, srv.URL, "raw-multi-file", "", body2)
	if got := second.header.Get("X-IQ-Blocks-Pruned"); got != "2" {
		t.Fatalf("second X-IQ-Blocks-Pruned=%q want 2; body=%s", got, second.body)
	}
	if !strings.Contains(second.body, "+++ b/src/a.go") || !strings.Contains(second.body, "+ \tprintln(\"file a changed\")") {
		t.Fatalf("missing src/a.go diff:\n%s", second.body)
	}
	if !strings.Contains(second.body, "+++ b/src/b.go") || !strings.Contains(second.body, "+ \tprintln(\"file b changed\")") {
		t.Fatalf("missing src/b.go diff:\n%s", second.body)
	}
	if !strings.Contains(second.body, "Find the bug across these files.") || !strings.Contains(second.body, "What should change?") {
		t.Fatalf("plain prompt text should remain outside code:\n%s", second.body)
	}
	if strings.Contains(second.body, "```go src/a.go") || strings.Contains(second.body, "```go src/b.go") {
		t.Fatalf("raw code fences should be replaced by diffs:\n%s", second.body)
	}
}

func TestOptimize_TextPlainMultiFilePromptSupportsMarkdownHeadings(t *testing.T) {
	t.Parallel()
	gov := govpkg.New(
		govpkg.WithHistory(govpkg.NewMemoryHistory()),
		govpkg.WithPruning(true, 8000),
	)
	srv := newTestServer(t, gov)

	body := strings.Join([]string{
		"Review these.",
		"",
		"### gateway/internal/proxy/handlers.go",
		testGoInner("handlers"),
		"",
		"### gateway/internal/proxy/browser_prompt.go",
		testGoInner("parser"),
	}, "\n")
	resp := postOptimizeText(t, srv.URL, "raw-multi-file-headings", "", body)
	if got := resp.header.Get("X-IQ-Blocks-Seen"); got != "2" {
		t.Fatalf("X-IQ-Blocks-Seen=%q want 2; body=%s", got, resp.body)
	}
	if !strings.Contains(resp.body, "```go gateway/internal/proxy/handlers.go") {
		t.Fatalf("expected handlers heading path as fence header:\n%s", resp.body)
	}
	if !strings.Contains(resp.body, "```go gateway/internal/proxy/browser_prompt.go") {
		t.Fatalf("expected parser heading path as fence header:\n%s", resp.body)
	}
}

func TestOptimize_TextPlainCodeThenQuestionKeepsQuestionOutsideCode(t *testing.T) {
	t.Parallel()
	gov := govpkg.New(
		govpkg.WithHistory(govpkg.NewMemoryHistory()),
		govpkg.WithPruning(true, 8000),
	)
	srv := newTestServer(t, gov)

	resp := postOptimizeText(t, srv.URL, "raw-code-then-question", "", "func calculate() {\n return 1 + 1\n }\n\nwhat is wrong here?")
	if resp.status != http.StatusOK {
		t.Fatalf("status=%d body=%s", resp.status, resp.body)
	}
	if got := resp.header.Get("X-IQ-Blocks-Seen"); got != "1" {
		t.Fatalf("X-IQ-Blocks-Seen=%q want 1; body=%s", got, resp.body)
	}
	if !strings.HasPrefix(resp.body, "```go "+defaultRawContextPath) {
		t.Fatalf("expected code to be fenced first:\n%s", resp.body)
	}
	if !strings.HasSuffix(strings.TrimSpace(resp.body), "what is wrong here?") {
		t.Fatalf("suffix question should stay outside fenced code:\n%s", resp.body)
	}
}

func TestOptimize_TextPlainNaturalLanguageStaysPlain(t *testing.T) {
	t.Parallel()
	gov := govpkg.New(
		govpkg.WithHistory(govpkg.NewMemoryHistory()),
		govpkg.WithPruning(true, 8000),
	)
	srv := newTestServer(t, gov)

	resp := postOptimizeText(t, srv.URL, "raw-natural-language", "", "Can you explain the architecture?")
	if resp.status != http.StatusOK {
		t.Fatalf("status=%d body=%s", resp.status, resp.body)
	}
	if resp.header.Get("X-IQ-Blocks-Seen") != "0" {
		t.Fatalf("expected no code blocks, headers=%v body=%s", resp.header, resp.body)
	}
	if strings.Contains(resp.body, "```") {
		t.Fatalf("natural language should not be auto-fenced:\n%s", resp.body)
	}
}

func TestOptimize_TextPlainLegacyDefaultContextDoesNotFenceNaturalLanguage(t *testing.T) {
	t.Parallel()
	gov := govpkg.New(
		govpkg.WithHistory(govpkg.NewMemoryHistory()),
		govpkg.WithPruning(true, 8000),
	)
	srv := newTestServer(t, gov)

	resp := postOptimizeTextWithHeaders(t, srv.URL, map[string]string{
		headerSessionKey:  "raw-legacy-defaults",
		headerContextPath: "browser-prompt.txt",
		headerContextLang: "txt",
	}, "what is the issue in the code i gave you?")
	if resp.status != http.StatusOK {
		t.Fatalf("status=%d body=%s", resp.status, resp.body)
	}
	if resp.header.Get("X-IQ-Blocks-Seen") != "0" {
		t.Fatalf("expected no code blocks, headers=%v body=%s", resp.header, resp.body)
	}
	if strings.Contains(resp.body, "```") || strings.Contains(resp.body, "browser-prompt.txt") {
		t.Fatalf("legacy default context should not fence natural language:\n%s", resp.body)
	}
}

func TestOptimize_TextPlainInjectsProjectMemory(t *testing.T) {
	t.Parallel()
	gov := govpkg.New(
		govpkg.WithHistory(govpkg.NewMemoryHistory()),
		govpkg.WithPruning(true, 8000),
		govpkg.WithProjectMemory("Always preserve repo-specific rules."),
	)
	srv := newTestServer(t, gov)

	resp := postOptimizeText(t, srv.URL, "", "Use short replies.", "hello")
	if resp.status != http.StatusOK {
		t.Fatalf("status=%d body=%s", resp.status, resp.body)
	}
	for _, want := range []string{
		"# IndexQube project memory",
		"Always preserve repo-specific rules.",
		"Use short replies.",
		"hello",
	} {
		if !strings.Contains(resp.body, want) {
			t.Fatalf("optimized text missing %q:\n%s", want, resp.body)
		}
	}
}

func TestOptimize_TextPlainEmptyPromptRejected(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t, &fakeGovernor{})

	resp := postOptimizeText(t, srv.URL, "session-a", "", "   \n\t")
	if resp.status != http.StatusBadRequest {
		t.Fatalf("status=%d want 400 body=%s", resp.status, resp.body)
	}
	if !strings.Contains(resp.body, "empty_prompt") {
		t.Fatalf("expected empty_prompt error, got %s", resp.body)
	}
}

func postOptimize(t *testing.T, baseURL, sessionKey, content string) optimizeResponseBody {
	t.Helper()
	body, err := json.Marshal(optimizeRequestBody{
		Messages:   []domain.Message{{Role: "user", Content: content}},
		SessionKey: sessionKey,
	})
	if err != nil {
		t.Fatalf("marshal optimize: %v", err)
	}
	resp, err := http.Post(baseURL+"/v1/optimize", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/optimize: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s", resp.StatusCode, payload)
	}
	var out optimizeResponseBody
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode optimize: %v", err)
	}
	return out
}

func postOptimizePromptContext(t *testing.T, baseURL string, fields map[string]string) optimizeResponseBody {
	t.Helper()
	body, err := json.Marshal(fields)
	if err != nil {
		t.Fatalf("marshal optimize fields: %v", err)
	}
	resp, err := http.Post(baseURL+"/v1/optimize", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/optimize: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s", resp.StatusCode, payload)
	}
	var out optimizeResponseBody
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode optimize: %v", err)
	}
	return out
}

type optimizeTextResponse struct {
	status int
	header http.Header
	body   string
}

func postOptimizeText(t *testing.T, baseURL, sessionKey, projectMemory, content string) optimizeTextResponse {
	t.Helper()
	headers := map[string]string{}
	if sessionKey != "" {
		headers[headerSessionKey] = sessionKey
	}
	if projectMemory != "" {
		headers[headerProjectMemory] = projectMemory
	}
	return postOptimizeTextWithHeaders(t, baseURL, headers, content)
}

func postOptimizeTextWithHeaders(t *testing.T, baseURL string, headers map[string]string, content string) optimizeTextResponse {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/optimize", strings.NewReader(content))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST text /v1/optimize: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return optimizeTextResponse{status: resp.StatusCode, header: resp.Header.Clone(), body: string(body)}
}

func testFencedGo(path, message string) string {
	return "```go " + path + "\n" + testGoInner(message) + "\n```"
}

func testGoInner(message string) string {
	lines := []string{
		"func calculate() {",
	}
	for i := 0; i < 80; i++ {
		lines = append(lines, "\tprintln(\"stable line\")")
	}
	lines = append(lines,
		"\tprintln(\""+message+"\")",
		"}",
	)
	return strings.Join(lines, "\n")
}

func TestParseInferenceRequest_BodyTooLarge(t *testing.T) {
	t.Parallel()

	// Use a small limit for the test
	const limit = 1024
	gov := &fakeGovernor{}
	p := New(gov, WithMaxRequestSize(limit))
	srv := httptest.NewServer(p.Handler())
	t.Cleanup(srv.Close)

	huge := bytes.Repeat([]byte("a"), limit+100)
	body, _ := json.Marshal(map[string]any{
		"model":    "claude-3-5-sonnet",
		"messages": []domain.Message{{Role: "user", Content: string(huge)}},
		"stream":   true,
	})

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions", bytes.NewReader(body))
	req.Header.Set(headerProvider, "anthropic")
	req.Header.Set(headerKey, "sk-ant-xyz")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("status=%d, want 413", resp.StatusCode)
	}
}

func TestNew_PanicsOnNilGovernor(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on nil governor")
		}
	}()
	_ = New(nil)
}
