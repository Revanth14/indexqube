package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/domain"
	govpkg "github.com/Revanth14/indexqube/gateway/internal/governor"
	"github.com/Revanth14/indexqube/gateway/internal/memory"
	"github.com/Revanth14/indexqube/gateway/internal/telemetry"
)

// fakeGovernor lets tests script the governor's streaming behavior.
type fakeGovernor struct {
	streamFunc   func(ctx context.Context, req *domain.InferenceRequest, tw domain.TokenWriter) error
	optimizeFunc func(ctx context.Context, tenant string, messages []domain.Message, projectMemory string) ([]domain.Message, domain.PruneStats, error)
	diagFunc     func(ctx context.Context) (domain.Diagnostics, error)
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

func (f *fakeGovernor) Diagnostics(ctx context.Context) (domain.Diagnostics, error) {
	if f.diagFunc != nil {
		return f.diagFunc(ctx)
	}
	return domain.Diagnostics{Status: "ok"}, nil
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

func newClaudeTestServer(t *testing.T, upstreamURL string, store *memory.Store, mode string, optimize bool) *httptest.Server {
	t.Helper()
	p := New(&fakeGovernor{}, WithClaudeMessages(ClaudeMessagesConfig{
		Mode:                 mode,
		DevToken:             "iq-dev-local",
		AnthropicAPIKey:      "sk-ant-test",
		AnthropicBaseURL:     upstreamURL,
		AnthropicVersion:     "2023-06-01",
		EnableBlockOptimizer: optimize,
		SessionStore:         store,
	}))
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

type reliabilityCaptureSink struct {
	events chan telemetry.ReliabilityEvent
}

func (s *reliabilityCaptureSink) Track(telemetry.UsageEvent) {}
func (s *reliabilityCaptureSink) TrackReliability(event telemetry.ReliabilityEvent) {
	s.events <- event
}

func TestReliabilityTelemetryAcceptsOnlyAggregateSchema(t *testing.T) {
	sink := &reliabilityCaptureSink{events: make(chan telemetry.ReliabilityEvent, 1)}
	p := New(&fakeGovernor{}, WithUsageTracker(sink))
	body := `{"machine_id":"anonymous","iq_version":"v1","os_arch":"darwin/arm64","generated_at":"2026-09-02T00:00:00Z","tasks_total":4}`
	req := httptest.NewRequest(http.MethodPost, "/v1/reliability", strings.NewReader(body))
	rec := httptest.NewRecorder()
	p.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	select {
	case event := <-sink.events:
		if event.TasksTotal != 4 || event.MachineID != "anonymous" {
			t.Fatalf("event=%+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("reliability event was not forwarded")
	}

	req = httptest.NewRequest(http.MethodPost, "/v1/reliability", strings.NewReader(`{"tasks_total":4,"prompt":"private"}`))
	rec = httptest.NewRecorder()
	p.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown private field status=%d body=%s", rec.Code, rec.Body.String())
	}
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

func TestExtractCredentialNativeOpenAIAuth(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	r.Header.Set("Authorization", "Bearer sk-native")
	cred, err := extractCredential(r)
	if err != nil {
		t.Fatalf("extractCredential: %v", err)
	}
	if cred.Provider != domain.ProviderOpenAI || cred.APIKey != "sk-native" {
		t.Fatalf("credential = %+v", cred)
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

func TestOpenAIResponsesNativeIngressPreservesUnknownFieldsAndStreams(t *testing.T) {
	var upstreamBody map[string]any
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/responses" {
			t.Errorf("path=%q, want /v1/responses", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer sk-native" {
			t.Errorf("Authorization=%q", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&upstreamBody); err != nil {
			t.Errorf("decode upstream body: %v", err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\n"))
	}))
	defer upstream.Close()
	t.Setenv("INDEXQUBE_OPENAI_BASE_URL", upstream.URL)

	gov := &fakeGovernor{
		optimizeFunc: func(ctx context.Context, tenant string, messages []domain.Message, projectMemory string) ([]domain.Message, domain.PruneStats, error) {
			if len(messages) != 1 || messages[0].Content != "hello" {
				t.Fatalf("messages = %+v", messages)
			}
			return []domain.Message{{Role: "user", Content: "hello optimized"}}, domain.PruneStats{BlocksSeen: 1}, nil
		},
	}
	p := New(gov, WithClaudeMessages(ClaudeMessagesConfig{HTTPClient: upstream.Client()}))
	srv := httptest.NewServer(p.Handler())
	defer srv.Close()

	body := `{
		"model":"gpt-5.5",
		"stream":true,
		"input":[{"role":"user","content":[{"type":"input_text","text":"hello"}]}],
		"metadata":{"keep":"yes"}
	}`
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/responses", strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer sk-native")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/responses: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d body=%s", resp.StatusCode, raw)
	}
	if !strings.Contains(string(raw), "response.output_text.delta") {
		t.Fatalf("missing streamed event body=%q", raw)
	}
	meta := upstreamBody["metadata"].(map[string]any)
	if meta["keep"] != "yes" {
		t.Fatalf("metadata not preserved: %+v", meta)
	}
	input := upstreamBody["input"].([]any)
	msg := input[0].(map[string]any)
	content := msg["content"].([]any)
	textBlock := content[0].(map[string]any)
	if textBlock["text"] != "hello optimized" {
		t.Fatalf("optimized text=%q", textBlock["text"])
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

func TestModels_FilterByProvider(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t, &fakeGovernor{})

	resp, err := http.Get(srv.URL + "/v1/models?provider=anthropic")
	if err != nil {
		t.Fatalf("GET /v1/models: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want 200", resp.StatusCode)
	}

	var got struct {
		Object string `json:"object"`
		Data   []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode models response: %v", err)
	}
	if got.Object != "list" {
		t.Fatalf("object=%q, want list", got.Object)
	}
	if len(got.Data) == 0 {
		t.Fatal("expected anthropic models")
	}
	for _, model := range got.Data {
		if !strings.HasPrefix(model.ID, "claude-") {
			t.Fatalf("unexpected anthropic model id %q", model.ID)
		}
	}
}

func TestDiagnostics_PrivacySafeHistorySummary(t *testing.T) {
	t.Parallel()
	gov, _ := govpkg.New(
		govpkg.WithHistory(govpkg.NewMemoryHistory()),
		govpkg.WithPruning(true, 8000),
	)
	srv := newTestServer(t, gov)

	const sessionKey = "diagnostics-session-secret"
	const path = "src/private.go"
	const code = "super proprietary implementation"
	_ = postOptimize(t, srv.URL, sessionKey, testFencedGo(path, code))

	resp, err := http.Get(srv.URL + "/v1/diagnostics")
	if err != nil {
		t.Fatalf("GET /v1/diagnostics: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read diagnostics: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d body=%s", resp.StatusCode, body)
	}
	var diag domain.Diagnostics
	if err := json.Unmarshal(body, &diag); err != nil {
		t.Fatalf("decode diagnostics: %v", err)
	}
	if diag.Status != "ok" {
		t.Fatalf("status=%q want ok", diag.Status)
	}
	if !diag.PruningEnabled {
		t.Fatal("pruning_enabled=false, want true")
	}
	if diag.ContractVersion != "v1" {
		t.Fatalf("contract_version=%q want v1", diag.ContractVersion)
	}
	if diag.History.Tenants != 1 || diag.History.Entries != 1 || diag.History.Bytes <= 0 {
		t.Fatalf("history=%+v want one bounded entry with bytes", diag.History)
	}
	bodyText := string(body)
	for _, forbidden := range []string{sessionKey, path, code, "private.go", "proprietary"} {
		if strings.Contains(bodyText, forbidden) {
			t.Fatalf("diagnostics leaked %q in body: %s", forbidden, bodyText)
		}
	}
}

func TestDiagnostics_GovernorError(t *testing.T) {
	t.Parallel()
	srv := newTestServer(t, &fakeGovernor{
		diagFunc: func(_ context.Context) (domain.Diagnostics, error) {
			return domain.Diagnostics{}, errors.New("diagnostics unavailable")
		},
	})

	resp, err := http.Get(srv.URL + "/v1/diagnostics")
	if err != nil {
		t.Fatalf("GET /v1/diagnostics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status=%d want 500", resp.StatusCode)
	}
	var env errorEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatalf("decode error envelope: %v", err)
	}
	if env.Error.Code != "diagnostics_failed" {
		t.Fatalf("code=%q want diagnostics_failed", env.Error.Code)
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

func TestClaudeMessages_AnthropicPassthroughStreaming(t *testing.T) {
	t.Parallel()
	var gotKey, gotVersion string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("x-api-key")
		gotVersion = r.Header.Get("anthropic-version")
		if r.URL.Path != "/v1/messages" {
			t.Errorf("path=%q, want /v1/messages", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: message_start\n")
		_, _ = io.WriteString(w, `data: {"type":"message_start","message":{"id":"msg_1","model":"claude-sonnet-4-6"}}`+"\n\n")
		_, _ = io.WriteString(w, "event: content_block_delta\n")
		_, _ = io.WriteString(w, `data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"hello"}}`+"\n\n")
		_, _ = io.WriteString(w, "event: message_stop\n")
		_, _ = io.WriteString(w, `data: {"type":"message_stop"}`+"\n\n")
	}))
	t.Cleanup(upstream.Close)
	srv := newClaudeTestServer(t, upstream.URL, memory.NewStore(time.Hour), "observe", false)

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/messages", strings.NewReader(`{"model":"claude-sonnet-4-6","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer iq-dev-local")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/messages: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d body=%s", resp.StatusCode, body)
	}
	if gotKey != "sk-ant-test" {
		t.Fatalf("x-api-key=%q, want test key", gotKey)
	}
	if gotVersion != "2023-06-01" {
		t.Fatalf("anthropic-version=%q, want 2023-06-01", gotVersion)
	}
	if !strings.Contains(string(body), "content_block_delta") || !strings.Contains(string(body), "hello") {
		t.Fatalf("body=%s, want anthropic SSE passthrough", body)
	}
}

func TestClaudeMessages_PassesThroughInboundAPIKey(t *testing.T) {
	t.Parallel()
	var gotKey, gotAuth string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("x-api-key")
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: message_stop\n")
		_, _ = io.WriteString(w, `data: {"type":"message_stop"}`+"\n\n")
	}))
	t.Cleanup(upstream.Close)

	p := New(&fakeGovernor{}, WithClaudeMessages(ClaudeMessagesConfig{
		Mode:             "observe",
		DevToken:         "iq-dev-local",
		AnthropicBaseURL: upstream.URL,
		AnthropicVersion: "2023-06-01",
		SessionStore:     memory.NewStore(time.Hour),
	}))
	srv := httptest.NewServer(p.Handler())
	t.Cleanup(srv.Close)

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/messages", strings.NewReader(`{"model":"claude-sonnet-4-6","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("x-api-key", "sk-ant-user")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/messages: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d body=%s", resp.StatusCode, body)
	}
	if gotKey != "sk-ant-user" {
		t.Fatalf("x-api-key=%q, want inbound key", gotKey)
	}
	if gotAuth != "" {
		t.Fatalf("Authorization=%q, want empty", gotAuth)
	}
}

func TestClaudeMessages_ResponseCacheReplaysRepeatedPromptWithPriorToolHistory(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: message_start\n")
		_, _ = io.WriteString(w, `data: {"type":"message_start","message":{"id":"msg_1","model":"claude-sonnet-4-6"}}`+"\n\n")
		_, _ = io.WriteString(w, "event: content_block_delta\n")
		_, _ = io.WriteString(w, `data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"project summary"}}`+"\n\n")
		_, _ = io.WriteString(w, "event: message_stop\n")
		_, _ = io.WriteString(w, `data: {"type":"message_stop"}`+"\n\n")
	}))
	t.Cleanup(upstream.Close)
	srv := newClaudeTestServer(t, upstream.URL, memory.NewStore(time.Hour), "optimize", true)

	body := `{"model":"claude-sonnet-4-6","stream":true,"messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"old file listing\nmain.go\nREADME.md"}]},{"role":"assistant","content":"I read the files."},{"role":"user","content":"what does this project do?"}]}`
	for i := 0; i < 2; i++ {
		req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/messages", strings.NewReader(body))
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer iq-dev-local")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST /v1/messages: %v", err)
		}
		got, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status=%d body=%s", resp.StatusCode, got)
		}
		if !strings.Contains(string(got), "project summary") {
			t.Fatalf("body=%s, want cached upstream response", got)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("upstream calls=%d, want 1", got)
	}
}

func TestClaudeMessages_ResponseCacheRecheckedAfterInflightWait(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		time.Sleep(100 * time.Millisecond)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: content_block_delta\n")
		_, _ = io.WriteString(w, `data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"cached concurrent answer"}}`+"\n\n")
		_, _ = io.WriteString(w, "event: message_stop\n")
		_, _ = io.WriteString(w, `data: {"type":"message_stop"}`+"\n\n")
	}))
	t.Cleanup(upstream.Close)
	srv := newClaudeTestServer(t, upstream.URL, memory.NewStore(time.Hour), "optimize", true)

	body := `{"model":"claude-sonnet-4-6","stream":true,"messages":[{"role":"user","content":"what does this project do?"}]}`
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/messages", strings.NewReader(body))
			if err != nil {
				errs <- err
				return
			}
			req.Header.Set("Authorization", "Bearer iq-dev-local")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				errs <- err
				return
			}
			got, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				errs <- fmt.Errorf("status=%d body=%s", resp.StatusCode, got)
				return
			}
			if !strings.Contains(string(got), "cached concurrent answer") {
				errs <- fmt.Errorf("body=%s, want cached concurrent answer", got)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("upstream calls=%d, want 1", got)
	}
}

func TestClaudeMessages_SentinelProbeDoesNotCallUpstream(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		t.Fatal("upstream should not be called for sentinel probe")
	}))
	t.Cleanup(upstream.Close)
	srv := newClaudeTestServer(t, upstream.URL, memory.NewStore(time.Hour), "observe", false)

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/messages", strings.NewReader(`{"model":"claude-sonnet-4-6","stream":true,"messages":[{"role":"user","content":"quota"}]}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer iq-dev-local")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/messages: %v", err)
	}
	got, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d body=%s", resp.StatusCode, got)
	}
	if !strings.Contains(string(got), "IndexQube active") {
		t.Fatalf("body=%s, want synthetic response", got)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("upstream calls=%d, want 0", got)
	}
}

func TestClaudeMessages_MissingAuth(t *testing.T) {
	t.Parallel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("upstream should not be called when no auth header is present")
	}))
	t.Cleanup(upstream.Close)
	srv := newClaudeTestServer(t, upstream.URL, memory.NewStore(time.Hour), "observe", false)

	// No Anthropic credential header — gateway must reject with 401.
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/messages", strings.NewReader(`{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/messages: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s, want 401", resp.StatusCode, body)
	}
}

func TestClaudeMessages_AgentSessionsRecordWithoutUsageTracker(t *testing.T) {
	t.Parallel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: message_stop\n")
		_, _ = io.WriteString(w, `data: {"type":"message_stop"}`+"\n\n")
	}))
	t.Cleanup(upstream.Close)

	p := New(&fakeGovernor{},
		WithAgentSessionStore(telemetry.NewAgentSessionStore(time.Hour)),
		WithClaudeMessages(ClaudeMessagesConfig{
			Mode:             "observe",
			DevToken:         "iq-dev-local",
			AnthropicAPIKey:  "sk-ant-test",
			AnthropicBaseURL: upstream.URL,
			AnthropicVersion: "2023-06-01",
			SessionStore:     memory.NewStore(time.Hour),
		}),
	)
	srv := httptest.NewServer(p.Handler())
	t.Cleanup(srv.Close)

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/messages", strings.NewReader(`{"model":"claude-sonnet-4-6","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer iq-dev-local")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/messages: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want 200", resp.StatusCode)
	}

	resp, err = http.Get(srv.URL + "/v1/agent-sessions")
	if err != nil {
		t.Fatalf("GET /v1/agent-sessions: %v", err)
	}
	defer resp.Body.Close()
	var body struct {
		TotalSessions int `json:"total_sessions"`
		Sessions      []struct {
			RequestsTotal int `json:"requests_total"`
		} `json:"sessions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.TotalSessions != 1 || len(body.Sessions) != 1 {
		t.Fatalf("sessions=%d len=%d, want one session", body.TotalSessions, len(body.Sessions))
	}
	if body.Sessions[0].RequestsTotal != 1 {
		t.Fatalf("requests_total=%d, want 1", body.Sessions[0].RequestsTotal)
	}
}

func TestClaudeCountTokens_Passthrough(t *testing.T) {
	t.Parallel()
	var gotPath, gotKey string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotKey = r.Header.Get("x-api-key")
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"input_tokens":123}`)
	}))
	t.Cleanup(upstream.Close)
	srv := newClaudeTestServer(t, upstream.URL, memory.NewStore(time.Hour), "observe", false)

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/messages/count_tokens", strings.NewReader(`{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer iq-dev-local")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/messages/count_tokens: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d body=%s, want 200", resp.StatusCode, body)
	}
	if gotPath != "/v1/messages/count_tokens" {
		t.Fatalf("upstream path=%q, want /v1/messages/count_tokens", gotPath)
	}
	if gotKey != "sk-ant-test" {
		t.Fatalf("x-api-key=%q, want test key", gotKey)
	}
	if !strings.Contains(string(body), `"input_tokens":123`) {
		t.Fatalf("body=%s, want input_tokens response", body)
	}
}

func TestClaudeCountTokens_PassesThroughInboundBearer(t *testing.T) {
	t.Parallel()
	var gotKey, gotAuth string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("x-api-key")
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"input_tokens":123}`)
	}))
	t.Cleanup(upstream.Close)

	p := New(&fakeGovernor{}, WithClaudeMessages(ClaudeMessagesConfig{
		Mode:             "observe",
		DevToken:         "iq-dev-local",
		AnthropicBaseURL: upstream.URL,
		AnthropicVersion: "2023-06-01",
		SessionStore:     memory.NewStore(time.Hour),
	}))
	srv := httptest.NewServer(p.Handler())
	t.Cleanup(srv.Close)

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/messages/count_tokens", strings.NewReader(`{"model":"claude-sonnet-4-6","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer sk-ant-oat-user")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/messages/count_tokens: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d body=%s, want 200", resp.StatusCode, body)
	}
	if gotKey != "" {
		t.Fatalf("x-api-key=%q, want empty", gotKey)
	}
	if gotAuth != "Bearer sk-ant-oat-user" {
		t.Fatalf("Authorization=%q, want inbound bearer", gotAuth)
	}
}

func TestClaudeMessages_OptimizePrunesRepeatedTextBlock(t *testing.T) {
	t.Parallel()
	var bodies []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(body))
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: message_stop\n")
		_, _ = io.WriteString(w, `data: {"type":"message_stop"}`+"\n\n")
	}))
	t.Cleanup(upstream.Close)
	srv := newClaudeTestServer(t, upstream.URL, memory.NewStore(time.Hour), "optimize", true)
	repeated := strings.Repeat("important project context line\n", 80)
	body := fmt.Sprintf(`{"model":"claude-sonnet-4-6","stream":true,"messages":[{"role":"user","content":%q},{"role":"user","content":"latest instruction stays"}]}`, repeated)

	for i := 0; i < 2; i++ {
		req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/messages", strings.NewReader(body))
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer iq-dev-local")
		req.Header.Set("Cache-Control", "no-cache")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST /v1/messages: %v", err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}

	if len(bodies) != 2 {
		t.Fatalf("got %d upstream bodies, want 2", len(bodies))
	}
	if strings.Contains(bodies[0], "omitted") {
		t.Fatalf("first request should warm session, got body=%s", bodies[0])
	}
	if !strings.Contains(bodies[1], "omitted") {
		t.Fatalf("second request should prune repeated block, got body=%s", bodies[1])
	}
	if !strings.Contains(bodies[1], "latest instruction stays") {
		t.Fatalf("latest instruction missing after optimize, got body=%s", bodies[1])
	}
}

func TestClaudeMessages_OptimizePrunesRepeatedLargeChunks(t *testing.T) {
	t.Parallel()
	var bodies []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(body))
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: message_stop\n")
		_, _ = io.WriteString(w, `data: {"type":"message_stop"}`+"\n\n")
	}))
	t.Cleanup(upstream.Close)
	srv := newClaudeTestServer(t, upstream.URL, memory.NewStore(time.Hour), "optimize", true)

	repeated := strings.Repeat("large repeated context line with enough bytes to chunk safely\n", 500)
	body := fmt.Sprintf(`{"model":"claude-sonnet-4-6","stream":true,"messages":[{"role":"user","content":%q},{"role":"user","content":"latest instruction stays"}]}`, repeated)

	for i := 0; i < 2; i++ {
		req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/messages", strings.NewReader(body))
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer iq-dev-local")
		req.Header.Set("Cache-Control", "no-cache")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST /v1/messages: %v", err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}

	if len(bodies) != 2 {
		t.Fatalf("got %d upstream bodies, want 2", len(bodies))
	}
	if strings.Contains(bodies[0], "omitted") {
		t.Fatalf("first request should warm session, got body=%s", bodies[0])
	}
	if !strings.Contains(bodies[1], "omitted") {
		t.Fatalf("second request should prune repeated large chunks, got body=%s", bodies[1])
	}
	if len(bodies[1]) >= len(bodies[0]) {
		t.Fatalf("second request did not shrink after repeated large chunk pruning: first=%d second=%d", len(bodies[0]), len(bodies[1]))
	}
	if !strings.Contains(bodies[1], "latest instruction stays") {
		t.Fatalf("latest instruction missing after optimize, got body=%s", bodies[1])
	}
}

// TestClaudeMessages_OptimizePreservesLatestTurnWhilePruningOldContent verifies
// Phase 3 policy: old repeated content is pruned, but the latest user turn is
// always preserved regardless of its content being previously seen.
func TestClaudeMessages_OptimizePreservesLatestTurnWhilePruningOldContent(t *testing.T) {
	t.Parallel()
	var bodies []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(body))
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: message_stop\n")
		_, _ = io.WriteString(w, `data: {"type":"message_stop"}`+"\n\n")
	}))
	t.Cleanup(upstream.Close)
	srv := newClaudeTestServer(t, upstream.URL, memory.NewStore(time.Hour), "optimize", true)

	// Large repeated context that will be saved on first request.
	repeated := strings.Repeat("repeated-context-line-that-should-be-pruned\n", 260)
	// Both requests use the same body: old repeated content in message[0],
	// new latest instruction in message[1] (the latest user turn).
	body := fmt.Sprintf(
		`{"model":"claude-sonnet-4-6","stream":true,"messages":[{"role":"user","content":%q},{"role":"user","content":"Please review the active file now."}]}`,
		repeated,
	)

	for i := 0; i < 2; i++ {
		req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/messages", strings.NewReader(body))
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer iq-dev-local")
		req.Header.Set("Cache-Control", "no-cache")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST /v1/messages: %v", err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}

	if len(bodies) != 2 {
		t.Fatalf("got %d upstream bodies, want 2", len(bodies))
	}
	if strings.Contains(bodies[0], "omitted") {
		t.Fatalf("first request should warm session, got body=%s", bodies[0])
	}
	// Old content (message[0]) must be pruned.
	if !strings.Contains(bodies[1], "omitted") {
		t.Fatalf("second request should prune old repeated content, got body=%s", bodies[1])
	}
	// Latest turn (message[1]) must be preserved verbatim.
	if !strings.Contains(bodies[1], "Please review the active file now.") {
		t.Fatalf("latest turn must be preserved, got body=%s", bodies[1])
	}
	// Body must have shrunk since the large repeated block was removed.
	if len(bodies[1]) >= len(bodies[0]) {
		t.Fatalf("second request did not shrink: first=%d second=%d", len(bodies[0]), len(bodies[1]))
	}
}

func TestClaudeMessages_OptimizeNeverPrunesSystemText(t *testing.T) {
	t.Parallel()
	p := New(&fakeGovernor{})
	cfg := ClaudeMessagesConfig{
		Mode:                 "optimize",
		EnableBlockOptimizer: true,
		SessionStore:         memory.NewStore(time.Hour),
		Optimizer: OptimizerConfig{
			MinSpanBytes:            512,
			EnableToolResultPruning: true,
			EnableSystemPruning:     true,
		},
	}
	systemText := strings.Repeat("system instruction must stay intact\n", 80)
	body := []byte(fmt.Sprintf(
		`{"model":"claude-sonnet-4-6","system":[{"type":"text","text":%q}],"messages":[{"role":"user","content":"old turn"},{"role":"user","content":"latest turn"}]}`,
		systemText,
	))

	if _, _, _, _, err := p.prepareClaudeBody(context.Background(), cfg, "system-test", body); err != nil {
		t.Fatalf("first prepare: %v", err)
	}
	forward, _, stats, _, err := p.prepareClaudeBody(context.Background(), cfg, "system-test", body)
	if err != nil {
		t.Fatalf("second prepare: %v", err)
	}

	if strings.Contains(string(forward), "omitted") {
		t.Fatalf("system text must not be replaced, body=%s", forward)
	}
	var got map[string]any
	if err := json.Unmarshal(forward, &got); err != nil {
		t.Fatalf("unmarshal forward body: %v", err)
	}
	gotSystem := got["system"].([]any)[0].(map[string]any)["text"].(string)
	if gotSystem != systemText {
		t.Fatalf("system text changed after optimize")
	}
	if stats.PreservedSystemCount != 1 {
		t.Fatalf("preserved_system_count=%d, want 1; stats=%+v", stats.PreservedSystemCount, stats)
	}
}

func TestClaudeMessages_OptimizePreservesProtectedInstructionToolResult(t *testing.T) {
	t.Parallel()
	p := New(&fakeGovernor{})
	cfg := ClaudeMessagesConfig{
		Mode:                 "optimize",
		EnableBlockOptimizer: true,
		SessionStore:         memory.NewStore(time.Hour),
		Optimizer: OptimizerConfig{
			MinSpanBytes:            512,
			EnableToolResultPruning: true,
		},
	}
	instructionBody := strings.Repeat("project instruction line that must remain visible\n", 80)
	body := []byte(fmt.Sprintf(
		`{"model":"claude-sonnet-4-6","messages":[{"role":"assistant","content":[{"type":"tool_use","id":"toolu_read_claude","name":"Read","input":{"file_path":"/repo/CLAUDE.md"}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_read_claude","content":%q}]},{"role":"user","content":"latest turn"}]}`,
		instructionBody,
	))

	if _, _, _, _, err := p.prepareClaudeBody(context.Background(), cfg, "protected-test", body); err != nil {
		t.Fatalf("first prepare: %v", err)
	}
	forward, _, stats, _, err := p.prepareClaudeBody(context.Background(), cfg, "protected-test", body)
	if err != nil {
		t.Fatalf("second prepare: %v", err)
	}

	if strings.Contains(string(forward), "omitted") {
		t.Fatalf("protected instruction file result must not be replaced, body=%s", forward)
	}
	var got map[string]any
	if err := json.Unmarshal(forward, &got); err != nil {
		t.Fatalf("unmarshal forward body: %v", err)
	}
	gotMessages := got["messages"].([]any)
	gotContent := gotMessages[1].(map[string]any)["content"].([]any)
	gotToolResult := gotContent[0].(map[string]any)["content"].(string)
	if gotToolResult != instructionBody {
		t.Fatalf("protected instruction body changed after optimize")
	}
	// Single-occurrence tool results are now protected by the last-occurrence
	// guard (not the instruction guard), so check the combined preserved count.
	if stats.PreservedLastOccurrenceCount+stats.PreservedInstructionCount < 1 {
		t.Fatalf("expected at least one preserved span, stats=%+v", stats)
	}
}

func TestClaudeMessages_ProtectedSystemBoilerplateBeatsCooldownPruning(t *testing.T) {
	t.Parallel()
	p := New(&fakeGovernor{})
	session := "protected-boilerplate-test"
	cfg := ClaudeMessagesConfig{
		Mode:                 "optimize",
		EnableBlockOptimizer: true,
		SessionStore:         memory.NewStore(time.Hour),
		Optimizer: OptimizerConfig{
			MinSpanBytes:            512,
			EnableToolResultPruning: true,
		},
	}
	bodyFor := func(secret string) []byte {
		systemText := "<system-reminder>\n" +
			strings.Repeat("stable harness reminder line\n", 30) +
			"Authorization: Bearer " + secret + "\n" +
			"CLAUDE.md must remain visible\n" +
			"</system-reminder>"
		return []byte(fmt.Sprintf(
			`{"model":"claude-sonnet-4-6","system":[{"type":"text","text":%q}],"messages":[{"role":"user","content":"latest turn"}]}`,
			systemText,
		))
	}

	p.resolveRequestID(session, "turn-1")
	if _, _, _, _, err := p.prepareClaudeBody(context.Background(), cfg, session, bodyFor("sk-proj-firstsecret")); err != nil {
		t.Fatalf("first prepare: %v", err)
	}
	p.resolveRequestID(session, "turn-2")
	if _, _, _, _, err := p.prepareClaudeBody(context.Background(), cfg, session, bodyFor("sk-proj-secondsecret")); err != nil {
		t.Fatalf("second prepare: %v", err)
	}
	p.resolveRequestID(session, "turn-3")
	forward, _, stats, _, err := p.prepareClaudeBody(context.Background(), cfg, session, bodyFor("sk-proj-thirdsecret"))
	if err != nil {
		t.Fatalf("third prepare: %v", err)
	}

	if strings.Contains(string(forward), "omitted") {
		t.Fatalf("protected system boilerplate must not be replaced during cooldown, body=%s", forward)
	}
	if !strings.Contains(string(forward), "Authorization: Bearer sk-proj-thirdsecret") {
		t.Fatalf("protected credential marker missing from forwarded body: %s", forward)
	}
	if stats.PreservedInstructionCount < 1 {
		t.Fatalf("expected protected boilerplate to be counted as preserved, stats=%+v", stats)
	}
}

func TestClaudeMessages_OptimizeStillPrunesOrdinaryToolResult(t *testing.T) {
	t.Parallel()
	p := New(&fakeGovernor{})
	cfg := ClaudeMessagesConfig{
		Mode:                 "optimize",
		EnableBlockOptimizer: true,
		SessionStore:         memory.NewStore(time.Hour),
		Optimizer: OptimizerConfig{
			MinSpanBytes:            512,
			EnableToolResultPruning: true,
		},
	}
	// Two tool results with identical content at different message positions.
	// The older one (messages[1]) should be pruned; the newer one (messages[3])
	// is the most recent occurrence and must be preserved so the model can see
	// the result without re-invoking the tool.
	fileBody := strings.Repeat("ordinary source code output line\n", 80)
	body := []byte(fmt.Sprintf(
		`{"model":"claude-sonnet-4-6","messages":[`+
			`{"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"Read","input":{"file_path":"/repo/src/main.go"}}]},`+
			`{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":%q}]},`+
			`{"role":"assistant","content":[{"type":"tool_use","id":"t2","name":"Read","input":{"file_path":"/repo/src/main.go"}}]},`+
			`{"role":"user","content":[{"type":"tool_result","tool_use_id":"t2","content":%q}]},`+
			`{"role":"user","content":"latest turn"}]}`,
		fileBody, fileBody,
	))

	// First request: warms the session cache; nothing pruned yet.
	if _, _, _, _, err := p.prepareClaudeBody(context.Background(), cfg, "ordinary-test", body); err != nil {
		t.Fatalf("first prepare: %v", err)
	}
	// Second request: older occurrence (messages[1]) is pruned; newer (messages[3]) preserved.
	forward, _, stats, _, err := p.prepareClaudeBody(context.Background(), cfg, "ordinary-test", body)
	if err != nil {
		t.Fatalf("second prepare: %v", err)
	}

	if !strings.Contains(string(forward), "omitted") {
		t.Fatalf("older duplicate tool result should be replaced, body=%s", forward)
	}
	if stats.BlocksPruned != 1 {
		t.Fatalf("blocks_pruned=%d, want 1; stats=%+v", stats.BlocksPruned, stats)
	}
	if stats.PreservedLastOccurrenceCount != 1 {
		t.Fatalf("preserved_last_occurrence_count=%d, want 1; stats=%+v", stats.PreservedLastOccurrenceCount, stats)
	}
}

func TestClaudeMessages_OptimizeDoesNotHTMLEscapePayload(t *testing.T) {
	t.Parallel()
	p := New(&fakeGovernor{})
	cfg := ClaudeMessagesConfig{
		Mode:                 "optimize",
		EnableBlockOptimizer: true,
		SessionStore:         memory.NewStore(time.Hour),
		Optimizer: OptimizerConfig{
			MinSpanBytes:            512,
			EnableToolResultPruning: true,
		},
	}
	// Two identical tool results: older (messages[1]) gets a readable omission placeholder,
	// newer (messages[3]) preserved. Verifies HTML chars survive without escaping.
	fileBody := strings.Repeat("<system-reminder>ordinary & repeated output</system-reminder>\n", 80)
	body := []byte(fmt.Sprintf(
		`{"model":"claude-sonnet-4-6","messages":[`+
			`{"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"Bash","input":{"command":"printf output"}}]},`+
			`{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":%q}]},`+
			`{"role":"assistant","content":[{"type":"tool_use","id":"t2","name":"Bash","input":{"command":"printf output"}}]},`+
			`{"role":"user","content":[{"type":"tool_result","tool_use_id":"t2","content":%q}]},`+
			`{"role":"user","content":"latest turn"}]}`,
		fileBody, fileBody,
	))

	if _, _, _, _, err := p.prepareClaudeBody(context.Background(), cfg, "html-test", body); err != nil {
		t.Fatalf("first prepare: %v", err)
	}
	forward, _, stats, _, err := p.prepareClaudeBody(context.Background(), cfg, "html-test", body)
	if err != nil {
		t.Fatalf("second prepare: %v", err)
	}

	if !strings.Contains(string(forward), "omitted") {
		t.Fatalf("older duplicate tool result should be replaced, body=%s", forward)
	}
	if bytes.Contains(forward, []byte(`\u003c`)) || bytes.Contains(forward, []byte(`\u003e`)) || bytes.Contains(forward, []byte(`\u0026`)) {
		t.Fatalf("optimized payload must not HTML-escape content: %s", forward)
	}
	if stats.EstimatedTokensSaved <= 0 {
		t.Fatalf("expected positive token savings, stats=%+v", stats)
	}
}

func TestProtectedInstructionSpanDetection(t *testing.T) {
	t.Parallel()
	if !isProtectedInstructionSpan(TextSpan{SourcePath: `/repo/.cursor/rules/backend.mdc`}) {
		t.Fatal("expected .cursor/rules path to be protected")
	}
	if !isProtectedInstructionSpan(TextSpan{Text: `Read /repo/CONTEXT.md before editing.`}) {
		t.Fatal("expected CONTEXT.md mention to be protected")
	}
	if isProtectedInstructionSpan(TextSpan{SourcePath: `/repo/src/main.go`, Text: `package main`}) {
		t.Fatal("ordinary source file should not be protected")
	}
	if !isProtectedInstructionSpan(TextSpan{Text: `Authorization: Bearer my-api-key`}) {
		t.Fatal("expected Authorization bearer text to be protected")
	}
}

func TestDumpClaudePayloadsAppendsSessionFile(t *testing.T) {
	dumpDir := t.TempDir()
	sessionFile := filepath.Join(dumpDir, "iq-session-test.jsonl")
	t.Setenv("IQ_DUMP_SESSION_FILE", sessionFile)

	dumpClaudePayloads("dump-test", []byte(`{"before":true}`), []byte(`{"after":true}`), claudeStreamStats{OutputRawText: "simulated response", OutputTokens: 10, Status: "completed"}, claudeOptimizerStats{})
	dumpClaudePayloads("dump-test-2", []byte(`{"before":2}`), []byte(`{"after":2}`), claudeStreamStats{}, claudeOptimizerStats{})

	raw, err := os.ReadFile(sessionFile)
	if err != nil {
		t.Fatalf("read session dump: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 2 {
		t.Fatalf("dump lines=%d, want 2; dump=%s", len(lines), raw)
	}
	var first payloadDumpRecord
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatalf("unmarshal first dump line: %v", err)
	}
	if first.RequestID != "dump-test" || first.BeforeBytes != 15 || first.AfterBytes != 14 || first.SavedBytes != 1 {
		t.Fatalf("unexpected first dump record: %+v", first)
	}
	if !strings.Contains(string(first.Before), `"before":true`) {
		t.Fatalf("before payload missing from first record: %s", first.Before)
	}
	if first.Response.Text != "simulated response" || first.Response.OutputTokens != 10 {
		t.Fatalf("unexpected response metrics inside first record: %+v", first.Response)
	}
	if _, err := os.Stat(filepath.Join(dumpDir, "iq-before-dump-test.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("session dumps should not create per-request before file, err=%v", err)
	}
}

func TestDumpClaudePayloadsFallsBackToPairFiles(t *testing.T) {
	dumpDir := t.TempDir()
	t.Setenv("IQ_DUMP_SESSION_FILE", "")
	t.Setenv("IQ_DUMP_DIR", dumpDir)

	dumpClaudePayloads("dump-test", []byte(`{"before":true}`), []byte(`{"after":true}`), claudeStreamStats{}, claudeOptimizerStats{})

	before, err := os.ReadFile(filepath.Join(dumpDir, "iq-before-dump-test.json"))
	if err != nil {
		t.Fatalf("read before dump: %v", err)
	}
	after, err := os.ReadFile(filepath.Join(dumpDir, "iq-after-dump-test.json"))
	if err != nil {
		t.Fatalf("read after dump: %v", err)
	}
	if !strings.Contains(string(before), `"before": true`) {
		t.Fatalf("before dump was not pretty printed: %s", before)
	}
	if !strings.Contains(string(after), `"after": true`) {
		t.Fatalf("after dump was not pretty printed: %s", after)
	}
}

func TestDumpClaudePayloadsEmitsOptimizerStats(t *testing.T) {
	sessionFile := filepath.Join(t.TempDir(), "session.jsonl")
	t.Setenv("IQ_DUMP_SESSION_FILE", sessionFile)

	// Realistic post-FIX-B stats: a turn where some bytes were pruned, some
	// were known-but-protected (CLAUDE.md), and the rest were known-but-
	// latest-turn. KnownBytes is the sum across all preservation paths plus
	// the pruned bytes.
	opt := claudeOptimizerStats{
		BlocksPruned:                 2,
		BlocksKnown:                  5,
		BytesPruned:                  1000,
		PreservedInstructionBytes:    3027,
		PreservedInstructionCount:    1,
		PreservedLatestTurnBytes:     500,
		PreservedLatestTurnCount:     1,
		PreservedLastOccurrenceBytes: 0,
		PreservedLastOccurrenceCount: 0,
	}
	opt.KnownBytes = opt.BytesPruned + opt.PreservedInstructionBytes +
		opt.PreservedLatestTurnBytes + opt.PreservedLastOccurrenceBytes

	dumpClaudePayloads("opt-test", []byte(`{"before":1}`), []byte(`{"after":1}`), claudeStreamStats{}, opt)

	raw, err := os.ReadFile(sessionFile)
	if err != nil {
		t.Fatalf("read session file: %v", err)
	}
	line := strings.TrimRight(string(raw), "\n")
	var rec struct {
		SavedBytes int `json:"saved_bytes"`
		Optimizer  *struct {
			BlocksPruned         int `json:"blocks_pruned"`
			BlocksKnown          int `json:"blocks_known"`
			BlocksKnownProtected int `json:"blocks_known_protected"`
			BytesPruned          int `json:"bytes_pruned"`
			ProtectedBytes       int `json:"protected_bytes"`
			KnownBytes           int `json:"known_bytes"`
			TrueCacheHitBytes    int `json:"true_cache_hit_bytes"`
		} `json:"optimizer"`
	}
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		t.Fatalf("parse dump line: %v\nline=%s", err, line)
	}
	if rec.Optimizer == nil {
		t.Fatalf("expected optimizer block in dump record; got: %s", line)
	}
	if got, want := rec.Optimizer.ProtectedBytes, opt.PreservedInstructionBytes; got != want {
		t.Errorf("protected_bytes: got %d, want %d", got, want)
	}
	if got, want := rec.Optimizer.BlocksKnownProtected, opt.PreservedInstructionCount; got != want {
		t.Errorf("blocks_known_protected: got %d, want %d", got, want)
	}
	if got, want := rec.Optimizer.BytesPruned, opt.BytesPruned; got != want {
		t.Errorf("bytes_pruned: got %d, want %d", got, want)
	}
	if got, want := rec.Optimizer.KnownBytes, opt.KnownBytes; got != want {
		t.Errorf("known_bytes: got %d, want %d", got, want)
	}
	if got, want := rec.Optimizer.TrueCacheHitBytes, opt.KnownBytes; got != want {
		t.Errorf("true_cache_hit_bytes: got %d, want %d (must equal KnownBytes)", got, want)
	}
	// Invariant: true_cache_hit_bytes == bytes_pruned + protected_bytes + (other preservations).
	// Here PreservedLastOccurrenceBytes==0 and PreservedLatestTurnBytes==500, so:
	wantInvariant := rec.Optimizer.BytesPruned + rec.Optimizer.ProtectedBytes + opt.PreservedLatestTurnBytes
	if rec.Optimizer.TrueCacheHitBytes != wantInvariant {
		t.Errorf("invariant broken: true_cache_hit_bytes=%d, expected %d",
			rec.Optimizer.TrueCacheHitBytes, wantInvariant)
	}
}

func TestProxyAnthropicStream_UsesMessageDeltaOutputTokens(t *testing.T) {
	t.Parallel()
	body := strings.Join([]string{
		`event: content_block_delta`,
		`data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"fallback text"}}`,
		``,
		`event: message_delta`,
		`data: {"type":"message_delta","usage":{"output_tokens":42}}`,
		``,
	}, "\n")
	resp := &http.Response{Body: io.NopCloser(strings.NewReader(body))}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)

	stats := proxyAnthropicStream(rec, req, resp)
	if stats.OutputTokens != 42 {
		t.Fatalf("OutputTokens=%d, want 42", stats.OutputTokens)
	}
	if got := stats.estimatedOutputTokens(); got != 42 {
		t.Fatalf("estimatedOutputTokens=%d, want exact usage token count", got)
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

func TestClassifyUpstreamError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		err      error
		wantCode string
	}{
		{"nil", nil, "provider_error"},
		{"cancelled", fmt.Errorf("operation: %w", context.Canceled), "request_cancelled"},
		{"401 status", fmt.Errorf("openai api error: status=401 body=unauthorized"), "provider_key_invalid"},
		{"403 status", fmt.Errorf("openai api error: status=403 body=forbidden"), "provider_key_invalid"},
		{"invalid key text", fmt.Errorf("anthropic api error: status=400 body=invalid api key"), "provider_key_invalid"},
		{"429 rate limit", fmt.Errorf("openai api error: status=429 body=rate limit exceeded"), "provider_rate_limited"},
		{"402 quota", fmt.Errorf("openai api error: status=402 body=insufficient_quota"), "provider_balance_exhausted"},
		{"quota text", fmt.Errorf("provider error: quota exceeded"), "provider_balance_exhausted"},
		{"timeout", fmt.Errorf("openai api error: status=408 body=request timeout"), "provider_timeout"},
		{"deadline exceeded", fmt.Errorf("context deadline exceeded"), "provider_timeout"},
		{"504 gateway timeout", fmt.Errorf("openai api error: status=504 body=gateway timeout"), "provider_timeout"},
		{"503 unavailable", fmt.Errorf("openai api error: status=503 body=service unavailable"), "provider_unavailable"},
		{"overloaded text", fmt.Errorf("anthropic api error: status=529 body=overloaded"), "provider_unavailable"},
		{"context_length 400", fmt.Errorf("openai api error: status=400 body=context_length_exceeded"), "gateway_context_too_large"},
		{"context length text 400", fmt.Errorf("openai api error: status=400 body=maximum context length is 4096"), "gateway_context_too_large"},
		{"request too large 400", fmt.Errorf("anthropic api error: status=400 body=request too large"), "gateway_context_too_large"},
		{"token limit 400", fmt.Errorf("openai api error: status=400 body=token limit exceeded"), "gateway_context_too_large"},
		{"400 not context", fmt.Errorf("openai api error: status=400 body=invalid model"), "provider_error"},
		{"generic error", fmt.Errorf("some unknown failure"), "provider_error"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			code, _ := classifyUpstreamError(tc.err)
			if code != tc.wantCode {
				t.Errorf("classifyUpstreamError(%v) code=%q, want %q", tc.err, code, tc.wantCode)
			}
		})
	}
}

func TestChatCompletions_SanitizesGovernorErrorEvent(t *testing.T) {
	t.Parallel()

	secret := "sk-proj-secret1234567890"
	gov := &fakeGovernor{
		streamFunc: func(_ context.Context, _ *domain.InferenceRequest, _ domain.TokenWriter) error {
			return errors.New(`openai api error: status=401 body={"error":{"message":"bad key ` + secret + `"}}`)
		},
	}
	srv := newTestServer(t, gov)

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions", bytes.NewReader(validBody(t)))
	req.Header.Set(headerProvider, "openai")
	req.Header.Set(headerKey, secret)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	got := string(body)
	if strings.Contains(got, secret) || strings.Contains(got, "body=") || strings.Contains(got, "bad key") {
		t.Fatalf("sse error leaked provider detail: %s", got)
	}
	if !strings.Contains(got, `"code":"provider_key_invalid"`) {
		t.Fatalf("missing classified provider error: %s", got)
	}
}

// cancellingWriter is an http.ResponseWriter whose Write always fails once
// cancelled. Used to simulate a client disconnecting mid-stream (Path B:
// broken pipe / connection reset, where the error is a network error rather
// than context.Canceled).
type cancellingWriter struct {
	httptest.ResponseRecorder
	ctx    context.Context
	cancel context.CancelFunc
}

func (cw *cancellingWriter) Write(b []byte) (int, error) {
	if err := cw.ctx.Err(); err != nil {
		return 0, fmt.Errorf("write tcp: broken pipe")
	}
	return cw.ResponseRecorder.Write(b)
}

func (cw *cancellingWriter) Unwrap() http.ResponseWriter {
	return &cw.ResponseRecorder
}

func TestChatCompletions_ClientDisconnectViaCancelledContext(t *testing.T) {
	t.Parallel()

	gov := &fakeGovernor{
		streamFunc: func(_ context.Context, _ *domain.InferenceRequest, tw domain.TokenWriter) error {
			_ = tw.WriteData([]byte(`{"delta":"partial"}`))
			return context.Canceled
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
	body, _ := io.ReadAll(resp.Body)
	got := string(body)

	if strings.Contains(got, "event: error") {
		t.Errorf("gateway must not emit SSE error frame on client disconnect; body=%q", got)
	}
	if strings.Contains(got, "[DONE]") {
		t.Errorf("gateway must not emit [DONE] sentinel on client disconnect; body=%q", got)
	}
	if !strings.Contains(got, `"delta":"partial"`) {
		t.Errorf("partial frame before disconnect must be present; body=%q", got)
	}
}

func TestChatCompletions_ClientDisconnectViaWriteError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())

	gov := &fakeGovernor{
		streamFunc: func(_ context.Context, _ *domain.InferenceRequest, tw domain.TokenWriter) error {
			cancel() // signal the cancellingWriter to start failing
			// This write will fail because the writer's context is now cancelled.
			return tw.WriteData([]byte(`{"delta":"after-disconnect"}`))
		},
	}

	p := New(gov)
	rec := &cancellingWriter{
		ResponseRecorder: *httptest.NewRecorder(),
		ctx:              ctx,
		cancel:           cancel,
	}

	body, _ := json.Marshal(domain.InferenceRequest{
		Model:    "claude-3-5-sonnet",
		Messages: []domain.Message{{Role: "user", Content: "hello"}},
		Stream:   true,
	})
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	r.Header.Set(headerProvider, "anthropic")
	r.Header.Set(headerKey, "sk-ant-xyz")
	r.Header.Set("Content-Type", "application/json")

	// Use a cancelled context to simulate the HTTP server detecting the disconnect.
	cancelledCtx, cancelReq := context.WithCancel(context.Background())
	cancelReq()
	r = r.WithContext(cancelledCtx)

	p.handleChatCompletions(rec, r)

	got := rec.Body.String()
	if strings.Contains(got, "event: error") {
		t.Errorf("gateway must not emit SSE error frame on write-error disconnect; body=%q", got)
	}
}

func TestOptimize_WithoutSessionKeyIsStateless(t *testing.T) {
	t.Parallel()
	gov, _ := govpkg.New(
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
	gov, _ := govpkg.New(
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
	gov, _ := govpkg.New(
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
	gov, _ := govpkg.New(
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
	gov, _ := govpkg.New(
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
		"X-IQ-Contract-Version",
		"X-IQ-Mode",
		"X-IQ-Blocks-Seen",
		"X-IQ-Blocks-Pruned",
		"X-IQ-Bytes-Before",
		"X-IQ-Bytes-After",
		"X-IQ-Bytes-Saved",
		"X-IQ-Tokens-Before",
		"X-IQ-Tokens-After",
		"X-IQ-Tokens-Saved",
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
	var out optimizeResponseBody
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode optimize response: %v", err)
	}
	if out.Version != "v1" || out.Mode != "diff" {
		t.Fatalf("version/mode=%q/%q want v1/diff", out.Version, out.Mode)
	}
	if out.BytesSaved <= 0 || out.EstimatedTokensSaved <= 0 {
		t.Fatalf("expected direct savings fields, got bytes=%d tokens=%d stats=%+v", out.BytesSaved, out.EstimatedTokensSaved, out.Stats)
	}
}

func TestOptimize_JSONContractModesAndSavings(t *testing.T) {
	t.Parallel()
	gov, _ := govpkg.New(
		govpkg.WithHistory(govpkg.NewMemoryHistory()),
		govpkg.WithPruning(true, 8000),
	)
	srv := newTestServer(t, gov)

	stateless := postOptimize(t, srv.URL, "", testFencedGo("src/contract.go", "hello"))
	if stateless.Version != "v1" || stateless.Mode != "stateless" {
		t.Fatalf("stateless version/mode=%q/%q want v1/stateless", stateless.Version, stateless.Mode)
	}
	if stateless.BytesSaved != 0 || stateless.EstimatedTokensSaved != 0 {
		t.Fatalf("stateless savings=%d/%d want zero", stateless.BytesSaved, stateless.EstimatedTokensSaved)
	}

	warmup := postOptimize(t, srv.URL, "contract-session", testFencedGo("src/contract.go", "hello"))
	if warmup.Mode != "warmup" {
		t.Fatalf("warmup mode=%q want warmup; stats=%+v", warmup.Mode, warmup.Stats)
	}

	unchanged := postOptimize(t, srv.URL, "contract-session", testFencedGo("src/contract.go", "hello"))
	if unchanged.Mode != "unchanged" {
		t.Fatalf("unchanged mode=%q want unchanged; stats=%+v body=%s", unchanged.Mode, unchanged.Stats, unchanged.Text)
	}
	if unchanged.BytesSaved <= 0 || unchanged.EstimatedTokensSaved <= 0 {
		t.Fatalf("unchanged direct savings should be positive, got bytes=%d tokens=%d", unchanged.BytesSaved, unchanged.EstimatedTokensSaved)
	}

	diff := postOptimize(t, srv.URL, "contract-session", testFencedGo("src/contract.go", "hello indexqube"))
	if diff.Mode != "diff" {
		t.Fatalf("diff mode=%q want diff; stats=%+v body=%s", diff.Mode, diff.Stats, diff.Text)
	}
	if diff.Stats.DiffExact != 1 {
		t.Fatalf("diff_exact=%d want 1; stats=%+v", diff.Stats.DiffExact, diff.Stats)
	}
	if diff.BytesSaved <= 0 || diff.EstimatedTokensSaved <= 0 {
		t.Fatalf("diff direct savings should be positive, got bytes=%d tokens=%d", diff.BytesSaved, diff.EstimatedTokensSaved)
	}
}

func TestOptimize_JSONContractSkippedMode(t *testing.T) {
	t.Parallel()
	gov, _ := govpkg.New(
		govpkg.WithHistory(govpkg.NewMemoryHistory()),
		govpkg.WithPruning(true, 8000),
	)
	srv := newTestServer(t, gov)

	body := "```go src/tiny.go\nhello\nworld\n```"
	_ = postOptimize(t, srv.URL, "contract-skipped", body)
	second := postOptimize(t, srv.URL, "contract-skipped", body)
	if second.Mode != "skipped" {
		t.Fatalf("mode=%q want skipped; stats=%+v", second.Mode, second.Stats)
	}
	if second.Stats.SkipReasons["not_smaller"] != 1 {
		t.Fatalf("skip reasons=%v want not_smaller=1", second.Stats.SkipReasons)
	}
}

func TestOptimize_JSONContextTextUsesDefaultSyntheticPath(t *testing.T) {
	t.Parallel()
	gov, _ := govpkg.New(
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
	gov, _ := govpkg.New(
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

func TestOptimize_TextPlainAcceptJSONReturnsContract(t *testing.T) {
	t.Parallel()
	gov, _ := govpkg.New(
		govpkg.WithHistory(govpkg.NewMemoryHistory()),
		govpkg.WithPruning(true, 8000),
	)
	srv := newTestServer(t, gov)

	_ = postOptimizeText(t, srv.URL, "text-json-contract", "", testFencedGo("src/x.go", "hello"))
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/optimize", strings.NewReader(testFencedGo("src/x.go", "hello indexqube")))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	req.Header.Set("Accept", "application/json")
	req.Header.Set(headerSessionKey, "text-json-contract")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST text /v1/optimize: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s", resp.StatusCode, payload)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("Content-Type=%q want application/json", ct)
	}
	var out optimizeResponseBody
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Version != "v1" || out.Mode != "diff" {
		t.Fatalf("version/mode=%q/%q want v1/diff", out.Version, out.Mode)
	}
	if out.Text == "" || !strings.Contains(out.Text, "```diff") {
		t.Fatalf("expected text field with optimized diff, got:\n%s", out.Text)
	}
	if out.BytesSaved <= 0 || out.EstimatedTokensSaved <= 0 {
		t.Fatalf("expected positive direct savings, got bytes=%d tokens=%d", out.BytesSaved, out.EstimatedTokensSaved)
	}
	if got := resp.Header.Get("X-IQ-Mode"); got != "diff" {
		t.Fatalf("X-IQ-Mode=%q want diff", got)
	}
}

func TestOptimize_TextPlainAutoWrapsRawCode(t *testing.T) {
	t.Parallel()
	gov, _ := govpkg.New(
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
	gov, _ := govpkg.New(
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
	gov, _ := govpkg.New(
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
	gov, _ := govpkg.New(
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
	gov, _ := govpkg.New(
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
	gov, _ := govpkg.New(
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
	gov, _ := govpkg.New(
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
	gov, _ := govpkg.New(
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

func TestOptimize_TextPlainNotSmallerExposesSkipReason(t *testing.T) {
	t.Parallel()
	gov, _ := govpkg.New(
		govpkg.WithHistory(govpkg.NewMemoryHistory()),
		govpkg.WithPruning(true, 8000),
	)
	srv := newTestServer(t, gov)

	body := "```go src/tiny.go\nhello\nworld\n```"
	_ = postOptimizeText(t, srv.URL, "raw-not-smaller", "", body)
	resp := postOptimizeText(t, srv.URL, "raw-not-smaller", "", body)
	if got := resp.header.Get("X-IQ-Blocks-Skipped"); got != "1" {
		t.Fatalf("X-IQ-Blocks-Skipped=%q want 1; body=%s", got, resp.body)
	}
	if got := resp.header.Get("X-IQ-Skip-Reasons"); got != "not_smaller=1" {
		t.Fatalf("X-IQ-Skip-Reasons=%q want not_smaller=1; headers=%v", got, resp.header)
	}
	if resp.body != body {
		t.Fatalf("not-smaller skip should leave body verbatim:\n%s", resp.body)
	}
}

func TestOptimize_TextPlainInjectsProjectMemory(t *testing.T) {
	t.Parallel()
	gov, _ := govpkg.New(
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
