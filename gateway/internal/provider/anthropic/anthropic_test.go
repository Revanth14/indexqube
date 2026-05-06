package anthropic

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/Revanth14/indexqube/gateway/internal/domain"
)

// recordingWriter captures frames emitted by the adapter.
type recordingWriter struct {
	mu     sync.Mutex
	frames [][]byte
}

func (r *recordingWriter) WriteData(data []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	r.frames = append(r.frames, cp)
	return nil
}
func (r *recordingWriter) WriteEvent(_ string, _ []byte) error { return nil }
func (r *recordingWriter) WriteDone() error                    { return nil }
func (r *recordingWriter) Flush() error                        { return nil }

func (r *recordingWriter) Frames() [][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([][]byte, len(r.frames))
	copy(out, r.frames)
	return out
}

// fakeAnthropic returns a httptest.Server emitting the given SSE body.
// status is applied to the response.
func fakeAnthropic(t *testing.T, status int, sseBody string, captureReq *http.Request) *httptest.Server {
	t.Helper()
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if captureReq != nil {
			*captureReq = *r.Clone(r.Context())
			body, _ := io.ReadAll(r.Body)
			captureReq.Body = io.NopCloser(strings.NewReader(string(body)))
		}
		if status != http.StatusOK {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(sseBody))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		// Flush per write so the adapter sees frames progressively.
		rc := http.NewResponseController(w)
		for _, line := range strings.Split(sseBody, "\n") {
			_, _ = w.Write([]byte(line + "\n"))
			_ = rc.Flush()
		}
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

const happyPathSSE = `event: message_start
data: {"type":"message_start","message":{"id":"msg_x","model":"claude-3-5-sonnet"}}

event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}

event: ping
data: {"type":"ping"}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":" world"}}

event: content_block_stop
data: {"type":"content_block_stop","index":0}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"end_turn"}}

event: message_stop
data: {"type":"message_stop"}
`

func newReq() *domain.InferenceRequest {
	return &domain.InferenceRequest{
		Model:    "claude-3-5-sonnet",
		Messages: []domain.Message{{Role: "user", Content: "say hello"}},
		Stream:   true,
		Credential: domain.Credential{
			Provider: domain.ProviderAnthropic,
			APIKey:   "sk-ant-test-key",
		},
	}
}

func TestDispatch_HappyPath(t *testing.T) {
	t.Parallel()
	var captured http.Request
	srv := fakeAnthropic(t, http.StatusOK, happyPathSSE, &captured)

	a := New(WithBaseURL(srv.URL))
	rec := &recordingWriter{}

	if err := a.Dispatch(context.Background(), newReq(), rec); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	frames := rec.Frames()
	if len(frames) < 4 {
		t.Fatalf("got %d frames, want >=4 (role, hello, world, finish)", len(frames))
	}

	// Frame 0: role assistant
	var f0 openAIChunk
	if err := json.Unmarshal(frames[0], &f0); err != nil {
		t.Fatalf("decode frame 0: %v", err)
	}
	if f0.Choices[0].Delta.Role != "assistant" {
		t.Errorf("frame 0 role=%q, want assistant", f0.Choices[0].Delta.Role)
	}
	if f0.Object != "chat.completion.chunk" {
		t.Errorf("object=%q, want chat.completion.chunk", f0.Object)
	}

	// Frame 1: "Hello"
	var f1 openAIChunk
	if err := json.Unmarshal(frames[1], &f1); err != nil {
		t.Fatalf("decode frame 1: %v", err)
	}
	if f1.Choices[0].Delta.Content != "Hello" {
		t.Errorf("frame 1 content=%q, want Hello", f1.Choices[0].Delta.Content)
	}

	// Frame 2: " world"
	var f2 openAIChunk
	if err := json.Unmarshal(frames[2], &f2); err != nil {
		t.Fatalf("decode frame 2: %v", err)
	}
	if f2.Choices[0].Delta.Content != " world" {
		t.Errorf("frame 2 content=%q, want ' world'", f2.Choices[0].Delta.Content)
	}

	// Last frame: finish_reason "stop"
	last := frames[len(frames)-1]
	var fLast openAIChunk
	if err := json.Unmarshal(last, &fLast); err != nil {
		t.Fatalf("decode last frame: %v", err)
	}
	if fLast.Choices[0].FinishReason == nil || *fLast.Choices[0].FinishReason != "stop" {
		t.Errorf("last finish_reason=%v, want 'stop'", fLast.Choices[0].FinishReason)
	}
	if fLast.Choices[0].Delta.Content != "" {
		t.Errorf("last frame content=%q, want empty", fLast.Choices[0].Delta.Content)
	}

	// Verify upstream call shape.
	if captured.Header.Get("x-api-key") != "sk-ant-test-key" {
		t.Errorf("upstream x-api-key=%q, want sk-ant-test-key", captured.Header.Get("x-api-key"))
	}
	if captured.Header.Get("anthropic-version") != apiVersion {
		t.Errorf("upstream anthropic-version=%q, want %q", captured.Header.Get("anthropic-version"), apiVersion)
	}
}

func TestDispatch_Returns4xxAsError(t *testing.T) {
	t.Parallel()
	srv := fakeAnthropic(t, http.StatusUnauthorized, `{"error":{"message":"invalid x-api-key"}}`, nil)

	a := New(WithBaseURL(srv.URL))
	err := a.Dispatch(context.Background(), newReq(), &recordingWriter{})
	if err == nil {
		t.Fatal("expected error on 401")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Errorf("err=%q, want to contain 401", err)
	}
}

func TestDispatch_MidStreamErrorEvent(t *testing.T) {
	t.Parallel()
	const errSSE = `event: message_start
data: {"type":"message_start"}

event: error
data: {"type":"error","error":{"type":"overloaded_error","message":"server overloaded"}}
`
	srv := fakeAnthropic(t, http.StatusOK, errSSE, nil)

	a := New(WithBaseURL(srv.URL))
	err := a.Dispatch(context.Background(), newReq(), &recordingWriter{})
	if err == nil {
		t.Fatal("expected error on mid-stream error event")
	}
	if !strings.Contains(err.Error(), "overloaded") {
		t.Errorf("err=%q, want to contain 'overloaded'", err)
	}
}

func TestDispatch_ContextCancellation(t *testing.T) {
	t.Parallel()
	srv := fakeAnthropic(t, http.StatusOK, happyPathSSE, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancelled

	a := New(WithBaseURL(srv.URL))
	err := a.Dispatch(ctx, newReq(), &recordingWriter{})
	if err == nil {
		t.Fatal("expected error on cancelled ctx")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err=%v, want wrapped context.Canceled", err)
	}
}

func TestBuildAnthropicRequest_SystemExtraction(t *testing.T) {
	t.Parallel()
	req := &domain.InferenceRequest{
		Model: "claude-3-5-sonnet",
		Messages: []domain.Message{
			{Role: "system", Content: "you are concise"},
			{Role: "user", Content: "hi"},
			{Role: "system", Content: "use markdown"},
			{Role: "assistant", Content: "ok"},
		},
		MaxTokens: 0, // exercises default
	}
	body, err := buildAnthropicRequest(req)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	var got anthropicRequest
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.System != "you are concise\n\nuse markdown" {
		t.Errorf("System=%q, want concatenated", got.System)
	}
	if len(got.Messages) != 2 {
		t.Errorf("got %d messages, want 2 (system removed)", len(got.Messages))
	}
	if got.MaxTokens != defaultMaxTokens {
		t.Errorf("MaxTokens=%d, want %d", got.MaxTokens, defaultMaxTokens)
	}
	if !got.Stream {
		t.Error("Stream should be true (we always stream upstream)")
	}
}

func TestBuildAnthropicRequest_NoNonSystemMessages(t *testing.T) {
	t.Parallel()
	req := &domain.InferenceRequest{
		Model:    "claude-3-5-sonnet",
		Messages: []domain.Message{{Role: "system", Content: "be brief"}},
	}
	_, err := buildAnthropicRequest(req)
	if err == nil {
		t.Fatal("expected error when only system messages provided")
	}
}

func TestMapStopReason(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"end_turn":      "stop",
		"stop_sequence": "stop",
		"max_tokens":    "length",
		"tool_use":      "tool_calls",
		"weird_thing":   "stop",
	}
	for in, want := range cases {
		if got := mapStopReason(in); got != want {
			t.Errorf("mapStopReason(%q)=%q, want %q", in, got, want)
		}
	}
}
