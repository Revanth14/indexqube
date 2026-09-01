package openai

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
	events []string
	doneN  int
}

func (r *recordingWriter) WriteData(data []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	r.frames = append(r.frames, cp)
	return nil
}
func (r *recordingWriter) WriteEvent(event string, _ []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
	return nil
}
func (r *recordingWriter) WriteDone() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.doneN++
	return nil
}
func (r *recordingWriter) Flush() error { return nil }

func (r *recordingWriter) Frames() [][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([][]byte, len(r.frames))
	copy(out, r.frames)
	return out
}

// fakeOpenAI returns an httptest.Server that emits the given SSE body.
// captureReq, if non-nil, is populated with the captured upstream
// request for header / body assertions.
func fakeOpenAI(t *testing.T, status int, sseBody string, captureReq *http.Request) *httptest.Server {
	t.Helper()
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if captureReq != nil {
			body, _ := io.ReadAll(r.Body)
			cloned := r.Clone(r.Context())
			cloned.Body = io.NopCloser(strings.NewReader(string(body)))
			*captureReq = *cloned
		}
		if status != http.StatusOK {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(sseBody))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		rc := http.NewResponseController(w)
		// Flush per line so the adapter sees frames progressively.
		for _, line := range strings.Split(sseBody, "\n") {
			_, _ = w.Write([]byte(line + "\n"))
			_ = rc.Flush()
		}
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

const happyPathSSE = `data: {"id":"chatcmpl-x","object":"chat.completion.chunk","created":1700000000,"model":"gpt-4o-mini","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}

data: {"id":"chatcmpl-x","object":"chat.completion.chunk","created":1700000000,"model":"gpt-4o-mini","choices":[{"index":0,"delta":{"content":"Hello"},"finish_reason":null}]}

data: {"id":"chatcmpl-x","object":"chat.completion.chunk","created":1700000000,"model":"gpt-4o-mini","choices":[{"index":0,"delta":{"content":" world"},"finish_reason":null}]}

data: {"id":"chatcmpl-x","object":"chat.completion.chunk","created":1700000000,"model":"gpt-4o-mini","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}

data: [DONE]
`

func newReq() *domain.InferenceRequest {
	return &domain.InferenceRequest{
		Model:    "gpt-4o-mini",
		Messages: []domain.Message{{Role: "user", Content: "say hello"}},
		Stream:   true,
		Credential: domain.Credential{
			Provider: domain.ProviderOpenAI,
			APIKey:   "sk-test-openai-key",
		},
	}
}

func TestDispatch_HappyPath(t *testing.T) {
	t.Parallel()
	var captured http.Request
	srv := fakeOpenAI(t, http.StatusOK, happyPathSSE, &captured)

	a := New(WithBaseURL(srv.URL))
	rec := &recordingWriter{}

	if err := a.Dispatch(context.Background(), newReq(), rec); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	frames := rec.Frames()
	// 4 forwarded data chunks; [DONE] is consumed by the adapter, not forwarded.
	if len(frames) != 4 {
		t.Fatalf("got %d frames, want 4 (4 chunks; [DONE] not forwarded)", len(frames))
	}

	// Frame 0: role assistant
	var f0 struct {
		Choices []struct {
			Delta struct{ Role, Content string } `json:"delta"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(frames[0], &f0); err != nil {
		t.Fatalf("decode frame 0: %v", err)
	}
	if f0.Choices[0].Delta.Role != "assistant" {
		t.Errorf("frame 0 role=%q, want assistant", f0.Choices[0].Delta.Role)
	}

	// Frame 1 + 2: content deltas
	for i, want := range []string{"Hello", " world"} {
		var f struct {
			Choices []struct {
				Delta struct{ Content string } `json:"delta"`
			} `json:"choices"`
		}
		if err := json.Unmarshal(frames[i+1], &f); err != nil {
			t.Fatalf("decode frame %d: %v", i+1, err)
		}
		if f.Choices[0].Delta.Content != want {
			t.Errorf("frame %d content=%q, want %q", i+1, f.Choices[0].Delta.Content, want)
		}
	}

	// Last forwarded frame should carry finish_reason "stop".
	last := frames[len(frames)-1]
	if !strings.Contains(string(last), `"finish_reason":"stop"`) {
		t.Errorf("last frame missing finish_reason=stop: %q", last)
	}

	// Verify upstream call shape.
	if got := captured.Header.Get("Authorization"); got != "Bearer sk-test-openai-key" {
		t.Errorf("upstream Authorization=%q, want Bearer sk-test-openai-key", got)
	}
	if captured.URL.Path != "/v1/chat/completions" {
		t.Errorf("upstream path=%q, want /v1/chat/completions", captured.URL.Path)
	}
	if captured.Method != http.MethodPost {
		t.Errorf("upstream method=%q, want POST", captured.Method)
	}
}

func TestDispatch_DoneSentinelNotForwarded(t *testing.T) {
	t.Parallel()
	srv := fakeOpenAI(t, http.StatusOK, happyPathSSE, nil)

	a := New(WithBaseURL(srv.URL))
	rec := &recordingWriter{}
	if err := a.Dispatch(context.Background(), newReq(), rec); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	for i, f := range rec.Frames() {
		if strings.Contains(string(f), "[DONE]") {
			t.Errorf("frame %d leaked [DONE] sentinel into TokenWriter: %q", i, f)
		}
	}
}

func TestDispatch_Returns4xxAsError(t *testing.T) {
	t.Parallel()
	srv := fakeOpenAI(t, http.StatusUnauthorized, `{"error":{"message":"invalid api key"}}`, nil)

	a := New(WithBaseURL(srv.URL))
	err := a.Dispatch(context.Background(), newReq(), &recordingWriter{})
	if err == nil {
		t.Fatal("expected error on 401")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Errorf("err=%q, want to contain 401", err)
	}
}

func TestDispatch_MidStreamErrorChunkBecomesAdapterError(t *testing.T) {
	t.Parallel()
	const errSSE = `data: {"id":"chatcmpl-x","object":"chat.completion.chunk","created":1700000000,"model":"gpt-4o-mini","choices":[{"index":0,"delta":{"content":"partial"},"finish_reason":null}]}

data: {"error":{"message":"server overloaded","type":"server_error","code":"overloaded"}}

`
	srv := fakeOpenAI(t, http.StatusOK, errSSE, nil)

	a := New(WithBaseURL(srv.URL))
	rec := &recordingWriter{}
	err := a.Dispatch(context.Background(), newReq(), rec)
	if err == nil {
		t.Fatal("expected error on mid-stream error chunk")
	}
	if !strings.Contains(err.Error(), "overloaded") {
		t.Errorf("err=%q, want to contain 'overloaded'", err)
	}

	// The "partial" chunk before the error must have been forwarded to
	// the writer (the live client gets its half-stream); the governor's
	// tee will then abandon the capture since the adapter returned an
	// error.
	if len(rec.Frames()) != 1 {
		t.Errorf("got %d frames, want 1 (the partial before error)", len(rec.Frames()))
	}
}

func TestDispatch_ContextCancellation(t *testing.T) {
	t.Parallel()
	srv := fakeOpenAI(t, http.StatusOK, happyPathSSE, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	a := New(WithBaseURL(srv.URL))
	err := a.Dispatch(ctx, newReq(), &recordingWriter{})
	if err == nil {
		t.Fatal("expected error on cancelled ctx")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err=%v, want wrapped context.Canceled", err)
	}
}

func TestBuildRequest_PreservesMessagesInline(t *testing.T) {
	t.Parallel()
	req := &domain.InferenceRequest{
		Model: "gpt-4o-mini",
		Messages: []domain.Message{
			{Role: "system", Content: "be terse"},
			{Role: "user", Content: "ping"},
			{Role: "assistant", Content: "pong"},
		},
		MaxTokens:   0,
		Temperature: 0.5,
	}
	body, err := buildRequest(req)
	if err != nil {
		t.Fatalf("buildRequest: %v", err)
	}
	var got openAIRequest
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// OpenAI keeps system messages INLINE in messages -- unlike Anthropic.
	if len(got.Messages) != 3 {
		t.Errorf("got %d messages, want 3 (system stays inline)", len(got.Messages))
	}
	if got.Messages[0].Role != "system" || got.Messages[0].Content != "be terse" {
		t.Errorf("system message dropped or rewritten: %+v", got.Messages[0])
	}
	if got.MaxTokens != defaultMaxTokens {
		t.Errorf("MaxTokens=%d, want %d", got.MaxTokens, defaultMaxTokens)
	}
	if got.Temperature == nil || *got.Temperature != 0.5 {
		t.Errorf("Temperature=%v, want pointer to 0.5", got.Temperature)
	}
	if !got.Stream {
		t.Error("Stream should be true (we always stream upstream)")
	}
}

func TestIsErrorChunk(t *testing.T) {
	t.Parallel()
	cases := map[string]bool{
		`{"error":{"message":"x"}}`:                                     true,
		`  {"error":{"x":1}}`:                                           true, // leading whitespace tolerated
		`{"id":"x","object":"chat.completion.chunk"}`:                   false,
		`{"choices":[{"delta":{"content":"this has the word error"}}]}`: false,
		``: false,
	}
	for in, want := range cases {
		if got := isErrorChunk([]byte(in)); got != want {
			t.Errorf("isErrorChunk(%q)=%v, want %v", in, got, want)
		}
	}
}
