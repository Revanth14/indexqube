package governor

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Revanth14/indexqube/gateway/internal/cache"
	"github.com/Revanth14/indexqube/gateway/internal/domain"
)

// stubAdapter is an in-memory Adapter for tests. The chunk slice
// describes the WriteData calls Dispatch should emit; setting err
// short-circuits dispatch.
type stubAdapter struct {
	calls    atomic.Int32
	tokens   [][]byte
	err      error
	readyErr error
	gotReq   *domain.InferenceRequest
	gotCtx   context.Context
}

func (s *stubAdapter) Dispatch(ctx context.Context, req *domain.InferenceRequest, tw domain.TokenWriter) error {
	s.calls.Add(1)
	s.gotReq = req
	s.gotCtx = ctx
	if s.err != nil {
		return s.err
	}
	for _, t := range s.tokens {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := tw.WriteData(t); err != nil {
			return err
		}
	}
	return nil
}

func (s *stubAdapter) Ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.readyErr
}

// recordingWriter captures the frames an Adapter emits.
type recordingWriter struct {
	frames [][]byte
	events []recordedEvent
}

type recordedEvent struct {
	name string
	data []byte
}

func (r *recordingWriter) WriteData(data []byte) error {
	cp := make([]byte, len(data))
	copy(cp, data)
	r.frames = append(r.frames, cp)
	return nil
}
func (r *recordingWriter) WriteEvent(event string, data []byte) error {
	cp := make([]byte, len(data))
	copy(cp, data)
	r.events = append(r.events, recordedEvent{name: event, data: cp})
	return nil
}
func (r *recordingWriter) WriteDone() error { return nil }
func (r *recordingWriter) Flush() error     { return nil }

func newReq(p domain.Provider) *domain.InferenceRequest {
	return &domain.InferenceRequest{
		Model:      "claude-3-5-sonnet",
		Messages:   []domain.Message{{Role: "user", Content: "hi"}},
		Stream:     true,
		Credential: domain.Credential{Provider: p, APIKey: "k"},
	}
}

func TestStream_RoutesToCorrectAdapter(t *testing.T) {
	t.Parallel()

	anthropic := &stubAdapter{tokens: [][]byte{[]byte("a"), []byte("b")}}
	openai := &stubAdapter{tokens: [][]byte{[]byte("x")}}

	g := New(
		WithAdapter(domain.ProviderAnthropic, anthropic),
		WithAdapter(domain.ProviderOpenAI, openai),
	)

	rec := &recordingWriter{}
	if err := g.Stream(context.Background(), newReq(domain.ProviderAnthropic), rec); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if anthropic.calls.Load() != 1 {
		t.Errorf("anthropic calls=%d, want 1", anthropic.calls.Load())
	}
	if openai.calls.Load() != 0 {
		t.Errorf("openai calls=%d, want 0 (wrong adapter routed)", openai.calls.Load())
	}
	if len(rec.frames) != 2 {
		t.Errorf("got %d frames, want 2", len(rec.frames))
	}
}

func TestStream_EmitsOptimizerReceiptForZeroSavings(t *testing.T) {
	t.Parallel()

	stub := &stubAdapter{tokens: [][]byte{[]byte("ok")}}
	g := New(WithAdapter(domain.ProviderAnthropic, stub))

	rec := &recordingWriter{}
	if err := g.Stream(context.Background(), newReq(domain.ProviderAnthropic), rec); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if len(rec.events) != 1 {
		t.Fatalf("events=%d want 1", len(rec.events))
	}
	got := string(rec.events[0].data)
	for _, want := range []string{`"version":"v1"`, `"source":"stream"`, `"mode":"none"`} {
		if !strings.Contains(got, want) {
			t.Fatalf("optimizer receipt missing %s: %s", want, got)
		}
	}
}

func TestStream_EmitsOptimizerEventForPrunedContext(t *testing.T) {
	t.Parallel()

	h := NewMemoryHistory()
	tenant := "ide-session"
	oldLines := makeNumberedLines(80)
	newLines := append([]string(nil), oldLines...)
	newLines[40] = "line 0041 changed from VS Code"
	h.Put(context.Background(), domain.ResolveTenantKey(tenant, ""), "src/main.go", strings.Join(oldLines, "\n"))

	stub := &stubAdapter{tokens: [][]byte{[]byte("ok")}}
	g := New(
		WithAdapter(domain.ProviderAnthropic, stub),
		WithHistory(h),
		WithPruning(true, 8000),
	)

	req := newReq(domain.ProviderAnthropic)
	req.SessionKey = tenant
	req.Messages = []domain.Message{{Role: "user", Content: "```go src/main.go\n" + strings.Join(newLines, "\n") + "\n```"}}

	rec := &recordingWriter{}
	if err := g.Stream(context.Background(), req, rec); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if len(rec.events) != 1 {
		t.Fatalf("events=%d want 1", len(rec.events))
	}
	if rec.events[0].name != "iq_optimizer" {
		t.Fatalf("event=%q want iq_optimizer", rec.events[0].name)
	}
	if !strings.Contains(string(rec.events[0].data), `"mode":"diff"`) {
		t.Fatalf("optimizer event missing diff mode: %s", rec.events[0].data)
	}
}

func TestStream_ErrorsOnUnregisteredProvider(t *testing.T) {
	t.Parallel()
	g := New() // empty registry
	err := g.Stream(context.Background(), newReq(domain.ProviderAnthropic), &recordingWriter{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "no adapter registered") {
		t.Errorf("got err=%q, want mention of 'no adapter registered'", err)
	}
}

func TestStream_ErrorsOnNilRequest(t *testing.T) {
	t.Parallel()
	g := New(WithAdapter(domain.ProviderAnthropic, &stubAdapter{}))
	err := g.Stream(context.Background(), nil, &recordingWriter{})
	if err == nil {
		t.Fatal("expected error on nil request")
	}
}

func TestStream_PropagatesAdapterError(t *testing.T) {
	t.Parallel()
	want := errors.New("upstream exploded")
	g := New(WithAdapter(domain.ProviderAnthropic, &stubAdapter{err: want}))
	got := g.Stream(context.Background(), newReq(domain.ProviderAnthropic), &recordingWriter{})
	if !errors.Is(got, want) {
		t.Errorf("got err=%v, want=%v", got, want)
	}
}

func TestStream_PropagatesContextCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stub := &stubAdapter{tokens: [][]byte{[]byte("a"), []byte("b"), []byte("c")}}
	g := New(WithAdapter(domain.ProviderAnthropic, stub))

	err := g.Stream(ctx, newReq(domain.ProviderAnthropic), &recordingWriter{})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("got err=%v, want context.Canceled", err)
	}
}

func TestWithAdapter_NilIsIgnored(t *testing.T) {
	t.Parallel()
	g := New(WithAdapter(domain.ProviderAnthropic, nil))
	err := g.Stream(context.Background(), newReq(domain.ProviderAnthropic), &recordingWriter{})
	if err == nil {
		t.Error("expected unrouteable error since nil adapter should be ignored")
	}
}

func TestReady_NoAdapters(t *testing.T) {
	t.Parallel()
	g := New()
	if err := g.Ready(context.Background()); err == nil {
		t.Fatal("expected no-adapters readiness error")
	}
}

func TestReady_AdapterError(t *testing.T) {
	t.Parallel()
	g := New(WithAdapter(domain.ProviderAnthropic, &stubAdapter{readyErr: errors.New("warming up")}))
	err := g.Ready(context.Background())
	if err == nil || !strings.Contains(err.Error(), "warming up") {
		t.Fatalf("Ready err=%v, want adapter error", err)
	}
}

func TestReady_HealthyAdapters(t *testing.T) {
	t.Parallel()
	g := New(WithAdapter(domain.ProviderAnthropic, &stubAdapter{}))
	if err := g.Ready(context.Background()); err != nil {
		t.Fatalf("Ready: %v", err)
	}
}

// --- Cache integration ---

func TestStream_CacheMissThenHit(t *testing.T) {
	t.Parallel()

	stub := &stubAdapter{tokens: [][]byte{[]byte("hello"), []byte("world")}}
	c := cache.NewMemoryCache(cache.MemoryConfig{MaxBytes: 1 << 20})
	g := New(
		WithAdapter(domain.ProviderAnthropic, stub),
		WithCache(c, 1<<20),
	)

	// First call: cache miss -> dispatch + persist.
	rec1 := &recordingWriter{}
	if err := g.Stream(context.Background(), newReq(domain.ProviderAnthropic), rec1); err != nil {
		t.Fatalf("first Stream: %v", err)
	}
	if stub.calls.Load() != 1 {
		t.Errorf("after first call, adapter calls=%d, want 1", stub.calls.Load())
	}
	if len(rec1.frames) != 2 {
		t.Errorf("first call frames=%d, want 2", len(rec1.frames))
	}

	// Second call: cache hit -> replay, adapter NOT called.
	rec2 := &recordingWriter{}
	if err := g.Stream(context.Background(), newReq(domain.ProviderAnthropic), rec2); err != nil {
		t.Fatalf("second Stream: %v", err)
	}
	if stub.calls.Load() != 1 {
		t.Errorf("after second call, adapter calls=%d, want STILL 1 (cache should serve)", stub.calls.Load())
	}
	if len(rec2.frames) != 2 {
		t.Errorf("second call frames=%d, want 2", len(rec2.frames))
	}
	if string(rec2.frames[0]) != "hello" || string(rec2.frames[1]) != "world" {
		t.Errorf("replayed frames mismatch: %q, %q", rec2.frames[0], rec2.frames[1])
	}
}

func TestStream_AdapterErrorDoesNotPopulateCache(t *testing.T) {
	t.Parallel()

	stub := &stubAdapter{
		tokens: [][]byte{[]byte("partial")},
		err:    errors.New("midstream"),
	}
	c := cache.NewMemoryCache(cache.MemoryConfig{MaxBytes: 1 << 20})
	g := New(
		WithAdapter(domain.ProviderAnthropic, stub),
		WithCache(c, 1<<20),
	)

	if err := g.Stream(context.Background(), newReq(domain.ProviderAnthropic), &recordingWriter{}); err == nil {
		t.Fatal("expected adapter error to propagate")
	}
	// Adapter set err, so it short-circuited before writing tokens.
	// Even if it had partial output, the adapter error must prevent caching.
	if c.Stats().Entries != 0 {
		t.Errorf("cache entries=%d after adapter error, want 0", c.Stats().Entries)
	}
}

// errorAdapter writes data and then fires WriteEvent before returning;
// the tee should abandon capture.
type errorEventAdapter struct{}

func (errorEventAdapter) Dispatch(_ context.Context, _ *domain.InferenceRequest, tw domain.TokenWriter) error {
	_ = tw.WriteData([]byte("partial"))
	_ = tw.WriteEvent("error", []byte(`{"x":1}`))
	return nil // returns clean even though it emitted an error frame
}

func (errorEventAdapter) Ready(context.Context) error { return nil }

func TestStream_TeeAbandonOnEventSkipsCacheWrite(t *testing.T) {
	t.Parallel()

	c := cache.NewMemoryCache(cache.MemoryConfig{MaxBytes: 1 << 20})
	g := New(
		WithAdapter(domain.ProviderAnthropic, errorEventAdapter{}),
		WithCache(c, 1<<20),
	)

	if err := g.Stream(context.Background(), newReq(domain.ProviderAnthropic), &recordingWriter{}); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if c.Stats().Entries != 0 {
		t.Errorf("cache should not contain partial-then-error responses; entries=%d", c.Stats().Entries)
	}
}

func TestStream_DifferentTenantsDoNotShareCache(t *testing.T) {
	t.Parallel()

	stub := &stubAdapter{tokens: [][]byte{[]byte("payload")}}
	c := cache.NewMemoryCache(cache.MemoryConfig{MaxBytes: 1 << 20})
	g := New(
		WithAdapter(domain.ProviderAnthropic, stub),
		WithCache(c, 1<<20),
	)

	reqA := newReq(domain.ProviderAnthropic)
	reqA.Credential.APIKey = "key-A"
	reqB := newReq(domain.ProviderAnthropic)
	reqB.Credential.APIKey = "key-B"

	if err := g.Stream(context.Background(), reqA, &recordingWriter{}); err != nil {
		t.Fatal(err)
	}
	if err := g.Stream(context.Background(), reqB, &recordingWriter{}); err != nil {
		t.Fatal(err)
	}
	// Both calls must hit the adapter -- different tenants should not share cache.
	if stub.calls.Load() != 2 {
		t.Errorf("adapter calls=%d, want 2 (tenants must not share cache)", stub.calls.Load())
	}
}
