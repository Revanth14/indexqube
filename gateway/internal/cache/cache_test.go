package cache

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/domain"
)

func sampleReq(apiKey, model, content string) *domain.InferenceRequest {
	return &domain.InferenceRequest{
		Model:    model,
		Messages: []domain.Message{{Role: "user", Content: content}},
		Stream:   true,
		Credential: domain.Credential{
			Provider: domain.ProviderAnthropic,
			APIKey:   apiKey,
		},
	}
}

// --- DeriveKey ---

func TestDeriveKey_DeterministicForSameInput(t *testing.T) {
	t.Parallel()
	r := sampleReq("sk-1", "claude-3-5-haiku", "hello world")
	k1, err := DeriveKey(r)
	if err != nil {
		t.Fatalf("DeriveKey: %v", err)
	}
	k2, err := DeriveKey(r)
	if err != nil {
		t.Fatalf("DeriveKey: %v", err)
	}
	if k1 != k2 {
		t.Errorf("non-deterministic: %q vs %q", k1, k2)
	}
	if len(k1) != 64 {
		t.Errorf("key length=%d, want 64 (hex sha256)", len(k1))
	}
}

func TestDeriveKey_TenantIsolation(t *testing.T) {
	t.Parallel()
	a := sampleReq("sk-A", "claude-3-5-haiku", "hello")
	b := sampleReq("sk-B", "claude-3-5-haiku", "hello")
	ka, _ := DeriveKey(a)
	kb, _ := DeriveKey(b)
	if ka == kb {
		t.Error("identical content under different api keys produced the same key")
	}
}

func TestDeriveKey_ContentSensitive(t *testing.T) {
	t.Parallel()
	a := sampleReq("sk-1", "claude-3-5-haiku", "hello")
	b := sampleReq("sk-1", "claude-3-5-haiku", "hello!")
	ka, _ := DeriveKey(a)
	kb, _ := DeriveKey(b)
	if ka == kb {
		t.Error("different prompts produced the same key")
	}
}

func TestDeriveKey_StreamFlagIgnored(t *testing.T) {
	t.Parallel()
	a := sampleReq("sk-1", "claude-3-5-haiku", "hi")
	b := sampleReq("sk-1", "claude-3-5-haiku", "hi")
	a.Stream = false
	b.Stream = true
	ka, _ := DeriveKey(a)
	kb, _ := DeriveKey(b)
	if ka != kb {
		t.Error("Stream flag affected key (it should not)")
	}
}

// --- MemoryCache ---

func newEntry(size int) *Entry {
	chunk := make([]byte, size)
	for i := range chunk {
		chunk[i] = byte('a' + (i % 26))
	}
	return &Entry{
		Provider:  domain.ProviderAnthropic,
		Model:     "claude-3-5-haiku",
		Chunks:    [][]byte{chunk},
		CreatedAt: time.Now(),
	}
}

func TestMemoryCache_GetMissOnEmpty(t *testing.T) {
	t.Parallel()
	c := NewMemoryCache(MemoryConfig{MaxBytes: 1024})
	_, hit, err := c.Get(context.Background(), "missing")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if hit {
		t.Error("hit on empty cache")
	}
}

func TestMemoryCache_PutThenGet(t *testing.T) {
	t.Parallel()
	c := NewMemoryCache(MemoryConfig{MaxBytes: 1024})
	want := newEntry(50)
	if err := c.Put(context.Background(), "k", want); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, hit, err := c.Get(context.Background(), "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !hit {
		t.Fatal("miss after Put")
	}
	if got.Bytes() != want.Bytes() {
		t.Errorf("bytes mismatch")
	}
}

func TestMemoryCache_LRUEvictionOnSizeBreach(t *testing.T) {
	t.Parallel()
	// Cap at 200 bytes; insert three 90-byte entries and observe
	// that the first one (oldest) is evicted.
	c := NewMemoryCache(MemoryConfig{MaxBytes: 200})
	ctx := context.Background()

	if err := c.Put(ctx, "a", newEntry(90)); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "b", newEntry(90)); err != nil {
		t.Fatal(err)
	}
	// Touch "a" to make "b" the LRU.
	if _, hit, _ := c.Get(ctx, "a"); !hit {
		t.Fatal("missed a")
	}
	if err := c.Put(ctx, "c", newEntry(90)); err != nil {
		t.Fatal(err)
	}

	if _, hit, _ := c.Get(ctx, "b"); hit {
		t.Error("b should have been evicted (it was the LRU after touching a)")
	}
	if _, hit, _ := c.Get(ctx, "a"); !hit {
		t.Error("a should be present (recently used)")
	}
	if _, hit, _ := c.Get(ctx, "c"); !hit {
		t.Error("c should be present (just inserted)")
	}
}

func TestMemoryCache_PutTooLargeRejected(t *testing.T) {
	t.Parallel()
	c := NewMemoryCache(MemoryConfig{MaxBytes: 100})
	err := c.Put(context.Background(), "k", newEntry(200))
	if !errors.Is(err, ErrEntryTooLarge) {
		t.Errorf("got err=%v, want ErrEntryTooLarge", err)
	}
	if c.Stats().Entries != 0 {
		t.Error("rejected entry still landed in cache")
	}
}

func TestMemoryCache_TTLExpiresLazyOnGet(t *testing.T) {
	t.Parallel()
	c := NewMemoryCache(MemoryConfig{MaxBytes: 1024, TTL: 50 * time.Millisecond})
	now := time.Now()
	c.nowFn = func() time.Time { return now }

	if err := c.Put(context.Background(), "k", newEntry(50)); err != nil {
		t.Fatal(err)
	}
	if _, hit, _ := c.Get(context.Background(), "k"); !hit {
		t.Fatal("expected hit immediately after Put")
	}

	// Advance virtual time past TTL.
	now = now.Add(60 * time.Millisecond)
	if _, hit, _ := c.Get(context.Background(), "k"); hit {
		t.Error("expected miss after TTL expiry")
	}
	if c.Stats().Entries != 0 {
		t.Error("expired entry was not removed on Get")
	}
}

func TestMemoryCache_DisabledByZeroMaxBytes(t *testing.T) {
	t.Parallel()
	c := NewMemoryCache(MemoryConfig{MaxBytes: 0})
	if err := c.Put(context.Background(), "k", newEntry(50)); err != nil {
		t.Fatalf("Put on disabled cache should be a no-op, got: %v", err)
	}
	if _, hit, _ := c.Get(context.Background(), "k"); hit {
		t.Error("disabled cache returned a hit")
	}
}

func TestMemoryCache_ConcurrentRaceFree(t *testing.T) {
	t.Parallel()
	c := NewMemoryCache(MemoryConfig{MaxBytes: 1 << 20})
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				k := Key(fmt.Sprintf("k-%d-%d", i, j%4))
				if j%2 == 0 {
					_ = c.Put(ctx, k, newEntry(64))
				} else {
					_, _, _ = c.Get(ctx, k)
				}
			}
		}(i)
	}
	wg.Wait()
}

// --- Bloom ---

func TestBloom_NoFalseNegatives(t *testing.T) {
	t.Parallel()
	b := NewBloom(1000, 0.01)
	keys := make([][]byte, 1000)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("entry-%d", i))
		b.Add(keys[i])
	}
	for i, k := range keys {
		if !b.Contains(k) {
			t.Errorf("false negative on key #%d", i)
		}
	}
}

func TestBloom_FalsePositiveRateBoundedRoughly(t *testing.T) {
	t.Parallel()
	const n = 5000
	const target = 0.01
	b := NewBloom(n, target)
	for i := 0; i < n; i++ {
		b.Add([]byte(fmt.Sprintf("inserted-%d", i)))
	}
	const probes = 10000
	var fp int
	for i := 0; i < probes; i++ {
		// All these keys are NOT in the filter.
		if b.Contains([]byte(fmt.Sprintf("not-inserted-%d", i))) {
			fp++
		}
	}
	rate := float64(fp) / float64(probes)
	// Allow generous slack: target=1% but stochastic with our small n,
	// caps at 4% prevents flakes while still catching gross sizing bugs.
	if rate > 0.04 {
		t.Errorf("false positive rate=%.4f far exceeds target=%.4f", rate, target)
	}
}

func TestBloom_ConcurrentAddAndContains(t *testing.T) {
	t.Parallel()
	b := NewBloom(10_000, 0.01)
	var wg sync.WaitGroup
	var hits atomic.Int64
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				k := []byte(fmt.Sprintf("k-%d-%d", w, i))
				b.Add(k)
				if b.Contains(k) {
					hits.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()
	if hits.Load() != 8*500 {
		t.Errorf("got %d hits, want %d (every Add must be visible to its own Contains)", hits.Load(), 8*500)
	}
}

// --- Tee ---

type recordingWriter struct {
	frames [][]byte
	events []string
	doneN  int
}

func (r *recordingWriter) WriteData(data []byte) error {
	cp := make([]byte, len(data))
	copy(cp, data)
	r.frames = append(r.frames, cp)
	return nil
}
func (r *recordingWriter) WriteEvent(event string, _ []byte) error {
	r.events = append(r.events, event)
	return nil
}
func (r *recordingWriter) WriteDone() error { r.doneN++; return nil }
func (r *recordingWriter) Flush() error     { return nil }

func TestTee_CapturesAndForwardsData(t *testing.T) {
	t.Parallel()
	rec := &recordingWriter{}
	tee := NewTee(rec, 10_000)
	if err := tee.WriteData([]byte(`{"x":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := tee.WriteData([]byte(`{"y":2}`)); err != nil {
		t.Fatal(err)
	}
	if err := tee.WriteDone(); err != nil {
		t.Fatal(err)
	}
	if len(rec.frames) != 2 {
		t.Errorf("forwarded frames=%d, want 2", len(rec.frames))
	}
	entry, ok := tee.Entry(domain.ProviderAnthropic, "claude")
	if !ok {
		t.Fatal("Entry returned ok=false; should have captured")
	}
	if len(entry.Chunks) != 2 {
		t.Errorf("captured chunks=%d, want 2", len(entry.Chunks))
	}
	// WriteDone is NOT captured -- proxy adds [DONE] on its own.
	for _, c := range entry.Chunks {
		if strings.Contains(string(c), "[DONE]") {
			t.Errorf("done sentinel leaked into capture: %q", c)
		}
	}
}

func TestTee_AbandonsOnEvent(t *testing.T) {
	t.Parallel()
	rec := &recordingWriter{}
	tee := NewTee(rec, 10_000)
	_ = tee.WriteData([]byte(`{"partial":1}`))
	_ = tee.WriteEvent("error", []byte(`{"x":1}`))
	if _, ok := tee.Entry(domain.ProviderAnthropic, "m"); ok {
		t.Error("Entry should be abandoned after WriteEvent")
	}
	if len(rec.events) != 1 {
		t.Errorf("event not forwarded; got %d", len(rec.events))
	}
}

func TestTee_AbandonsOnExceedingMaxCapture(t *testing.T) {
	t.Parallel()
	rec := &recordingWriter{}
	tee := NewTee(rec, 10) // tiny cap
	_ = tee.WriteData([]byte("123456789"))
	_ = tee.WriteData([]byte("0"))   // capture still under cap (size=10 fits)
	_ = tee.WriteData([]byte("XXX")) // pushes over cap -> abandon
	if _, ok := tee.Entry(domain.ProviderAnthropic, "m"); ok {
		t.Error("Entry should be abandoned after exceeding maxCapture")
	}
	// Live forwarding must continue regardless of capture state.
	if len(rec.frames) != 3 {
		t.Errorf("live frames forwarded=%d, want 3", len(rec.frames))
	}
}

func TestTee_DefensiveCopy(t *testing.T) {
	t.Parallel()
	rec := &recordingWriter{}
	tee := NewTee(rec, 10_000)
	buf := []byte("original")
	_ = tee.WriteData(buf)
	// Mutate the caller's buffer afterwards (simulates pool reuse).
	for i := range buf {
		buf[i] = 'X'
	}
	entry, ok := tee.Entry(domain.ProviderAnthropic, "m")
	if !ok {
		t.Fatal("entry not captured")
	}
	if string(entry.Chunks[0]) != "original" {
		t.Errorf("captured chunk corrupted by caller mutation: %q", entry.Chunks[0])
	}
}

// --- Entry.Replay ---

func TestEntry_ReplayWritesAllChunks(t *testing.T) {
	t.Parallel()
	rec := &recordingWriter{}
	e := &Entry{
		Chunks: [][]byte{[]byte("a"), []byte("b"), []byte("c")},
	}
	if err := e.Replay(rec); err != nil {
		t.Fatal(err)
	}
	if len(rec.frames) != 3 {
		t.Errorf("frames=%d, want 3", len(rec.frames))
	}
}
