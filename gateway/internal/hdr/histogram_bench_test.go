package hdr_test

import (
	"testing"

	"github.com/Revanth14/indexqube/gateway/internal/hdr"
)

// ── Record ───────────────────────────────────────────────────────────────────

// BenchmarkRecord measures the cost of a single Record call under a mutex.
// Expected: 0 allocs/op, ~20–60 ns/op (dominated by sync.Mutex).
func BenchmarkRecord(b *testing.B) {
	h := hdr.Default()
	b.ReportAllocs()
	b.ResetTimer()
	for i := b.N; i > 0; i-- {
		h.Record(int64(i % 1_000_000))
	}
}

// BenchmarkRecord_Parallel measures Record throughput across GOMAXPROCS goroutines.
// Contention on the mutex becomes visible here.
func BenchmarkRecord_Parallel(b *testing.B) {
	h := hdr.Default()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int64(0)
		for pb.Next() {
			h.Record(i % 1_000_000)
			i++
		}
	})
}

// BenchmarkRecordN measures bulk recording (one lock acquisition per N values).
func BenchmarkRecordN(b *testing.B) {
	h := hdr.Default()
	b.ReportAllocs()
	b.ResetTimer()
	for i := b.N; i > 0; i-- {
		h.RecordN(int64(i%1_000_000), 10)
	}
}

// ── Percentile queries ────────────────────────────────────────────────────────

// BenchmarkValueAtPercentile measures a single percentile query.
// Expected: 0 allocs/op (walks the counts array under the lock).
func BenchmarkValueAtPercentile(b *testing.B) {
	h := filledHistogram()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = h.ValueAtPercentile(99)
	}
}

// BenchmarkValueAtPercentile_P999 walks slightly further into the tail.
func BenchmarkValueAtPercentile_P999(b *testing.B) {
	h := filledHistogram()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = h.ValueAtPercentile(99.9)
	}
}

// ── Snapshot ─────────────────────────────────────────────────────────────────

// BenchmarkSnapshot measures the cost of a full Snapshot (5 percentiles + mean
// computed in a single O(N) pass).  Expected: 0 allocs/op.
func BenchmarkSnapshot(b *testing.B) {
	h := filledHistogram()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = h.Snapshot()
	}
}

// ── AllocsPerRun assertions ───────────────────────────────────────────────────

// TestRecordZeroAllocs asserts that Record never allocates on the heap.
func TestRecordZeroAllocs(t *testing.T) {
	h := hdr.Default()
	allocs := testing.AllocsPerRun(100, func() {
		h.Record(12_345)
	})
	if allocs > 0 {
		t.Errorf("Record: %.0f allocs/op; want 0", allocs)
	}
}

// TestValueAtPercentileZeroAllocs asserts that ValueAtPercentile never allocates.
func TestValueAtPercentileZeroAllocs(t *testing.T) {
	h := filledHistogram()
	allocs := testing.AllocsPerRun(100, func() {
		_ = h.ValueAtPercentile(99)
	})
	if allocs > 0 {
		t.Errorf("ValueAtPercentile: %.0f allocs/op; want 0", allocs)
	}
}

// TestSnapshotZeroAllocs asserts that Snapshot never allocates on the heap
// (it returns a plain struct by value; no slices or maps inside).
func TestSnapshotZeroAllocs(t *testing.T) {
	h := filledHistogram()
	allocs := testing.AllocsPerRun(100, func() {
		_ = h.Snapshot()
	})
	if allocs > 0 {
		t.Errorf("Snapshot: %.0f allocs/op; want 0", allocs)
	}
}

// ── Overflow ──────────────────────────────────────────────────────────────────

// TestOverflowCount verifies that out-of-range values are counted and exposed.
func TestOverflowCount(t *testing.T) {
	cfg := hdr.Config{
		LowestDiscernibleValue: 1,
		HighestTrackableValue:  1_000,
		SignificantFigures:     2,
	}
	h, err := hdr.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	h.Record(500)       // within range
	h.RecordN(9_999, 3) // above HighestTrackableValue — clamped, counted as overflow

	if h.OverflowCount() != 3 {
		t.Errorf("OverflowCount() = %d; want 3", h.OverflowCount())
	}
	// Total count still includes the clamped values.
	if h.Count() != 4 {
		t.Errorf("Count() = %d; want 4 (in-range + clamped)", h.Count())
	}
	// Snapshot surfaces overflow too.
	snap := h.Snapshot()
	if snap.OverflowCount != 3 {
		t.Errorf("Snapshot.OverflowCount = %d; want 3", snap.OverflowCount)
	}
}

func TestOverflowCountReset(t *testing.T) {
	h := hdr.Default()
	h.RecordN(hdr.DefaultHighestTrackableValue+1, 5)
	if h.OverflowCount() != 5 {
		t.Fatalf("pre-reset OverflowCount = %d; want 5", h.OverflowCount())
	}
	h.Reset()
	if h.OverflowCount() != 0 {
		t.Errorf("post-reset OverflowCount = %d; want 0", h.OverflowCount())
	}
}

func TestOverflowCountMerge(t *testing.T) {
	cfg := hdr.DefaultConfig()
	a, _ := hdr.New(cfg)
	b, _ := hdr.New(cfg)

	a.RecordN(hdr.DefaultHighestTrackableValue+1, 2)
	b.RecordN(hdr.DefaultHighestTrackableValue+1, 3)

	if err := a.Merge(b); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if a.OverflowCount() != 5 {
		t.Errorf("merged OverflowCount = %d; want 5", a.OverflowCount())
	}
}

// ── Helper ────────────────────────────────────────────────────────────────────

// filledHistogram returns a histogram pre-loaded with 100 000 values spread
// across the µs–minute range, giving benchmarks a realistic counts distribution.
func filledHistogram() *hdr.Histogram {
	h := hdr.Default()
	// Bimodal: 90% fast (1–10 ms), 10% slow (1–5 s).
	for i := int64(0); i < 90_000; i++ {
		h.Record((i % 10_000) + 1_000) // 1 000–11 000 µs
	}
	for i := int64(0); i < 10_000; i++ {
		h.Record((i % 4_000_000) + 1_000_000) // 1–5 s in µs
	}
	return h
}
