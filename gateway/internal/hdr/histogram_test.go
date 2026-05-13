package hdr_test

import (
	"math"
	"sync"
	"testing"

	"github.com/Revanth14/indexqube/gateway/internal/hdr"
)

// ── Construction ──────────────────────────────────────────────────────────────

func TestNewRejectsInvalidConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  hdr.Config
	}{
		{
			name: "zero LowestDiscernibleValue",
			cfg:  hdr.Config{LowestDiscernibleValue: 0, HighestTrackableValue: 1000, SignificantFigures: 2},
		},
		{
			name: "negative LowestDiscernibleValue",
			cfg:  hdr.Config{LowestDiscernibleValue: -1, HighestTrackableValue: 1000, SignificantFigures: 2},
		},
		{
			name: "HighestTrackableValue < 2×LowestDiscernibleValue",
			cfg:  hdr.Config{LowestDiscernibleValue: 100, HighestTrackableValue: 100, SignificantFigures: 2},
		},
		{
			name: "SignificantFigures = 0",
			cfg:  hdr.Config{LowestDiscernibleValue: 1, HighestTrackableValue: 1000, SignificantFigures: 0},
		},
		{
			name: "SignificantFigures = 6",
			cfg:  hdr.Config{LowestDiscernibleValue: 1, HighestTrackableValue: 1000, SignificantFigures: 6},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := hdr.New(tc.cfg)
			if err == nil {
				t.Errorf("New(%+v): expected error, got nil", tc.cfg)
			}
		})
	}
}

func TestDefaultConstructsSuccessfully(t *testing.T) {
	h := hdr.Default()
	if h == nil {
		t.Fatal("Default() returned nil")
	}
	if h.Count() != 0 {
		t.Errorf("new histogram count = %d; want 0", h.Count())
	}
}

// ── Empty-histogram behaviour ─────────────────────────────────────────────────

func TestEmptyHistogramReturnsZeros(t *testing.T) {
	h := hdr.Default()
	if got := h.Count(); got != 0 {
		t.Errorf("Count() = %d; want 0", got)
	}
	if got := h.Min(); got != 0 {
		t.Errorf("Min() = %d; want 0", got)
	}
	if got := h.Max(); got != 0 {
		t.Errorf("Max() = %d; want 0", got)
	}
	if got := h.Mean(); got != 0 {
		t.Errorf("Mean() = %f; want 0", got)
	}
	for _, p := range []float64{0, 50, 90, 99, 100} {
		if got := h.ValueAtPercentile(p); got != 0 {
			t.Errorf("ValueAtPercentile(%v) = %d; want 0", p, got)
		}
	}
	snap := h.Snapshot()
	if snap.Count != 0 || snap.P50 != 0 || snap.P99 != 0 {
		t.Errorf("empty Snapshot not zero: %+v", snap)
	}
}

// ── Single-value recording ────────────────────────────────────────────────────

func TestRecordSingleValue(t *testing.T) {
	h := hdr.Default()
	h.Record(1000) // 1 ms in µs

	if h.Count() != 1 {
		t.Errorf("Count() = %d; want 1", h.Count())
	}
	if h.Min() != 1000 {
		t.Errorf("Min() = %d; want 1000", h.Min())
	}
	if h.Max() != 1000 {
		t.Errorf("Max() = %d; want 1000", h.Max())
	}
	// p50 and p100 should both resolve to an equivalent value near 1000.
	p50 := h.ValueAtPercentile(50)
	if !withinPct(p50, 1000, 1.0) {
		t.Errorf("p50 = %d; want ~1000 (within 1%%)", p50)
	}
}

// ── Precision / equivalence-range ────────────────────────────────────────────

// TestPrecision verifies that the relative error stays within the expected
// bound for 2 significant figures throughout the dynamic range.
func TestPrecision(t *testing.T) {
	h := hdr.Default() // 2 sigfigs ≈ 1% relative error

	// Sample values across many orders of magnitude.
	probes := []int64{
		1, 10, 100, 1_000, 10_000, 100_000,
		1_000_000, 10_000_000, 100_000_000, 1_000_000_000,
	}

	for _, v := range probes {
		h.Reset()
		h.Record(v)

		// The retrieved percentile value should be within 1% of the recorded value.
		got := h.ValueAtPercentile(100)
		if !withinPct(got, v, 1.0) {
			t.Errorf("value %d: p100 = %d, relative error %.3f%% > 1%%",
				v, got, relativeErrorPct(got, v))
		}
	}
}

// ── Percentile accuracy ───────────────────────────────────────────────────────

// TestValueAtPercentile_UniformDistribution records values 1..1000 and checks
// that the retrieved percentiles are accurate to within 1%.
func TestValueAtPercentile_UniformDistribution(t *testing.T) {
	h := hdr.Default()
	const n = 1000
	for i := int64(1); i <= n; i++ {
		h.Record(i)
	}

	cases := []struct {
		pct  float64
		want int64
	}{
		{50, 500},
		{75, 750},
		{90, 900},
		{95, 950},
		{99, 990},
	}

	for _, tc := range cases {
		got := h.ValueAtPercentile(tc.pct)
		if !withinPct(got, tc.want, 1.0) {
			t.Errorf("p%.0f = %d; want ~%d (within 1%%)", tc.pct, got, tc.want)
		}
	}
}

// TestPercentileOf is the inverse of ValueAtPercentile:
// PercentileOf(ValueAtPercentile(p)) should be ≥ p.
func TestPercentileOf_InverseOfValueAtPercentile(t *testing.T) {
	h := hdr.Default()
	for i := int64(1); i <= 10_000; i++ {
		h.Record(i)
	}

	for _, p := range []float64{10, 25, 50, 75, 90, 95, 99} {
		v := h.ValueAtPercentile(p)
		got := h.PercentileOf(v)
		if got < p-1.0 { // allow 1 percentage point tolerance
			t.Errorf("PercentileOf(ValueAtPercentile(%.1f)) = %.2f; want ≥ %.1f", p, got, p)
		}
	}
}

// ── Percentile clamping ───────────────────────────────────────────────────────

func TestValueAtPercentileClamps(t *testing.T) {
	h := hdr.Default()
	h.Record(500)

	// Percentiles outside [0, 100] should be clamped, not panic.
	_ = h.ValueAtPercentile(-5)
	_ = h.ValueAtPercentile(200)
}

// ── Clamping of out-of-range values ──────────────────────────────────────────

func TestRecordClampsNegative(t *testing.T) {
	h := hdr.Default()
	h.Record(-999)
	// Clamped to 0 — the min should be 0 (or the lowest-equivalent value of 0).
	if h.Min() < 0 {
		t.Errorf("Min() = %d after recording -999; want ≥ 0", h.Min())
	}
}

func TestRecordClampsAboveHighest(t *testing.T) {
	cfg := hdr.Config{
		LowestDiscernibleValue: 1,
		HighestTrackableValue:  1_000,
		SignificantFigures:     2,
	}
	h, err := hdr.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h.Record(999_999) // way above HighestTrackableValue
	if h.Max() > 1_000 {
		t.Errorf("Max() = %d; want ≤ 1000 (clamped)", h.Max())
	}
}

// ── RecordN ───────────────────────────────────────────────────────────────────

func TestRecordN(t *testing.T) {
	h := hdr.Default()
	h.RecordN(42, 100)
	if h.Count() != 100 {
		t.Errorf("Count() = %d; want 100", h.Count())
	}
}

// ── Min / Max ─────────────────────────────────────────────────────────────────

func TestMinMax(t *testing.T) {
	h := hdr.Default()
	for _, v := range []int64{500, 1, 9999, 3, 10000} {
		h.Record(v)
	}
	if h.Min() > 1 {
		t.Errorf("Min() = %d; want ≤ 1", h.Min())
	}
	if h.Max() < 10000 {
		t.Errorf("Max() = %d; want ≥ 10000", h.Max())
	}
}

// ── Mean ──────────────────────────────────────────────────────────────────────

func TestMean(t *testing.T) {
	h := hdr.Default()
	// Record 1, 2, 3 — mean should be 2.
	h.Record(1)
	h.Record(2)
	h.Record(3)
	mean := h.Mean()
	if math.Abs(mean-2.0) > 0.05 {
		t.Errorf("Mean() = %.4f; want ~2.0", mean)
	}
}

// ── Reset ─────────────────────────────────────────────────────────────────────

func TestReset(t *testing.T) {
	h := hdr.Default()
	for i := int64(1); i <= 1000; i++ {
		h.Record(i)
	}
	if h.Count() == 0 {
		t.Fatal("pre-reset Count() is 0; something is wrong")
	}
	h.Reset()

	if h.Count() != 0 {
		t.Errorf("post-reset Count() = %d; want 0", h.Count())
	}
	if h.Min() != 0 {
		t.Errorf("post-reset Min() = %d; want 0", h.Min())
	}
	if h.Max() != 0 {
		t.Errorf("post-reset Max() = %d; want 0", h.Max())
	}
	if p := h.ValueAtPercentile(99); p != 0 {
		t.Errorf("post-reset p99 = %d; want 0", p)
	}
}

// ── Merge ─────────────────────────────────────────────────────────────────────

func TestMerge_CombinesCounts(t *testing.T) {
	cfg := hdr.DefaultConfig()
	a, _ := hdr.New(cfg)
	b, _ := hdr.New(cfg)

	for i := int64(1); i <= 500; i++ {
		a.Record(i)
	}
	for i := int64(501); i <= 1000; i++ {
		b.Record(i)
	}

	if err := a.Merge(b); err != nil {
		t.Fatalf("Merge: %v", err)
	}

	if a.Count() != 1000 {
		t.Errorf("merged Count() = %d; want 1000", a.Count())
	}
	p50 := a.ValueAtPercentile(50)
	if !withinPct(p50, 500, 1.0) {
		t.Errorf("merged p50 = %d; want ~500", p50)
	}
}

func TestMerge_RejectsIncompatibleConfig(t *testing.T) {
	a := hdr.Default()
	b, _ := hdr.New(hdr.Config{
		LowestDiscernibleValue: 1,
		HighestTrackableValue:  1_000,
		SignificantFigures:     2,
	})
	if err := a.Merge(b); err == nil {
		t.Error("Merge with incompatible config: expected error, got nil")
	}
}

// ── Snapshot ──────────────────────────────────────────────────────────────────

func TestSnapshot_PercentilesConsistent(t *testing.T) {
	h := hdr.Default()
	for i := int64(1); i <= 10_000; i++ {
		h.Record(i)
	}
	snap := h.Snapshot()

	// Percentiles must be non-decreasing.
	order := []int64{snap.P50, snap.P90, snap.P95, snap.P99, snap.P999}
	for i := 1; i < len(order); i++ {
		if order[i] < order[i-1] {
			t.Errorf("percentile order violated: %v", order)
		}
	}

	// Spot-check against direct ValueAtPercentile calls.
	if !withinPct(snap.P99, h.ValueAtPercentile(99), 0.01) {
		t.Errorf("Snapshot.P99 (%d) differs from ValueAtPercentile(99) (%d)",
			snap.P99, h.ValueAtPercentile(99))
	}
	if snap.Count != h.Count() {
		t.Errorf("Snapshot.Count = %d; want %d", snap.Count, h.Count())
	}
}

func TestSnapshot_MeanMatchesMean(t *testing.T) {
	h := hdr.Default()
	for _, v := range []int64{100, 200, 300} {
		h.Record(v)
	}
	snap := h.Snapshot()
	if math.Abs(snap.Mean-h.Mean()) > 0.001 {
		t.Errorf("Snapshot.Mean = %.4f; h.Mean() = %.4f", snap.Mean, h.Mean())
	}
}

// ── Concurrent safety ─────────────────────────────────────────────────────────

// TestConcurrentRecord runs N goroutines each recording M values.
// With -race this detects data races; the final Count must be exact.
func TestConcurrentRecord(t *testing.T) {
	h := hdr.Default()
	const goroutines = 50
	const perGoroutine = 1_000

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(start int64) {
			defer wg.Done()
			for i := start; i < start+perGoroutine; i++ {
				h.Record(i % 1_000_000)
			}
		}(int64(g * perGoroutine))
	}
	wg.Wait()

	want := int64(goroutines * perGoroutine)
	if h.Count() != want {
		t.Errorf("Count() = %d; want %d", h.Count(), want)
	}
}

// TestConcurrentRecordAndSnapshot checks that Snapshot can be called
// concurrently with Record without panicking or deadlocking.
func TestConcurrentRecordAndSnapshot(t *testing.T) {
	h := hdr.Default()
	done := make(chan struct{})

	go func() {
		defer close(done)
		for i := int64(0); i < 10_000; i++ {
			h.Record(i)
		}
	}()

	for {
		select {
		case <-done:
			return
		default:
			_ = h.Snapshot()
		}
	}
}

// ── Gateway-realistic scenario ────────────────────────────────────────────────

// TestGatewayLatencyScenario simulates recording LLM request latencies
// (cache hits vs. generation) and verifies that the bimodal distribution
// is reflected correctly in the snapshot.
func TestGatewayLatencyScenario(t *testing.T) {
	h := hdr.Default()

	// 90% cache hits: ~5 ms (5_000 µs)
	for i := 0; i < 900; i++ {
		h.RecordN(5_000, 1)
	}
	// 10% generation requests: ~3 s (3_000_000 µs)
	for i := 0; i < 100; i++ {
		h.RecordN(3_000_000, 1)
	}

	snap := h.Snapshot()

	// p50 should be near the cache-hit latency.
	if !withinPct(snap.P50, 5_000, 1.0) {
		t.Errorf("p50 = %d µs; want ~5000 µs (cache hit)", snap.P50)
	}
	// p99 should be near the generation latency.
	if !withinPct(snap.P99, 3_000_000, 1.0) {
		t.Errorf("p99 = %d µs; want ~3_000_000 µs (generation)", snap.P99)
	}
	if snap.Count != 1000 {
		t.Errorf("Count = %d; want 1000", snap.Count)
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// withinPct reports whether got is within pct% of want.
func withinPct(got, want int64, pct float64) bool {
	if want == 0 {
		return got == 0
	}
	return relativeErrorPct(got, want) <= pct
}

func relativeErrorPct(got, want int64) float64 {
	return math.Abs(float64(got)-float64(want)) / float64(want) * 100
}
