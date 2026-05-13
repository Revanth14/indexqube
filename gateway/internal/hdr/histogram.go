// Package hdr implements a High Dynamic Range (HDR) Histogram from scratch.
//
// # Problem
//
// Tracking LLM request latencies is hard with ordinary fixed-bucket histograms:
// a cached hit takes ~1 ms while a streaming generation can take 5 minutes —
// seven orders of magnitude. A histogram with enough buckets to cover that range
// at fine granularity wastes megabytes of memory; one with coarse buckets loses
// the tail-latency signal you care most about.
//
// # Solution
//
// The HDR Histogram (Gil Tene, Azul Systems — http://hdrhistogram.org) maintains
// configurable significant-digit precision across the entire dynamic range.
// Recording and querying are O(1) and O(buckets) respectively, and memory usage
// is O(log(max/min) × precision) — typically 25–200 KiB.
//
// # Data structure
//
// Values are stored in a flat counts array that conceptually forms a 2-D grid:
//
//	 Bucket 0 │ sub-bucket [0 … S)   covers values [0 … S)   — resolution 1
//	 Bucket 1 │ sub-bucket [S/2 … S) covers values [S … 2S)  — resolution 2
//	 Bucket 2 │ sub-bucket [S/2 … S) covers values [2S … 4S) — resolution 4
//	 …
//
// where S = subBucketCount (a power of two derived from SignificantFigures).
// Within each bucket the sub-bucket index encodes the top significant digits of
// the value; the bucket index encodes the magnitude.  The index computation uses
// math/bits.LeadingZeros64 — a single instruction on modern CPUs.
//
// # Equivalence ranges
//
// Two values share a bucket when they differ by less than the resolution at
// their magnitude.  For example, with 2 significant figures (S = 256):
//   - Values 1–255  each occupy a unique bucket.  Resolution = 1.
//   - Values 256–511 are grouped into 128 buckets, two values each.  Resolution = 2.
//   - Values 512–1023 → 128 buckets, four values each.  Resolution = 4.
//
// The relative error is always ≤ 1/(S/2) ≈ 0.78 % for sigfigs = 2.
package hdr

import (
	"errors"
	"fmt"
	"math"
	"math/bits"
	"sync"
)

// Defaults tuned for tracking LLM gateway latency in microseconds.
const (
	// DefaultLowestDiscernibleValue is 1 µs — values below this are clamped up.
	DefaultLowestDiscernibleValue int64 = 1
	// DefaultHighestTrackableValue is 1 hour expressed in microseconds.
	DefaultHighestTrackableValue int64 = 3_600_000_000
	// DefaultSignificantFigures gives ~0.78 % relative error throughout the range.
	DefaultSignificantFigures int = 2
)

// Config holds the HDR Histogram construction parameters.
// All three fields must be set; use DefaultConfig() for sane starting values.
type Config struct {
	// LowestDiscernibleValue is the smallest unit of resolution (≥ 1).
	// Values smaller than this are recorded as this value.
	LowestDiscernibleValue int64

	// HighestTrackableValue is the maximum value tracked with full precision.
	// Must be ≥ 2 × LowestDiscernibleValue.
	// Values above this are clamped to this before recording.
	HighestTrackableValue int64

	// SignificantFigures controls decimal precision [1, 5].
	// 2 → ~25 KiB, 3 → ~200 KiB, 4 → ~1.6 MiB.
	SignificantFigures int
}

// DefaultConfig returns a Config suited for tracking LLM request latency
// in microseconds with 2 significant figures of precision.
func DefaultConfig() Config {
	return Config{
		LowestDiscernibleValue: DefaultLowestDiscernibleValue,
		HighestTrackableValue:  DefaultHighestTrackableValue,
		SignificantFigures:     DefaultSignificantFigures,
	}
}

// Snapshot is an immutable point-in-time summary exported by Histogram.Snapshot.
// All latency values use the same unit as the recorded values (e.g., µs).
type Snapshot struct {
	Count int64
	Min   int64
	Max   int64
	Mean  float64
	P50   int64
	P90   int64
	P95   int64
	P99   int64
	P999          int64 // 99.9th percentile
	OverflowCount int64 // values clamped above HighestTrackableValue
}

// Histogram is an HDR Histogram safe for concurrent use.
// Construct with New or Default; the zero value must not be used.
type Histogram struct {
	// Immutable geometry — computed once in New.
	cfg                         Config
	unitMagnitude               int   // floor(log2(LowestDiscernibleValue))
	subBucketHalfCountMagnitude int   // controls sub-bucket count
	subBucketCount              int   // S: total sub-buckets per power-of-2 range
	subBucketHalfCount          int   // S/2: "new" sub-buckets added per range
	subBucketMask               int64 // (S-1) << unitMagnitude
	bucketCount                 int   // number of power-of-2 ranges needed

	// Mutable state — all access serialised through mu.
	mu            sync.Mutex
	counts        []int64 // flat counts array; length = (bucketCount+1)*subBucketHalfCount
	totalCount    int64
	overflowCount int64 // values clamped above HighestTrackableValue
	minValue      int64 // MaxInt64 when empty
	maxValue      int64 // 0 when empty
}

// New constructs a Histogram from cfg.
// Returns an error if the configuration is invalid.
func New(cfg Config) (*Histogram, error) {
	if cfg.LowestDiscernibleValue < 1 {
		return nil, errors.New("hdr: LowestDiscernibleValue must be ≥ 1")
	}
	if cfg.HighestTrackableValue < 2*cfg.LowestDiscernibleValue {
		return nil, fmt.Errorf("hdr: HighestTrackableValue (%d) must be ≥ 2×LowestDiscernibleValue (%d)",
			cfg.HighestTrackableValue, cfg.LowestDiscernibleValue)
	}
	if cfg.SignificantFigures < 1 || cfg.SignificantFigures > 5 {
		return nil, fmt.Errorf("hdr: SignificantFigures must be in [1, 5], got %d", cfg.SignificantFigures)
	}

	// ── Derive sub-bucket geometry ──────────────────────────────────────────
	//
	// We need enough sub-buckets so that the two values nearest to any power-of-2
	// boundary differ by less than one unit of relative precision.  Concretely:
	//
	//   largestSingleUnit = 2 × 10^sigfigs
	//   subBucketCountMagnitude = ⌈log₂(largestSingleUnit)⌉
	//
	// This ensures that within each power-of-2 bucket, the sub-bucket spacing
	// is ≤ 1/10^sigfigs of the bucket's lower bound.
	largestSingleUnit := 2 * int64(math.Pow10(cfg.SignificantFigures))
	subBucketCountMagnitude := int(math.Ceil(math.Log2(float64(largestSingleUnit))))
	subBucketCountMagnitude = max(subBucketCountMagnitude, 1)

	subBucketHalfCountMagnitude := subBucketCountMagnitude - 1
	subBucketCount := 1 << uint(subBucketHalfCountMagnitude+1)
	subBucketHalfCount := subBucketCount / 2

	unitMagnitude := int(math.Floor(math.Log2(float64(cfg.LowestDiscernibleValue))))
	unitMagnitude = max(unitMagnitude, 0)
	subBucketMask := int64(subBucketCount-1) << uint(unitMagnitude)

	bucketCount := requiredBuckets(cfg.HighestTrackableValue, subBucketCount, unitMagnitude)
	counts := make([]int64, (bucketCount+1)*subBucketHalfCount)

	return &Histogram{
		cfg:                         cfg,
		unitMagnitude:               unitMagnitude,
		subBucketHalfCountMagnitude: subBucketHalfCountMagnitude,
		subBucketCount:              subBucketCount,
		subBucketHalfCount:          subBucketHalfCount,
		subBucketMask:               subBucketMask,
		bucketCount:                 bucketCount,
		counts:                      counts,
		minValue:                    math.MaxInt64,
	}, nil
}

// Default returns a Histogram built with DefaultConfig.
// Panics on construction error (impossible with compile-time constants).
func Default() *Histogram {
	h, err := New(DefaultConfig())
	if err != nil {
		panic("hdr: default config is invalid: " + err.Error())
	}
	return h
}

// ── Recording ────────────────────────────────────────────────────────────────

// Record records a single observation of value.
// Values outside [0, HighestTrackableValue] are clamped to the nearest bound.
func (h *Histogram) Record(value int64) { h.RecordN(value, 1) }

// RecordN records n occurrences of value in a single lock acquisition.
func (h *Histogram) RecordN(value, n int64) {
	if value < 0 {
		value = 0
	}
	overflow := value > h.cfg.HighestTrackableValue
	if overflow {
		value = h.cfg.HighestTrackableValue
	}
	idx := h.countsIndexFor(value)

	h.mu.Lock()
	h.counts[idx] += n
	h.totalCount += n
	if overflow {
		h.overflowCount += n
	}
	if value < h.minValue {
		h.minValue = value
	}
	if value > h.maxValue {
		h.maxValue = value
	}
	h.mu.Unlock()
}

// ── Point queries ─────────────────────────────────────────────────────────────

// Count returns the total number of recorded values.
func (h *Histogram) Count() int64 {
	h.mu.Lock()
	c := h.totalCount
	h.mu.Unlock()
	return c
}

// Min returns the smallest recorded value, or 0 if the histogram is empty.
func (h *Histogram) Min() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.totalCount == 0 {
		return 0
	}
	return h.minValue
}

// Max returns the largest recorded value, or 0 if the histogram is empty.
func (h *Histogram) Max() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.maxValue
}

// OverflowCount returns the number of recorded values that exceeded
// HighestTrackableValue and were clamped.  A non-zero result is a signal to
// either raise HighestTrackableValue or investigate unexpectedly large latencies.
func (h *Histogram) OverflowCount() int64 {
	h.mu.Lock()
	c := h.overflowCount
	h.mu.Unlock()
	return c
}

// Mean returns the arithmetic mean of all recorded values, or 0 if empty.
// The mean is computed from the midpoint of each equivalence range to avoid
// systematic bias.
func (h *Histogram) Mean() float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.mean()
}

// mean is the lock-free inner implementation (caller must hold mu).
func (h *Histogram) mean() float64 {
	if h.totalCount == 0 {
		return 0
	}
	total := float64(0)
	for i, c := range h.counts {
		if c != 0 {
			total += float64(c) * float64(h.medianEquivalentValue(h.valueFromIndex(i)))
		}
	}
	return total / float64(h.totalCount)
}

// ValueAtPercentile returns the highest value v such that at least percentile %
// of all recorded values are ≤ v.  percentile is clamped to [0, 100].
func (h *Histogram) ValueAtPercentile(percentile float64) int64 {
	percentile = clamp(percentile, 0, 100)

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.totalCount == 0 {
		return 0
	}
	threshold := countAtPercentile(h.totalCount, percentile)
	running := int64(0)
	for i, c := range h.counts {
		running += c
		if running >= threshold {
			return h.highestEquivalentValue(h.valueFromIndex(i))
		}
	}
	return h.highestEquivalentValue(h.maxValue)
}

// PercentileOf returns the percentile [0, 100] at which value falls; i.e., the
// percentage of recorded values that are ≤ value.
// Returns 100 for an empty histogram.
func (h *Histogram) PercentileOf(value int64) float64 {
	if value < 0 {
		value = 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.totalCount == 0 {
		return 100
	}
	idx := h.countsIndexFor(value)
	if idx >= len(h.counts) {
		idx = len(h.counts) - 1
	}
	running := int64(0)
	for i := 0; i <= idx; i++ {
		running += h.counts[i]
	}
	return 100.0 * float64(running) / float64(h.totalCount)
}

// ── Bulk operations ───────────────────────────────────────────────────────────

// Reset clears all recorded values, returning the histogram to its empty state.
func (h *Histogram) Reset() {
	h.mu.Lock()
	for i := range h.counts {
		h.counts[i] = 0
	}
	h.totalCount = 0
	h.overflowCount = 0
	h.minValue = math.MaxInt64
	h.maxValue = 0
	h.mu.Unlock()
}

// Merge adds all counts from other into h.
// Both histograms must have been constructed with identical Config values.
// Returns an error if the configurations differ.
func (h *Histogram) Merge(other *Histogram) error {
	if h.cfg != other.cfg {
		return errors.New("hdr: cannot merge histograms with different configurations")
	}
	// Snapshot other under its own lock to avoid holding two locks simultaneously.
	other.mu.Lock()
	otherCounts := make([]int64, len(other.counts))
	copy(otherCounts, other.counts)
	otherTotal := other.totalCount
	otherOverflow := other.overflowCount
	otherMin := other.minValue
	otherMax := other.maxValue
	other.mu.Unlock()

	h.mu.Lock()
	for i, c := range otherCounts {
		h.counts[i] += c
	}
	h.totalCount += otherTotal
	h.overflowCount += otherOverflow
	if otherMin < h.minValue {
		h.minValue = otherMin
	}
	if otherMax > h.maxValue {
		h.maxValue = otherMax
	}
	h.mu.Unlock()
	return nil
}

// Snapshot returns an immutable summary of the histogram's current state.
// All five percentiles are computed in a single O(N) pass through the counts array.
func (h *Histogram) Snapshot() Snapshot {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.totalCount == 0 {
		return Snapshot{}
	}

	snap := Snapshot{
		Count:         h.totalCount,
		OverflowCount: h.overflowCount,
		Min:           h.minValue,
		Max:           h.maxValue,
		Mean:          h.mean(),
	}

	// Percentile thresholds in ascending order — filled in one pass.
	type target struct {
		threshold int64
		dest      *int64
	}
	targets := [5]target{
		{countAtPercentile(h.totalCount, 50), &snap.P50},
		{countAtPercentile(h.totalCount, 90), &snap.P90},
		{countAtPercentile(h.totalCount, 95), &snap.P95},
		{countAtPercentile(h.totalCount, 99), &snap.P99},
		{countAtPercentile(h.totalCount, 99.9), &snap.P999},
	}

	running := int64(0)
	next := 0 // index into targets
	for i, c := range h.counts {
		running += c
		for next < len(targets) && running >= targets[next].threshold {
			*targets[next].dest = h.highestEquivalentValue(h.valueFromIndex(i))
			next++
		}
		if next >= len(targets) {
			break
		}
	}
	return snap
}

// ── Index arithmetic ──────────────────────────────────────────────────────────

// getBucketIndex returns the power-of-2 bucket that value falls into.
// The computation uses a single LeadingZeros64 call — one CPU instruction.
func (h *Histogram) getBucketIndex(value int64) int {
	// OR with subBucketMask ensures we count enough bits for the minimum
	// representable value in this unit magnitude.
	pow2ceiling := 64 - bits.LeadingZeros64(uint64(value|h.subBucketMask))
	idx := pow2ceiling - h.unitMagnitude - (h.subBucketHalfCountMagnitude + 1)
	if idx < 0 {
		return 0
	}
	return idx
}

// getSubBucketIdx returns the sub-bucket index within the given bucket.
func (h *Histogram) getSubBucketIdx(value int64, bucketIdx int) int {
	shift := max(bucketIdx+h.unitMagnitude, 0)
	return int(value >> uint(shift))
}

// countsIndex converts a (bucket, sub-bucket) pair to a flat array index.
//
// Layout of the counts array:
//
//	Bucket 0:  indices [0,                   subBucketCount)
//	Bucket 1:  indices [subBucketHalfCount,  subBucketHalfCount*2)  ← new half only
//	Bucket 2:  indices [subBucketHalfCount*2, subBucketHalfCount*3) ← new half only
//	…
func (h *Histogram) countsIndex(bucketIdx, subBucketIdx int) int {
	return (bucketIdx+1)*h.subBucketHalfCount + subBucketIdx - h.subBucketHalfCount
}

// countsIndexFor returns the flat counts-array index for value.
func (h *Histogram) countsIndexFor(value int64) int {
	b := h.getBucketIndex(value)
	s := h.getSubBucketIdx(value, b)
	return h.countsIndex(b, s)
}

// ── Inverse mapping: index → value ───────────────────────────────────────────

// valueFromIndex converts a flat counts-array index back to the lowest value
// in that bucket's equivalence range.
func (h *Histogram) valueFromIndex(idx int) int64 {
	bucketIdx := (idx >> h.subBucketHalfCountMagnitude) - 1
	subBucketIdx := (idx & (h.subBucketHalfCount - 1)) + h.subBucketHalfCount
	if bucketIdx < 0 {
		subBucketIdx -= h.subBucketHalfCount
		bucketIdx = 0
	}
	return int64(subBucketIdx) << uint(bucketIdx+h.unitMagnitude)
}

// ── Equivalence-range helpers ─────────────────────────────────────────────────

// sizeOfEquivalentRange returns the width of the equivalence range containing value.
// All values in the range map to the same bucket and are indistinguishable.
func (h *Histogram) sizeOfEquivalentRange(value int64) int64 {
	b := h.getBucketIndex(value)
	shift := max(b+h.unitMagnitude, 0)
	return int64(1) << uint(shift)
}

// lowestEquivalentValue returns the smallest value that maps to the same bucket as value.
func (h *Histogram) lowestEquivalentValue(value int64) int64 {
	b := h.getBucketIndex(value)
	s := h.getSubBucketIdx(value, b)
	return int64(s) << uint(b+h.unitMagnitude)
}

// highestEquivalentValue returns the largest value that maps to the same bucket as value.
func (h *Histogram) highestEquivalentValue(value int64) int64 {
	return h.lowestEquivalentValue(value) + h.sizeOfEquivalentRange(value) - 1
}

// medianEquivalentValue returns the midpoint of value's equivalence range.
// Used for mean computation to avoid systematic rounding bias.
func (h *Histogram) medianEquivalentValue(value int64) int64 {
	return h.lowestEquivalentValue(value) + h.sizeOfEquivalentRange(value)/2
}

// ── Package-level helpers ─────────────────────────────────────────────────────

// requiredBuckets computes how many power-of-2 ranges are needed to cover highest.
func requiredBuckets(highest int64, subBucketCount, unitMagnitude int) int {
	// smallestUntrackable is the first value that falls outside bucket 0.
	smallestUntrackable := int64(subBucketCount) << uint(unitMagnitude)
	needed := 1
	for smallestUntrackable <= highest {
		if smallestUntrackable > math.MaxInt64/2 {
			return needed + 1
		}
		smallestUntrackable <<= 1
		needed++
	}
	return needed
}

// countAtPercentile converts a percentile to the cumulative count threshold.
func countAtPercentile(total int64, percentile float64) int64 {
	c := int64(math.Ceil((percentile / 100.0) * float64(total)))
	if c < 1 {
		return 1
	}
	return c
}

// clamp constrains v to [lo, hi].
func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
