package chunker_test

import (
	"bytes"
	"testing"

	"github.com/Revanth14/indexqube/gateway/internal/chunker"
)

// pseudoRandData generates n bytes of deterministic pseudo-random data using
// an xorshift64 PRNG.  Determinism makes benchmark results reproducible across
// runs; the data is varied enough that the Rabin-Karp hash finds real boundaries.
func pseudoRandData(n int) []byte {
	data := make([]byte, n)
	x := uint64(0x123456789abcdef0)
	for i := range data {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		data[i] = byte(x)
	}
	return data
}

// ── Split (1 alloc: the result slice header) ──────────────────────────────────

func BenchmarkSplit_4KB(b *testing.B) {
	benchSplit(b, 4*1024)
}

func BenchmarkSplit_64KB(b *testing.B) {
	benchSplit(b, 64*1024)
}

func BenchmarkSplit_1MB(b *testing.B) {
	benchSplit(b, 1024*1024)
}

func BenchmarkSplit_8MB(b *testing.B) {
	benchSplit(b, 8*1024*1024)
}

// BenchmarkSplit_WorstCase uses a run of identical bytes, which is the
// degenerate case for content-defined chunking: the hash never hits a boundary
// naturally so every chunk is forced at MaxSize.
func BenchmarkSplit_WorstCase_1MB(b *testing.B) {
	data := bytes.Repeat([]byte{0x42}, 1024*1024)
	c := chunker.Default()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = c.Split(data)
	}
}

func benchSplit(b *testing.B, size int) {
	b.Helper()
	data := pseudoRandData(size)
	c := chunker.Default()
	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = c.Split(data)
	}
}

// ── SplitInto — zero allocations on the hot path ─────────────────────────────

func BenchmarkSplitInto_4KB(b *testing.B) {
	benchSplitInto(b, 4*1024)
}

func BenchmarkSplitInto_64KB(b *testing.B) {
	benchSplitInto(b, 64*1024)
}

func BenchmarkSplitInto_1MB(b *testing.B) {
	benchSplitInto(b, 1024*1024)
}

func BenchmarkSplitInto_8MB(b *testing.B) {
	benchSplitInto(b, 8*1024*1024)
}

func benchSplitInto(b *testing.B, size int) {
	b.Helper()
	data := pseudoRandData(size)
	c := chunker.Default()
	// Pre-warm: run once before ResetTimer so dst grows to its steady-state
	// capacity and subsequent iterations are truly 0-alloc.
	dst := make([]chunker.Chunk, 0, 16)
	dst = c.SplitInto(data, dst[:0])

	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		dst = c.SplitInto(data, dst[:0])
	}
}

// ── AllocsPerRun assertions (machine-verifiable 0-alloc guarantee) ────────────

// TestSplitIntoZeroAllocs asserts that SplitInto with a pre-warmed dst slice
// incurs zero heap allocations per call.
func TestSplitIntoZeroAllocs(t *testing.T) {
	c := chunker.Default()
	data := pseudoRandData(256 * 1024)

	// Warm up: let Split determine the typical chunk count so dst has capacity.
	initial := c.Split(data)
	dst := make([]chunker.Chunk, 0, len(initial)+4)

	allocs := testing.AllocsPerRun(20, func() {
		dst = c.SplitInto(data, dst[:0])
	})
	if allocs > 0 {
		t.Errorf("SplitInto: %.0f allocs/op; want 0 (pre-warmed dst must not grow)", allocs)
	}
	// Sanity: we still produced the right number of chunks.
	if len(dst) != len(initial) {
		t.Errorf("SplitInto produced %d chunks; Split produced %d", len(dst), len(initial))
	}
}

// TestSplitOneAllocPerCall asserts that Split itself makes exactly 1 allocation
// per call (the result-slice backing array), regardless of input size.
func TestSplitOneAllocPerCall(t *testing.T) {
	c := chunker.Default()
	for _, size := range []int{4 * 1024, 64 * 1024, 1024 * 1024} {
		data := pseudoRandData(size)
		allocs := testing.AllocsPerRun(10, func() {
			_ = c.Split(data)
		})
		if allocs > 1 {
			t.Errorf("Split(%d bytes): %.0f allocs/op; want ≤ 1", size, allocs)
		}
	}
}
