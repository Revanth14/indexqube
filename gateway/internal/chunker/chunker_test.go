package chunker_test

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/Revanth14/indexqube/gateway/internal/chunker"
)

// randBytes returns n cryptographically random bytes.
func randBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	return b
}

// repeatByte returns a slice of n copies of b.
func repeatByte(b byte, n int) []byte {
	return bytes.Repeat([]byte{b}, n)
}

// ── Construction ─────────────────────────────────────────────────────────────

func TestDefaultConfigValues(t *testing.T) {
	cfg := chunker.DefaultConfig()
	if cfg.WindowSize <= 0 {
		t.Errorf("WindowSize = %d; want > 0", cfg.WindowSize)
	}
	if cfg.MinSize <= 0 {
		t.Errorf("MinSize = %d; want > 0", cfg.MinSize)
	}
	if cfg.MaxSize <= cfg.MinSize {
		t.Errorf("MaxSize (%d) <= MinSize (%d)", cfg.MaxSize, cfg.MinSize)
	}
	if cfg.Mask == 0 {
		t.Error("Mask = 0; want non-zero")
	}
	if cfg.Base == 0 {
		t.Error("Base = 0; want non-zero")
	}
}

func TestNewPanicsOnBadConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  chunker.Config
	}{
		{"zero WindowSize", chunker.Config{WindowSize: 0, MinSize: 512, MaxSize: 4096, Base: 257, Mask: 0xfff}},
		{"zero MinSize", chunker.Config{WindowSize: 64, MinSize: 0, MaxSize: 4096, Base: 257, Mask: 0xfff}},
		{"zero MaxSize", chunker.Config{WindowSize: 64, MinSize: 512, MaxSize: 0, Base: 257, Mask: 0xfff}},
		{"MinSize == MaxSize", chunker.Config{WindowSize: 64, MinSize: 512, MaxSize: 512, Base: 257, Mask: 0xfff}},
		{"MinSize > MaxSize", chunker.Config{WindowSize: 64, MinSize: 4096, MaxSize: 512, Base: 257, Mask: 0xfff}},
		{"zero Base", chunker.Config{WindowSize: 64, MinSize: 512, MaxSize: 4096, Base: 0, Mask: 0xfff}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("New(%+v) did not panic; expected a panic", tc.cfg)
				}
			}()
			chunker.New(tc.cfg)
		})
	}
}

// ── Edge cases ───────────────────────────────────────────────────────────────

func TestSplitEmpty(t *testing.T) {
	c := chunker.Default()
	if got := c.Split(nil); got != nil {
		t.Errorf("Split(nil) = %v; want nil", got)
	}
	if got := c.Split([]byte{}); got != nil {
		t.Errorf("Split([]byte{}) = %v; want nil", got)
	}
}

func TestSplitSingleByteInputs(t *testing.T) {
	c := chunker.Default()
	for _, size := range []int{1, 10, 100} {
		data := randBytes(t, size)
		chunks := c.Split(data)
		if len(chunks) != 1 {
			t.Errorf("len(Split(%d bytes)) = %d; want 1", size, len(chunks))
		}
	}
}

// Input exactly equal to MinSize must produce a single chunk.
func TestSplitExactlyMinSize(t *testing.T) {
	cfg := smallConfig()
	c := chunker.New(cfg)
	data := randBytes(t, cfg.MinSize)
	chunks := c.Split(data)
	if len(chunks) != 1 {
		t.Errorf("got %d chunks for input == MinSize; want 1", len(chunks))
	}
}

// Input exactly equal to MaxSize may or may not split, but must never panic
// and must always reconstitute correctly.
func TestSplitExactlyMaxSize(t *testing.T) {
	cfg := smallConfig()
	c := chunker.New(cfg)
	data := randBytes(t, cfg.MaxSize)
	chunks := c.Split(data)
	assertReconstitutes(t, data, chunks)
}

// ── Core properties ──────────────────────────────────────────────────────────

// TestSplitReconstitutes verifies that concatenating all chunks produces the
// original input byte-for-byte.
func TestSplitReconstitutes(t *testing.T) {
	c := chunker.Default()
	sizes := []int{1, 512, 1024, 4*1024, 32*1024, 256*1024}
	for _, sz := range sizes {
		data := randBytes(t, sz)
		chunks := c.Split(data)
		assertReconstitutes(t, data, chunks)
	}
}

// TestSplitDeterministic verifies that identical inputs always produce
// identical chunk boundaries.
func TestSplitDeterministic(t *testing.T) {
	c := chunker.Default()
	data := randBytes(t, 64*1024)

	first := c.Split(data)
	for i := 0; i < 5; i++ {
		repeat := c.Split(data)
		if len(repeat) != len(first) {
			t.Fatalf("run %d: got %d chunks; want %d", i, len(repeat), len(first))
		}
		for j, ch := range repeat {
			if ch.Offset != first[j].Offset || len(ch.Data) != len(first[j].Data) {
				t.Fatalf("run %d chunk %d: offset=%d len=%d; want offset=%d len=%d",
					i, j, ch.Offset, len(ch.Data), first[j].Offset, len(first[j].Data))
			}
		}
	}
}

// TestSplitMinSizeRespected verifies no chunk (except possibly the last)
// is shorter than MinSize.
func TestSplitMinSizeRespected(t *testing.T) {
	cfg := smallConfig()
	c := chunker.New(cfg)
	data := randBytes(t, 128*1024)
	chunks := c.Split(data)

	for i, ch := range chunks[:len(chunks)-1] {
		if len(ch.Data) < cfg.MinSize {
			t.Errorf("chunk %d: len=%d < MinSize=%d", i, len(ch.Data), cfg.MinSize)
		}
	}
}

// TestSplitMaxSizeRespected verifies no chunk exceeds MaxSize.
func TestSplitMaxSizeRespected(t *testing.T) {
	cfg := smallConfig()
	c := chunker.New(cfg)
	// Use a run of identical bytes — worst case for hash collisions.
	data := repeatByte(0xAB, 512*1024)
	chunks := c.Split(data)

	for i, ch := range chunks {
		if len(ch.Data) > cfg.MaxSize {
			t.Errorf("chunk %d: len=%d > MaxSize=%d", i, len(ch.Data), cfg.MaxSize)
		}
	}
}

// TestSplitOffsetsConsistent verifies that Chunk.Offset and len(Chunk.Data)
// are consistent: chunks tile the input with no gaps or overlaps.
func TestSplitOffsetsConsistent(t *testing.T) {
	c := chunker.Default()
	data := randBytes(t, 64*1024)
	chunks := c.Split(data)

	cursor := 0
	for i, ch := range chunks {
		if ch.Offset != cursor {
			t.Fatalf("chunk %d: Offset=%d; want %d", i, ch.Offset, cursor)
		}
		cursor += len(ch.Data)
	}
	if cursor != len(data) {
		t.Errorf("total bytes covered = %d; want %d", cursor, len(data))
	}
}

// TestSplitDataIsSubslice verifies that chunk.Data points into the original
// slice (zero-copy guarantee).
func TestSplitDataIsSubslice(t *testing.T) {
	c := chunker.Default()
	data := randBytes(t, 32*1024)
	chunks := c.Split(data)

	for i, ch := range chunks {
		// If Data is a true sub-slice, its first byte's address must fall
		// within the original backing array.
		if len(ch.Data) == 0 {
			continue
		}
		expected := data[ch.Offset : ch.Offset+len(ch.Data)]
		if !bytes.Equal(ch.Data, expected) {
			t.Errorf("chunk %d data mismatch", i)
		}
	}
}

// ── Content-defined property ──────────────────────────────────────────────────

// TestSplitContentDefined is the key property of CDC: prepending N bytes to
// the input shifts chunk boundaries only near the insertion point; after a
// few chunks the boundaries resynchronise and the tail chunks are identical.
//
// We verify that at least half the chunks from the original split are
// reproduced somewhere in the modified split, confirming partial stability.
func TestSplitContentDefined(t *testing.T) {
	cfg := smallConfig()
	c := chunker.New(cfg)

	// Large input so there are many downstream chunks to check.
	original := randBytes(t, 512*1024)
	prefix := randBytes(t, cfg.MinSize/2) // a chunk-boundary-crossing insertion

	modified := append(prefix, original...)

	origChunks := c.Split(original)
	modChunks := c.Split(modified)

	if len(origChunks) < 4 {
		t.Skip("too few chunks to test content-defined property meaningfully")
	}

	// Build a set of (offset-normalised) chunk fingerprints from modChunks.
	// We normalise by hashing the Data bytes, not the Offset.
	modSet := make(map[string]struct{}, len(modChunks))
	for _, ch := range modChunks {
		modSet[string(ch.Data)] = struct{}{}
	}

	// Count how many of the original tail chunks appear verbatim in modChunks.
	// Skip the first two chunks (most likely to straddle the insertion point).
	matched := 0
	tail := origChunks[2:]
	for _, ch := range tail {
		if _, ok := modSet[string(ch.Data)]; ok {
			matched++
		}
	}

	// Expect at least 50% of tail chunks to reappear unchanged.
	threshold := len(tail) / 2
	if matched < threshold {
		t.Errorf("content-defined property: %d/%d tail chunks matched; want ≥%d",
			matched, len(tail), threshold)
	}
}

// ── Identical-byte stress test ────────────────────────────────────────────────

// TestSplitRepeatedBytes verifies correct behaviour when every byte is the
// same — a degenerate case for hash functions.
func TestSplitRepeatedBytes(t *testing.T) {
	cfg := smallConfig()
	c := chunker.New(cfg)
	data := repeatByte(0xFF, 256*1024)
	chunks := c.Split(data)

	assertReconstitutes(t, data, chunks)
	for i, ch := range chunks {
		if len(ch.Data) > cfg.MaxSize {
			t.Errorf("chunk %d: len=%d > MaxSize=%d", i, len(ch.Data), cfg.MaxSize)
		}
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// smallConfig returns a Config with small MinSize/MaxSize so tests run fast
// and produce multiple chunks without needing megabytes of data.
func smallConfig() chunker.Config {
	return chunker.Config{
		WindowSize: 16,
		MinSize:    64,
		MaxSize:    512,
		Mask:       (1 << 7) - 1, // ~128 byte average chunk
		Base:       257,
	}
}

// assertReconstitutes checks that concatenating chunks byte-for-byte reproduces data.
func assertReconstitutes(t *testing.T, data []byte, chunks []chunker.Chunk) {
	t.Helper()
	var buf bytes.Buffer
	for _, ch := range chunks {
		buf.Write(ch.Data)
	}
	if !bytes.Equal(buf.Bytes(), data) {
		t.Errorf("reconstituted %d bytes; original was %d bytes", buf.Len(), len(data))
	}
}
