// Package chunker implements Rabin-Karp content-defined chunking (CDC).
//
// Content-defined chunking splits a byte stream at positions determined by
// the content itself rather than by fixed offsets. This means that inserting
// or removing bytes near the beginning of the stream shifts the boundary of
// the affected chunk but leaves most downstream chunks unchanged — the
// "sliding-window" property that makes CDC ideal for deduplication and
// semantic caching of LLM prompts.
//
// # Algorithm
//
// A polynomial rolling hash is maintained over a sliding window of W bytes:
//
//	H[l,r) = data[l]·B^(W-1) + data[l+1]·B^(W-2) + … + data[r-1]·B^0
//
// When the window advances by one position (removing data[l], adding data[r]):
//
//	H[l+1,r+1) = H[l,r) · B  −  data[l] · B^W  +  data[r]
//
// A chunk boundary is declared at byte i when H & Mask == 0.
// MinSize and MaxSize clamp the search range so that no chunk is
// pathologically small or large regardless of the data distribution.
package chunker

import (
	"bytes"
	"strings"
)

// goDeclarationPrefixes are the newline-prefixed tokens that open a top-level
// Go declaration. The leading '\n' ensures we only match at line starts.
var goDeclarationPrefixes = []string{"\nfunc ", "\ntype ", "\nconst ", "\nvar "}

// goDeclarationPrefixBytes mirrors goDeclarationPrefixes as []byte to allow
// allocation-free searching in hot paths.
var goDeclarationPrefixBytes = [][]byte{
	[]byte("\nfunc "), []byte("\ntype "), []byte("\nconst "), []byte("\nvar "),
}

// IsCodeContent returns true when data contains Go/C-family declaration keywords
// at a density that suggests source code rather than prose.
// Only the first 4 KiB is sampled so this runs in O(1) for large files.
func IsCodeContent(data []byte) bool {
	sample := data
	if len(sample) > 4096 {
		sample = sample[:4096]
	}
	text := string(sample)
	hits := 0
	for _, kw := range goDeclarationPrefixes {
		hits += strings.Count(text, kw)
	}
	hits += strings.Count(text, "\nfunction ") // JS/TS
	hits += strings.Count(text, "\nclass ")    // Python/Java/JS
	hits += strings.Count(text, "\ndef ")      // Python
	// Threshold: at least one declaration per 400 bytes → treat as code.
	return hits > 0 && len(sample)/hits <= 400
}

// AlignToDeclarationBoundary advances offset forward within data until it lands
// at the start of a top-level Go declaration line (func/type/const/var), or
// returns the original offset when none is found within maxAdvance bytes.
// The returned position skips the leading '\n' so it points at the first letter
// of the keyword ('f', 't', 'c', or 'v').
func AlignToDeclarationBoundary(data []byte, offset, maxAdvance int) int {
	end := offset + maxAdvance
	if end > len(data) {
		end = len(data)
	}
	region := string(data[offset:end])
	best := -1
	for _, prefix := range goDeclarationPrefixes {
		if idx := strings.Index(region, prefix); idx >= 0 && (best < 0 || idx < best) {
			best = idx
		}
	}
	if best < 0 {
		return offset
	}
	return offset + best + 1 // +1: skip the leading '\n'
}

// RetractToDeclarationBoundary searches backward from forced toward
// forced-retreatRadius for the last top-level Go declaration boundary.
// It is the inverse of AlignToDeclarationBoundary and is used to snap
// forced MaxSize cuts to stable semantic positions so that minor byte
// shifts between turns (e.g. a 21-byte header difference) do not produce
// different fingerprints for otherwise identical content.
//
// Uses []byte patterns to avoid heap allocations in the hot path.
// Returns forced unchanged when no boundary is found within retreatRadius.
func RetractToDeclarationBoundary(data []byte, forced, retreatRadius int) int {
	if retreatRadius <= 0 || forced <= 0 || forced > len(data) {
		return forced
	}
	start := forced - retreatRadius
	if start < 0 {
		start = 0
	}
	region := data[start:forced]
	best := -1
	for _, prefix := range goDeclarationPrefixBytes {
		idx := bytes.LastIndex(region, prefix)
		if idx >= 0 && idx > best {
			best = idx
		}
	}
	if best < 0 {
		return forced
	}
	return start + best + 1 // +1: skip the leading '\n'
}

// IsPathList returns true when data appears to be a file-path listing.
// Uses two heuristics: overall slash density (>3%) or high slash/dot line ratio (>60%).
// The lower threshold catches short listings like 15-path tool outputs.
func IsPathList(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	overallSlash := float64(bytes.Count(data, []byte("/"))) / float64(len(data))
	if overallSlash > 0.03 {
		return true
	}
	lines := bytes.Split(data, []byte("\n"))
	if len(lines) == 0 {
		return false
	}
	slashLines := 0
	for _, line := range lines {
		if bytes.Contains(line, []byte("/")) || bytes.Contains(line, []byte(".")) {
			slashLines++
		}
	}
	return float64(slashLines)/float64(len(lines)) > 0.60
}

// IsSystemProseContent returns true when data begins with an XML-like system tag
// (e.g. <system-reminder>, <claude_info>). These blocks use SystemProseConfig so
// a single changed field (e.g. currentDate) only invalidates its own small chunk.
func IsSystemProseContent(data []byte) bool {
	if len(data) < 16 {
		return false
	}
	sample := bytes.TrimSpace(data)
	if len(sample) > 64 {
		sample = sample[:64]
	}
	return bytes.HasPrefix(sample, []byte("<system-reminder>")) ||
		bytes.HasPrefix(sample, []byte("<claude_info>")) ||
		bytes.HasPrefix(sample, []byte("<system>")) ||
		bytes.HasPrefix(sample, []byte("<claude_code_"))
}

// PathListConfig returns a Config tuned for file-path listing content.
// The narrow 32-byte window and 64-byte MinSize produce fine-grained chunks
// that survive per-path-line edits without invalidating adjacent chunks.
func PathListConfig() Config {
	return Config{
		WindowSize: 32,
		MinSize:    64,
		MaxSize:    2048,
		Mask:       (1 << 10) - 1, // ~1 KiB average chunk size
		Base:       defaultBase,
	}
}

// CodeConfig returns a Config tuned for source-code content.
// The wider 256-byte window produces fewer, more stable chunks: edits within
// one function are less likely to shift boundaries in adjacent functions.
// MinSize of 2 KiB means content shorter than 4 KiB is returned as a single
// chunk (fast-path: len < 2×MinSize is handled by the caller).
func CodeConfig() Config {
	return Config{
		WindowSize: 256,
		MinSize:    2048,
		MaxSize:    16 * 1024,
		Mask:       (1 << 12) - 1, // ~4 KiB average, same as DefaultConfig
		Base:       defaultBase,
	}
}

// SystemProseConfig returns a Config tuned for large system-reminder/XML-tagged
// prose blocks. The 4 KiB MaxSize means a single changed field (e.g. currentDate)
// only invalidates its containing chunk, not the entire 126 KB block.
func SystemProseConfig() Config {
	return Config{
		WindowSize: 48,
		MinSize:    256,
		MaxSize:    4096,
		Mask:       0x1FFF, // ~8 KiB avg — forces most splits to MaxSize for stable boundaries
		Base:       defaultBase,
	}
}

// Default tuning constants. Mask gives ~4 KB average chunk size.
const (
	defaultBase       uint64 = 257
	defaultWindowSize int    = 64
	defaultMinSize    int    = 512
	defaultMaxSize    int    = 8 * 1024      // 8 KiB hard cap
	defaultMask       uint64 = (1 << 12) - 1 // boundary when lower 12 bits are zero → ~4 KiB avg
)

// Config holds the CDC tuning parameters.
// All size values are in bytes.
type Config struct {
	// WindowSize is the number of bytes covered by the rolling hash at any
	// given position. Larger windows reduce false positives from short
	// repeated sequences. Typical range: 32–128.
	WindowSize int

	// MinSize is the minimum chunk length. Boundaries are not considered
	// before this many bytes have been consumed. The very last chunk may
	// be shorter if the remaining data is exhausted.
	MinSize int

	// MaxSize is the hard upper bound on chunk length. A boundary is forced
	// here even if the hash condition is never satisfied.
	MaxSize int

	// Mask is the bit-mask used to detect boundaries: a cut is made when
	// (hash & Mask) == 0. The average chunk size is approximately
	// (MaxSize + MinSize) / 2 when Mask ≈ TargetSize - 1.
	// Use a value of the form (1<<k)−1 for a uniform distribution.
	Mask uint64

	// Base is the polynomial base for the rolling hash. A prime slightly
	// above 256 avoids degenerate collisions with byte values.
	Base uint64
}

// DefaultConfig returns a Config tuned for an average chunk size of ~4 KiB
// with a 64-byte rolling window, suitable for LLM prompt segmentation.
func DefaultConfig() Config {
	return Config{
		WindowSize: defaultWindowSize,
		MinSize:    defaultMinSize,
		MaxSize:    defaultMaxSize,
		Mask:       defaultMask,
		Base:       defaultBase,
	}
}

// Chunk is a contiguous, non-overlapping slice of the original input.
// Data is a sub-slice (not a copy) so allocations are O(number of chunks).
type Chunk struct {
	Data   []byte // sub-slice of the original input; do not modify
	Offset int    // byte offset of Data[0] within the original input
}

// Chunker splits byte slices using Rabin-Karp CDC.
// A single Chunker instance is safe for concurrent use (it is read-only after
// construction).
type Chunker struct {
	cfg Config
	pop uint64 // precomputed Base^WindowSize used to evict the outgoing byte
}

// New returns a Chunker configured with cfg.
// It panics if any of the size parameters are non-positive or if MinSize ≥ MaxSize.
func New(cfg Config) *Chunker {
	if cfg.WindowSize <= 0 {
		panic("chunker: WindowSize must be positive")
	}
	if cfg.MinSize <= 0 {
		panic("chunker: MinSize must be positive")
	}
	if cfg.MaxSize <= 0 {
		panic("chunker: MaxSize must be positive")
	}
	if cfg.MinSize >= cfg.MaxSize {
		panic("chunker: MinSize must be less than MaxSize")
	}
	if cfg.Base == 0 {
		panic("chunker: Base must be non-zero")
	}

	// Precompute Base^WindowSize (mod 2^64).
	// This is the coefficient we subtract when the oldest byte leaves the window.
	pop := uint64(1)
	for i := 0; i < cfg.WindowSize; i++ {
		pop *= cfg.Base
	}

	return &Chunker{cfg: cfg, pop: pop}
}

// Default returns a Chunker with DefaultConfig().
func Default() *Chunker {
	return New(DefaultConfig())
}

// Split partitions data into content-defined chunks.
//
// Properties guaranteed by this implementation:
//   - Completeness: the concatenation of all Chunk.Data equals data.
//   - Offset correctness: chunks[i].Offset + len(chunks[i].Data) == chunks[i+1].Offset.
//   - Min/Max bounds: len(chunk.Data) <= MaxSize for every chunk; only the
//     final chunk may be shorter than MinSize (when remaining bytes < MinSize).
//   - Determinism: identical inputs always produce identical chunk boundaries.
//
// Returns nil for empty input.
func (c *Chunker) Split(data []byte) []Chunk {
	if len(data) == 0 {
		return nil
	}
	// Pre-allocate enough capacity for the worst case (every chunk == MinSize).
	// This guarantees at most 1 allocation regardless of the actual chunk count,
	// at the cost of slightly over-allocating when chunks are large.
	estimated := len(data)/c.cfg.MinSize + 1
	return c.SplitInto(data, make([]Chunk, 0, estimated))
}

// SplitInto is the zero-allocation variant of Split for hot-path callers.
// It reuses dst as the backing slice, appending to dst[:0] each call, so a
// caller that pre-allocates dst once (e.g. via sync.Pool) incurs 0 heap
// allocations per split regardless of how many chunks are produced — as long
// as dst has sufficient capacity.
//
//	var pool = sync.Pool{New: func() any { return make([]chunker.Chunk, 0, 32) }}
//
//	dst := pool.Get().([]chunker.Chunk)
//	dst = c.SplitInto(data, dst[:0])
//	// … use dst …
//	pool.Put(dst)
func (c *Chunker) SplitInto(data []byte, dst []Chunk) []Chunk {
	if len(data) == 0 {
		return dst
	}
	dst = dst[:0]
	offset := 0
	for offset < len(data) {
		end := c.nextBoundary(data, offset)
		dst = append(dst, Chunk{
			Data:   data[offset:end],
			Offset: offset,
		})
		offset = end
	}
	return dst
}

// retractToBlankLine searches backward from forced for the last "\n\n"
// within retreatRadius bytes. Used as a fallback when no declaration boundary
// is found, to avoid splitting in the middle of a paragraph.
// Returns forced unchanged when no blank line is found.
func retractToBlankLine(data []byte, forced, retreatRadius int) int {
	if retreatRadius <= 0 || forced <= 0 || forced > len(data) {
		return forced
	}
	start := forced - retreatRadius
	if start < 0 {
		start = 0
	}
	region := data[start:forced]
	idx := bytes.LastIndex(region, []byte("\n\n"))
	if idx < 0 {
		return forced
	}
	return start + idx + 2 // position after the blank line
}
func (c *Chunker) nextBoundary(data []byte, start int) int {
	n := len(data)

	// Hard boundaries derived from the min/max config.
	minEnd := start + c.cfg.MinSize
	maxEnd := start + c.cfg.MaxSize

	maxEnd = min(maxEnd, n)
	// If fewer bytes remain than MinSize, emit the rest as a single short chunk.
	if minEnd >= n {
		return n
	}

	// ── Phase 1: fill the initial rolling-hash window ──────────────────────
	//
	// The window should cover the W bytes ending just before minEnd, i.e.,
	// [minEnd-W, minEnd). If that range extends before `start` (when
	// MinSize < WindowSize), clamp it to `start`.
	hash := uint64(0)
	winStart := max(minEnd-c.cfg.WindowSize, start)
	for i := winStart; i < minEnd; i++ {
		hash = hash*c.cfg.Base + uint64(data[i])
	}

	// ── Phase 2: slide the window, looking for a boundary ─────────────────
	//
	// At each step we advance the window by one byte:
	//   • incoming byte: data[i]
	//   • outgoing byte: data[i-WindowSize]  (if it is within the chunk)
	//
	// Update rule:  H_new = H_old · B − data[out] · B^W + data[in]
	for i := minEnd; i < maxEnd; i++ {
		out := i - c.cfg.WindowSize
		if out >= start {
			// Full window: evict the outgoing byte using the precomputed pop.
			hash = hash*c.cfg.Base + uint64(data[i]) - c.pop*uint64(data[out])
		} else {
			// Window is not yet fully formed (only when WindowSize > MinSize).
			hash = hash*c.cfg.Base + uint64(data[i])
		}

		if hash&c.cfg.Mask == 0 {
			return i + 1
		}
	}

	// No boundary found before MaxSize — snap to the nearest declaration
	// boundary before the hard cap. Lookback radius scales with span size:
	// large files (>32 KB) use 1024 bytes to reach declaration boundaries
	// that are spaced farther apart. Minor byte shifts between turns won't
	// change the boundary as long as the snapped position itself is stable.
	lookback := 512
	if len(data) > 32*1024 {
		lookback = 1024
	}
	snapped := RetractToDeclarationBoundary(data, maxEnd, lookback)
	// Secondary fallback: if no declaration keyword found, snap to the
	// nearest blank line within 256 bytes to avoid mid-paragraph splits.
	if snapped == maxEnd {
		snapped = retractToBlankLine(data, maxEnd, 256)
	}
	return snapped
}
