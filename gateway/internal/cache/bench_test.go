package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/domain"
)

// ─── Fixtures ─────────────────────────────────────────────────────────────────

// benchSize parameterises one sub-benchmark run.
type benchSize struct {
	name       string
	numChunks  int
	chunkBytes int
}

// benchSizes covers four realistic entry profiles:
//
//	tiny   — single-token or one-liner response (~60 B raw chunks)
//	small  — sentence-length response (~600 B)
//	medium — paragraph response (~3.2 KB)
//	large  — long-form response (~16 KB)
var benchSizes = []benchSize{
	{"1chunk_tiny", 1, 60},
	{"5chunk_small", 5, 120},
	{"20chunk_medium", 20, 160},
	{"100chunk_large", 100, 160},
}

// makeBenchEntry builds an Entry with realistic pre-marshaled SSE frames.
// Each chunk looks like an OpenAI-shaped content_block_delta event, which
// is what the tee in cache/tee.go actually captures.
func makeBenchEntry(numChunks, chunkBytes int) *Entry {
	chunks := make([][]byte, numChunks)
	for i := range chunks {
		// 88 bytes of fixed JSON scaffolding; remainder is text payload.
		text := strings.Repeat("x", max(1, chunkBytes-88))
		chunks[i] = []byte(fmt.Sprintf(
			`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":%q}}`,
			text,
		))
	}
	return &Entry{
		Provider:  domain.ProviderAnthropic,
		Model:     "claude-3-5-sonnet-20241022",
		Chunks:    chunks,
		CreatedAt: time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC),
	}
}

// benchKey returns a 64-character cache key (same width as a SHA-256 hex
// digest produced by DeriveKey) with a numeric suffix for uniqueness.
func benchKey(i int) Key {
	return Key(fmt.Sprintf("benchkey%056d", i)) // 8 + 56 = 64 chars
}

const benchHitKey Key = "benchhitkey000000000000000000000000000000000000000000000000000000" // 64 chars

// preMarshal returns the JSON encoding of e, panicking on failure.
func preMarshal(e *Entry) []byte {
	data, err := json.Marshal(e)
	if err != nil {
		panic(err)
	}
	return data
}

// ─── Group 1: Pure serialization — no cache layer ─────────────────────────────
//
// These are the baseline numbers. Every LSMCache.Put pays json.Marshal;
// every LSMCache.Get hit pays json.Unmarshal. Comparing Group 3 against
// Group 1 isolates exactly what the MemTable layer adds on top.

// BenchmarkMarshal measures json.Marshal for Entry structs of varying sizes.
// b.SetBytes is the raw chunk total (entry.Bytes()), so MB/s reflects
// throughput of the actual payload — before base64 expansion.
func BenchmarkMarshal(b *testing.B) {
	for _, sz := range benchSizes {
		sz := sz
		b.Run(sz.name, func(b *testing.B) {
			e := makeBenchEntry(sz.numChunks, sz.chunkBytes)
			b.SetBytes(e.Bytes())
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				data, _ := json.Marshal(e)
				_ = data
			}
		})
	}
}

// BenchmarkUnmarshal measures json.Unmarshal cost — the deserialization step
// on every LSMCache.Get hit. b.SetBytes is the JSON wire size (after base64
// expansion), so MB/s reflects true I/O throughput from the LSM layer.
func BenchmarkUnmarshal(b *testing.B) {
	for _, sz := range benchSizes {
		sz := sz
		b.Run(sz.name, func(b *testing.B) {
			e := makeBenchEntry(sz.numChunks, sz.chunkBytes)
			data := preMarshal(e)
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				var out Entry
				_ = json.Unmarshal(data, &out)
			}
		})
	}
}

// BenchmarkMarshalUnmarshalRoundtrip measures the combined Put+Get
// serialization cost with no storage layer. Subtract this from
// BenchmarkLSMCachePut + BenchmarkLSMCacheGet to get pure MemTable overhead.
func BenchmarkMarshalUnmarshalRoundtrip(b *testing.B) {
	for _, sz := range benchSizes {
		sz := sz
		b.Run(sz.name, func(b *testing.B) {
			e := makeBenchEntry(sz.numChunks, sz.chunkBytes)
			b.SetBytes(e.Bytes())
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				data, _ := json.Marshal(e)
				var out Entry
				_ = json.Unmarshal(data, &out)
			}
		})
	}
}

// ─── Group 2: LSMCache serialization overhead in isolation ────────────────────
//
// These benchmarks replicate the exact work LSMCache.Put does before calling
// engine.Put — Bytes() budget check, json.Marshal, and key cast — without
// touching the storage layer. The difference between BenchmarkMarshal and
// BenchmarkPutSerialization is the cost of the Bytes() loop and key encoding.

// BenchmarkPutSerialization benchmarks every serialization step inside
// LSMCache.Put except the engine write: entry.Bytes() + json.Marshal +
// []byte(key) cast. Subtract BenchmarkMarshal to isolate the Bytes() overhead.
func BenchmarkPutSerialization(b *testing.B) {
	for _, sz := range benchSizes {
		sz := sz
		b.Run(sz.name, func(b *testing.B) {
			e := makeBenchEntry(sz.numChunks, sz.chunkBytes)
			b.SetBytes(e.Bytes())
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_ = e.Bytes()
				data, _ := json.Marshal(e)
				lsmKey := []byte(benchHitKey)
				_, _ = data, lsmKey
			}
		})
	}
}

// BenchmarkBytesCheck benchmarks entry.Bytes() alone — the pre-marshal budget
// guard that iterates every chunk header. This is O(numChunks), not O(payload).
func BenchmarkBytesCheck(b *testing.B) {
	for _, sz := range benchSizes {
		sz := sz
		b.Run(sz.name, func(b *testing.B) {
			e := makeBenchEntry(sz.numChunks, sz.chunkBytes)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_ = e.Bytes()
			}
		})
	}
}

// ─── Group 3: Full cache path — serialization + MemTable ──────────────────────
//
// These are the end-to-end numbers. Subtract Group 1 from Group 3 to get the
// pure MemTable skip-list overhead. For Put, unique keys are used to exercise
// the new-node insertion path (not update-in-place), matching production usage
// where each unique request maps to its own SHA-256 key.

// BenchmarkLSMCachePut measures the full LSMCache.Put path: Bytes() + Marshal +
// MemTable skip-list insert. Each iteration writes a unique key to exercise the
// new-node path. The MemTable may flush to disk at high b.N for large entries;
// this reflects the real amortised production cost.
func BenchmarkLSMCachePut(b *testing.B) {
	for _, sz := range benchSizes {
		sz := sz
		b.Run(sz.name, func(b *testing.B) {
			dir := b.TempDir()
			c, err := NewLSMCache(dir, 0) // 0 = no per-entry size cap
			if err != nil {
				b.Fatalf("NewLSMCache: %v", err)
			}
			defer c.Close()
			e := makeBenchEntry(sz.numChunks, sz.chunkBytes)
			ctx := context.Background()
			b.SetBytes(e.Bytes())
			b.ReportAllocs()
			b.ResetTimer()
			n := 0
			for b.Loop() {
				n++
				_ = c.Put(ctx, benchKey(n), e)
			}
		})
	}
}

// BenchmarkLSMCacheGet measures the full LSMCache.Get path: MemTable lookup +
// Unmarshal. The key is preloaded before timing to benchmark hot-path reads
// (guaranteed MemTable hit, no SSTable disk I/O).
func BenchmarkLSMCacheGet(b *testing.B) {
	for _, sz := range benchSizes {
		sz := sz
		b.Run(sz.name, func(b *testing.B) {
			dir := b.TempDir()
			c, err := NewLSMCache(dir, 0)
			if err != nil {
				b.Fatalf("NewLSMCache: %v", err)
			}
			defer c.Close()
			e := makeBenchEntry(sz.numChunks, sz.chunkBytes)
			ctx := context.Background()
			if err := c.Put(ctx, benchHitKey, e); err != nil {
				b.Fatalf("pre-load Put: %v", err)
			}
			b.SetBytes(e.Bytes())
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_, _, _ = c.Get(ctx, benchHitKey)
			}
		})
	}
}

// ─── Group 4: Serialized-size reporting ───────────────────────────────────────
//
// BenchmarkSerializedExpansion reports the divergence between what entry.Bytes()
// measures (raw chunk bytes, what the budget cap enforces) and what json.Marshal
// actually writes into the LSM (base64-encoded chunks + JSON framing).
//
// Because [][]byte is base64-encoded by encoding/json, every Put stores
// more bytes than the configured maxEntryBytes cap enforces. This benchmark
// makes that gap visible as custom metrics (raw_B, json_B, overhead_B,
// expansion_pct) alongside ns/op.
//
// Note: uses the traditional b.N loop (not b.Loop) so that b.ReportMetric
// calls survive across calibration passes in Go 1.24+.
func BenchmarkSerializedExpansion(b *testing.B) {
	for _, sz := range benchSizes {
		sz := sz
		b.Run(sz.name, func(b *testing.B) {
			e := makeBenchEntry(sz.numChunks, sz.chunkBytes)
			data := preMarshal(e)
			rawBytes := e.Bytes()
			jsonBytes := int64(len(data))

			b.SetBytes(rawBytes)
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				d, _ := json.Marshal(e)
				_ = d
			}

			// Report after the loop so metrics survive b.Loop calibration passes.
			b.ReportMetric(float64(rawBytes), "raw_B")
			b.ReportMetric(float64(jsonBytes), "json_B")
			b.ReportMetric(float64(jsonBytes-rawBytes), "overhead_B")
			if rawBytes > 0 {
				b.ReportMetric(float64(jsonBytes)/float64(rawBytes)*100, "expansion_pct")
			}
		})
	}
}
