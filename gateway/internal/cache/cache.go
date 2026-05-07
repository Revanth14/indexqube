// Package cache provides the gateway's response cache.
//
// V1 ships an in-memory LRU cache with byte-size cap and TTL eviction.
// The Bloom filter implementation in this package is built but NOT yet
// wired into the read path -- it exists for the upcoming Supabase L2
// tier where it serves as a cheap negative-lookup guard before each
// network round trip. Until L2 lands, ignore it.
//
// The cache stores ordered TokenWriter chunks (the OpenAI-shaped JSON
// frames the adapter emits). On a hit, those chunks are replayed
// straight to the client through the same TokenWriter, skipping the
// upstream provider entirely.
package cache

import (
	"context"
	"errors"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/domain"
)

// Key is the deterministic content-addressable cache key. It is a hex
// SHA-256 of (api_key || normalized_request_json), making it tenant-
// scoped: two requests with different credentials never share a slot,
// even when message content is identical.
type Key string

// Entry is the cached value backing a Key.
//
// Chunks are pre-marshaled OpenAI-shaped JSON frames (one per upstream
// content delta + a final finish-reason chunk). Each frame is what the
// adapter would have passed to TokenWriter.WriteData, captured by the
// tee in cache/tee.go.
//
// The proxy's [DONE] sentinel is NOT stored -- the proxy emits it
// after the governor returns clean, so cached replays inherit the
// same termination semantics for free.
type Entry struct {
	Provider  domain.Provider
	Model     string
	Chunks    [][]byte
	CreatedAt time.Time
}

// Bytes returns the total byte size of the captured chunks. Used by the
// MemoryCache to enforce maxBytes.
func (e *Entry) Bytes() int64 {
	var n int64
	for _, c := range e.Chunks {
		n += int64(len(c))
	}
	return n
}

// Replay writes every captured chunk through the supplied TokenWriter.
// Returns on the first write error (typically client disconnect); the
// caller decides how to surface it.
func (e *Entry) Replay(tw domain.TokenWriter) error {
	for _, c := range e.Chunks {
		if err := tw.WriteData(c); err != nil {
			return err
		}
	}
	return nil
}

// Cache is the storage contract used by the governor.
//
// Implementations MUST be safe for concurrent use. Get returns
// (entry, true, nil) on hit, (nil, false, nil) on miss, and
// (nil, false, err) on backend failure -- the governor treats errors
// as misses and continues to dispatch to the adapter.
type Cache interface {
	Get(ctx context.Context, key Key) (*Entry, bool, error)
	GetSemantic(ctx context.Context, tenantID string, embedding []float32, threshold float64) (*Entry, bool, error)
	Put(ctx context.Context, key Key, entry *Entry) error
	PutSemantic(ctx context.Context, tenantID string, key Key, entry *Entry, embedding []float32) error
}

// Stats is the introspectable state of a single cache layer.
type Stats struct {
	Entries int
	Bytes   int64
}

// ErrEntryTooLarge is returned by Put when the entry exceeds the
// configured per-entry size cap. It is non-fatal -- the governor will
// continue serving the live stream; the response simply isn't cached.
var ErrEntryTooLarge = errors.New("cache: entry too large")
