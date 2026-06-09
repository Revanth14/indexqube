package cache

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/store/lsm"
)

// LSMCache implements cache.Cache using a local pure-Go LSM storage engine.
// It persists cache items durably across process boundaries.
type LSMCache struct {
	engine        *lsm.Engine
	maxEntryBytes int64
	ttl           time.Duration
}

// NewLSMCache opens (or creates) an LSM storage engine in dir.
func NewLSMCache(dir string, maxEntryBytes int64, ttl time.Duration) (*LSMCache, error) {
	opts := lsm.Options{
		MemTableSize:  4 * 1024 * 1024, // 4 MiB flushes
		MaxL0Tables:   4,
		BlockSize:     4096, // page-aligned
		BloomFPRate:   0.01, // 1% false positive
		BloomExpected: 10000,
		MaxTableSize:  2 * 1024 * 1024, // 2 MiB tables
		L1Budget:      10 * 1024 * 1024,
	}
	engine, err := lsm.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	return &LSMCache{
		engine:        engine,
		maxEntryBytes: maxEntryBytes,
		ttl:           ttl,
	}, nil
}

// Get looks up key in the LSM-backed cache.
func (c *LSMCache) Get(ctx context.Context, key Key) (*Entry, bool, error) {
	val, found, err := c.engine.Get([]byte(key))
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}

	var entry Entry
	if err := json.Unmarshal(val, &entry); err != nil {
		return nil, false, err
	}
	if c.ttl > 0 && !entry.CreatedAt.IsZero() && time.Since(entry.CreatedAt) > c.ttl {
		return nil, false, nil
	}
	return &entry, true, nil
}

// GetSemantic is a no-op fallback for simple key-value engines.
func (c *LSMCache) GetSemantic(ctx context.Context, tenantID string, embedding []float32, threshold float64) (*Entry, bool, error) {
	return nil, false, nil
}

// Put serializes and inserts entry under key in the LSM-backed cache.
func (c *LSMCache) Put(ctx context.Context, key Key, entry *Entry) error {
	if entry == nil {
		return nil
	}
	if c.maxEntryBytes > 0 && entry.Bytes() > c.maxEntryBytes {
		return ErrEntryTooLarge
	}

	val, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	return c.engine.Put([]byte(key), val)
}

// PutSemantic acts as a fallback to Put for simple key-value engines.
func (c *LSMCache) PutSemantic(ctx context.Context, tenantID string, key Key, entry *Entry, embedding []float32) error {
	return c.Put(ctx, key, entry)
}

// Close flushes active writes and closes the underlying LSM engine cleanly.
func (c *LSMCache) Close() error {
	return c.engine.Close()
}
