package cache

import (
	"container/list"
	"context"
	"sync"
	"time"
)

// MemoryCache is an in-process LRU cache bounded by total byte size and
// per-entry TTL. It is the L1 tier and is safe for concurrent use.
//
// On Put: if the new entry alone exceeds maxBytes, the request is
// rejected with ErrEntryTooLarge. Otherwise older entries are evicted
// from the back of the LRU until the cache fits.
//
// On Get: expired entries (now > CreatedAt+TTL) are removed lazily and
// reported as a miss. There is no background sweep in v1.
type MemoryCache struct {
	mu       sync.Mutex
	maxBytes int64
	bytes    int64
	ttl      time.Duration

	// items maps Key -> list element holding *cacheItem.
	items map[Key]*list.Element
	// lru is ordered most-recently-used (front) to least (back).
	lru *list.List

	// nowFn lets tests advance time without sleeping.
	nowFn func() time.Time
}

type cacheItem struct {
	key   Key
	entry *Entry
	bytes int64
}

// MemoryConfig configures a MemoryCache.
//
// MaxBytes <= 0 disables the cache (every Put is a no-op). TTL <= 0
// disables time-based expiry (only LRU and explicit eviction apply).
type MemoryConfig struct {
	MaxBytes int64
	TTL      time.Duration
}

// NewMemoryCache returns a fresh MemoryCache.
func NewMemoryCache(cfg MemoryConfig) *MemoryCache {
	return &MemoryCache{
		maxBytes: cfg.MaxBytes,
		ttl:      cfg.TTL,
		items:    make(map[Key]*list.Element),
		lru:      list.New(),
		nowFn:    time.Now,
	}
}

// Get looks up key. Hits move the entry to the front of the LRU.
func (c *MemoryCache) Get(_ context.Context, key Key) (*Entry, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	el, ok := c.items[key]
	if !ok {
		return nil, false, nil
	}
	item := el.Value.(*cacheItem)

	if c.expired(item.entry.CreatedAt) {
		c.removeElement(el)
		return nil, false, nil
	}

	c.lru.MoveToFront(el)
	return item.entry, true, nil
}

func (c *MemoryCache) GetSemantic(_ context.Context, _ string, _ []float32, _ float64) (*Entry, bool, error) {
	return nil, false, nil
}

// Put inserts or refreshes an entry. Triggers eviction if over budget.
func (c *MemoryCache) Put(_ context.Context, key Key, entry *Entry) error {
	if c.maxBytes <= 0 {
		// Cache disabled by config -- treat Put as a successful no-op.
		return nil
	}
	if entry == nil {
		return nil
	}
	size := entry.Bytes()
	if size > c.maxBytes {
		return ErrEntryTooLarge
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Replace existing entry under this key in place.
	if el, ok := c.items[key]; ok {
		c.bytes -= el.Value.(*cacheItem).bytes
		el.Value = &cacheItem{key: key, entry: entry, bytes: size}
		c.bytes += size
		c.lru.MoveToFront(el)
		c.evictUntilUnderBudget()
		return nil
	}

	item := &cacheItem{key: key, entry: entry, bytes: size}
	el := c.lru.PushFront(item)
	c.items[key] = el
	c.bytes += size
	c.evictUntilUnderBudget()
	return nil
}

func (c *MemoryCache) PutSemantic(ctx context.Context, _ string, key Key, entry *Entry, _ []float32) error {
	return c.Put(ctx, key, entry)
}

// Stats returns the current observed state.
func (c *MemoryCache) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{Entries: c.lru.Len(), Bytes: c.bytes}
}

// Purge drops every entry. Useful for tests and ops escape hatches.
func (c *MemoryCache) Purge() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = make(map[Key]*list.Element)
	c.lru.Init()
	c.bytes = 0
}

// expired reports whether a CreatedAt timestamp is past its TTL.
// Caller must hold c.mu (uses c.ttl, c.nowFn).
func (c *MemoryCache) expired(createdAt time.Time) bool {
	if c.ttl <= 0 {
		return false
	}
	return c.nowFn().Sub(createdAt) > c.ttl
}

// evictUntilUnderBudget pops the LRU tail until c.bytes <= c.maxBytes.
// Caller must hold c.mu.
func (c *MemoryCache) evictUntilUnderBudget() {
	for c.bytes > c.maxBytes {
		tail := c.lru.Back()
		if tail == nil {
			return // shouldn't happen; defensive
		}
		c.removeElement(tail)
	}
}

// removeElement detaches el from both the LRU and the items map.
// Caller must hold c.mu.
func (c *MemoryCache) removeElement(el *list.Element) {
	item := el.Value.(*cacheItem)
	c.lru.Remove(el)
	delete(c.items, item.key)
	c.bytes -= item.bytes
}
