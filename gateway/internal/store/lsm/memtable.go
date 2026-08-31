package lsm

import (
	"bytes"
	"math/rand/v2"
	"sync"
)

// skipList tuning constants.
const (
	maxSkipLevel = 12   // supports ~4 M entries at p=0.25
	skipP        = 0.25 // probability of promoting to the next level
	nodeOverhead = 96   // conservative per-node GC/pointer overhead in bytes
)

// skipNode is one node in the probabilistic skip list.
type skipNode struct {
	key  []byte
	val  []byte      // nil signals a tombstone (deleted key)
	next []*skipNode // len(next) == node level
}

// MemTable is a write-optimised in-memory sorted map backed by a skip list.
//
// # Complexity
//
//   - Put / Delete / Get: O(log N) expected
//   - Ordered iteration: O(N) via the level-0 linked list
//
// A MemTable is safe for concurrent use.
type MemTable struct {
	mu      sync.RWMutex
	head    *skipNode // sentinel; never holds a real key
	level   int       // highest level currently in use (1-indexed)
	count   int       // total slots including tombstones
	size    int64     // approximate memory usage in bytes
	maxSize int64     // size threshold that triggers a flush
}

// newMemTable returns a MemTable that reports "should flush" once its
// approximate memory usage reaches maxSize.
func newMemTable(maxSize int64) *MemTable {
	return &MemTable{
		head:    &skipNode{next: make([]*skipNode, maxSkipLevel)},
		level:   1,
		maxSize: maxSize,
	}
}

// Put inserts or updates key → val.
// Returns true when the MemTable has reached its size threshold.
func (m *MemTable) Put(key, val []byte) bool {
	m.mu.Lock()
	m.set(key, val)
	full := m.size >= m.maxSize
	m.mu.Unlock()
	return full
}

// Delete marks key as deleted (tombstone).
// Returns true when the MemTable should be flushed.
func (m *MemTable) Delete(key []byte) bool {
	m.mu.Lock()
	m.set(key, nil)
	full := m.size >= m.maxSize
	m.mu.Unlock()
	return full
}

// Get returns (value, found, isTombstone).
//   - found=false:            key is not present
//   - found=true, tomb=true:  key was deleted
//   - found=true, tomb=false: key is present with the returned value
func (m *MemTable) Get(key []byte) (val []byte, found, tombstone bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	cur := m.head
	for i := m.level - 1; i >= 0; i-- {
		for cur.next[i] != nil && bytes.Compare(cur.next[i].key, key) < 0 {
			cur = cur.next[i]
		}
	}
	node := cur.next[0]
	if node == nil || !bytes.Equal(node.key, key) {
		return nil, false, false
	}
	if node.val == nil {
		return nil, true, true // tombstone
	}
	return node.val, true, false
}

// Size returns the approximate memory used by the table in bytes.
func (m *MemTable) Size() int64 {
	m.mu.RLock()
	s := m.size
	m.mu.RUnlock()
	return s
}

// Count returns the number of entries (including tombstones).
func (m *MemTable) Count() int {
	m.mu.RLock()
	c := m.count
	m.mu.RUnlock()
	return c
}

// Iterator returns a snapshot-like in-order iterator.
// The iterator is not protected by a lock after construction; callers must
// ensure no concurrent writes happen while iterating (the engine does this
// by only iterating immutable MemTables).
func (m *MemTable) Iterator() *MemIter {
	m.mu.RLock()
	first := m.head.next[0]
	m.mu.RUnlock()
	return &MemIter{cur: first}
}

// set inserts or updates the entry for key. Caller must hold mu for writing.
func (m *MemTable) set(key, val []byte) {
	// Walk down from the top level collecting nodes that must be updated.
	update := make([]*skipNode, maxSkipLevel)
	cur := m.head
	for i := m.level - 1; i >= 0; i-- {
		for cur.next[i] != nil && bytes.Compare(cur.next[i].key, key) < 0 {
			cur = cur.next[i]
		}
		update[i] = cur
	}

	target := cur.next[0]
	if target != nil && bytes.Equal(target.key, key) {
		// Update in place — adjust size delta.
		oldValLen := int64(len(target.val))
		newValLen := int64(len(val))
		m.size += newValLen - oldValLen
		target.val = val
		if val != nil {
			target.val = append([]byte(nil), val...)
		}
		return
	}

	// New key: insert a node at a random level.
	lvl := m.randomLevel()
	if lvl > m.level {
		for i := m.level; i < lvl; i++ {
			update[i] = m.head
		}
		m.level = lvl
	}

	n := &skipNode{
		key:  append([]byte(nil), key...),
		next: make([]*skipNode, lvl),
	}
	if val != nil {
		n.val = append([]byte(nil), val...)
	}
	for i := 0; i < lvl; i++ {
		n.next[i] = update[i].next[i]
		update[i].next[i] = n
	}
	m.size += int64(len(key)+len(val)) + nodeOverhead
	m.count++
}

// randomLevel generates a level in [1, maxSkipLevel] with geometric distribution.
func (m *MemTable) randomLevel() int {
	level := 1
	for level < maxSkipLevel && rand.Float64() < skipP {
		level++
	}
	return level
}

// ─── Iterator ─────────────────────────────────────────────────────────────────

// MemIter is a forward-only in-order iterator over a MemTable.
// It follows the level-0 linked list — O(1) per step, O(N) total.
type MemIter struct {
	cur *skipNode
}

// Valid reports whether the iterator is positioned at a valid entry.
func (it *MemIter) Valid() bool { return it.cur != nil }

// Key returns the current key. Only valid when Valid() is true.
func (it *MemIter) Key() []byte { return it.cur.key }

// Val returns the current value, or nil for a tombstone.
func (it *MemIter) Val() []byte { return it.cur.val }

// IsTombstone reports whether the current entry is a deletion marker.
func (it *MemIter) IsTombstone() bool { return it.cur != nil && it.cur.val == nil }

// Next advances to the next entry.
func (it *MemIter) Next() { it.cur = it.cur.next[0] }
