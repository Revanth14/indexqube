// Package lsm implements a Log-Structured Merge-Tree (LSM-Tree) storage engine.
//
// # Architecture
//
//	Write path:  Put/Delete → MemTable (skip list, in-memory sorted map)
//	                        → when full, freeze & flush → L0 SSTable on disk
//	                        → when |L0| ≥ MaxL0Tables, compact → L1 SSTable(s)
//	                        → repeat for L1 → L2 → … when size budget exceeded
//
//	Read path:   Get → MemTable → immutable MemTable → L0 (newest-first)
//	                 → L1 … (Bloom filter short-circuits most disk reads)
//
// # File naming
//
//	{level:02d}-{seq:010d}.sst   e.g. "00-0000000001.sst" (L0), "01-0000000002.sst" (L1)
//
// # Durability
//
// This implementation does not include a Write-Ahead Log (WAL). Entries in the
// active MemTable are lost on crash. Add a WAL for production use.
//
// # Concurrency
//
// Engine.Get is safe for concurrent use.
// Engine.Put / Engine.Delete acquire a write lock for the MemTable update only.
// SSTable reads use os.File.ReadAt which is concurrent-safe on all major OSes.
package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Options configures an LSM-Tree Engine.
type Options struct {
	// MemTableSize is the size threshold (bytes) that triggers a MemTable flush.
	// Default: 4 MiB.
	MemTableSize int64

	// MaxL0Tables is the number of L0 SSTables that triggers L0 → L1 compaction.
	// Default: 4.
	MaxL0Tables int

	// BlockSize is the target data block size inside each SSTable (bytes).
	// Default: 4096.
	BlockSize int

	// BloomFPRate is the target false-positive rate for Bloom filters.
	// Default: 0.01 (1%).
	BloomFPRate float64

	// BloomExpected is the expected number of keys per SSTable for sizing the
	// Bloom filter. Default: 65536.
	BloomExpected int

	// MaxTableSize caps individual SSTable file sizes during compaction (bytes).
	// Default: 2 MiB.
	MaxTableSize int64

	// L1Budget is the total SSTable size allowed at L1 before L1→L2 compaction.
	// Each subsequent level is 10× larger. Default: 10 MiB.
	L1Budget int64
}

func (o *Options) setDefaults() {
	if o.MemTableSize == 0 {
		o.MemTableSize = 4 * 1024 * 1024
	}
	if o.MaxL0Tables == 0 {
		o.MaxL0Tables = 4
	}
	if o.BlockSize == 0 {
		o.BlockSize = 4096
	}
	if o.BloomFPRate == 0 {
		o.BloomFPRate = 0.01
	}
	if o.BloomExpected == 0 {
		o.BloomExpected = 65536
	}
	if o.MaxTableSize == 0 {
		o.MaxTableSize = 2 * 1024 * 1024
	}
	if o.L1Budget == 0 {
		o.L1Budget = 10 * 1024 * 1024
	}
}

// Engine is an LSM-Tree key-value store.
// Open it with Open; close it with Close when done.
type Engine struct {
	mu  sync.RWMutex
	mem *MemTable // active mutable MemTable
	imm *MemTable // immutable MemTable waiting to be flushed (nil if none)

	// levels[0] = L0 (may have overlapping key ranges)
	// levels[k] = Lk (non-overlapping after compaction)
	levels [][]*SSTable

	dir  string
	opts Options
	seq  atomic.Int64 // monotonically increasing SSTable sequence number

	compactC chan struct{} // signal background compactor to wake up
	done     chan struct{}
	wg       sync.WaitGroup
}

// Open opens or creates the Engine at dir.
// The directory is created if it does not exist.
func Open(dir string, opts Options) (*Engine, error) {
	opts.setDefaults()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("lsm: mkdir %s: %w", dir, err)
	}

	e := &Engine{
		dir:      dir,
		opts:     opts,
		mem:      newMemTable(opts.MemTableSize),
		levels:   make([][]*SSTable, 1), // start with L0
		compactC: make(chan struct{}, 1),
		done:     make(chan struct{}),
	}

	if err := e.loadExistingTables(); err != nil {
		return nil, err
	}

	e.wg.Add(1)
	go e.backgroundLoop()
	return e, nil
}

// loadExistingTables scans dir for *.sst files and opens them.
func (e *Engine) loadExistingTables() error {
	entries, err := os.ReadDir(e.dir)
	if err != nil {
		return fmt.Errorf("lsm: read dir: %w", err)
	}

	type tableInfo struct {
		lvl  int
		seq  int64
		path string
	}
	var tables []tableInfo
	maxSeq := int64(0)

	for _, de := range entries {
		name := de.Name()
		if !strings.HasSuffix(name, ".sst") {
			continue
		}
		// Expected: "{lvl:02d}-{seq:010d}.sst"
		parts := strings.SplitN(strings.TrimSuffix(name, ".sst"), "-", 2)
		if len(parts) != 2 {
			continue
		}
		lvl, err1 := strconv.Atoi(parts[0])
		seq, err2 := strconv.ParseInt(parts[1], 10, 64)
		if err1 != nil || err2 != nil || lvl < 0 {
			continue
		}
		tables = append(tables, tableInfo{lvl: lvl, seq: seq, path: filepath.Join(e.dir, name)})
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	e.seq.Store(maxSeq)

	// Sort within each level by sequence number (ascending = oldest first).
	sort.Slice(tables, func(i, j int) bool {
		if tables[i].lvl != tables[j].lvl {
			return tables[i].lvl < tables[j].lvl
		}
		return tables[i].seq < tables[j].seq
	})

	for _, ti := range tables {
		t, err := openSSTable(ti.path)
		if err != nil {
			return fmt.Errorf("lsm: open %s: %w", ti.path, err)
		}
		e.ensureLevel(ti.lvl)
		e.levels[ti.lvl] = append(e.levels[ti.lvl], t)
	}
	return nil
}

// Get retrieves the value for key.
//
//   - (val, true, nil):  key found
//   - (nil, false, nil): key absent
//   - (_, _, err):       I/O error
//
// Tombstones (deleted keys) are invisible: (nil, false, nil) is returned.
func (e *Engine) Get(key []byte) (val []byte, found bool, err error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// 1. Active MemTable.
	if v, ok, tomb := e.mem.Get(key); ok {
		if tomb {
			return nil, false, nil
		}
		return v, true, nil
	}

	// 2. Immutable MemTable (being flushed).
	if e.imm != nil {
		if v, ok, tomb := e.imm.Get(key); ok {
			if tomb {
				return nil, false, nil
			}
			return v, true, nil
		}
	}

	// 3. L0 — newest-first (L0 tables may have overlapping key ranges).
	l0 := e.levels[0]
	for i := len(l0) - 1; i >= 0; i-- {
		v, ok, err := l0[i].Get(key)
		if err != nil {
			return nil, false, err
		}
		if ok {
			if v == nil {
				return nil, false, nil // tombstone
			}
			return v, true, nil
		}
	}

	// 4. L1 and deeper — non-overlapping key ranges; check each level linearly
	//    (Bloom filters make most checks free).
	for lvl := 1; lvl < len(e.levels); lvl++ {
		for _, t := range e.levels[lvl] {
			v, ok, err := t.Get(key)
			if err != nil {
				return nil, false, err
			}
			if ok {
				if v == nil {
					return nil, false, nil // tombstone
				}
				return v, true, nil
			}
		}
	}
	return nil, false, nil
}

// Put inserts or updates key → val.
func (e *Engine) Put(key, val []byte) error {
	e.mu.Lock()
	full := e.mem.Put(key, val)
	e.mu.Unlock()
	if full {
		e.triggerFlush()
	}
	return nil
}

// Delete removes key by writing a tombstone.
func (e *Engine) Delete(key []byte) error {
	e.mu.Lock()
	full := e.mem.Delete(key)
	e.mu.Unlock()
	if full {
		e.triggerFlush()
	}
	return nil
}

// Close flushes all pending writes and shuts down the background goroutine.
func (e *Engine) Close() error {
	// Flush the active MemTable.
	e.mu.Lock()
	if e.mem.Count() > 0 {
		e.imm = e.mem
		e.mem = newMemTable(e.opts.MemTableSize)
	}
	e.mu.Unlock()

	if e.imm != nil {
		if err := e.flushImm(); err != nil {
			return err
		}
	}

	// Signal background loop to exit.
	close(e.done)
	e.wg.Wait()

	// Close all SSTable file handles.
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, level := range e.levels {
		for _, t := range level {
			t.Close()
		}
	}
	return nil
}

// ─── Internal: flush & compaction ────────────────────────────────────────────

// triggerFlush freezes the active MemTable and signals the background loop.
func (e *Engine) triggerFlush() {
	e.mu.Lock()
	if e.imm == nil {
		e.imm = e.mem
		e.mem = newMemTable(e.opts.MemTableSize)
	}
	e.mu.Unlock()
	select {
	case e.compactC <- struct{}{}:
	default:
	}
}

// backgroundLoop handles flushes and compaction in a single goroutine.
func (e *Engine) backgroundLoop() {
	defer e.wg.Done()
	c := &compactor{eng: e}
	for {
		select {
		case <-e.done:
			return
		case <-e.compactC:
			// Flush immutable MemTable first.
			e.mu.RLock()
			hasImm := e.imm != nil
			e.mu.RUnlock()
			if hasImm {
				_ = e.flushImm()
			}
			// Then check whether compaction is needed.
			c.maybeCompact()
		}
	}
}

// flushImm writes the immutable MemTable to a new L0 SSTable.
func (e *Engine) flushImm() error {
	e.mu.RLock()
	imm := e.imm
	e.mu.RUnlock()
	if imm == nil {
		return nil
	}

	path := e.newTablePath(0)
	b, err := newBuilder(path, e.opts)
	if err != nil {
		return err
	}

	it := imm.Iterator()
	for it.Valid() {
		if err := b.Add(it.Key(), it.Val()); err != nil {
			return err
		}
		it.Next()
	}
	if err := b.Finish(); err != nil {
		return err
	}

	t, err := openSSTable(path)
	if err != nil {
		return err
	}

	e.mu.Lock()
	e.ensureLevel(0)
	e.levels[0] = append(e.levels[0], t)
	e.imm = nil
	e.mu.Unlock()
	return nil
}

// ─── Level / path helpers ─────────────────────────────────────────────────────

// newTablePath allocates a new unique SSTable file path for the given level.
func (e *Engine) newTablePath(lvl int) string {
	seq := e.seq.Add(1)
	return filepath.Join(e.dir, fmt.Sprintf("%02d-%010d.sst", lvl, seq))
}

// levelBudget returns the maximum total SSTable size allowed at level lvl.
// Budget grows 10× per level: L1 = L1Budget, L2 = 10×L1Budget, etc.
func (e *Engine) levelBudget(lvl int) int64 {
	budget := e.opts.L1Budget
	for i := 1; i < lvl; i++ {
		budget *= 10
	}
	return budget
}

// ensureLevel guarantees that e.levels has at least lvl+1 slots.
// Caller must hold e.mu for writing.
func (e *Engine) ensureLevel(lvl int) {
	for len(e.levels) <= lvl {
		e.levels = append(e.levels, nil)
	}
}
