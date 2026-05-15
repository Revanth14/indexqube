package lsm

import (
	"bytes"
	"container/heap"
	"fmt"
	"os"
)

// compactor runs background leveled compaction for the Engine.
//
// # Leveled compaction rules
//
//   - L0 holds direct MemTable flushes; keys may overlap across files.
//     Compaction is triggered when |L0| ≥ opts.MaxL0Tables.
//     All L0 tables are merged into new L1 table(s).
//
//   - L1+ holds non-overlapping key-range files sorted by first key.
//     Compaction is triggered when ΣfileSize(Lk) > levelBudget(k).
//     The oldest file in the overflowing level is merged into Lk+1.
//
// The compactor is the only writer to the SSTable file list; it holds
// the engine write-lock only for the brief atomic swap at the end.
type compactor struct {
	eng *Engine
}

// maybeCompact checks all levels and runs one round of compaction if needed.
// It is called from the background goroutine after each flush or previous compaction.
func (c *compactor) maybeCompact() {
	eng := c.eng

	eng.mu.RLock()
	l0count := len(eng.levels[0])
	eng.mu.RUnlock()

	if l0count >= eng.opts.MaxL0Tables {
		if err := c.compactL0(); err != nil {
			// Log and continue; compaction is best-effort.
			_ = err
		}
	}

	// Check higher levels.
	for lvl := 1; ; lvl++ {
		eng.mu.RLock()
		if lvl >= len(eng.levels) {
			eng.mu.RUnlock()
			break
		}
		tables := make([]*SSTable, len(eng.levels[lvl]))
		copy(tables, eng.levels[lvl])
		eng.mu.RUnlock()

		budget := eng.levelBudget(lvl)
		if totalSize(tables) <= budget {
			break
		}
		if err := c.compactLevel(lvl, tables); err != nil {
			break
		}
	}
}

// compactL0 merges all L0 SSTables into new L1 SSTable(s).
func (c *compactor) compactL0() error {
	eng := c.eng

	eng.mu.RLock()
	l0 := make([]*SSTable, len(eng.levels[0]))
	copy(l0, eng.levels[0])
	var l1 []*SSTable
	if len(eng.levels) > 1 {
		l1 = make([]*SSTable, len(eng.levels[1]))
		copy(l1, eng.levels[1])
	}
	eng.mu.RUnlock()

	// Build iterators: L0 newest-first (highest priority), then L1.
	// When keys collide, the higher-priority (newer) entry wins.
	iters := make([]iterWithPrio, 0, len(l0)+len(l1))
	for i, t := range l0 {
		it := newSSTIter(t)
		it.Next()
		if it.Valid() {
			iters = append(iters, iterWithPrio{it: it, prio: len(l0) - i})
		}
	}
	for _, t := range l1 {
		it := newSSTIter(t)
		it.Next()
		if it.Valid() {
			iters = append(iters, iterWithPrio{it: it, prio: 0})
		}
	}

	newL1, err := c.mergeToLevel(1, iters)
	if err != nil {
		return err
	}

	// Atomically replace L0 and L1 with the compaction output.
	eng.mu.Lock()
	old := append(l0, l1...)
	eng.ensureLevel(1)
	eng.levels[0] = nil
	eng.levels[1] = newL1
	eng.mu.Unlock()

	// Delete old files after releasing the lock.
	for _, t := range old {
		t.Close()
		os.Remove(t.Path())
	}
	return nil
}

// compactLevel merges the oldest SSTable in level lvl into level lvl+1.
func (c *compactor) compactLevel(lvl int, tables []*SSTable) error {
	eng := c.eng

	// Pick the oldest table (index 0 = oldest by insertion order).
	victim := tables[0]

	eng.mu.Lock()
	eng.ensureLevel(lvl + 1)
	next := make([]*SSTable, len(eng.levels[lvl+1]))
	copy(next, eng.levels[lvl+1])
	eng.mu.Unlock()

	// Find overlapping tables in lvl+1.
	overlapping := overlappingTables(next, victim)

	inputs := append([]*SSTable{victim}, overlapping...)
	iters := make([]iterWithPrio, 0, len(inputs))
	for i, t := range inputs {
		it := newSSTIter(t)
		it.Next()
		if it.Valid() {
			iters = append(iters, iterWithPrio{it: it, prio: len(inputs) - i})
		}
	}

	newTables, err := c.mergeToLevel(lvl+1, iters)
	if err != nil {
		return err
	}

	eng.mu.Lock()
	// Remove victim from lvl.
	remaining := make([]*SSTable, 0, len(tables)-1)
	for _, t := range tables {
		if t != victim {
			remaining = append(remaining, t)
		}
	}
	eng.levels[lvl] = remaining
	// Replace overlapping in lvl+1 with newTables.
	eng.ensureLevel(lvl + 1)
	replacement := make([]*SSTable, 0, len(next)-len(overlapping)+len(newTables))
	overlapSet := make(map[*SSTable]bool, len(overlapping))
	for _, t := range overlapping {
		overlapSet[t] = true
	}
	for _, t := range next {
		if !overlapSet[t] {
			replacement = append(replacement, t)
		}
	}
	replacement = append(replacement, newTables...)
	eng.levels[lvl+1] = replacement
	eng.mu.Unlock()

	for _, t := range inputs {
		t.Close()
		os.Remove(t.Path())
	}
	return nil
}

// mergeToLevel runs a k-way merge of iters and writes new SSTables at the
// given level. Each output file is capped at opts.MaxTableSize bytes.
// The last tombstone seen for a key at the deepest level is dropped.
func (c *compactor) mergeToLevel(lvl int, iters []iterWithPrio) ([]*SSTable, error) {
	eng := c.eng
	mh := make(mergeHeap, 0, len(iters))
	for _, ip := range iters {
		mh = append(mh, ip)
	}
	heap.Init(&mh)

	var (
		out     []*SSTable
		builder *Builder
		written int64
		lastKey []byte
	)

	flush := func() error {
		if builder == nil {
			return nil
		}
		if err := builder.Finish(); err != nil {
			return err
		}
		t, err := openSSTable(builder.path)
		if err != nil {
			return err
		}
		out = append(out, t)
		builder = nil
		written = 0
		return nil
	}

	for mh.Len() > 0 {
		top := heap.Pop(&mh).(iterWithPrio)

		key := top.it.Key()
		val := top.it.Val()

		// Skip duplicate keys — only the highest-priority entry counts.
		isDup := bytes.Equal(key, lastKey)
		lastKey = append(lastKey[:0], key...)

		// Advance the iterator and push it back if still valid.
		top.it.Next()
		if top.it.Valid() {
			heap.Push(&mh, top)
		}

		// Also drain any lower-priority entries for the same key.
		for mh.Len() > 0 && bytes.Equal(mh[0].it.Key(), key) {
			dup := heap.Pop(&mh).(iterWithPrio)
			dup.it.Next()
			if dup.it.Valid() {
				heap.Push(&mh, dup)
			}
		}

		if isDup {
			continue
		}
		// Drop tombstones at the bottom level (no older data can exist).
		if val == nil && lvl >= len(eng.levels)-1 {
			continue
		}

		if builder == nil {
			path := eng.newTablePath(lvl)
			var err error
			builder, err = newBuilder(path, eng.opts)
			if err != nil {
				return nil, fmt.Errorf("lsm: compaction builder: %w", err)
			}
		}
		if err := builder.Add(key, val); err != nil {
			return nil, err
		}
		written += int64(len(key) + len(val) + 6)
		if written >= eng.opts.MaxTableSize {
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return out, nil
}

// ─── k-way merge helpers ──────────────────────────────────────────────────────

// iterWithPrio wraps an SSTIter with a priority for tie-breaking.
// Higher priority = more recent data (wins when keys collide).
type iterWithPrio struct {
	it   *SSTIter
	prio int
}

// mergeHeap implements heap.Interface over iterWithPrio.
// It sorts by key ASC, then by priority DESC (higher priority = pops first).
type mergeHeap []iterWithPrio

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].it.Key(), h[j].it.Key())
	if cmp != 0 {
		return cmp < 0
	}
	return h[i].prio > h[j].prio // higher priority first
}
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)   { *h = append(*h, x.(iterWithPrio)) }
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// ─── Utility helpers ──────────────────────────────────────────────────────────

// totalSize sums the on-disk sizes of the given SSTables.
func totalSize(tables []*SSTable) int64 {
	total := int64(0)
	for _, t := range tables {
		if fi, err := t.f.Stat(); err == nil {
			total += fi.Size()
		}
	}
	return total
}

// overlappingTables returns tables from next whose key range overlaps victim.
func overlappingTables(next []*SSTable, victim *SSTable) []*SSTable {
	if len(victim.index) == 0 {
		return nil
	}
	vMin := victim.index[0].firstKey
	vMax := victim.index[len(victim.index)-1].firstKey

	var out []*SSTable
	for _, t := range next {
		if len(t.index) == 0 {
			continue
		}
		tMin := t.index[0].firstKey
		tMax := t.index[len(t.index)-1].firstKey
		// Overlap if vMin ≤ tMax and tMin ≤ vMax.
		if bytes.Compare(vMin, tMax) <= 0 && bytes.Compare(tMin, vMax) <= 0 {
			out = append(out, t)
		}
	}
	return out
}
