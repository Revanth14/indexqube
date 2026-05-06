package governor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"
)

// Snapshot is the last remembered state for one logical file.
type Snapshot struct {
	Content string
	Hash    string
}

// History stores the last seen snapshot of each logical file path per tenant.
// Implementations must be safe for concurrent use. Used by the pruning engine
// to compute line diffs between successive user prompts (Path A + Path B).
//
// A future Supabase/pgvector tier implements this interface without changing
// the pruner.
type History interface {
	Get(ctx context.Context, tenantID, path string) (Snapshot, bool)
	Put(ctx context.Context, tenantID, path, content string)
}

// MemoryHistoryConfig bounds the raw-code snapshots retained for pruning.
// Zero values disable the corresponding limit.
type MemoryHistoryConfig struct {
	MaxTenants        int
	MaxFilesPerTenant int
	MaxFileBytes      int64
	MaxBytes          int64
	TTL               time.Duration
}

// MemoryHistoryStats is a point-in-time view of in-memory history pressure.
type MemoryHistoryStats struct {
	Tenants int
	Entries int
	Bytes   int64
}

// MemoryHistory is an in-process History for single-droplet deployments.
// Entries are bounded, TTL'd, and never persisted across process restart.
// L2 backing store swaps in behind the same interface later.
type MemoryHistory struct {
	mu    sync.Mutex
	data  map[string]map[string]*historyEntry // tenant -> path -> last snapshot
	bytes int64
	cfg   MemoryHistoryConfig
	nowFn func() time.Time
}

type historyEntry struct {
	content    string
	hash       string
	bytes      int64
	lastAccess time.Time
}

// NewMemoryHistory returns an empty MemoryHistory.
func NewMemoryHistory() *MemoryHistory {
	return NewMemoryHistoryWithConfig(MemoryHistoryConfig{
		MaxTenants:        1024,
		MaxFilesPerTenant: 256,
		MaxFileBytes:      2 << 20,
		MaxBytes:          64 << 20,
		TTL:               2 * time.Hour,
	})
}

// NewMemoryHistoryWithConfig returns an empty MemoryHistory using cfg.
func NewMemoryHistoryWithConfig(cfg MemoryHistoryConfig) *MemoryHistory {
	return &MemoryHistory{
		data:  make(map[string]map[string]*historyEntry),
		cfg:   cfg,
		nowFn: time.Now,
	}
}

func (m *MemoryHistory) Get(_ context.Context, tenantID, path string) (Snapshot, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.nowFn()
	t, ok := m.data[tenantID]
	if !ok {
		return Snapshot{}, false
	}
	entry, ok := t[path]
	if !ok {
		return Snapshot{}, false
	}
	if m.expired(entry, now) {
		m.removeFileLocked(tenantID, path)
		return Snapshot{}, false
	}
	entry.lastAccess = now
	return Snapshot{Content: entry.content, Hash: entry.hash}, true
}

func (m *MemoryHistory) Put(_ context.Context, tenantID, path, content string) {
	if tenantID == "" || path == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.nowFn()
	m.evictExpiredLocked(now)

	size := int64(len(content))
	if m.cfg.MaxFileBytes > 0 && size > m.cfg.MaxFileBytes {
		m.removeFileLocked(tenantID, path)
		return
	}

	t, ok := m.data[tenantID]
	if !ok {
		t = make(map[string]*historyEntry)
		m.data[tenantID] = t
	}
	if old, ok := t[path]; ok {
		m.bytes -= old.bytes
	}
	t[path] = &historyEntry{content: content, hash: ContentHash(content), bytes: size, lastAccess: now}
	m.bytes += size

	m.evictFilesForTenantLocked(tenantID)
	m.evictTenantsLocked()
	m.evictUntilUnderBudgetLocked()
}

// ContentHash returns the stable checksum used to compare file snapshots.
func ContentHash(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

// Stats returns current history pressure. It is intended for tests and future
// operator endpoints.
func (m *MemoryHistory) Stats() MemoryHistoryStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	entries := 0
	for _, files := range m.data {
		entries += len(files)
	}
	return MemoryHistoryStats{Tenants: len(m.data), Entries: entries, Bytes: m.bytes}
}

func (m *MemoryHistory) expired(entry *historyEntry, now time.Time) bool {
	return m.cfg.TTL > 0 && now.Sub(entry.lastAccess) > m.cfg.TTL
}

func (m *MemoryHistory) evictExpiredLocked(now time.Time) {
	if m.cfg.TTL <= 0 {
		return
	}
	for tenantID, files := range m.data {
		for path, entry := range files {
			if m.expired(entry, now) {
				m.removeFileLocked(tenantID, path)
			}
		}
	}
}

func (m *MemoryHistory) evictFilesForTenantLocked(tenantID string) {
	limit := m.cfg.MaxFilesPerTenant
	if limit <= 0 {
		return
	}
	for {
		files := m.data[tenantID]
		if len(files) <= limit {
			return
		}
		path, ok := oldestFileLocked(files)
		if !ok {
			return
		}
		m.removeFileLocked(tenantID, path)
	}
}

func (m *MemoryHistory) evictTenantsLocked() {
	limit := m.cfg.MaxTenants
	if limit <= 0 {
		return
	}
	for len(m.data) > limit {
		tenantID, ok := m.oldestTenantLocked()
		if !ok {
			return
		}
		m.removeTenantLocked(tenantID)
	}
}

func (m *MemoryHistory) evictUntilUnderBudgetLocked() {
	limit := m.cfg.MaxBytes
	if limit <= 0 {
		return
	}
	for m.bytes > limit {
		tenantID, path, ok := m.oldestFileAcrossTenantsLocked()
		if !ok {
			return
		}
		m.removeFileLocked(tenantID, path)
	}
}

func (m *MemoryHistory) oldestTenantLocked() (string, bool) {
	var oldestTenant string
	var oldest time.Time
	found := false
	for tenantID, files := range m.data {
		for _, entry := range files {
			if !found || entry.lastAccess.Before(oldest) {
				oldestTenant = tenantID
				oldest = entry.lastAccess
				found = true
			}
		}
		if len(files) == 0 && !found {
			oldestTenant = tenantID
			found = true
		}
	}
	return oldestTenant, found
}

func (m *MemoryHistory) oldestFileAcrossTenantsLocked() (string, string, bool) {
	var oldestTenant, oldestPath string
	var oldest time.Time
	found := false
	for tenantID, files := range m.data {
		path, ok := oldestFileLocked(files)
		if !ok {
			continue
		}
		entry := files[path]
		if !found || entry.lastAccess.Before(oldest) {
			oldestTenant = tenantID
			oldestPath = path
			oldest = entry.lastAccess
			found = true
		}
	}
	return oldestTenant, oldestPath, found
}

func oldestFileLocked(files map[string]*historyEntry) (string, bool) {
	var oldestPath string
	var oldest time.Time
	found := false
	for path, entry := range files {
		if !found || entry.lastAccess.Before(oldest) {
			oldestPath = path
			oldest = entry.lastAccess
			found = true
		}
	}
	return oldestPath, found
}

func (m *MemoryHistory) removeTenantLocked(tenantID string) {
	for path := range m.data[tenantID] {
		m.removeFileLocked(tenantID, path)
	}
	delete(m.data, tenantID)
}

func (m *MemoryHistory) removeFileLocked(tenantID, path string) {
	files, ok := m.data[tenantID]
	if !ok {
		return
	}
	if entry, ok := files[path]; ok {
		m.bytes -= entry.bytes
		delete(files, path)
	}
	if len(files) == 0 {
		delete(m.data, tenantID)
	}
}
