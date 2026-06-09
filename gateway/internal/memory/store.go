// Package memory holds volatile session state for the Claude Code MVP.
//
// It intentionally stores only hashes and metadata, never raw code or prompt
// text. This gives us enough signal for exact duplicate detection without
// turning the gateway into a source-code database.
package memory

import (
	"sync"
	"time"
)

type Block struct {
	Hash   string
	Kind   string
	Bytes  int
	Tokens int
}

type Store struct {
	mu       sync.Mutex
	ttl      time.Duration
	sessions map[string]*Session
}

type Session struct {
	Key        string
	CreatedAt  time.Time
	LastSeenAt time.Time
	Blocks     map[string]BlockMeta
	Totals     UsageTotals
}

type BlockMeta struct {
	Hash        string
	Kind        string
	Bytes       int
	Tokens      int
	FirstSeenAt time.Time
	LastSeenAt  time.Time
	SeenCount   int
}

type UsageTotals struct {
	Requests    int
	TokensIn    int
	TokensOut   int
	TokensSaved int
	BytesIn     int
	BytesSaved  int
	// Measured upstream input usage (ground truth from Anthropic's usage object).
	// CacheReadTokens is context served from Anthropic's prompt cache this turn.
	CacheReadTokens     int
	CacheCreationTokens int
}

func NewStore(ttl time.Duration) *Store {
	if ttl <= 0 {
		ttl = 4 * time.Hour
	}
	return &Store{
		ttl:      ttl,
		sessions: make(map[string]*Session),
	}
}

func (s *Store) Get(sessionKey, hash string) (BlockMeta, bool) {
	if sessionKey == "" || hash == "" {
		return BlockMeta{}, false
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	session := s.getOrCreateLocked(sessionKey, now)
	meta, ok := session.Blocks[hash]
	return meta, ok
}

func (s *Store) Seen(sessionKey, hash string) bool {
	if sessionKey == "" || hash == "" {
		return false
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	session := s.getOrCreateLocked(sessionKey, now)
	_, ok := session.Blocks[hash]
	return ok
}

func (s *Store) SaveBlock(sessionKey string, block Block) {
	if sessionKey == "" || block.Hash == "" {
		return
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	session := s.getOrCreateLocked(sessionKey, now)
	meta, ok := session.Blocks[block.Hash]
	if !ok {
		meta = BlockMeta{
			Hash:        block.Hash,
			Kind:        block.Kind,
			Bytes:       block.Bytes,
			Tokens:      block.Tokens,
			FirstSeenAt: now,
		}
	}
	meta.LastSeenAt = now
	meta.SeenCount++
	session.Blocks[block.Hash] = meta
}

func (s *Store) RecordUsage(sessionKey string, usage UsageTotals) {
	if sessionKey == "" {
		return
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	session := s.getOrCreateLocked(sessionKey, now)
	session.Totals.Requests += usage.Requests
	session.Totals.TokensIn += usage.TokensIn
	session.Totals.TokensOut += usage.TokensOut
	session.Totals.TokensSaved += usage.TokensSaved
	session.Totals.BytesIn += usage.BytesIn
	session.Totals.BytesSaved += usage.BytesSaved
	session.Totals.CacheReadTokens += usage.CacheReadTokens
	session.Totals.CacheCreationTokens += usage.CacheCreationTokens
}

func (s *Store) CleanupExpired() {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, session := range s.sessions {
		if now.Sub(session.LastSeenAt) > s.ttl {
			delete(s.sessions, key)
		}
	}
}

func (s *Store) getOrCreateLocked(key string, now time.Time) *Session {
	if session, ok := s.sessions[key]; ok {
		if now.Sub(session.LastSeenAt) <= s.ttl {
			session.LastSeenAt = now
			return session
		}
		delete(s.sessions, key)
	}
	session := &Session{
		Key:        key,
		CreatedAt:  now,
		LastSeenAt: now,
		Blocks:     make(map[string]BlockMeta),
	}
	s.sessions[key] = session
	return session
}
