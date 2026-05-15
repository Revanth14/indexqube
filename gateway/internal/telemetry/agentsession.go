package telemetry

import (
	"sort"
	"sync"
	"time"
)

// AgentSession is a per-session aggregate of guard decisions and token spend.
// Its shape mirrors the agent_sessions schema in CONTEXT.md so it can be
// persisted to a database layer without structural changes.
type AgentSession struct {
	SessionID       string  `json:"session_id"`
	StartedAt       int64   `json:"started_at"` // Unix seconds
	LastSeenAt      int64   `json:"last_seen_at"`
	TokensAttempted int64   `json:"tokens_attempted"`
	TokensSent      int64   `json:"tokens_sent"`
	TokensSaved     int64   `json:"tokens_saved"`
	RequestsTotal   int     `json:"requests_total"`
	LoopDetected    int     `json:"loop_detected"` // velocity/circuit warn events
	KillEvents      int     `json:"kill_events"`   // guard blocks (HTTP 429)
	KillReason      string  `json:"kill_reason"`   // most recent block reason
	DollarsSaved    float64 `json:"dollars_saved"`
	Status          string  `json:"status"` // "active" | "killed" | "ended"
}

// KillEvent is emitted whenever a guard blocks a request (HTTP 429).
// It is the loop-detection audit trail: how many times a session was stopped,
// why, and how much spend was prevented.
type KillEvent struct {
	SessionID    string  `json:"session_id"`
	Timestamp    int64   `json:"timestamp"`     // Unix seconds
	Reason       string  `json:"reason"`        // "velocity_exceeded" | "budget_exceeded" | …
	TokensSaved  int64   `json:"tokens_saved"`  // tokens not forwarded due to block
	DollarsSaved float64 `json:"dollars_saved"` // cost of those tokens
}

// RequestOutcome is the per-request signal passed to AgentSessionStore.Record.
// It is extracted from the guard decision and optimizer stats at the end of
// every request, whether allowed or blocked.
type RequestOutcome struct {
	TokensAttempted int
	TokensSent      int
	TokensSaved     int
	DollarsSaved    float64
	GuardReason     string
	Killed          bool // guard returned !Allow (HTTP 429 was sent)
	Warned          bool // guard returned Allow+Warn
}

// AgentSessionStore tracks per-session observability in memory.
// It is safe for concurrent use and is intentionally lightweight — no
// persistence, no external dependencies. A future version can flush to the
// agent_sessions table defined in CONTEXT.md.
type AgentSessionStore struct {
	mu         sync.Mutex
	sessions   map[string]*agentSessionEntry
	killLog    []KillEvent
	ttl        time.Duration
	maxKillLog int
}

type agentSessionEntry struct {
	AgentSession
	lastKilledAt time.Time
}

// NewAgentSessionStore returns a store that expires sessions after ttl of
// inactivity. A ttl of 0 defaults to 4 hours.
func NewAgentSessionStore(ttl time.Duration) *AgentSessionStore {
	if ttl <= 0 {
		ttl = 4 * time.Hour
	}
	return &AgentSessionStore{
		sessions:   make(map[string]*agentSessionEntry),
		ttl:        ttl,
		maxKillLog: 500,
	}
}

// Record updates the session aggregate with the outcome of one request.
// sessionKey is the per-invocation key produced by claudeSessionKey.
func (s *AgentSessionStore) Record(sessionKey string, out RequestOutcome) {
	if sessionKey == "" {
		return
	}
	now := time.Now()
	nowUnix := now.Unix()

	s.mu.Lock()
	defer s.mu.Unlock()

	entry := s.getOrCreateLocked(sessionKey, now)

	entry.LastSeenAt = nowUnix
	entry.RequestsTotal++
	entry.TokensAttempted += int64(out.TokensAttempted)
	entry.TokensSent += int64(out.TokensSent)
	entry.TokensSaved += int64(out.TokensSaved)
	entry.DollarsSaved += out.DollarsSaved

	if out.Killed {
		entry.KillEvents++
		entry.KillReason = out.GuardReason
		entry.lastKilledAt = now
		entry.Status = "killed"

		kill := KillEvent{
			SessionID:    sessionKey,
			Timestamp:    nowUnix,
			Reason:       out.GuardReason,
			TokensSaved:  int64(out.TokensAttempted - out.TokensSent),
			DollarsSaved: out.DollarsSaved,
		}
		s.killLog = append(s.killLog, kill)
		if len(s.killLog) > s.maxKillLog {
			// Drop oldest entries when the log fills up.
			s.killLog = s.killLog[len(s.killLog)-s.maxKillLog:]
		}
	} else if out.Warned {
		entry.LoopDetected++
		if entry.Status != "killed" {
			entry.Status = "active"
		}
	} else {
		if entry.Status != "killed" {
			entry.Status = "active"
		}
	}
}

// Snapshot returns a point-in-time copy of all sessions, sorted by last-seen
// descending (most recently active first). Expired sessions are excluded.
func (s *AgentSessionStore) Snapshot() []AgentSession {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	out := make([]AgentSession, 0, len(s.sessions))
	for _, e := range s.sessions {
		if now.Unix()-e.LastSeenAt > int64(s.ttl.Seconds()) {
			continue
		}
		snap := e.AgentSession
		// Promote "killed" sessions that have had no activity for >30s to "ended".
		if snap.Status == "killed" && now.Sub(e.lastKilledAt) > 30*time.Second {
			snap.Status = "ended"
		}
		out = append(out, snap)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].LastSeenAt > out[j].LastSeenAt
	})
	return out
}

// KillLog returns the recent kill events, newest first.
func (s *AgentSessionStore) KillLog() []KillEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]KillEvent, len(s.killLog))
	copy(out, s.killLog)
	// Reverse: newest first.
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

// CleanupExpired removes sessions that have been inactive for longer than ttl.
// Call periodically from a background goroutine.
func (s *AgentSessionStore) CleanupExpired() {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, e := range s.sessions {
		if now.Unix()-e.LastSeenAt > int64(s.ttl.Seconds()) {
			delete(s.sessions, key)
		}
	}
}

func (s *AgentSessionStore) getOrCreateLocked(key string, now time.Time) *agentSessionEntry {
	if e, ok := s.sessions[key]; ok {
		return e
	}
	e := &agentSessionEntry{
		AgentSession: AgentSession{
			SessionID:  key,
			StartedAt:  now.Unix(),
			LastSeenAt: now.Unix(),
			Status:     "active",
		},
	}
	s.sessions[key] = e
	return e
}
