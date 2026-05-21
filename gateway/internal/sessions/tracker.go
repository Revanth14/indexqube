// Package sessions provides durable, SQLite-backed persistence for agent
// session observability data. It is the local counterpart to the Supabase
// telemetry sink: kill events and per-session aggregates survive process
// restart and are queryable without a network connection.
//
// Writes are non-blocking — every call to Record enqueues an event on a
// buffered channel. A single background goroutine drains the channel and
// executes upserts, so the hot request path is never blocked by disk I/O.
//
// The schema uses two tables:
//   - agent_sessions: one row per session, upserted on every request
//   - kill_events:    append-only audit log, one row per guard kill
package sessions

import (
	"database/sql"
	"log/slog"
	"sync"
	"time"

	_ "modernc.org/sqlite" // pure-Go SQLite driver, no CGO required

	"github.com/Revanth14/indexqube/gateway/internal/telemetry"
)

const schema = `
PRAGMA journal_mode = WAL;
PRAGMA synchronous  = NORMAL;

CREATE TABLE IF NOT EXISTS agent_sessions (
    session_id          TEXT    PRIMARY KEY,
    started_at          INTEGER NOT NULL,
    last_seen_at        INTEGER NOT NULL,
    tokens_attempted    INTEGER NOT NULL DEFAULT 0,
    tokens_sent         INTEGER NOT NULL DEFAULT 0,
    tokens_deduplicated INTEGER NOT NULL DEFAULT 0,
    requests_total      INTEGER NOT NULL DEFAULT 0,
    loop_detected       INTEGER NOT NULL DEFAULT 0,
    kill_events         INTEGER NOT NULL DEFAULT 0,
    kill_reason         TEXT    NOT NULL DEFAULT '',
    status              TEXT    NOT NULL DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS kill_events (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id          TEXT    NOT NULL,
    timestamp           INTEGER NOT NULL,
    reason              TEXT    NOT NULL,
    tokens_deduplicated INTEGER NOT NULL DEFAULT 0
);
`

// SessionRow is the read-side representation of one row in agent_sessions.
type SessionRow struct {
	SessionID          string `json:"session_id"`
	StartedAt          int64  `json:"started_at"`
	LastSeenAt         int64  `json:"last_seen_at"`
	TokensAttempted    int64  `json:"tokens_attempted"`
	TokensSent         int64  `json:"tokens_sent"`
	TokensDeduplicated int64  `json:"tokens_deduplicated"`
	RequestsTotal      int64  `json:"requests_total"`
	LoopDetected       int64  `json:"loop_detected"`
	KillEvents         int64  `json:"kill_events"`
	KillReason         string `json:"kill_reason"`
	Status             string `json:"status"`
}

// KillRow is the read-side representation of one row in kill_events.
type KillRow struct {
	ID                 int64  `json:"id"`
	SessionID          string `json:"session_id"`
	Timestamp          int64  `json:"timestamp"`
	Reason             string `json:"reason"`
	TokensDeduplicated int64  `json:"tokens_deduplicated"`
}

type writeEvent struct {
	sessionID string
	outcome   telemetry.RequestOutcome
	ts        int64
}

// Tracker persists agent session data to a local SQLite database.
// It is safe for concurrent use. Construct via Open; call Close when done.
type Tracker struct {
	db     *sql.DB
	ch     chan writeEvent
	done   chan struct{}
	wg     sync.WaitGroup
	logger *slog.Logger
}

// Open opens (or creates) the SQLite database at path, applies the schema,
// and starts the background write goroutine.
func Open(path string, logger *slog.Logger) (*Tracker, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // serialise writers; WAL allows concurrent readers

	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, err
	}

	if logger == nil {
		logger = slog.Default()
	}

	t := &Tracker{
		db:     db,
		ch:     make(chan writeEvent, 512),
		done:   make(chan struct{}),
		logger: logger,
	}
	t.wg.Add(1)
	go t.loop()
	return t, nil
}

// Record enqueues a request outcome for async persistence. It never blocks:
// if the channel is full the event is silently dropped rather than stalling
// the request path.
func (t *Tracker) Record(sessionID string, outcome telemetry.RequestOutcome) {
	if sessionID == "" {
		return
	}
	select {
	case t.ch <- writeEvent{sessionID: sessionID, outcome: outcome, ts: time.Now().Unix()}:
	default:
		t.logger.Warn("sessions: write channel full, dropping event",
			slog.String("session_id", sessionID),
		)
	}
}

// Sessions returns all rows from agent_sessions, ordered by last_seen_at
// descending (most recently active first).
func (t *Tracker) Sessions() ([]SessionRow, error) {
	rows, err := t.db.Query(`
		SELECT session_id, started_at, last_seen_at,
		       tokens_attempted, tokens_sent, tokens_deduplicated,
		       requests_total, loop_detected, kill_events,
		       kill_reason, status
		FROM   agent_sessions
		ORDER  BY last_seen_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []SessionRow
	for rows.Next() {
		var r SessionRow
		if err := rows.Scan(
			&r.SessionID, &r.StartedAt, &r.LastSeenAt,
			&r.TokensAttempted, &r.TokensSent, &r.TokensDeduplicated,
			&r.RequestsTotal, &r.LoopDetected, &r.KillEvents,
			&r.KillReason, &r.Status,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// KillLog returns the 100 most recent kill events, newest first.
func (t *Tracker) KillLog() ([]KillRow, error) {
	rows, err := t.db.Query(`
		SELECT id, session_id, timestamp, reason, tokens_deduplicated
		FROM   kill_events
		ORDER  BY id DESC
		LIMIT  100
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []KillRow
	for rows.Next() {
		var r KillRow
		if err := rows.Scan(&r.ID, &r.SessionID, &r.Timestamp, &r.Reason, &r.TokensDeduplicated); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ToAgentSession converts a SQLite row to the telemetry type used by the
// in-memory store and the /v1/agent-sessions API response.
func ToAgentSession(r SessionRow) telemetry.AgentSession {
	return telemetry.AgentSession{
		SessionID:          r.SessionID,
		StartedAt:          r.StartedAt,
		LastSeenAt:         r.LastSeenAt,
		TokensAttempted:    r.TokensAttempted,
		TokensSent:         r.TokensSent,
		TokensDeduplicated: r.TokensDeduplicated,
		RequestsTotal:      int(r.RequestsTotal),
		LoopDetected:       int(r.LoopDetected),
		KillEvents:         int(r.KillEvents),
		KillReason:         r.KillReason,
		Status:             r.Status,
	}
}

// ToKillEvent converts a SQLite kill_events row to the telemetry type.
func ToKillEvent(r KillRow) telemetry.KillEvent {
	return telemetry.KillEvent{
		SessionID:          r.SessionID,
		Timestamp:          r.Timestamp,
		Reason:             r.Reason,
		TokensDeduplicated: r.TokensDeduplicated,
	}
}

// Close drains the write channel, flushes any remaining events to SQLite,
// then closes the database. It blocks until the background goroutine exits.
func (t *Tracker) Close() error {
	close(t.done)
	t.wg.Wait()
	return t.db.Close()
}

// loop is the background goroutine that drains the write channel.
func (t *Tracker) loop() {
	defer t.wg.Done()
	for {
		select {
		case ev := <-t.ch:
			t.write(ev)
		case <-t.done:
			// Drain any events that arrived before Close was called.
			for {
				select {
				case ev := <-t.ch:
					t.write(ev)
				default:
					return
				}
			}
		}
	}
}

// write executes the upsert for one event. Errors are logged and swallowed —
// telemetry must never crash the gateway.
func (t *Tracker) write(ev writeEvent) {
	status := "active"
	if ev.outcome.Killed {
		status = "killed"
	}

	loopInc := 0
	if ev.outcome.Warned {
		loopInc = 1
	}
	killInc := 0
	if ev.outcome.Killed {
		killInc = 1
	}

	killReason := ""
	if ev.outcome.Killed {
		killReason = ev.outcome.GuardReason
	}

	tokensDeduplicated := ev.outcome.TokensAttempted - ev.outcome.TokensSent
	if tokensDeduplicated < 0 {
		tokensDeduplicated = 0
	}

	_, err := t.db.Exec(`
		INSERT INTO agent_sessions
		    (session_id, started_at, last_seen_at,
		     tokens_attempted, tokens_sent, tokens_deduplicated,
		     requests_total, loop_detected, kill_events, kill_reason, status)
		VALUES
		    (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
		ON CONFLICT(session_id) DO UPDATE SET
		    last_seen_at        = excluded.last_seen_at,
		    tokens_attempted    = tokens_attempted    + excluded.tokens_attempted,
		    tokens_sent         = tokens_sent         + excluded.tokens_sent,
		    tokens_deduplicated = tokens_deduplicated + excluded.tokens_deduplicated,
		    requests_total      = requests_total      + 1,
		    loop_detected       = loop_detected       + excluded.loop_detected,
		    kill_events         = kill_events         + excluded.kill_events,
		    kill_reason         = CASE WHEN excluded.kill_reason != ''
		                              THEN excluded.kill_reason
		                              ELSE kill_reason END,
		    status              = CASE WHEN excluded.status = 'killed'
		                              THEN 'killed'
		                              ELSE status END
	`,
		ev.sessionID, ev.ts, ev.ts,
		ev.outcome.TokensAttempted, ev.outcome.TokensSent, tokensDeduplicated,
		loopInc, killInc, killReason, status,
	)
	if err != nil {
		t.logger.Warn("sessions: upsert agent_sessions failed",
			slog.String("session_id", ev.sessionID),
			slog.Any("err", err),
		)
		return
	}

	if ev.outcome.Killed {
		_, err = t.db.Exec(`
			INSERT INTO kill_events (session_id, timestamp, reason, tokens_deduplicated)
			VALUES (?, ?, ?, ?)
		`, ev.sessionID, ev.ts, ev.outcome.GuardReason, tokensDeduplicated)
		if err != nil {
			t.logger.Warn("sessions: insert kill_events failed",
				slog.String("session_id", ev.sessionID),
				slog.Any("err", err),
			)
		}
	}
}
