package main

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

// TestReadSessionMetrics covers the reusable DB read that both the session
// summary and the `iq bench` A/B comparison depend on: a row is located by the
// IQ_SESSION_ID suffix the proxy appends to session_id, and the real
// Anthropic-reported usage is returned (not the byte estimate).
func TestReadSessionMetrics(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "sessions.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE agent_sessions (
		session_id            TEXT    PRIMARY KEY,
		last_seen_at          INTEGER NOT NULL DEFAULT 0,
		requests_total        INTEGER NOT NULL DEFAULT 0,
		tokens_attempted      INTEGER NOT NULL DEFAULT 0,
		tokens_sent           INTEGER NOT NULL DEFAULT 0,
		tokens_deduplicated   INTEGER NOT NULL DEFAULT 0,
		input_tokens_real     INTEGER NOT NULL DEFAULT 0,
		cache_read_tokens     INTEGER NOT NULL DEFAULT 0,
		cache_creation_tokens INTEGER NOT NULL DEFAULT 0,
		status                TEXT    NOT NULL DEFAULT 'active'
	)`); err != nil {
		t.Fatalf("create: %v", err)
	}

	sessionID := "abcdef1234567890" // proxy stores session_id ending in sessionID[:8]
	if _, err := db.Exec(`INSERT INTO agent_sessions
		(session_id, last_seen_at, requests_total, tokens_attempted, tokens_deduplicated,
		 input_tokens_real, cache_read_tokens, cache_creation_tokens, status)
		VALUES (?,?,?,?,?,?,?,?,?)`,
		"deadbeef-"+sessionID[:8], 100, 11, 395276, 79, 612374, 521776, 87694, "active"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	m, ok := readSessionMetrics(dbPath, sessionID)
	if !ok {
		t.Fatalf("readSessionMetrics ok=false, want true")
	}
	if m.requests != 11 || m.inputReal != 612374 || m.cacheRead != 521776 || m.cacheCreation != 87694 {
		t.Fatalf("metrics = %+v", m)
	}
	if got := benchCacheRatio(m); got < 85.1 || got > 85.3 {
		t.Fatalf("cache ratio = %.2f, want ~85.2", got)
	}
	if got := benchFreshInput(m); got != 612374-521776-87694 {
		t.Fatalf("fresh = %d, want %d", got, 612374-521776-87694)
	}

	// Unknown session must report not-found rather than a bogus row.
	if _, ok := readSessionMetrics(dbPath, "0000000000000000"); ok {
		t.Fatalf("expected not-found for unknown session")
	}
}

func TestSignedNumber(t *testing.T) {
	cases := map[int64]string{0: "0", 1234: "+1,234", -1234: "-1,234"}
	for in, want := range cases {
		if got := signedNumber(in); got != want {
			t.Fatalf("signedNumber(%d) = %q, want %q", in, got, want)
		}
	}
}
