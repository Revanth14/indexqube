package main

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	// read*0.1 + write*1.25 + fresh = 52177.6 + 109617.5 + 2904 = 164699.1
	if got := benchEffectiveInput(m); got != 164699 {
		t.Fatalf("effective = %d, want 164699", got)
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

func TestSessionsDBPathHonorsIndexQubeHome(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("INDEXQUBE_HOME", dir)
	if got, want := sessionsDBPath(), filepath.Join(dir, "sessions.db"); got != want {
		t.Fatalf("sessionsDBPath = %q, want %q", got, want)
	}
}

func TestDaemonStateRoundTripAndLocalTelemetryDefault(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("INDEXQUBE_HOME", filepath.Join(dir, ".indexqube"))

	st := daemonState{
		PID:       12345,
		Addr:      defaultDaemonAddr,
		URL:       daemonURL(defaultDaemonAddr),
		LogPath:   filepath.Join(dir, "daemon.log"),
		StartedAt: time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC),
		Version:   "test",
	}
	if err := writeDaemonState(st); err != nil {
		t.Fatalf("writeDaemonState: %v", err)
	}
	got, err := readDaemonState()
	if err != nil {
		t.Fatalf("readDaemonState: %v", err)
	}
	if got.PID != st.PID || got.Addr != st.Addr || got.LogPath != st.LogPath {
		t.Fatalf("state = %+v, want %+v", got, st)
	}

	env := envMap(daemonEnv([]string{"PATH=/bin"}, defaultDaemonAddr))
	if env["IQ_TELEMETRY"] != "off" {
		t.Fatalf("IQ_TELEMETRY=%q, want off", env["IQ_TELEMETRY"])
	}
}

func TestTailLines(t *testing.T) {
	got := tailLines(strings.NewReader("a\nb\nc\nd\n"), 2)
	if strings.Join(got, ",") != "c,d" {
		t.Fatalf("tailLines = %v, want c,d", got)
	}
}

func TestSetupCodexBackupAndRollback(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	t.Setenv("INDEXQUBE_HOME", filepath.Join(dir, ".indexqube"))
	configPath := filepath.Join(dir, ".codex", "config.toml")
	t.Setenv("IQ_CODEX_CONFIG", configPath)

	original := "model = \"gpt-5.5\"\n\n[features]\nhooks = true\n"
	if err := os.MkdirAll(filepath.Dir(configPath), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(configPath, []byte(original), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := setupCodex(defaultDaemonAddr); err != nil {
		t.Fatalf("setupCodex: %v", err)
	}
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read patched config: %v", err)
	}
	patched := string(raw)
	if !strings.Contains(patched, `model_provider = "indexqube"`) {
		t.Fatalf("missing model_provider in patched config:\n%s", patched)
	}
	if !strings.Contains(patched, `[model_providers.indexqube]`) || !strings.Contains(patched, `wire_api = "responses"`) {
		t.Fatalf("missing provider block in patched config:\n%s", patched)
	}
	if strings.Index(patched, `model_provider = "indexqube"`) > strings.Index(patched, "[features]") {
		t.Fatalf("model_provider must stay top-level before tables:\n%s", patched)
	}

	if err := unsetupAgents([]string{"codex"}); err != nil {
		t.Fatalf("unsetupAgents: %v", err)
	}
	restored, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read restored config: %v", err)
	}
	if string(restored) != original {
		t.Fatalf("restored config = %q, want %q", restored, original)
	}
}

func TestSetupClaudeBackupAndRollback(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	t.Setenv("INDEXQUBE_HOME", filepath.Join(dir, ".indexqube"))
	rcPath := filepath.Join(dir, ".zshrc")
	t.Setenv("IQ_CLAUDE_SHELL_RC", rcPath)
	original := "export PATH=/bin\n"
	if err := os.WriteFile(rcPath, []byte(original), 0o600); err != nil {
		t.Fatalf("write rc: %v", err)
	}
	if err := setupClaude(defaultDaemonAddr); err != nil {
		t.Fatalf("setupClaude: %v", err)
	}
	raw, err := os.ReadFile(rcPath)
	if err != nil {
		t.Fatalf("read patched rc: %v", err)
	}
	if !strings.Contains(string(raw), "ANTHROPIC_BASE_URL") {
		t.Fatalf("missing ANTHROPIC_BASE_URL in patched rc:\n%s", raw)
	}
	if err := unsetupAgents([]string{"claude"}); err != nil {
		t.Fatalf("unsetupAgents: %v", err)
	}
	restored, err := os.ReadFile(rcPath)
	if err != nil {
		t.Fatalf("read restored rc: %v", err)
	}
	if string(restored) != original {
		t.Fatalf("restored rc = %q, want %q", restored, original)
	}
}
