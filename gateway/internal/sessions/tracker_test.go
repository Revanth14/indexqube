package sessions

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/telemetry"
)

func TestTracker_FilePermissions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "iq-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	tracker, err := Open(filepath.Join(tmpDir, "sessions.db"), logger)
	if err != nil {
		t.Fatalf("failed to open tracker: %v", err)
	}
	defer tracker.Close()

	info, err := os.Stat(filepath.Join(tmpDir, "sessions.db"))
	if err != nil {
		t.Fatalf("failed to stat db: %v", err)
	}
	mode := info.Mode().Perm()
	if mode != 0o600 {
		t.Fatalf("expected file mode 0o600, got 0o%03o", mode)
	}
}

func TestTracker_RecordAndRetrieve(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "iq-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "sessions.db")
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	tracker, err := Open(dbPath, logger)
	if err != nil {
		t.Fatalf("failed to open tracker: %v", err)
	}
	defer tracker.Close()

	sessionID := "test-session-123"
	outcome := telemetry.RequestOutcome{
		TokensAttempted: 1000,
		TokensSent:      300,
		Warned:          false,
		Killed:          false,
	}

	// Record an outcome
	tracker.Record(sessionID, outcome)

	// Give the async goroutine a moment to persist the record
	time.Sleep(100 * time.Millisecond)

	// Fetch by ID
	row, found, err := tracker.SessionByID(sessionID)
	if err != nil {
		t.Fatalf("SessionByID failed: %v", err)
	}
	if !found {
		t.Fatalf("session not found")
	}

	if row.SessionID != sessionID {
		t.Errorf("expected session ID %s, got %s", sessionID, row.SessionID)
	}
	if row.TokensAttempted != 1000 {
		t.Errorf("expected 1000 attempted tokens, got %d", row.TokensAttempted)
	}
	if row.TokensSent != 300 {
		t.Errorf("expected 300 sent tokens, got %d", row.TokensSent)
	}
	if row.TokensDeduplicated != 700 {
		t.Errorf("expected 700 deduplicated tokens, got %d", row.TokensDeduplicated)
	}
	if row.RequestsTotal != 1 {
		t.Errorf("expected 1 request total, got %d", row.RequestsTotal)
	}
	if row.Status != "active" {
		t.Errorf("expected status active, got %s", row.Status)
	}

	// Verify it shows up in Sessions list
	list, err := tracker.Sessions()
	if err != nil {
		t.Fatalf("Sessions failed: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 session in list, got %d", len(list))
	}
	if list[0].SessionID != sessionID {
		t.Errorf("expected list session ID %s, got %s", sessionID, list[0].SessionID)
	}
}

func TestTracker_KillEvent(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "iq-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "sessions.db")
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	tracker, err := Open(dbPath, logger)
	if err != nil {
		t.Fatalf("failed to open tracker: %v", err)
	}
	defer tracker.Close()

	sessionID := "test-session-kill"
	outcome := telemetry.RequestOutcome{
		TokensAttempted: 1000,
		TokensSent:      500,
		Warned:          true,
		Killed:          true,
		GuardReason:     "velocity_exceeded",
	}

	tracker.Record(sessionID, outcome)

	time.Sleep(100 * time.Millisecond)

	row, found, err := tracker.SessionByID(sessionID)
	if err != nil {
		t.Fatalf("SessionByID failed: %v", err)
	}
	if !found {
		t.Fatalf("session not found")
	}

	if row.Status != "killed" {
		t.Errorf("expected status killed, got %s", row.Status)
	}
	if row.KillEvents != 1 {
		t.Errorf("expected 1 kill event, got %d", row.KillEvents)
	}
	if row.KillReason != "velocity_exceeded" {
		t.Errorf("expected kill reason velocity_exceeded, got %s", row.KillReason)
	}

	// Verify KillLog
	killList, err := tracker.KillLog()
	if err != nil {
		t.Fatalf("KillLog failed: %v", err)
	}
	if len(killList) != 1 {
		t.Fatalf("expected 1 kill event in log, got %d", len(killList))
	}
	if killList[0].SessionID != sessionID {
		t.Errorf("expected kill log session ID %s, got %s", sessionID, killList[0].SessionID)
	}
	if killList[0].Reason != "velocity_exceeded" {
		t.Errorf("expected kill log reason velocity_exceeded, got %s", killList[0].Reason)
	}
}
