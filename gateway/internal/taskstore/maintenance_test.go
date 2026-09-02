package taskstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
)

func TestOpenMigratesLegacyDatabaseAndCreatesBackup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tasks.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	legacy := `
CREATE TABLE tasks (
 task_id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, workspace_path TEXT NOT NULL,
 original_goal TEXT NOT NULL, permission_mode TEXT NOT NULL, preferred_backend TEXT NOT NULL,
 status TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 1, created_at INTEGER NOT NULL,
 updated_at INTEGER NOT NULL, retention_deadline INTEGER NOT NULL
);
CREATE TABLE route_attempts (
 route_attempt_id TEXT PRIMARY KEY, turn_id TEXT NOT NULL, ordinal INTEGER NOT NULL,
 backend TEXT NOT NULL, backend_session_id TEXT, decision_reason TEXT NOT NULL,
 status TEXT NOT NULL, failure_class TEXT NOT NULL DEFAULT '', mutation_observed INTEGER NOT NULL DEFAULT 0,
 pre_fingerprint TEXT NOT NULL DEFAULT '', post_fingerprint TEXT NOT NULL DEFAULT '',
 started_at INTEGER NOT NULL, completed_at INTEGER, UNIQUE(turn_id, ordinal)
);`
	if _, err := db.Exec(legacy); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	version, err := store.SchemaVersion(context.Background())
	if err != nil || version != CurrentSchemaVersion {
		t.Fatalf("schema version=%d err=%v", version, err)
	}
	var fallbackColumn int
	rows, err := store.db.Query(`PRAGMA table_info(route_attempts)`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var cid, notNull, primary int
		var name, dataType string
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &primary); err != nil {
			t.Fatal(err)
		}
		if name == "fallback_eligible" {
			fallbackColumn++
		}
	}
	rows.Close()
	if fallbackColumn != 1 {
		t.Fatalf("fallback column count=%d", fallbackColumn)
	}
	backups, err := filepath.Glob(path + ".backup-v0-*")
	if err != nil || len(backups) != 1 {
		t.Fatalf("backups=%v err=%v", backups, err)
	}
	if info, err := os.Stat(backups[0]); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("backup info=%v err=%v", info, err)
	}
}

func TestOpenFailsClosedOnNewerSchemaAndCorruption(t *testing.T) {
	t.Run("newer schema", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "tasks.db")
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`PRAGMA user_version = 999`); err != nil {
			t.Fatal(err)
		}
		db.Close()
		_, err = Open(path)
		if err == nil || !strings.Contains(err.Error(), "newer than supported") {
			t.Fatalf("error=%v", err)
		}
	})

	t.Run("corruption", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "tasks.db")
		if err := os.WriteFile(path, []byte("not a sqlite database"), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := Open(path)
		if err == nil || !strings.Contains(err.Error(), "integrity check failed") {
			t.Fatalf("error=%v", err)
		}
	})
}

func TestBackupIsConsistentAndRefusesOverwrite(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	backup := filepath.Join(t.TempDir(), "manual.sqlite")
	if err := store.Backup(ctx, backup); err != nil {
		t.Fatal(err)
	}
	if err := store.Backup(ctx, backup); err == nil {
		t.Fatal("backup unexpectedly overwrote an existing file")
	}
	copy, err := Open(backup)
	if err != nil {
		t.Fatal(err)
	}
	defer copy.Close()
	if err := copy.IntegrityCheck(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestApplyRetentionDeletesOnlyExpiredClosedTasks(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	create := func(id string) (Task, Turn, RouteAttempt) {
		task, turn, route, err := store.CreateTask(ctx, CreateTaskInput{
			TaskID: id, TurnID: "turn_" + id, RouteAttemptID: "route_" + id,
			WorkspaceID: "ws_" + id, WorkspacePath: "/repo/" + id, Goal: id,
			Permission: agent.PermissionReadOnly, PreferredBackend: agent.BackendFake, Now: base,
		})
		if err != nil {
			t.Fatal(err)
		}
		return task, turn, route
	}
	closed, closedTurn, closedRoute := create("closed")
	if err := store.CancelTurn(ctx, closed.ID, closedTurn.ID, closedRoute.ID, "done", "", false, base); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CloseTask(ctx, closed.ID, base); err != nil {
		t.Fatal(err)
	}
	open, openTurn, openRoute := create("open")
	if err := store.CancelTurn(ctx, open.ID, openTurn.ID, openRoute.ID, "done", "", false, base); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AppendEvent(ctx, agent.Event{TaskID: closed.ID, TurnID: closedTurn.ID, Type: agent.EventCancelled}); err != nil {
		t.Fatal(err)
	}

	result, err := store.ApplyRetention(ctx, base.Add(defaultRetention+time.Second))
	if err != nil || result.TasksDeleted != 1 {
		t.Fatalf("retention=%+v err=%v", result, err)
	}
	if _, ok, err := store.TaskByID(ctx, closed.ID); err != nil || ok {
		t.Fatalf("closed task ok=%v err=%v", ok, err)
	}
	if _, ok, err := store.TaskByID(ctx, open.ID); err != nil || !ok {
		t.Fatalf("open task ok=%v err=%v", ok, err)
	}
	var outbox int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM outbox WHERE task_id=?`, closed.ID).Scan(&outbox); err != nil || outbox != 0 {
		t.Fatalf("outbox=%d err=%v", outbox, err)
	}
}

func TestAppendEventRedactsAndBoundsCanonicalPayload(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	_, _, _, err := store.CreateTask(ctx, CreateTaskInput{
		TaskID: "task_event", TurnID: "turn_event", RouteAttemptID: "route_event",
		WorkspaceID: "ws_event", WorkspacePath: "/repo/event", Goal: "test events",
		Permission: agent.PermissionReadOnly, PreferredBackend: agent.BackendFake, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	secret := "sk-proj-abc1234567890"
	metadata := map[string]string{"authorization": "Bearer " + secret}
	for i := 0; i < maxMetadataItems+10; i++ {
		metadata["key-"+string(rune('a'+i))] = strings.Repeat("v", maxMetadataValue+10)
	}
	event, err := store.AppendEvent(ctx, agent.Event{
		TaskID: "task_event", Type: agent.EventCommandFinished,
		Message:  &agent.MessageEvent{Text: strings.Repeat("m", maxMessageBytes+20) + secret},
		Command:  &agent.CommandEvent{Command: "echo " + secret, AggregatedOutput: strings.Repeat("o", maxOutputBytes+20) + secret},
		Metadata: metadata,
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := jsonMarshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), secret) {
		t.Fatalf("event leaked secret: %s", raw)
	}
	if len(event.Message.Text) > maxMessageBytes || len(event.Command.Command) > maxCommandBytes || len(event.Command.AggregatedOutput) > maxOutputBytes {
		t.Fatalf("event fields not bounded: message=%d command=%d output=%d", len(event.Message.Text), len(event.Command.Command), len(event.Command.AggregatedOutput))
	}
	if len(event.Metadata) != maxMetadataItems {
		t.Fatalf("metadata entries=%d", len(event.Metadata))
	}
	stored, err := store.EventsAfter(ctx, event.TaskID, 0)
	if err != nil || len(stored) != 1 || strings.Contains(stored[0].Command.Command, secret) {
		t.Fatalf("stored=%+v err=%v", stored, err)
	}
}

func TestBackendProcessRegistrySurvivesRestartAndClearsOnExit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tasks.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	process := agent.ProcessInfo{PID: 424242, Token: "unique-token", TaskID: "task", TurnID: "turn", Executable: "/bin/agent", StartedAt: time.Now().UTC()}
	if err := store.ProcessStarted(ctx, process); err != nil {
		t.Fatal(err)
	}
	store.Close()

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	processes, err := reopened.BackendProcesses(ctx)
	if err != nil || len(processes) != 1 || processes[0].Token != process.Token || processes[0].TaskID != process.TaskID {
		t.Fatalf("processes=%+v err=%v", processes, err)
	}
	if err := reopened.ProcessExited(ctx, process.PID); err != nil {
		t.Fatal(err)
	}
	processes, err = reopened.BackendProcesses(ctx)
	if err != nil || len(processes) != 0 {
		t.Fatalf("processes after exit=%+v err=%v", processes, err)
	}
}

func jsonMarshal(value any) ([]byte, error) {
	// Kept local so this test asserts the entire canonical payload, not just
	// selected fields.
	return json.Marshal(value)
}
