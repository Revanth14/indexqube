package taskstore

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
)

func TestCreateTaskPersistsCanonicalBundleAndLineage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tasks.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("mode=%o want 600", got)
	}

	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	task, turn, attempt, err := store.CreateTask(context.Background(), CreateTaskInput{
		TaskID: "task_123", TurnID: "turn_123", RouteAttemptID: "route_123",
		WorkspaceID: "ws_123", WorkspacePath: "/repo", Goal: "hello",
		Permission: agent.PermissionReadOnly, PreferredBackend: agent.BackendFake, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if task.ID != "task_123" || turn.TaskID != task.ID || attempt.TurnID != turn.ID {
		t.Fatalf("invalid bundle: task=%+v turn=%+v attempt=%+v", task, turn, attempt)
	}

	first := BackendSession{
		ID: "bs_1", TaskID: task.ID, Backend: agent.BackendFake, NativeSessionID: "native_1",
		CreationReason: "initial", Status: "active", CreatedAt: now,
	}
	if err := store.CreateBackendSession(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	second := BackendSession{
		ID: "bs_2", TaskID: task.ID, Backend: agent.BackendFake, NativeSessionID: "native_2",
		PredecessorID: first.ID, CreationReason: "resume_recovery", Status: "active", CreatedAt: now.Add(time.Second),
	}
	if err := store.CreateBackendSession(context.Background(), second); err != nil {
		t.Fatal(err)
	}

	for table, want := range map[string]int{"tasks": 1, "turns": 1, "route_attempts": 1, "backend_sessions": 2} {
		got, err := store.CountRows(context.Background(), table)
		if err != nil || got != want {
			t.Fatalf("%s count=%d err=%v want=%d", table, got, err, want)
		}
	}
}

func TestAppendEventIsOrderedAndTransactionalWithOutbox(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "tasks.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	_, _, _, err = store.CreateTask(ctx, CreateTaskInput{
		TaskID: "task_1", TurnID: "turn_1", RouteAttemptID: "route_1", WorkspaceID: "ws_1",
		WorkspacePath: "/repo", Goal: "hello", Permission: agent.PermissionReadOnly,
		PreferredBackend: agent.BackendFake, Now: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, typ := range []agent.EventType{agent.EventSessionStarted, agent.EventCompleted} {
		if _, err := store.AppendEvent(ctx, agent.Event{TaskID: "task_1", TurnID: "turn_1", Type: typ, Backend: agent.BackendFake}); err != nil {
			t.Fatal(err)
		}
	}
	events, err := store.EventsAfter(ctx, "task_1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Sequence != 1 || events[1].Sequence != 2 {
		t.Fatalf("events=%+v", events)
	}
	if count, _ := store.CountRows(ctx, "outbox"); count != 2 {
		t.Fatalf("outbox count=%d want 2", count)
	}
}
