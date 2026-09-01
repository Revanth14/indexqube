package taskstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "tasks.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

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

func TestTaskEvidenceSurvivesStoreReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tasks.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	task, turn, attempt, err := store.CreateTask(ctx, CreateTaskInput{
		TaskID: "task_evidence", TurnID: "turn_evidence", RouteAttemptID: "route_evidence",
		WorkspaceID: "ws_evidence", WorkspacePath: "/repo", Goal: "change a file",
		Permission: agent.PermissionWrite, PreferredBackend: agent.BackendCodex, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StartTurn(ctx, task.ID, turn.ID, attempt.ID, 7, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	exitCode := 0
	for _, event := range []agent.Event{
		{TaskID: task.ID, TurnID: turn.ID, Type: agent.EventCommandFinished, Backend: agent.BackendCodex,
			Command: &agent.CommandEvent{Command: "go test ./...", Status: "completed", ExitCode: &exitCode}, Timestamp: now.Add(2 * time.Second)},
		{TaskID: task.ID, TurnID: turn.ID, Type: agent.EventFileChanged, Backend: agent.BackendCodex,
			File: &agent.FileEvent{Path: "client.go", Operation: "update"}, Timestamp: now.Add(3 * time.Second)},
	} {
		if _, err := store.AppendEvent(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.AddWorkspaceFileDeltas(ctx, []WorkspaceFileDelta{{
		ID: "delta_1", TaskID: task.ID, TurnID: turn.ID, Path: "client.go", Operation: "modified",
		AfterFingerprint: "after", RecordedAt: now.Add(3 * time.Second),
	}}); err != nil {
		t.Fatal(err)
	}
	if err := store.AddSnapshot(ctx, WorkspaceSnapshot{
		ID: "snap_evidence", TaskID: task.ID, TurnID: turn.ID, Phase: "post", WorkspaceID: task.WorkspaceID,
		StagedHash: "staged", UnstagedHash: "unstaged", UntrackedHash: "untracked", Fingerprint: "post",
		CapturedAt: now.Add(3 * time.Second), Files: []WorkspaceFileState{{
			SnapshotID: "snap_evidence", TaskID: task.ID, TurnID: turn.ID, Path: "client.go",
			WorktreeStatus: "M", Fingerprint: "after",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteTurn(ctx, task.ID, turn.ID, attempt.ID, "done", "post", true, false, now.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	tasks, err := reopened.ListTasks(ctx, 10)
	if err != nil || len(tasks) != 1 || tasks[0].ID != task.ID {
		t.Fatalf("tasks=%+v err=%v", tasks, err)
	}
	evidence, ok, err := reopened.TaskEvidence(ctx, task.ID)
	if err != nil || !ok {
		t.Fatalf("evidence ok=%v err=%v", ok, err)
	}
	if len(evidence.Commands) != 1 || evidence.Commands[0].Command != "go test ./..." {
		t.Fatalf("commands=%+v", evidence.Commands)
	}
	if len(evidence.Files) != 1 || evidence.Files[0].Path != "client.go" {
		t.Fatalf("files=%+v", evidence.Files)
	}
	if len(evidence.ReportedFiles) != 1 || evidence.EvidenceMismatch {
		t.Fatalf("reported=%+v mismatch=%v", evidence.ReportedFiles, evidence.EvidenceMismatch)
	}
	if len(evidence.Turns) != 1 || evidence.Turns[0].AssistantMessage != "done" || len(evidence.Routes) != 1 {
		t.Fatalf("evidence=%+v", evidence)
	}
	if len(evidence.Snapshots) != 1 || len(evidence.Snapshots[0].Files) != 1 || evidence.Snapshots[0].Files[0].Fingerprint != "after" {
		t.Fatalf("snapshots=%+v", evidence.Snapshots)
	}
}

func TestApprovalDecisionIsDurableAndOneShot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tasks.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	now := time.Date(2026, 9, 1, 15, 0, 0, 0, time.UTC)
	task, turn, attempt, err := store.CreateTask(ctx, CreateTaskInput{
		TaskID: "task_approval", TurnID: "turn_approval", RouteAttemptID: "route_approval",
		WorkspaceID: "ws", WorkspacePath: "/repo", Goal: "run guarded command",
		Permission: agent.PermissionWrite, PreferredBackend: agent.BackendCodex, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StartTurn(ctx, task.ID, turn.ID, attempt.ID, 3, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	approval, err := store.CreateApproval(ctx, CreateApprovalInput{Approval: Approval{
		ID: "approval_1", TaskID: task.ID, TurnID: turn.ID, Backend: agent.BackendCodex,
		BackendRequestID: "91", Kind: agent.ApprovalCommand, ItemID: "cmd_1",
		Reason: "network access", Command: "curl https://example.com", CWD: "/repo",
	}, Now: now.Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if approval.Status != ApprovalPending {
		t.Fatalf("approval=%+v", approval)
	}
	state, ok, err := store.TaskState(ctx, task.ID)
	if err != nil || !ok || state.Task.Status != TaskAwaitingApproval || state.LatestTurn.Status != TurnAwaitingApproval {
		t.Fatalf("state=%+v ok=%v err=%v", state, ok, err)
	}
	resolved, err := store.ResolveApproval(ctx, approval.ID, agent.ApprovalAccept, ApprovalApproved, now.Add(3*time.Second))
	if err != nil || resolved.Status != ApprovalApproved || resolved.Decision != agent.ApprovalAccept {
		t.Fatalf("resolved=%+v err=%v", resolved, err)
	}
	if _, err := store.ResolveApproval(ctx, approval.ID, agent.ApprovalDecline, ApprovalDenied, now.Add(4*time.Second)); !errors.Is(err, ErrApprovalNotPending) {
		t.Fatalf("second decision error=%v", err)
	}
	state, ok, err = store.TaskState(ctx, task.ID)
	if err != nil || !ok || state.Task.Status != TaskRunning || state.LatestTurn.Status != TurnRunning {
		t.Fatalf("resumed state=%+v ok=%v err=%v", state, ok, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	approvals, err := reopened.ListApprovals(ctx, task.ID, "", 10)
	if err != nil || len(approvals) != 1 || approvals[0].Status != ApprovalApproved || approvals[0].DecidedAt == nil {
		t.Fatalf("approvals=%+v err=%v", approvals, err)
	}
	evidence, ok, err := reopened.TaskEvidence(ctx, task.ID)
	if err != nil || !ok || len(evidence.Approvals) != 1 || evidence.Approvals[0].ID != approval.ID {
		t.Fatalf("evidence=%+v ok=%v err=%v", evidence, ok, err)
	}
}

func TestTerminalTurnCancelsPendingApproval(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	task, turn, attempt, err := store.CreateTask(ctx, CreateTaskInput{
		TaskID: "task_cancel_approval", TurnID: "turn_cancel_approval", RouteAttemptID: "route_cancel_approval",
		WorkspaceID: "ws", WorkspacePath: "/repo", Goal: "guarded", Permission: agent.PermissionReadOnly,
		PreferredBackend: agent.BackendFake, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StartTurn(ctx, task.ID, turn.ID, attempt.ID, 0, now); err != nil {
		t.Fatal(err)
	}
	approval, err := store.CreateApproval(ctx, CreateApprovalInput{Approval: Approval{
		ID: "approval_cancel", TaskID: task.ID, TurnID: turn.ID, Backend: agent.BackendFake,
		BackendRequestID: "request_cancel", Kind: agent.ApprovalCommand,
	}, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CancelTurn(ctx, task.ID, turn.ID, attempt.ID, "cancelled", "", false, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	stored, ok, err := store.ApprovalByID(ctx, approval.ID)
	if err != nil || !ok || stored.Status != ApprovalCancelled || stored.Decision != agent.ApprovalCancel {
		t.Fatalf("approval=%+v ok=%v err=%v", stored, ok, err)
	}
}
