package orchestrator

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
	codexbackend "github.com/Revanth14/indexqube/gateway/internal/agent/codex"
	"github.com/Revanth14/indexqube/gateway/internal/agent/fake"
	"github.com/Revanth14/indexqube/gateway/internal/taskstore"
	"github.com/Revanth14/indexqube/gateway/internal/workspace"
)

func TestFakeAgentProcess(t *testing.T) {
	if os.Getenv("INDEXQUBE_FAKE_HELPER") != "1" {
		return
	}
	os.Exit(fake.RunHelper(os.Stdin, os.Stdout, os.Stderr))
}

func TestOrchestratorCodexProcess(t *testing.T) {
	mode := os.Getenv("INDEXQUBE_ORCHESTRATOR_CODEX_HELPER")
	if mode == "" {
		return
	}
	enc := json.NewEncoder(os.Stdout)
	if mode == "resume-lost" && slices.Contains(os.Args, "resume") {
		_ = enc.Encode(map[string]any{"type": "error", "message": "session not found"})
		os.Exit(3)
	}
	prompt, _ := io.ReadAll(os.Stdin)
	sessionID := "codex-thread-fixture"
	if strings.Contains(string(prompt), "INDEXQUBE CANONICAL SESSION RECOVERY") {
		sessionID = "codex-thread-recovered"
	}
	_ = enc.Encode(map[string]any{"type": "thread.started", "thread_id": sessionID})
	_ = enc.Encode(map[string]any{"type": "item.completed", "item": map[string]any{
		"id": "message-1", "type": "agent_message", "text": "codex fixture answer",
	}})
	_ = enc.Encode(map[string]any{"type": "turn.completed"})
	os.Exit(0)
}

func TestFakeTaskPersistsCanonicalMilestone(t *testing.T) {
	service, store, root := newTestService(t)
	task, err := service.StartTask(context.Background(), StartTaskInput{
		Workspace: root, Prompt: "hello", Provider: agent.BackendFake, Permission: agent.PermissionReadOnly,
	})
	if err != nil {
		t.Fatal(err)
	}
	events := waitForTerminal(t, service, task.ID)
	if events[len(events)-1].Type != agent.EventCompleted {
		last := events[len(events)-1]
		if last.Result != nil {
			t.Fatalf("terminal event=%s result=%+v", last.Type, *last.Result)
		}
		t.Fatalf("terminal event=%+v", last)
	}
	for table, want := range map[string]int{
		"tasks": 1, "turns": 1, "backend_sessions": 1, "route_attempts": 1, "workspace_snapshots": 2,
	} {
		got, err := store.CountRows(context.Background(), table)
		if err != nil || got != want {
			t.Fatalf("%s count=%d err=%v want=%d", table, got, err, want)
		}
	}
	stored, ok, err := service.Task(context.Background(), task.ID)
	if err != nil || !ok || stored.Status != taskstore.TaskOpen {
		t.Fatalf("stored task=%+v ok=%v err=%v", stored, ok, err)
	}
}

func TestCodexReadOnlyTaskUsesCanonicalStateAndRejectsWriteBeforeCreation(t *testing.T) {
	service, store, root := newTestService(t)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	service.registry = NewRegistry(codexbackend.NewCommand(agent.NewRunner(), binary,
		[]string{"-test.run=TestOrchestratorCodexProcess", "--"},
		[]string{"INDEXQUBE_ORCHESTRATOR_CODEX_HELPER=1"}, "codex-cli test"))
	task, err := service.StartTask(context.Background(), StartTaskInput{
		Workspace: root, Prompt: "analyze", Provider: agent.BackendCodex, Permission: agent.PermissionReadOnly,
	})
	if err != nil {
		t.Fatal(err)
	}
	events := waitForTerminal(t, service, task.ID)
	if events[len(events)-1].Type != agent.EventCompleted {
		t.Fatalf("terminal=%+v", events[len(events)-1])
	}
	session, ok, err := store.LatestBackendSession(context.Background(), task.ID, agent.BackendCodex)
	if err != nil || !ok || session.NativeSessionID != "codex-thread-fixture" {
		t.Fatalf("session=%+v ok=%v err=%v", session, ok, err)
	}
	before, _ := store.CountRows(context.Background(), "tasks")
	if _, err := service.StartTask(context.Background(), StartTaskInput{
		Workspace: root, Prompt: "write", Provider: agent.BackendCodex, Permission: agent.PermissionWrite,
	}); err == nil {
		t.Fatal("expected Codex write permission rejection")
	}
	after, _ := store.CountRows(context.Background(), "tasks")
	if after != before {
		t.Fatalf("write rejection created task: before=%d after=%d", before, after)
	}

	afterSequence, err := service.LatestEventSequence(context.Background(), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	service.registry = NewRegistry(codexbackend.NewCommand(agent.NewRunner(), binary,
		[]string{"-test.run=TestOrchestratorCodexProcess", "--"},
		[]string{"INDEXQUBE_ORCHESTRATOR_CODEX_HELPER=resume-lost"}, "codex-cli test"))
	if _, err := service.ContinueTask(context.Background(), ContinueTaskInput{TaskID: task.ID, Prompt: "continue analysis"}); err != nil {
		t.Fatal(err)
	}
	events = waitForTerminalAfter(t, service, task.ID, afterSequence)
	if events[len(events)-1].Type != agent.EventCompleted {
		t.Fatalf("recovery terminal=%+v", events[len(events)-1])
	}
	recovered, ok, err := store.LatestBackendSession(context.Background(), task.ID, agent.BackendCodex)
	if err != nil || !ok || recovered.NativeSessionID != "codex-thread-recovered" || recovered.PredecessorID != session.ID {
		t.Fatalf("recovered session=%+v ok=%v err=%v", recovered, ok, err)
	}
}

func TestFakeMutationUsesWriteEpochAndStaleEventFails(t *testing.T) {
	service, store, root := newTestService(t)
	task, err := service.StartTask(context.Background(), StartTaskInput{
		Workspace: root, Prompt: "[fake:mutate][fake:stale]", Provider: agent.BackendFake, Permission: agent.PermissionWrite,
	})
	if err != nil {
		t.Fatal(err)
	}
	events := waitForTerminal(t, service, task.ID)
	if events[len(events)-1].Type != agent.EventError {
		t.Fatalf("terminal event=%s want error", events[len(events)-1].Type)
	}
	stored, _, err := service.Task(context.Background(), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != taskstore.TaskNeedsAttention {
		t.Fatalf("task status=%s want needs_attention", stored.Status)
	}
	if count, _ := store.CountRows(context.Background(), "workspace_write_epochs"); count != 1 {
		t.Fatalf("write epoch count=%d", count)
	}
}

func TestFakeFailureBeforeMutationLeavesTaskRecoverable(t *testing.T) {
	service, _, root := newTestService(t)
	task, err := service.StartTask(context.Background(), StartTaskInput{
		Workspace: root, Prompt: "[fake:fail]", Provider: agent.BackendFake, Permission: agent.PermissionReadOnly,
	})
	if err != nil {
		t.Fatal(err)
	}
	events := waitForTerminal(t, service, task.ID)
	if events[len(events)-1].Type != agent.EventError {
		t.Fatalf("terminal event=%s want error", events[len(events)-1].Type)
	}
	stored, _, err := service.Task(context.Background(), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != taskstore.TaskOpen {
		t.Fatalf("task status=%s want open", stored.Status)
	}
}

func TestFakeMutationAndFailureNeedsAttention(t *testing.T) {
	service, _, root := newTestService(t)
	task, err := service.StartTask(context.Background(), StartTaskInput{
		Workspace: root, Prompt: "[fake:mutate][fake:fail]", Provider: agent.BackendFake, Permission: agent.PermissionWrite,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForTerminal(t, service, task.ID)
	stored, _, err := service.Task(context.Background(), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != taskstore.TaskNeedsAttention {
		t.Fatalf("task status=%s want needs_attention", stored.Status)
	}
}

func TestFakeCancellationStopsChildAndCommitsCancelledEvent(t *testing.T) {
	service, _, root := newTestService(t)
	task, err := service.StartTask(context.Background(), StartTaskInput{
		Workspace: root, Prompt: "[fake:sleep]", Provider: agent.BackendFake, Permission: agent.PermissionReadOnly,
	})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		events, err := service.EventsAfter(context.Background(), task.ID, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range events {
			if event.Type == agent.EventSessionStarted {
				if !service.Cancel(task.ID) {
					t.Fatal("cancel returned false")
				}
				events = waitForTerminal(t, service, task.ID)
				if events[len(events)-1].Type != agent.EventCancelled {
					t.Fatalf("terminal event=%s want cancelled", events[len(events)-1].Type)
				}
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("fake backend did not start")
}

func TestLostNativeSessionStartsNewSessionFromCanonicalState(t *testing.T) {
	service, store, root := newTestService(t)
	task, err := service.StartTask(context.Background(), StartTaskInput{
		Workspace: root, Prompt: "first", Provider: agent.BackendFake, Permission: agent.PermissionReadOnly,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForTerminal(t, service, task.ID)
	after, err := service.LatestEventSequence(context.Background(), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.ContinueTask(context.Background(), ContinueTaskInput{
		TaskID: task.ID, Prompt: "[fake:resume-lost] recover me",
	}); err != nil {
		t.Fatal(err)
	}
	events := waitForTerminalAfter(t, service, task.ID, after)
	if events[len(events)-1].Type != agent.EventCompleted {
		t.Fatalf("terminal event=%s want completed", events[len(events)-1].Type)
	}
	foundWarning := false
	for _, event := range events {
		if event.Type == agent.EventWarning && event.Metadata["error_code"] == "resume_lost" {
			foundWarning = true
		}
	}
	if !foundWarning {
		t.Fatal("missing native-session recovery warning")
	}
	for table, want := range map[string]int{"turns": 2, "backend_sessions": 2, "route_attempts": 3, "workspace_snapshots": 5} {
		got, err := store.CountRows(context.Background(), table)
		if err != nil || got != want {
			t.Fatalf("%s count=%d err=%v want=%d", table, got, err, want)
		}
	}
	latest, ok, err := store.LatestBackendSession(context.Background(), task.ID, agent.BackendFake)
	if err != nil || !ok {
		t.Fatalf("latest session ok=%v err=%v", ok, err)
	}
	if latest.CreationReason != "native_session_recovery" || latest.PredecessorID == "" {
		t.Fatalf("latest session=%+v", latest)
	}
}

func TestReconcileInterruptedTurns(t *testing.T) {
	cases := []struct {
		name       string
		permission agent.PermissionMode
		started    bool
		wantStatus taskstore.TaskStatus
		wantCode   string
	}{
		{name: "queued write never became mutation capable", permission: agent.PermissionWrite, wantStatus: taskstore.TaskOpen, wantCode: "daemon_interrupted_pre_run"},
		{name: "running read only is recoverable", permission: agent.PermissionReadOnly, started: true, wantStatus: taskstore.TaskOpen, wantCode: "daemon_interrupted_read_only"},
		{name: "running write needs attention", permission: agent.PermissionWrite, started: true, wantStatus: taskstore.TaskNeedsAttention, wantCode: "daemon_interrupted_write"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			service, store, root := newTestService(t)
			identity, err := workspace.Resolve(context.Background(), root)
			if err != nil {
				t.Fatal(err)
			}
			task, turn, attempt, err := store.CreateTask(context.Background(), taskstore.CreateTaskInput{
				TaskID: taskstore.NewID("task"), TurnID: taskstore.NewID("turn"), RouteAttemptID: taskstore.NewID("route"),
				WorkspaceID: identity.ID, WorkspacePath: identity.Root, Goal: "interrupted", Permission: tc.permission,
				PreferredBackend: agent.BackendFake, Now: time.Now().UTC(),
			})
			if err != nil {
				t.Fatal(err)
			}
			if tc.started {
				if err := store.StartTurn(context.Background(), task.ID, turn.ID, attempt.ID, 1, time.Now().UTC()); err != nil {
					t.Fatal(err)
				}
			}
			report, err := service.ReconcileInterrupted(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantStatus == taskstore.TaskNeedsAttention && report.NeedsAttention != 1 {
				t.Fatalf("report=%+v", report)
			}
			if tc.wantStatus == taskstore.TaskOpen && report.Recovered != 1 {
				t.Fatalf("report=%+v", report)
			}
			state, ok, err := service.TaskState(context.Background(), task.ID)
			if err != nil || !ok {
				t.Fatalf("state ok=%v err=%v", ok, err)
			}
			if state.Task.Status != tc.wantStatus || state.LatestTurn == nil || state.LatestTurn.ErrorCode != tc.wantCode {
				t.Fatalf("state=%+v", state)
			}
			events, err := service.EventsAfter(context.Background(), task.ID, 0)
			if err != nil || len(events) != 1 || events[0].Metadata["error_code"] != tc.wantCode {
				t.Fatalf("events=%+v err=%v", events, err)
			}
		})
	}
}

func newTestService(t *testing.T) (*Service, *taskstore.Store, string) {
	t.Helper()
	root := t.TempDir()
	runGit(t, root, "init", "-q")
	runGit(t, root, "config", "user.email", "test@indexqube.local")
	runGit(t, root, "config", "user.name", "IndexQube Test")
	if err := os.WriteFile(filepath.Join(root, "README.md"), []byte("test\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, root, "add", "README.md")
	runGit(t, root, "commit", "-q", "-m", "initial")
	state := t.TempDir()
	store, err := taskstore.Open(filepath.Join(state, "tasks.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	locks, err := workspace.NewLockManager(filepath.Join(state, "locks"), store, "test-daemon")
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	backend := fake.NewCommand(agent.NewRunner(), binary, []string{"-test.run=TestFakeAgentProcess"}, []string{"INDEXQUBE_FAKE_HELPER=1"})
	service, err := NewService(context.Background(), store, locks, NewRegistry(backend))
	if err != nil {
		t.Fatal(err)
	}
	return service, store, root
}

func waitForTerminal(t *testing.T, service *Service, taskID string) []agent.Event {
	return waitForTerminalAfter(t, service, taskID, 0)
}

func waitForTerminalAfter(t *testing.T, service *Service, taskID string, after int64) []agent.Event {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		events, err := service.EventsAfter(context.Background(), taskID, after)
		if err != nil {
			t.Fatal(err)
		}
		if len(events) > 0 {
			last := events[len(events)-1]
			if last.Type == agent.EventCompleted || last.Type == agent.EventError || last.Type == agent.EventCancelled {
				return events
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("timed out waiting for terminal event")
	return nil
}

func runGit(t *testing.T, root string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", root}, args...)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
}
