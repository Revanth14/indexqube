package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
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
	"github.com/Revanth14/indexqube/gateway/internal/verification"
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
	writeChange := (mode == "write" || mode == "write-go" || mode == "unreported-write" || mode == "failed-write") &&
		!strings.Contains(string(prompt), "inspect the durable change")
	changePath := "codex-orchestrated-write.txt"
	changeContent := "durable write evidence\n"
	if mode == "write-go" {
		changePath = "verified_change.go"
		changeContent = "package fixture\n\nconst VerifiedChange = true\n"
	}
	if writeChange {
		if os.Getenv("INDEXQUBE_WORKSPACE_LOCK_FD") == "" {
			os.Exit(8)
		}
		if err := os.WriteFile(changePath, []byte(changeContent), 0o600); err != nil {
			os.Exit(9)
		}
	}
	sessionID := "codex-thread-fixture"
	if strings.Contains(string(prompt), "INDEXQUBE CANONICAL SESSION RECOVERY") {
		sessionID = "codex-thread-recovered"
	}
	_ = enc.Encode(map[string]any{"type": "thread.started", "thread_id": sessionID})
	_ = enc.Encode(map[string]any{"type": "item.completed", "item": map[string]any{
		"id": "command-1", "type": "command_execution", "command": "go test ./...", "status": "completed",
		"exit_code": 0, "aggregated_output": "ok",
	}})
	if writeChange && (mode == "write" || mode == "write-go") {
		_ = enc.Encode(map[string]any{"type": "item.completed", "item": map[string]any{
			"id": "file-1", "type": "file_change", "changes": []map[string]any{{"path": changePath, "kind": "add"}},
		}})
	}
	if mode == "failed-write" {
		_ = enc.Encode(map[string]any{"type": "error", "message": "fixture failed after write"})
		os.Exit(3)
	}
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

func TestCodexReadOnlyTaskUsesCanonicalStateAndRecoversLostSession(t *testing.T) {
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

func TestCodexWriteTaskPersistsEvidenceAndContinues(t *testing.T) {
	service, store, root := newTestService(t)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	service.registry = NewRegistry(codexbackend.NewCommand(agent.NewRunner(), binary,
		[]string{"-test.run=TestOrchestratorCodexProcess", "--"},
		[]string{"INDEXQUBE_ORCHESTRATOR_CODEX_HELPER=write"}, "codex-cli test"))
	task, err := service.StartTask(context.Background(), StartTaskInput{
		Workspace: root, Prompt: "make a durable change", Backend: agent.BackendCodex, Permission: agent.PermissionWrite,
	})
	if err != nil {
		t.Fatal(err)
	}
	events := waitForTerminal(t, service, task.ID)
	if events[len(events)-1].Type != agent.EventCompleted {
		t.Fatalf("terminal=%+v", events[len(events)-1])
	}
	if raw, err := os.ReadFile(filepath.Join(root, "codex-orchestrated-write.txt")); err != nil || string(raw) != "durable write evidence\n" {
		t.Fatalf("workspace write raw=%q err=%v", raw, err)
	}
	evidence, ok, err := store.TaskEvidence(context.Background(), task.ID)
	if err != nil || !ok {
		t.Fatalf("evidence ok=%v err=%v", ok, err)
	}
	if len(evidence.Commands) != 1 || evidence.Commands[0].Command != "go test ./..." {
		t.Fatalf("commands=%+v", evidence.Commands)
	}
	if len(evidence.Files) != 1 || evidence.Files[0].Path != "codex-orchestrated-write.txt" {
		t.Fatalf("files=%+v", evidence.Files)
	}
	if len(evidence.Snapshots) != 2 || !evidence.Routes[0].MutationObserved {
		t.Fatalf("snapshots=%d routes=%+v", len(evidence.Snapshots), evidence.Routes)
	}

	after, err := service.LatestEventSequence(context.Background(), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.ContinueTask(context.Background(), ContinueTaskInput{TaskID: task.ID, Prompt: "inspect the durable change"}); err != nil {
		t.Fatal(err)
	}
	events = waitForTerminalAfter(t, service, task.ID, after)
	if events[len(events)-1].Type != agent.EventCompleted {
		t.Fatalf("continuation terminal=%+v", events[len(events)-1])
	}
	evidence, ok, err = store.TaskEvidence(context.Background(), task.ID)
	if err != nil || !ok || len(evidence.Turns) != 2 || len(evidence.Routes) != 2 || evidence.Task.Status != taskstore.TaskOpen {
		t.Fatalf("continued evidence=%+v ok=%v err=%v", evidence, ok, err)
	}
}

func TestFailedPostTurnVerificationNeedsAttentionAndPersistsEvidence(t *testing.T) {
	service, store, root := newTestService(t)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	service.registry = NewRegistry(codexbackend.NewCommand(agent.NewRunner(), binary,
		[]string{"-test.run=TestOrchestratorCodexProcess", "--"},
		[]string{"INDEXQUBE_ORCHESTRATOR_CODEX_HELPER=write"}, "codex-cli test"))
	exit := 1
	now := time.Now().UTC()
	service.verifier = fixedVerifier{result: verification.Result{
		Status: verification.StatusFailed, Summary: "1 of 1 verification check(s) failed",
		StartedAt: now, CompletedAt: now.Add(time.Second),
		Checks: []verification.CheckResult{{
			Name: "Go tests", Kind: "test", Command: "go test -mod=readonly ./...", CWD: ".",
			Status: verification.CheckFailed, ExitCode: &exit, Output: "FAIL", StartedAt: now,
			CompletedAt: now.Add(time.Second),
		}},
	}}
	task, err := service.StartTask(context.Background(), StartTaskInput{
		Workspace: root, Prompt: "make a verified change", Backend: agent.BackendCodex, Permission: agent.PermissionWrite,
	})
	if err != nil {
		t.Fatal(err)
	}
	events := waitForTerminal(t, service, task.ID)
	if events[len(events)-1].Type != agent.EventCompleted {
		t.Fatalf("terminal=%+v", events[len(events)-1])
	}
	foundVerificationEvent := false
	for _, event := range events {
		if event.Type == agent.EventVerificationCompleted &&
			event.Metadata["verification_status"] == string(taskstore.VerificationFailed) {
			foundVerificationEvent = true
		}
	}
	if !foundVerificationEvent {
		t.Fatalf("events=%+v", events)
	}
	evidence, ok, err := store.TaskEvidence(context.Background(), task.ID)
	if err != nil || !ok {
		t.Fatalf("evidence ok=%v err=%v", ok, err)
	}
	if evidence.Task.Status != taskstore.TaskNeedsAttention || len(evidence.VerificationRuns) != 1 ||
		evidence.VerificationRuns[0].Status != taskstore.VerificationFailed ||
		len(evidence.VerificationRuns[0].Checks) != 1 {
		t.Fatalf("evidence=%+v", evidence)
	}
}

func TestSuccessfulGoChangeRunsAutomaticPostTurnVerification(t *testing.T) {
	service, store, root := newTestService(t)
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/indexqube-fixture\n\ngo 1.22\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "fixture.go"), []byte("package fixture\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, root, "add", "go.mod", "fixture.go")
	runGit(t, root, "commit", "-q", "-m", "add go fixture")
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	service.registry = NewRegistry(codexbackend.NewCommand(agent.NewRunner(), binary,
		[]string{"-test.run=TestOrchestratorCodexProcess", "--"},
		[]string{"INDEXQUBE_ORCHESTRATOR_CODEX_HELPER=write-go"}, "codex-cli test"))
	task, err := service.StartTask(context.Background(), StartTaskInput{
		Workspace: root, Prompt: "make a verified Go change", Backend: agent.BackendCodex, Permission: agent.PermissionWrite,
	})
	if err != nil {
		t.Fatal(err)
	}
	events := waitForTerminal(t, service, task.ID)
	if events[len(events)-1].Type != agent.EventCompleted {
		t.Fatalf("terminal=%+v", events[len(events)-1])
	}
	evidence, ok, err := store.TaskEvidence(context.Background(), task.ID)
	if err != nil || !ok {
		t.Fatalf("evidence ok=%v err=%v", ok, err)
	}
	if evidence.Task.Status != taskstore.TaskOpen || len(evidence.VerificationRuns) != 1 ||
		evidence.VerificationRuns[0].Status != taskstore.VerificationPassed ||
		len(evidence.VerificationRuns[0].Checks) != 1 ||
		evidence.VerificationRuns[0].Checks[0].Command != "go test -mod=readonly ./..." {
		t.Fatalf("evidence=%+v", evidence)
	}
}

func TestVerificationWorkspaceMutationFailsClosed(t *testing.T) {
	service, store, root := newTestService(t)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	service.registry = NewRegistry(codexbackend.NewCommand(agent.NewRunner(), binary,
		[]string{"-test.run=TestOrchestratorCodexProcess", "--"},
		[]string{"INDEXQUBE_ORCHESTRATOR_CODEX_HELPER=write"}, "codex-cli test"))
	service.verifier = mutatingVerifier{}
	task, err := service.StartTask(context.Background(), StartTaskInput{
		Workspace: root, Prompt: "make a change then verify", Backend: agent.BackendCodex, Permission: agent.PermissionWrite,
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = waitForTerminal(t, service, task.ID)
	evidence, ok, err := store.TaskEvidence(context.Background(), task.ID)
	if err != nil || !ok {
		t.Fatalf("evidence ok=%v err=%v", ok, err)
	}
	if evidence.Task.Status != taskstore.TaskNeedsAttention || !evidence.EvidenceMismatch ||
		len(evidence.VerificationRuns) != 1 || evidence.VerificationRuns[0].Status != taskstore.VerificationFailed ||
		len(evidence.VerificationRuns[0].Checks) != 2 || len(evidence.Files) != 2 ||
		len(evidence.Snapshots) != 3 {
		t.Fatalf("evidence=%+v", evidence)
	}
}

func TestAuthoritativeWorkspaceDeltaFlagsUnreportedAndFailedWrites(t *testing.T) {
	for _, tc := range []struct {
		name         string
		mode         string
		wantTerminal agent.EventType
	}{
		{name: "successful but unreported", mode: "unreported-write", wantTerminal: agent.EventCompleted},
		{name: "failed after unreported write", mode: "failed-write", wantTerminal: agent.EventError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, store, root := newTestService(t)
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			service.registry = NewRegistry(codexbackend.NewCommand(agent.NewRunner(), binary,
				[]string{"-test.run=TestOrchestratorCodexProcess", "--"},
				[]string{"INDEXQUBE_ORCHESTRATOR_CODEX_HELPER=" + tc.mode}, "codex-cli test"))
			task, err := service.StartTask(context.Background(), StartTaskInput{
				Workspace: root, Prompt: "write without reporting it", Backend: agent.BackendCodex, Permission: agent.PermissionWrite,
			})
			if err != nil {
				t.Fatal(err)
			}
			events := waitForTerminal(t, service, task.ID)
			if events[len(events)-1].Type != tc.wantTerminal {
				t.Fatalf("terminal=%+v", events[len(events)-1])
			}
			state, ok, err := service.TaskState(context.Background(), task.ID)
			if err != nil || !ok || state.Task.Status != taskstore.TaskNeedsAttention {
				t.Fatalf("state=%+v ok=%v err=%v", state, ok, err)
			}
			evidence, ok, err := store.TaskEvidence(context.Background(), task.ID)
			if err != nil || !ok || !evidence.EvidenceMismatch || len(evidence.Files) != 1 || len(evidence.ReportedFiles) != 0 {
				t.Fatalf("evidence=%+v ok=%v err=%v", evidence, ok, err)
			}
			foundWarning := false
			for _, event := range evidence.Events {
				if event.Type == agent.EventWarning && event.Metadata["error_code"] == "workspace_evidence_mismatch" {
					foundWarning = true
				}
			}
			if !foundWarning {
				t.Fatal("missing workspace evidence mismatch warning")
			}
		})
	}
}

func TestCompareMutationEvidenceAllowsBothSidesOfRename(t *testing.T) {
	deltas := []taskstore.WorkspaceFileDelta{{Path: "new name.go", PreviousPath: "old name.go", Operation: "renamed"}}
	if mismatch, message := compareMutationEvidence(deltas, map[string]struct{}{
		"new name.go": {}, "old name.go": {},
	}); mismatch {
		t.Fatalf("unexpected mismatch: %s", message)
	}
	if mismatch, _ := compareMutationEvidence(deltas, map[string]struct{}{"old name.go": {}}); !mismatch {
		t.Fatal("missing destination path should mismatch")
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
				requested, err := service.Cancel(context.Background(), task.ID)
				if err != nil || requested.Cancellation.Status != taskstore.CancellationRequested {
					t.Fatalf("cancel: %v", err)
				}
				events = waitForTerminal(t, service, task.ID)
				if events[len(events)-1].Type != agent.EventCancelled {
					t.Fatalf("terminal event=%s want cancelled", events[len(events)-1].Type)
				}
				completed, err := service.Cancel(context.Background(), task.ID)
				if err != nil || completed.Cancellation.ID != requested.Cancellation.ID || completed.Cancellation.Status != taskstore.CancellationCompleted {
					t.Fatalf("repeated cancellation=%+v err=%v", completed, err)
				}
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("fake backend did not start")
}

type approvalBackend struct{}

func (approvalBackend) ID() agent.BackendID { return agent.BackendFake }

func (approvalBackend) Probe(context.Context) agent.BackendHealth {
	return agent.BackendHealth{Backend: agent.BackendFake, Status: agent.HealthAvailable, Version: "approval-test", CheckedAt: time.Now().UTC()}
}

func (approvalBackend) Execute(ctx context.Context, req agent.Request, sink agent.EventSink) (agent.Result, error) {
	if req.Approvals == nil {
		return agent.Result{}, errors.New("missing approval handler")
	}
	decision, err := req.Approvals.RequestApproval(ctx, agent.ApprovalRequest{
		BackendRequestID: "backend-request-1", Kind: agent.ApprovalCommand, ItemID: "command-1",
		NativeThreadID: "approval-thread", NativeTurnID: "approval-turn", Reason: "run the guarded fixture",
		Command: "write approved-change.txt", CWD: req.Workspace,
	})
	if err != nil {
		return agent.Result{}, err
	}
	if decision == agent.ApprovalAccept {
		if err := os.WriteFile(filepath.Join(req.Workspace, "approved-change.txt"), []byte("approved\n"), 0o600); err != nil {
			return agent.Result{}, err
		}
		if err := sink.Publish(ctx, agent.Event{Type: agent.EventFileChanged,
			File: &agent.FileEvent{Path: "approved-change.txt", Operation: "add"}}); err != nil {
			return agent.Result{}, err
		}
	} else if decision == agent.ApprovalCancel {
		return agent.Result{}, errors.New("approval cancelled")
	}
	return agent.Result{NativeSessionID: "approval-thread", FinalMessage: "approval fixture complete", MutationSeen: decision == agent.ApprovalAccept}, nil
}

func TestDurableApprovalApproveDenyCancelAndTimeout(t *testing.T) {
	for _, tc := range []struct {
		name               string
		action             string
		wantApprovalStatus taskstore.ApprovalStatus
		wantTerminal       agent.EventType
		wantFile           bool
	}{
		{name: "approve", action: "approve", wantApprovalStatus: taskstore.ApprovalApproved, wantTerminal: agent.EventCompleted, wantFile: true},
		{name: "deny", action: "deny", wantApprovalStatus: taskstore.ApprovalDenied, wantTerminal: agent.EventCompleted},
		{name: "cancel", action: "cancel", wantApprovalStatus: taskstore.ApprovalCancelled, wantTerminal: agent.EventCancelled},
		{name: "timeout", action: "timeout", wantApprovalStatus: taskstore.ApprovalExpired, wantTerminal: agent.EventError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, store, root := newTestService(t)
			service.registry = NewRegistry(approvalBackend{})
			if tc.action == "timeout" {
				service.approvalTimeout = 40 * time.Millisecond
			}
			task, err := service.StartTask(context.Background(), StartTaskInput{
				Workspace: root, Prompt: "guard this action", Backend: agent.BackendFake, Permission: agent.PermissionWrite,
			})
			if err != nil {
				t.Fatal(err)
			}
			approvalEvent := waitForApprovalRequest(t, service, task.ID)
			approvalID := approvalEvent.Approval.ApprovalID
			state, ok, err := service.TaskState(context.Background(), task.ID)
			if err != nil || !ok || state.Task.Status != taskstore.TaskAwaitingApproval || state.LatestTurn.Status != taskstore.TurnAwaitingApproval {
				t.Fatalf("awaiting state=%+v ok=%v err=%v", state, ok, err)
			}
			switch tc.action {
			case "approve", "deny":
				if _, err := service.DecideApproval(context.Background(), approvalID, tc.action); err != nil {
					t.Fatal(err)
				}
			case "cancel":
				if _, err := service.Cancel(context.Background(), task.ID); err != nil {
					t.Fatalf("cancel: %v", err)
				}
			case "timeout":
			}
			events := waitForTerminal(t, service, task.ID)
			if events[len(events)-1].Type != tc.wantTerminal {
				t.Fatalf("terminal=%+v", events[len(events)-1])
			}
			approval, found, err := store.ApprovalByID(context.Background(), approvalID)
			if err != nil || !found || approval.Status != tc.wantApprovalStatus {
				t.Fatalf("approval=%+v found=%v err=%v", approval, found, err)
			}
			_, statErr := os.Stat(filepath.Join(root, "approved-change.txt"))
			if tc.wantFile && statErr != nil {
				t.Fatalf("approved file missing: %v", statErr)
			}
			if !tc.wantFile && !os.IsNotExist(statErr) {
				t.Fatalf("unexpected approved file: %v", statErr)
			}
			if tc.action == "approve" {
				if _, err := service.DecideApproval(context.Background(), approvalID, "deny"); err == nil {
					t.Fatal("second decision unexpectedly succeeded")
				}
			}
		})
	}
}

func TestReconcileCancelsPendingApprovalWithoutAuthorizingIt(t *testing.T) {
	service, store, root := newTestService(t)
	identity, err := workspace.Resolve(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	task, turn, attempt, err := store.CreateTask(context.Background(), taskstore.CreateTaskInput{
		TaskID: "task_restart_approval", TurnID: "turn_restart_approval", RouteAttemptID: "route_restart_approval",
		WorkspaceID: identity.ID, WorkspacePath: root, Goal: "guarded", Permission: agent.PermissionWrite,
		PreferredBackend: agent.BackendFake, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StartTurn(context.Background(), task.ID, turn.ID, attempt.ID, 1, now); err != nil {
		t.Fatal(err)
	}
	approval, err := store.CreateApproval(context.Background(), taskstore.CreateApprovalInput{Approval: taskstore.Approval{
		ID: "approval_restart", TaskID: task.ID, TurnID: turn.ID, Backend: agent.BackendFake,
		BackendRequestID: "restart-request", Kind: agent.ApprovalCommand, Command: "dangerous action",
	}, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	report, err := service.ReconcileInterrupted(context.Background())
	if err != nil || report.NeedsAttention != 1 {
		t.Fatalf("report=%+v err=%v", report, err)
	}
	stored, ok, err := store.ApprovalByID(context.Background(), approval.ID)
	if err != nil || !ok || stored.Status != taskstore.ApprovalCancelled || stored.Decision != agent.ApprovalCancel {
		t.Fatalf("approval=%+v ok=%v err=%v", stored, ok, err)
	}
	state, ok, err := service.TaskState(context.Background(), task.ID)
	if err != nil || !ok || state.Task.Status != taskstore.TaskNeedsAttention {
		t.Fatalf("state=%+v ok=%v err=%v", state, ok, err)
	}
}

func waitForApprovalRequest(t *testing.T, service *Service, taskID string) agent.Event {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		events, err := service.EventsAfter(context.Background(), taskID, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range events {
			if event.Type == agent.EventApprovalRequested && event.Approval != nil {
				return event
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("timed out waiting for approval request")
	return agent.Event{}
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

func TestReconcileCompletesDurableCancellation(t *testing.T) {
	for _, tc := range []struct {
		name       string
		permission agent.PermissionMode
		wantTask   taskstore.TaskStatus
	}{
		{name: "read only reopens", permission: agent.PermissionReadOnly, wantTask: taskstore.TaskOpen},
		{name: "write requires inspection", permission: agent.PermissionWrite, wantTask: taskstore.TaskNeedsAttention},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, store, root := newTestService(t)
			identity, err := workspace.Resolve(context.Background(), root)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			task, turn, attempt, err := store.CreateTask(context.Background(), taskstore.CreateTaskInput{
				TaskID: taskstore.NewID("task"), TurnID: taskstore.NewID("turn"), RouteAttemptID: taskstore.NewID("route"),
				WorkspaceID: identity.ID, WorkspacePath: identity.Root, Goal: "cancel before restart", Permission: tc.permission,
				PreferredBackend: agent.BackendFake, Now: now,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := store.StartTurn(context.Background(), task.ID, turn.ID, attempt.ID, 1, now.Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			_, requested, err := store.RequestCancellation(context.Background(), task.ID, now.Add(2*time.Second))
			if err != nil || requested.Status != taskstore.CancellationRequested {
				t.Fatalf("request=%+v err=%v", requested, err)
			}
			report, err := service.ReconcileInterrupted(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantTask == taskstore.TaskNeedsAttention && report.NeedsAttention != 1 {
				t.Fatalf("report=%+v", report)
			}
			if tc.wantTask == taskstore.TaskOpen && report.Recovered != 1 {
				t.Fatalf("report=%+v", report)
			}
			state, ok, err := service.TaskState(context.Background(), task.ID)
			if err != nil || !ok || state.Task.Status != tc.wantTask || state.LatestTurn.Status != taskstore.TurnCancelled {
				t.Fatalf("state=%+v ok=%v err=%v", state, ok, err)
			}
			completed, ok, err := store.CancellationForTurn(context.Background(), turn.ID)
			if err != nil || !ok || completed.Status != taskstore.CancellationCompleted || completed.ID != requested.ID {
				t.Fatalf("cancellation=%+v ok=%v err=%v", completed, ok, err)
			}
			events, err := service.EventsAfter(context.Background(), task.ID, 0)
			if err != nil || len(events) != 1 || events[0].Type != agent.EventCancelled {
				t.Fatalf("events=%+v err=%v", events, err)
			}
		})
	}
}

type fixedVerifier struct {
	result verification.Result
}

func (v fixedVerifier) Verify(context.Context, verification.Request) verification.Result {
	return v.result
}

type mutatingVerifier struct{}

func (mutatingVerifier) Verify(_ context.Context, request verification.Request) verification.Result {
	now := time.Now().UTC()
	_ = os.WriteFile(filepath.Join(request.Workspace, "verification-side-effect.txt"), []byte("unexpected\n"), 0o600)
	exit := 0
	return verification.Result{
		Status: verification.StatusVerified, Summary: "1 verification check(s) passed",
		StartedAt: now, CompletedAt: now,
		Checks: []verification.CheckResult{{
			Name: "fixture check", Kind: "test", Command: "fixture", CWD: ".",
			Status: verification.CheckPassed, ExitCode: &exit, StartedAt: now, CompletedAt: now,
		}},
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
