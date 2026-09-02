package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
	"github.com/Revanth14/indexqube/gateway/internal/taskstore"
)

func TestRenderUIScreenShowsControlPlaneViewsAndSanitizesAgentText(t *testing.T) {
	exit := 0
	now := time.Now().UTC()
	evidence := taskstore.TaskEvidence{
		Task: taskstore.Task{
			ID: "task_1234567890", WorkspacePath: "/repo", OriginalGoal: "ship it", Status: taskstore.TaskOpen,
			PreferredBackend: agent.BackendCodex, Permission: agent.PermissionWrite,
		},
		Turns:    []taskstore.Turn{{Sequence: 1, UserMessage: "change it", AssistantMessage: "done\x1b[2J\nreally", Status: taskstore.TurnSucceeded}},
		Files:    []taskstore.FileEvidence{{Path: "main.go", Operation: "modified"}},
		Commands: []taskstore.CommandEvidence{{Command: "go test ./...", Status: "completed", ExitCode: &exit}},
		VerificationRuns: []taskstore.VerificationRun{{Status: taskstore.VerificationPassed, Summary: "checks passed", Checks: []taskstore.VerificationCheck{{
			Name: "Go tests", Status: taskstore.VerificationCheckPassed,
		}}}},
		Routes:   []taskstore.RouteAttempt{{Ordinal: 1, Backend: agent.BackendCodex, Status: "succeeded", DecisionReason: "explicit_provider"}},
		Handoffs: []taskstore.Handoff{{FromBackend: agent.BackendClaude, ToBackend: agent.BackendCodex}},
	}
	snapshot := uiSnapshot{
		workspace: "/repo", tasks: []taskstore.Task{evidence.Task}, selectedID: evidence.Task.ID, evidence: &evidence,
		approvals: []taskstore.Approval{{ID: "approval_1234567890", TaskID: evidence.Task.ID, Backend: agent.BackendCodex, Status: taskstore.ApprovalPending, Command: "go test ./..."}},
		backends:  []agent.BackendHealth{{Backend: agent.BackendCodex, Status: agent.HealthAvailable, Version: "codex-cli 0.149.1", CheckedAt: now}},
		view:      uiViewEvidence, status: "ready", input: "next request",
	}
	screen := renderUIScreen(snapshot, 140, 40)
	for _, want := range []string{"IndexQube", "codex:available", "TASKS", "FILES", "COMMANDS", "VERIFICATION", "ROUTES", "HANDOFFS", "go test ./...", "claude -> codex"} {
		if !strings.Contains(screen, want) {
			t.Fatalf("screen missing %q:\n%s", want, screen)
		}
	}
	if strings.Contains(screen, "\x1b") || strings.Contains(screen, "\nreally") {
		t.Fatalf("agent-controlled terminal escape reached screen: %q", screen)
	}

	snapshot.view = uiViewOverview
	overview := renderUIScreen(snapshot, 140, 40)
	for _, want := range []string{"CONVERSATION", "APPROVALS", "EVIDENCE", "Verify:", "File:", "Cmd:", "Route:", "Handoff:"} {
		if !strings.Contains(overview, want) {
			t.Fatalf("overview missing %q:\n%s", want, overview)
		}
	}
}

func TestUIInputDetachesWithoutCancellingAndSupportsSelection(t *testing.T) {
	app := &uiApp{tasks: []taskstore.Task{{ID: "task_one"}, {ID: "task_two"}}, selectedID: "task_one", view: uiViewOverview}
	app.syncSelection()
	app.handleByte(14)
	if app.selectedID != "task_two" {
		t.Fatalf("selected=%q", app.selectedID)
	}
	for _, value := range []byte("hello") {
		app.handleByte(value)
	}
	app.handleByte(127)
	if string(app.input) != "hell" {
		t.Fatalf("input=%q", string(app.input))
	}
	if detach, _ := app.handleByte(3); !detach {
		t.Fatal("Ctrl-C did not detach")
	}
	if !app.executeCommand(context.Background(), "/quit") {
		t.Fatal("/quit did not detach")
	}
	app.executeCommand(context.Background(), "/view evidence")
	if app.view != uiViewEvidence {
		t.Fatalf("view=%q", app.view)
	}
}

func TestUITerminalTextRemovesControlSequencesAndAcceptsUTF8(t *testing.T) {
	if got := terminalText("safe\x1b[2J\nnext"); strings.ContainsRune(got, '\x1b') || strings.ContainsRune(got, '\n') {
		t.Fatalf("terminal text=%q", got)
	}
	app := &uiApp{}
	for _, value := range []byte("café") {
		app.handleByte(value)
	}
	if got := string(app.input); got != "café" {
		t.Fatalf("input=%q", got)
	}
}

func TestUIDefaultTaskAndBackendSelectionAreDeterministic(t *testing.T) {
	tasks := []taskstore.Task{
		{ID: "closed", Status: taskstore.TaskClosed},
		{ID: "open", Status: taskstore.TaskOpen},
		{ID: "running", Status: taskstore.TaskRunning},
		{ID: "approval", Status: taskstore.TaskAwaitingApproval},
	}
	if got := defaultUITaskIndex(tasks); got != 3 {
		t.Fatalf("default task index=%d", got)
	}
	app := uiApp{backends: []agent.BackendHealth{
		{Backend: agent.BackendClaude, Status: agent.HealthAvailable},
		{Backend: agent.BackendCodex, Status: agent.HealthAvailable},
	}}
	if got := app.preferredAvailableBackend(); got != agent.BackendCodex {
		t.Fatalf("backend=%s", got)
	}
	app.backends[1].Status = agent.HealthIncompatible
	if got := app.preferredAvailableBackend(); got != agent.BackendClaude {
		t.Fatalf("backend=%s", got)
	}
}
