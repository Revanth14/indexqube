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
	if !app.executeCommand(context.Background(), "exit") {
		t.Fatal("plain exit did not detach")
	}
	if !app.executeCommand(context.Background(), "QUIT") {
		t.Fatal("plain quit did not detach case-insensitively")
	}
	app.executeCommand(context.Background(), "/view evidence")
	if app.view != uiViewEvidence {
		t.Fatalf("view=%q", app.view)
	}
}

func TestUIUsesAlternateScreenAndSkipsIdenticalFrames(t *testing.T) {
	if !strings.Contains(uiEnterTerminal, "\x1b[?1049h") {
		t.Fatalf("enter sequence does not enable the alternate screen: %q", uiEnterTerminal)
	}
	if !strings.Contains(uiExitTerminal, "\x1b[?1049l") {
		t.Fatalf("exit sequence does not restore the primary screen: %q", uiExitTerminal)
	}

	app := &uiApp{workspace: "/repo", view: uiViewOverview, status: "ready"}
	first, changed := app.nextFrame(80, 24)
	if !changed || first == "" {
		t.Fatal("initial frame was suppressed")
	}
	if frame, changed := app.nextFrame(80, 24); changed || frame != "" {
		t.Fatalf("identical frame was emitted: changed=%v frame=%q", changed, frame)
	}
	app.input = []rune("x")
	if frame, changed := app.nextFrame(80, 24); !changed || frame == "" {
		t.Fatal("changed input did not produce a new frame")
	}
}

func TestUIPaintErasesStaleCharactersAndClearsOnlyOnResize(t *testing.T) {
	painted := paintUIFrame("header\r\nlong status line\r\n> ", false)
	if strings.Contains(painted, "\x1b[2J") {
		t.Fatalf("steady repaint cleared the whole screen: %q", painted)
	}
	if !strings.HasPrefix(painted, "\x1b[H") {
		t.Fatalf("repaint did not home the cursor: %q", painted)
	}
	// Frame lines are shorter than the terminal is wide, so every line must
	// erase to end of line or the previous frame's tail stays visible.
	for _, line := range strings.Split(painted, "\r\n") {
		if !strings.HasSuffix(strings.TrimSuffix(line, "\x1b[J"), "\x1b[K") {
			t.Fatalf("line %q does not erase to end of line", line)
		}
	}
	if !strings.HasSuffix(painted, "\x1b[J") {
		t.Fatalf("repaint did not clear below the frame: %q", painted)
	}
	if resized := paintUIFrame("header", true); !strings.HasPrefix(resized, "\x1b[2J") {
		t.Fatalf("resize repaint did not clear the screen: %q", resized)
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
