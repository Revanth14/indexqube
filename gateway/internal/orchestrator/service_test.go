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
	writeChange := (mode == "write" || mode == "write-go" || mode == "write-recipe" || mode == "write-risk-high" || mode == "write-risk-medium" || mode == "unreported-write" || mode == "failed-write") &&
		!strings.Contains(string(prompt), "inspect the durable change")
	changePath := "codex-orchestrated-write.txt"
	changeContent := "durable write evidence\n"
	if mode == "write-go" {
		changePath = "verified_change.go"
		changeContent = "package fixture\n\nconst VerifiedChange = true\n"
	} else if mode == "write-recipe" {
		changePath = verification.RecipePath
		changeContent = "{\"version\":1,\"checks\":[{\"name\":\"Agent recipe\",\"command\":[\"go\",\"version\"]}]}\n"
	} else if mode == "write-risk-high" {
		changePath = "risky.js"
		changeContent = "child_process.exec(req.query.cmd)\n"
	} else if mode == "write-risk-medium" {
		changePath = "risky.py"
		changeContent = "verify = False\n"
	}
	if writeChange {
		if os.Getenv("INDEXQUBE_WORKSPACE_LOCK_FD") == "" {
			os.Exit(8)
		}
		if err := os.MkdirAll(filepath.Dir(changePath), 0o700); err != nil {
			os.Exit(9)
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
	if writeChange && (mode == "write" || mode == "write-go" || mode == "write-recipe" || mode == "write-risk-high" || mode == "write-risk-medium") {
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

type handoffCaptureBackend struct {
	id         agent.BackendID
	requests   chan agent.Request
	health     agent.HealthStatus
	resumeLost bool
}

type classifiedFailureBackend struct {
	id           agent.BackendID
	failure      error
	mutation     bool
	requireGuard bool
}

func (b *classifiedFailureBackend) ID() agent.BackendID { return b.id }
func (b *classifiedFailureBackend) Probe(context.Context) agent.BackendHealth {
	return agent.BackendHealth{Backend: b.id, Status: agent.HealthAvailable, CheckedAt: time.Now().UTC()}
}
func (b *classifiedFailureBackend) Execute(_ context.Context, request agent.Request, _ agent.EventSink) (agent.Result, error) {
	if b.requireGuard && request.Guard == nil {
		return agent.Result{}, errors.New("missing inherited write guard")
	}
	if b.mutation {
		if err := os.WriteFile(filepath.Join(request.Workspace, "classified-failure.txt"), []byte("changed\n"), 0o600); err != nil {
			return agent.Result{}, err
		}
	}
	return agent.Result{MutationSeen: b.mutation, ExitCode: 1}, b.failure
}

func TestBackendFailureClassificationRequiresSafeUnpinnedWorkspace(t *testing.T) {
	tests := []struct {
		name       string
		failure    error
		permission agent.PermissionMode
		mutation   bool
		pin        bool
		wantClass  agent.FailureClass
		eligible   bool
		wantStatus taskstore.TaskStatus
	}{
		{name: "allowlisted pre mutation", failure: errors.New("request failed: rate_limit_exceeded"), permission: agent.PermissionReadOnly,
			wantClass: agent.FailureRateLimited, eligible: true, wantStatus: taskstore.TaskOpen},
		{name: "pinned route", failure: errors.New("request failed: rate_limit_exceeded"), permission: agent.PermissionReadOnly, pin: true,
			wantClass: agent.FailureRateLimited, wantStatus: taskstore.TaskOpen},
		{name: "unknown failure", failure: errors.New("opaque backend failure"), permission: agent.PermissionReadOnly,
			wantClass: agent.FailureUnknown, wantStatus: taskstore.TaskOpen},
		{name: "mutation boundary", failure: errors.New("request failed: rate_limit_exceeded"), permission: agent.PermissionWrite, mutation: true,
			wantClass: agent.FailureRateLimited, wantStatus: taskstore.TaskNeedsAttention},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service, store, root := newTestService(t)
			backend := &classifiedFailureBackend{id: agent.BackendFake, failure: test.failure, mutation: test.mutation}
			service.registry = NewRegistry(backend)
			task, err := service.StartTask(context.Background(), StartTaskInput{
				Workspace: root, Prompt: "classify this", Backend: agent.BackendFake, Permission: test.permission, PinBackend: test.pin,
			})
			if err != nil {
				t.Fatal(err)
			}
			events := waitForTerminal(t, service, task.ID)
			terminal := events[len(events)-1]
			if terminal.Type != agent.EventError || terminal.Metadata["failure_class"] != string(test.wantClass) ||
				(terminal.Metadata["fallback_eligible"] == "true") != test.eligible {
				t.Fatalf("terminal=%+v", terminal)
			}
			evidence, found, err := store.TaskEvidence(context.Background(), task.ID)
			if err != nil || !found || len(evidence.Routes) != 1 {
				t.Fatalf("evidence=%+v found=%v err=%v", evidence, found, err)
			}
			route := evidence.Routes[0]
			if route.FailureClass != test.wantClass || route.FallbackEligible != test.eligible || route.MutationObserved != test.mutation ||
				evidence.Task.Status != test.wantStatus {
				t.Fatalf("route=%+v task=%+v", route, evidence.Task)
			}
		})
	}
}

func TestAutomaticFallbackUsesDurableOrderedRouteAndCanonicalContext(t *testing.T) {
	service, store, root := newTestService(t)
	source := &classifiedFailureBackend{id: agent.BackendCodex, failure: errors.New("request failed: rate_limit_exceeded")}
	destination := &handoffCaptureBackend{id: agent.BackendClaude, requests: make(chan agent.Request, 1)}
	service.registry = NewRegistry(source, destination)
	task, err := service.StartTask(context.Background(), StartTaskInput{
		Workspace: root, Prompt: "finish the fallback fixture", Backend: agent.BackendCodex, Permission: agent.PermissionReadOnly,
	})
	if err != nil {
		t.Fatal(err)
	}
	events := waitForTerminal(t, service, task.ID)
	if events[len(events)-1].Type != agent.EventCompleted {
		t.Fatalf("events=%+v", events)
	}
	request := <-destination.requests
	if request.NativeSessionID != "" || !strings.Contains(request.Prompt, "INDEXQUBE AUTOMATIC FALLBACK") ||
		!strings.Contains(request.Prompt, "rate_limited") || !strings.Contains(request.Prompt, "finish the fallback fixture") {
		t.Fatalf("fallback request=%+v", request)
	}
	evidence, found, err := store.TaskEvidence(context.Background(), task.ID)
	if err != nil || !found || len(evidence.Routes) != 2 {
		t.Fatalf("evidence=%+v found=%v err=%v", evidence, found, err)
	}
	first, second := evidence.Routes[0], evidence.Routes[1]
	if first.Backend != agent.BackendCodex || first.Status != "failed" || first.FailureClass != agent.FailureRateLimited ||
		!first.FallbackEligible || first.PreFingerprint == "" || first.PreFingerprint != first.PostFingerprint {
		t.Fatalf("source route=%+v", first)
	}
	if second.Backend != agent.BackendClaude || second.Status != string(taskstore.TurnSucceeded) ||
		second.DecisionReason != "automatic_fallback_v1" || second.Ordinal != 2 {
		t.Fatalf("destination route=%+v", second)
	}
	if evidence.Task.PreferredBackend != agent.BackendCodex || evidence.BackendPin != nil || evidence.Task.Status != taskstore.TaskOpen {
		t.Fatalf("task=%+v pin=%+v", evidence.Task, evidence.BackendPin)
	}
	session, found, err := store.LatestBackendSession(context.Background(), task.ID, agent.BackendClaude)
	if err != nil || !found || session.CreationReason != "automatic_fallback" {
		t.Fatalf("session=%+v found=%v err=%v", session, found, err)
	}
}

func TestAutomaticFallbackNeverCyclesAndRetainsWriteGuard(t *testing.T) {
	service, store, root := newTestService(t)
	source := &classifiedFailureBackend{id: agent.BackendCodex, failure: errors.New("service unavailable")}
	destination := &classifiedFailureBackend{id: agent.BackendClaude, failure: errors.New("rate limit exceeded"), requireGuard: true}
	service.registry = NewRegistry(source, destination)
	task, err := service.StartTask(context.Background(), StartTaskInput{
		Workspace: root, Prompt: "bounded fallback", Backend: agent.BackendCodex, Permission: agent.PermissionWrite,
	})
	if err != nil {
		t.Fatal(err)
	}
	events := waitForTerminal(t, service, task.ID)
	if events[len(events)-1].Type != agent.EventError {
		t.Fatalf("events=%+v", events)
	}
	evidence, found, err := store.TaskEvidence(context.Background(), task.ID)
	if err != nil || !found || len(evidence.Routes) != 2 {
		t.Fatalf("routes=%+v found=%v err=%v", evidence.Routes, found, err)
	}
	if evidence.Routes[0].Backend != agent.BackendCodex || evidence.Routes[1].Backend != agent.BackendClaude ||
		!evidence.Routes[0].FallbackEligible || !evidence.Routes[1].FallbackEligible {
		t.Fatalf("routes=%+v", evidence.Routes)
	}
	if evidence.Task.Status != taskstore.TaskOpen {
		t.Fatalf("task=%+v", evidence.Task)
	}
}

func TestAutomaticFallbackRespectsPinsAndMutationBoundaries(t *testing.T) {
	for _, test := range []struct {
		name       string
		permission agent.PermissionMode
		pin        bool
		mutation   bool
		wantStatus taskstore.TaskStatus
	}{
		{name: "pinned task", permission: agent.PermissionReadOnly, pin: true, wantStatus: taskstore.TaskOpen},
		{name: "uncertain write boundary", permission: agent.PermissionWrite, mutation: true, wantStatus: taskstore.TaskNeedsAttention},
	} {
		t.Run(test.name, func(t *testing.T) {
			service, store, root := newTestService(t)
			source := &classifiedFailureBackend{
				id: agent.BackendCodex, failure: errors.New("rate limit exceeded"), mutation: test.mutation,
			}
			destination := &handoffCaptureBackend{id: agent.BackendClaude, requests: make(chan agent.Request, 1)}
			service.registry = NewRegistry(source, destination)
			task, err := service.StartTask(context.Background(), StartTaskInput{
				Workspace: root, Prompt: "do not cross this boundary", Backend: agent.BackendCodex,
				Permission: test.permission, PinBackend: test.pin,
			})
			if err != nil {
				t.Fatal(err)
			}
			events := waitForTerminal(t, service, task.ID)
			if events[len(events)-1].Type != agent.EventError {
				t.Fatalf("events=%+v", events)
			}
			select {
			case request := <-destination.requests:
				t.Fatalf("unsafe destination started: %+v", request)
			default:
			}
			evidence, found, err := store.TaskEvidence(context.Background(), task.ID)
			if err != nil || !found || len(evidence.Routes) != 1 || evidence.Routes[0].FallbackEligible ||
				evidence.Task.Status != test.wantStatus {
				t.Fatalf("evidence=%+v found=%v err=%v", evidence, found, err)
			}
		})
	}
}

func TestReconcileResumesDurablyQueuedAutomaticFallback(t *testing.T) {
	service, store, root := newTestService(t)
	destination := &handoffCaptureBackend{id: agent.BackendClaude, requests: make(chan agent.Request, 1)}
	service.registry = NewRegistry(destination)
	identity, err := workspace.Resolve(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	task, turn, source, err := store.CreateTask(context.Background(), taskstore.CreateTaskInput{
		TaskID: taskstore.NewID("task"), TurnID: taskstore.NewID("turn"), RouteAttemptID: taskstore.NewID("route"),
		WorkspaceID: identity.ID, WorkspacePath: identity.Root, Goal: "resume queued fallback",
		Permission: agent.PermissionReadOnly, PreferredBackend: agent.BackendCodex, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	pre, err := workspace.Capture(context.Background(), identity, task.ID, turn.ID, "pre")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddSnapshot(context.Background(), pre); err != nil {
		t.Fatal(err)
	}
	if err := store.SetAttemptPreFingerprint(context.Background(), source.ID, pre.Fingerprint); err != nil {
		t.Fatal(err)
	}
	if err := store.StartTurn(context.Background(), task.ID, turn.ID, source.ID, 0, now); err != nil {
		t.Fatal(err)
	}
	next := taskstore.RouteAttempt{
		ID: taskstore.NewID("route"), TurnID: turn.ID, Ordinal: 2, Backend: agent.BackendClaude,
		DecisionReason: "automatic_fallback_v1", Status: "queued", StartedAt: now.Add(time.Second),
	}
	if err := store.BeginAutomaticFallback(context.Background(), taskstore.BeginAutomaticFallbackInput{
		CurrentAttemptID: source.ID, NextAttempt: next, FailureClass: agent.FailureRateLimited,
		PostFingerprint: pre.Fingerprint, Now: now.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	report, err := service.ReconcileInterrupted(context.Background())
	if err != nil || report.Recovered != 1 || report.NeedsAttention != 0 {
		t.Fatalf("report=%+v err=%v", report, err)
	}
	select {
	case request := <-destination.requests:
		if !strings.Contains(request.Prompt, "INDEXQUBE AUTOMATIC FALLBACK") {
			t.Fatalf("request=%+v", request)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("queued fallback did not resume")
	}
	events := waitForTerminal(t, service, task.ID)
	if events[len(events)-1].Type != agent.EventCompleted {
		t.Fatalf("events=%+v", events)
	}
	evidence, found, err := store.TaskEvidence(context.Background(), task.ID)
	if err != nil || !found || len(evidence.Routes) != 2 || evidence.Routes[1].Status != string(taskstore.TurnSucceeded) {
		t.Fatalf("evidence=%+v found=%v err=%v", evidence, found, err)
	}
}

func (b *handoffCaptureBackend) ID() agent.BackendID { return b.id }

func (b *handoffCaptureBackend) Probe(context.Context) agent.BackendHealth {
	status := b.health
	if status == "" {
		status = agent.HealthAvailable
	}
	return agent.BackendHealth{Backend: b.id, Status: status, Reason: "fixture unavailable", CheckedAt: time.Now().UTC()}
}

func (b *handoffCaptureBackend) ValidatePermission(permission agent.PermissionMode) error {
	if permission != agent.PermissionReadOnly && permission != agent.PermissionWrite {
		return errors.New("unsupported permission")
	}
	return nil
}

func (b *handoffCaptureBackend) Execute(_ context.Context, request agent.Request, sink agent.EventSink) (agent.Result, error) {
	b.requests <- request
	if b.resumeLost && request.NativeSessionID != "" {
		return agent.Result{ResumeLost: true}, errors.New("session not found")
	}
	_ = sink.Publish(context.Background(), agent.Event{
		Type: agent.EventAssistantMessage, Message: &agent.MessageEvent{Text: "handoff destination answer"},
	})
	sessionID := "handoff-destination-session"
	if strings.Contains(request.Prompt, "INDEXQUBE CANONICAL SESSION RECOVERY") {
		sessionID = "handoff-recovered-session"
	}
	return agent.Result{NativeSessionID: sessionID, FinalMessage: "handoff destination answer"}, nil
}

func TestExplicitHandoffPersistsCanonicalPacketAndDestinationLineage(t *testing.T) {
	service, store, root := newTestService(t)
	source, err := service.registry.Get(agent.BackendFake)
	if err != nil {
		t.Fatal(err)
	}
	destination := &handoffCaptureBackend{id: agent.BackendClaude, requests: make(chan agent.Request, 1)}
	service.registry = NewRegistry(source, destination)
	task, err := service.StartTask(context.Background(), StartTaskInput{
		Workspace: root, Prompt: "explain the repository", Backend: agent.BackendFake, Permission: agent.PermissionReadOnly,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForTerminal(t, service, task.ID)
	sourceSession, ok, err := store.LatestBackendSession(context.Background(), task.ID, agent.BackendFake)
	if err != nil || !ok {
		t.Fatalf("source session=%+v ok=%v err=%v", sourceSession, ok, err)
	}
	after, err := service.LatestEventSequence(context.Background(), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.HandoffTask(context.Background(), HandoffTaskInput{
		TaskID: task.ID, ToBackend: agent.BackendClaude, Prompt: "review the remaining edge cases",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Task.PreferredBackend != agent.BackendClaude || result.Handoff.FromBackend != agent.BackendFake || result.Handoff.ToBackend != agent.BackendClaude {
		t.Fatalf("result=%+v", result)
	}
	var request agent.Request
	select {
	case request = <-destination.requests:
	case <-time.After(3 * time.Second):
		t.Fatal("destination backend did not start")
	}
	if request.NativeSessionID != "" || !strings.Contains(request.Prompt, "INDEXQUBE CANONICAL HANDOFF") ||
		!strings.Contains(request.Prompt, "explain the repository") || !strings.Contains(request.Prompt, "review the remaining edge cases") ||
		!strings.HasSuffix(request.Prompt, string(result.Handoff.Packet)) {
		t.Fatalf("destination request=%+v", request)
	}
	events := waitForTerminalAfter(t, service, task.ID, after)
	if events[len(events)-1].Type != agent.EventCompleted {
		t.Fatalf("events=%+v", events)
	}
	if events[0].Type != agent.EventRouteSelected || events[0].Metadata["handoff_id"] != result.Handoff.ID ||
		events[0].Metadata["decision_reason"] != "explicit_handoff" {
		t.Fatalf("handoff route event=%+v", events[0])
	}
	evidence, ok, err := store.TaskEvidence(context.Background(), task.ID)
	if err != nil || !ok {
		t.Fatalf("evidence ok=%v err=%v", ok, err)
	}
	if len(evidence.Handoffs) != 1 || len(evidence.Routes) != 2 || evidence.Routes[1].DecisionReason != "explicit_handoff" ||
		evidence.Task.PreferredBackend != agent.BackendClaude || evidence.Task.Status != taskstore.TaskOpen {
		t.Fatalf("evidence=%+v", evidence)
	}
	var packet CanonicalHandoffPacket
	if err := json.Unmarshal(evidence.Handoffs[0].Packet, &packet); err != nil {
		t.Fatal(err)
	}
	if packet.Version != 1 || packet.Workspace.Fingerprint == "" || packet.CurrentRequest != "review the remaining edge cases" || len(packet.Conversation) != 1 {
		t.Fatalf("packet=%+v", packet)
	}
	destinationSession, ok, err := store.LatestBackendSession(context.Background(), task.ID, agent.BackendClaude)
	if err != nil || !ok || destinationSession.CreationReason != "explicit_handoff" || destinationSession.PredecessorID != sourceSession.ID {
		t.Fatalf("destination session=%+v ok=%v err=%v", destinationSession, ok, err)
	}
}

func TestWriteTaskHandoffRetainsPermissionAndWorkspaceGuard(t *testing.T) {
	service, _, root := newTestService(t)
	source, err := service.registry.Get(agent.BackendFake)
	if err != nil {
		t.Fatal(err)
	}
	destination := &handoffCaptureBackend{id: agent.BackendClaude, requests: make(chan agent.Request, 1)}
	service.registry = NewRegistry(source, destination)
	task, err := service.StartTask(context.Background(), StartTaskInput{
		Workspace: root, Prompt: "[fake:mutate]", Backend: agent.BackendFake, Permission: agent.PermissionWrite,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForTerminal(t, service, task.ID)
	after, _ := service.LatestEventSequence(context.Background(), task.ID)
	handoff, err := service.HandoffTask(context.Background(), HandoffTaskInput{TaskID: task.ID, ToBackend: agent.BackendClaude})
	if err != nil {
		t.Fatal(err)
	}
	var packet CanonicalHandoffPacket
	if err := json.Unmarshal(handoff.Handoff.Packet, &packet); err != nil || len(packet.Files) != 1 || packet.Verification == nil {
		t.Fatalf("packet=%+v err=%v", packet, err)
	}
	select {
	case request := <-destination.requests:
		if request.Permission != agent.PermissionWrite || request.Guard == nil || request.WriteEpoch == 0 {
			t.Fatalf("write handoff request=%+v", request)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("write destination did not start")
	}
	waitForTerminalAfter(t, service, task.ID, after)
}

func TestDestinationLostSessionRecoversFromCanonicalHistoryAfterHandoff(t *testing.T) {
	service, store, root := newTestService(t)
	source, err := service.registry.Get(agent.BackendFake)
	if err != nil {
		t.Fatal(err)
	}
	destination := &handoffCaptureBackend{id: agent.BackendClaude, requests: make(chan agent.Request, 4)}
	service.registry = NewRegistry(source, destination)
	task, err := service.StartTask(context.Background(), StartTaskInput{
		Workspace: root, Prompt: "initial", Backend: agent.BackendFake, Permission: agent.PermissionReadOnly,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForTerminal(t, service, task.ID)
	after, _ := service.LatestEventSequence(context.Background(), task.ID)
	if _, err := service.HandoffTask(context.Background(), HandoffTaskInput{TaskID: task.ID, ToBackend: agent.BackendClaude}); err != nil {
		t.Fatal(err)
	}
	waitForTerminalAfter(t, service, task.ID, after)
	<-destination.requests
	prior, ok, err := store.LatestBackendSession(context.Background(), task.ID, agent.BackendClaude)
	if err != nil || !ok {
		t.Fatalf("prior=%+v ok=%v err=%v", prior, ok, err)
	}
	destination.resumeLost = true
	after, _ = service.LatestEventSequence(context.Background(), task.ID)
	if _, err := service.ContinueTask(context.Background(), ContinueTaskInput{TaskID: task.ID, Prompt: "continue after handoff"}); err != nil {
		t.Fatal(err)
	}
	events := waitForTerminalAfter(t, service, task.ID, after)
	if events[len(events)-1].Type != agent.EventCompleted {
		t.Fatalf("events=%+v", events)
	}
	resumed := <-destination.requests
	recovered := <-destination.requests
	if resumed.NativeSessionID != prior.NativeSessionID || recovered.NativeSessionID != "" ||
		!strings.Contains(recovered.Prompt, "INDEXQUBE CANONICAL SESSION RECOVERY") {
		t.Fatalf("resumed=%+v recovered=%+v", resumed, recovered)
	}
	latest, ok, err := store.LatestBackendSession(context.Background(), task.ID, agent.BackendClaude)
	if err != nil || !ok || latest.NativeSessionID != "handoff-recovered-session" || latest.PredecessorID != prior.ID {
		t.Fatalf("latest=%+v ok=%v err=%v", latest, ok, err)
	}
}

func TestHandoffRejectsActiveNeedsAttentionSameAndUnavailableDestinations(t *testing.T) {
	for _, tc := range []struct {
		name        string
		prompt      string
		permission  agent.PermissionMode
		destination agent.BackendID
		health      agent.HealthStatus
		whileActive bool
	}{
		{name: "active", prompt: "[fake:sleep]", permission: agent.PermissionReadOnly, destination: agent.BackendClaude, whileActive: true},
		{name: "needs attention", prompt: "[fake:mutate][fake:fail]", permission: agent.PermissionWrite, destination: agent.BackendClaude},
		{name: "same backend", prompt: "done", permission: agent.PermissionReadOnly, destination: agent.BackendFake},
		{name: "unavailable", prompt: "done", permission: agent.PermissionReadOnly, destination: agent.BackendClaude, health: agent.HealthUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, _, root := newTestService(t)
			source, err := service.registry.Get(agent.BackendFake)
			if err != nil {
				t.Fatal(err)
			}
			destination := &handoffCaptureBackend{id: agent.BackendClaude, requests: make(chan agent.Request, 1), health: tc.health}
			service.registry = NewRegistry(source, destination)
			task, err := service.StartTask(context.Background(), StartTaskInput{
				Workspace: root, Prompt: tc.prompt, Backend: agent.BackendFake, Permission: tc.permission,
			})
			if err != nil {
				t.Fatal(err)
			}
			if !tc.whileActive {
				waitForTerminal(t, service, task.ID)
			}
			if _, err := service.HandoffTask(context.Background(), HandoffTaskInput{TaskID: task.ID, ToBackend: tc.destination}); err == nil {
				t.Fatal("unsafe handoff was accepted")
			}
			if tc.whileActive {
				_, _ = service.Cancel(context.Background(), task.ID)
				waitForTerminal(t, service, task.ID)
			}
		})
	}
}

func TestCanonicalHandoffPacketIsValidAndBounded(t *testing.T) {
	evidence := taskstore.TaskEvidence{Task: taskstore.Task{
		ID: "task_large", OriginalGoal: strings.Repeat("goal", 20_000), PreferredBackend: agent.BackendCodex,
		Permission: agent.PermissionReadOnly,
	}}
	for sequence := 1; sequence <= 80; sequence++ {
		evidence.Turns = append(evidence.Turns, taskstore.Turn{
			Sequence: int64(sequence), Status: taskstore.TurnSucceeded,
			UserMessage: strings.Repeat("user", 4_000), AssistantMessage: strings.Repeat("assistant", 4_000),
		})
	}
	for index := 0; index < 500; index++ {
		evidence.Files = append(evidence.Files, taskstore.FileEvidence{Path: strings.Repeat("path", 300), Operation: "modified"})
	}
	packet := buildHandoffPacket(evidence, taskstore.WorkspaceSnapshot{
		Fingerprint: "fingerprint", BoundedDiff: strings.Repeat("diff", 100_000),
	}, agent.BackendClaude, strings.Repeat("request", 10_000))
	raw, err := fitHandoffPacket(&packet)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) > maxHandoffPacket || !json.Valid(raw) || !packet.Truncated {
		t.Fatalf("packet bytes=%d valid=%v truncated=%v", len(raw), json.Valid(raw), packet.Truncated)
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
		[]string{"INDEXQUBE_ORCHESTRATOR_CODEX_HELPER=1"}, "codex-cli 0.149.1"))
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
		[]string{"INDEXQUBE_ORCHESTRATOR_CODEX_HELPER=resume-lost"}, "codex-cli 0.149.1"))
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
		[]string{"INDEXQUBE_ORCHESTRATOR_CODEX_HELPER=write"}, "codex-cli 0.149.1"))
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
	if len(evidence.Snapshots) != 3 || len(evidence.VerificationRuns) != 1 ||
		len(evidence.VerificationRuns[0].Checks) != 1 || evidence.VerificationRuns[0].Checks[0].Kind != "security" ||
		!evidence.Routes[0].MutationObserved {
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
		[]string{"INDEXQUBE_ORCHESTRATOR_CODEX_HELPER=write"}, "codex-cli 0.149.1"))
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
		[]string{"INDEXQUBE_ORCHESTRATOR_CODEX_HELPER=write-go"}, "codex-cli 0.149.1"))
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
		len(evidence.VerificationRuns[0].Checks) != 2 ||
		evidence.VerificationRuns[0].Checks[0].Command != "go test -mod=readonly ./..." {
		t.Fatalf("evidence=%+v", evidence)
	}
}

func TestAutomaticSecurityAuditPersistsSeverityAndAppliesPolicy(t *testing.T) {
	for _, tc := range []struct {
		name       string
		mode       string
		wantTask   taskstore.TaskStatus
		wantRun    taskstore.VerificationStatus
		wantCheck  taskstore.VerificationCheckStatus
		wantRuleID string
	}{
		{name: "high blocks", mode: "write-risk-high", wantTask: taskstore.TaskNeedsAttention, wantRun: taskstore.VerificationFailed, wantCheck: taskstore.VerificationCheckFailed, wantRuleID: "code.shell_injection"},
		{name: "medium warns", mode: "write-risk-medium", wantTask: taskstore.TaskOpen, wantRun: taskstore.VerificationWarnings, wantCheck: taskstore.VerificationCheckWarning, wantRuleID: "code.tls_verification_disabled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, store, root := newTestService(t)
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			service.registry = NewRegistry(codexbackend.NewCommand(agent.NewRunner(), binary,
				[]string{"-test.run=TestOrchestratorCodexProcess", "--"},
				[]string{"INDEXQUBE_ORCHESTRATOR_CODEX_HELPER=" + tc.mode}, "codex-cli 0.149.1"))
			task, err := service.StartTask(context.Background(), StartTaskInput{
				Workspace: root, Prompt: "make a security-audited change", Backend: agent.BackendCodex, Permission: agent.PermissionWrite,
			})
			if err != nil {
				t.Fatal(err)
			}
			events := waitForTerminal(t, service, task.ID)
			if events[len(events)-1].Type != agent.EventCompleted {
				t.Fatalf("terminal=%+v", events[len(events)-1])
			}
			evidence, ok, err := store.TaskEvidence(context.Background(), task.ID)
			if err != nil || !ok || evidence.Task.Status != tc.wantTask || len(evidence.VerificationRuns) != 1 {
				t.Fatalf("evidence=%+v ok=%v err=%v", evidence, ok, err)
			}
			run := evidence.VerificationRuns[0]
			if run.Status != tc.wantRun || len(run.Checks) != 1 || run.Checks[0].Status != tc.wantCheck ||
				len(run.Checks[0].Findings) != 1 || run.Checks[0].Findings[0].RuleID != tc.wantRuleID {
				t.Fatalf("verification=%+v", run)
			}
		})
	}
}

func TestAgentCreatedVerificationRecipeIsNotExecuted(t *testing.T) {
	service, store, root := newTestService(t)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	service.registry = NewRegistry(codexbackend.NewCommand(agent.NewRunner(), binary,
		[]string{"-test.run=TestOrchestratorCodexProcess", "--"},
		[]string{"INDEXQUBE_ORCHESTRATOR_CODEX_HELPER=write-recipe"}, "codex-cli 0.149.1"))
	task, err := service.StartTask(context.Background(), StartTaskInput{
		Workspace: root, Prompt: "add a verification recipe", Backend: agent.BackendCodex, Permission: agent.PermissionWrite,
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
	if evidence.Task.Status != taskstore.TaskNeedsAttention || evidence.EvidenceMismatch ||
		len(evidence.VerificationRuns) != 1 || evidence.VerificationRuns[0].Status != taskstore.VerificationFailed ||
		len(evidence.VerificationRuns[0].Checks) != 1 || evidence.VerificationRuns[0].Checks[0].Kind != "configuration" ||
		!strings.Contains(evidence.VerificationRuns[0].Checks[0].Output, "changed during this turn") {
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
		[]string{"INDEXQUBE_ORCHESTRATOR_CODEX_HELPER=write"}, "codex-cli 0.149.1"))
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
				[]string{"INDEXQUBE_ORCHESTRATOR_CODEX_HELPER=" + tc.mode}, "codex-cli 0.149.1"))
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

func TestSingleWriterConflictIsRejectedBeforeTaskOrTurnCreation(t *testing.T) {
	service, store, root := newTestService(t)
	first, err := service.StartTask(context.Background(), StartTaskInput{
		Workspace: root, Prompt: "[fake:sleep] hold writer", Backend: agent.BackendFake, Permission: agent.PermissionWrite,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.StartTask(context.Background(), StartTaskInput{
		Workspace: root, Prompt: "second writer", Backend: agent.BackendFake, Permission: agent.PermissionWrite,
	}); !errors.Is(err, workspace.ErrWorkspaceLocked) {
		t.Fatalf("second writer error=%v", err)
	} else {
		var conflict *workspace.WorkspaceLockedError
		if !errors.As(err, &conflict) || conflict.TaskID != first.ID || conflict.TurnID == "" {
			t.Fatalf("conflict=%+v error=%v", conflict, err)
		}
	}
	tasks, err := service.Tasks(context.Background(), 10)
	if err != nil || len(tasks) != 1 || tasks[0].ID != first.ID {
		t.Fatalf("tasks=%+v err=%v", tasks, err)
	}
	if _, err := service.Cancel(context.Background(), first.ID); err != nil {
		t.Fatal(err)
	}
	waitForTerminal(t, service, first.ID)

	completed, err := service.StartTask(context.Background(), StartTaskInput{
		Workspace: root, Prompt: "writer after release", Backend: agent.BackendFake, Permission: agent.PermissionWrite,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForTerminal(t, service, completed.ID)
	evidence, ok, err := store.TaskEvidence(context.Background(), completed.ID)
	if err != nil || !ok || len(evidence.Turns) != 1 {
		t.Fatalf("evidence=%+v ok=%v err=%v", evidence, ok, err)
	}

	active, err := service.StartTask(context.Background(), StartTaskInput{
		Workspace: root, Prompt: "[fake:sleep] hold writer again", Backend: agent.BackendFake, Permission: agent.PermissionWrite,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.ContinueTask(context.Background(), ContinueTaskInput{
		TaskID: completed.ID, Prompt: "conflicting continuation",
	}); !errors.Is(err, workspace.ErrWorkspaceLocked) {
		t.Fatalf("continuation error=%v", err)
	}
	evidence, ok, err = store.TaskEvidence(context.Background(), completed.ID)
	if err != nil || !ok || len(evidence.Turns) != 1 {
		t.Fatalf("conflicting continuation created a turn: evidence=%+v ok=%v err=%v", evidence, ok, err)
	}
	if _, err := service.Cancel(context.Background(), active.ID); err != nil {
		t.Fatal(err)
	}
	waitForTerminal(t, service, active.ID)
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
	evidence, found, err := store.TaskEvidence(context.Background(), task.ID)
	if err != nil || !found || len(evidence.Routes) != 3 || evidence.Routes[1].FailureClass != agent.FailureNativeSessionLost ||
		!evidence.Routes[1].FallbackEligible {
		t.Fatalf("recovery routes=%+v found=%v err=%v", evidence.Routes, found, err)
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
