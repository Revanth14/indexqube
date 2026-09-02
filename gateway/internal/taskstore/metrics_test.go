package taskstore

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
)

func TestReliabilityMetricsDeriveOnlyAggregateCanonicalOutcomes(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
	create := func(id string) (Task, Turn, RouteAttempt) {
		task, turn, route, err := store.CreateTask(ctx, CreateTaskInput{
			TaskID: "task_" + id, TurnID: "turn_" + id, RouteAttemptID: "route_" + id,
			WorkspaceID: "workspace_" + id, WorkspacePath: "/private/" + id, Goal: "secret prompt " + id,
			Permission: agent.PermissionReadOnly, PreferredBackend: agent.BackendFake, Now: base,
		})
		if err != nil {
			t.Fatal(err)
		}
		return task, turn, route
	}

	first, firstTurn, firstRoute := create("verified")
	if err := store.CompleteTurn(ctx, first.ID, firstTurn.ID, firstRoute.ID, "done", "", false, false, base.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	completed := base.Add(time.Second)
	if err := store.RecordVerificationRun(ctx, VerificationRun{
		ID: "verification_1", TaskID: first.ID, TurnID: firstTurn.ID, Status: VerificationPassed,
		Trigger: "automatic", StartedAt: base, CompletedAt: &completed,
	}); err != nil {
		t.Fatal(err)
	}

	second, secondTurn, secondRoute := create("recovered")
	if err := store.FailTurn(ctx, second.ID, secondTurn.ID, secondRoute.ID, "daemon_interrupted_write", "restart", "", true, base.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}

	third, thirdTurn, thirdRoute := create("handoff")
	if err := store.CompleteTurn(ctx, third.ID, thirdTurn.ID, thirdRoute.ID, "done", "fp", false, false, base.Add(500*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	handoffTurn, handoffRoute, _, err := store.CreateHandoffTurn(ctx, CreateHandoffInput{
		HandoffID: "handoff_1", TaskID: third.ID, TurnID: "turn_handoff_dest", RouteAttemptID: "route_handoff_dest",
		FromBackend: agent.BackendFake, ToBackend: agent.BackendCodex, Message: "switch", Permission: agent.PermissionReadOnly,
		WorkspaceFingerprint: "fp", Packet: json.RawMessage(`{"version":1}`), Now: base.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteTurn(ctx, third.ID, handoffTurn.ID, handoffRoute.ID, "done", "fp", false, false, base.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRouteAttempt(ctx, RouteAttempt{
		ID: "route_fallback", TurnID: thirdTurn.ID, Ordinal: 2, Backend: agent.BackendClaude,
		DecisionReason: "automatic_fallback_v1", Status: "failed", StartedAt: base,
	}); err != nil {
		t.Fatal(err)
	}

	metrics, err := store.ReliabilityMetrics(ctx, base.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if metrics.TasksTotal != 3 || metrics.TurnsTotal != 4 || metrics.TurnsSucceeded != 3 || metrics.TurnsFailed != 1 || metrics.Handoffs != 1 || metrics.AutomaticFallbacks != 1 {
		t.Fatalf("metrics=%+v", metrics)
	}
	if metrics.CrashRecoveries != 1 || metrics.CrashRecoveriesAttention != 1 || metrics.VerifiedWithoutManualSwitch != 1 || metrics.VerificationOutcomes[string(VerificationPassed)] != 1 {
		t.Fatalf("reliability outcomes=%+v", metrics)
	}
	if metrics.SuccessfulLatency.Count != 3 || metrics.SuccessfulLatency.P50MS != 1000 || metrics.SuccessfulLatency.P95MS != 2000 {
		t.Fatalf("successful latency=%+v", metrics.SuccessfulLatency)
	}
	raw, err := json.Marshal(metrics)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{"secret prompt", "/private/", "task_verified", "workspace_verified"} {
		if stringContains(string(raw), private) {
			t.Fatalf("aggregate metrics leaked %q: %s", private, raw)
		}
	}
}

func TestReliabilityTelemetryClaimIsDurableAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tasks.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	now := time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
	claimed, err := store.ClaimReliabilityTelemetry(ctx, now, 24*time.Hour)
	if err != nil || !claimed {
		t.Fatalf("first claim=%v err=%v", claimed, err)
	}
	store.Close()
	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	claimed, err = store.ClaimReliabilityTelemetry(ctx, now.Add(23*time.Hour), 24*time.Hour)
	if err != nil || claimed {
		t.Fatalf("early claim=%v err=%v", claimed, err)
	}
	claimed, err = store.ClaimReliabilityTelemetry(ctx, now.Add(24*time.Hour), 24*time.Hour)
	if err != nil || !claimed {
		t.Fatalf("due claim=%v err=%v", claimed, err)
	}
}

func stringContains(value, fragment string) bool {
	return len(fragment) != 0 && strings.Contains(value, fragment)
}
