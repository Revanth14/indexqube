package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
	"github.com/Revanth14/indexqube/gateway/internal/orchestrator"
	"github.com/Revanth14/indexqube/gateway/internal/taskstore"
)

func TestLifecycleCLICommands(t *testing.T) {
	token := installControlTestCredential(t)
	now := time.Now().UTC()
	task := taskstore.Task{ID: "task_cli_lifecycle", Status: taskstore.TaskRunning}
	seen := make([]string, 0, 5)
	oldClient := lifecycleHTTPClient
	lifecycleHTTPClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			t.Errorf("missing control API authentication")
		}
		seen = append(seen, r.Method+" "+r.URL.Path)
		status := http.StatusOK
		var payload any
		switch r.URL.Path {
		case "/control/v1/tasks/task_cli_lifecycle/cancel":
			status = http.StatusAccepted
			payload = orchestrator.CancelTaskResult{Task: task, Cancellation: taskstore.Cancellation{
				ID: "cancel_cli", TaskID: task.ID, TurnID: "turn_cli", Status: taskstore.CancellationRequested, RequestedAt: now,
			}}
		case "/control/v1/tasks/task_cli_lifecycle/close":
			closed := task
			closed.Status = taskstore.TaskClosed
			payload = orchestrator.TaskTransitionResult{Task: closed, Changed: true}
		case "/control/v1/tasks/task_cli_lifecycle/reopen":
			open := task
			open.Status = taskstore.TaskOpen
			payload = orchestrator.TaskTransitionResult{Task: open, Changed: false}
		case "/control/v1/tasks/task_cli_lifecycle/pin":
			payload = orchestrator.TaskPinResult{Task: task, BackendPin: &taskstore.BackendPin{
				TaskID: task.ID, Backend: agent.BackendCodex, CreatedAt: now, UpdatedAt: now,
			}, Changed: true}
		case "/control/v1/tasks/task_cli_lifecycle/unpin":
			payload = orchestrator.TaskPinResult{Task: task, Changed: true}
		default:
			status = http.StatusNotFound
			payload = map[string]string{"error": "not found"}
		}
		raw, _ := json.Marshal(payload)
		return &http.Response{StatusCode: status, Header: authenticatedControlHeader(), Body: io.NopCloser(bytes.NewReader(raw)), Request: r}, nil
	})}
	t.Cleanup(func() { lifecycleHTTPClient = oldClient })
	t.Setenv("INDEXQUBE_CONTROL_URL", "http://127.0.0.1:17374")

	var cancelled bytes.Buffer
	if err := runCancelCommand(context.Background(), []string{task.ID}, &cancelled); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(cancelled.String(), "cancellation requested") {
		t.Fatalf("cancel output=%q", cancelled.String())
	}
	var closed bytes.Buffer
	if err := runTaskLifecycleCommand(context.Background(), []string{task.ID}, "close", &closed); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(closed.String(), "closed") {
		t.Fatalf("close output=%q", closed.String())
	}
	var reopened bytes.Buffer
	if err := runTaskLifecycleCommand(context.Background(), []string{task.ID}, "reopen", &reopened); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(reopened.String(), "open (unchanged)") {
		t.Fatalf("reopen output=%q", reopened.String())
	}
	var pinned bytes.Buffer
	if err := runTaskPinCommand(context.Background(), []string{task.ID}, "pin", &pinned); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(pinned.String(), "pinned to codex") {
		t.Fatalf("pin output=%q", pinned.String())
	}
	var unpinned bytes.Buffer
	if err := runTaskPinCommand(context.Background(), []string{task.ID}, "unpin", &unpinned); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(unpinned.String(), "backend unpinned") {
		t.Fatalf("unpin output=%q", unpinned.String())
	}
	if len(seen) != 5 || seen[0] != "POST /control/v1/tasks/task_cli_lifecycle/cancel" ||
		seen[1] != "POST /control/v1/tasks/task_cli_lifecycle/close" || seen[2] != "POST /control/v1/tasks/task_cli_lifecycle/reopen" ||
		seen[3] != "POST /control/v1/tasks/task_cli_lifecycle/pin" || seen[4] != "POST /control/v1/tasks/task_cli_lifecycle/unpin" {
		t.Fatalf("requests=%v", seen)
	}
}

func TestTaskEvidenceRendersCancellation(t *testing.T) {
	var out bytes.Buffer
	renderTaskEvidence(&out, taskstore.TaskEvidence{
		Task: taskstore.Task{ID: "task_cancelled", Status: taskstore.TaskOpen},
		Cancellations: []taskstore.Cancellation{{
			ID: "cancel_1", TaskID: "task_cancelled", TurnID: "turn_1", Status: taskstore.CancellationCompleted,
		}},
	})
	if !strings.Contains(out.String(), "Cancellations:") || !strings.Contains(out.String(), "cancel_1") ||
		!strings.Contains(out.String(), "completed") {
		t.Fatalf("output=%s", out.String())
	}
}
