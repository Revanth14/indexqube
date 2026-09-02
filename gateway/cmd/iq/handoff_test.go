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
	"github.com/Revanth14/indexqube/gateway/internal/taskstore"
)

func TestHandoffCLIParsesDocumentedOrderAndStreamsDestination(t *testing.T) {
	token := installControlTestCredential(t)
	oldClient := handoffHTTPClient
	oldStream := handoffEventStream
	t.Cleanup(func() {
		handoffHTTPClient = oldClient
		handoffEventStream = oldStream
	})
	var requestBody map[string]any
	handoffHTTPClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodPost || request.URL.Path != "/control/v1/tasks/task_cli_handoff/handoffs" {
			t.Errorf("request=%s %s", request.Method, request.URL.Path)
		}
		if request.Header.Get("Authorization") != "Bearer "+token {
			t.Errorf("missing control authentication")
		}
		if err := json.NewDecoder(request.Body).Decode(&requestBody); err != nil {
			t.Fatal(err)
		}
		payload, _ := json.Marshal(map[string]any{
			"task": taskstore.Task{ID: "task_cli_handoff", PreferredBackend: agent.BackendClaude, Status: taskstore.TaskRunning},
			"handoff": taskstore.Handoff{ID: "handoff_cli", TaskID: "task_cli_handoff", TurnID: "turn_cli",
				FromBackend: agent.BackendCodex, ToBackend: agent.BackendClaude, Packet: json.RawMessage(`{"version":1}`), CreatedAt: time.Now().UTC()},
			"after_sequence": 17,
		})
		return &http.Response{StatusCode: http.StatusAccepted, Header: authenticatedControlHeader(),
			Body: io.NopCloser(bytes.NewReader(payload)), Request: request}, nil
	})}
	var streamedTask string
	var streamedAfter int64
	handoffEventStream = func(_ context.Context, _ string, taskID string, after int64, _, _ io.Writer) error {
		streamedTask, streamedAfter = taskID, after
		return nil
	}
	t.Setenv("INDEXQUBE_CONTROL_URL", "http://127.0.0.1:17374")
	var stdout, stderr bytes.Buffer
	if err := runHandoffCommand(context.Background(), []string{"task_cli_handoff", "--to", "claude", "finish", "the", "review"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if requestBody["to_backend"] != "claude" || requestBody["prompt"] != "finish the review" {
		t.Fatalf("request body=%+v", requestBody)
	}
	if streamedTask != "task_cli_handoff" || streamedAfter != 17 || !strings.Contains(stderr.String(), "codex -> claude") {
		t.Fatalf("task=%q after=%d stderr=%q", streamedTask, streamedAfter, stderr.String())
	}
}

func TestParseHandoffArgsAcceptsFlagBeforeOrAfterTask(t *testing.T) {
	for _, args := range [][]string{{"task_1", "--to", "claude"}, {"--to=codex", "task_1"}} {
		taskID, backend, _, err := parseHandoffArgs(args)
		if err != nil || taskID != "task_1" || backend == "" {
			t.Fatalf("args=%v task=%q backend=%q err=%v", args, taskID, backend, err)
		}
	}
	if _, _, _, err := parseHandoffArgs([]string{"task_1"}); err == nil {
		t.Fatal("missing destination accepted")
	}
}
