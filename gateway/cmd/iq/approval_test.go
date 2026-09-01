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

func TestApprovalCLIListsAndDecides(t *testing.T) {
	now := time.Now().UTC()
	approval := taskstore.Approval{
		ID: "approval_cli", TaskID: "task_cli", TurnID: "turn_cli", Backend: agent.BackendCodex,
		Kind: agent.ApprovalCommand, Command: "go test ./...", Status: taskstore.ApprovalPending, RequestedAt: now,
	}
	var gotDecision string
	oldClient := approvalHTTPClient
	approvalHTTPClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		var status = http.StatusOK
		var payload any
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/control/v1/approvals":
			if r.URL.Query().Get("status") != "pending" || r.URL.Query().Get("task_id") != "task_cli" {
				t.Errorf("query=%s", r.URL.RawQuery)
			}
			payload = map[string]any{"approvals": []taskstore.Approval{approval}}
		case r.Method == http.MethodPost && r.URL.Path == "/control/v1/approvals/approval_cli/decision":
			var body map[string]string
			_ = json.NewDecoder(r.Body).Decode(&body)
			gotDecision = body["decision"]
			resolved := approval
			resolved.Status = taskstore.ApprovalApproved
			resolved.Decision = agent.ApprovalAccept
			payload = resolved
		default:
			status = http.StatusNotFound
			payload = map[string]string{"error": "not found"}
		}
		raw, _ := json.Marshal(payload)
		return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(raw)), Request: r}, nil
	})}
	t.Cleanup(func() { approvalHTTPClient = oldClient })
	t.Setenv("INDEXQUBE_CONTROL_URL", "http://indexqube.test")

	var listed, errOut bytes.Buffer
	if err := runApprovalsCommand(context.Background(), []string{"--task", "task_cli"}, &listed, &errOut); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(listed.String(), "approval_cli") || !strings.Contains(listed.String(), "go test ./...") {
		t.Fatalf("list output=%s", listed.String())
	}
	var decided bytes.Buffer
	if err := runApprovalDecisionCommand(context.Background(), []string{"approval_cli"}, "approve", &decided); err != nil {
		t.Fatal(err)
	}
	if gotDecision != "approve" || !strings.Contains(decided.String(), "approved") {
		t.Fatalf("decision=%q output=%s", gotDecision, decided.String())
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestTaskEvidenceRendersApproval(t *testing.T) {
	evidence := taskstore.TaskEvidence{
		Task: taskstore.Task{ID: "task_approval", Status: taskstore.TaskAwaitingApproval},
		Approvals: []taskstore.Approval{{
			ID: "approval_1", Status: taskstore.ApprovalPending, Kind: agent.ApprovalCommand,
			Command: "curl https://example.com",
		}},
	}
	var out bytes.Buffer
	renderTaskEvidence(&out, evidence)
	if !strings.Contains(out.String(), "Approvals:") || !strings.Contains(out.String(), "approval_1") ||
		!strings.Contains(out.String(), "curl https://example.com") {
		t.Fatalf("output=%s", out.String())
	}
}
