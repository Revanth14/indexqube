package control

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
	"github.com/Revanth14/indexqube/gateway/internal/agent/fake"
	"github.com/Revanth14/indexqube/gateway/internal/orchestrator"
	"github.com/Revanth14/indexqube/gateway/internal/taskstore"
	"github.com/Revanth14/indexqube/gateway/internal/workspace"
)

const controlTestToken = "test-control-token"

func authorizedControlRequest(method, target string, body io.Reader) *http.Request {
	req := httptest.NewRequest(method, target, body)
	req.Header.Set("Authorization", "Bearer "+controlTestToken)
	return req
}

func TestControlFakeAgentProcess(t *testing.T) {
	if os.Getenv("INDEXQUBE_CONTROL_FAKE_HELPER") != "1" {
		return
	}
	os.Exit(fake.RunHelper(os.Stdin, os.Stdout, os.Stderr))
}

func TestControlAPIRequiresAuthenticationOnEveryRoute(t *testing.T) {
	handler, _ := newControlTestHandler(t)
	routes := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/control/healthz"},
		{http.MethodGet, "/control/v1/backends"},
		{http.MethodGet, "/control/v1/approvals"},
		{http.MethodPost, "/control/v1/approvals/approval/decision"},
		{http.MethodGet, "/control/v1/tasks"},
		{http.MethodPost, "/control/v1/tasks"},
		{http.MethodGet, "/control/v1/tasks/task"},
		{http.MethodGet, "/control/v1/tasks/task/state"},
		{http.MethodGet, "/control/v1/tasks/task/evidence"},
		{http.MethodPost, "/control/v1/tasks/task/turns"},
		{http.MethodPost, "/control/v1/tasks/task/handoffs"},
		{http.MethodPost, "/control/v1/tasks/task/cancel"},
		{http.MethodPost, "/control/v1/tasks/task/close"},
		{http.MethodPost, "/control/v1/tasks/task/reopen"},
		{http.MethodGet, "/control/v1/tasks/task/events"},
	}
	for _, route := range routes {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			for _, authorization := range []string{"", "Bearer wrong-token", "Basic " + controlTestToken} {
				req := httptest.NewRequest(route.method, route.path, nil)
				if authorization != "" {
					req.Header.Set("Authorization", authorization)
				}
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
				if rec.Code != http.StatusUnauthorized || !strings.Contains(rec.Body.String(), `"code":"unauthorized"`) {
					t.Fatalf("authorization=%q status=%d body=%s", authorization, rec.Code, rec.Body.String())
				}
				if got := rec.Header().Get("WWW-Authenticate"); got == "" {
					t.Fatal("missing WWW-Authenticate header")
				}
				if got := rec.Header().Get("Cache-Control"); got != "no-store" {
					t.Fatalf("Cache-Control=%q", got)
				}
				if got := rec.Header().Get(AuthContractHeader); got != AuthContractValue {
					t.Fatalf("authentication contract header=%q", got)
				}
			}
		})
	}

	req := authorizedControlRequest(http.MethodGet, "/control/healthz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"status":"ok"`) {
		t.Fatalf("authenticated health status=%d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get(AuthContractHeader); got != AuthContractValue {
		t.Fatalf("authenticated health contract header=%q", got)
	}
}

type controlHandoffBackend struct{}

func (controlHandoffBackend) ID() agent.BackendID { return agent.BackendClaude }
func (controlHandoffBackend) Probe(context.Context) agent.BackendHealth {
	return agent.BackendHealth{Backend: agent.BackendClaude, Status: agent.HealthAvailable, CheckedAt: time.Now().UTC()}
}
func (controlHandoffBackend) Execute(_ context.Context, req agent.Request, _ agent.EventSink) (agent.Result, error) {
	if !strings.Contains(req.Prompt, "INDEXQUBE CANONICAL HANDOFF") {
		return agent.Result{}, errors.New("missing canonical handoff packet")
	}
	return agent.Result{NativeSessionID: "control-handoff-session", FinalMessage: "handoff complete"}, nil
}

func TestHandoffAPIStartsDestinationAndReturnsDurablePacket(t *testing.T) {
	handler, root := newControlHandoffTestHandler(t)
	body, _ := json.Marshal(createTaskRequest{
		Workspace: root, Prompt: "initial task", Backend: agent.BackendFake, Permission: agent.PermissionReadOnly,
	})
	create := authorizedControlRequest(http.MethodPost, "/control/v1/tasks", bytes.NewReader(body))
	created := httptest.NewRecorder()
	handler.ServeHTTP(created, create)
	if created.Code != http.StatusAccepted {
		t.Fatalf("create status=%d body=%s", created.Code, created.Body.String())
	}
	var task taskstore.Task
	if err := json.Unmarshal(created.Body.Bytes(), &task); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	open := false
	for time.Now().Before(deadline) {
		stored, ok, err := handler.service.Task(context.Background(), task.ID)
		if err == nil && ok && stored.Status == taskstore.TaskOpen {
			open = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !open {
		t.Fatal("timed out waiting for source task completion")
	}
	handoffBody, _ := json.Marshal(handoffTaskRequest{ToBackend: agent.BackendClaude, Prompt: "finish on Claude"})
	request := authorizedControlRequest(http.MethodPost, "/control/v1/tasks/"+task.ID+"/handoffs", bytes.NewReader(handoffBody))
	request.SetPathValue("taskID", task.ID)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("handoff status=%d body=%s", response.Code, response.Body.String())
	}
	var result struct {
		Task    taskstore.Task    `json:"task"`
		Handoff taskstore.Handoff `json:"handoff"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Task.PreferredBackend != agent.BackendClaude || result.Handoff.ID == "" || !json.Valid(result.Handoff.Packet) {
		t.Fatalf("result=%+v", result)
	}
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		evidence, ok, err := handler.service.TaskEvidence(context.Background(), task.ID)
		if err == nil && ok && evidence.Task.Status == taskstore.TaskOpen {
			if len(evidence.Handoffs) != 1 || evidence.Routes[len(evidence.Routes)-1].DecisionReason != "explicit_handoff" {
				t.Fatalf("evidence=%+v", evidence)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("timed out waiting for handoff completion")
}

func TestCreateTaskAndReplaySSE(t *testing.T) {
	handler, root := newControlTestHandler(t)
	body, _ := json.Marshal(createTaskRequest{
		Workspace: root, Prompt: "hello", Provider: agent.BackendFake, Permission: agent.PermissionReadOnly,
	})
	req := authorizedControlRequest(http.MethodPost, "/control/v1/tasks", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("create status=%d body=%s", rec.Code, rec.Body.String())
	}
	var task taskstore.Task
	if err := json.Unmarshal(rec.Body.Bytes(), &task); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		get := authorizedControlRequest(http.MethodGet, "/control/v1/tasks/"+task.ID+"/events", nil)
		get.SetPathValue("taskID", task.ID)
		stream := httptest.NewRecorder()
		handler.ServeHTTP(stream, get)
		if strings.Contains(stream.Body.String(), "event: completed") {
			scanner := bufio.NewScanner(strings.NewReader(stream.Body.String()))
			var dataLines int
			for scanner.Scan() {
				if strings.HasPrefix(scanner.Text(), "data: ") {
					dataLines++
				}
			}
			if dataLines < 3 {
				t.Fatalf("SSE data lines=%d body=%s", dataLines, stream.Body.String())
			}
			list := authorizedControlRequest(http.MethodGet, "/control/v1/tasks?limit=10", nil)
			listed := httptest.NewRecorder()
			handler.ServeHTTP(listed, list)
			if listed.Code != http.StatusOK || !strings.Contains(listed.Body.String(), task.ID) {
				t.Fatalf("list status=%d body=%s", listed.Code, listed.Body.String())
			}
			evidenceReq := authorizedControlRequest(http.MethodGet, "/control/v1/tasks/"+task.ID+"/evidence", nil)
			evidenceReq.SetPathValue("taskID", task.ID)
			evidenceRec := httptest.NewRecorder()
			handler.ServeHTTP(evidenceRec, evidenceReq)
			if evidenceRec.Code != http.StatusOK {
				t.Fatalf("evidence status=%d body=%s", evidenceRec.Code, evidenceRec.Body.String())
			}
			var evidence taskstore.TaskEvidence
			if err := json.Unmarshal(evidenceRec.Body.Bytes(), &evidence); err != nil || len(evidence.Turns) != 1 || len(evidence.Snapshots) != 2 {
				t.Fatalf("evidence=%+v err=%v", evidence, err)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("timed out waiting for completed stream")
}

type controlApprovalBackend struct{}

func (controlApprovalBackend) ID() agent.BackendID { return agent.BackendFake }

func (controlApprovalBackend) Probe(context.Context) agent.BackendHealth {
	return agent.BackendHealth{Backend: agent.BackendFake, Status: agent.HealthAvailable, CheckedAt: time.Now().UTC()}
}

func (controlApprovalBackend) Execute(ctx context.Context, req agent.Request, _ agent.EventSink) (agent.Result, error) {
	decision, err := req.Approvals.RequestApproval(ctx, agent.ApprovalRequest{
		BackendRequestID: "control-request", Kind: agent.ApprovalCommand, Command: "go test ./...", Reason: "control fixture",
	})
	if err != nil {
		return agent.Result{}, err
	}
	return agent.Result{NativeSessionID: "control-approval-session", FinalMessage: "decision: " + string(decision)}, nil
}

func TestApprovalListAndDecisionAPI(t *testing.T) {
	handler, root := newControlApprovalTestHandler(t)
	body, _ := json.Marshal(createTaskRequest{
		Workspace: root, Prompt: "guard this", Backend: agent.BackendFake, Permission: agent.PermissionWrite,
	})
	create := authorizedControlRequest(http.MethodPost, "/control/v1/tasks", bytes.NewReader(body))
	created := httptest.NewRecorder()
	handler.ServeHTTP(created, create)
	if created.Code != http.StatusAccepted {
		t.Fatalf("create status=%d body=%s", created.Code, created.Body.String())
	}
	var task taskstore.Task
	if err := json.Unmarshal(created.Body.Bytes(), &task); err != nil {
		t.Fatal(err)
	}

	var approval taskstore.Approval
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		list := authorizedControlRequest(http.MethodGet, "/control/v1/approvals?status=pending&task_id="+task.ID, nil)
		listed := httptest.NewRecorder()
		handler.ServeHTTP(listed, list)
		if listed.Code != http.StatusOK {
			t.Fatalf("list status=%d body=%s", listed.Code, listed.Body.String())
		}
		var response struct {
			Approvals []taskstore.Approval `json:"approvals"`
		}
		if err := json.Unmarshal(listed.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		if len(response.Approvals) == 1 {
			approval = response.Approvals[0]
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if approval.ID == "" || approval.Command != "go test ./..." {
		t.Fatalf("approval=%+v", approval)
	}
	decisionBody, _ := json.Marshal(approvalDecisionRequest{Decision: "approve"})
	decision := authorizedControlRequest(http.MethodPost, "/control/v1/approvals/"+approval.ID+"/decision", bytes.NewReader(decisionBody))
	decided := httptest.NewRecorder()
	handler.ServeHTTP(decided, decision)
	if decided.Code != http.StatusOK || !strings.Contains(decided.Body.String(), `"status":"approved"`) {
		t.Fatalf("decision status=%d body=%s", decided.Code, decided.Body.String())
	}

	for time.Now().Before(deadline) {
		evidenceReq := authorizedControlRequest(http.MethodGet, "/control/v1/tasks/"+task.ID+"/evidence", nil)
		evidenceRec := httptest.NewRecorder()
		handler.ServeHTTP(evidenceRec, evidenceReq)
		if evidenceRec.Code == http.StatusOK {
			var evidence taskstore.TaskEvidence
			if json.Unmarshal(evidenceRec.Body.Bytes(), &evidence) == nil && evidence.Task.Status == taskstore.TaskOpen {
				if len(evidence.Approvals) != 1 || evidence.Approvals[0].Status != taskstore.ApprovalApproved {
					t.Fatalf("evidence=%+v", evidence)
				}
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("timed out waiting for approved task completion")
}

func TestCancellationAndLifecycleAPIIsIdempotent(t *testing.T) {
	handler, root := newControlTestHandler(t)
	body, _ := json.Marshal(createTaskRequest{
		Workspace: root, Prompt: "[fake:sleep]", Backend: agent.BackendFake, Permission: agent.PermissionReadOnly,
	})
	create := authorizedControlRequest(http.MethodPost, "/control/v1/tasks", bytes.NewReader(body))
	created := httptest.NewRecorder()
	handler.ServeHTTP(created, create)
	if created.Code != http.StatusAccepted {
		t.Fatalf("create status=%d body=%s", created.Code, created.Body.String())
	}
	var task taskstore.Task
	if err := json.Unmarshal(created.Body.Bytes(), &task); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	requestCancel := func() (int, orchestrator.CancelTaskResult) {
		t.Helper()
		req := authorizedControlRequest(http.MethodPost, "/control/v1/tasks/"+task.ID+"/cancel", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		var result orchestrator.CancelTaskResult
		if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatalf("decode cancellation status=%d body=%s: %v", rec.Code, rec.Body.String(), err)
		}
		return rec.Code, result
	}
	firstStatus, first := requestCancel()
	if firstStatus != http.StatusAccepted || first.Cancellation.Status != taskstore.CancellationRequested {
		t.Fatalf("first status=%d result=%+v", firstStatus, first)
	}
	secondStatus, second := requestCancel()
	if secondStatus != http.StatusAccepted && secondStatus != http.StatusOK {
		t.Fatalf("second status=%d result=%+v", secondStatus, second)
	}
	if second.Cancellation.ID != first.Cancellation.ID {
		t.Fatalf("cancellations differ: first=%+v second=%+v", first, second)
	}

	for time.Now().Before(deadline) {
		stateReq := authorizedControlRequest(http.MethodGet, "/control/v1/tasks/"+task.ID+"/state", nil)
		stateRec := httptest.NewRecorder()
		handler.ServeHTTP(stateRec, stateReq)
		var state taskstore.TaskState
		if stateRec.Code == http.StatusOK && json.Unmarshal(stateRec.Body.Bytes(), &state) == nil &&
			state.LatestTurn != nil && state.LatestTurn.Status == taskstore.TurnCancelled {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	finalStatus, final := requestCancel()
	if finalStatus != http.StatusOK || final.Cancellation.ID != first.Cancellation.ID || final.Cancellation.Status != taskstore.CancellationCompleted {
		t.Fatalf("final status=%d result=%+v", finalStatus, final)
	}

	transition := func(action string) (int, orchestrator.TaskTransitionResult) {
		t.Helper()
		req := authorizedControlRequest(http.MethodPost, "/control/v1/tasks/"+task.ID+"/"+action, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		var result orchestrator.TaskTransitionResult
		if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatalf("decode %s status=%d body=%s: %v", action, rec.Code, rec.Body.String(), err)
		}
		return rec.Code, result
	}
	if status, result := transition("close"); status != http.StatusOK || !result.Changed || result.Task.Status != taskstore.TaskClosed {
		t.Fatalf("close status=%d result=%+v", status, result)
	}
	if status, result := transition("close"); status != http.StatusOK || result.Changed || result.Task.Status != taskstore.TaskClosed {
		t.Fatalf("second close status=%d result=%+v", status, result)
	}
	continueBody, _ := json.Marshal(continueTaskRequest{Prompt: "should fail"})
	continueReq := authorizedControlRequest(http.MethodPost, "/control/v1/tasks/"+task.ID+"/turns", bytes.NewReader(continueBody))
	continueRec := httptest.NewRecorder()
	handler.ServeHTTP(continueRec, continueReq)
	if continueRec.Code != http.StatusConflict {
		t.Fatalf("continue closed status=%d body=%s", continueRec.Code, continueRec.Body.String())
	}
	if status, result := transition("reopen"); status != http.StatusOK || !result.Changed || result.Task.Status != taskstore.TaskOpen {
		t.Fatalf("reopen status=%d result=%+v", status, result)
	}
	if status, result := transition("reopen"); status != http.StatusOK || result.Changed || result.Task.Status != taskstore.TaskOpen {
		t.Fatalf("second reopen status=%d result=%+v", status, result)
	}
}

func newControlTestHandler(t *testing.T) (*Handler, string) {
	t.Helper()
	root := t.TempDir()
	runControlGit(t, root, "init", "-q")
	runControlGit(t, root, "config", "user.email", "test@indexqube.local")
	runControlGit(t, root, "config", "user.name", "IndexQube Test")
	if err := os.WriteFile(filepath.Join(root, "README.md"), []byte("test\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runControlGit(t, root, "add", "README.md")
	runControlGit(t, root, "commit", "-q", "-m", "initial")
	state := t.TempDir()
	store, err := taskstore.Open(filepath.Join(state, "tasks.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	locks, err := workspace.NewLockManager(filepath.Join(state, "locks"), store, "control-test")
	if err != nil {
		t.Fatal(err)
	}
	binary, _ := os.Executable()
	backend := fake.NewCommand(agent.NewRunner(), binary, []string{"-test.run=TestControlFakeAgentProcess"}, []string{"INDEXQUBE_CONTROL_FAKE_HELPER=1"})
	service, err := orchestrator.NewService(context.Background(), store, locks, orchestrator.NewRegistry(backend))
	if err != nil {
		t.Fatal(err)
	}
	return NewHandler(service, controlTestToken), root
}

func newControlApprovalTestHandler(t *testing.T) (*Handler, string) {
	t.Helper()
	root := t.TempDir()
	runControlGit(t, root, "init", "-q")
	runControlGit(t, root, "config", "user.email", "test@indexqube.local")
	runControlGit(t, root, "config", "user.name", "IndexQube Test")
	if err := os.WriteFile(filepath.Join(root, "README.md"), []byte("test\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runControlGit(t, root, "add", "README.md")
	runControlGit(t, root, "commit", "-q", "-m", "initial")
	state := t.TempDir()
	store, err := taskstore.Open(filepath.Join(state, "tasks.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	locks, err := workspace.NewLockManager(filepath.Join(state, "locks"), store, "control-approval-test")
	if err != nil {
		t.Fatal(err)
	}
	service, err := orchestrator.NewService(context.Background(), store, locks, orchestrator.NewRegistry(controlApprovalBackend{}))
	if err != nil {
		t.Fatal(err)
	}
	return NewHandler(service, controlTestToken), root
}

func newControlHandoffTestHandler(t *testing.T) (*Handler, string) {
	t.Helper()
	root := t.TempDir()
	runControlGit(t, root, "init", "-q")
	runControlGit(t, root, "config", "user.email", "test@indexqube.local")
	runControlGit(t, root, "config", "user.name", "IndexQube Test")
	if err := os.WriteFile(filepath.Join(root, "README.md"), []byte("test\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runControlGit(t, root, "add", "README.md")
	runControlGit(t, root, "commit", "-q", "-m", "initial")
	state := t.TempDir()
	store, err := taskstore.Open(filepath.Join(state, "tasks.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	locks, err := workspace.NewLockManager(filepath.Join(state, "locks"), store, "control-handoff-test")
	if err != nil {
		t.Fatal(err)
	}
	binary, _ := os.Executable()
	source := fake.NewCommand(agent.NewRunner(), binary, []string{"-test.run=TestControlFakeAgentProcess"}, []string{"INDEXQUBE_CONTROL_FAKE_HELPER=1"})
	service, err := orchestrator.NewService(context.Background(), store, locks, orchestrator.NewRegistry(source, controlHandoffBackend{}))
	if err != nil {
		t.Fatal(err)
	}
	return NewHandler(service, controlTestToken), root
}

func runControlGit(t *testing.T, root string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", root}, args...)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
}
