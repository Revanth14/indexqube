package control

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
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

func TestControlFakeAgentProcess(t *testing.T) {
	if os.Getenv("INDEXQUBE_CONTROL_FAKE_HELPER") != "1" {
		return
	}
	os.Exit(fake.RunHelper(os.Stdin, os.Stdout, os.Stderr))
}

func TestCreateTaskAndReplaySSE(t *testing.T) {
	handler, root := newControlTestHandler(t)
	body, _ := json.Marshal(createTaskRequest{
		Workspace: root, Prompt: "hello", Provider: agent.BackendFake, Permission: agent.PermissionReadOnly,
	})
	req := httptest.NewRequest(http.MethodPost, "/control/v1/tasks", bytes.NewReader(body))
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
		get := httptest.NewRequest(http.MethodGet, "/control/v1/tasks/"+task.ID+"/events", nil)
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
			list := httptest.NewRequest(http.MethodGet, "/control/v1/tasks?limit=10", nil)
			listed := httptest.NewRecorder()
			handler.ServeHTTP(listed, list)
			if listed.Code != http.StatusOK || !strings.Contains(listed.Body.String(), task.ID) {
				t.Fatalf("list status=%d body=%s", listed.Code, listed.Body.String())
			}
			evidenceReq := httptest.NewRequest(http.MethodGet, "/control/v1/tasks/"+task.ID+"/evidence", nil)
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
	return NewHandler(service), root
}

func runControlGit(t *testing.T, root string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", root}, args...)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
}
