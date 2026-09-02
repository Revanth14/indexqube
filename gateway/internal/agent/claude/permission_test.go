package claude

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
)

func TestPermissionBridgeClassifiesCommandsAndRejectsEscapes(t *testing.T) {
	useTCPPermissionEndpoint(t)
	handler := &claudeApprovalHandler{
		requests: make(chan agent.ApprovalRequest, 1), decisions: make(chan agent.ApprovalDecision, 1),
	}
	workspace := t.TempDir()
	bridge, err := startPermissionBridge(context.Background(), handler, workspace)
	if err != nil {
		t.Fatal(err)
	}
	defer bridge.Close()

	input := json.RawMessage(`{"command":"go test ./...","description":"verify changes"}`)
	done := make(chan permissionResponse, 1)
	go func() { done <- bridge.decide(context.Background(), permissionRequest{ToolName: "Bash", Input: input}) }()
	request := <-handler.requests
	canonicalWorkspace, err := filepath.EvalSymlinks(workspace)
	if err != nil {
		t.Fatal(err)
	}
	if request.Kind != agent.ApprovalCommand || request.Command != "go test ./..." || request.CWD != canonicalWorkspace || request.Reason != "verify changes" {
		t.Fatalf("request=%+v", request)
	}
	handler.decisions <- agent.ApprovalAccept
	if response := <-done; response.Behavior != "allow" || string(response.UpdatedInput) != string(input) {
		t.Fatalf("response=%+v", response)
	}

	outside := filepath.Join(filepath.Dir(workspace), "outside.txt")
	response := bridge.decide(context.Background(), permissionRequest{
		ToolName: "Write", Input: json.RawMessage(`{"file_path":` + mustJSON(t, outside) + `}`),
	})
	if response.Behavior != "deny" || !strings.Contains(response.Message, "outside") {
		t.Fatalf("outside response=%+v", response)
	}
	outsideDir := t.TempDir()
	if err := os.Symlink(outsideDir, filepath.Join(workspace, "escape")); err != nil {
		t.Fatal(err)
	}
	response = bridge.decide(context.Background(), permissionRequest{
		ToolName: "Write", Input: json.RawMessage(`{"file_path":"escape/through-link.txt"}`),
	})
	if response.Behavior != "deny" || !strings.Contains(response.Message, "outside") {
		t.Fatalf("symlink escape response=%+v", response)
	}
}

func TestPermissionMCPForwardsClaudeDecisionContract(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	serverDone := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer connection.Close()
		scanner := bufio.NewScanner(connection)
		if !scanner.Scan() {
			serverDone <- scanner.Err()
			return
		}
		var request permissionRequest
		if err := json.Unmarshal(scanner.Bytes(), &request); err != nil {
			serverDone <- err
			return
		}
		if request.ToolName != "Bash" {
			serverDone <- &unexpectedValue{got: request.ToolName, want: "Bash"}
			return
		}
		serverDone <- json.NewEncoder(connection).Encode(permissionResponse{Behavior: "allow", UpdatedInput: request.Input})
	}()

	input := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18"}}`,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`,
		`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"approval_prompt","arguments":{"tool_name":"Bash","input":{"command":"go test ./..."}}}}`,
	}, "\n") + "\n"
	var output, stderr bytes.Buffer
	if code := RunPermissionMCP([]string{"--network", "tcp", "--socket", listener.Addr().String()}, strings.NewReader(input), &output, &stderr); code != 0 {
		t.Fatalf("code=%d stderr=%s", code, stderr.String())
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("permission MCP did not forward the request")
	}
	var responses []jsonRPCResponse
	scanner := bufio.NewScanner(&output)
	for scanner.Scan() {
		var response jsonRPCResponse
		if err := json.Unmarshal(scanner.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		responses = append(responses, response)
	}
	if len(responses) != 3 {
		t.Fatalf("responses=%s", output.String())
	}
	encoded, _ := json.Marshal(responses[2].Result)
	if !bytes.Contains(encoded, []byte(`\"behavior\":\"allow\"`)) || !bytes.Contains(encoded, []byte(`\"updatedInput\"`)) {
		t.Fatalf("tool response=%s", encoded)
	}
}

func TestProductionPermissionEndpointIsPrivateUnixSocket(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "iq-perm-endpoint-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	endpoint, err := openPermissionEndpoint(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer endpoint.listener.Close()
	if endpoint.network != "unix" {
		t.Fatalf("network=%q", endpoint.network)
	}
	info, err := os.Stat(endpoint.address)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("socket mode=%o", info.Mode().Perm())
	}
}

type unexpectedValue struct{ got, want string }

func (e *unexpectedValue) Error() string { return "got " + e.got + ", want " + e.want }

func mustJSON(t *testing.T, value string) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}
