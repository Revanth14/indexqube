package claude

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
)

const (
	permissionServerName = "indexqube"
	permissionToolName   = "mcp__indexqube__approval_prompt"
	writeTools           = "Read,Glob,Grep,Bash,Edit,Write,NotebookEdit,MultiEdit"
	maxPermissionBytes   = 1 << 20
)

type permissionRequest struct {
	ToolName string          `json:"tool_name"`
	Input    json.RawMessage `json:"input"`
}

type permissionResponse struct {
	Behavior     string          `json:"behavior"`
	UpdatedInput json.RawMessage `json:"updatedInput,omitempty"`
	Message      string          `json:"message,omitempty"`
}

type permissionBridge struct {
	dir           string
	configPath    string
	listener      net.Listener
	done          chan struct{}
	closeOnce     sync.Once
	connectionsMu sync.Mutex
	connections   map[net.Conn]struct{}
	ctx           context.Context
	cancel        context.CancelFunc
	handler       agent.ApprovalHandler
	workspace     string
	requestID     string
	sequence      atomic.Uint64
}

type mcpConfig struct {
	Servers map[string]mcpServer `json:"mcpServers"`
}

type mcpServer struct {
	Type    string   `json:"type"`
	Command string   `json:"command"`
	Args    []string `json:"args"`
}

type permissionEndpoint struct {
	listener net.Listener
	network  string
	address  string
}

var openPermissionEndpoint = func(dir string) (permissionEndpoint, error) {
	socketPath := filepath.Join(dir, "approval.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return permissionEndpoint{}, err
	}
	if err := os.Chmod(socketPath, 0o600); err != nil {
		_ = listener.Close()
		return permissionEndpoint{}, err
	}
	return permissionEndpoint{listener: listener, network: "unix", address: socketPath}, nil
}

func startPermissionBridge(ctx context.Context, handler agent.ApprovalHandler, workspace string) (*permissionBridge, error) {
	if handler == nil {
		return nil, errors.New("claude backend: durable approval handler is required for workspace-write")
	}
	absWorkspace, err := filepath.Abs(workspace)
	if err != nil {
		return nil, fmt.Errorf("claude backend: resolve workspace: %w", err)
	}
	absWorkspace, err = filepath.EvalSymlinks(absWorkspace)
	if err != nil {
		return nil, fmt.Errorf("claude backend: resolve workspace links: %w", err)
	}
	// Unix-domain socket paths are short on macOS. /tmp is the platform's
	// private temporary root there, and MkdirTemp still creates mode 0700.
	dir, err := os.MkdirTemp("/tmp", "iq-claude-perm-")
	if err != nil {
		return nil, fmt.Errorf("claude backend: create permission runtime: %w", err)
	}
	cleanup := func() { _ = os.RemoveAll(dir) }
	if err := os.Chmod(dir, 0o700); err != nil {
		cleanup()
		return nil, fmt.Errorf("claude backend: secure permission runtime: %w", err)
	}
	endpoint, err := openPermissionEndpoint(dir)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("claude backend: listen for permission helper: %w", err)
	}
	listener := endpoint.listener
	executable, err := os.Executable()
	if err != nil {
		_ = listener.Close()
		cleanup()
		return nil, fmt.Errorf("claude backend: resolve permission helper: %w", err)
	}
	config := mcpConfig{Servers: map[string]mcpServer{
		permissionServerName: {
			Type: "stdio", Command: executable,
			Args: []string{"__claude-permission-mcp", "--network", endpoint.network, "--socket", endpoint.address},
		},
	}}
	configBytes, err := json.Marshal(config)
	if err != nil {
		_ = listener.Close()
		cleanup()
		return nil, fmt.Errorf("claude backend: encode permission config: %w", err)
	}
	configPath := filepath.Join(dir, "mcp.json")
	if err := os.WriteFile(configPath, configBytes, 0o600); err != nil {
		_ = listener.Close()
		cleanup()
		return nil, fmt.Errorf("claude backend: write permission config: %w", err)
	}
	prefix, err := randomPermissionID()
	if err != nil {
		_ = listener.Close()
		cleanup()
		return nil, err
	}
	bridgeCtx, cancel := context.WithCancel(ctx)
	bridge := &permissionBridge{
		dir: dir, configPath: configPath, listener: listener, done: make(chan struct{}),
		connections: make(map[net.Conn]struct{}),
		ctx:         bridgeCtx, cancel: cancel, handler: handler,
		workspace: filepath.Clean(absWorkspace), requestID: prefix,
	}
	go bridge.serve()
	return bridge, nil
}

func randomPermissionID() (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("claude backend: generate permission request ID: %w", err)
	}
	return "claude-" + hex.EncodeToString(raw), nil
}

func (b *permissionBridge) Close() {
	b.closeOnce.Do(func() {
		b.cancel()
		_ = b.listener.Close()
		b.connectionsMu.Lock()
		for connection := range b.connections {
			_ = connection.Close()
		}
		b.connectionsMu.Unlock()
		<-b.done
		_ = os.RemoveAll(b.dir)
	})
}

func (b *permissionBridge) serve() {
	defer close(b.done)
	for {
		connection, err := b.listener.Accept()
		if err != nil {
			return
		}
		b.connectionsMu.Lock()
		b.connections[connection] = struct{}{}
		b.connectionsMu.Unlock()
		b.handleConnection(connection)
		b.connectionsMu.Lock()
		delete(b.connections, connection)
		b.connectionsMu.Unlock()
	}
}

func (b *permissionBridge) handleConnection(connection net.Conn) {
	defer connection.Close()
	scanner := bufio.NewScanner(connection)
	scanner.Buffer(make([]byte, 64<<10), maxPermissionBytes)
	encoder := json.NewEncoder(connection)
	for scanner.Scan() {
		var request permissionRequest
		if err := json.Unmarshal(scanner.Bytes(), &request); err != nil {
			_ = encoder.Encode(permissionResponse{Behavior: "deny", Message: "IndexQube received an invalid permission request"})
			continue
		}
		response := b.decide(b.ctx, request)
		if err := encoder.Encode(response); err != nil {
			return
		}
	}
}

func (b *permissionBridge) decide(ctx context.Context, request permissionRequest) permissionResponse {
	input := request.Input
	if len(input) == 0 || string(input) == "null" {
		input = json.RawMessage(`{}`)
	}
	var inputObject map[string]any
	if err := json.Unmarshal(input, &inputObject); err != nil || inputObject == nil {
		return permissionResponse{Behavior: "deny", Message: "IndexQube denied malformed tool input"}
	}
	tool := strings.TrimSpace(request.ToolName)
	if isReadTool(tool) {
		path, hasPath := toolPath(tool, input)
		if tool == "Read" && !hasPath {
			return permissionResponse{Behavior: "deny", Message: "IndexQube denied a read without a target path"}
		}
		if hasPath && !b.withinWorkspace(path) {
			return permissionResponse{Behavior: "deny", Message: "IndexQube denied a read outside the task workspace"}
		}
		return permissionResponse{Behavior: "allow", UpdatedInput: input}
	}
	approval, denial := b.approvalRequest(tool, input)
	if denial != "" {
		return permissionResponse{Behavior: "deny", Message: denial}
	}
	approval.BackendRequestID = fmt.Sprintf("%s-%d", b.requestID, b.sequence.Add(1))
	decision, err := b.handler.RequestApproval(ctx, approval)
	if err != nil {
		return permissionResponse{Behavior: "deny", Message: "IndexQube approval was cancelled: " + bounded(err.Error(), 256)}
	}
	if decision != agent.ApprovalAccept {
		return permissionResponse{Behavior: "deny", Message: "The user denied this operation in IndexQube"}
	}
	return permissionResponse{Behavior: "allow", UpdatedInput: input}
}

func (b *permissionBridge) approvalRequest(tool string, input json.RawMessage) (agent.ApprovalRequest, string) {
	var values map[string]any
	if err := json.Unmarshal(input, &values); err != nil {
		return agent.ApprovalRequest{}, "IndexQube denied malformed tool input"
	}
	reason, _ := values["description"].(string)
	switch tool {
	case "Bash":
		command, _ := values["command"].(string)
		if strings.TrimSpace(command) == "" {
			return agent.ApprovalRequest{}, "IndexQube denied a shell request without a command"
		}
		if len(command) > 32<<10 {
			return agent.ApprovalRequest{}, "IndexQube denied a shell request too large to review safely"
		}
		cwd := b.workspace
		if candidate, _ := values["cwd"].(string); candidate != "" {
			var err error
			cwd, err = b.canonicalPath(candidate)
			if err != nil || !b.withinWorkspace(cwd) {
				return agent.ApprovalRequest{}, "IndexQube denied a command outside the task workspace"
			}
		}
		return agent.ApprovalRequest{
			Kind: agent.ApprovalCommand, Reason: bounded(reason, 1024),
			Command: command, CWD: cwd,
		}, ""
	case "Edit", "Write", "NotebookEdit", "MultiEdit":
		path, ok := toolPath(tool, input)
		if !ok || strings.TrimSpace(path) == "" {
			return agent.ApprovalRequest{}, "IndexQube denied a file change without a target path"
		}
		absolute, err := b.canonicalPath(path)
		if err != nil || !b.withinWorkspace(absolute) {
			return agent.ApprovalRequest{}, "IndexQube denied a file change outside the task workspace"
		}
		return agent.ApprovalRequest{
			Kind: agent.ApprovalFileChange, Reason: bounded(reason, 1024), GrantRoot: absolute,
		}, ""
	default:
		return agent.ApprovalRequest{}, "IndexQube denied an unavailable Claude tool"
	}
}

func isReadTool(tool string) bool {
	return tool == "Read" || tool == "Glob" || tool == "Grep"
}

func toolPath(tool string, input json.RawMessage) (string, bool) {
	var values map[string]any
	if json.Unmarshal(input, &values) != nil {
		return "", false
	}
	keys := []string{"file_path", "path"}
	if tool == "NotebookEdit" {
		keys = []string{"notebook_path", "file_path", "path"}
	}
	for _, key := range keys {
		if value, ok := values[key].(string); ok && value != "" {
			return value, true
		}
	}
	return "", false
}

func (b *permissionBridge) resolvePath(path string) string {
	if !filepath.IsAbs(path) {
		path = filepath.Join(b.workspace, path)
	}
	return filepath.Clean(path)
}

func (b *permissionBridge) withinWorkspace(path string) bool {
	absolute, err := b.canonicalPath(path)
	if err != nil {
		return false
	}
	relative, err := filepath.Rel(b.workspace, absolute)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func (b *permissionBridge) canonicalPath(path string) (string, error) {
	path = b.resolvePath(path)
	current := path
	var suffix []string
	for {
		resolved, err := filepath.EvalSymlinks(current)
		if err == nil {
			parts := append([]string{resolved}, suffix...)
			return filepath.Clean(filepath.Join(parts...)), nil
		}
		if !os.IsNotExist(err) {
			return "", err
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", err
		}
		suffix = append([]string{filepath.Base(current)}, suffix...)
		current = parent
	}
}
