package claude

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
)

const fixtureSessionID = "11111111-1111-4111-8111-111111111111"

func TestClaudeProcessHelper(t *testing.T) {
	mode := os.Getenv("INDEXQUBE_CLAUDE_HELPER")
	if mode == "" {
		return
	}
	args := argsAfterSeparator(os.Args)
	prompt, _ := io.ReadAll(os.Stdin)
	requireArg := func(value string) {
		if !slices.Contains(args, value) {
			fmt.Fprintf(os.Stderr, "missing argument %q in %v\n", value, args)
			os.Exit(9)
		}
	}
	for _, arg := range []string{
		"--print", "--output-format", "stream-json", "--input-format", "text", "--verbose",
		"--restricted", "--no-chrome", "--disable-slash-commands",
	} {
		requireArg(arg)
	}
	writeMode := strings.HasPrefix(mode, "write")
	if writeMode {
		for _, arg := range []string{
			"--permission-mode", "manual", "--tools", writeTools,
			"--allowedTools", permissionToolName, "--mcp-config", "--strict-mcp-config",
			"--permission-prompt-tool", "--prompt-suggestions", "false",
		} {
			requireArg(arg)
		}
		if os.Getenv("INDEXQUBE_TEST_CLAUDE_GUARD") != "attached" {
			fmt.Fprintln(os.Stderr, "missing write guard")
			os.Exit(14)
		}
		if slices.Contains(args, "--safe-mode") {
			fmt.Fprintln(os.Stderr, "safe mode disables the explicit permission MCP server")
			os.Exit(14)
		}
	} else {
		for _, arg := range []string{"--safe-mode", "--permission-mode", "dontAsk", "--tools", readOnlyTools, "--prompt-suggestions", "false"} {
			requireArg(arg)
		}
	}
	for _, forbidden := range []string{"--dangerously-skip-permissions", "bypassPermissions"} {
		if slices.Contains(args, forbidden) {
			fmt.Fprintf(os.Stderr, "unsafe argument %q in %v\n", forbidden, args)
			os.Exit(10)
		}
	}
	if !writeMode {
		for _, forbidden := range []string{"Bash", "Edit", "Write"} {
			if slices.Contains(args, forbidden) {
				fmt.Fprintf(os.Stderr, "unsafe read-only argument %q in %v\n", forbidden, args)
				os.Exit(10)
			}
		}
	}
	if !strings.Contains(string(prompt), "fixture prompt") {
		fmt.Fprintln(os.Stderr, "missing stdin prompt")
		os.Exit(11)
	}
	if mode == "resume" || mode == "resume-lost" {
		requireArg("--resume")
		requireArg(fixtureSessionID)
	} else if slices.Contains(args, "--resume") {
		fmt.Fprintln(os.Stderr, "unexpected resume")
		os.Exit(12)
	}
	if writeMode {
		runClaudeWriteFixture(args)
	}
	fixture := "testdata/read_only_success.jsonl"
	if mode == "resume-lost" {
		fixture = "testdata/resume_lost.jsonl"
	} else if mode == "missing-result" {
		fixture = "testdata/missing_result.jsonl"
	}
	raw, err := os.ReadFile(fixture)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(13)
	}
	_, _ = os.Stdout.Write(raw)
	os.Exit(0)
}

func runClaudeWriteFixture(args []string) {
	configPath := argumentValue(args, "--mcp-config")
	configBytes, err := os.ReadFile(configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(15)
	}
	info, err := os.Stat(configPath)
	if err != nil || info.Mode().Perm() != 0o600 {
		fmt.Fprintln(os.Stderr, "permission config is not mode 0600")
		os.Exit(16)
	}
	var config mcpConfig
	if err := json.Unmarshal(configBytes, &config); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(17)
	}
	server := config.Servers[permissionServerName]
	socketPath := argumentValue(server.Args, "--socket")
	network := argumentValue(server.Args, "--network")
	connection, err := net.Dial(network, socketPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(18)
	}
	defer connection.Close()
	input := json.RawMessage(`{"file_path":"claude-write.txt","content":"approved by Claude fixture\n","description":"fixture file change"}`)
	if err := json.NewEncoder(connection).Encode(permissionRequest{ToolName: "Write", Input: input}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(19)
	}
	scanner := bufio.NewScanner(connection)
	if !scanner.Scan() {
		fmt.Fprintln(os.Stderr, "permission bridge closed without a decision")
		os.Exit(20)
	}
	var decision permissionResponse
	if err := json.Unmarshal(scanner.Bytes(), &decision); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(21)
	}
	approved := decision.Behavior == "allow"
	if approved {
		if err := os.WriteFile("claude-write.txt", []byte("approved by Claude fixture\n"), 0o600); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(22)
		}
	}
	encoder := json.NewEncoder(os.Stdout)
	_ = encoder.Encode(map[string]any{"type": "system", "subtype": "init", "session_id": fixtureSessionID})
	_ = encoder.Encode(map[string]any{"type": "assistant", "message": map[string]any{"content": []any{map[string]any{
		"type": "tool_use", "id": "write-1", "name": "Write", "input": map[string]any{"file_path": "claude-write.txt"},
	}}}})
	_ = encoder.Encode(map[string]any{"type": "user", "message": map[string]any{"content": []any{map[string]any{
		"type": "tool_result", "tool_use_id": "write-1", "is_error": !approved,
		"content": map[bool]string{true: "wrote file", false: "permission denied"}[approved],
	}}}})
	_ = encoder.Encode(map[string]any{"type": "result", "subtype": "success", "session_id": fixtureSessionID, "result": "fixture write turn complete"})
	os.Exit(0)
}

func argumentValue(args []string, flag string) string {
	for index := 0; index+1 < len(args); index++ {
		if args[index] == flag {
			return args[index+1]
		}
	}
	return ""
}

func TestDecodeDocumentedStreamJSONFixture(t *testing.T) {
	raw, err := os.Open("testdata/read_only_success.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	result := agent.Result{}
	decoder := newStreamDecoder(agent.Request{}, &result)
	var events []agent.Event
	scanner := bufio.NewScanner(raw)
	for scanner.Scan() {
		decoded, err := decoder.DecodeEvents(scanner.Bytes())
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, decoded...)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	want := []agent.EventType{
		agent.EventSessionStarted, agent.EventAssistantMessage, agent.EventToolStarted,
		agent.EventToolFinished, agent.EventAssistantMessage, agent.EventCompleted,
	}
	got := make([]agent.EventType, 0, len(events))
	for _, event := range events {
		got = append(got, event.Type)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("event types=%v want=%v", got, want)
	}
	if events[2].Tool.Name != "Read" || events[3].Tool.Name != "Read" || result.NativeSessionID != fixtureSessionID || result.FinalMessage != "fixture Claude answer" {
		t.Fatalf("events=%+v result=%+v", events, result)
	}
}

func TestDecoderEmitsCanonicalCommandEvidence(t *testing.T) {
	result := agent.Result{}
	decoder := newStreamDecoder(agent.Request{TaskID: "task", TurnID: "turn", Permission: agent.PermissionWrite}, &result)
	started, err := decoder.DecodeEvents([]byte(`{"type":"assistant","message":{"content":[{"type":"tool_use","id":"bash-1","name":"Bash","input":{"command":"go test ./..."}}]}}`))
	if err != nil {
		t.Fatal(err)
	}
	finished, err := decoder.DecodeEvents([]byte(`{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"bash-1","content":"ok"}]}}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(started) != 1 || started[0].Command == nil || started[0].Command.Command != "go test ./..." {
		t.Fatalf("started=%+v", started)
	}
	if len(finished) != 2 || finished[1].Type != agent.EventCommandFinished || finished[1].Command.AggregatedOutput != "ok" {
		t.Fatalf("finished=%+v", finished)
	}
	if !result.MutationSeen {
		t.Fatal("write-capable shell command did not mark mutation risk")
	}
}

func TestBackendExecutesInitialAndResumeReadOnly(t *testing.T) {
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name      string
		mode      string
		nativeID  string
		wantLost  bool
		wantError bool
	}{
		{name: "initial", mode: "initial"},
		{name: "resume", mode: "resume", nativeID: fixtureSessionID},
		{name: "resume lost", mode: "resume-lost", nativeID: fixtureSessionID, wantLost: true, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := NewCommand(agent.NewRunner(), binary,
				[]string{"-test.run=TestClaudeProcessHelper", "--"},
				[]string{"INDEXQUBE_CLAUDE_HELPER=" + tc.mode}, "2.1.test (Claude Code)")
			var events []agent.Event
			result, err := backend.Execute(context.Background(), agent.Request{
				TaskID: "task", TurnID: "turn", Workspace: ".", Prompt: "fixture prompt",
				Permission: agent.PermissionReadOnly, NativeSessionID: tc.nativeID,
			}, agent.EventSinkFunc(func(_ context.Context, event agent.Event) error {
				events = append(events, event)
				return nil
			}))
			if (err != nil) != tc.wantError || result.ResumeLost != tc.wantLost {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			if !tc.wantError && (result.NativeSessionID != fixtureSessionID || result.FinalMessage != "fixture Claude answer" || len(events) != 6) {
				t.Fatalf("result=%+v events=%+v", result, events)
			}
		})
	}
}

type claudeApprovalHandler struct {
	requests  chan agent.ApprovalRequest
	decisions chan agent.ApprovalDecision
	once      sync.Once
}

func (h *claudeApprovalHandler) RequestApproval(ctx context.Context, request agent.ApprovalRequest) (agent.ApprovalDecision, error) {
	h.once.Do(func() { h.requests <- request })
	select {
	case decision := <-h.decisions:
		return decision, nil
	case <-ctx.Done():
		return agent.ApprovalCancel, ctx.Err()
	}
}

type claudeProcessGuard struct{}

func (claudeProcessGuard) PrepareCommand(command *exec.Cmd) error {
	command.Env = append(command.Env, "INDEXQUBE_TEST_CLAUDE_GUARD=attached")
	return nil
}

func TestBackendWritePausesForDurableApproval(t *testing.T) {
	useTCPPermissionEndpoint(t)
	for _, decision := range []agent.ApprovalDecision{agent.ApprovalAccept, agent.ApprovalDecline} {
		t.Run(string(decision), func(t *testing.T) {
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			backend := NewCommand(agent.NewRunner(), binary,
				[]string{"-test.run=TestClaudeProcessHelper", "--"},
				[]string{"INDEXQUBE_CLAUDE_HELPER=write"}, "2.1.test (Claude Code)")
			handler := &claudeApprovalHandler{
				requests: make(chan agent.ApprovalRequest, 1), decisions: make(chan agent.ApprovalDecision, 1),
			}
			workspace := t.TempDir()
			type outcome struct {
				result agent.Result
				events []agent.Event
				err    error
			}
			done := make(chan outcome, 1)
			go func() {
				var events []agent.Event
				result, err := backend.Execute(context.Background(), agent.Request{
					TaskID: "task", TurnID: "turn", Workspace: workspace, Prompt: "fixture prompt",
					Permission: agent.PermissionWrite, Guard: claudeProcessGuard{}, Approvals: handler,
				}, agent.EventSinkFunc(func(_ context.Context, event agent.Event) error {
					events = append(events, event)
					return nil
				}))
				done <- outcome{result: result, events: events, err: err}
			}()
			var request agent.ApprovalRequest
			select {
			case request = <-handler.requests:
			case <-time.After(3 * time.Second):
				t.Fatal("Claude did not request approval")
			}
			canonicalWorkspace, err := filepath.EvalSymlinks(workspace)
			if err != nil {
				t.Fatal(err)
			}
			wantPath := filepath.Join(canonicalWorkspace, "claude-write.txt")
			if request.Kind != agent.ApprovalFileChange || request.GrantRoot != wantPath || request.BackendRequestID == "" {
				t.Fatalf("approval request=%+v", request)
			}
			select {
			case early := <-done:
				t.Fatalf("Claude completed before approval: %+v", early)
			case <-time.After(30 * time.Millisecond):
			}
			handler.decisions <- decision
			var completed outcome
			select {
			case completed = <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("Claude did not finish after approval decision")
			}
			if completed.err != nil {
				t.Fatal(completed.err)
			}
			_, statErr := os.Stat(wantPath)
			if decision == agent.ApprovalAccept && statErr != nil {
				t.Fatalf("approved file missing: %v", statErr)
			}
			if decision == agent.ApprovalDecline && !os.IsNotExist(statErr) {
				t.Fatalf("denied file exists: %v", statErr)
			}
			fileEvents := 0
			for _, event := range completed.events {
				if event.Type == agent.EventFileChanged {
					fileEvents++
				}
			}
			if fileEvents != map[bool]int{true: 1, false: 0}[decision == agent.ApprovalAccept] || !completed.result.MutationSeen {
				t.Fatalf("result=%+v events=%+v", completed.result, completed.events)
			}
		})
	}
}

func TestBackendWriteCancellationStopsPendingApproval(t *testing.T) {
	useTCPPermissionEndpoint(t)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	backend := NewCommand(agent.NewRunner(), binary,
		[]string{"-test.run=TestClaudeProcessHelper", "--"},
		[]string{"INDEXQUBE_CLAUDE_HELPER=write"}, "2.1.test (Claude Code)")
	handler := &claudeApprovalHandler{
		requests: make(chan agent.ApprovalRequest, 1), decisions: make(chan agent.ApprovalDecision),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := backend.Execute(ctx, agent.Request{
			Workspace: t.TempDir(), Prompt: "fixture prompt", Permission: agent.PermissionWrite,
			Guard: claudeProcessGuard{}, Approvals: handler,
		}, agent.EventSinkFunc(func(context.Context, agent.Event) error { return nil }))
		done <- err
	}()
	select {
	case <-handler.requests:
	case <-time.After(3 * time.Second):
		t.Fatal("Claude did not request approval")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error=%v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Claude did not stop after cancellation")
	}
}

func TestBackendRejectsUnguardedWriteAndUnknownPermissions(t *testing.T) {
	backend := New(agent.NewRunner(), "claude")
	for _, permission := range []agent.PermissionMode{agent.PermissionWrite, agent.PermissionMode("root")} {
		_, err := backend.Execute(context.Background(), agent.Request{Permission: permission}, agent.EventSinkFunc(func(context.Context, agent.Event) error { return nil }))
		if err == nil {
			t.Fatalf("permission %q was accepted", permission)
		}
	}
}

func TestDecoderRejectsMalformedKnownMessagesAndMissingResult(t *testing.T) {
	result := agent.Result{}
	decoder := newStreamDecoder(agent.Request{}, &result)
	if _, err := decoder.DecodeEvents([]byte(`{"type":"system","subtype":"init"}`)); err == nil {
		t.Fatal("accepted init without session_id")
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	backend := NewCommand(agent.NewRunner(), binary,
		[]string{"-test.run=TestClaudeProcessHelper", "--"},
		[]string{"INDEXQUBE_CLAUDE_HELPER=missing-result"}, "2.1.test (Claude Code)")
	_, err = backend.Execute(context.Background(), agent.Request{
		Workspace: ".", Prompt: "fixture prompt", Permission: agent.PermissionReadOnly,
	}, agent.EventSinkFunc(func(context.Context, agent.Event) error { return nil }))
	if err == nil || !strings.Contains(err.Error(), "without a result") {
		t.Fatalf("error=%v", err)
	}
}

func TestDetectedVersion(t *testing.T) {
	if got := detectedVersion("warning\n2.1.252 (Claude Code)\n"); got != "2.1.252 (Claude Code)" {
		t.Fatalf("version=%q", got)
	}
}

func argsAfterSeparator(args []string) []string {
	for index, arg := range args {
		if arg == "--" {
			return args[index+1:]
		}
	}
	return nil
}

func useTCPPermissionEndpoint(t *testing.T) {
	t.Helper()
	previous := openPermissionEndpoint
	openPermissionEndpoint = func(string) (permissionEndpoint, error) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return permissionEndpoint{}, err
		}
		return permissionEndpoint{listener: listener, network: "tcp", address: listener.Addr().String()}, nil
	}
	t.Cleanup(func() { openPermissionEndpoint = previous })
}
