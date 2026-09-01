package codex

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
)

func TestCodexProcessHelper(t *testing.T) {
	mode := os.Getenv("INDEXQUBE_CODEX_HELPER")
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
	requireArg("--json")
	writeMode := mode == "write"
	if writeMode {
		requireArg(`sandbox_mode="workspace-write"`)
		requireArg("--approve-for-me")
		if os.Getenv("INDEXQUBE_TEST_WRITE_GUARD") != "attached" {
			fmt.Fprintln(os.Stderr, "missing write guard environment")
			os.Exit(11)
		}
	} else {
		requireArg(`sandbox_mode="read-only"`)
		requireArg(`approval_policy="never"`)
	}
	if !strings.Contains(string(prompt), "fixture prompt") {
		fmt.Fprintln(os.Stderr, "missing stdin prompt")
		os.Exit(10)
	}
	isResume := slices.Contains(args, "resume")
	if !isResume {
		if writeMode {
			if slices.Contains(args, "--sandbox") {
				fmt.Fprintln(os.Stderr, "--approve-for-me cannot be combined with --sandbox")
				os.Exit(13)
			}
		} else {
			requireArg("--sandbox")
			requireArg("read-only")
		}
		requireArg("-C")
	} else {
		requireArg("resume")
		requireArg("codex-session-1")
	}
	enc := json.NewEncoder(os.Stdout)
	if mode == "resume-lost" && isResume {
		_ = enc.Encode(map[string]any{"type": "error", "message": "session not found"})
		os.Exit(3)
	}
	sessionID := "codex-session-1"
	if strings.Contains(string(prompt), "INDEXQUBE CANONICAL SESSION RECOVERY") {
		sessionID = "codex-session-2"
	}
	_ = enc.Encode(map[string]any{"type": "thread.started", "thread_id": sessionID})
	_ = enc.Encode(map[string]any{"type": "item.started", "item": map[string]any{
		"id": "cmd-1", "type": "command_execution", "command": "git status", "status": "in_progress",
	}})
	_ = enc.Encode(map[string]any{"type": "item.completed", "item": map[string]any{
		"id": "cmd-1", "type": "command_execution", "command": "git status", "status": "completed",
		"exit_code": 0, "aggregated_output": "clean",
	}})
	if writeMode {
		if err := os.WriteFile(filepath.Join(".", "codex-write.txt"), []byte("written by codex fixture\n"), 0o600); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(12)
		}
		_ = enc.Encode(map[string]any{"type": "item.completed", "item": map[string]any{
			"id": "file-1", "type": "file_change", "changes": []map[string]any{{"path": "codex-write.txt", "kind": "add"}},
		}})
	}
	_ = enc.Encode(map[string]any{"type": "item.completed", "item": map[string]any{
		"id": "msg-1", "type": "agent_message", "text": "fixture answer",
	}})
	_ = enc.Encode(map[string]any{"type": "turn.completed", "usage": map[string]int{"input_tokens": 1}})
	os.Exit(0)
}

func TestDecodeDocumentedJSONLShape(t *testing.T) {
	lines := []string{
		`{"type":"thread.started","thread_id":"0199a213"}`,
		`{"type":"turn.started"}`,
		`{"type":"item.started","item":{"id":"item_1","type":"command_execution","command":"bash -lc ls","status":"in_progress"}}`,
		`{"type":"item.completed","item":{"id":"item_2","type":"command_execution","command":"bash -lc ls","status":"completed","exit_code":0}}`,
		`{"type":"item.started","item":{"id":"item_file","type":"file_change","changes":[{"path":"a.go","kind":"update"}]}}`,
		`{"type":"item.completed","item":{"id":"item_file","type":"file_change","changes":[{"path":"a.go","kind":"update"},{"path":"b.go","kind":"add"}]}}`,
		`{"type":"item.completed","item":{"id":"item_3","type":"agent_message","text":"Repo contains docs."}}`,
		`{"type":"turn.completed","usage":{"input_tokens":10}}`,
	}
	var types []agent.EventType
	var final, session string
	for _, line := range lines {
		event, ok, gotFinal, gotSession, failure, err := decodeEvent([]byte(line))
		if err != nil || failure != "" {
			t.Fatalf("decode err=%v failure=%q", err, failure)
		}
		if ok {
			types = append(types, event.Type)
		}
		if gotFinal != "" {
			final = gotFinal
		}
		if gotSession != "" {
			session = gotSession
		}
	}
	want := []agent.EventType{agent.EventSessionStarted, agent.EventToolStarted, agent.EventCommandFinished, agent.EventFileChanged, agent.EventAssistantMessage, agent.EventCompleted}
	if !slices.Equal(types, want) {
		t.Fatalf("types=%v want=%v", types, want)
	}
	if final != "Repo contains docs." || session != "0199a213" {
		t.Fatalf("final=%q session=%q", final, session)
	}
}

func TestBackendExecutesInitialAndResumeReadOnly(t *testing.T) {
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name       string
		mode       string
		nativeID   string
		permission agent.PermissionMode
		wantLost   bool
		wantError  bool
	}{
		{name: "initial", mode: "initial", permission: agent.PermissionReadOnly},
		{name: "resume", mode: "resume", nativeID: "codex-session-1", permission: agent.PermissionReadOnly},
		{name: "resume lost", mode: "resume-lost", nativeID: "codex-session-1", permission: agent.PermissionReadOnly, wantLost: true, wantError: true},
		{name: "workspace write", mode: "write", permission: agent.PermissionWrite},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := NewCommand(agent.NewRunner(), binary,
				[]string{"-test.run=TestCodexProcessHelper", "--"},
				[]string{"INDEXQUBE_CODEX_HELPER=" + tc.mode}, "codex-cli test")
			var events []agent.Event
			request := agent.Request{
				TaskID: "task", TurnID: "turn", Workspace: t.TempDir(), Prompt: "fixture prompt",
				Permission: tc.permission, NativeSessionID: tc.nativeID,
			}
			if tc.permission == agent.PermissionWrite {
				request.Guard = testProcessGuard{}
			}
			result, err := backend.Execute(context.Background(), request, agent.EventSinkFunc(func(_ context.Context, event agent.Event) error {
				events = append(events, event)
				return nil
			}))
			if (err != nil) != tc.wantError {
				t.Fatalf("err=%v wantError=%v", err, tc.wantError)
			}
			if result.ResumeLost != tc.wantLost {
				t.Fatalf("result=%+v", result)
			}
			if !tc.wantError && (result.NativeSessionID != "codex-session-1" || result.FinalMessage != "fixture answer") {
				t.Fatalf("result=%+v", result)
			}
			if tc.mode == "initial" && len(events) != 5 {
				t.Fatalf("events=%+v", events)
			}
			if tc.mode == "write" {
				if _, err := os.Stat(filepath.Join(request.Workspace, "codex-write.txt")); err != nil {
					t.Fatalf("write fixture missing: %v", err)
				}
			}
		})
	}
}

func TestBackendRequiresWriteGuardAndRejectsUnknownPermission(t *testing.T) {
	backend := New(agent.NewRunner(), "codex")
	_, err := backend.Execute(context.Background(), agent.Request{Permission: agent.PermissionWrite}, agent.EventSinkFunc(func(context.Context, agent.Event) error { return nil }))
	if err == nil || !strings.Contains(err.Error(), "write guard") {
		t.Fatalf("error=%v", err)
	}
	_, err = backend.Execute(context.Background(), agent.Request{Permission: agent.PermissionMode("root")}, agent.EventSinkFunc(func(context.Context, agent.Event) error { return nil }))
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("error=%v", err)
	}
}

type testProcessGuard struct{}

func (testProcessGuard) PrepareCommand(cmd *exec.Cmd) error {
	cmd.Env = append(cmd.Env, "INDEXQUBE_TEST_WRITE_GUARD=attached")
	return nil
}

func TestDetectedVersionIgnoresCLIWarnings(t *testing.T) {
	got := detectedVersion("WARNING: could not create aliases\ncodex-cli 0.149.1\n")
	if got != "codex-cli 0.149.1" {
		t.Fatalf("version=%q", got)
	}
}

func TestNormalizeFileEventMakesWorkspacePathsRelative(t *testing.T) {
	root := t.TempDir()
	event := &agent.FileEvent{
		Path: filepath.Join(root, "internal", "client.go"),
		Changes: []agent.FileChange{
			{Path: filepath.Join(root, "internal", "client.go"), Operation: "update"},
			{Path: filepath.Join(root, "internal", "client_test.go"), Operation: "add"},
		},
	}
	normalizeFileEvent(root, event)
	if event.Path != "internal/client.go" || event.Changes[1].Path != "internal/client_test.go" {
		t.Fatalf("event=%+v", event)
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
