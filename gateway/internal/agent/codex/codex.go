// Package codex adapts the Codex CLI JSONL protocol to IndexQube's normalized
// backend contract. V1 deliberately supports read-only execution only.
package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
)

type Backend struct {
	runner     *agent.Runner
	binary     string
	prefixArgs []string
	env        []string
	mu         sync.Mutex
	version    string
}

func New(runner *agent.Runner, binary string) *Backend {
	return &Backend{runner: runner, binary: binary}
}

// NewCommand lets protocol tests substitute a deterministic command while
// exercising the same process supervisor and decoder as the real CLI.
func NewCommand(runner *agent.Runner, binary string, prefixArgs, env []string, version string) *Backend {
	return &Backend{
		runner: runner, binary: binary, prefixArgs: append([]string(nil), prefixArgs...),
		env: append([]string(nil), env...), version: version,
	}
}

func (b *Backend) ID() agent.BackendID { return agent.BackendCodex }

func (b *Backend) ValidatePermission(permission agent.PermissionMode) error {
	if permission != agent.PermissionReadOnly {
		return fmt.Errorf("codex backend: only read-only permission is enabled")
	}
	return nil
}

func (b *Backend) Probe(ctx context.Context) agent.BackendHealth {
	b.mu.Lock()
	defer b.mu.Unlock()
	health := agent.BackendHealth{Backend: b.ID(), CheckedAt: time.Now().UTC()}
	if b.runner == nil || strings.TrimSpace(b.binary) == "" {
		health.Status = agent.HealthUnavailable
		health.Reason = "codex executable not found"
		return health
	}
	if b.version != "" {
		health.Status = agent.HealthAvailable
		health.Version = b.version
		return health
	}
	probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(probeCtx, b.binary, "--version") //nolint:gosec -- executable is resolved by the daemon
	out, err := cmd.CombinedOutput()
	if err != nil {
		health.Status = agent.HealthUnavailable
		health.Reason = bounded(strings.TrimSpace(string(out)), 256)
		if health.Reason == "" {
			health.Reason = err.Error()
		}
		return health
	}
	health.Status = agent.HealthAvailable
	b.version = detectedVersion(string(out))
	health.Version = b.version
	return health
}

func (b *Backend) Execute(ctx context.Context, req agent.Request, sink agent.EventSink) (agent.Result, error) {
	if err := b.ValidatePermission(req.Permission); err != nil {
		return agent.Result{}, err
	}
	if b.runner == nil || b.binary == "" {
		return agent.Result{}, fmt.Errorf("codex backend: executable is not configured")
	}
	args := b.commandArgs(req)
	result := agent.Result{}
	streamFailure := ""
	decoder := agent.EventDecoderFunc(func(line []byte) (agent.Event, bool, error) {
		event, ok, final, sessionID, failure, err := decodeEvent(line)
		if err != nil {
			return agent.Event{}, false, err
		}
		if sessionID != "" {
			result.NativeSessionID = sessionID
		}
		if final != "" {
			result.FinalMessage = final
		}
		if failure != "" {
			streamFailure = failure
			if req.NativeSessionID != "" && isResumeLost(failure) {
				result.ResumeLost = true
			}
		}
		if ok {
			event.TaskID = req.TaskID
			event.TurnID = req.TurnID
			event.Backend = b.ID()
			event.Timestamp = time.Now().UTC()
		}
		return event, ok, nil
	})
	processResult, runErr := b.runner.Run(ctx, agent.ProcessSpec{
		Path: b.binary, Args: append(append([]string(nil), b.prefixArgs...), args...), Dir: req.Workspace,
		Env: b.env, Stdin: []byte(req.Prompt + "\n"),
	}, nil, decoder, sink)
	result.ExitCode = processResult.ExitCode
	if runErr != nil && req.NativeSessionID != "" && isResumeLost(runErr.Error()+" "+processResult.Stderr) {
		result.ResumeLost = true
	}
	if runErr == nil && streamFailure != "" {
		runErr = errors.New(streamFailure)
	}
	return result, runErr
}

func (b *Backend) commandArgs(req agent.Request) []string {
	permissionOverrides := []string{"-c", `sandbox_mode="read-only"`, "-c", `approval_policy="never"`}
	if req.NativeSessionID != "" {
		args := []string{"exec", "resume", "--json"}
		args = append(args, permissionOverrides...)
		return append(args, req.NativeSessionID, "-")
	}
	args := []string{"exec", "--json", "--sandbox", "read-only", "--color", "never", "-C", req.Workspace}
	args = append(args, permissionOverrides...)
	return append(args, "-")
}

type wireEnvelope struct {
	Type     string          `json:"type"`
	ThreadID string          `json:"thread_id,omitempty"`
	Item     json.RawMessage `json:"item,omitempty"`
	Error    json.RawMessage `json:"error,omitempty"`
	Message  string          `json:"message,omitempty"`
}

type wireItem struct {
	ID               string          `json:"id,omitempty"`
	Type             string          `json:"type"`
	Text             string          `json:"text,omitempty"`
	Command          string          `json:"command,omitempty"`
	Status           string          `json:"status,omitempty"`
	ExitCode         *int            `json:"exit_code,omitempty"`
	AggregatedOutput string          `json:"aggregated_output,omitempty"`
	Changes          json.RawMessage `json:"changes,omitempty"`
	Tool             string          `json:"tool,omitempty"`
}

func decodeEvent(line []byte) (agent.Event, bool, string, string, string, error) {
	var wire wireEnvelope
	if err := json.Unmarshal(line, &wire); err != nil {
		return agent.Event{}, false, "", "", "", fmt.Errorf("codex backend: decode JSONL: %w", err)
	}
	switch wire.Type {
	case "thread.started":
		if wire.ThreadID == "" {
			return agent.Event{}, false, "", "", "", fmt.Errorf("codex backend: thread.started missing thread_id")
		}
		return agent.Event{Type: agent.EventSessionStarted, Metadata: map[string]string{"native_session_id": wire.ThreadID}}, true, "", wire.ThreadID, "", nil
	case "item.started", "item.completed":
		var item wireItem
		if err := json.Unmarshal(wire.Item, &item); err != nil {
			return agent.Event{}, false, "", "", "", fmt.Errorf("codex backend: decode item: %w", err)
		}
		metadata := map[string]string{}
		if item.ID != "" {
			metadata["native_event_id"] = item.ID
		}
		started := wire.Type == "item.started"
		switch item.Type {
		case "agent_message":
			if started {
				return agent.Event{}, false, "", "", "", nil
			}
			return agent.Event{Type: agent.EventAssistantMessage, Message: &agent.MessageEvent{Text: item.Text}, Metadata: metadata}, true, item.Text, "", "", nil
		case "command_execution":
			typ := agent.EventToolStarted
			if !started {
				typ = agent.EventCommandFinished
			}
			event := agent.Event{Type: typ, Tool: &agent.ToolEvent{Name: "command", Status: item.Status}, Metadata: metadata}
			if item.ExitCode != nil {
				event.Result = &agent.ResultEvent{ExitCode: *item.ExitCode, Status: item.Status}
			}
			return event, true, "", "", "", nil
		case "file_change":
			return agent.Event{Type: agent.EventFileChanged, File: &agent.FileEvent{Path: firstChangedPath(item.Changes), Operation: "changed"}, Metadata: metadata}, true, "", "", "", nil
		case "mcp_tool_call", "web_search":
			typ := agent.EventToolStarted
			if !started {
				typ = agent.EventToolFinished
			}
			name := item.Tool
			if name == "" {
				name = item.Type
			}
			return agent.Event{Type: typ, Tool: &agent.ToolEvent{Name: bounded(name, 128), Status: item.Status}, Metadata: metadata}, true, "", "", "", nil
		default:
			return agent.Event{}, false, "", "", "", nil
		}
	case "turn.completed":
		return agent.Event{Type: agent.EventCompleted, Result: &agent.ResultEvent{Status: "succeeded"}}, true, "", "", "", nil
	case "turn.failed", "error":
		failure := errorText(wire)
		if failure == "" {
			failure = "Codex turn failed"
		}
		return agent.Event{Type: agent.EventError, Result: &agent.ResultEvent{Status: "failed", Error: failure}}, true, "", "", failure, nil
	default:
		return agent.Event{}, false, "", "", "", nil
	}
}

func errorText(wire wireEnvelope) string {
	if wire.Message != "" {
		return bounded(wire.Message, 1024)
	}
	if len(wire.Error) == 0 || string(wire.Error) == "null" {
		return ""
	}
	var text string
	if json.Unmarshal(wire.Error, &text) == nil {
		return bounded(text, 1024)
	}
	var object struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(wire.Error, &object) == nil && object.Message != "" {
		return bounded(object.Message, 1024)
	}
	return bounded(string(wire.Error), 1024)
}

func firstChangedPath(raw json.RawMessage) string {
	var changes []struct {
		Path string `json:"path"`
	}
	if json.Unmarshal(raw, &changes) == nil && len(changes) > 0 {
		return bounded(changes[0].Path, 1024)
	}
	return ""
}

func isResumeLost(text string) bool {
	text = strings.ToLower(text)
	patterns := []string{"session not found", "thread not found", "conversation not found", "no rollout found", "could not find session", "could not find thread"}
	for _, pattern := range patterns {
		if strings.Contains(text, pattern) {
			return true
		}
	}
	return false
}

func bounded(value string, max int) string {
	if len(value) <= max {
		return value
	}
	return value[:max]
}

func detectedVersion(output string) string {
	lines := strings.Split(strings.TrimSpace(output), "\n")
	for index := len(lines) - 1; index >= 0; index-- {
		line := strings.TrimSpace(lines[index])
		if strings.HasPrefix(line, "codex-cli ") {
			return bounded(line, 128)
		}
	}
	return bounded(strings.TrimSpace(output), 128)
}

var _ agent.Backend = (*Backend)(nil)
