// Package claude adapts Claude Code's documented stream-json protocol to
// IndexQube's normalized backend contract.
package claude

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
)

const readOnlyTools = "Read,Glob,Grep"

var supportedCLIVersions = agent.CLIVersionPolicy{
	Product:      "Claude Code",
	MinInclusive: agent.SemanticVersion{Major: 2, Minor: 1, Patch: 0},
	MaxExclusive: agent.SemanticVersion{Major: 2, Minor: 2, Patch: 0},
}

type Backend struct {
	runner     *agent.Runner
	binary     string
	prefixArgs []string
	env        []string
	mu         sync.Mutex
	version    string
}

func New(runner *agent.Runner, binary string) *Backend {
	return &Backend{runner: runner, binary: binary, env: []string{"DISABLE_AUTOUPDATER=1"}}
}

// NewCommand lets protocol tests substitute a deterministic process while
// exercising the production argument builder, supervisor, and decoder.
func NewCommand(runner *agent.Runner, binary string, prefixArgs, env []string, version string) *Backend {
	return &Backend{
		runner: runner, binary: binary, prefixArgs: append([]string(nil), prefixArgs...),
		env: append([]string{"DISABLE_AUTOUPDATER=1"}, env...), version: version,
	}
}

func (b *Backend) ID() agent.BackendID { return agent.BackendClaude }

func (b *Backend) ValidatePermission(permission agent.PermissionMode) error {
	if permission == agent.PermissionReadOnly || permission == agent.PermissionWrite {
		return nil
	}
	return fmt.Errorf("claude backend: unsupported permission %q", permission)
}

func (b *Backend) Probe(ctx context.Context) agent.BackendHealth {
	b.mu.Lock()
	defer b.mu.Unlock()
	health := agent.BackendHealth{Backend: b.ID(), CheckedAt: time.Now().UTC()}
	if b.runner == nil || strings.TrimSpace(b.binary) == "" {
		health.Status = agent.HealthUnavailable
		health.Reason = "claude executable not found"
		return health
	}
	if b.version != "" {
		health.Version = b.version
		if _, err := supportedCLIVersions.Check(b.version); err != nil {
			health.Status = agent.HealthIncompatible
			health.Reason = err.Error()
			return health
		}
		health.Status = agent.HealthAvailable
		return health
	}
	probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(probeCtx, b.binary, "--version") //nolint:gosec -- executable is resolved by the daemon
	cmd.Env = append(os.Environ(), "DISABLE_AUTOUPDATER=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		health.Status = agent.HealthUnavailable
		health.Reason = bounded(strings.TrimSpace(string(out)), 256)
		if health.Reason == "" {
			health.Reason = err.Error()
		}
		return health
	}
	b.version = detectedVersion(string(out))
	health.Version = b.version
	if _, err := supportedCLIVersions.Check(b.version); err != nil {
		health.Status = agent.HealthIncompatible
		health.Reason = err.Error()
		return health
	}
	health.Status = agent.HealthAvailable
	return health
}

func (b *Backend) Execute(ctx context.Context, req agent.Request, sink agent.EventSink) (agent.Result, error) {
	if err := b.ValidatePermission(req.Permission); err != nil {
		return agent.Result{}, err
	}
	if b.runner == nil || strings.TrimSpace(b.binary) == "" {
		return agent.Result{}, errors.New("claude backend: executable is not configured")
	}
	if health := b.Probe(ctx); health.Status != agent.HealthAvailable {
		return agent.Result{}, fmt.Errorf("claude backend: %s", health.Reason)
	}
	var bridge *permissionBridge
	if req.Permission == agent.PermissionWrite {
		if req.Guard == nil {
			return agent.Result{}, errors.New("claude backend: workspace-write requires a process guard")
		}
		var err error
		bridge, err = startPermissionBridge(ctx, req.Approvals, req.Workspace)
		if err != nil {
			return agent.Result{}, err
		}
		defer bridge.Close()
	}
	result := agent.Result{}
	decoder := newStreamDecoder(req, &result)
	configPath := ""
	if bridge != nil {
		configPath = bridge.configPath
	}
	processResult, runErr := b.runner.Run(ctx, agent.ProcessSpec{
		Path: b.binary, Args: append(append([]string(nil), b.prefixArgs...), b.commandArgs(req, configPath)...),
		Dir: req.Workspace, Env: b.env, Stdin: []byte(req.Prompt + "\n"),
	}, req.Guard, decoder, sink)
	result.ExitCode = processResult.ExitCode
	if runErr != nil && req.NativeSessionID != "" && isResumeLost(runErr.Error()+" "+processResult.Stderr) {
		result.ResumeLost = true
	}
	if runErr == nil && decoder.failure != "" {
		runErr = errors.New(decoder.failure)
	}
	if runErr == nil && !decoder.sawResult {
		runErr = errors.New("claude backend: stream ended without a result message")
	}
	return result, runErr
}

func (b *Backend) commandArgs(req agent.Request, permissionConfig string) []string {
	args := []string{
		"--print", "--output-format", "stream-json", "--input-format", "text", "--verbose",
		"--restricted", "--no-chrome", "--disable-slash-commands",
	}
	if req.Permission == agent.PermissionWrite {
		// Claude's safe mode disables even explicitly supplied MCP servers. The
		// restricted + strict-MCP combination ignores workspace/user settings,
		// confines file tools, and preserves only our permission callback.
		args = append(args,
			"--permission-mode", "manual", "--tools", writeTools,
			"--allowedTools", permissionToolName,
			"--mcp-config", permissionConfig, "--strict-mcp-config",
			"--permission-prompt-tool", permissionToolName,
		)
	} else {
		args = append(args, "--safe-mode", "--permission-mode", "dontAsk", "--tools", readOnlyTools)
	}
	args = append(args, "--prompt-suggestions", "false")
	if req.NativeSessionID != "" {
		args = append(args, "--resume", req.NativeSessionID)
	}
	return args
}

type streamEnvelope struct {
	Type      string          `json:"type"`
	Subtype   string          `json:"subtype,omitempty"`
	SessionID string          `json:"session_id,omitempty"`
	IsError   bool            `json:"is_error,omitempty"`
	Result    string          `json:"result,omitempty"`
	Error     json.RawMessage `json:"error,omitempty"`
	Message   json.RawMessage `json:"message,omitempty"`
}

type streamMessage struct {
	Model   string        `json:"model,omitempty"`
	Content []streamBlock `json:"content"`
}

type streamBlock struct {
	Type      string          `json:"type"`
	ID        string          `json:"id,omitempty"`
	Name      string          `json:"name,omitempty"`
	Text      string          `json:"text,omitempty"`
	ToolUseID string          `json:"tool_use_id,omitempty"`
	IsError   bool            `json:"is_error,omitempty"`
	Input     json.RawMessage `json:"input,omitempty"`
	Content   json.RawMessage `json:"content,omitempty"`
}

type toolInvocation struct {
	name     string
	command  string
	filePath string
}

type streamDecoder struct {
	req       agent.Request
	result    *agent.Result
	tools     map[string]toolInvocation
	sawResult bool
	failure   string
}

func newStreamDecoder(req agent.Request, result *agent.Result) *streamDecoder {
	return &streamDecoder{req: req, result: result, tools: make(map[string]toolInvocation)}
}

// Decode satisfies EventDecoder; Runner uses DecodeEvents because this type
// also implements EventBatchDecoder.
func (d *streamDecoder) Decode(line []byte) (agent.Event, bool, error) {
	events, err := d.DecodeEvents(line)
	if err != nil || len(events) == 0 {
		return agent.Event{}, false, err
	}
	return events[0], true, nil
}

func (d *streamDecoder) DecodeEvents(line []byte) ([]agent.Event, error) {
	var wire streamEnvelope
	if err := json.Unmarshal(line, &wire); err != nil {
		return nil, fmt.Errorf("claude backend: decode stream JSONL: %w", err)
	}
	switch wire.Type {
	case "system":
		if wire.Subtype != "init" {
			return nil, nil
		}
		if strings.TrimSpace(wire.SessionID) == "" {
			return nil, errors.New("claude backend: init message missing session_id")
		}
		d.result.NativeSessionID = wire.SessionID
		return d.stamp([]agent.Event{{
			Type: agent.EventSessionStarted, Metadata: map[string]string{"native_session_id": wire.SessionID},
		}}), nil
	case "assistant", "user":
		events, err := d.decodeMessage(wire.Type, wire.Message)
		return d.stamp(events), err
	case "result":
		d.sawResult = true
		if wire.SessionID != "" {
			d.result.NativeSessionID = wire.SessionID
		}
		if wire.Result != "" {
			d.result.FinalMessage = bounded(wire.Result, 256<<10)
		}
		if wire.IsError || (wire.Subtype != "" && wire.Subtype != "success") {
			d.failure = resultFailure(wire)
			if d.req.NativeSessionID != "" && isResumeLost(d.failure) {
				d.result.ResumeLost = true
			}
			return d.stamp([]agent.Event{{Type: agent.EventError, Result: &agent.ResultEvent{Status: "failed", Error: d.failure}}}), nil
		}
		return d.stamp([]agent.Event{{Type: agent.EventCompleted, Result: &agent.ResultEvent{Status: "succeeded"}}}), nil
	default:
		return nil, nil
	}
}

func (d *streamDecoder) stamp(events []agent.Event) []agent.Event {
	for index := range events {
		events[index].TaskID = d.req.TaskID
		events[index].TurnID = d.req.TurnID
		events[index].Backend = agent.BackendClaude
		events[index].Timestamp = time.Now().UTC()
	}
	return events
}

func (d *streamDecoder) decodeMessage(kind string, raw json.RawMessage) ([]agent.Event, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, fmt.Errorf("claude backend: %s message missing payload", kind)
	}
	var message streamMessage
	if err := json.Unmarshal(raw, &message); err != nil {
		return nil, fmt.Errorf("claude backend: decode %s message: %w", kind, err)
	}
	events := make([]agent.Event, 0, len(message.Content))
	var assistantText strings.Builder
	for _, block := range message.Content {
		metadata := map[string]string{}
		if block.ID != "" {
			metadata["native_event_id"] = bounded(block.ID, 256)
		}
		if message.Model != "" {
			metadata["model"] = bounded(message.Model, 256)
		}
		switch block.Type {
		case "text":
			if kind != "assistant" || block.Text == "" {
				continue
			}
			text := bounded(block.Text, 256<<10)
			assistantText.WriteString(text)
			events = append(events, agent.Event{Type: agent.EventAssistantMessage, Message: &agent.MessageEvent{Text: text}, Metadata: metadata})
		case "tool_use":
			if kind != "assistant" {
				continue
			}
			name := bounded(block.Name, 128)
			invocation := decodeToolInvocation(name, block.Input)
			if block.ID != "" {
				d.tools[block.ID] = invocation
			}
			event := agent.Event{Type: agent.EventToolStarted, Tool: &agent.ToolEvent{Name: name, Status: "started"}, Metadata: metadata}
			if invocation.command != "" {
				event.Command = &agent.CommandEvent{Command: invocation.command, Status: "started"}
			}
			if invocation.command != "" || invocation.filePath != "" {
				d.result.MutationSeen = true
			}
			events = append(events, event)
		case "tool_result":
			if kind != "user" {
				continue
			}
			invocation := d.tools[block.ToolUseID]
			if invocation.name == "" {
				invocation.name = "tool"
			}
			status := "completed"
			if block.IsError {
				status = "failed"
			}
			metadata = map[string]string{}
			if block.ToolUseID != "" {
				metadata["native_event_id"] = bounded(block.ToolUseID, 256)
			}
			events = append(events, agent.Event{Type: agent.EventToolFinished, Tool: &agent.ToolEvent{Name: invocation.name, Status: status}, Metadata: metadata})
			if invocation.command != "" {
				events = append(events, agent.Event{
					Type:     agent.EventCommandFinished,
					Command:  &agent.CommandEvent{Command: invocation.command, Status: status, AggregatedOutput: bounded(toolResultText(block.Content), 64<<10)},
					Metadata: metadata,
				})
			}
			if invocation.filePath != "" && !block.IsError {
				events = append(events, agent.Event{
					Type:     agent.EventFileChanged,
					File:     &agent.FileEvent{Path: d.displayPath(invocation.filePath), Operation: "change"},
					Metadata: metadata,
				})
			}
		}
	}
	if assistantText.Len() > 0 {
		d.result.FinalMessage = assistantText.String()
	}
	return events, nil
}

func decodeToolInvocation(name string, input json.RawMessage) toolInvocation {
	invocation := toolInvocation{name: name}
	var values map[string]any
	if json.Unmarshal(input, &values) != nil {
		return invocation
	}
	if name == "Bash" {
		invocation.command, _ = values["command"].(string)
	}
	if name == "Edit" || name == "Write" || name == "MultiEdit" || name == "NotebookEdit" {
		for _, key := range []string{"file_path", "notebook_path", "path"} {
			if value, ok := values[key].(string); ok && value != "" {
				invocation.filePath = value
				break
			}
		}
	}
	return invocation
}

func toolResultText(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return text
	}
	var blocks []struct {
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &blocks) == nil {
		parts := make([]string, 0, len(blocks))
		for _, block := range blocks {
			if block.Text != "" {
				parts = append(parts, block.Text)
			}
		}
		return strings.Join(parts, "\n")
	}
	return string(raw)
}

func (d *streamDecoder) displayPath(path string) string {
	if !filepath.IsAbs(path) || d.req.Workspace == "" {
		return filepath.Clean(path)
	}
	workspace, err := filepath.Abs(d.req.Workspace)
	if err != nil {
		return filepath.Clean(path)
	}
	relative, err := filepath.Rel(workspace, filepath.Clean(path))
	if err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return relative
	}
	return filepath.Clean(path)
}

func resultFailure(wire streamEnvelope) string {
	if strings.TrimSpace(wire.Result) != "" {
		return bounded(strings.TrimSpace(wire.Result), 1024)
	}
	if len(wire.Error) > 0 && string(wire.Error) != "null" {
		var text string
		if json.Unmarshal(wire.Error, &text) == nil && strings.TrimSpace(text) != "" {
			return bounded(strings.TrimSpace(text), 1024)
		}
		var object struct {
			Message string `json:"message"`
		}
		if json.Unmarshal(wire.Error, &object) == nil && strings.TrimSpace(object.Message) != "" {
			return bounded(strings.TrimSpace(object.Message), 1024)
		}
	}
	if wire.Subtype != "" {
		return "Claude Code result: " + bounded(wire.Subtype, 128)
	}
	return "Claude Code turn failed"
}

func isResumeLost(text string) bool {
	text = strings.ToLower(text)
	for _, pattern := range []string{
		"session not found", "conversation not found", "no conversation found", "could not find session", "session does not exist",
	} {
		if strings.Contains(text, pattern) {
			return true
		}
	}
	return false
}

func detectedVersion(output string) string {
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		line = strings.TrimSpace(line)
		if strings.Contains(line, "Claude Code") {
			return bounded(line, 128)
		}
	}
	return bounded(strings.TrimSpace(output), 128)
}

func bounded(value string, max int) string {
	if len(value) <= max {
		return value
	}
	return value[:max]
}
