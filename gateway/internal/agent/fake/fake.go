// Package fake provides a deterministic child-process backend used to prove
// the orchestration invariants before any real coding agent is integrated.
package fake

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
)

type Backend struct {
	runner *agent.Runner
	binary string
	args   []string
	env    []string
}

func New(runner *agent.Runner, binary string) *Backend {
	return &Backend{runner: runner, binary: binary, args: []string{"__fake-agent"}}
}

// NewCommand is used by tests to run the helper through the Go test binary.
func NewCommand(runner *agent.Runner, binary string, args []string, env []string) *Backend {
	return &Backend{runner: runner, binary: binary, args: append([]string(nil), args...), env: append([]string(nil), env...)}
}

func (b *Backend) ID() agent.BackendID { return agent.BackendFake }

func (b *Backend) Probe(context.Context) agent.BackendHealth {
	status := agent.HealthAvailable
	reason := ""
	if b.runner == nil || b.binary == "" {
		status = agent.HealthUnavailable
		reason = "fake backend is not configured"
	}
	return agent.BackendHealth{Backend: b.ID(), Status: status, Version: "v1", Reason: reason, CheckedAt: time.Now().UTC()}
}

type helperRequest struct {
	TaskID          string               `json:"task_id"`
	TurnID          string               `json:"turn_id"`
	Workspace       string               `json:"workspace"`
	Prompt          string               `json:"prompt"`
	Permission      agent.PermissionMode `json:"permission"`
	NativeSessionID string               `json:"native_session_id,omitempty"`
	WriteEpoch      uint64               `json:"write_epoch,omitempty"`
}

type wireEvent struct {
	Type            agent.EventType   `json:"type"`
	Text            string            `json:"text,omitempty"`
	Path            string            `json:"path,omitempty"`
	Operation       string            `json:"operation,omitempty"`
	Status          string            `json:"status,omitempty"`
	Error           string            `json:"error,omitempty"`
	ExitCode        int               `json:"exit_code,omitempty"`
	NativeSessionID string            `json:"native_session_id,omitempty"`
	WriteEpoch      uint64            `json:"write_epoch,omitempty"`
	Metadata        map[string]string `json:"metadata,omitempty"`
}

func (b *Backend) Execute(ctx context.Context, req agent.Request, sink agent.EventSink) (agent.Result, error) {
	if b.runner == nil || b.binary == "" {
		return agent.Result{}, fmt.Errorf("fake backend is not configured")
	}
	raw, err := json.Marshal(helperRequest{
		TaskID: req.TaskID, TurnID: req.TurnID, Workspace: req.Workspace, Prompt: req.Prompt,
		Permission: req.Permission, NativeSessionID: req.NativeSessionID, WriteEpoch: req.WriteEpoch,
	})
	if err != nil {
		return agent.Result{}, err
	}
	result := agent.Result{}
	decoder := agent.EventDecoderFunc(func(line []byte) (agent.Event, bool, error) {
		var wire wireEvent
		if err := json.Unmarshal(line, &wire); err != nil {
			return agent.Event{}, false, err
		}
		event := agent.Event{
			Type: wire.Type, TaskID: req.TaskID, TurnID: req.TurnID, Backend: agent.BackendFake,
			Timestamp: time.Now().UTC(), Metadata: agent.NormalizeMetadata(wire.Metadata),
		}
		if wire.NativeSessionID != "" {
			if event.Metadata == nil {
				event.Metadata = make(map[string]string)
			}
			event.Metadata["native_session_id"] = wire.NativeSessionID
			result.NativeSessionID = wire.NativeSessionID
		}
		if wire.WriteEpoch > 0 {
			if event.Metadata == nil {
				event.Metadata = make(map[string]string)
			}
			event.Metadata["write_epoch"] = strconv.FormatUint(wire.WriteEpoch, 10)
		}
		switch wire.Type {
		case agent.EventAssistantDelta, agent.EventAssistantMessage:
			event.Message = &agent.MessageEvent{Text: wire.Text}
			if wire.Type == agent.EventAssistantMessage {
				result.FinalMessage = wire.Text
			}
		case agent.EventFileChanged:
			event.File = &agent.FileEvent{Path: wire.Path, Operation: wire.Operation}
			result.MutationSeen = true
		case agent.EventCompleted, agent.EventError, agent.EventCancelled:
			event.Result = &agent.ResultEvent{ExitCode: wire.ExitCode, Status: wire.Status, Error: wire.Error}
			if wire.Metadata["error_code"] == "resume_lost" {
				result.ResumeLost = true
			}
		}
		return event, true, nil
	})
	processResult, runErr := b.runner.Run(ctx, agent.ProcessSpec{
		Path: b.binary, Args: b.args, Dir: req.Workspace, Env: b.env, Stdin: append(raw, '\n'),
		TaskID: req.TaskID, TurnID: req.TurnID,
	}, req.Guard, decoder, sink)
	result.ExitCode = processResult.ExitCode
	return result, runErr
}

// RunHelper is the hidden child-process entrypoint used by the iq binary.
func RunHelper(in io.Reader, out io.Writer, errOut io.Writer) int {
	var req helperRequest
	if err := json.NewDecoder(bufio.NewReader(in)).Decode(&req); err != nil {
		fmt.Fprintf(errOut, "decode fake request: %v\n", err)
		return 2
	}
	nativeID := req.NativeSessionID
	if nativeID == "" {
		nativeID = "fake_native_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	emit := func(event wireEvent) {
		_ = json.NewEncoder(out).Encode(event)
	}
	emit(wireEvent{Type: agent.EventSessionStarted, NativeSessionID: nativeID, WriteEpoch: req.WriteEpoch})

	if strings.Contains(req.Prompt, "[fake:resume-lost]") && req.NativeSessionID != "" {
		emit(wireEvent{Type: agent.EventError, Error: "native session unavailable", ExitCode: 42, Metadata: map[string]string{"error_code": "resume_lost"}})
		return 42
	}
	if strings.Contains(req.Prompt, "[fake:sleep]") {
		time.Sleep(30 * time.Second)
	}
	if strings.Contains(req.Prompt, "[fake:mutate]") {
		if req.Permission != agent.PermissionWrite {
			emit(wireEvent{Type: agent.EventError, Error: "write permission required", ExitCode: 4})
			return 4
		}
		path := filepath.Join(req.Workspace, ".indexqube-fake-change")
		if err := os.WriteFile(path, []byte("fake mutation\n"), 0o600); err != nil {
			emit(wireEvent{Type: agent.EventError, Error: err.Error(), ExitCode: 5})
			return 5
		}
		epoch := req.WriteEpoch
		if strings.Contains(req.Prompt, "[fake:stale]") && epoch > 0 {
			epoch += 1000
		}
		emit(wireEvent{Type: agent.EventFileChanged, Path: ".indexqube-fake-change", Operation: "created", WriteEpoch: epoch})
	}
	if strings.Contains(req.Prompt, "[fake:fail]") {
		emit(wireEvent{Type: agent.EventError, Error: "requested fake failure", ExitCode: 3})
		return 3
	}
	message := "fake: " + strings.TrimSpace(req.Prompt)
	emit(wireEvent{Type: agent.EventAssistantMessage, Text: message})
	emit(wireEvent{Type: agent.EventCompleted, Status: "succeeded", ExitCode: 0})
	return 0
}

var _ agent.Backend = (*Backend)(nil)

var ErrResumeLost = errors.New("fake native session lost")
