package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
)

const (
	appServerInitializeID = 1
	appServerThreadID     = 2
	appServerTurnID       = 3
)

type appServerEnvelope struct {
	ID     json.RawMessage `json:"id,omitempty"`
	Method string          `json:"method,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *appServerError `json:"error,omitempty"`
}

type appServerError struct {
	Code    int    `json:"code,omitempty"`
	Message string `json:"message"`
}

type appServerItem struct {
	ID               string          `json:"id,omitempty"`
	Type             string          `json:"type"`
	Text             string          `json:"text,omitempty"`
	Command          string          `json:"command,omitempty"`
	CWD              string          `json:"cwd,omitempty"`
	Status           string          `json:"status,omitempty"`
	ExitCode         *int            `json:"exitCode,omitempty"`
	AggregatedOutput string          `json:"aggregatedOutput,omitempty"`
	Changes          json.RawMessage `json:"changes,omitempty"`
}

type appServerApprovalParams struct {
	ApprovalID             string `json:"approvalId,omitempty"`
	ItemID                 string `json:"itemId"`
	ThreadID               string `json:"threadId"`
	TurnID                 string `json:"turnId"`
	Reason                 string `json:"reason,omitempty"`
	Command                string `json:"command,omitempty"`
	CWD                    string `json:"cwd,omitempty"`
	GrantRoot              string `json:"grantRoot,omitempty"`
	NetworkApprovalContext *struct {
		Host     string `json:"host"`
		Protocol string `json:"protocol"`
	} `json:"networkApprovalContext,omitempty"`
}

func (b *Backend) executeAppServer(ctx context.Context, req agent.Request, sink agent.EventSink) (agent.Result, error) {
	if req.Approvals == nil {
		return agent.Result{}, fmt.Errorf("codex backend: App Server execution requires an approval handler")
	}
	result := agent.Result{}
	initial := make([][]byte, 0, 3)
	for _, message := range []any{
		map[string]any{"method": "initialize", "id": appServerInitializeID, "params": map[string]any{
			"clientInfo": map[string]string{"name": "indexqube", "title": "IndexQube", "version": "0.1.0"},
		}},
		map[string]any{"method": "initialized", "params": map[string]any{}},
		b.appServerThreadRequest(req),
	} {
		raw, err := json.Marshal(message)
		if err != nil {
			return agent.Result{}, err
		}
		initial = append(initial, raw)
	}
	args := append(append([]string(nil), b.prefixArgs...), "app-server", "--listen", "stdio://")
	processResult, runErr := b.runner.RunInteractive(ctx, agent.ProcessSpec{
		Path: b.binary, Args: args, Dir: req.Workspace, Env: b.env, TaskID: req.TaskID, TurnID: req.TurnID,
	}, req.Guard, initial, func(lineCtx context.Context, line []byte, send func([]byte) error) (bool, error) {
		var message appServerEnvelope
		if err := json.Unmarshal(line, &message); err != nil {
			return false, fmt.Errorf("codex backend: decode App Server JSONL: %w", err)
		}
		if message.Error != nil {
			if req.NativeSessionID != "" && isResumeLost(message.Error.Message) {
				result.ResumeLost = true
			}
			return false, fmt.Errorf("codex backend: App Server error %d: %s", message.Error.Code, bounded(message.Error.Message, 1024))
		}
		if len(message.ID) > 0 && message.Method == "" {
			id := rpcNumericID(message.ID)
			if id != appServerThreadID {
				return false, nil
			}
			var response struct {
				Thread struct {
					ID string `json:"id"`
				} `json:"thread"`
			}
			if err := json.Unmarshal(message.Result, &response); err != nil || response.Thread.ID == "" {
				return false, fmt.Errorf("codex backend: invalid thread start response")
			}
			result.NativeSessionID = response.Thread.ID
			if err := sink.Publish(lineCtx, agent.Event{
				Type: agent.EventSessionStarted, Metadata: map[string]string{"native_session_id": response.Thread.ID},
			}); err != nil {
				return false, err
			}
			turnStart, err := json.Marshal(map[string]any{
				"method": "turn/start", "id": appServerTurnID, "params": map[string]any{
					"threadId": response.Thread.ID,
					"input":    []map[string]string{{"type": "text", "text": req.Prompt}},
					"cwd":      req.Workspace, "approvalPolicy": "on-request",
					"sandboxPolicy": map[string]any{
						"type": "workspaceWrite", "writableRoots": []string{req.Workspace}, "networkAccess": false,
					},
				},
			})
			if err != nil {
				return false, err
			}
			return false, send(turnStart)
		}
		switch message.Method {
		case "item/commandExecution/requestApproval", "item/fileChange/requestApproval":
			return false, b.handleAppServerApproval(lineCtx, req, message, send)
		case "item/started", "item/completed":
			event, final, ok, err := appServerItemEvent(message.Method, message.Params, req.Workspace)
			if err != nil {
				return false, err
			}
			if final != "" {
				result.FinalMessage = final
			}
			if ok {
				if event.File != nil {
					result.MutationSeen = true
				}
				if err := sink.Publish(lineCtx, event); err != nil {
					return false, err
				}
			}
		case "turn/completed":
			status, failure, err := appServerTurnCompletion(message.Params)
			if err != nil {
				return false, err
			}
			switch status {
			case "completed":
				return true, nil
			case "interrupted":
				return true, fmt.Errorf("codex backend: turn interrupted")
			default:
				if failure == "" {
					failure = "Codex turn failed"
				}
				if req.NativeSessionID != "" && isResumeLost(failure) {
					result.ResumeLost = true
				}
				return true, errors.New(bounded(failure, 1024))
			}
		case "error":
			// A terminal turn/completed notification remains authoritative. Keep
			// reading so its status and error become the canonical result.
			return false, nil
		}
		return false, nil
	})
	result.ExitCode = processResult.ExitCode
	return result, runErr
}

func (b *Backend) appServerThreadRequest(req agent.Request) map[string]any {
	params := map[string]any{
		"cwd": req.Workspace, "approvalPolicy": "on-request", "sandbox": "workspace-write", "serviceName": "indexqube",
	}
	method := "thread/start"
	if req.NativeSessionID != "" {
		method = "thread/resume"
		params["threadId"] = req.NativeSessionID
	}
	return map[string]any{"method": method, "id": appServerThreadID, "params": params}
}

func (b *Backend) handleAppServerApproval(ctx context.Context, req agent.Request, message appServerEnvelope, send func([]byte) error) error {
	if len(message.ID) == 0 {
		return fmt.Errorf("codex backend: approval request missing JSON-RPC id")
	}
	var params appServerApprovalParams
	if err := json.Unmarshal(message.Params, &params); err != nil {
		return fmt.Errorf("codex backend: decode approval request: %w", err)
	}
	kind := agent.ApprovalCommand
	if message.Method == "item/fileChange/requestApproval" {
		kind = agent.ApprovalFileChange
	}
	requestID := string(message.ID)
	if params.ApprovalID != "" {
		requestID += ":" + params.ApprovalID
	}
	request := agent.ApprovalRequest{
		BackendRequestID: requestID, Kind: kind, ItemID: params.ItemID, NativeThreadID: params.ThreadID,
		NativeTurnID: params.TurnID, Reason: params.Reason, Command: params.Command, CWD: params.CWD, GrantRoot: params.GrantRoot,
	}
	if params.NetworkApprovalContext != nil {
		request.NetworkHost = params.NetworkApprovalContext.Host
		request.NetworkProtocol = params.NetworkApprovalContext.Protocol
	}
	decision, approvalErr := req.Approvals.RequestApproval(ctx, request)
	if decision == "" {
		decision = agent.ApprovalCancel
	}
	response, err := json.Marshal(struct {
		ID     json.RawMessage `json:"id"`
		Result map[string]any  `json:"result"`
	}{ID: message.ID, Result: map[string]any{"decision": decision}})
	if err != nil {
		return err
	}
	if err := send(response); err != nil {
		return err
	}
	return approvalErr
}

func appServerItemEvent(method string, raw json.RawMessage, workspace string) (agent.Event, string, bool, error) {
	var params struct {
		ThreadID string        `json:"threadId"`
		TurnID   string        `json:"turnId"`
		Item     appServerItem `json:"item"`
	}
	if err := json.Unmarshal(raw, &params); err != nil {
		return agent.Event{}, "", false, fmt.Errorf("codex backend: decode item notification: %w", err)
	}
	completed := method == "item/completed"
	metadata := map[string]string{"native_event_id": params.Item.ID, "native_turn_id": params.TurnID}
	switch params.Item.Type {
	case "agentMessage":
		if !completed {
			return agent.Event{}, "", false, nil
		}
		text := bounded(params.Item.Text, 256<<10)
		return agent.Event{Type: agent.EventAssistantMessage, Message: &agent.MessageEvent{Text: text}, Metadata: metadata}, text, true, nil
	case "commandExecution":
		typ := agent.EventToolStarted
		if completed {
			typ = agent.EventCommandFinished
		}
		event := agent.Event{Type: typ, Tool: &agent.ToolEvent{Name: "command", Status: params.Item.Status}, Metadata: metadata,
			Command: &agent.CommandEvent{Command: bounded(params.Item.Command, 4096), Status: bounded(params.Item.Status, 128),
				ExitCode: params.Item.ExitCode, AggregatedOutput: bounded(params.Item.AggregatedOutput, 16<<10)}}
		return event, "", true, nil
	case "fileChange":
		if !completed {
			return agent.Event{}, "", false, nil
		}
		changes := appServerChangedPaths(params.Item.Changes)
		path := ""
		if len(changes) > 0 {
			path = changes[0].Path
		}
		event := agent.Event{Type: agent.EventFileChanged,
			File: &agent.FileEvent{Path: path, Operation: "changed", Changes: changes}, Metadata: metadata}
		normalizeFileEvent(workspace, event.File)
		return event, "", true, nil
	default:
		return agent.Event{}, "", false, nil
	}
}

func appServerChangedPaths(raw json.RawMessage) []agent.FileChange {
	var changes []struct {
		Path string `json:"path"`
		Kind struct {
			Type     string `json:"type"`
			MovePath string `json:"move_path,omitempty"`
		} `json:"kind"`
	}
	if json.Unmarshal(raw, &changes) != nil {
		return nil
	}
	result := make([]agent.FileChange, 0, len(changes))
	for _, change := range changes {
		operation := change.Kind.Type
		if operation == "update" && change.Kind.MovePath != "" {
			operation = "move"
		}
		result = append(result, agent.FileChange{Path: change.Path, Operation: operation})
	}
	return result
}

func appServerTurnCompletion(raw json.RawMessage) (string, string, error) {
	var params struct {
		Turn struct {
			Status string `json:"status"`
			Error  any    `json:"error"`
		} `json:"turn"`
	}
	if err := json.Unmarshal(raw, &params); err != nil {
		return "", "", fmt.Errorf("codex backend: decode turn completion: %w", err)
	}
	failure := ""
	if params.Turn.Error != nil {
		rawError, _ := json.Marshal(params.Turn.Error)
		var object struct {
			Message string `json:"message"`
		}
		if json.Unmarshal(rawError, &object) == nil && object.Message != "" {
			failure = object.Message
		} else {
			failure = strings.TrimSpace(string(rawError))
		}
	}
	return params.Turn.Status, failure, nil
}

func rpcNumericID(raw json.RawMessage) int {
	var id int
	_ = json.Unmarshal(raw, &id)
	return id
}
