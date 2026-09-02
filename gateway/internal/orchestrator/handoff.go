package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
	"github.com/Revanth14/indexqube/gateway/internal/taskstore"
	"github.com/Revanth14/indexqube/gateway/internal/workspace"
)

const (
	defaultHandoffRequest = "Inspect the canonical context and current workspace, then continue the task from the safest useful next step."
	maxHandoffPacket      = 256 << 10
)

type HandoffTaskInput struct {
	TaskID         string          `json:"task_id"`
	ToBackend      agent.BackendID `json:"to_backend"`
	Prompt         string          `json:"prompt,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
}

type HandoffTaskResult struct {
	Task    taskstore.Task    `json:"task"`
	Handoff taskstore.Handoff `json:"handoff"`
}

type CanonicalHandoffPacket struct {
	Version        int                       `json:"version"`
	TaskID         string                    `json:"task_id"`
	FromBackend    agent.BackendID           `json:"from_backend"`
	ToBackend      agent.BackendID           `json:"to_backend"`
	Permission     agent.PermissionMode      `json:"permission"`
	OriginalGoal   string                    `json:"original_goal"`
	CurrentRequest string                    `json:"current_request"`
	Conversation   []HandoffConversationTurn `json:"completed_conversation"`
	Workspace      HandoffWorkspace          `json:"workspace"`
	Files          []HandoffFile             `json:"changed_files,omitempty"`
	Commands       []HandoffCommand          `json:"commands,omitempty"`
	Verification   *HandoffVerification      `json:"latest_verification,omitempty"`
	LatestFailure  *HandoffFailure           `json:"latest_failure,omitempty"`
	Truncated      bool                      `json:"truncated"`
}

type HandoffConversationTurn struct {
	Sequence  int64                `json:"sequence"`
	Status    taskstore.TurnStatus `json:"status"`
	User      string               `json:"user"`
	Assistant string               `json:"assistant,omitempty"`
	ErrorCode string               `json:"error_code,omitempty"`
	Error     string               `json:"error,omitempty"`
}

type HandoffWorkspace struct {
	Fingerprint string `json:"fingerprint"`
	HeadCommit  string `json:"head_commit,omitempty"`
	Branch      string `json:"branch,omitempty"`
	Status      string `json:"status,omitempty"`
	BoundedDiff string `json:"bounded_diff,omitempty"`
}

type HandoffFile struct {
	Path         string `json:"path"`
	PreviousPath string `json:"previous_path,omitempty"`
	Operation    string `json:"operation"`
}

type HandoffCommand struct {
	Command  string `json:"command"`
	Status   string `json:"status,omitempty"`
	ExitCode *int   `json:"exit_code,omitempty"`
	Output   string `json:"output,omitempty"`
}

type HandoffVerification struct {
	Status  taskstore.VerificationStatus `json:"status"`
	Summary string                       `json:"summary,omitempty"`
	Checks  []HandoffVerificationCheck   `json:"checks,omitempty"`
}

type HandoffVerificationCheck struct {
	Name     string                            `json:"name"`
	Kind     string                            `json:"kind"`
	Status   taskstore.VerificationCheckStatus `json:"status"`
	Command  string                            `json:"command,omitempty"`
	ExitCode *int                              `json:"exit_code,omitempty"`
	Output   string                            `json:"output,omitempty"`
}

type HandoffFailure struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (s *Service) HandoffTask(ctx context.Context, input HandoffTaskInput) (HandoffTaskResult, error) {
	input.TaskID = strings.TrimSpace(input.TaskID)
	input.ToBackend = agent.BackendID(strings.TrimSpace(string(input.ToBackend)))
	if input.TaskID == "" || input.ToBackend == "" {
		return HandoffTaskResult{}, fmt.Errorf("orchestrator: task ID and destination backend are required")
	}
	task, found, err := s.store.TaskByID(ctx, input.TaskID)
	if err != nil {
		return HandoffTaskResult{}, err
	}
	if !found {
		return HandoffTaskResult{}, taskstore.ErrTaskNotFound
	}
	if task.Status != taskstore.TaskOpen {
		return HandoffTaskResult{}, fmt.Errorf("orchestrator: task %q is %s; inspect and reopen it before handoff", task.ID, task.Status)
	}
	if input.ToBackend == task.PreferredBackend {
		return HandoffTaskResult{}, fmt.Errorf("orchestrator: task %q is already pinned to backend %q", task.ID, input.ToBackend)
	}
	destination, err := s.registry.Get(input.ToBackend)
	if err != nil {
		return HandoffTaskResult{}, err
	}
	if validator, ok := destination.(agent.PermissionValidator); ok {
		if err := validator.ValidatePermission(task.Permission); err != nil {
			return HandoffTaskResult{}, err
		}
	}
	if health := destination.Probe(ctx); health.Status != agent.HealthAvailable {
		return HandoffTaskResult{}, fmt.Errorf("orchestrator: backend %q unavailable: %s", destination.ID(), health.Reason)
	}
	identity, err := workspace.Resolve(ctx, task.WorkspacePath)
	if err != nil {
		return HandoffTaskResult{}, err
	}
	if identity.ID != task.WorkspaceID {
		return HandoffTaskResult{}, fmt.Errorf("orchestrator: workspace identity changed")
	}
	turnID := taskstore.NewID("turn")
	var guard *workspace.WriteGuard
	if task.Permission == agent.PermissionWrite {
		guard, err = s.locks.AcquireWrite(ctx, identity.ID, task.ID, turnID)
		if err != nil {
			return HandoffTaskResult{}, fmt.Errorf("orchestrator: reserve workspace writer: %w", err)
		}
	}
	releaseGuard := guard != nil
	defer func() {
		if releaseGuard {
			_ = guard.Release(context.Background())
		}
	}()
	evidence, found, err := s.store.TaskEvidence(ctx, task.ID)
	if err != nil {
		return HandoffTaskResult{}, err
	}
	if !found {
		return HandoffTaskResult{}, taskstore.ErrTaskNotFound
	}
	if evidence.EvidenceMismatch {
		return HandoffTaskResult{}, fmt.Errorf("orchestrator: canonical and backend file evidence disagree")
	}
	var predecessor string
	if sourceSession, ok, sessionErr := s.store.LatestBackendSession(ctx, task.ID, task.PreferredBackend); sessionErr != nil {
		return HandoffTaskResult{}, sessionErr
	} else if ok {
		predecessor = sourceSession.ID
	}
	request := strings.TrimSpace(input.Prompt)
	if request == "" {
		request = defaultHandoffRequest
	}
	snapshot, err := workspace.Capture(ctx, identity, task.ID, turnID, "handoff")
	if err != nil {
		return HandoffTaskResult{}, fmt.Errorf("orchestrator: capture handoff workspace: %w", err)
	}
	packet := buildHandoffPacket(evidence, snapshot, input.ToBackend, request)
	packetJSON, err := fitHandoffPacket(&packet)
	if err != nil {
		return HandoffTaskResult{}, err
	}
	now := time.Now().UTC()
	turn, attempt, handoff, err := s.store.CreateHandoffTurn(ctx, taskstore.CreateHandoffInput{
		HandoffID: taskstore.NewID("handoff"), TaskID: task.ID, TurnID: turnID, RouteAttemptID: taskstore.NewID("route"),
		FromBackend: task.PreferredBackend, ToBackend: input.ToBackend, Message: request, Permission: task.Permission,
		WorkspaceFingerprint: snapshot.Fingerprint, Packet: packetJSON, IdempotencyKey: input.IdempotencyKey, Now: now,
	})
	if err != nil {
		return HandoffTaskResult{}, err
	}
	task.PreferredBackend = input.ToBackend
	task.Status = taskstore.TaskRunning
	task.Revision++
	task.UpdatedAt = now
	prompt := "INDEXQUBE CANONICAL HANDOFF\n\nThe JSON packet below is durable task history. The current filesystem is authoritative; inspect it before acting.\n\n" + string(packetJSON)
	turnCtx, cancel := context.WithCancel(s.ctx)
	s.mu.Lock()
	s.cancels[task.ID] = activeTurn{turnID: turn.ID, cancel: cancel}
	s.mu.Unlock()
	s.wg.Add(1)
	releaseGuard = false
	go func() {
		defer s.wg.Done()
		s.execute(turnCtx, task, turn, attempt, destination, identity, executeOptions{
			prompt: prompt, sessionCreationReason: "explicit_handoff", predecessorSessionID: predecessor,
			routeMetadata: map[string]string{
				"decision_reason": "explicit_handoff", "handoff_id": handoff.ID,
				"from_backend": string(handoff.FromBackend), "to_backend": string(handoff.ToBackend),
			},
			inheritedGuard: guard, releaseInheritedGuard: guard != nil,
		})
	}()
	return HandoffTaskResult{Task: task, Handoff: handoff}, nil
}

func buildHandoffPacket(evidence taskstore.TaskEvidence, snapshot taskstore.WorkspaceSnapshot, destination agent.BackendID, request string) CanonicalHandoffPacket {
	packet := CanonicalHandoffPacket{
		Version: 1, TaskID: evidence.Task.ID, FromBackend: evidence.Task.PreferredBackend, ToBackend: destination,
		Permission: evidence.Task.Permission,
		Workspace:  HandoffWorkspace{Fingerprint: snapshot.Fingerprint, HeadCommit: snapshot.HeadCommit, Branch: snapshot.Branch},
	}
	packet.OriginalGoal, packet.Truncated = boundedHandoff(evidence.Task.OriginalGoal, 16<<10, packet.Truncated)
	packet.CurrentRequest, packet.Truncated = boundedHandoff(request, 16<<10, packet.Truncated)
	packet.Workspace.Status, packet.Truncated = boundedHandoff(snapshot.StatusSummary, 8<<10, packet.Truncated)
	packet.Workspace.BoundedDiff, packet.Truncated = boundedHandoff(snapshot.BoundedDiff, 16<<10, packet.Truncated)
	turnStart := 0
	if len(evidence.Turns) > 16 {
		turnStart = len(evidence.Turns) - 16
		packet.Truncated = true
	}
	for _, turn := range evidence.Turns[turnStart:] {
		item := HandoffConversationTurn{Sequence: turn.Sequence, Status: turn.Status, ErrorCode: turn.ErrorCode}
		item.User, packet.Truncated = boundedHandoff(turn.UserMessage, 1<<10, packet.Truncated)
		item.Assistant, packet.Truncated = boundedHandoff(turn.AssistantMessage, 2<<10, packet.Truncated)
		item.Error, packet.Truncated = boundedHandoff(turn.ErrorMessage, 512, packet.Truncated)
		packet.Conversation = append(packet.Conversation, item)
	}
	fileStart := 0
	if len(evidence.Files) > 100 {
		fileStart = len(evidence.Files) - 100
		packet.Truncated = true
	}
	for _, file := range evidence.Files[fileStart:] {
		path, truncated := boundedHandoff(file.Path, 512, packet.Truncated)
		previous, truncated := boundedHandoff(file.PreviousPath, 512, truncated)
		packet.Truncated = truncated
		packet.Files = append(packet.Files, HandoffFile{Path: path, PreviousPath: previous, Operation: file.Operation})
	}
	commandStart := 0
	if len(evidence.Commands) > 20 {
		commandStart = len(evidence.Commands) - 20
		packet.Truncated = true
	}
	for _, command := range evidence.Commands[commandStart:] {
		text, truncated := boundedHandoff(command.Command, 1<<10, packet.Truncated)
		output, truncated := boundedHandoff(command.AggregatedOutput, 512, truncated)
		packet.Truncated = truncated
		packet.Commands = append(packet.Commands, HandoffCommand{
			Command: text, Status: command.Status, ExitCode: command.ExitCode, Output: output,
		})
	}
	if len(evidence.VerificationRuns) > 0 {
		run := evidence.VerificationRuns[len(evidence.VerificationRuns)-1]
		verification := &HandoffVerification{Status: run.Status}
		verification.Summary, packet.Truncated = boundedHandoff(run.Summary, 2<<10, packet.Truncated)
		checks := run.Checks
		if len(checks) > 12 {
			checks = checks[len(checks)-12:]
			packet.Truncated = true
		}
		for _, check := range checks {
			name, truncated := boundedHandoff(check.Name, 256, packet.Truncated)
			command, truncated := boundedHandoff(check.Command, 1<<10, truncated)
			output, truncated := boundedHandoff(check.Output, 512, truncated)
			packet.Truncated = truncated
			verification.Checks = append(verification.Checks, HandoffVerificationCheck{
				Name: name, Kind: check.Kind, Status: check.Status, Command: command, ExitCode: check.ExitCode, Output: output,
			})
		}
		packet.Verification = verification
	}
	if len(evidence.Turns) > 0 {
		latest := evidence.Turns[len(evidence.Turns)-1]
		if latest.ErrorCode != "" {
			message, truncated := boundedHandoff(latest.ErrorMessage, 2<<10, packet.Truncated)
			packet.Truncated = truncated
			packet.LatestFailure = &HandoffFailure{Code: latest.ErrorCode, Message: message}
		}
	}
	return packet
}

func fitHandoffPacket(packet *CanonicalHandoffPacket) (json.RawMessage, error) {
	for {
		raw, err := json.Marshal(packet)
		if err != nil {
			return nil, fmt.Errorf("orchestrator: encode handoff packet: %w", err)
		}
		if len(raw) <= maxHandoffPacket {
			return raw, nil
		}
		packet.Truncated = true
		if packet.Workspace.BoundedDiff != "" {
			packet.Workspace.BoundedDiff = ""
			continue
		}
		clearedOutput := false
		for index := range packet.Commands {
			if packet.Commands[index].Output != "" {
				packet.Commands[index].Output = ""
				clearedOutput = true
			}
		}
		if clearedOutput {
			continue
		}
		if len(packet.Conversation) > 1 {
			packet.Conversation = packet.Conversation[1:]
			continue
		}
		return nil, fmt.Errorf("orchestrator: canonical handoff packet exceeds %d bytes", maxHandoffPacket)
	}
}

func boundedHandoff(value string, limit int, alreadyTruncated bool) (string, bool) {
	if len(value) <= limit {
		return value, alreadyTruncated
	}
	return value[:limit] + "\n[indexqube: truncated]", true
}
