// Package orchestrator coordinates canonical task state, workspace safety,
// backend execution, and normalized event publication.
package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
	"github.com/Revanth14/indexqube/gateway/internal/taskstore"
	"github.com/Revanth14/indexqube/gateway/internal/workspace"
)

var ErrStaleWriteEpoch = errors.New("orchestrator: event belongs to a stale write epoch")

const defaultApprovalTimeout = 30 * time.Minute

type StartTaskInput struct {
	Workspace string          `json:"workspace"`
	Prompt    string          `json:"prompt"`
	Backend   agent.BackendID `json:"backend"`
	// Provider is a compatibility alias for early control-plane clients. New
	// clients should use Backend; provider names belong to the model data plane.
	Provider       agent.BackendID      `json:"provider,omitempty"`
	Permission     agent.PermissionMode `json:"permission"`
	IdempotencyKey string               `json:"idempotency_key,omitempty"`
}

type ContinueTaskInput struct {
	TaskID         string `json:"task_id"`
	Prompt         string `json:"prompt"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

type ReconciliationReport struct {
	Recovered      int
	NeedsAttention int
}

type CancelTaskResult struct {
	Task         taskstore.Task         `json:"task"`
	Cancellation taskstore.Cancellation `json:"cancellation"`
	Signalled    bool                   `json:"signalled"`
}

type TaskTransitionResult struct {
	Task    taskstore.Task `json:"task"`
	Changed bool           `json:"changed"`
}

type activeTurn struct {
	turnID string
	cancel context.CancelFunc
}

type Service struct {
	ctx      context.Context
	store    *taskstore.Store
	locks    *workspace.LockManager
	registry *Registry
	bus      *eventBus

	mu              sync.Mutex
	cancels         map[string]activeTurn
	approvalWaiters map[string]chan agent.ApprovalDecision
	approvalTimeout time.Duration
	wg              sync.WaitGroup
}

func NewService(ctx context.Context, store *taskstore.Store, locks *workspace.LockManager, registry *Registry) (*Service, error) {
	if ctx == nil || store == nil || locks == nil || registry == nil {
		return nil, fmt.Errorf("orchestrator: context, store, locks, and registry are required")
	}
	return &Service{
		ctx: ctx, store: store, locks: locks, registry: registry, bus: newEventBus(),
		cancels: make(map[string]activeTurn), approvalWaiters: make(map[string]chan agent.ApprovalDecision),
		approvalTimeout: defaultApprovalTimeout,
	}, nil
}

func (s *Service) StartTask(ctx context.Context, input StartTaskInput) (taskstore.Task, error) {
	backendID := input.Backend
	if backendID == "" {
		backendID = input.Provider
	}
	if backendID == "" {
		backendID = agent.BackendFake
	}
	if input.Permission == "" {
		input.Permission = agent.PermissionReadOnly
	}
	if input.Permission != agent.PermissionReadOnly && input.Permission != agent.PermissionWrite {
		return taskstore.Task{}, fmt.Errorf("orchestrator: invalid permission %q", input.Permission)
	}
	if input.Prompt == "" {
		return taskstore.Task{}, fmt.Errorf("orchestrator: empty prompt")
	}
	backend, err := s.registry.Get(backendID)
	if err != nil {
		return taskstore.Task{}, err
	}
	if validator, ok := backend.(agent.PermissionValidator); ok {
		if err := validator.ValidatePermission(input.Permission); err != nil {
			return taskstore.Task{}, err
		}
	}
	health := backend.Probe(ctx)
	if health.Status != agent.HealthAvailable {
		return taskstore.Task{}, fmt.Errorf("orchestrator: backend %q unavailable: %s", backendID, health.Reason)
	}
	identity, err := workspace.Resolve(ctx, input.Workspace)
	if err != nil {
		return taskstore.Task{}, err
	}
	now := time.Now().UTC()
	task, turn, attempt, err := s.store.CreateTask(ctx, taskstore.CreateTaskInput{
		TaskID: taskstore.NewID("task"), TurnID: taskstore.NewID("turn"), RouteAttemptID: taskstore.NewID("route"),
		WorkspaceID: identity.ID, WorkspacePath: identity.Root, Goal: input.Prompt, Permission: input.Permission,
		PreferredBackend: backendID, IdempotencyKey: input.IdempotencyKey, Now: now,
	})
	if err != nil {
		return taskstore.Task{}, err
	}
	turnCtx, cancel := context.WithCancel(s.ctx)
	s.mu.Lock()
	s.cancels[task.ID] = activeTurn{turnID: turn.ID, cancel: cancel}
	s.mu.Unlock()
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.execute(turnCtx, task, turn, attempt, backend, identity, nil)
	}()
	return task, nil
}

func (s *Service) ContinueTask(ctx context.Context, input ContinueTaskInput) (taskstore.Task, error) {
	if strings.TrimSpace(input.Prompt) == "" {
		return taskstore.Task{}, fmt.Errorf("orchestrator: empty prompt")
	}
	task, ok, err := s.store.TaskByID(ctx, input.TaskID)
	if err != nil {
		return taskstore.Task{}, err
	}
	if !ok {
		return taskstore.Task{}, fmt.Errorf("orchestrator: task %q not found", input.TaskID)
	}
	if task.Status != taskstore.TaskOpen {
		return taskstore.Task{}, fmt.Errorf("orchestrator: task %q is %s, not open", task.ID, task.Status)
	}
	backend, err := s.registry.Get(task.PreferredBackend)
	if err != nil {
		return taskstore.Task{}, err
	}
	if validator, ok := backend.(agent.PermissionValidator); ok {
		if err := validator.ValidatePermission(task.Permission); err != nil {
			return taskstore.Task{}, err
		}
	}
	if health := backend.Probe(ctx); health.Status != agent.HealthAvailable {
		return taskstore.Task{}, fmt.Errorf("orchestrator: backend %q unavailable: %s", backend.ID(), health.Reason)
	}
	identity, err := workspace.Resolve(ctx, task.WorkspacePath)
	if err != nil {
		return taskstore.Task{}, err
	}
	if identity.ID != task.WorkspaceID {
		return taskstore.Task{}, fmt.Errorf("orchestrator: workspace identity changed")
	}
	var prior *taskstore.BackendSession
	if session, found, err := s.store.LatestBackendSession(ctx, task.ID, backend.ID()); err != nil {
		return taskstore.Task{}, err
	} else if found && session.Status == "active" {
		prior = &session
	}
	turn, attempt, err := s.store.CreateTurn(ctx, taskstore.CreateTurnInput{
		TurnID: taskstore.NewID("turn"), RouteAttemptID: taskstore.NewID("route"), TaskID: task.ID,
		Message: input.Prompt, Permission: task.Permission, Backend: backend.ID(),
		IdempotencyKey: input.IdempotencyKey, Now: time.Now().UTC(),
	})
	if err != nil {
		return taskstore.Task{}, err
	}
	turnCtx, cancel := context.WithCancel(s.ctx)
	s.mu.Lock()
	s.cancels[task.ID] = activeTurn{turnID: turn.ID, cancel: cancel}
	s.mu.Unlock()
	task.Status = taskstore.TaskRunning
	task.Revision++
	task.UpdatedAt = time.Now().UTC()
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.execute(turnCtx, task, turn, attempt, backend, identity, prior)
	}()
	return task, nil
}

func (s *Service) execute(ctx context.Context, task taskstore.Task, turn taskstore.Turn, attempt taskstore.RouteAttempt, backend agent.Backend, identity workspace.Identity, priorSession *taskstore.BackendSession) {
	defer func() {
		s.mu.Lock()
		if active, ok := s.cancels[task.ID]; ok && active.turnID == turn.ID {
			delete(s.cancels, task.ID)
		}
		s.mu.Unlock()
	}()

	var guard *workspace.WriteGuard
	var err error
	if task.Permission == agent.PermissionWrite {
		guard, err = s.locks.AcquireWrite(ctx, identity.ID, task.ID, turn.ID)
		if err != nil {
			s.failBeforeRun(ctx, task, turn, attempt, "workspace_locked", err)
			return
		}
		defer guard.Release(context.Background())
	}

	pre, err := workspace.Capture(ctx, identity, task.ID, turn.ID, "pre")
	if err != nil {
		s.failBeforeRun(ctx, task, turn, attempt, "snapshot_failed", err)
		return
	}
	if err := s.store.AddSnapshot(ctx, pre); err != nil {
		s.failBeforeRun(ctx, task, turn, attempt, "state_failed", err)
		return
	}
	_ = s.store.SetAttemptPreFingerprint(ctx, attempt.ID, pre.Fingerprint)
	epoch := uint64(0)
	if guard != nil {
		epoch = guard.Epoch()
	}
	if err := s.store.StartTurn(ctx, task.ID, turn.ID, attempt.ID, epoch, time.Now().UTC()); err != nil {
		s.failBeforeRun(ctx, task, turn, attempt, "state_failed", err)
		return
	}
	_ = s.emit(ctx, agent.Event{
		Type: agent.EventRouteSelected, TaskID: task.ID, TurnID: turn.ID, Backend: backend.ID(),
	})

	tracker := &turnEventSink{
		service: s, taskID: task.ID, turnID: turn.ID, backend: backend.ID(), writeEpoch: epoch,
		mutationCapable: task.Permission == agent.PermissionWrite,
	}
	var processGuard agent.ProcessGuard
	if guard != nil {
		processGuard = guard
	}
	currentAttempt := attempt
	if priorSession != nil {
		_ = s.store.AttachBackendSession(ctx, turn.ID, currentAttempt.ID, priorSession.ID)
	}
	nativeSessionID := ""
	if priorSession != nil {
		nativeSessionID = priorSession.NativeSessionID
	}
	result, runErr := backend.Execute(ctx, agent.Request{
		TaskID: task.ID, TurnID: turn.ID, Workspace: identity.Root, Prompt: turn.UserMessage,
		Permission: task.Permission, NativeSessionID: nativeSessionID, WriteEpoch: epoch, Guard: processGuard,
		Approvals: &turnApprovalHandler{service: s, taskID: task.ID, turnID: turn.ID, backend: backend.ID()},
	}, tracker)
	mutationBeforeRecovery := false
	recoveredNativeSession := false
	if result.ResumeLost && priorSession != nil && !errors.Is(runErr, context.Canceled) {
		check, checkErr := workspace.Capture(context.Background(), identity, task.ID, turn.ID, "resume_recovery_check")
		if checkErr == nil {
			checkErr = s.store.AddSnapshot(context.Background(), check)
		}
		recoveryPrompt, promptErr := s.canonicalRecoveryPrompt(context.Background(), task, turn)
		if checkErr == nil && promptErr != nil {
			checkErr = promptErr
		}
		mutationBeforeRecovery = tracker.mutationSeen || result.MutationSeen || (checkErr == nil && pre.Fingerprint != check.Fingerprint)
		if checkErr == nil && !mutationBeforeRecovery {
			_ = s.store.FailRouteAttempt(context.Background(), currentAttempt.ID, "resume_lost", check.Fingerprint, false, time.Now().UTC())
			_ = s.store.SetBackendSessionStatus(context.Background(), priorSession.ID, "resume_lost", time.Now().UTC())
			recoveryAttempt := taskstore.RouteAttempt{
				ID: taskstore.NewID("route"), TurnID: turn.ID, Ordinal: currentAttempt.Ordinal + 1, Backend: backend.ID(),
				DecisionReason: "native_session_recovery", Status: "running", PreFingerprint: check.Fingerprint, StartedAt: time.Now().UTC(),
			}
			if err := s.store.CreateRouteAttempt(context.Background(), recoveryAttempt); err == nil {
				currentAttempt = recoveryAttempt
				_ = s.emit(context.Background(), agent.Event{
					Type: agent.EventWarning, TaskID: task.ID, TurnID: turn.ID, Backend: backend.ID(),
					Message:  &agent.MessageEvent{Text: "native session unavailable; recovered from canonical task context"},
					Metadata: map[string]string{"error_code": "resume_lost"},
				})
				tracker = &turnEventSink{
					service: s, taskID: task.ID, turnID: turn.ID, backend: backend.ID(), writeEpoch: epoch,
					mutationCapable: task.Permission == agent.PermissionWrite,
				}
				result, runErr = backend.Execute(ctx, agent.Request{
					TaskID: task.ID, TurnID: turn.ID, Workspace: identity.Root, Prompt: recoveryPrompt,
					Permission: task.Permission, WriteEpoch: epoch, Guard: processGuard,
					Approvals: &turnApprovalHandler{service: s, taskID: task.ID, turnID: turn.ID, backend: backend.ID()},
				}, tracker)
				recoveredNativeSession = true
			}
		}
	}

	post, snapshotErr := workspace.Capture(context.Background(), identity, task.ID, turn.ID, "post")
	var fileDeltas []taskstore.WorkspaceFileDelta
	if snapshotErr == nil {
		if err := s.store.AddSnapshot(context.Background(), post); err != nil {
			snapshotErr = err
		} else {
			fileDeltas = workspace.DiffFileStates(pre, post)
			if err := s.store.AddWorkspaceFileDeltas(context.Background(), fileDeltas); err != nil {
				snapshotErr = err
			}
		}
	}
	postFingerprint := ""
	workspaceChanged := false
	if snapshotErr == nil {
		postFingerprint = post.Fingerprint
		workspaceChanged = pre.Fingerprint != post.Fingerprint
	}
	mutation := len(fileDeltas) > 0 || workspaceChanged || mutationBeforeRecovery || tracker.mutationSeen || result.MutationSeen
	evidenceMismatch, evidenceMessage := compareMutationEvidence(fileDeltas, tracker.reportedFiles)
	if evidenceMismatch {
		_ = s.emit(context.Background(), agent.Event{
			Type: agent.EventWarning, TaskID: task.ID, TurnID: turn.ID, Backend: backend.ID(),
			Message:  &agent.MessageEvent{Text: evidenceMessage},
			Metadata: map[string]string{"error_code": "workspace_evidence_mismatch"},
		})
	}

	if result.NativeSessionID != "" {
		if priorSession == nil || result.NativeSessionID != priorSession.NativeSessionID {
			reason := "initial"
			predecessor := ""
			if recoveredNativeSession {
				reason = "native_session_recovery"
				predecessor = priorSession.ID
			}
			session := taskstore.BackendSession{
				ID: taskstore.NewID("bs"), TaskID: task.ID, Backend: backend.ID(), NativeSessionID: result.NativeSessionID,
				PredecessorID: predecessor, CreationReason: reason, Status: "active", CreatedAt: time.Now().UTC(),
			}
			if err := s.store.CreateBackendSession(context.Background(), session); err == nil {
				_ = s.store.AttachBackendSession(context.Background(), turn.ID, currentAttempt.ID, session.ID)
			}
		} else {
			_ = s.store.AttachBackendSession(context.Background(), turn.ID, currentAttempt.ID, priorSession.ID)
		}
	}

	if runErr != nil || snapshotErr != nil {
		failure := runErr
		code := "backend_failed"
		if snapshotErr != nil {
			failure = snapshotErr
			code = "snapshot_failed"
		}
		if errors.Is(runErr, context.Canceled) {
			code = "cancelled"
		}
		cancelled := code == "cancelled"
		var finishErr error
		if cancelled {
			finishErr = s.store.CancelTurn(context.Background(), task.ID, turn.ID, currentAttempt.ID, safeError(failure), postFingerprint, mutation || evidenceMismatch, time.Now().UTC())
		} else {
			finishErr = s.store.FailTurn(context.Background(), task.ID, turn.ID, currentAttempt.ID, code, safeError(failure), postFingerprint, mutation || evidenceMismatch, time.Now().UTC())
		}
		if errors.Is(finishErr, taskstore.ErrCancellationRequested) {
			cancelled = true
			failure = context.Canceled
			finishErr = s.store.CancelTurn(context.Background(), task.ID, turn.ID, currentAttempt.ID, "cancellation requested", postFingerprint, mutation || evidenceMismatch, time.Now().UTC())
		}
		if finishErr != nil {
			return
		}
		typ := agent.EventError
		resultStatus := taskstore.TurnFailed
		if cancelled {
			typ = agent.EventCancelled
			resultStatus = taskstore.TurnCancelled
		}
		_ = s.emit(context.Background(), agent.Event{
			Type: typ, TaskID: task.ID, TurnID: turn.ID, Backend: backend.ID(),
			Result: &agent.ResultEvent{ExitCode: result.ExitCode, Status: string(resultStatus), Error: safeError(failure)},
		})
		return
	}
	if err := s.store.CompleteTurn(context.Background(), task.ID, turn.ID, currentAttempt.ID, result.FinalMessage, postFingerprint, mutation, evidenceMismatch, time.Now().UTC()); errors.Is(err, taskstore.ErrCancellationRequested) {
		if cancelErr := s.store.CancelTurn(context.Background(), task.ID, turn.ID, currentAttempt.ID, "cancellation requested", postFingerprint, mutation || evidenceMismatch, time.Now().UTC()); cancelErr != nil {
			return
		}
		_ = s.emit(context.Background(), agent.Event{
			Type: agent.EventCancelled, TaskID: task.ID, TurnID: turn.ID, Backend: backend.ID(),
			Result: &agent.ResultEvent{Status: string(taskstore.TurnCancelled), Error: "cancellation requested"},
		})
		return
	} else if err != nil {
		if failErr := s.store.FailTurn(context.Background(), task.ID, turn.ID, currentAttempt.ID, "state_failed", safeError(err), postFingerprint, mutation || evidenceMismatch, time.Now().UTC()); failErr == nil {
			_ = s.emit(context.Background(), agent.Event{Type: agent.EventError, TaskID: task.ID, TurnID: turn.ID, Backend: backend.ID(),
				Result: &agent.ResultEvent{Status: string(taskstore.TurnFailed), Error: safeError(err)}})
		}
		return
	}
	_ = s.emit(context.Background(), agent.Event{
		Type: agent.EventCompleted, TaskID: task.ID, TurnID: turn.ID, Backend: backend.ID(),
		Result: &agent.ResultEvent{ExitCode: 0, Status: string(taskstore.TurnSucceeded)},
	})
}

func (s *Service) failBeforeRun(ctx context.Context, task taskstore.Task, turn taskstore.Turn, attempt taskstore.RouteAttempt, code string, err error) {
	finishErr := s.store.FailTurn(context.Background(), task.ID, turn.ID, attempt.ID, code, safeError(err), "", false, time.Now().UTC())
	if errors.Is(finishErr, taskstore.ErrCancellationRequested) {
		if cancelErr := s.store.CancelTurn(context.Background(), task.ID, turn.ID, attempt.ID, "cancellation requested", "", false, time.Now().UTC()); cancelErr != nil {
			return
		}
		_ = s.emit(context.Background(), agent.Event{
			Type: agent.EventCancelled, TaskID: task.ID, TurnID: turn.ID, Backend: attempt.Backend,
			Result: &agent.ResultEvent{Status: string(taskstore.TurnCancelled), Error: "cancellation requested"},
		})
		return
	}
	if finishErr != nil {
		return
	}
	_ = s.emit(context.Background(), agent.Event{
		Type: agent.EventError, TaskID: task.ID, TurnID: turn.ID, Backend: attempt.Backend,
		Result: &agent.ResultEvent{Status: string(taskstore.TurnFailed), Error: safeError(err)},
	})
}

func (s *Service) emit(ctx context.Context, event agent.Event) error {
	event.Metadata = agent.NormalizeMetadata(event.Metadata)
	stored, err := s.store.AppendEvent(ctx, event)
	if err != nil {
		return err
	}
	s.bus.publish(stored)
	return nil
}

func (s *Service) Task(ctx context.Context, taskID string) (taskstore.Task, bool, error) {
	return s.store.TaskByID(ctx, taskID)
}

func (s *Service) TaskState(ctx context.Context, taskID string) (taskstore.TaskState, bool, error) {
	return s.store.TaskState(ctx, taskID)
}

func (s *Service) Tasks(ctx context.Context, limit int) ([]taskstore.Task, error) {
	return s.store.ListTasks(ctx, limit)
}

func (s *Service) TaskEvidence(ctx context.Context, taskID string) (taskstore.TaskEvidence, bool, error) {
	return s.store.TaskEvidence(ctx, taskID)
}

func (s *Service) Approvals(ctx context.Context, taskID string, status taskstore.ApprovalStatus, limit int) ([]taskstore.Approval, error) {
	if status != "" && status != taskstore.ApprovalPending && status != taskstore.ApprovalApproved &&
		status != taskstore.ApprovalDenied && status != taskstore.ApprovalCancelled && status != taskstore.ApprovalExpired {
		return nil, fmt.Errorf("orchestrator: invalid approval status %q", status)
	}
	return s.store.ListApprovals(ctx, taskID, status, limit)
}

// DecideApproval commits the user's choice before waking the waiting backend.
// Decisions are intentionally one-shot; retrying an already resolved request
// returns a conflict instead of extending its authority.
func (s *Service) DecideApproval(ctx context.Context, approvalID, requestedDecision string) (taskstore.Approval, error) {
	var decision agent.ApprovalDecision
	var status taskstore.ApprovalStatus
	switch strings.ToLower(strings.TrimSpace(requestedDecision)) {
	case "approve", "approved", "accept":
		decision, status = agent.ApprovalAccept, taskstore.ApprovalApproved
	case "deny", "denied", "decline":
		decision, status = agent.ApprovalDecline, taskstore.ApprovalDenied
	default:
		return taskstore.Approval{}, fmt.Errorf("orchestrator: decision must be approve or deny")
	}
	s.mu.Lock()
	waiter, active := s.approvalWaiters[approvalID]
	s.mu.Unlock()
	if !active {
		if approval, ok, err := s.store.ApprovalByID(ctx, approvalID); err != nil {
			return taskstore.Approval{}, err
		} else if !ok {
			return taskstore.Approval{}, fmt.Errorf("orchestrator: approval %q not found", approvalID)
		} else {
			return taskstore.Approval{}, fmt.Errorf("orchestrator: approval %q is %s and has no active backend", approvalID, approval.Status)
		}
	}
	approval, err := s.store.ResolveApproval(ctx, approvalID, decision, status, time.Now().UTC())
	if err != nil {
		return taskstore.Approval{}, err
	}
	_ = s.emit(context.Background(), agent.Event{
		Type: agent.EventApprovalResolved, TaskID: approval.TaskID, TurnID: approval.TurnID, Backend: approval.Backend,
		Approval: &agent.ApprovalEvent{ApprovalID: approval.ID, Kind: approval.Kind, Status: string(approval.Status),
			Decision: approval.Decision, Reason: approval.Reason, Command: approval.Command, CWD: approval.CWD,
			GrantRoot: approval.GrantRoot, NetworkHost: approval.NetworkHost, NetworkProtocol: approval.NetworkProtocol},
	})
	select {
	case waiter <- decision:
	default:
		return taskstore.Approval{}, fmt.Errorf("orchestrator: approval %q backend is no longer waiting", approvalID)
	}
	return approval, nil
}

type turnApprovalHandler struct {
	service *Service
	taskID  string
	turnID  string
	backend agent.BackendID
}

func (h *turnApprovalHandler) RequestApproval(ctx context.Context, request agent.ApprovalRequest) (agent.ApprovalDecision, error) {
	if request.BackendRequestID == "" {
		return agent.ApprovalCancel, fmt.Errorf("orchestrator: backend approval request has no request ID")
	}
	approvalID := taskstore.NewID("approval")
	waiter := make(chan agent.ApprovalDecision, 1)
	h.service.mu.Lock()
	h.service.approvalWaiters[approvalID] = waiter
	h.service.mu.Unlock()
	defer func() {
		h.service.mu.Lock()
		delete(h.service.approvalWaiters, approvalID)
		h.service.mu.Unlock()
	}()
	approval, err := h.service.store.CreateApproval(ctx, taskstore.CreateApprovalInput{Approval: taskstore.Approval{
		ID: approvalID, TaskID: h.taskID, TurnID: h.turnID, Backend: h.backend,
		BackendRequestID: request.BackendRequestID, Kind: request.Kind, ItemID: request.ItemID,
		NativeThreadID: request.NativeThreadID, NativeTurnID: request.NativeTurnID, Reason: boundedApprovalText(request.Reason, 2048),
		Command: boundedApprovalText(request.Command, 4096), CWD: boundedApprovalText(request.CWD, 4096),
		GrantRoot: boundedApprovalText(request.GrantRoot, 4096), NetworkHost: boundedApprovalText(request.NetworkHost, 1024),
		NetworkProtocol: boundedApprovalText(request.NetworkProtocol, 128),
	}, Now: time.Now().UTC()})
	if err != nil {
		return agent.ApprovalCancel, err
	}
	if err := h.service.emit(ctx, agent.Event{
		Type: agent.EventApprovalRequested, TaskID: h.taskID, TurnID: h.turnID, Backend: h.backend,
		Approval: &agent.ApprovalEvent{ApprovalID: approval.ID, Kind: approval.Kind, Status: string(approval.Status),
			Reason: approval.Reason, Command: approval.Command, CWD: approval.CWD,
			GrantRoot: approval.GrantRoot, NetworkHost: approval.NetworkHost, NetworkProtocol: approval.NetworkProtocol},
	}); err != nil {
		_, _ = h.service.store.ResolveApproval(context.Background(), approval.ID, agent.ApprovalCancel, taskstore.ApprovalCancelled, time.Now().UTC())
		return agent.ApprovalCancel, err
	}
	timeout := h.service.approvalTimeout
	if timeout <= 0 {
		timeout = defaultApprovalTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case decision := <-waiter:
		return decision, nil
	case <-ctx.Done():
		_, _ = h.service.store.ResolveApproval(context.Background(), approval.ID, agent.ApprovalCancel, taskstore.ApprovalCancelled, time.Now().UTC())
		return agent.ApprovalCancel, ctx.Err()
	case <-timer.C:
		expired, resolveErr := h.service.store.ResolveApproval(context.Background(), approval.ID, agent.ApprovalCancel, taskstore.ApprovalExpired, time.Now().UTC())
		if resolveErr == nil {
			_ = h.service.emit(context.Background(), agent.Event{
				Type: agent.EventApprovalResolved, TaskID: expired.TaskID, TurnID: expired.TurnID, Backend: expired.Backend,
				Approval: &agent.ApprovalEvent{ApprovalID: expired.ID, Kind: expired.Kind, Status: string(expired.Status), Decision: expired.Decision,
					Reason: expired.Reason, Command: expired.Command, CWD: expired.CWD, GrantRoot: expired.GrantRoot,
					NetworkHost: expired.NetworkHost, NetworkProtocol: expired.NetworkProtocol},
			})
		}
		return agent.ApprovalCancel, nil
	}
}

func boundedApprovalText(value string, limit int) string {
	value = strings.TrimSpace(value)
	if limit > 0 && len(value) > limit {
		return value[:limit]
	}
	return value
}

// ReconcileInterrupted converts stale queued/running turns into durable failed
// turns before the control API starts accepting work. A started write turn is
// always marked needs_attention: local Git equality cannot prove that commands
// had no external side effects before the daemon disappeared.
func (s *Service) ReconcileInterrupted(ctx context.Context) (ReconciliationReport, error) {
	runs, err := s.store.InterruptedRuns(ctx)
	if err != nil {
		return ReconciliationReport{}, err
	}
	var report ReconciliationReport
	for _, run := range runs {
		code := "daemon_interrupted_pre_run"
		message := "daemon stopped before backend execution began"
		mutationRisk := false
		if run.TurnStatus == taskstore.TurnRunning || run.TurnStatus == taskstore.TurnAwaitingApproval {
			code = "daemon_interrupted_read_only"
			message = "daemon stopped during a read-only backend run; continuation is safe"
			if run.TurnStatus == taskstore.TurnAwaitingApproval {
				code = "daemon_interrupted_approval"
				message = "daemon stopped while an approval was pending; the request was cancelled and never authorized"
			}
			if run.Permission == agent.PermissionWrite {
				code = "daemon_interrupted_write"
				message = "daemon stopped during a mutation-capable run; manual inspection is required"
				mutationRisk = true
			}
		}
		if run.CancellationRequested {
			code = "cancelled"
			message = "durable cancellation completed during daemon recovery"
			mutationRisk = run.Permission == agent.PermissionWrite &&
				(run.TurnStatus == taskstore.TurnRunning || run.TurnStatus == taskstore.TurnAwaitingApproval)
		}

		postFingerprint := ""
		if identity, resolveErr := workspace.Resolve(ctx, run.WorkspacePath); resolveErr == nil && identity.ID == run.WorkspaceID {
			if snap, captureErr := workspace.Capture(ctx, identity, run.TaskID, run.TurnID, "recovery"); captureErr == nil {
				if addErr := s.store.AddSnapshot(ctx, snap); addErr == nil {
					postFingerprint = snap.Fingerprint
				}
			}
		}
		if run.CancellationRequested {
			if err := s.store.CancelTurn(ctx, run.TaskID, run.TurnID, run.AttemptID, message, postFingerprint, mutationRisk, time.Now().UTC()); err != nil {
				return report, err
			}
		} else if err := s.store.RecoverInterrupted(ctx, run, code, message, postFingerprint, mutationRisk, time.Now().UTC()); err != nil {
			return report, err
		}
		if mutationRisk {
			report.NeedsAttention++
		} else {
			report.Recovered++
		}
		eventType := agent.EventError
		turnStatus := taskstore.TurnFailed
		if run.CancellationRequested {
			eventType = agent.EventCancelled
			turnStatus = taskstore.TurnCancelled
		}
		if err := s.emit(ctx, agent.Event{
			Type: eventType, TaskID: run.TaskID, TurnID: run.TurnID,
			Result:   &agent.ResultEvent{Status: string(turnStatus), Error: message},
			Metadata: map[string]string{"error_code": code},
		}); err != nil {
			return report, err
		}
	}
	return report, nil
}

func (s *Service) EventsAfter(ctx context.Context, taskID string, sequence int64) ([]agent.Event, error) {
	return s.store.EventsAfter(ctx, taskID, sequence)
}

func (s *Service) LatestEventSequence(ctx context.Context, taskID string) (int64, error) {
	return s.store.LatestEventSequence(ctx, taskID)
}

func (s *Service) Subscribe(taskID string) (<-chan agent.Event, func()) {
	return s.bus.subscribe(taskID)
}

func (s *Service) Backends(ctx context.Context) []agent.BackendHealth {
	return s.registry.Health(ctx)
}

func (s *Service) Cancel(ctx context.Context, taskID string) (CancelTaskResult, error) {
	task, cancellation, err := s.store.RequestCancellation(ctx, taskID, time.Now().UTC())
	if err != nil {
		return CancelTaskResult{}, err
	}
	s.mu.Lock()
	active, ok := s.cancels[taskID]
	s.mu.Unlock()
	signalled := ok && cancellation.Status == taskstore.CancellationRequested
	if signalled {
		active.cancel()
	}
	return CancelTaskResult{Task: task, Cancellation: cancellation, Signalled: signalled}, nil
}

func (s *Service) CloseTask(ctx context.Context, taskID string) (TaskTransitionResult, error) {
	task, changed, err := s.store.CloseTask(ctx, taskID, time.Now().UTC())
	return TaskTransitionResult{Task: task, Changed: changed}, err
}

func (s *Service) ReopenTask(ctx context.Context, taskID string) (TaskTransitionResult, error) {
	task, changed, err := s.store.ReopenTask(ctx, taskID, time.Now().UTC())
	return TaskTransitionResult{Task: task, Changed: changed}, err
}

// Wait blocks until every supervised backend process has stopped and its
// terminal state has been committed. Daemon shutdown uses this before closing
// SQLite so an interrupted task is still recoverable.
func (s *Service) Wait() {
	s.wg.Wait()
}

func (s *Service) canonicalRecoveryPrompt(ctx context.Context, task taskstore.Task, current taskstore.Turn) (string, error) {
	turns, err := s.store.TurnsBefore(ctx, task.ID, current.Sequence)
	if err != nil {
		return "", err
	}
	var prompt strings.Builder
	prompt.WriteString("INDEXQUBE CANONICAL SESSION RECOVERY\n\n")
	prompt.WriteString("Original goal:\n")
	prompt.WriteString(task.OriginalGoal)
	prompt.WriteString("\n\nCompleted conversation:\n")
	for _, turn := range turns {
		prompt.WriteString("\nUser: ")
		prompt.WriteString(turn.UserMessage)
		if turn.AssistantMessage != "" {
			prompt.WriteString("\nAssistant: ")
			prompt.WriteString(turn.AssistantMessage)
		}
	}
	prompt.WriteString("\n\nCurrent request:\n")
	prompt.WriteString(current.UserMessage)
	prompt.WriteString("\n\nThe filesystem is authoritative. Inspect the current workspace before drawing conclusions. This is a read-only task; do not modify files.\n")
	text := prompt.String()
	const maxRecoveryPrompt = 256 << 10
	if len(text) > maxRecoveryPrompt {
		text = text[:maxRecoveryPrompt] + "\n[indexqube: canonical context truncated]\n"
	}
	return text, nil
}

type turnEventSink struct {
	service         *Service
	taskID          string
	turnID          string
	backend         agent.BackendID
	writeEpoch      uint64
	mutationCapable bool
	mutationSeen    bool
	reportedFiles   map[string]struct{}
}

func (s *turnEventSink) Publish(ctx context.Context, event agent.Event) error {
	event.TaskID = s.taskID
	event.TurnID = s.turnID
	event.Backend = s.backend
	if rawEpoch := event.Metadata["write_epoch"]; rawEpoch != "" {
		epoch, err := strconv.ParseUint(rawEpoch, 10, 64)
		if err != nil || epoch != s.writeEpoch {
			return ErrStaleWriteEpoch
		}
	}
	if event.Type == agent.EventFileChanged || (s.mutationCapable && (event.Type == agent.EventToolStarted || event.Type == agent.EventCommandFinished)) {
		s.mutationSeen = true
	}
	if event.Type == agent.EventFileChanged && event.File != nil {
		if s.reportedFiles == nil {
			s.reportedFiles = make(map[string]struct{})
		}
		changes := event.File.Changes
		if len(changes) == 0 && event.File.Path != "" {
			changes = []agent.FileChange{{Path: event.File.Path}}
		}
		for _, change := range changes {
			if path := normalizeEvidencePath(change.Path); path != "" {
				s.reportedFiles[path] = struct{}{}
			}
		}
	}
	// Terminal events are emitted only after the canonical turn state has been
	// committed, so subscribers never observe completion ahead of SQLite.
	if event.Type == agent.EventCompleted || event.Type == agent.EventError || event.Type == agent.EventCancelled {
		return nil
	}
	return s.service.emit(ctx, event)
}

func compareMutationEvidence(deltas []taskstore.WorkspaceFileDelta, reported map[string]struct{}) (bool, string) {
	required := make(map[string]struct{}, len(deltas))
	allowed := make(map[string]struct{}, len(deltas)*2)
	for _, delta := range deltas {
		path := normalizeEvidencePath(delta.Path)
		if path != "" {
			required[path] = struct{}{}
			allowed[path] = struct{}{}
		}
		if previous := normalizeEvidencePath(delta.PreviousPath); previous != "" {
			allowed[previous] = struct{}{}
		}
	}
	missing := make([]string, 0)
	extra := make([]string, 0)
	for path := range required {
		if _, ok := reported[path]; !ok {
			missing = append(missing, path)
		}
	}
	for path := range reported {
		if _, ok := allowed[path]; !ok {
			extra = append(extra, path)
		}
	}
	if len(missing) == 0 && len(extra) == 0 {
		return false, ""
	}
	sort.Strings(missing)
	sort.Strings(extra)
	parts := []string{"agent file events did not match the authoritative workspace delta; manual inspection is required"}
	if len(missing) > 0 {
		parts = append(parts, "unreported: "+strings.Join(missing, ", "))
	}
	if len(extra) > 0 {
		parts = append(parts, "not present in final delta: "+strings.Join(extra, ", "))
	}
	return true, strings.Join(parts, "; ")
}

func normalizeEvidencePath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	return filepath.ToSlash(filepath.Clean(path))
}

func safeError(err error) string {
	if err == nil {
		return ""
	}
	text := err.Error()
	if len(text) > 1024 {
		text = text[:1024]
	}
	return text
}
