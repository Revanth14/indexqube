// Package orchestrator coordinates canonical task state, workspace safety,
// backend execution, and normalized event publication.
package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
	"github.com/Revanth14/indexqube/gateway/internal/taskstore"
	"github.com/Revanth14/indexqube/gateway/internal/workspace"
)

var ErrStaleWriteEpoch = errors.New("orchestrator: event belongs to a stale write epoch")

type StartTaskInput struct {
	Workspace      string               `json:"workspace"`
	Prompt         string               `json:"prompt"`
	Provider       agent.BackendID      `json:"provider"`
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

type Service struct {
	ctx      context.Context
	store    *taskstore.Store
	locks    *workspace.LockManager
	registry *Registry
	bus      *eventBus

	mu      sync.Mutex
	cancels map[string]context.CancelFunc
	wg      sync.WaitGroup
}

func NewService(ctx context.Context, store *taskstore.Store, locks *workspace.LockManager, registry *Registry) (*Service, error) {
	if ctx == nil || store == nil || locks == nil || registry == nil {
		return nil, fmt.Errorf("orchestrator: context, store, locks, and registry are required")
	}
	return &Service{ctx: ctx, store: store, locks: locks, registry: registry, bus: newEventBus(), cancels: make(map[string]context.CancelFunc)}, nil
}

func (s *Service) StartTask(ctx context.Context, input StartTaskInput) (taskstore.Task, error) {
	if input.Provider == "" {
		input.Provider = agent.BackendFake
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
	backend, err := s.registry.Get(input.Provider)
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
		return taskstore.Task{}, fmt.Errorf("orchestrator: backend %q unavailable: %s", input.Provider, health.Reason)
	}
	identity, err := workspace.Resolve(ctx, input.Workspace)
	if err != nil {
		return taskstore.Task{}, err
	}
	now := time.Now().UTC()
	task, turn, attempt, err := s.store.CreateTask(ctx, taskstore.CreateTaskInput{
		TaskID: taskstore.NewID("task"), TurnID: taskstore.NewID("turn"), RouteAttemptID: taskstore.NewID("route"),
		WorkspaceID: identity.ID, WorkspacePath: identity.Root, Goal: input.Prompt, Permission: input.Permission,
		PreferredBackend: input.Provider, IdempotencyKey: input.IdempotencyKey, Now: now,
	})
	if err != nil {
		return taskstore.Task{}, err
	}
	turnCtx, cancel := context.WithCancel(s.ctx)
	s.mu.Lock()
	s.cancels[task.ID] = cancel
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
	s.cancels[task.ID] = cancel
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
		delete(s.cancels, task.ID)
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
				}, tracker)
				recoveredNativeSession = true
			}
		}
	}

	post, snapshotErr := workspace.Capture(context.Background(), identity, task.ID, turn.ID, "post")
	if snapshotErr == nil {
		_ = s.store.AddSnapshot(context.Background(), post)
	}
	postFingerprint := ""
	workspaceChanged := false
	if snapshotErr == nil {
		postFingerprint = post.Fingerprint
		workspaceChanged = pre.Fingerprint != post.Fingerprint
	}
	mutation := workspaceChanged || mutationBeforeRecovery || tracker.mutationSeen || result.MutationSeen

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
		if code == "cancelled" {
			_ = s.store.CancelTurn(context.Background(), task.ID, turn.ID, currentAttempt.ID, safeError(failure), postFingerprint, mutation, time.Now().UTC())
		} else {
			_ = s.store.FailTurn(context.Background(), task.ID, turn.ID, currentAttempt.ID, code, safeError(failure), postFingerprint, mutation, time.Now().UTC())
		}
		typ := agent.EventError
		resultStatus := taskstore.TurnFailed
		if code == "cancelled" {
			typ = agent.EventCancelled
			resultStatus = taskstore.TurnCancelled
		}
		_ = s.emit(context.Background(), agent.Event{
			Type: typ, TaskID: task.ID, TurnID: turn.ID, Backend: backend.ID(),
			Result: &agent.ResultEvent{ExitCode: result.ExitCode, Status: string(resultStatus), Error: safeError(failure)},
		})
		return
	}
	_ = s.store.CompleteTurn(context.Background(), task.ID, turn.ID, currentAttempt.ID, result.FinalMessage, postFingerprint, mutation, time.Now().UTC())
	_ = s.emit(context.Background(), agent.Event{
		Type: agent.EventCompleted, TaskID: task.ID, TurnID: turn.ID, Backend: backend.ID(),
		Result: &agent.ResultEvent{ExitCode: 0, Status: string(taskstore.TurnSucceeded)},
	})
}

func (s *Service) failBeforeRun(ctx context.Context, task taskstore.Task, turn taskstore.Turn, attempt taskstore.RouteAttempt, code string, err error) {
	_ = s.store.FailTurn(context.Background(), task.ID, turn.ID, attempt.ID, code, safeError(err), "", false, time.Now().UTC())
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
		if run.TurnStatus == taskstore.TurnRunning {
			code = "daemon_interrupted_read_only"
			message = "daemon stopped during a read-only backend run; continuation is safe"
			if run.Permission == agent.PermissionWrite {
				code = "daemon_interrupted_write"
				message = "daemon stopped during a mutation-capable run; manual inspection is required"
				mutationRisk = true
			}
		}

		postFingerprint := ""
		if identity, resolveErr := workspace.Resolve(ctx, run.WorkspacePath); resolveErr == nil && identity.ID == run.WorkspaceID {
			if snap, captureErr := workspace.Capture(ctx, identity, run.TaskID, run.TurnID, "recovery"); captureErr == nil {
				if addErr := s.store.AddSnapshot(ctx, snap); addErr == nil {
					postFingerprint = snap.Fingerprint
				}
			}
		}
		if err := s.store.RecoverInterrupted(ctx, run, code, message, postFingerprint, mutationRisk, time.Now().UTC()); err != nil {
			return report, err
		}
		if mutationRisk {
			report.NeedsAttention++
		} else {
			report.Recovered++
		}
		if err := s.emit(ctx, agent.Event{
			Type: agent.EventError, TaskID: run.TaskID, TurnID: run.TurnID,
			Result:   &agent.ResultEvent{Status: string(taskstore.TurnFailed), Error: message},
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

func (s *Service) Cancel(taskID string) bool {
	s.mu.Lock()
	cancel, ok := s.cancels[taskID]
	s.mu.Unlock()
	if ok {
		cancel()
	}
	return ok
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
	// Terminal events are emitted only after the canonical turn state has been
	// committed, so subscribers never observe completion ahead of SQLite.
	if event.Type == agent.EventCompleted || event.Type == agent.EventError || event.Type == agent.EventCancelled {
		return nil
	}
	return s.service.emit(ctx, event)
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
