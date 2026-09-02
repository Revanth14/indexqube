// Package taskstore persists IndexQube's canonical agent-task history.
package taskstore

import (
	"encoding/json"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
)

type TaskStatus string

const (
	// TaskOpen is idle and accepts a continuation.
	TaskOpen TaskStatus = "open"
	// TaskRunning and TaskAwaitingApproval have one active turn and reject
	// continuations and lifecycle transitions.
	TaskRunning          TaskStatus = "running"
	TaskAwaitingApproval TaskStatus = "awaiting_approval"
	// TaskNeedsAttention blocks continuation until an explicit reopen
	// acknowledges that the workspace has been inspected.
	TaskNeedsAttention TaskStatus = "needs_attention"
	// TaskClosed is an idle archived task; reopen restores it to TaskOpen.
	TaskClosed TaskStatus = "closed"
)

type TurnStatus string

const (
	TurnQueued           TurnStatus = "queued"
	TurnRunning          TurnStatus = "running"
	TurnAwaitingApproval TurnStatus = "awaiting_approval"
	TurnSucceeded        TurnStatus = "succeeded"
	TurnFailed           TurnStatus = "failed"
	TurnCancelled        TurnStatus = "cancelled"
)

type Task struct {
	ID               string               `json:"task_id"`
	WorkspaceID      string               `json:"workspace_id"`
	WorkspacePath    string               `json:"workspace_path"`
	OriginalGoal     string               `json:"original_goal"`
	Permission       agent.PermissionMode `json:"permission"`
	PreferredBackend agent.BackendID      `json:"preferred_backend"`
	Status           TaskStatus           `json:"status"`
	Revision         int64                `json:"revision"`
	CreatedAt        time.Time            `json:"created_at"`
	UpdatedAt        time.Time            `json:"updated_at"`
}

// BackendPin is an explicit durable routing constraint. Changing backends is
// intentionally not part of this record; cross-backend continuation must use
// a canonical handoff.
type BackendPin struct {
	TaskID    string          `json:"task_id"`
	Backend   agent.BackendID `json:"backend"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

type Turn struct {
	ID               string               `json:"turn_id"`
	TaskID           string               `json:"task_id"`
	Sequence         int64                `json:"sequence"`
	IdempotencyKey   string               `json:"idempotency_key,omitempty"`
	UserMessage      string               `json:"user_message"`
	AssistantMessage string               `json:"assistant_message,omitempty"`
	BackendSessionID string               `json:"backend_session_id,omitempty"`
	Permission       agent.PermissionMode `json:"permission"`
	Status           TurnStatus           `json:"status"`
	WriteEpoch       uint64               `json:"write_epoch,omitempty"`
	ErrorCode        string               `json:"error_code,omitempty"`
	ErrorMessage     string               `json:"error_message,omitempty"`
	CreatedAt        time.Time            `json:"created_at"`
	StartedAt        *time.Time           `json:"started_at,omitempty"`
	CompletedAt      *time.Time           `json:"completed_at,omitempty"`
}

type BackendSession struct {
	ID               string          `json:"backend_session_id"`
	TaskID           string          `json:"task_id"`
	Backend          agent.BackendID `json:"backend"`
	NativeSessionID  string          `json:"native_session_id"`
	PredecessorID    string          `json:"predecessor_id,omitempty"`
	CreationReason   string          `json:"creation_reason"`
	Status           string          `json:"status"`
	ProviderMetadata string          `json:"provider_metadata,omitempty"`
	CreatedAt        time.Time       `json:"created_at"`
	LastUsedAt       time.Time       `json:"last_used_at"`
	TerminatedAt     *time.Time      `json:"terminated_at,omitempty"`
}

type RouteAttempt struct {
	ID               string             `json:"route_attempt_id"`
	TurnID           string             `json:"turn_id"`
	Ordinal          int                `json:"ordinal"`
	Backend          agent.BackendID    `json:"backend"`
	BackendSessionID string             `json:"backend_session_id,omitempty"`
	DecisionReason   string             `json:"decision_reason"`
	Status           string             `json:"status"`
	FailureClass     agent.FailureClass `json:"failure_class,omitempty"`
	MutationObserved bool               `json:"mutation_observed"`
	FallbackEligible bool               `json:"automatic_fallback_eligible"`
	PreFingerprint   string             `json:"pre_fingerprint,omitempty"`
	PostFingerprint  string             `json:"post_fingerprint,omitempty"`
	StartedAt        time.Time          `json:"started_at"`
	CompletedAt      *time.Time         `json:"completed_at,omitempty"`
}

func (r RouteAttempt) CanAutomaticallyFallback() bool {
	return agent.AutomaticFallbackEligible(r.FailureClass) && !r.MutationObserved &&
		r.PreFingerprint != "" && r.PostFingerprint == r.PreFingerprint
}

type WorkspaceSnapshot struct {
	ID            string               `json:"snapshot_id"`
	TaskID        string               `json:"task_id"`
	TurnID        string               `json:"turn_id"`
	Phase         string               `json:"phase"`
	WorkspaceID   string               `json:"workspace_id"`
	HeadCommit    string               `json:"head_commit,omitempty"`
	Branch        string               `json:"branch,omitempty"`
	StagedHash    string               `json:"staged_hash"`
	UnstagedHash  string               `json:"unstaged_hash"`
	UntrackedHash string               `json:"untracked_hash"`
	Fingerprint   string               `json:"fingerprint"`
	StatusSummary string               `json:"status_summary,omitempty"`
	BoundedDiff   string               `json:"bounded_diff,omitempty"`
	CapturedAt    time.Time            `json:"captured_at"`
	Files         []WorkspaceFileState `json:"files,omitempty"`
}

// WorkspaceFileState is the canonical Git-visible state of one dirty path at
// a snapshot boundary. A per-path fingerprint lets IndexQube distinguish an
// agent edit from a pre-existing dirty baseline even when porcelain status is
// unchanged (for example M before and M after).
type WorkspaceFileState struct {
	SnapshotID     string `json:"snapshot_id"`
	TaskID         string `json:"task_id"`
	TurnID         string `json:"turn_id"`
	Path           string `json:"path"`
	OriginalPath   string `json:"original_path,omitempty"`
	IndexStatus    string `json:"index_status,omitempty"`
	WorktreeStatus string `json:"worktree_status,omitempty"`
	Fingerprint    string `json:"fingerprint"`
}

// WorkspaceFileDelta is authoritative mutation evidence derived from the
// before/after file states. Agent events are compared with, never substituted
// for, this record.
type WorkspaceFileDelta struct {
	ID                string    `json:"delta_id"`
	TaskID            string    `json:"task_id"`
	TurnID            string    `json:"turn_id"`
	Path              string    `json:"path"`
	PreviousPath      string    `json:"previous_path,omitempty"`
	Operation         string    `json:"operation"`
	BeforeFingerprint string    `json:"before_fingerprint,omitempty"`
	AfterFingerprint  string    `json:"after_fingerprint,omitempty"`
	RecordedAt        time.Time `json:"recorded_at"`
}

type CreateTaskInput struct {
	TaskID           string
	TurnID           string
	RouteAttemptID   string
	WorkspaceID      string
	WorkspacePath    string
	Goal             string
	Permission       agent.PermissionMode
	PreferredBackend agent.BackendID
	PinBackend       bool
	IdempotencyKey   string
	Now              time.Time
}

type CreateTurnInput struct {
	TurnID         string
	RouteAttemptID string
	TaskID         string
	Message        string
	Permission     agent.PermissionMode
	Backend        agent.BackendID
	IdempotencyKey string
	Now            time.Time
}

type CreateHandoffInput struct {
	HandoffID            string
	TaskID               string
	TurnID               string
	RouteAttemptID       string
	FromBackend          agent.BackendID
	ToBackend            agent.BackendID
	Message              string
	Permission           agent.PermissionMode
	WorkspaceFingerprint string
	Packet               json.RawMessage
	IdempotencyKey       string
	Now                  time.Time
}

// Handoff is the durable boundary between two backend-native conversations.
// Packet is the exact bounded canonical context delivered to the destination.
type Handoff struct {
	ID                   string          `json:"handoff_id"`
	TaskID               string          `json:"task_id"`
	TurnID               string          `json:"turn_id"`
	FromBackend          agent.BackendID `json:"from_backend"`
	ToBackend            agent.BackendID `json:"to_backend"`
	WorkspaceFingerprint string          `json:"workspace_fingerprint"`
	Packet               json.RawMessage `json:"packet"`
	CreatedAt            time.Time       `json:"created_at"`
}

// InterruptedRun is the minimal canonical state needed to reconcile a turn
// that was queued or running when the daemon stopped unexpectedly.
type InterruptedRun struct {
	TaskID                string
	TurnID                string
	AttemptID             string
	WorkspaceID           string
	WorkspacePath         string
	Permission            agent.PermissionMode
	TurnStatus            TurnStatus
	PreFingerprint        string
	CancellationRequested bool
}

type TaskState struct {
	Task         Task            `json:"task"`
	BackendPin   *BackendPin     `json:"backend_pin,omitempty"`
	LatestTurn   *Turn           `json:"latest_turn,omitempty"`
	Session      *BackendSession `json:"latest_backend_session,omitempty"`
	Cancellation *Cancellation   `json:"latest_cancellation,omitempty"`
}

// CommandEvidence and FileEvidence are projections over normalized events.
// They keep clients from having to understand backend-specific event shapes.
type CommandEvidence struct {
	EventID          string          `json:"event_id"`
	TurnID           string          `json:"turn_id"`
	Backend          agent.BackendID `json:"backend"`
	Command          string          `json:"command"`
	Status           string          `json:"status,omitempty"`
	ExitCode         *int            `json:"exit_code,omitempty"`
	AggregatedOutput string          `json:"aggregated_output,omitempty"`
	ObservedAt       time.Time       `json:"observed_at"`
}

type FileEvidence struct {
	EventID      string          `json:"event_id"`
	TurnID       string          `json:"turn_id"`
	Backend      agent.BackendID `json:"backend"`
	Path         string          `json:"path"`
	PreviousPath string          `json:"previous_path,omitempty"`
	Operation    string          `json:"operation,omitempty"`
	Source       string          `json:"source,omitempty"`
	ObservedAt   time.Time       `json:"observed_at"`
}

type ApprovalStatus string

const (
	ApprovalPending   ApprovalStatus = "pending"
	ApprovalApproved  ApprovalStatus = "approved"
	ApprovalDenied    ApprovalStatus = "denied"
	ApprovalCancelled ApprovalStatus = "cancelled"
	ApprovalExpired   ApprovalStatus = "expired"
)

// Approval is IndexQube's canonical record of one backend authorization
// boundary. BackendRequestID is opaque provider routing state; Approval.ID is
// the stable identifier exposed to users and clients.
type Approval struct {
	ID               string                 `json:"approval_id"`
	TaskID           string                 `json:"task_id"`
	TurnID           string                 `json:"turn_id"`
	Backend          agent.BackendID        `json:"backend"`
	BackendRequestID string                 `json:"backend_request_id"`
	Kind             agent.ApprovalKind     `json:"kind"`
	ItemID           string                 `json:"item_id,omitempty"`
	NativeThreadID   string                 `json:"native_thread_id,omitempty"`
	NativeTurnID     string                 `json:"native_turn_id,omitempty"`
	Reason           string                 `json:"reason,omitempty"`
	Command          string                 `json:"command,omitempty"`
	CWD              string                 `json:"cwd,omitempty"`
	GrantRoot        string                 `json:"grant_root,omitempty"`
	NetworkHost      string                 `json:"network_host,omitempty"`
	NetworkProtocol  string                 `json:"network_protocol,omitempty"`
	Status           ApprovalStatus         `json:"status"`
	Decision         agent.ApprovalDecision `json:"decision,omitempty"`
	RequestedAt      time.Time              `json:"requested_at"`
	DecidedAt        *time.Time             `json:"decided_at,omitempty"`
}

type CreateApprovalInput struct {
	Approval
	Now time.Time
}

type CancellationStatus string

const (
	CancellationRequested CancellationStatus = "requested"
	CancellationCompleted CancellationStatus = "completed"
)

// Cancellation is the durable intent to stop one turn. The request is written
// before the orchestrator signals the backend, so daemon recovery can honor a
// cancellation even when the process exits between those two operations.
type Cancellation struct {
	ID          string             `json:"cancellation_id"`
	TaskID      string             `json:"task_id"`
	TurnID      string             `json:"turn_id"`
	Status      CancellationStatus `json:"status"`
	RequestedAt time.Time          `json:"requested_at"`
	CompletedAt *time.Time         `json:"completed_at,omitempty"`
}

type VerificationStatus string

const (
	VerificationRunning  VerificationStatus = "running"
	VerificationPassed   VerificationStatus = "verified"
	VerificationWarnings VerificationStatus = "verified_with_warnings"
	VerificationFailed   VerificationStatus = "verification_failed"
	VerificationSkipped  VerificationStatus = "verification_skipped"
)

type VerificationCheckStatus string

const (
	VerificationCheckPassed  VerificationCheckStatus = "passed"
	VerificationCheckWarning VerificationCheckStatus = "warning"
	VerificationCheckFailed  VerificationCheckStatus = "failed"
)

// VerificationRun is the durable outcome of IndexQube's own post-agent check
// phase. It stays distinct from agent-reported command events so clients can
// tell an agent assertion from independently observed verification.
type VerificationRun struct {
	ID          string              `json:"verification_run_id"`
	TaskID      string              `json:"task_id"`
	TurnID      string              `json:"turn_id"`
	Status      VerificationStatus  `json:"status"`
	Trigger     string              `json:"trigger"`
	Summary     string              `json:"summary,omitempty"`
	StartedAt   time.Time           `json:"started_at"`
	CompletedAt *time.Time          `json:"completed_at,omitempty"`
	Checks      []VerificationCheck `json:"checks"`
}

type VerificationCheck struct {
	ID                string                  `json:"verification_check_id"`
	VerificationRunID string                  `json:"verification_run_id"`
	Ordinal           int                     `json:"ordinal"`
	Name              string                  `json:"name"`
	Kind              string                  `json:"kind"`
	Command           string                  `json:"command"`
	CWD               string                  `json:"cwd"`
	Status            VerificationCheckStatus `json:"status"`
	ExitCode          *int                    `json:"exit_code,omitempty"`
	Output            string                  `json:"output,omitempty"`
	StartedAt         time.Time               `json:"started_at"`
	CompletedAt       *time.Time              `json:"completed_at,omitempty"`
	Findings          []VerificationFinding   `json:"findings,omitempty"`
}

type VerificationFinding struct {
	ID                  string `json:"verification_finding_id"`
	VerificationCheckID string `json:"verification_check_id"`
	Ordinal             int    `json:"ordinal"`
	RuleID              string `json:"rule_id"`
	Severity            string `json:"severity"`
	Category            string `json:"category"`
	Scope               string `json:"scope"`
	Source              string `json:"source"`
	Path                string `json:"path,omitempty"`
	Line                int    `json:"line,omitempty"`
	Evidence            string `json:"evidence"`
	Detail              string `json:"detail"`
	Count               int    `json:"count"`
}

// TaskEvidence is the canonical read model consumed by CLI, TUI, and dashboard
// clients. It is assembled from normalized durable records rather than stored
// as another source of truth.
type TaskEvidence struct {
	Task             Task                `json:"task"`
	BackendPin       *BackendPin         `json:"backend_pin,omitempty"`
	Turns            []Turn              `json:"turns"`
	Sessions         []BackendSession    `json:"backend_sessions"`
	Routes           []RouteAttempt      `json:"route_attempts"`
	Handoffs         []Handoff           `json:"handoffs"`
	Snapshots        []WorkspaceSnapshot `json:"workspace_snapshots"`
	Commands         []CommandEvidence   `json:"commands"`
	Files            []FileEvidence      `json:"files_changed"`
	ReportedFiles    []FileEvidence      `json:"agent_reported_files"`
	Approvals        []Approval          `json:"approvals"`
	Cancellations    []Cancellation      `json:"cancellations"`
	VerificationRuns []VerificationRun   `json:"verification_runs"`
	EvidenceMismatch bool                `json:"evidence_mismatch"`
	Events           []agent.Event       `json:"events"`
}
