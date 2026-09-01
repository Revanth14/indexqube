// Package taskstore persists IndexQube's canonical agent-task history.
package taskstore

import (
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
)

type TaskStatus string

const (
	TaskOpen           TaskStatus = "open"
	TaskRunning        TaskStatus = "running"
	TaskNeedsAttention TaskStatus = "needs_attention"
	TaskClosed         TaskStatus = "closed"
)

type TurnStatus string

const (
	TurnQueued    TurnStatus = "queued"
	TurnRunning   TurnStatus = "running"
	TurnSucceeded TurnStatus = "succeeded"
	TurnFailed    TurnStatus = "failed"
	TurnCancelled TurnStatus = "cancelled"
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
	ID               string          `json:"route_attempt_id"`
	TurnID           string          `json:"turn_id"`
	Ordinal          int             `json:"ordinal"`
	Backend          agent.BackendID `json:"backend"`
	BackendSessionID string          `json:"backend_session_id,omitempty"`
	DecisionReason   string          `json:"decision_reason"`
	Status           string          `json:"status"`
	FailureClass     string          `json:"failure_class,omitempty"`
	MutationObserved bool            `json:"mutation_observed"`
	PreFingerprint   string          `json:"pre_fingerprint,omitempty"`
	PostFingerprint  string          `json:"post_fingerprint,omitempty"`
	StartedAt        time.Time       `json:"started_at"`
	CompletedAt      *time.Time      `json:"completed_at,omitempty"`
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

// InterruptedRun is the minimal canonical state needed to reconcile a turn
// that was queued or running when the daemon stopped unexpectedly.
type InterruptedRun struct {
	TaskID         string
	TurnID         string
	AttemptID      string
	WorkspaceID    string
	WorkspacePath  string
	Permission     agent.PermissionMode
	TurnStatus     TurnStatus
	PreFingerprint string
}

type TaskState struct {
	Task       Task            `json:"task"`
	LatestTurn *Turn           `json:"latest_turn,omitempty"`
	Session    *BackendSession `json:"latest_backend_session,omitempty"`
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

// TaskEvidence is the canonical read model consumed by CLI, TUI, and dashboard
// clients. It is assembled from normalized durable records rather than stored
// as another source of truth.
type TaskEvidence struct {
	Task             Task                `json:"task"`
	Turns            []Turn              `json:"turns"`
	Sessions         []BackendSession    `json:"backend_sessions"`
	Routes           []RouteAttempt      `json:"route_attempts"`
	Snapshots        []WorkspaceSnapshot `json:"workspace_snapshots"`
	Commands         []CommandEvidence   `json:"commands"`
	Files            []FileEvidence      `json:"files_changed"`
	ReportedFiles    []FileEvidence      `json:"agent_reported_files"`
	EvidenceMismatch bool                `json:"evidence_mismatch"`
	Events           []agent.Event       `json:"events"`
}
