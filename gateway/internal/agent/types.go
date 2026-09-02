// Package agent defines the provider-neutral contract used by coding-agent
// backends. Provider-specific wire events must be normalized at the adapter
// boundary before they enter the task service.
package agent

import (
	"context"
	"os/exec"
	"time"
)

type BackendID string

const (
	BackendFake   BackendID = "fake"
	BackendCodex  BackendID = "codex"
	BackendClaude BackendID = "claude"
)

type PermissionMode string

const (
	PermissionReadOnly PermissionMode = "read_only"
	PermissionWrite    PermissionMode = "write"
)

type EventType string

const (
	EventRouteSelected         EventType = "route_selected"
	EventSessionStarted        EventType = "session_started"
	EventAssistantDelta        EventType = "assistant_delta"
	EventAssistantMessage      EventType = "assistant_message"
	EventToolStarted           EventType = "tool_started"
	EventToolFinished          EventType = "tool_finished"
	EventFileChanged           EventType = "file_changed"
	EventCommandFinished       EventType = "command_finished"
	EventApprovalRequested     EventType = "approval_requested"
	EventApprovalResolved      EventType = "approval_resolved"
	EventVerificationCompleted EventType = "verification_completed"
	EventWarning               EventType = "warning"
	EventError                 EventType = "error"
	EventCompleted             EventType = "completed"
	EventCancelled             EventType = "cancelled"
)

type MessageEvent struct {
	Text string `json:"text"`
}

type ToolEvent struct {
	Name   string `json:"name"`
	Status string `json:"status,omitempty"`
}

// CommandEvent is durable, user-visible evidence of a command requested by an
// agent. Output is bounded by each adapter before the event enters canonical
// storage.
type CommandEvent struct {
	Command          string `json:"command"`
	Status           string `json:"status,omitempty"`
	ExitCode         *int   `json:"exit_code,omitempty"`
	AggregatedOutput string `json:"aggregated_output,omitempty"`
}

type FileChange struct {
	Path      string `json:"path"`
	Operation string `json:"operation,omitempty"`
}

type FileEvent struct {
	// Path and Operation retain the original single-change event contract.
	// Changes carries every path when a backend reports a batch of edits.
	Path      string       `json:"path"`
	Operation string       `json:"operation,omitempty"`
	Changes   []FileChange `json:"changes,omitempty"`
}

type ResultEvent struct {
	ExitCode int    `json:"exit_code,omitempty"`
	Status   string `json:"status,omitempty"`
	Error    string `json:"error,omitempty"`
}

// ApprovalKind identifies the operation class a backend wants the user to
// authorize. The values are backend-neutral even when the provider uses a
// different method name on the wire.
type ApprovalKind string

const (
	ApprovalCommand    ApprovalKind = "command"
	ApprovalFileChange ApprovalKind = "file_change"
)

// ApprovalDecision is the normalized response sent back to an agent backend.
// Session-wide grants are deliberately excluded from the first durable
// protocol so a restart can never inherit an implicit authorization.
type ApprovalDecision string

const (
	ApprovalAccept  ApprovalDecision = "accept"
	ApprovalDecline ApprovalDecision = "decline"
	ApprovalCancel  ApprovalDecision = "cancel"
)

// ApprovalRequest contains the bounded, user-visible portion of a backend
// approval request. BackendRequestID is opaque and is used only to route the
// durable IndexQube decision back to the waiting backend process.
type ApprovalRequest struct {
	BackendRequestID string       `json:"backend_request_id"`
	Kind             ApprovalKind `json:"kind"`
	ItemID           string       `json:"item_id,omitempty"`
	NativeThreadID   string       `json:"native_thread_id,omitempty"`
	NativeTurnID     string       `json:"native_turn_id,omitempty"`
	Reason           string       `json:"reason,omitempty"`
	Command          string       `json:"command,omitempty"`
	CWD              string       `json:"cwd,omitempty"`
	GrantRoot        string       `json:"grant_root,omitempty"`
	NetworkHost      string       `json:"network_host,omitempty"`
	NetworkProtocol  string       `json:"network_protocol,omitempty"`
}

type ApprovalEvent struct {
	ApprovalID      string           `json:"approval_id"`
	Kind            ApprovalKind     `json:"kind"`
	Status          string           `json:"status"`
	Decision        ApprovalDecision `json:"decision,omitempty"`
	Reason          string           `json:"reason,omitempty"`
	Command         string           `json:"command,omitempty"`
	CWD             string           `json:"cwd,omitempty"`
	GrantRoot       string           `json:"grant_root,omitempty"`
	NetworkHost     string           `json:"network_host,omitempty"`
	NetworkProtocol string           `json:"network_protocol,omitempty"`
}

// Event is the canonical event envelope. Metadata is intentionally small and
// string-only; adapters may retain provider IDs and version information, but
// must not put arbitrary provider payloads here.
type Event struct {
	ID        string            `json:"id,omitempty"`
	Sequence  int64             `json:"sequence,omitempty"`
	Type      EventType         `json:"type"`
	TaskID    string            `json:"task_id"`
	TurnID    string            `json:"turn_id,omitempty"`
	Backend   BackendID         `json:"backend,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
	Message   *MessageEvent     `json:"message,omitempty"`
	Tool      *ToolEvent        `json:"tool,omitempty"`
	Command   *CommandEvent     `json:"command,omitempty"`
	File      *FileEvent        `json:"file,omitempty"`
	Approval  *ApprovalEvent    `json:"approval,omitempty"`
	Result    *ResultEvent      `json:"result,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

type HealthStatus string

const (
	HealthAvailable   HealthStatus = "available"
	HealthUnavailable HealthStatus = "unavailable"
)

type BackendHealth struct {
	Backend   BackendID    `json:"backend"`
	Status    HealthStatus `json:"status"`
	Version   string       `json:"version,omitempty"`
	Reason    string       `json:"reason,omitempty"`
	CheckedAt time.Time    `json:"checked_at"`
}

// ProcessGuard lets the platform-specific workspace lock attach its lifetime
// to a child process without exposing file descriptors to the task service.
type ProcessGuard interface {
	PrepareCommand(*exec.Cmd) error
}

type Request struct {
	TaskID          string
	TurnID          string
	Workspace       string
	Prompt          string
	Permission      PermissionMode
	NativeSessionID string
	WriteEpoch      uint64
	Guard           ProcessGuard
	Approvals       ApprovalHandler
}

type Result struct {
	NativeSessionID string
	FinalMessage    string
	ExitCode        int
	ResumeLost      bool
	MutationSeen    bool
}

type EventSink interface {
	Publish(context.Context, Event) error
}

type EventSinkFunc func(context.Context, Event) error

func (f EventSinkFunc) Publish(ctx context.Context, event Event) error {
	return f(ctx, event)
}

// ApprovalHandler persists an approval request before blocking the backend
// and returns only after the user, cancellation, or timeout supplies a durable
// decision.
type ApprovalHandler interface {
	RequestApproval(context.Context, ApprovalRequest) (ApprovalDecision, error)
}

type Backend interface {
	ID() BackendID
	Probe(context.Context) BackendHealth
	Execute(context.Context, Request, EventSink) (Result, error)
}

// PermissionValidator is an optional backend capability gate. The task service
// calls it before creating canonical state so unsupported permissions fail
// atomically instead of creating a doomed task.
type PermissionValidator interface {
	ValidatePermission(PermissionMode) error
}
