package taskstore

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
)

const schema = `
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;

CREATE TABLE IF NOT EXISTS tasks (
    task_id            TEXT PRIMARY KEY,
    workspace_id       TEXT NOT NULL,
    workspace_path     TEXT NOT NULL,
    original_goal      TEXT NOT NULL,
    permission_mode    TEXT NOT NULL,
    preferred_backend  TEXT NOT NULL,
    status             TEXT NOT NULL,
    revision           INTEGER NOT NULL DEFAULT 1,
    created_at         INTEGER NOT NULL,
    updated_at         INTEGER NOT NULL,
    retention_deadline INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS turns (
    turn_id             TEXT PRIMARY KEY,
    task_id             TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
    sequence            INTEGER NOT NULL,
    idempotency_key     TEXT,
    user_message        TEXT NOT NULL,
    assistant_message   TEXT NOT NULL DEFAULT '',
    backend_session_id  TEXT,
    permission_mode     TEXT NOT NULL,
    status              TEXT NOT NULL,
    write_epoch         INTEGER NOT NULL DEFAULT 0,
    error_code          TEXT NOT NULL DEFAULT '',
    error_message       TEXT NOT NULL DEFAULT '',
    created_at          INTEGER NOT NULL,
    started_at          INTEGER,
    completed_at        INTEGER,
    UNIQUE(task_id, sequence),
    UNIQUE(task_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS backend_sessions (
    backend_session_id  TEXT PRIMARY KEY,
    task_id             TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
    backend             TEXT NOT NULL,
    native_session_id   TEXT NOT NULL,
    predecessor_id      TEXT REFERENCES backend_sessions(backend_session_id),
    creation_reason     TEXT NOT NULL,
    status              TEXT NOT NULL,
    provider_metadata   TEXT NOT NULL DEFAULT '{}',
    created_at          INTEGER NOT NULL,
    last_used_at        INTEGER NOT NULL,
    terminated_at       INTEGER
);

CREATE TABLE IF NOT EXISTS route_attempts (
    route_attempt_id    TEXT PRIMARY KEY,
    turn_id             TEXT NOT NULL REFERENCES turns(turn_id) ON DELETE CASCADE,
    ordinal             INTEGER NOT NULL,
    backend             TEXT NOT NULL,
    backend_session_id  TEXT REFERENCES backend_sessions(backend_session_id),
    decision_reason     TEXT NOT NULL,
    status              TEXT NOT NULL,
    failure_class       TEXT NOT NULL DEFAULT '',
    mutation_observed   INTEGER NOT NULL DEFAULT 0,
    pre_fingerprint     TEXT NOT NULL DEFAULT '',
    post_fingerprint    TEXT NOT NULL DEFAULT '',
    started_at          INTEGER NOT NULL,
    completed_at        INTEGER,
    UNIQUE(turn_id, ordinal)
);

CREATE TABLE IF NOT EXISTS workspace_snapshots (
    snapshot_id         TEXT PRIMARY KEY,
    task_id             TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
    turn_id             TEXT NOT NULL REFERENCES turns(turn_id) ON DELETE CASCADE,
    phase               TEXT NOT NULL,
    workspace_id        TEXT NOT NULL,
    head_commit         TEXT NOT NULL DEFAULT '',
    branch_name         TEXT NOT NULL DEFAULT '',
    staged_hash         TEXT NOT NULL,
    unstaged_hash       TEXT NOT NULL,
    untracked_hash      TEXT NOT NULL,
    fingerprint         TEXT NOT NULL,
    status_summary      TEXT NOT NULL DEFAULT '',
    bounded_diff        TEXT NOT NULL DEFAULT '',
    captured_at         INTEGER NOT NULL,
    UNIQUE(turn_id, phase)
);

CREATE TABLE IF NOT EXISTS workspace_file_states (
    snapshot_id         TEXT NOT NULL REFERENCES workspace_snapshots(snapshot_id) ON DELETE CASCADE,
    task_id             TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
    turn_id             TEXT NOT NULL REFERENCES turns(turn_id) ON DELETE CASCADE,
    path                TEXT NOT NULL,
    original_path       TEXT NOT NULL DEFAULT '',
    index_status        TEXT NOT NULL DEFAULT '',
    worktree_status     TEXT NOT NULL DEFAULT '',
    fingerprint         TEXT NOT NULL,
    PRIMARY KEY(snapshot_id, path)
);

CREATE TABLE IF NOT EXISTS workspace_file_deltas (
    delta_id            TEXT PRIMARY KEY,
    task_id             TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
    turn_id             TEXT NOT NULL REFERENCES turns(turn_id) ON DELETE CASCADE,
    path                TEXT NOT NULL,
    previous_path       TEXT NOT NULL DEFAULT '',
    operation           TEXT NOT NULL,
    before_fingerprint  TEXT NOT NULL DEFAULT '',
    after_fingerprint   TEXT NOT NULL DEFAULT '',
    recorded_at         INTEGER NOT NULL,
    UNIQUE(turn_id, path, previous_path)
);

CREATE INDEX IF NOT EXISTS workspace_file_states_turn_idx ON workspace_file_states(turn_id, snapshot_id);
CREATE INDEX IF NOT EXISTS workspace_file_deltas_task_idx ON workspace_file_deltas(task_id, recorded_at);

CREATE TABLE IF NOT EXISTS events (
    event_id            TEXT PRIMARY KEY,
    task_id             TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
    turn_id             TEXT,
    sequence            INTEGER NOT NULL,
    event_type          TEXT NOT NULL,
    backend             TEXT NOT NULL DEFAULT '',
    payload_json        TEXT NOT NULL,
    metadata_json       TEXT NOT NULL DEFAULT '{}',
    created_at          INTEGER NOT NULL,
    UNIQUE(task_id, sequence)
);

CREATE TABLE IF NOT EXISTS outbox (
    outbox_id           INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id             TEXT NOT NULL,
    event_id            TEXT NOT NULL,
    payload_json        TEXT NOT NULL,
    created_at          INTEGER NOT NULL,
    delivered_at        INTEGER,
    attempts            INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS workspace_write_epochs (
    workspace_id        TEXT NOT NULL,
    epoch               INTEGER NOT NULL,
    task_id             TEXT NOT NULL,
    turn_id             TEXT NOT NULL,
    owner_instance_id   TEXT NOT NULL,
    status              TEXT NOT NULL,
    acquired_at         INTEGER NOT NULL,
    released_at         INTEGER,
    PRIMARY KEY(workspace_id, epoch)
);

CREATE TABLE IF NOT EXISTS backend_health_observations (
    observation_id      TEXT PRIMARY KEY,
    backend             TEXT NOT NULL,
    status              TEXT NOT NULL,
    reason              TEXT NOT NULL DEFAULT '',
    latency_ms          INTEGER NOT NULL DEFAULT 0,
    version             TEXT NOT NULL DEFAULT '',
    observed_at         INTEGER NOT NULL
);
`

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("taskstore: create parent: %w", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("taskstore: open: %w", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("taskstore: migrate: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		db.Close()
		return nil, fmt.Errorf("taskstore: chmod: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func NewID(prefix string) string {
	buf := make([]byte, 12)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	}
	return prefix + "_" + hex.EncodeToString(buf)
}

func (s *Store) CreateTask(ctx context.Context, in CreateTaskInput) (Task, Turn, RouteAttempt, error) {
	if in.Now.IsZero() {
		in.Now = time.Now().UTC()
	}
	task := Task{
		ID: in.TaskID, WorkspaceID: in.WorkspaceID, WorkspacePath: in.WorkspacePath,
		OriginalGoal: in.Goal, Permission: in.Permission, PreferredBackend: in.PreferredBackend,
		Status: TaskRunning, Revision: 1, CreatedAt: in.Now, UpdatedAt: in.Now,
	}
	turn := Turn{
		ID: in.TurnID, TaskID: in.TaskID, Sequence: 1, IdempotencyKey: in.IdempotencyKey,
		UserMessage: in.Goal, Permission: in.Permission, Status: TurnQueued, CreatedAt: in.Now,
	}
	attempt := RouteAttempt{
		ID: in.RouteAttemptID, TurnID: in.TurnID, Ordinal: 1, Backend: in.PreferredBackend,
		DecisionReason: "explicit_provider", Status: "queued", StartedAt: in.Now,
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Task{}, Turn{}, RouteAttempt{}, err
	}
	defer tx.Rollback()
	retention := in.Now.Add(30 * 24 * time.Hour).UnixMilli()
	if _, err = tx.ExecContext(ctx, `INSERT INTO tasks
		(task_id,workspace_id,workspace_path,original_goal,permission_mode,preferred_backend,status,revision,created_at,updated_at,retention_deadline)
		VALUES(?,?,?,?,?,?,?,?,?,?,?)`, task.ID, task.WorkspaceID, task.WorkspacePath, task.OriginalGoal,
		task.Permission, task.PreferredBackend, task.Status, task.Revision, in.Now.UnixMilli(), in.Now.UnixMilli(), retention); err != nil {
		return Task{}, Turn{}, RouteAttempt{}, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO turns
		(turn_id,task_id,sequence,idempotency_key,user_message,permission_mode,status,created_at)
		VALUES(?,?,?,?,?,?,?,?)`, turn.ID, turn.TaskID, turn.Sequence, nullableString(turn.IdempotencyKey),
		turn.UserMessage, turn.Permission, turn.Status, in.Now.UnixMilli()); err != nil {
		return Task{}, Turn{}, RouteAttempt{}, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO route_attempts
		(route_attempt_id,turn_id,ordinal,backend,decision_reason,status,started_at)
		VALUES(?,?,?,?,?,?,?)`, attempt.ID, attempt.TurnID, attempt.Ordinal, attempt.Backend,
		attempt.DecisionReason, attempt.Status, in.Now.UnixMilli()); err != nil {
		return Task{}, Turn{}, RouteAttempt{}, err
	}
	if err = tx.Commit(); err != nil {
		return Task{}, Turn{}, RouteAttempt{}, err
	}
	return task, turn, attempt, nil
}

// CreateTurn atomically reserves an open task for one continuation. This keeps
// two CLI clients from starting concurrent turns for the same logical task.
func (s *Store) CreateTurn(ctx context.Context, in CreateTurnInput) (Turn, RouteAttempt, error) {
	if in.Now.IsZero() {
		in.Now = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Turn{}, RouteAttempt{}, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE tasks SET status=?,revision=revision+1,updated_at=? WHERE task_id=? AND status=?`,
		TaskRunning, in.Now.UnixMilli(), in.TaskID, TaskOpen)
	if err != nil {
		return Turn{}, RouteAttempt{}, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return Turn{}, RouteAttempt{}, err
	}
	if rows != 1 {
		return Turn{}, RouteAttempt{}, fmt.Errorf("taskstore: task %s is not open", in.TaskID)
	}
	var sequence int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence),0)+1 FROM turns WHERE task_id=?`, in.TaskID).Scan(&sequence); err != nil {
		return Turn{}, RouteAttempt{}, err
	}
	turn := Turn{
		ID: in.TurnID, TaskID: in.TaskID, Sequence: sequence, IdempotencyKey: in.IdempotencyKey,
		UserMessage: in.Message, Permission: in.Permission, Status: TurnQueued, CreatedAt: in.Now,
	}
	attempt := RouteAttempt{
		ID: in.RouteAttemptID, TurnID: in.TurnID, Ordinal: 1, Backend: in.Backend,
		DecisionReason: "pinned_backend", Status: "queued", StartedAt: in.Now,
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO turns
		(turn_id,task_id,sequence,idempotency_key,user_message,permission_mode,status,created_at)
		VALUES(?,?,?,?,?,?,?,?)`, turn.ID, turn.TaskID, turn.Sequence, nullableString(turn.IdempotencyKey),
		turn.UserMessage, turn.Permission, turn.Status, in.Now.UnixMilli()); err != nil {
		return Turn{}, RouteAttempt{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO route_attempts
		(route_attempt_id,turn_id,ordinal,backend,decision_reason,status,started_at)
		VALUES(?,?,?,?,?,?,?)`, attempt.ID, attempt.TurnID, attempt.Ordinal, attempt.Backend,
		attempt.DecisionReason, attempt.Status, in.Now.UnixMilli()); err != nil {
		return Turn{}, RouteAttempt{}, err
	}
	if err := tx.Commit(); err != nil {
		return Turn{}, RouteAttempt{}, err
	}
	return turn, attempt, nil
}

func (s *Store) StartTurn(ctx context.Context, taskID, turnID, attemptID string, epoch uint64, now time.Time) error {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `UPDATE turns SET status=?, write_epoch=?, started_at=? WHERE turn_id=?`,
		TurnRunning, epoch, now.UnixMilli(), turnID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE route_attempts SET status=?, started_at=? WHERE route_attempt_id=?`,
		"running", now.UnixMilli(), attemptID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE tasks SET status=?, revision=revision+1, updated_at=? WHERE task_id=?`,
		TaskRunning, now.UnixMilli(), taskID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) CreateBackendSession(ctx context.Context, session BackendSession) error {
	if session.CreatedAt.IsZero() {
		session.CreatedAt = time.Now().UTC()
	}
	if session.LastUsedAt.IsZero() {
		session.LastUsedAt = session.CreatedAt
	}
	if session.ProviderMetadata == "" {
		session.ProviderMetadata = "{}"
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO backend_sessions
		(backend_session_id,task_id,backend,native_session_id,predecessor_id,creation_reason,status,provider_metadata,created_at,last_used_at)
		VALUES(?,?,?,?,?,?,?,?,?,?)`, session.ID, session.TaskID, session.Backend, session.NativeSessionID,
		nullableString(session.PredecessorID), session.CreationReason, session.Status, session.ProviderMetadata,
		session.CreatedAt.UnixMilli(), session.LastUsedAt.UnixMilli())
	return err
}

func (s *Store) LatestBackendSession(ctx context.Context, taskID string, backend agent.BackendID) (BackendSession, bool, error) {
	var session BackendSession
	var predecessor sql.NullString
	var terminated sql.NullInt64
	var created, lastUsed int64
	err := s.db.QueryRowContext(ctx, `SELECT backend_session_id,task_id,backend,native_session_id,predecessor_id,
		creation_reason,status,provider_metadata,created_at,last_used_at,terminated_at
		FROM backend_sessions WHERE task_id=? AND backend=? ORDER BY created_at DESC, rowid DESC LIMIT 1`, taskID, backend).Scan(
		&session.ID, &session.TaskID, &session.Backend, &session.NativeSessionID, &predecessor,
		&session.CreationReason, &session.Status, &session.ProviderMetadata, &created, &lastUsed, &terminated,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return BackendSession{}, false, nil
	}
	if err != nil {
		return BackendSession{}, false, err
	}
	session.PredecessorID = predecessor.String
	session.CreatedAt = time.UnixMilli(created).UTC()
	session.LastUsedAt = time.UnixMilli(lastUsed).UTC()
	if terminated.Valid {
		value := time.UnixMilli(terminated.Int64).UTC()
		session.TerminatedAt = &value
	}
	return session, true, nil
}

func (s *Store) SetBackendSessionStatus(ctx context.Context, sessionID, status string, now time.Time) error {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	_, err := s.db.ExecContext(ctx, `UPDATE backend_sessions SET status=?,last_used_at=?,terminated_at=? WHERE backend_session_id=?`,
		status, now.UnixMilli(), now.UnixMilli(), sessionID)
	return err
}

func (s *Store) AttachBackendSession(ctx context.Context, turnID, attemptID, sessionID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `UPDATE turns SET backend_session_id=? WHERE turn_id=?`, sessionID, turnID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE route_attempts SET backend_session_id=? WHERE route_attempt_id=?`, sessionID, attemptID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) AddSnapshot(ctx context.Context, snap WorkspaceSnapshot) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO workspace_snapshots
		(snapshot_id,task_id,turn_id,phase,workspace_id,head_commit,branch_name,staged_hash,unstaged_hash,untracked_hash,fingerprint,status_summary,bounded_diff,captured_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, snap.ID, snap.TaskID, snap.TurnID, snap.Phase, snap.WorkspaceID,
		snap.HeadCommit, snap.Branch, snap.StagedHash, snap.UnstagedHash, snap.UntrackedHash, snap.Fingerprint,
		snap.StatusSummary, snap.BoundedDiff, snap.CapturedAt.UnixMilli()); err != nil {
		return err
	}
	for _, file := range snap.Files {
		if _, err := tx.ExecContext(ctx, `INSERT INTO workspace_file_states
			(snapshot_id,task_id,turn_id,path,original_path,index_status,worktree_status,fingerprint)
			VALUES(?,?,?,?,?,?,?,?)`, snap.ID, snap.TaskID, snap.TurnID, file.Path, file.OriginalPath,
			file.IndexStatus, file.WorktreeStatus, file.Fingerprint); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) AddWorkspaceFileDeltas(ctx context.Context, deltas []WorkspaceFileDelta) error {
	if len(deltas) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, delta := range deltas {
		if delta.ID == "" {
			delta.ID = NewID("delta")
		}
		if delta.RecordedAt.IsZero() {
			delta.RecordedAt = time.Now().UTC()
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO workspace_file_deltas
			(delta_id,task_id,turn_id,path,previous_path,operation,before_fingerprint,after_fingerprint,recorded_at)
			VALUES(?,?,?,?,?,?,?,?,?)`, delta.ID, delta.TaskID, delta.TurnID, delta.Path, delta.PreviousPath,
			delta.Operation, delta.BeforeFingerprint, delta.AfterFingerprint, delta.RecordedAt.UnixMilli()); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) AppendEvent(ctx context.Context, event agent.Event) (agent.Event, error) {
	if event.ID == "" {
		event.ID = NewID("evt")
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return agent.Event{}, err
	}
	metadata, err := json.Marshal(event.Metadata)
	if err != nil {
		return agent.Event{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return agent.Event{}, err
	}
	defer tx.Rollback()
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence),0)+1 FROM events WHERE task_id=?`, event.TaskID).Scan(&event.Sequence); err != nil {
		return agent.Event{}, err
	}
	payload, err = json.Marshal(event)
	if err != nil {
		return agent.Event{}, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO events
		(event_id,task_id,turn_id,sequence,event_type,backend,payload_json,metadata_json,created_at)
		VALUES(?,?,?,?,?,?,?,?,?)`, event.ID, event.TaskID, nullableString(event.TurnID), event.Sequence,
		event.Type, event.Backend, string(payload), string(metadata), event.Timestamp.UnixMilli()); err != nil {
		return agent.Event{}, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO outbox(task_id,event_id,payload_json,created_at) VALUES(?,?,?,?)`,
		event.TaskID, event.ID, string(payload), event.Timestamp.UnixMilli()); err != nil {
		return agent.Event{}, err
	}
	if err = tx.Commit(); err != nil {
		return agent.Event{}, err
	}
	return event, nil
}

func (s *Store) EventsAfter(ctx context.Context, taskID string, sequence int64) ([]agent.Event, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT payload_json FROM events WHERE task_id=? AND sequence>? ORDER BY sequence`, taskID, sequence)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []agent.Event
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var event agent.Event
		if err := json.Unmarshal([]byte(raw), &event); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (s *Store) LatestEventSequence(ctx context.Context, taskID string) (int64, error) {
	var sequence int64
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence),0) FROM events WHERE task_id=?`, taskID).Scan(&sequence)
	return sequence, err
}

func (s *Store) CompleteTurn(ctx context.Context, taskID, turnID, attemptID, message, postFingerprint string, mutation, needsAttention bool, now time.Time) error {
	taskStatus := TaskOpen
	if needsAttention {
		taskStatus = TaskNeedsAttention
	}
	return s.finishTurn(ctx, taskID, turnID, attemptID, TurnSucceeded, taskStatus, message, "", "", postFingerprint, mutation, now)
}

func (s *Store) FailTurn(ctx context.Context, taskID, turnID, attemptID, code, message, postFingerprint string, mutation bool, now time.Time) error {
	taskStatus := TaskOpen
	if mutation {
		taskStatus = TaskNeedsAttention
	}
	return s.finishTurn(ctx, taskID, turnID, attemptID, TurnFailed, taskStatus, "", code, message, postFingerprint, mutation, now)
}

func (s *Store) CancelTurn(ctx context.Context, taskID, turnID, attemptID, message, postFingerprint string, mutation bool, now time.Time) error {
	taskStatus := TaskOpen
	if mutation {
		taskStatus = TaskNeedsAttention
	}
	return s.finishTurn(ctx, taskID, turnID, attemptID, TurnCancelled, taskStatus, "", "cancelled", message, postFingerprint, mutation, now)
}

func (s *Store) finishTurn(ctx context.Context, taskID, turnID, attemptID string, turnStatus TurnStatus, taskStatus TaskStatus, assistant, code, message, postFingerprint string, mutation bool, now time.Time) error {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `UPDATE turns SET status=?,assistant_message=?,error_code=?,error_message=?,completed_at=? WHERE turn_id=?`,
		turnStatus, assistant, code, message, now.UnixMilli(), turnID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE route_attempts SET status=?,failure_class=?,mutation_observed=?,post_fingerprint=?,completed_at=? WHERE route_attempt_id=?`,
		string(turnStatus), code, boolInt(mutation), postFingerprint, now.UnixMilli(), attemptID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE tasks SET status=?,revision=revision+1,updated_at=?,retention_deadline=? WHERE task_id=?`,
		taskStatus, now.UnixMilli(), now.Add(30*24*time.Hour).UnixMilli(), taskID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) SetAttemptPreFingerprint(ctx context.Context, attemptID, fingerprint string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE route_attempts SET pre_fingerprint=? WHERE route_attempt_id=?`, fingerprint, attemptID)
	return err
}

func (s *Store) CreateRouteAttempt(ctx context.Context, attempt RouteAttempt) error {
	if attempt.StartedAt.IsZero() {
		attempt.StartedAt = time.Now().UTC()
	}
	if attempt.Status == "" {
		attempt.Status = "running"
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO route_attempts
		(route_attempt_id,turn_id,ordinal,backend,decision_reason,status,pre_fingerprint,started_at)
		VALUES(?,?,?,?,?,?,?,?)`, attempt.ID, attempt.TurnID, attempt.Ordinal, attempt.Backend,
		attempt.DecisionReason, attempt.Status, attempt.PreFingerprint, attempt.StartedAt.UnixMilli())
	return err
}

func (s *Store) FailRouteAttempt(ctx context.Context, attemptID, code, postFingerprint string, mutation bool, now time.Time) error {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	_, err := s.db.ExecContext(ctx, `UPDATE route_attempts SET status='failed',failure_class=?,mutation_observed=?,post_fingerprint=?,completed_at=? WHERE route_attempt_id=?`,
		code, boolInt(mutation), postFingerprint, now.UnixMilli(), attemptID)
	return err
}

func (s *Store) TaskByID(ctx context.Context, taskID string) (Task, bool, error) {
	var task Task
	var created, updated int64
	err := s.db.QueryRowContext(ctx, `SELECT task_id,workspace_id,workspace_path,original_goal,permission_mode,preferred_backend,status,revision,created_at,updated_at FROM tasks WHERE task_id=?`, taskID).Scan(
		&task.ID, &task.WorkspaceID, &task.WorkspacePath, &task.OriginalGoal, &task.Permission,
		&task.PreferredBackend, &task.Status, &task.Revision, &created, &updated,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Task{}, false, nil
	}
	if err != nil {
		return Task{}, false, err
	}
	task.CreatedAt = time.UnixMilli(created).UTC()
	task.UpdatedAt = time.UnixMilli(updated).UTC()
	return task, true, nil
}

func (s *Store) ListTasks(ctx context.Context, limit int) ([]Task, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	rows, err := s.db.QueryContext(ctx, `SELECT task_id,workspace_id,workspace_path,original_goal,permission_mode,
		preferred_backend,status,revision,created_at,updated_at FROM tasks ORDER BY updated_at DESC,task_id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tasks := make([]Task, 0)
	for rows.Next() {
		var task Task
		var created, updated int64
		if err := rows.Scan(&task.ID, &task.WorkspaceID, &task.WorkspacePath, &task.OriginalGoal, &task.Permission,
			&task.PreferredBackend, &task.Status, &task.Revision, &created, &updated); err != nil {
			return nil, err
		}
		task.CreatedAt = time.UnixMilli(created).UTC()
		task.UpdatedAt = time.UnixMilli(updated).UTC()
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}

func (s *Store) TaskEvidence(ctx context.Context, taskID string) (TaskEvidence, bool, error) {
	task, ok, err := s.TaskByID(ctx, taskID)
	if err != nil || !ok {
		return TaskEvidence{}, ok, err
	}
	evidence := TaskEvidence{Task: task}
	if evidence.Turns, err = s.turns(ctx, taskID); err != nil {
		return TaskEvidence{}, false, err
	}
	if evidence.Sessions, err = s.backendSessions(ctx, taskID); err != nil {
		return TaskEvidence{}, false, err
	}
	if evidence.Routes, err = s.routeAttempts(ctx, taskID); err != nil {
		return TaskEvidence{}, false, err
	}
	if evidence.Snapshots, err = s.workspaceSnapshots(ctx, taskID); err != nil {
		return TaskEvidence{}, false, err
	}
	if evidence.Events, err = s.EventsAfter(ctx, taskID, 0); err != nil {
		return TaskEvidence{}, false, err
	}
	if evidence.Files, err = s.workspaceFileEvidence(ctx, taskID); err != nil {
		return TaskEvidence{}, false, err
	}
	evidence.Commands, evidence.ReportedFiles = projectEvidence(evidence.Events)
	evidence.EvidenceMismatch = fileEvidenceMismatch(evidence.Files, evidence.ReportedFiles)
	return evidence, true, nil
}

func (s *Store) turns(ctx context.Context, taskID string) ([]Turn, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT turn_id,task_id,sequence,idempotency_key,user_message,assistant_message,
		backend_session_id,permission_mode,status,write_epoch,error_code,error_message,created_at,started_at,completed_at
		FROM turns WHERE task_id=? ORDER BY sequence`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	turns := make([]Turn, 0)
	for rows.Next() {
		turn, err := scanTurn(rows)
		if err != nil {
			return nil, err
		}
		turns = append(turns, turn)
	}
	return turns, rows.Err()
}

func (s *Store) backendSessions(ctx context.Context, taskID string) ([]BackendSession, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT backend_session_id,task_id,backend,native_session_id,predecessor_id,
		creation_reason,status,provider_metadata,created_at,last_used_at,terminated_at
		FROM backend_sessions WHERE task_id=? ORDER BY created_at,rowid`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	sessions := make([]BackendSession, 0)
	for rows.Next() {
		var session BackendSession
		var predecessor sql.NullString
		var terminated sql.NullInt64
		var created, lastUsed int64
		if err := rows.Scan(&session.ID, &session.TaskID, &session.Backend, &session.NativeSessionID, &predecessor,
			&session.CreationReason, &session.Status, &session.ProviderMetadata, &created, &lastUsed, &terminated); err != nil {
			return nil, err
		}
		session.PredecessorID = predecessor.String
		session.CreatedAt = time.UnixMilli(created).UTC()
		session.LastUsedAt = time.UnixMilli(lastUsed).UTC()
		if terminated.Valid {
			value := time.UnixMilli(terminated.Int64).UTC()
			session.TerminatedAt = &value
		}
		sessions = append(sessions, session)
	}
	return sessions, rows.Err()
}

func (s *Store) routeAttempts(ctx context.Context, taskID string) ([]RouteAttempt, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT ra.route_attempt_id,ra.turn_id,ra.ordinal,ra.backend,ra.backend_session_id,
		ra.decision_reason,ra.status,ra.failure_class,ra.mutation_observed,ra.pre_fingerprint,ra.post_fingerprint,
		ra.started_at,ra.completed_at FROM route_attempts ra JOIN turns tr ON tr.turn_id=ra.turn_id
		WHERE tr.task_id=? ORDER BY tr.sequence,ra.ordinal`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	routes := make([]RouteAttempt, 0)
	for rows.Next() {
		var route RouteAttempt
		var backendSession sql.NullString
		var mutation int
		var started int64
		var completed sql.NullInt64
		if err := rows.Scan(&route.ID, &route.TurnID, &route.Ordinal, &route.Backend, &backendSession,
			&route.DecisionReason, &route.Status, &route.FailureClass, &mutation, &route.PreFingerprint,
			&route.PostFingerprint, &started, &completed); err != nil {
			return nil, err
		}
		route.BackendSessionID = backendSession.String
		route.MutationObserved = mutation != 0
		route.StartedAt = time.UnixMilli(started).UTC()
		if completed.Valid {
			value := time.UnixMilli(completed.Int64).UTC()
			route.CompletedAt = &value
		}
		routes = append(routes, route)
	}
	return routes, rows.Err()
}

func (s *Store) workspaceSnapshots(ctx context.Context, taskID string) ([]WorkspaceSnapshot, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT snapshot_id,task_id,turn_id,phase,workspace_id,head_commit,branch_name,
		staged_hash,unstaged_hash,untracked_hash,fingerprint,status_summary,bounded_diff,captured_at
		FROM workspace_snapshots WHERE task_id=? ORDER BY captured_at,rowid`, taskID)
	if err != nil {
		return nil, err
	}
	snapshots := make([]WorkspaceSnapshot, 0)
	for rows.Next() {
		var snapshot WorkspaceSnapshot
		var captured int64
		if err := rows.Scan(&snapshot.ID, &snapshot.TaskID, &snapshot.TurnID, &snapshot.Phase, &snapshot.WorkspaceID,
			&snapshot.HeadCommit, &snapshot.Branch, &snapshot.StagedHash, &snapshot.UnstagedHash, &snapshot.UntrackedHash,
			&snapshot.Fingerprint, &snapshot.StatusSummary, &snapshot.BoundedDiff, &captured); err != nil {
			return nil, err
		}
		snapshot.CapturedAt = time.UnixMilli(captured).UTC()
		snapshots = append(snapshots, snapshot)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for index := range snapshots {
		files, err := s.workspaceFileStates(ctx, snapshots[index].ID)
		if err != nil {
			return nil, err
		}
		snapshots[index].Files = files
	}
	return snapshots, nil
}

func (s *Store) workspaceFileStates(ctx context.Context, snapshotID string) ([]WorkspaceFileState, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT snapshot_id,task_id,turn_id,path,original_path,index_status,worktree_status,fingerprint
		FROM workspace_file_states WHERE snapshot_id=? ORDER BY path`, snapshotID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	states := make([]WorkspaceFileState, 0)
	for rows.Next() {
		var state WorkspaceFileState
		if err := rows.Scan(&state.SnapshotID, &state.TaskID, &state.TurnID, &state.Path, &state.OriginalPath,
			&state.IndexStatus, &state.WorktreeStatus, &state.Fingerprint); err != nil {
			return nil, err
		}
		states = append(states, state)
	}
	return states, rows.Err()
}

func (s *Store) workspaceFileEvidence(ctx context.Context, taskID string) ([]FileEvidence, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT delta_id,turn_id,path,previous_path,operation,recorded_at
		FROM workspace_file_deltas WHERE task_id=? ORDER BY recorded_at,path`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	files := make([]FileEvidence, 0)
	for rows.Next() {
		var file FileEvidence
		var recorded int64
		if err := rows.Scan(&file.EventID, &file.TurnID, &file.Path, &file.PreviousPath, &file.Operation, &recorded); err != nil {
			return nil, err
		}
		file.Source = "workspace"
		file.ObservedAt = time.UnixMilli(recorded).UTC()
		files = append(files, file)
	}
	return files, rows.Err()
}

func projectEvidence(events []agent.Event) ([]CommandEvidence, []FileEvidence) {
	commands := make([]CommandEvidence, 0)
	files := make([]FileEvidence, 0)
	for _, event := range events {
		if event.Type == agent.EventCommandFinished && event.Command != nil {
			commands = append(commands, CommandEvidence{
				EventID: event.ID, TurnID: event.TurnID, Backend: event.Backend, Command: event.Command.Command,
				Status: event.Command.Status, ExitCode: event.Command.ExitCode,
				AggregatedOutput: event.Command.AggregatedOutput, ObservedAt: event.Timestamp,
			})
		}
		if event.Type != agent.EventFileChanged || event.File == nil {
			continue
		}
		changes := event.File.Changes
		if len(changes) == 0 && event.File.Path != "" {
			changes = []agent.FileChange{{Path: event.File.Path, Operation: event.File.Operation}}
		}
		for _, change := range changes {
			files = append(files, FileEvidence{
				EventID: event.ID, TurnID: event.TurnID, Backend: event.Backend, Path: change.Path,
				Operation: change.Operation, Source: "agent", ObservedAt: event.Timestamp,
			})
		}
	}
	return commands, files
}

func fileEvidenceMismatch(authoritative, reported []FileEvidence) bool {
	required := make(map[string]struct{}, len(authoritative))
	allowed := make(map[string]struct{}, len(authoritative)*2)
	reports := make(map[string]struct{}, len(reported))
	for _, file := range authoritative {
		path := file.TurnID + "\x00" + filepath.ToSlash(filepath.Clean(file.Path))
		required[path] = struct{}{}
		allowed[path] = struct{}{}
		if file.PreviousPath != "" {
			allowed[file.TurnID+"\x00"+filepath.ToSlash(filepath.Clean(file.PreviousPath))] = struct{}{}
		}
	}
	for _, file := range reported {
		reports[file.TurnID+"\x00"+filepath.ToSlash(filepath.Clean(file.Path))] = struct{}{}
	}
	for path := range required {
		if _, ok := reports[path]; !ok {
			return true
		}
	}
	for path := range reports {
		if _, ok := allowed[path]; !ok {
			return true
		}
	}
	return false
}

func (s *Store) TaskState(ctx context.Context, taskID string) (TaskState, bool, error) {
	task, ok, err := s.TaskByID(ctx, taskID)
	if err != nil || !ok {
		return TaskState{}, ok, err
	}
	state := TaskState{Task: task}
	turn, found, err := s.latestTurn(ctx, taskID)
	if err != nil {
		return TaskState{}, false, err
	}
	if found {
		state.LatestTurn = &turn
	}
	session, found, err := s.LatestBackendSession(ctx, taskID, task.PreferredBackend)
	if err != nil {
		return TaskState{}, false, err
	}
	if found {
		state.Session = &session
	}
	return state, true, nil
}

type rowScanner interface {
	Scan(...any) error
}

func scanTurn(row rowScanner) (Turn, error) {
	var turn Turn
	var idempotency, assistant, backendSession, errorCode, errorMessage sql.NullString
	var created int64
	var started, completed sql.NullInt64
	if err := row.Scan(&turn.ID, &turn.TaskID, &turn.Sequence, &idempotency, &turn.UserMessage, &assistant,
		&backendSession, &turn.Permission, &turn.Status, &turn.WriteEpoch, &errorCode, &errorMessage,
		&created, &started, &completed); err != nil {
		return Turn{}, err
	}
	turn.IdempotencyKey = idempotency.String
	turn.AssistantMessage = assistant.String
	turn.BackendSessionID = backendSession.String
	turn.ErrorCode = errorCode.String
	turn.ErrorMessage = errorMessage.String
	turn.CreatedAt = time.UnixMilli(created).UTC()
	if started.Valid {
		value := time.UnixMilli(started.Int64).UTC()
		turn.StartedAt = &value
	}
	if completed.Valid {
		value := time.UnixMilli(completed.Int64).UTC()
		turn.CompletedAt = &value
	}
	return turn, nil
}

func (s *Store) latestTurn(ctx context.Context, taskID string) (Turn, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT turn_id,task_id,sequence,idempotency_key,user_message,assistant_message,
		backend_session_id,permission_mode,status,write_epoch,error_code,error_message,created_at,started_at,completed_at
		FROM turns WHERE task_id=? ORDER BY sequence DESC LIMIT 1`, taskID)
	turn, err := scanTurn(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Turn{}, false, nil
	}
	if err != nil {
		return Turn{}, false, err
	}
	return turn, true, nil
}

func (s *Store) TurnsBefore(ctx context.Context, taskID string, sequence int64) ([]Turn, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT turn_id,task_id,sequence,idempotency_key,user_message,assistant_message,
		backend_session_id,permission_mode,status,write_epoch,error_code,error_message,created_at,started_at,completed_at
		FROM turns WHERE task_id=? AND sequence<? ORDER BY sequence`, taskID, sequence)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var turns []Turn
	for rows.Next() {
		turn, err := scanTurn(rows)
		if err != nil {
			return nil, err
		}
		turns = append(turns, turn)
	}
	return turns, rows.Err()
}

func (s *Store) InterruptedRuns(ctx context.Context) ([]InterruptedRun, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT t.task_id,tr.turn_id,
		COALESCE((SELECT ra.route_attempt_id FROM route_attempts ra WHERE ra.turn_id=tr.turn_id ORDER BY ra.ordinal DESC LIMIT 1),''),
		t.workspace_id,t.workspace_path,t.permission_mode,tr.status,
		COALESCE((SELECT ra.pre_fingerprint FROM route_attempts ra WHERE ra.turn_id=tr.turn_id ORDER BY ra.ordinal DESC LIMIT 1),'')
		FROM tasks t JOIN turns tr ON tr.task_id=t.task_id
		WHERE t.status=? AND tr.status IN (?,?) ORDER BY t.created_at,tr.sequence`, TaskRunning, TurnQueued, TurnRunning)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var runs []InterruptedRun
	for rows.Next() {
		var run InterruptedRun
		if err := rows.Scan(&run.TaskID, &run.TurnID, &run.AttemptID, &run.WorkspaceID, &run.WorkspacePath,
			&run.Permission, &run.TurnStatus, &run.PreFingerprint); err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, rows.Err()
}

func (s *Store) RecoverInterrupted(ctx context.Context, run InterruptedRun, code, message, postFingerprint string, mutation bool, now time.Time) error {
	if run.AttemptID == "" {
		return fmt.Errorf("taskstore: interrupted turn %s has no route attempt", run.TurnID)
	}
	return s.FailTurn(ctx, run.TaskID, run.TurnID, run.AttemptID, code, message, postFingerprint, mutation, now)
}

func (s *Store) BeginWriteEpoch(ctx context.Context, workspaceID, taskID, turnID, owner string, now time.Time) (uint64, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var next uint64
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(epoch),0)+1 FROM workspace_write_epochs WHERE workspace_id=?`, workspaceID).Scan(&next); err != nil {
		return 0, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO workspace_write_epochs
		(workspace_id,epoch,task_id,turn_id,owner_instance_id,status,acquired_at) VALUES(?,?,?,?,?,?,?)`,
		workspaceID, next, taskID, turnID, owner, "acquired", now.UnixMilli()); err != nil {
		return 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return next, nil
}

func (s *Store) ReleaseWriteEpoch(ctx context.Context, workspaceID string, epoch uint64, now time.Time) error {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	_, err := s.db.ExecContext(ctx, `UPDATE workspace_write_epochs SET status='released',released_at=? WHERE workspace_id=? AND epoch=? AND status='acquired'`,
		now.UnixMilli(), workspaceID, epoch)
	return err
}

func (s *Store) CountRows(ctx context.Context, table string) (int, error) {
	allowed := map[string]bool{
		"tasks": true, "turns": true, "backend_sessions": true, "route_attempts": true,
		"workspace_snapshots": true, "workspace_file_states": true, "workspace_file_deltas": true,
		"events": true, "outbox": true, "workspace_write_epochs": true,
	}
	if !allowed[table] {
		return 0, fmt.Errorf("taskstore: unsupported table %q", table)
	}
	var count int
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count) //nolint:gosec -- table is allowlisted above
	return count, err
}

func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
