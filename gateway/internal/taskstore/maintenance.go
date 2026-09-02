package taskstore

import (
	"context"
	"sort"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
	"github.com/Revanth14/indexqube/gateway/internal/redact"
)

const (
	defaultRetention = 30 * 24 * time.Hour
	maxMessageBytes  = 128 << 10
	maxCommandBytes  = 32 << 10
	maxOutputBytes   = 128 << 10
	maxErrorBytes    = 32 << 10
	maxPathBytes     = 4 << 10
	maxMetadataItems = 32
	maxMetadataKey   = 128
	maxMetadataValue = 4 << 10
)

type RetentionResult struct {
	TasksDeleted int64 `json:"tasks_deleted"`
}

// ApplyRetention removes only explicitly closed tasks whose inactivity window
// has expired. Active, open, and needs-attention tasks are never swept.
func (s *Store) ApplyRetention(ctx context.Context, now time.Time) (RetentionResult, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RetentionResult{}, err
	}
	defer tx.Rollback()
	args := []any{TaskClosed, now.UnixMilli(), TurnQueued, TurnRunning, TurnAwaitingApproval}
	eligible := `SELECT task_id FROM tasks WHERE status=? AND retention_deadline<=?
		AND NOT EXISTS (SELECT 1 FROM turns WHERE turns.task_id=tasks.task_id AND turns.status IN (?,?,?))`
	if _, err := tx.ExecContext(ctx, `DELETE FROM outbox WHERE task_id IN (`+eligible+`)`, args...); err != nil {
		return RetentionResult{}, err
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM tasks WHERE task_id IN (`+eligible+`)`, args...)
	if err != nil {
		return RetentionResult{}, err
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		return RetentionResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM workspace_write_epochs WHERE status!='active' AND task_id NOT IN (SELECT task_id FROM tasks)`); err != nil {
		return RetentionResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return RetentionResult{}, err
	}
	return RetentionResult{TasksDeleted: deleted}, nil
}

func normalizeDurableEvent(event agent.Event) agent.Event {
	if event.Message != nil {
		copy := *event.Message
		copy.Text = safeText(copy.Text, maxMessageBytes)
		event.Message = &copy
	}
	if event.Tool != nil {
		copy := *event.Tool
		copy.Name = safeText(copy.Name, maxMetadataValue)
		copy.Status = safeText(copy.Status, maxMetadataValue)
		event.Tool = &copy
	}
	if event.Command != nil {
		copy := *event.Command
		copy.Command = safeText(copy.Command, maxCommandBytes)
		copy.Status = safeText(copy.Status, maxMetadataValue)
		copy.AggregatedOutput = safeText(copy.AggregatedOutput, maxOutputBytes)
		event.Command = &copy
	}
	if event.File != nil {
		copy := *event.File
		copy.Path = safeText(copy.Path, maxPathBytes)
		copy.Operation = safeText(copy.Operation, maxMetadataValue)
		if len(copy.Changes) > 256 {
			copy.Changes = copy.Changes[:256]
		}
		copy.Changes = append([]agent.FileChange(nil), copy.Changes...)
		for i := range copy.Changes {
			copy.Changes[i].Path = safeText(copy.Changes[i].Path, maxPathBytes)
			copy.Changes[i].Operation = safeText(copy.Changes[i].Operation, maxMetadataValue)
		}
		event.File = &copy
	}
	if event.Approval != nil {
		copy := *event.Approval
		copy.Reason = safeText(copy.Reason, maxErrorBytes)
		copy.Command = safeText(copy.Command, maxCommandBytes)
		copy.CWD = safeText(copy.CWD, maxPathBytes)
		copy.GrantRoot = safeText(copy.GrantRoot, maxPathBytes)
		copy.NetworkHost = safeText(copy.NetworkHost, maxMetadataValue)
		copy.NetworkProtocol = safeText(copy.NetworkProtocol, maxMetadataValue)
		event.Approval = &copy
	}
	if event.Result != nil {
		copy := *event.Result
		copy.Status = safeText(copy.Status, maxMetadataValue)
		copy.Error = safeText(copy.Error, maxErrorBytes)
		event.Result = &copy
	}
	if len(event.Metadata) != 0 {
		metadata := make(map[string]string, min(len(event.Metadata), maxMetadataItems))
		keys := make([]string, 0, len(event.Metadata))
		for key := range event.Metadata {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for i, key := range keys {
			if i == maxMetadataItems {
				break
			}
			value := event.Metadata[key]
			key = redact.Truncate(key, maxMetadataKey)
			metadata[key] = redact.Truncate(redact.ValueForKey(key, value), maxMetadataValue)
		}
		event.Metadata = metadata
	}
	return event
}

func safeText(value string, limit int) string {
	return redact.Truncate(redact.String(value), limit)
}

func normalizeApproval(approval Approval) Approval {
	approval.Reason = safeText(approval.Reason, maxErrorBytes)
	approval.Command = safeText(approval.Command, maxCommandBytes)
	approval.CWD = safeText(approval.CWD, maxPathBytes)
	approval.GrantRoot = safeText(approval.GrantRoot, maxPathBytes)
	approval.NetworkHost = safeText(approval.NetworkHost, maxMetadataValue)
	approval.NetworkProtocol = safeText(approval.NetworkProtocol, maxMetadataValue)
	return approval
}

func normalizeVerificationRun(run VerificationRun) VerificationRun {
	run.Summary = safeText(run.Summary, maxErrorBytes)
	run.Trigger = safeText(run.Trigger, maxMetadataValue)
	for checkIndex := range run.Checks {
		check := &run.Checks[checkIndex]
		check.Name = safeText(check.Name, maxMetadataValue)
		check.Kind = safeText(check.Kind, maxMetadataValue)
		check.Command = safeText(check.Command, maxCommandBytes)
		check.CWD = safeText(check.CWD, maxPathBytes)
		check.Output = safeText(check.Output, maxOutputBytes)
		for findingIndex := range check.Findings {
			finding := &check.Findings[findingIndex]
			finding.RuleID = safeText(finding.RuleID, maxMetadataValue)
			finding.Severity = safeText(finding.Severity, maxMetadataValue)
			finding.Category = safeText(finding.Category, maxMetadataValue)
			finding.Scope = safeText(finding.Scope, maxMetadataValue)
			finding.Source = safeText(finding.Source, maxErrorBytes)
			finding.Path = safeText(finding.Path, maxPathBytes)
			finding.Evidence = safeText(finding.Evidence, maxErrorBytes)
			finding.Detail = safeText(finding.Detail, maxErrorBytes)
		}
	}
	return run
}
