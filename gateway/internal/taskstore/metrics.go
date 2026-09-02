package taskstore

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"time"
)

type DurationStats struct {
	Count     int64 `json:"count"`
	AverageMS int64 `json:"average_ms"`
	P50MS     int64 `json:"p50_ms"`
	P95MS     int64 `json:"p95_ms"`
	MaxMS     int64 `json:"max_ms"`
}

type ReliabilityMetrics struct {
	GeneratedAt                 time.Time        `json:"generated_at"`
	TasksTotal                  int64            `json:"tasks_total"`
	TurnsTotal                  int64            `json:"turns_total"`
	TurnsSucceeded              int64            `json:"turns_succeeded"`
	TurnsFailed                 int64            `json:"turns_failed"`
	TurnsCancelled              int64            `json:"turns_cancelled"`
	TerminalLatency             DurationStats    `json:"terminal_latency"`
	SuccessfulLatency           DurationStats    `json:"successful_latency"`
	Handoffs                    int64            `json:"handoffs"`
	AutomaticFallbacks          int64            `json:"automatic_fallbacks"`
	VerificationOutcomes        map[string]int64 `json:"verification_outcomes"`
	CrashRecoveries             int64            `json:"crash_recoveries"`
	CrashRecoveriesAttention    int64            `json:"crash_recoveries_needing_attention"`
	VerifiedWithoutManualSwitch int64            `json:"verified_without_manual_switch"`
}

// ClaimReliabilityTelemetry durably rate-limits reporting across daemon
// restarts. The claim is written before a best-effort network attempt.
func (s *Store) ClaimReliabilityTelemetry(ctx context.Context, now time.Time, interval time.Duration) (bool, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if interval <= 0 {
		interval = 24 * time.Hour
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var last int64
	err = tx.QueryRowContext(ctx, `SELECT updated_at FROM maintenance_state WHERE key='reliability_telemetry'`).Scan(&last)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	if err == nil && now.Sub(time.UnixMilli(last)) < interval {
		return false, tx.Commit()
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO maintenance_state(key,value,updated_at) VALUES('reliability_telemetry','claimed',?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at`, now.UnixMilli()); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// ReliabilityMetrics derives aggregate product outcomes from canonical state.
// It intentionally returns no task IDs, prompts, paths, commands, or output.
func (s *Store) ReliabilityMetrics(ctx context.Context, now time.Time) (ReliabilityMetrics, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	metrics := ReliabilityMetrics{GeneratedAt: now, VerificationOutcomes: make(map[string]int64)}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM tasks`).Scan(&metrics.TasksTotal); err != nil {
		return ReliabilityMetrics{}, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT status,created_at,completed_at,error_code FROM turns ORDER BY created_at`)
	if err != nil {
		return ReliabilityMetrics{}, err
	}
	var terminalDurations, successfulDurations []int64
	for rows.Next() {
		var status TurnStatus
		var created int64
		var completed *int64
		var errorCode string
		if err := rows.Scan(&status, &created, &completed, &errorCode); err != nil {
			rows.Close()
			return ReliabilityMetrics{}, err
		}
		metrics.TurnsTotal++
		switch status {
		case TurnSucceeded:
			metrics.TurnsSucceeded++
		case TurnFailed:
			metrics.TurnsFailed++
		case TurnCancelled:
			metrics.TurnsCancelled++
		}
		if completed != nil {
			duration := *completed - created
			if duration < 0 {
				duration = 0
			}
			terminalDurations = append(terminalDurations, duration)
			if status == TurnSucceeded {
				successfulDurations = append(successfulDurations, duration)
			}
		}
		if len(errorCode) >= len("daemon_interrupted_") && errorCode[:len("daemon_interrupted_")] == "daemon_interrupted_" {
			metrics.CrashRecoveries++
			if errorCode == "daemon_interrupted_write" {
				metrics.CrashRecoveriesAttention++
			}
		}
	}
	if err := rows.Close(); err != nil {
		return ReliabilityMetrics{}, err
	}
	metrics.TerminalLatency = durationStats(terminalDurations)
	metrics.SuccessfulLatency = durationStats(successfulDurations)
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM task_handoffs`).Scan(&metrics.Handoffs); err != nil {
		return ReliabilityMetrics{}, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM route_attempts WHERE decision_reason='automatic_fallback_v1'`).Scan(&metrics.AutomaticFallbacks); err != nil {
		return ReliabilityMetrics{}, err
	}
	verificationRows, err := s.db.QueryContext(ctx, `SELECT status,COUNT(*) FROM verification_runs GROUP BY status ORDER BY status`)
	if err != nil {
		return ReliabilityMetrics{}, err
	}
	for verificationRows.Next() {
		var status string
		var count int64
		if err := verificationRows.Scan(&status, &count); err != nil {
			verificationRows.Close()
			return ReliabilityMetrics{}, err
		}
		metrics.VerificationOutcomes[status] = count
	}
	if err := verificationRows.Close(); err != nil {
		return ReliabilityMetrics{}, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(DISTINCT turns.task_id)
		FROM turns JOIN verification_runs ON verification_runs.turn_id=turns.turn_id
		WHERE turns.status=? AND verification_runs.status=?
		AND NOT EXISTS (SELECT 1 FROM task_handoffs WHERE task_handoffs.task_id=turns.task_id)`,
		TurnSucceeded, VerificationPassed).Scan(&metrics.VerifiedWithoutManualSwitch); err != nil {
		return ReliabilityMetrics{}, err
	}
	return metrics, nil
}

func durationStats(values []int64) DurationStats {
	if len(values) == 0 {
		return DurationStats{}
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	var sum int64
	for _, value := range values {
		sum += value
	}
	return DurationStats{
		Count: int64(len(values)), AverageMS: sum / int64(len(values)),
		P50MS: percentile(values, 50), P95MS: percentile(values, 95), MaxMS: values[len(values)-1],
	}
}

func percentile(sorted []int64, percent int) int64 {
	if len(sorted) == 0 {
		return 0
	}
	index := (percent*len(sorted) + 99) / 100
	if index < 1 {
		index = 1
	}
	return sorted[index-1]
}
