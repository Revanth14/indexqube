package taskstore

import (
	"context"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
)

func (s *Store) ProcessStarted(ctx context.Context, process agent.ProcessInfo) error {
	if process.StartedAt.IsZero() {
		process.StartedAt = time.Now().UTC()
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO backend_processes
		(pid,process_token,task_id,turn_id,executable,started_at) VALUES(?,?,?,?,?,?)
		ON CONFLICT(pid) DO UPDATE SET process_token=excluded.process_token,task_id=excluded.task_id,
		turn_id=excluded.turn_id,executable=excluded.executable,started_at=excluded.started_at`,
		process.PID, process.Token, process.TaskID, process.TurnID, process.Executable, process.StartedAt.UnixMilli())
	return err
}

func (s *Store) ProcessExited(ctx context.Context, pid int) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM backend_processes WHERE pid=?`, pid)
	return err
}

func (s *Store) BackendProcesses(ctx context.Context) ([]agent.ProcessInfo, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT pid,process_token,task_id,turn_id,executable,started_at FROM backend_processes ORDER BY started_at,pid`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var processes []agent.ProcessInfo
	for rows.Next() {
		var process agent.ProcessInfo
		var started int64
		if err := rows.Scan(&process.PID, &process.Token, &process.TaskID, &process.TurnID, &process.Executable, &started); err != nil {
			return nil, err
		}
		process.StartedAt = time.UnixMilli(started).UTC()
		processes = append(processes, process)
	}
	return processes, rows.Err()
}
