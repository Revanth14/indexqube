package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
	claudebackend "github.com/Revanth14/indexqube/gateway/internal/agent/claude"
	codexbackend "github.com/Revanth14/indexqube/gateway/internal/agent/codex"
	"github.com/Revanth14/indexqube/gateway/internal/localstate"
	"github.com/Revanth14/indexqube/gateway/internal/taskstore"
	"github.com/Revanth14/indexqube/gateway/internal/telemetry"
)

func runBackup(args []string) {
	if err := runBackupCommand(context.Background(), args, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "iq: backup failed: %v\n", err)
		os.Exit(1)
	}
}

func runBackupCommand(ctx context.Context, args []string, out, errOut io.Writer) error {
	fs := flag.NewFlagSet("backup", flag.ContinueOnError)
	fs.SetOutput(errOut)
	output := fs.String("output", "", "backup destination (must not already exist)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("usage: iq backup [--output PATH]")
	}
	home, err := indexQubeHome()
	if err != nil {
		return err
	}
	destination := strings.TrimSpace(*output)
	if destination == "" {
		destination = filepath.Join(home, "backups", "tasks-"+time.Now().UTC().Format("20060102T150405.000000000Z")+".db")
	} else if destination, err = filepath.Abs(destination); err != nil {
		return err
	}
	store, err := taskstore.Open(filepath.Join(home, "tasks.db"))
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.Backup(ctx, destination); err != nil {
		return err
	}
	fmt.Fprintf(out, "Task database backup: %s\n", destination)
	return nil
}

func runRetentionLoop(ctx context.Context, store *taskstore.Store, errOut io.Writer) {
	ticker := time.NewTicker(6 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			result, err := store.ApplyRetention(ctx, now.UTC())
			if err != nil && !errors.Is(err, context.Canceled) {
				fmt.Fprintf(errOut, "indexqube: retention failed: %v\n", err)
			} else if result.TasksDeleted > 0 {
				fmt.Fprintf(errOut, "indexqube: retention removed %d expired closed task(s)\n", result.TasksDeleted)
			}
		}
	}
}

func runReliabilityTelemetryLoop(ctx context.Context, store *taskstore.Store) {
	if !telemetry.Enabled() {
		return
	}
	endpoint := strings.TrimSpace(os.Getenv("IQ_TELEMETRY_ENDPOINT"))
	if endpoint == "" {
		endpoint = telemetryEndpoint
	}
	client := telemetry.NewGatewayClient(endpoint)
	publish := func(now time.Time) {
		claimed, err := store.ClaimReliabilityTelemetry(ctx, now.UTC(), 24*time.Hour)
		if err != nil || !claimed {
			return
		}
		metrics, err := store.ReliabilityMetrics(ctx, now.UTC())
		if err != nil {
			return
		}
		client.TrackReliability(reliabilityEvent(metrics))
	}
	publish(time.Now())
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			publish(now)
		}
	}
}

func reliabilityEvent(metrics taskstore.ReliabilityMetrics) telemetry.ReliabilityEvent {
	event := telemetry.NewReliabilityEvent(version)
	event.GeneratedAt = metrics.GeneratedAt
	event.TasksTotal = metrics.TasksTotal
	event.TurnsTotal = metrics.TurnsTotal
	event.TurnsSucceeded = metrics.TurnsSucceeded
	event.TurnsFailed = metrics.TurnsFailed
	event.TurnsCancelled = metrics.TurnsCancelled
	event.SuccessfulLatencyP50MS = metrics.SuccessfulLatency.P50MS
	event.SuccessfulLatencyP95MS = metrics.SuccessfulLatency.P95MS
	event.Handoffs = metrics.Handoffs
	event.AutomaticFallbacks = metrics.AutomaticFallbacks
	event.VerificationsPassed = metrics.VerificationOutcomes[string(taskstore.VerificationPassed)]
	event.VerificationsWarnings = metrics.VerificationOutcomes[string(taskstore.VerificationWarnings)]
	event.VerificationsFailed = metrics.VerificationOutcomes[string(taskstore.VerificationFailed)]
	event.VerificationsSkipped = metrics.VerificationOutcomes[string(taskstore.VerificationSkipped)]
	event.CrashRecoveries = metrics.CrashRecoveries
	event.CrashRecoveriesAttention = metrics.CrashRecoveriesAttention
	event.VerifiedWithoutSwitch = metrics.VerifiedWithoutManualSwitch
	return event
}

func cleanupRecordedProcesses(ctx context.Context, store *taskstore.Store, errOut io.Writer) (int, error) {
	processes, err := store.BackendProcesses(ctx)
	if err != nil {
		return 0, err
	}
	cleaned := 0
	for _, process := range processes {
		terminated, err := agent.TerminateRecordedProcess(process, 500*time.Millisecond)
		if err != nil {
			fmt.Fprintf(errOut, "indexqube: orphan process %d requires manual cleanup: %v\n", process.PID, err)
			continue
		}
		// A nonmatching live PID is PID reuse, and a missing PID is already
		// gone. Both records are stale and safe to forget.
		if err := store.ProcessExited(ctx, process.PID); err != nil {
			return cleaned, err
		}
		if terminated {
			cleaned++
		}
	}
	return cleaned, nil
}

func writeDoctor(w io.Writer) {
	fmt.Fprintln(w, "IndexQube doctor")
	fmt.Fprintln(w, "---------------")
	home, homeErr := localstate.Dir()
	if homeErr != nil {
		fmt.Fprintf(w, "state directory: error (%v)\n", homeErr)
	} else if info, err := os.Lstat(home); errors.Is(err, os.ErrNotExist) {
		fmt.Fprintf(w, "state directory: not created (%s)\n", home)
	} else if err != nil {
		fmt.Fprintf(w, "state directory: error (%v)\n", err)
	} else if !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		fmt.Fprintf(w, "state directory: unsafe permissions (%s %04o)\n", home, info.Mode().Perm())
	} else {
		fmt.Fprintf(w, "state directory: ok (%s)\n", home)
	}

	st, stateErr := readDaemonState()
	addr := defaultDaemonAddr
	if stateErr == nil && st.Addr != "" {
		addr = normalizeDaemonAddr(st.Addr)
	}
	if isDaemonHealthy(addr) {
		fmt.Fprintf(w, "daemon: ok (%s)\n", daemonURL(addr))
		controlAddr := st.ControlAddr
		if controlAddr == "" {
			controlAddr = defaultControlAddr
		}
		if isControlHealthy(controlAddr) {
			fmt.Fprintf(w, "control API: ok (%s)\n", daemonURL(controlAddr))
		} else {
			fmt.Fprintf(w, "control API: unavailable or credential invalid (%s)\n", daemonURL(controlAddr))
		}
	} else if stateErr == nil {
		fmt.Fprintf(w, "daemon: stopped with stale state (pid %d)\n", st.PID)
	} else {
		fmt.Fprintf(w, "daemon: not running (%s)\n", daemonURL(addr))
	}

	if homeErr == nil {
		dbPath := filepath.Join(home, "tasks.db")
		if _, err := os.Stat(dbPath); errors.Is(err, os.ErrNotExist) {
			fmt.Fprintln(w, "task database: not created")
		} else if err != nil {
			fmt.Fprintf(w, "task database: error (%v)\n", err)
		} else if diagnostics, err := taskstore.Diagnose(context.Background(), dbPath); err != nil {
			fmt.Fprintf(w, "task database: corrupt or unreadable (%v)\n", err)
		} else if diagnostics.SchemaVersion > taskstore.CurrentSchemaVersion {
			fmt.Fprintf(w, "task database: newer schema %d (binary supports %d)\n", diagnostics.SchemaVersion, taskstore.CurrentSchemaVersion)
		} else {
			fmt.Fprintf(w, "task database: ok (schema %d, integrity %s)\n", diagnostics.SchemaVersion, diagnostics.Integrity)
		}
		backups, _ := filepath.Glob(filepath.Join(home, "tasks.db.backup-v*"))
		manual, _ := filepath.Glob(filepath.Join(home, "backups", "tasks-*.db"))
		fmt.Fprintf(w, "task backups: %d\n", len(backups)+len(manual))
		writeOwnerOnlyPathCheck(w, "workspace locks", filepath.Join(home, "locks"), true)
		writeOwnerOnlyPathCheck(w, "daemon logs", filepath.Join(home, "logs"), true)
		writeOwnerOnlyPathCheck(w, "control credential", filepath.Join(home, controlCredentialFile), false)
	}

	writeBackendHealth(w, "codex")
	writeBackendHealth(w, "claude")
	if codexConfigHasIndexQube() {
		fmt.Fprintln(w, "codex setup: configured")
	} else {
		fmt.Fprintln(w, "codex setup: not configured (optional for task UI)")
	}
	if claudeShellHasIndexQube() {
		fmt.Fprintln(w, "claude setup: configured")
	} else {
		fmt.Fprintln(w, "claude setup: not configured (optional for task UI)")
	}
	telemetrySetting := strings.ToLower(strings.TrimSpace(os.Getenv("IQ_TELEMETRY")))
	if !telemetry.Enabled() {
		fmt.Fprintln(w, "telemetry: disabled (default)")
	} else {
		fmt.Fprintf(w, "telemetry: explicitly enabled (%s)\n", telemetrySetting)
	}
}

func writeBackendHealth(w io.Writer, name string) {
	path, err := exec.LookPath(name)
	if err != nil {
		fmt.Fprintf(w, "%s backend: unavailable (CLI not found)\n", name)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	runner := agent.NewRunner()
	var health agent.BackendHealth
	switch name {
	case "codex":
		health = codexbackend.New(runner, path).Probe(ctx)
	case "claude":
		health = claudebackend.New(runner, path).Probe(ctx)
	}
	if health.Version != "" {
		fmt.Fprintf(w, "%s backend: %s (%s", name, health.Status, health.Version)
	} else {
		fmt.Fprintf(w, "%s backend: %s", name, health.Status)
	}
	if health.Version != "" {
		if health.Reason != "" {
			fmt.Fprintf(w, ", %s", health.Reason)
		}
		fmt.Fprintln(w, ")")
	} else if health.Reason != "" {
		fmt.Fprintf(w, " (%s)\n", health.Reason)
	} else {
		fmt.Fprintln(w)
	}
}

func writeOwnerOnlyPathCheck(w io.Writer, label, path string, directory bool) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		fmt.Fprintf(w, "%s: not created\n", label)
		return
	}
	if err != nil {
		fmt.Fprintf(w, "%s: error (%v)\n", label, err)
		return
	}
	typeOK := info.Mode().IsRegular()
	if directory {
		typeOK = info.IsDir()
	}
	if !typeOK || info.Mode().Perm()&0o077 != 0 {
		fmt.Fprintf(w, "%s: unsafe type or permissions (%04o)\n", label, info.Mode().Perm())
		return
	}
	fmt.Fprintln(w, label+": ok")
}

func maintainDaemonLogs(logDir string, now time.Time) error {
	entries, err := filepath.Glob(filepath.Join(logDir, "daemon-*.log"))
	if err != nil {
		return err
	}
	type logEntry struct {
		path string
		info os.FileInfo
	}
	logs := make([]logEntry, 0, len(entries))
	for _, path := range entries {
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			continue
		}
		logs = append(logs, logEntry{path: path, info: info})
	}
	sort.Slice(logs, func(i, j int) bool { return logs[i].info.ModTime().After(logs[j].info.ModTime()) })
	for index, entry := range logs {
		// Reserve one slot for the log startDaemon is about to create.
		if index >= maxDaemonLogs-1 || now.Sub(entry.info.ModTime()) > maxDaemonLogAge {
			if err := os.Remove(entry.path); err != nil {
				return err
			}
			continue
		}
		if entry.info.Size() > maxDaemonLogBytes {
			if err := truncateLogTail(entry.path, maxDaemonLogBytes); err != nil {
				return err
			}
		}
	}
	return nil
}

func truncateLogTail(path string, maxBytes int64) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.Size() <= maxBytes {
		return err
	}
	if _, err := f.Seek(-maxBytes, io.SeekEnd); err != nil {
		return err
	}
	raw, err := io.ReadAll(io.LimitReader(f, maxBytes))
	if err != nil {
		return err
	}
	marker := []byte("[older daemon log content rotated]\n")
	if len(raw) > len(marker) {
		raw = append(marker, raw[len(marker):]...)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".log-rotate-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}
