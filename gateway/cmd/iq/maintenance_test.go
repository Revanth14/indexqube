package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/taskstore"
)

func TestRunBackupCommandCreatesConsistentOwnerOnlySnapshot(t *testing.T) {
	home := filepath.Join(t.TempDir(), ".indexqube")
	t.Setenv("INDEXQUBE_HOME", home)
	destination := filepath.Join(t.TempDir(), "snapshot.db")
	var out, errOut bytes.Buffer
	if err := runBackupCommand(context.Background(), []string{"--output", destination}, &out, &errOut); err != nil {
		t.Fatalf("backup: %v (%s)", err, errOut.String())
	}
	if !strings.Contains(out.String(), destination) {
		t.Fatalf("output=%q", out.String())
	}
	info, err := os.Stat(destination)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("info=%v err=%v", info, err)
	}
	diagnostics, err := taskstore.Diagnose(context.Background(), destination)
	if err != nil || diagnostics.Integrity != "ok" || diagnostics.SchemaVersion != taskstore.CurrentSchemaVersion {
		t.Fatalf("diagnostics=%+v err=%v", diagnostics, err)
	}
	if err := runBackupCommand(context.Background(), []string{"--output", destination}, &out, &errOut); err == nil {
		t.Fatal("existing backup destination was overwritten")
	}
}

func TestMaintainDaemonLogsBoundsAgeCountAndSize(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	for i := 0; i < maxDaemonLogs+3; i++ {
		path := filepath.Join(dir, "daemon-20260902-"+twoDigits(i)+".log")
		if err := os.WriteFile(path, []byte("log\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		stamp := now.Add(-time.Duration(i) * time.Hour)
		if err := os.Chtimes(path, stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}
	large := filepath.Join(dir, "daemon-20260903-000000.log")
	f, err := os.OpenFile(large, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(maxDaemonLogBytes + 4096); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := os.Chtimes(large, now.Add(time.Minute), now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	old := filepath.Join(dir, "daemon-20200101-000000.log")
	if err := os.WriteFile(old, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	oldStamp := now.Add(-maxDaemonLogAge - time.Hour)
	if err := os.Chtimes(old, oldStamp, oldStamp); err != nil {
		t.Fatal(err)
	}

	if err := maintainDaemonLogs(dir, now); err != nil {
		t.Fatal(err)
	}
	logs, err := filepath.Glob(filepath.Join(dir, "daemon-*.log"))
	if err != nil || len(logs) > maxDaemonLogs-1 {
		t.Fatalf("logs=%d err=%v", len(logs), err)
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatalf("old log still exists: %v", err)
	}
	info, err := os.Stat(large)
	if err != nil || info.Size() > maxDaemonLogBytes {
		t.Fatalf("large log size=%v err=%v", info, err)
	}
}

func TestDoctorReportsDatabaseIntegrityAndTelemetryDefault(t *testing.T) {
	home := filepath.Join(t.TempDir(), ".indexqube")
	t.Setenv("INDEXQUBE_HOME", home)
	store, err := taskstore.Open(filepath.Join(home, "tasks.db"))
	if err != nil {
		t.Fatal(err)
	}
	store.Close()
	var out bytes.Buffer
	writeDoctor(&out)
	got := out.String()
	for _, want := range []string{"task database: ok", "schema 2", "telemetry: disabled (default)"} {
		if !strings.Contains(got, want) {
			t.Fatalf("doctor missing %q:\n%s", want, got)
		}
	}
}

func TestReliabilityEventContainsOnlyAggregateMetrics(t *testing.T) {
	t.Setenv("INDEXQUBE_HOME", t.TempDir())
	metrics := taskstore.ReliabilityMetrics{
		GeneratedAt: time.Now().UTC(), TasksTotal: 10, TurnsTotal: 12, TurnsSucceeded: 8,
		SuccessfulLatency: taskstore.DurationStats{P50MS: 100, P95MS: 900}, Handoffs: 2,
		VerificationOutcomes: map[string]int64{string(taskstore.VerificationPassed): 7},
	}
	event := reliabilityEvent(metrics)
	if event.TasksTotal != 10 || event.SuccessfulLatencyP95MS != 900 || event.VerificationsPassed != 7 || event.OSArch == "" {
		t.Fatalf("event=%+v", event)
	}
	raw, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"task_id", "workspace", "prompt", "command", "output"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("event exposed forbidden field %q: %s", forbidden, raw)
		}
	}
}

func twoDigits(value int) string {
	return string([]byte{'0' + byte(value/10), '0' + byte(value%10)})
}
