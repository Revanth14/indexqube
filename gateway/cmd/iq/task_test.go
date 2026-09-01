package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
	"github.com/Revanth14/indexqube/gateway/internal/taskstore"
)

func TestRunTaskShowRendersAuthoritativeMismatch(t *testing.T) {
	evidence := taskstore.TaskEvidence{
		Task: taskstore.Task{
			ID: "task_1", WorkspacePath: "/repo", OriginalGoal: "rename it", Status: taskstore.TaskNeedsAttention,
			PreferredBackend: agent.BackendCodex, Permission: agent.PermissionWrite,
		},
		Files: []taskstore.FileEvidence{{
			TurnID: "turn_1", Path: "new.go", PreviousPath: "old.go", Operation: "renamed", Source: "workspace",
		}},
		ReportedFiles: []taskstore.FileEvidence{{
			TurnID: "turn_1", Path: "old.go", Operation: "changed", Source: "agent",
		}},
		EvidenceMismatch: true,
	}
	var out bytes.Buffer
	renderTaskEvidence(&out, evidence)
	for _, want := range []string{
		"Files changed (workspace-authoritative):", "renamed new.go (from old.go)",
		"Attention: agent file events do not match", "Agent-reported files:",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("output missing %q:\n%s", want, out.String())
		}
	}
}

func TestTaskShowRendersDurableVerification(t *testing.T) {
	exit := 1
	evidence := taskstore.TaskEvidence{
		Task: taskstore.Task{ID: "task_verify", Status: taskstore.TaskNeedsAttention},
		VerificationRuns: []taskstore.VerificationRun{{
			ID: "verify_1", Status: taskstore.VerificationFailed, Summary: "1 of 1 verification check(s) failed",
			Checks: []taskstore.VerificationCheck{{
				Name: "Go tests", Command: "go test -mod=readonly ./...", CWD: "gateway",
				Status: taskstore.VerificationCheckFailed, ExitCode: &exit, Output: "FAIL example.com/package",
				Findings: []taskstore.VerificationFinding{{
					RuleID: "code.shell_injection", Severity: "high", Path: "server.js", Line: 8,
					Evidence: "child_process.exec(",
				}},
			}},
		}},
	}
	var out bytes.Buffer
	renderTaskEvidence(&out, evidence)
	for _, want := range []string{
		"Verification:", "verification_failed", "[failed exit=1] Go tests — go test -mod=readonly ./... (cwd gateway)",
		"FAIL example.com/package",
		"high code.shell_injection at server.js:8: child_process.exec(",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("output missing %q:\n%s", want, out.String())
		}
	}
}
