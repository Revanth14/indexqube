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
