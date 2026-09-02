package orchestrator

import (
	"context"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
	"github.com/Revanth14/indexqube/gateway/internal/taskstore"
)

func classifyRouteFailure(code string, err error, result agent.Result) agent.FailureClass {
	switch code {
	case "cancelled":
		return agent.FailureCancelled
	case "workspace_locked":
		return agent.FailureWorkspaceLocked
	case "snapshot_failed", "verification_snapshot_failed", "state_failed", "verification_state_failed":
		return agent.FailurePlatformState
	default:
		return agent.ClassifyExecutionFailure(err, result.ResumeLost)
	}
}

func (s *Service) fallbackEligible(ctx context.Context, taskID string, class agent.FailureClass, mutation bool, preFingerprint, postFingerprint string) bool {
	safe := (taskstore.RouteAttempt{
		FailureClass: class, MutationObserved: mutation,
		PreFingerprint: preFingerprint, PostFingerprint: postFingerprint,
	}).CanAutomaticallyFallback()
	if !safe {
		return false
	}
	_, pinned, err := s.store.BackendPin(ctx, taskID)
	return err == nil && !pinned
}

func failureMetadata(code string, class agent.FailureClass, eligible bool) map[string]string {
	return map[string]string{
		"error_code": code, "failure_class": string(class),
		"fallback_eligible": boolString(eligible),
	}
}

func boolString(value bool) string {
	if value {
		return "true"
	}
	return "false"
}
