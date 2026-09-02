package agent

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
)

// FailureClass is the bounded backend-neutral reason a route attempt failed.
// Only the explicit allowlist in AutomaticFallbackEligible may be retried on
// another backend, and the orchestrator must separately prove no mutation.
type FailureClass string

const (
	FailureUnknown             FailureClass = "unknown_backend_error"
	FailureBackendUnavailable  FailureClass = "backend_unavailable"
	FailureRateLimited         FailureClass = "rate_limited"
	FailureProviderUnavailable FailureClass = "provider_unavailable"
	FailureNativeSessionLost   FailureClass = "native_session_lost"
	FailureProtocol            FailureClass = "protocol_error"
	FailurePlatformState       FailureClass = "platform_state_error"
	FailureWorkspaceLocked     FailureClass = "workspace_locked"
	FailureCancelled           FailureClass = "cancelled"
	FailureDaemonInterrupted   FailureClass = "daemon_interrupted"
)

func AutomaticFallbackEligible(class FailureClass) bool {
	switch class {
	case FailureBackendUnavailable, FailureRateLimited, FailureProviderUnavailable, FailureNativeSessionLost:
		return true
	default:
		return false
	}
}

// ClassifyExecutionFailure recognizes only stable process conditions and a
// deliberately small set of provider phrases. Everything else fails closed as
// unknown. Classification alone never authorizes fallback.
func ClassifyExecutionFailure(err error, resumeLost bool) FailureClass {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return FailureCancelled
	}
	if resumeLost {
		return FailureNativeSessionLost
	}
	if errors.Is(err, exec.ErrNotFound) || os.IsNotExist(err) {
		return FailureBackendUnavailable
	}
	text := strings.ToLower(strings.TrimSpace(errorText(err)))
	if text == "" {
		return FailureUnknown
	}
	if strings.Contains(text, "executable is not configured") || strings.Contains(text, "executable file not found") {
		return FailureBackendUnavailable
	}
	for _, marker := range []string{
		"rate_limit_exceeded", "rate limit exceeded", "too many requests", "http status 429", "status code 429",
	} {
		if strings.Contains(text, marker) {
			return FailureRateLimited
		}
	}
	for _, marker := range []string{
		"overloaded_error", "service unavailable", "temporarily unavailable", "provider unavailable",
		"http status 529", "status code 529",
	} {
		if strings.Contains(text, marker) {
			return FailureProviderUnavailable
		}
	}
	for _, marker := range []string{
		"decode events", "decode jsonl", "decode stream", "interactive protocol", "stream ended without a result",
		"unsupported cli version",
	} {
		if strings.Contains(text, marker) {
			return FailureProtocol
		}
	}
	return FailureUnknown
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
