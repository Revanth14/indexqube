package agent

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"testing"
)

func TestClassifyExecutionFailureIsConservative(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		resumeLost bool
		want       FailureClass
		eligible   bool
	}{
		{name: "missing executable", err: fmt.Errorf("agent: start: %w", exec.ErrNotFound), want: FailureBackendUnavailable, eligible: true},
		{name: "rate limit", err: errors.New("request failed: rate_limit_exceeded"), want: FailureRateLimited, eligible: true},
		{name: "provider unavailable", err: errors.New("upstream service unavailable"), want: FailureProviderUnavailable, eligible: true},
		{name: "lost session", err: errors.New("opaque provider failure"), resumeLost: true, want: FailureNativeSessionLost, eligible: true},
		{name: "protocol", err: errors.New("agent: decode events: malformed JSON"), want: FailureProtocol},
		{name: "unsupported CLI", err: errors.New("codex backend: unsupported CLI version 1.0.0"), want: FailureProtocol},
		{name: "cancelled", err: context.Canceled, want: FailureCancelled},
		{name: "ambiguous 429", err: errors.New("command 429 failed"), want: FailureUnknown},
		{name: "unknown", err: errors.New("something broke"), want: FailureUnknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := ClassifyExecutionFailure(test.err, test.resumeLost)
			if got != test.want || AutomaticFallbackEligible(got) != test.eligible {
				t.Fatalf("class=%s eligible=%v want class=%s eligible=%v", got, AutomaticFallbackEligible(got), test.want, test.eligible)
			}
		})
	}
}
