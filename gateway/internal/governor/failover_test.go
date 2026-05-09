package governor

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/Revanth14/indexqube/gateway/internal/domain"
)

func TestStream_Failover(t *testing.T) {
	t.Parallel()

	// Primary adapter fails with 429 — must be a *domain.ProviderError for isRetryable.
	primary := &stubAdapter{
		err: &domain.ProviderError{Provider: domain.ProviderAnthropic, StatusCode: http.StatusTooManyRequests, Message: "rate limit exceeded"},
	}
	// Secondary adapter succeeds.
	secondary := &stubAdapter{
		tokens: [][]byte{[]byte("failover-success")},
	}

	g, _ := New(
		WithAdapter(domain.ProviderAnthropic, primary),
		WithAdapter(domain.ProviderBedrock, secondary),
	)

	rec := &recordingWriter{}
	req := newReq(domain.ProviderAnthropic)

	if err := g.Stream(context.Background(), req, rec); err != nil {
		t.Fatalf("Stream should have succeeded via failover: %v", err)
	}

	if primary.calls.Load() != 1 {
		t.Errorf("primary calls=%d, want 1", primary.calls.Load())
	}
	if secondary.calls.Load() != 1 {
		t.Errorf("secondary calls=%d, want 1 (failover occurred)", secondary.calls.Load())
	}
	if len(rec.frames) != 1 || string(rec.frames[0]) != "failover-success" {
		t.Errorf("unexpected frames: %v", rec.frames)
	}
}

func TestStream_Failover_NotRetryable(t *testing.T) {
	t.Parallel()

	// Primary adapter fails with 401 — not retryable.
	primary := &stubAdapter{
		err: &domain.ProviderError{Provider: domain.ProviderAnthropic, StatusCode: http.StatusUnauthorized, Message: "invalid key"},
	}
	secondary := &stubAdapter{}

	g, _ := New(
		WithAdapter(domain.ProviderAnthropic, primary),
		WithAdapter(domain.ProviderBedrock, secondary),
	)

	err := g.Stream(context.Background(), newReq(domain.ProviderAnthropic), &recordingWriter{})
	if err == nil {
		t.Fatal("expected error to propagate, got nil")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Errorf("expected 401 error, got: %v", err)
	}
	if secondary.calls.Load() != 0 {
		t.Error("secondary adapter should not have been called for 401")
	}
}
