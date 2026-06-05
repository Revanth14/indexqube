package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/domain"
)

type timeoutCheckGovernor struct {
	deadline time.Time
}

func (g *timeoutCheckGovernor) Stream(ctx context.Context, _ *domain.InferenceRequest, _ domain.TokenWriter) error {
	if d, ok := ctx.Deadline(); ok {
		g.deadline = d
	}
	return nil
}

func (g *timeoutCheckGovernor) Optimize(ctx context.Context, tenant string, messages []domain.Message, projectMemory string) ([]domain.Message, domain.PruneStats, error) {
	return messages, domain.PruneStats{}, nil
}

func (g *timeoutCheckGovernor) Diagnostics(ctx context.Context) (domain.Diagnostics, error) {
	return domain.Diagnostics{}, nil
}

func (g *timeoutCheckGovernor) Ready(ctx context.Context) error {
	return nil
}

func TestStreamTimeoutSetsDeadline(t *testing.T) {
	gov := &timeoutCheckGovernor{}
	p := New(gov, WithStreamTimeout(50*time.Millisecond))
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	rec := httptest.NewRecorder()
	p.streamThroughGovernor(rec, req, &domain.InferenceRequest{Model: "claude-3", Messages: []domain.Message{{Role: "user"}}, Stream: true})

	if gov.deadline.IsZero() {
		t.Fatal("expected governor context to have a deadline")
	}
	if time.Until(gov.deadline) > time.Second {
		t.Fatalf("deadline too far in the future: %v", time.Until(gov.deadline))
	}
}

func TestStreamTimeoutDisabledByDefault(t *testing.T) {
	gov := &timeoutCheckGovernor{}
	p := New(gov) // default streamTimeout is 0 (disabled)
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	rec := httptest.NewRecorder()
	p.streamThroughGovernor(rec, req, &domain.InferenceRequest{Model: "claude-3", Messages: []domain.Message{{Role: "user"}}, Stream: true})

	if !gov.deadline.IsZero() {
		t.Fatal("expected no deadline when stream timeout disabled by default")
	}
}
