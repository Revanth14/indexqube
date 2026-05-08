// Package proxy is the HTTP entrypoint for the IndexQube Gateway.
//
// It is intentionally thin: it parses HTTP, extracts BYO-Key credentials,
// frames Server-Sent Events, and hands canonical InferenceRequests off to
// a Governor. It does not select providers, translate prompts, cache,
// retry, or authenticate -- those concerns live in sibling packages.
package proxy

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/Revanth14/indexqube/gateway/internal/domain"
	"github.com/Revanth14/indexqube/gateway/internal/telemetry"
)

// Governor is the upstream contract the proxy depends on.
type Governor interface {
	Stream(ctx context.Context, req *domain.InferenceRequest, tw domain.TokenWriter) error
	Optimize(ctx context.Context, tenant string, messages []domain.Message, projectMemory string) ([]domain.Message, domain.PruneStats, error)
	Diagnostics(ctx context.Context) (domain.Diagnostics, error)
	Ready(ctx context.Context) error
}

// Proxy is the HTTP-facing component. Construct via New.
type Proxy struct {
	governor       Governor
	logger         *slog.Logger
	mux            *http.ServeMux
	maxRequestSize int64
	metrics        *telemetry.Metrics
}

// Option configures a Proxy at construction time.
type Option func(*Proxy)

// WithLogger overrides the default slog.Default() logger.
func WithLogger(l *slog.Logger) Option {
	return func(p *Proxy) {
		if l != nil {
			p.logger = l
		}
	}
}

// WithMaxRequestSize overrides the default inbound request size limit (8 MiB).
func WithMaxRequestSize(size int64) Option {
	return func(p *Proxy) {
		if size > 0 {
			p.maxRequestSize = size
		}
	}
}

// WithMetrics wires Prometheus metrics into the proxy for per-request instrumentation.
func WithMetrics(m *telemetry.Metrics) Option {
	return func(p *Proxy) {
		if m != nil {
			p.metrics = m
		}
	}
}

// New returns a wired Proxy. A nil governor is a programmer error and
// panics fast at boot rather than 5xx-ing every request in production.
func New(gov Governor, opts ...Option) *Proxy {
	if gov == nil {
		panic("proxy: governor is required")
	}
	p := &Proxy{
		governor:       gov,
		logger:         slog.Default(),
		mux:            http.NewServeMux(),
		maxRequestSize: 8 << 20, // default 8 MiB
	}
	for _, opt := range opts {
		opt(p)
	}
	p.registerRoutes()
	return p
}

// Handler returns the http.Handler to mount on the server.
func (p *Proxy) Handler() http.Handler {
	return p.mux
}

// Mux returns the underlying *http.ServeMux. Useful for middleware that
// needs to introspect the registered patterns (e.g. RouteResolver
// stamps the matched pattern onto the request context for metric labels).
func (p *Proxy) Mux() *http.ServeMux {
	return p.mux
}

func (p *Proxy) registerRoutes() {
	p.mux.HandleFunc("GET /healthz", p.handleHealth)
	p.mux.HandleFunc("GET /readyz", p.handleReady)
	p.mux.HandleFunc("GET /v1/diagnostics", p.handleDiagnostics)
	p.mux.HandleFunc("GET /v1/models", p.handleModels)
	p.mux.HandleFunc("POST /v1/chat/completions", p.handleChatCompletions)
	p.mux.HandleFunc("POST /v1/optimize", p.handleOptimize)
}
