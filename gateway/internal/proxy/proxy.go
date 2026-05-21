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
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/domain"
	"github.com/Revanth14/indexqube/gateway/internal/memory"
	"github.com/Revanth14/indexqube/gateway/internal/sessions"
	"github.com/Revanth14/indexqube/gateway/internal/telemetry"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
)

// Governor is the upstream contract the proxy depends on.
type Governor interface {
	Stream(ctx context.Context, req *domain.InferenceRequest, tw domain.TokenWriter) error
	Optimize(ctx context.Context, tenant string, messages []domain.Message, projectMemory string) ([]domain.Message, domain.PruneStats, error)
	Diagnostics(ctx context.Context) (domain.Diagnostics, error)
	Ready(ctx context.Context) error
}

// Version is stamped at build time via -ldflags "-X .../proxy.Version=v0.1.0".
var Version = "dev"

// Proxy is the HTTP-facing component. Construct via New.
type Proxy struct {
	governor        Governor
	logger          *slog.Logger
	mux             *http.ServeMux
	maxRequestSize  int64
	optimizeTimeout time.Duration
	metrics         *telemetry.Metrics
	claude          ClaudeMessagesConfig
	usageTracker    telemetry.Sink
	sessionTracker  *telemetry.AgentSessionStore
	sessionPersist  *sessions.Tracker
	supabaseStats   *StatsHandler
}

// BedrockConfig routes /v1/messages to AWS Bedrock instead of Anthropic's API.
// When Enabled is true the gateway signs requests with SigV4 and maps Claude
// model names to their Bedrock equivalents.
type BedrockConfig struct {
	Enabled       bool
	Region        string
	ModelPrefix   string // "us." for cross-region inference, "" for single-region
	ModelOverride string // force a specific Bedrock model ID for all requests
	Client        *bedrockruntime.Client
	Models        []ModelEntry // populated at startup from ListFoundationModels
}

// OptimizerConfig tunes the class-aware block optimizer. Zero values enable
// safe defaults via claudeDefaults().
type OptimizerConfig struct {
	MinSpanBytes            int  // minimum span size to consider for pruning (default 512)
	TargetChunkBytes        int  // target chunk size for future long-block strategy (default 2048)
	MaxChunkBytes           int  // maximum chunk size (default 8192)
	MinSavedTokens          int  // skip rewrite if savings below this threshold (default 10)
	EnableToolResultPruning bool // prune old tool_result spans (default true via claudeDefaults)
	EnableAssistantPruning  bool // prune old assistant text spans (default false)
	EnableSystemPruning     bool // deprecated; system text spans are never pruned
	Diagnostics             bool // emit verbose per-class diagnostics in logs
}

type ClaudeMessagesConfig struct {
	Mode                 string
	DevToken             string
	AnthropicAPIKey      string
	AnthropicBaseURL     string
	AnthropicVersion     string
	EnableLogPruner      bool
	EnableBlockOptimizer bool
	Optimizer            OptimizerConfig
	Bedrock              BedrockConfig
	SessionStore         *memory.Store
	HTTPClient           *http.Client
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

// WithOptimizeTimeout caps the duration of /v1/optimize requests. Streaming
// requests are unaffected. A non-positive value disables the cap.
func WithOptimizeTimeout(d time.Duration) Option {
	return func(p *Proxy) {
		if d > 0 {
			p.optimizeTimeout = d
		}
	}
}

func WithClaudeMessages(cfg ClaudeMessagesConfig) Option {
	return func(p *Proxy) {
		p.claude = cfg
	}
}

// WithUsageTracker wires a telemetry sink for per-request stats.
// Nil disables telemetry silently.
func WithUsageTracker(c telemetry.Sink) Option {
	return func(p *Proxy) {
		p.usageTracker = c
	}
}

// WithAgentSessionStore wires the in-memory agent session tracker.
// Nil disables session aggregation silently.
func WithAgentSessionStore(s *telemetry.AgentSessionStore) Option {
	return func(p *Proxy) {
		p.sessionTracker = s
	}
}

// WithSessionPersist wires the SQLite-backed session tracker.
// When set, every request outcome is also persisted to the local database
// so session data survives process restart.
func WithSessionPersist(t *sessions.Tracker) Option {
	return func(p *Proxy) {
		p.sessionPersist = t
	}
}

// WithSupabaseStats wires a Supabase-backed stats handler for GET /stats.
// When set, handleStats fetches global totals from Supabase instead of the
// local SQLite store.
func WithSupabaseStats(supabaseURL, serviceKey string) Option {
	return func(p *Proxy) {
		if supabaseURL != "" && serviceKey != "" {
			p.supabaseStats = NewStatsHandler(supabaseURL, serviceKey)
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
		governor:        gov,
		logger:          slog.Default(),
		mux:             http.NewServeMux(),
		maxRequestSize:  8 << 20, // default 8 MiB
		optimizeTimeout: 30 * time.Second,
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
	p.mux.HandleFunc("HEAD /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	p.mux.HandleFunc("GET /readyz", p.handleReady)
	p.mux.HandleFunc("GET /stats", p.handleStats)
	p.mux.HandleFunc("GET /v1/agent-sessions", p.handleAgentSessions)
	p.mux.HandleFunc("GET /v1/diagnostics", p.handleDiagnostics)
	p.mux.HandleFunc("GET /v1/models", p.handleModels)
	p.mux.HandleFunc("POST /v1/messages/count_tokens", p.handleClaudeCountTokens)
	p.mux.HandleFunc("POST /v1/messages", p.handleClaudeMessages)
	p.mux.HandleFunc("POST /v1/chat/completions", p.handleChatCompletions)
	p.mux.HandleFunc("POST /v1/optimize", p.handleOptimize)
	p.mux.HandleFunc("POST /v1/telemetry", p.handleTelemetry)
}
