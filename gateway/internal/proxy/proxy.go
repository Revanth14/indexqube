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
	"sync"
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
	streamTimeout   time.Duration
	metrics         *telemetry.Metrics
	claude          ClaudeMessagesConfig
	usageTracker    telemetry.Sink
	sessionTracker  *telemetry.AgentSessionStore
	sessionPersist  *sessions.Tracker
	supabaseStats   *StatsHandler

	// sessionTurnCounters tracks the number of turns per session key.
	// Used for synthetic request ID generation (FIX 3).
	sessionTurnCounters sync.Map // map[string]*sessionTurnState

	// sessionWarmUpDone tracks which sessions have completed cache warm-up.
	// Set after pre-registering system spans on the first turn (FIX 6).
	sessionWarmUpDone sync.Map // map[string]bool

	// sessionBoilerplateState tracks the last turn/byte-offset where system
	// boilerplate was forwarded for injection cooldown logic (FIX 7).
	sessionBoilerplateState sync.Map // map[string]*boilerplateState

	// inFlightRequests prevents simultaneous duplicate upstream dispatches.
	// When a second identical prompt arrives while the first is in-flight it
	// waits for the first to complete before dispatching (FIX 2).
	inFlightRequests *inFlightTracker

	// sessionPrefixHints stores per-session (hash → byteLength) entries for
	// spans shorter than SmallFileBytes. When a larger span arrives, its prefix
	// is checked against these hints to detect "small payload is prefix of
	// large payload" reuse patterns (FIX 3).
	sessionPrefixHints sync.Map // map[string]*prefixHintSet

	// sessionLastUsed tracks the last access time for each session key.
	// Used by the background cleanup goroutine to evict idle entries.
	sessionLastUsed sync.Map // map[string]time.Time

	// cleanupCtx/cleanupCancel manage the background TTL eviction goroutine.
	cleanupCtx    context.Context
	cleanupCancel context.CancelFunc
	cleanupDone   chan struct{}

	// sessionSuggestionTs tracks the last time a suggestion-mode request was
	// processed per session. Used to rate-limit harness meta-prompt injections
	// to max 1 per 10 seconds so ephemeral payloads don't pollute the chunk store.
	sessionSuggestionTs sync.Map // map[string]time.Time
}

// inFlightTracker serialises duplicate identical requests so they do not
// all fire upstream concurrently. The first arrival dispatches normally;
// subsequent arrivals with the same prompt hash block until it finishes.
type inFlightTracker struct {
	mu       sync.Mutex
	inflight map[string]chan struct{} // prompt_hash → done channel
}

func newInFlightTracker() *inFlightTracker {
	return &inFlightTracker{inflight: make(map[string]chan struct{})}
}

// acquire attempts to register promptHash as in-flight.
// Returns (doneFn, nil) when this is the first arrival — the caller must call
// doneFn exactly once after the request completes.
// Returns (nil, waitChan) when a duplicate is already in-flight — the caller
// should wait on waitChan before dispatching.
func (t *inFlightTracker) acquire(promptHash string) (doneFn func(), waitChan chan struct{}) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if ch, ok := t.inflight[promptHash]; ok {
		return nil, ch
	}
	done := make(chan struct{})
	t.inflight[promptHash] = done
	return func() {
		t.mu.Lock()
		delete(t.inflight, promptHash)
		t.mu.Unlock()
		close(done)
	}, nil
}

// prefixHintSet stores (hash → byteLength) entries for small spans so that
// larger spans can be checked for prefix reuse (FIX 3).
type prefixHintSet struct {
	mu    sync.Mutex
	hints map[string]int // content-hash → byte-length of the small chunk
}

type cachedResponse struct {
	promptHash string
	payload    []byte
}

// sessionTurnState holds per-session counters needed for FIX 3 and FIX 7.
type sessionTurnState struct {
	mu                     sync.Mutex
	turnIndex              int
	missingIDWindow        []int64 // Unix timestamps of turns with missing request IDs
	contextBytesCumulative int64   // running total of context bytes seen across turns
	cachedResponses        []cachedResponse
}

func (s *sessionTurnState) getCachedResponse(promptHash string) ([]byte, bool) {
	for _, cr := range s.cachedResponses {
		if cr.promptHash == promptHash {
			return cr.payload, true
		}
	}
	return nil, false
}

func (s *sessionTurnState) saveCachedResponse(promptHash string, payload []byte) {
	if len(s.cachedResponses) >= 10 {
		s.cachedResponses = s.cachedResponses[1:]
	}
	s.cachedResponses = append(s.cachedResponses, cachedResponse{
		promptHash: promptHash,
		payload:    append([]byte(nil), payload...),
	})
}

// boilerplateState tracks when system boilerplate was last forwarded.
type boilerplateState struct {
	mu                    sync.Mutex
	lastForwardedTurn     int
	lastForwardedCtxBytes int
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
	TargetChunkBytes        int  // target chunk size for Rabin-Karp chunker (default 2048)
	MaxChunkBytes           int  // maximum chunk size (default 8192)
	MinSavedTokens          int  // skip rewrite if savings below this threshold (default 10)
	EnableToolResultPruning bool // prune old tool_result spans (default true via claudeDefaults)
	EnableAssistantPruning  bool // prune old assistant text spans (default false)
	EnableSystemPruning     bool // deprecated; system text spans are never pruned
	Diagnostics             bool // emit verbose per-class diagnostics in logs

	// Sub-span chunking: splits large tool_result spans with Rabin-Karp CDC so
	// that only spans whose every chunk is known get pruned. This preserves
	// context when a file is partially edited while still deduplicating unchanged
	// sections across turns.
	EnableSubspanChunking bool // split large spans for chunk-level dedup (default true)
	SmallFileBytes        int  // content shorter than this bypasses the chunker (default 4096)

	// EnablePromptCache injects Anthropic cache_control: {type: "ephemeral"} on
	// the last system block when the system prompt is identical to the prior turn.
	// Anthropic's server-side cache then covers the stable prefix, reducing
	// billable input tokens for sessions with a large, unchanged system prompt.
	EnablePromptCache bool
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

// WithStreamTimeout caps the duration of streaming governor requests.
// A non-positive value uses the default (5 minutes).
func WithStreamTimeout(d time.Duration) Option {
	return func(p *Proxy) {
		if d > 0 {
			p.streamTimeout = d
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
		governor:         gov,
		logger:           slog.Default(),
		mux:              http.NewServeMux(),
		maxRequestSize:   8 << 20, // default 8 MiB
		optimizeTimeout:  30 * time.Second,
		streamTimeout:    0, // disabled by default; use WithStreamTimeout to enable
		inFlightRequests: newInFlightTracker(),
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
