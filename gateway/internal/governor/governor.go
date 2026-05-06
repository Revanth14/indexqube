// Package governor is the orchestration layer between the proxy and
// upstream provider adapters — the IndexQube "brain".
//
// Responsibilities:
//
//  1. Context pruning: diff fenced code blocks against per-tenant session
//     history so repeat prompts ship unified diffs instead of full files.
//  2. Project memory injection (leading system message from headers/body).
//  3. Response cache (exact-match LRU) before upstream dispatch.
//  4. Adapter dispatch to Anthropic / OpenAI / future Bedrock.
//
// Semantic vector caching (pgvector) and overflow routing are planned;
// hooks compose around dispatch without changing the proxy contract.
package governor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/Revanth14/indexqube/gateway/internal/cache"
	"github.com/Revanth14/indexqube/gateway/internal/domain"
	"github.com/Revanth14/indexqube/gateway/internal/telemetry"
)

// Adapter is the contract every upstream provider integration implements.
//
// Implementations MUST honor ctx cancellation. They MUST NOT close or
// otherwise mutate the provided TokenWriter beyond writing frames to it.
type Adapter interface {
	Dispatch(ctx context.Context, req *domain.InferenceRequest, tw domain.TokenWriter) error
	Ready(ctx context.Context) error
}

// Governor implements proxy.Governor by routing to registered adapters,
// with optional pruning history and response cache.
type Governor struct {
	adapters      map[domain.Provider]Adapter
	logger        *slog.Logger
	cache         cache.Cache
	maxEntryBytes int64
	metrics       *telemetry.Metrics
	history       History
	pruneEnabled  bool
	pruneMaxLines int
	projectMemory string
}

// Option configures a Governor at construction time.
type Option func(*Governor)

// WithAdapter registers an Adapter under a provider tag.
func WithAdapter(p domain.Provider, a Adapter) Option {
	return func(g *Governor) {
		if a == nil {
			return
		}
		g.adapters[p] = a
	}
}

// WithLogger overrides the default slog.Default() logger.
func WithLogger(l *slog.Logger) Option {
	return func(g *Governor) {
		if l != nil {
			g.logger = l
		}
	}
}

// WithCache enables response caching.
func WithCache(c cache.Cache, maxEntryBytes int64) Option {
	return func(g *Governor) {
		g.cache = c
		g.maxEntryBytes = maxEntryBytes
	}
}

// WithMetrics enables Prometheus emission for cache lookups and writes.
func WithMetrics(m *telemetry.Metrics) Option {
	return func(g *Governor) {
		g.metrics = m
	}
}

// WithHistory wires session storage for code-block pruning. Nil disables
// pruning even when PruneEnabled is true.
func WithHistory(h History) Option {
	return func(g *Governor) {
		g.history = h
	}
}

// WithPruning toggles the diff-based pruning engine and sets the max line
// count per file for LCS diff (files larger than this skip diff pruning).
func WithPruning(enabled bool, maxLines int) Option {
	return func(g *Governor) {
		g.pruneEnabled = enabled
		if maxLines > 0 {
			g.pruneMaxLines = maxLines
		}
	}
}

// WithProjectMemory configures static project memory loaded from
// indexqube_context.md (or another configured path).
func WithProjectMemory(memory string) Option {
	return func(g *Governor) {
		g.projectMemory = memory
	}
}

// New returns a Governor with the given configuration.
func New(opts ...Option) *Governor {
	g := &Governor{
		adapters: make(map[domain.Provider]Adapter),
		logger:   slog.Default(),
	}
	for _, opt := range opts {
		opt(g)
	}
	return g
}

func (g *Governor) effectivePruneMaxLines() int {
	if g.pruneMaxLines > 0 {
		return g.pruneMaxLines
	}
	return 8000
}

func cloneInferenceRequest(req *domain.InferenceRequest) *domain.InferenceRequest {
	w := *req
	w.Messages = cloneMessages(req.Messages)
	return &w
}

// Optimize runs pruning (+ optional project memory) without calling any LLM.
// Used by POST /v1/optimize (Chrome pre-processor Path A).
func (g *Governor) Optimize(ctx context.Context, tenant string, msgs []domain.Message, projectMemory string) ([]domain.Message, domain.PruneStats, error) {
	out := cloneMessages(msgs)
	out = InjectProjectMemory(out, MergeProjectMemory(g.projectMemory, projectMemory))
	if tenant == "" {
		st := finishPruneStats(domain.PruneStats{})
		g.recordOptimization(ctx, "optimize", st)
		return out, st, nil
	}
	if !g.pruneEnabled || g.history == nil {
		st := finishPruneStats(domain.PruneStats{})
		g.recordOptimization(ctx, "optimize", st)
		return out, st, nil
	}
	stMsgs, st := PruneMessages(ctx, g.history, tenant, out, g.effectivePruneMaxLines(), g.logger)
	g.recordOptimization(ctx, "optimize", st)
	return stMsgs, st, nil
}

// Stream is the proxy.Governor entrypoint for Path B streaming.
func (g *Governor) Stream(ctx context.Context, req *domain.InferenceRequest, tw domain.TokenWriter) error {
	if req == nil {
		return fmt.Errorf("governor: nil request")
	}

	work := cloneInferenceRequest(req)
	tenant := domain.ResolveTenantKey(work.SessionKey, work.Credential.APIKey)
	work.ProjectMemory = MergeProjectMemory(g.projectMemory, work.ProjectMemory)
	work.Messages = InjectProjectMemory(work.Messages, work.ProjectMemory)
	if g.pruneEnabled && g.history != nil {
		var st domain.PruneStats
		work.Messages, st = PruneMessages(ctx, g.history, tenant, work.Messages, g.effectivePruneMaxLines(), g.logger)
		g.recordOptimization(ctx, "stream", st)
	} else {
		g.recordOptimization(ctx, "stream", finishPruneStats(domain.PruneStats{}))
	}

	a, ok := g.adapters[work.Credential.Provider]
	if !ok {
		return fmt.Errorf("governor: no adapter registered for provider %q", work.Credential.Provider)
	}

	if g.cache == nil {
		g.logger.DebugContext(ctx, "dispatching to adapter (cache disabled)",
			slog.String("provider", string(work.Credential.Provider)),
			slog.String("model", work.Model),
		)
		return a.Dispatch(ctx, work, tw)
	}

	key, err := cache.DeriveKey(work)
	if err != nil {
		g.recordLookup("error")
		g.logger.WarnContext(ctx, "cache key derivation failed; bypassing cache", slog.Any("err", err))
		return a.Dispatch(ctx, work, tw)
	}

	if entry, hit, err := g.cache.Get(ctx, key); err != nil {
		g.recordLookup("error")
		g.logger.WarnContext(ctx, "cache get failed; falling back to upstream", slog.Any("err", err))
	} else if hit {
		g.recordLookup("hit")
		g.logger.DebugContext(ctx, "cache hit",
			slog.String("provider", string(work.Credential.Provider)),
			slog.String("model", work.Model),
		)
		return entry.Replay(tw)
	} else {
		g.recordLookup("miss")
	}

	tee := cache.NewTee(tw, g.maxEntryBytes)
	if err := a.Dispatch(ctx, work, tee); err != nil {
		return err
	}

	entry, ok := tee.Entry(work.Credential.Provider, work.Model)
	if !ok {
		g.recordWrite("skipped")
		return nil
	}

	if err := g.cache.Put(ctx, key, entry); err != nil {
		if errors.Is(err, cache.ErrEntryTooLarge) {
			g.recordWrite("too_large")
		} else {
			g.recordWrite("error")
			g.logger.WarnContext(ctx, "cache put failed", slog.Any("err", err))
		}
		return nil
	}
	g.recordWrite("ok")
	return nil
}

func (g *Governor) recordLookup(result string) {
	if g.metrics == nil {
		return
	}
	g.metrics.CacheLookups.WithLabelValues(result).Inc()
}

func (g *Governor) recordWrite(result string) {
	if g.metrics == nil {
		return
	}
	g.metrics.CacheWrites.WithLabelValues(result).Inc()
}

func (g *Governor) recordOptimization(ctx context.Context, source string, st domain.PruneStats) {
	if g.metrics != nil {
		g.metrics.OptimizerRequests.WithLabelValues(source).Inc()
		g.metrics.OptimizerBytes.WithLabelValues(source, "before").Add(float64(st.BytesBefore))
		g.metrics.OptimizerBytes.WithLabelValues(source, "after").Add(float64(st.BytesAfter))
		g.metrics.OptimizerBlocks.WithLabelValues(source, "seen").Add(float64(st.BlocksSeen))
		g.metrics.OptimizerBlocks.WithLabelValues(source, "pruned").Add(float64(st.BlocksPruned))
		g.metrics.OptimizerBlocks.WithLabelValues(source, "skipped").Add(float64(st.BlocksSkipped))
		g.metrics.OptimizerDiffs.WithLabelValues(source, "exact").Add(float64(st.DiffExact))
		g.metrics.OptimizerDiffs.WithLabelValues(source, "fallback").Add(float64(st.DiffFallback))
		for reason, n := range st.SkipReasons {
			g.metrics.OptimizerSkips.WithLabelValues(source, reason).Add(float64(n))
		}
		g.metrics.OptimizerReduction.WithLabelValues(source).Observe(st.ReductionRatio)
	}
	g.logger.InfoContext(ctx, "optimization summary",
		slog.String("source", source),
		slog.Int("blocks_seen", st.BlocksSeen),
		slog.Int("blocks_pruned", st.BlocksPruned),
		slog.Int("blocks_skipped", st.BlocksSkipped),
		slog.Int("bytes_before", st.BytesBefore),
		slog.Int("bytes_after", st.BytesAfter),
		slog.Int("estimated_tokens_before", st.TokensBefore),
		slog.Int("estimated_tokens_after", st.TokensAfter),
		slog.Float64("reduction_ratio", st.ReductionRatio),
		slog.Int("diff_exact", st.DiffExact),
		slog.Int("diff_fallback", st.DiffFallback),
	)
}

// Ready checks if the governor is prepared to serve requests. It verifies
// that at least one adapter is registered and that all registered
// adapters are themselves ready. If caching is enabled, it also checks
// the cache's health.
func (g *Governor) Ready(ctx context.Context) error {
	if len(g.adapters) == 0 {
		return fmt.Errorf("governor: no adapters registered")
	}

	for p, a := range g.adapters {
		if err := a.Ready(ctx); err != nil {
			return fmt.Errorf("governor: adapter %q not ready: %w", p, err)
		}
	}

	// In v1, MemoryCache is always ready if initialized. Future L2
	// implementations (Supabase/Redis) will implement a health check here.
	return nil
}
