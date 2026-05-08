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
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

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

	semanticEnabled   bool
	semanticThreshold float64
	embedder          func(ctx context.Context, apiKey, text string) ([]float32, error)
}

// Option configures a Governor at construction time.
type Option func(*Governor)

// WithSemanticCaching enables pgvector-based similarity caching.
func WithSemanticCaching(enabled bool, threshold float64, embedder func(context.Context, string, string) ([]float32, error)) Option {
	return func(g *Governor) {
		g.semanticEnabled = enabled
		g.semanticThreshold = threshold
		g.embedder = embedder
	}
}

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
	started := time.Now()
	out := cloneMessages(msgs)
	out = InjectProjectMemory(out, MergeProjectMemory(g.projectMemory, projectMemory))
	if tenant == "" {
		st := finishPruneStats(domain.PruneStats{})
		g.recordOptimization(ctx, "optimize", st, "", "")
		g.recordOptimizationDuration("optimize", time.Since(started))
		return out, st, nil
	}
	if !g.pruneEnabled || g.history == nil {
		st := finishPruneStats(domain.PruneStats{})
		g.recordOptimization(ctx, "optimize", st, "", "")
		g.recordOptimizationDuration("optimize", time.Since(started))
		return out, st, nil
	}
	stMsgs, st := PruneMessages(ctx, g.history, tenant, out, g.effectivePruneMaxLines(), g.logger)
	g.recordOptimization(ctx, "optimize", st, "", "")
	g.recordOptimizationDuration("optimize", time.Since(started))
	return stMsgs, st, nil
}

// Diagnostics returns a privacy-safe local optimizer health snapshot.
func (g *Governor) Diagnostics(ctx context.Context) (domain.Diagnostics, error) {
	if err := ctx.Err(); err != nil {
		return domain.Diagnostics{}, err
	}
	diag := domain.Diagnostics{
		Status:         "ok",
		PruningEnabled: g.pruneEnabled && g.history != nil,
	}
	if h, ok := g.history.(interface{ Stats() MemoryHistoryStats }); ok {
		stats := h.Stats()
		diag.History = domain.HistoryDiagnostics{
			Tenants: stats.Tenants,
			Entries: stats.Entries,
			Bytes:   stats.Bytes,
		}
	}
	return diag, nil
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
	var st domain.PruneStats
	optStarted := time.Now()
	if g.pruneEnabled && g.history != nil {
		work.Messages, st = PruneMessages(ctx, g.history, tenant, work.Messages, g.effectivePruneMaxLines(), g.logger)
		g.recordOptimization(ctx, "stream", st, string(work.Credential.Provider), work.Model)
	} else {
		st = finishPruneStats(domain.PruneStats{})
		g.recordOptimization(ctx, "stream", st, string(work.Credential.Provider), work.Model)
	}
	g.recordOptimizationDuration("stream", time.Since(optStarted))
	if err := emitOptimizerEvent(tw, st); err != nil {
		return err
	}

	// 1. Cache lookup
	var cacheKey cache.Key
	if g.cache != nil {
		if key, err := cache.DeriveKey(work); err != nil {
			g.recordLookup("error")
			g.logger.WarnContext(ctx, "cache key derivation failed; bypassing cache", slog.Any("err", err))
		} else {
			cacheKey = key
			if entry, hit, err := g.cache.Get(ctx, cacheKey); err != nil {
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

			// 1b. Semantic Cache Lookup (L2)
			if g.semanticEnabled && g.embedder != nil && work.Credential.Provider == domain.ProviderOpenAI {
				// Only embed the user's last message for context search.
				// This is a common pattern for semantic caching.
				lastMsg := getLastUserMessage(work.Messages)
				if lastMsg != "" {
					embedding, err := g.embedder(ctx, work.Credential.APIKey, lastMsg)
					if err == nil {
						if entry, hit, err := g.cache.GetSemantic(ctx, tenant, embedding, g.semanticThreshold); err == nil && hit {
							g.logger.InfoContext(ctx, "semantic cache hit",
								slog.String("provider", string(work.Credential.Provider)),
								slog.String("model", work.Model),
							)
							// Optional: update L1 cache with this result for next time.
							_ = g.cache.Put(ctx, cacheKey, entry)
							return entry.Replay(tw)
						}
					} else {
						g.logger.WarnContext(ctx, "embedding generation failed", slog.Any("err", err))
					}
				}
			}
		}
	}

	// 2. Dispatch with failover
	entry, err := g.dispatchWithFailover(ctx, work, tw)
	if err != nil {
		return err
	}

	// 3. Persist to cache
	if entry != nil && g.cache != nil {
		key, kerr := cache.DeriveKey(work)
		if kerr == nil {
			var embedding []float32
			if g.semanticEnabled && g.embedder != nil && work.Credential.Provider == domain.ProviderOpenAI {
				lastMsg := getLastUserMessage(work.Messages)
				if lastMsg != "" {
					embedding, _ = g.embedder(ctx, work.Credential.APIKey, lastMsg)
				}
			}

			var perr error
			if len(embedding) > 0 {
				perr = g.cache.PutSemantic(ctx, tenant, key, entry, embedding)
			} else {
				perr = g.cache.Put(ctx, key, entry)
			}
			if perr != nil {
				if errors.Is(perr, cache.ErrEntryTooLarge) {
					g.recordWrite("too_large")
				} else {
					g.recordWrite("error")
					g.logger.WarnContext(ctx, "cache put failed", slog.Any("err", perr))
				}
			} else {
				g.recordWrite("ok")
			}
		}
	}

	return nil
}

func emitOptimizerEvent(tw domain.TokenWriter, st domain.PruneStats) error {
	payload, err := json.Marshal(struct {
		Version string            `json:"version"`
		Source  string            `json:"source"`
		Mode    string            `json:"mode"`
		Stats   domain.PruneStats `json:"stats"`
	}{
		Version: "v1",
		Source:  "stream",
		Mode:    streamPruneMode(st),
		Stats:   st,
	})
	if err != nil {
		return fmt.Errorf("marshal optimizer event: %w", err)
	}
	return tw.WriteEvent("iq_optimizer", payload)
}

func streamPruneMode(st domain.PruneStats) string {
	if st.BlocksPruned > 0 {
		if st.DiffExact+st.DiffFallback > 0 {
			return "diff"
		}
		return "unchanged"
	}
	if st.BlocksSkipped > 0 {
		return "skipped"
	}
	if st.BlocksSeen > 0 {
		return "warmup"
	}
	return "none"
}

func (g *Governor) dispatchWithFailover(ctx context.Context, work *domain.InferenceRequest, tw domain.TokenWriter) (*cache.Entry, error) {
	a, ok := g.adapters[work.Credential.Provider]
	if !ok {
		return nil, fmt.Errorf("governor: no adapter registered for provider %q", work.Credential.Provider)
	}

	// Use a Tee if caching is enabled to capture the response.
	// If failover happens, we skip caching for the fallback response in v1
	// to avoid key mismatch / complexity.
	var sink domain.TokenWriter = tw
	var tee *cache.Tee
	if g.cache != nil {
		tee = cache.NewTee(tw, g.maxEntryBytes)
		sink = tee
	}

	started := time.Now()
	err := a.Dispatch(ctx, work, sink)
	g.recordProviderDuration(work.Credential.Provider, work.Model, err, time.Since(started))
	if err != nil && isRetryable(err) {
		if fallback, ok := g.getFallbackProvider(work.Credential.Provider); ok {
			if fa, fok := g.adapters[fallback]; fok {
				g.logger.InfoContext(ctx, "primary provider failed; failing over",
					slog.String("from", string(work.Credential.Provider)),
					slog.String("to", string(fallback)),
					slog.Any("err", err),
				)
				if g.metrics != nil {
					g.metrics.FailoverRequests.WithLabelValues(string(work.Credential.Provider), string(fallback)).Inc()
				}
				work.Credential.Provider = fallback
				// Failover bypasses the tee/cache for now.
				started := time.Now()
				err := fa.Dispatch(ctx, work, tw)
				g.recordProviderDuration(work.Credential.Provider, work.Model, err, time.Since(started))
				return nil, err
			}
		}
	}
	if err != nil {
		return nil, err
	}

	// 3. Extract entry if we used a tee.
	if tee != nil {
		if entry, ok := tee.Entry(work.Credential.Provider, work.Model); ok {
			return entry, nil
		}
	}

	return nil, nil
}

func getLastUserMessage(msgs []domain.Message) string {
	for i := len(msgs) - 1; i >= 0; i-- {
		if strings.ToLower(msgs[i].Role) == "user" {
			return msgs[i].Content
		}
	}
	return ""
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

func (g *Governor) recordOptimizationDuration(source string, d time.Duration) {
	if g.metrics == nil {
		return
	}
	g.metrics.OptimizerDuration.WithLabelValues(source).Observe(d.Seconds())
}

func (g *Governor) recordProviderDuration(provider domain.Provider, model string, err error, d time.Duration) {
	if g.metrics == nil {
		return
	}
	result := "ok"
	if err != nil {
		result = "error"
	}
	g.metrics.ProviderDuration.WithLabelValues(string(provider), model, result).Observe(d.Seconds())
}

func (g *Governor) getFallbackProvider(p domain.Provider) (domain.Provider, bool) {
	// Static failover map for v1.
	// Anthropic -> Bedrock (Claude 3.5 failover)
	// OpenAI -> Azure (GPT failover)
	switch p {
	case domain.ProviderAnthropic:
		return domain.ProviderBedrock, true
	case domain.ProviderOpenAI:
		return domain.ProviderAzure, true
	default:
		return "", false
	}
}

func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	// Detect 429 (Rate Limit) and 503 (Service Unavailable)
	return strings.Contains(msg, "429") ||
		strings.Contains(msg, "503") ||
		strings.Contains(msg, "overloaded") ||
		strings.Contains(msg, "rate limit")
}

func (g *Governor) recordOptimization(ctx context.Context, source string, st domain.PruneStats, provider, model string) {
	if g.metrics != nil {
		g.metrics.OptimizerRequests.WithLabelValues(source).Inc()
		g.metrics.OptimizerBytes.WithLabelValues(source, "before").Add(float64(st.BytesBefore))
		g.metrics.OptimizerBytes.WithLabelValues(source, "after").Add(float64(st.BytesAfter))
		g.metrics.OptimizerTokens.WithLabelValues(source, "before").Add(float64(st.TokensBefore))
		g.metrics.OptimizerTokens.WithLabelValues(source, "after").Add(float64(st.TokensAfter))
		g.metrics.OptimizerBlocks.WithLabelValues(source, "seen").Add(float64(st.BlocksSeen))
		g.metrics.OptimizerBlocks.WithLabelValues(source, "pruned").Add(float64(st.BlocksPruned))
		g.metrics.OptimizerBlocks.WithLabelValues(source, "skipped").Add(float64(st.BlocksSkipped))
		g.metrics.OptimizerDiffs.WithLabelValues(source, "exact").Add(float64(st.DiffExact))
		g.metrics.OptimizerDiffs.WithLabelValues(source, "fallback").Add(float64(st.DiffFallback))
		for reason, n := range st.SkipReasons {
			g.metrics.OptimizerSkips.WithLabelValues(source, reason).Add(float64(n))
		}
		g.metrics.OptimizerReduction.WithLabelValues(source).Observe(st.ReductionRatio)

		tokensSaved := st.TokensBefore - st.TokensAfter
		if tokensSaved > 0 {
			usd := telemetry.EstimateCostSaved(provider, model, tokensSaved)
			g.metrics.EstimatedCostSavedUSD.WithLabelValues(provider, model).Add(usd)
		}
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
