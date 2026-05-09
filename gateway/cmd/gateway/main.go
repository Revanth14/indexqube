package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/cache"
	"github.com/Revanth14/indexqube/gateway/internal/config"
	"github.com/Revanth14/indexqube/gateway/internal/domain"
	"github.com/Revanth14/indexqube/gateway/internal/governor"
	"github.com/Revanth14/indexqube/gateway/internal/memory"
	"github.com/Revanth14/indexqube/gateway/internal/middleware"
	"github.com/Revanth14/indexqube/gateway/internal/provider/anthropic"
	"github.com/Revanth14/indexqube/gateway/internal/provider/azure"
	"github.com/Revanth14/indexqube/gateway/internal/provider/bedrock"
	"github.com/Revanth14/indexqube/gateway/internal/provider/openai"
	"github.com/Revanth14/indexqube/gateway/internal/proxy"
	"github.com/Revanth14/indexqube/gateway/internal/storage/supabase"
	"github.com/Revanth14/indexqube/gateway/internal/telemetry"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

const (
	// memoryJanitorInterval bounds how often expired session entries are
	// pruned. Sub-minute is overkill; multi-minute risks letting expired
	// state pile up between sweeps.
	memoryJanitorInterval = time.Minute
)

const (
	shutdownGrace = 15 * time.Second
)

func main() {
	// Bootstrap a temporary stderr logger; it's replaced by the telemetry
	// stack as soon as Init succeeds. Without this, config errors are
	// silently swallowed.
	bootLogger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	cfg, err := config.Load()
	if err != nil {
		bootLogger.Error("config load failed", slog.Any("err", err))
		os.Exit(1)
	}

	// Telemetry first: subsequent components log through this provider's
	// trace-aware handler.
	tp, err := telemetry.Init(context.Background(), cfg.Telemetry)
	if err != nil {
		bootLogger.Error("telemetry init failed", slog.Any("err", err))
		os.Exit(1)
	}
	logger := tp.Logger
	logger.Info("config loaded",
		slog.String("env", cfg.Environment),
		slog.String("bind_addr", cfg.Server.BindAddr),
		slog.String("port", cfg.Server.Port),
		slog.String("admin_port", cfg.Server.AdminPort),
		slog.String("mode", cfg.ClaudeCode.Mode),
	)

	// Provider adapters -> governor -> proxy. Adapters know how to call
	// upstream LLMs; the governor routes by provider tag and mediates
	// the response cache; the proxy handles HTTP framing and SSE.
	//
	// All non-AWS adapters share a tuned http.Transport so we keep TLS
	// connections warm across requests. The default transport caps idle
	// conns at 2 per host, which means every burst pays a fresh TLS
	// handshake. Streaming requests are NOT pooled (they don't go back
	// into the idle list) -- this mostly helps non-streaming control
	// calls (e.g. count_tokens, embeddings).
	upstreamClient := &http.Client{Transport: newUpstreamTransport()}
	anthropicAdapter := anthropic.New(
		anthropic.WithLogger(logger),
		anthropic.WithHTTPClient(upstreamClient),
	)
	openaiAdapter := openai.New(
		openai.WithLogger(logger),
		openai.WithHTTPClient(upstreamClient),
	)
	azureAdapter := azure.New(
		azure.WithLogger(logger),
		azure.WithHTTPClient(upstreamClient),
	)
	bedrockAdapter := bedrock.New(
		bedrock.WithLogger(logger),
		bedrock.WithRegion(cfg.AWS.Region),
	)

	// Pruning history stays volatile in v1. Persisting raw code snapshots would
	// violate the Path A privacy model; Supabase is used only for response cache.
	hist := governor.NewMemoryHistoryWithConfig(governor.MemoryHistoryConfig{
		MaxTenants:        cfg.Governor.HistoryMaxTenants,
		MaxFilesPerTenant: cfg.Governor.HistoryMaxFilesPerTenant,
		MaxFileBytes:      cfg.Governor.HistoryMaxFileBytes,
		MaxBytes:          cfg.Governor.HistoryMaxBytes,
		TTL:               cfg.Governor.HistoryTTL,
	})
	var c cache.Cache
	if cfg.Cache.Enabled && cfg.Supabase.DBURL != "" {
		pool, err := pgxpool.New(context.Background(), cfg.Supabase.DBURL)
		if err != nil {
			logger.Error("supabase pool failed", slog.Any("err", err))
			os.Exit(1)
		}
		defer pool.Close()
		c = supabase.NewCache(pool, cfg.Cache.MaxEntryBytes)
		logger.Info("using supabase response cache with volatile pruning history")
	} else {
		if cfg.Cache.Enabled {
			c = cache.NewMemoryCache(cache.MemoryConfig{
				MaxBytes: cfg.Cache.MaxBytes,
				TTL:      cfg.Cache.TTL,
			})
		}
		logger.Info("using volatile in-memory storage")
	}

	projectMemory, err := governor.LoadProjectMemory(cfg.Governor.ProjectMemoryPath)
	if err != nil {
		logger.Error("project memory load failed", slog.Any("err", err))
		os.Exit(1)
	}

	govOpts := []governor.Option{
		governor.WithAdapter(domain.ProviderAnthropic, anthropicAdapter),
		governor.WithAdapter(domain.ProviderOpenAI, openaiAdapter),
		governor.WithAdapter(domain.ProviderAzure, azureAdapter),
		governor.WithAdapter(domain.ProviderBedrock, bedrockAdapter),
		governor.WithLogger(logger),
		governor.WithMetrics(tp.Metrics),
		governor.WithHistory(hist),
		governor.WithPruning(cfg.Governor.PruneEnabled, cfg.Governor.PruneMaxLines),
		governor.WithProjectMemory(projectMemory),
	}
	if c != nil {
		govOpts = append(govOpts, governor.WithCache(c, cfg.Cache.MaxEntryBytes))
	}
	if c != nil && cfg.Cache.SemanticEnabled {
		govOpts = append(govOpts, governor.WithSemanticCaching(true, cfg.Cache.SemanticThreshold, openaiAdapter.Embed))
		logger.Info("semantic cache enabled",
			slog.Float64("threshold", cfg.Cache.SemanticThreshold),
		)
	}

	gov := governor.New(govOpts...)
	claudeStore := memory.NewStore(cfg.ClaudeCode.SessionTTL)
	p := proxy.New(gov,
		proxy.WithLogger(logger),
		proxy.WithMaxRequestSize(cfg.Governor.MaxRequestSize),
		proxy.WithMetrics(tp.Metrics),
		proxy.WithOptimizeTimeout(cfg.Governor.OptimizeTimeout),
		proxy.WithClaudeMessages(proxy.ClaudeMessagesConfig{
			Mode:                 cfg.ClaudeCode.Mode,
			DevToken:             cfg.ClaudeCode.DevToken,
			AnthropicAPIKey:      cfg.ClaudeCode.AnthropicAPIKey,
			AnthropicBaseURL:     cfg.ClaudeCode.AnthropicBaseURL,
			AnthropicVersion:     cfg.ClaudeCode.AnthropicVersion,
			EnableLogPruner:      cfg.ClaudeCode.EnableLogPruner,
			EnableBlockOptimizer: cfg.ClaudeCode.EnableBlockOptimizer,
			SessionStore:         claudeStore,
			HTTPClient:           upstreamClient,
			RateLimitCooldown:    cfg.ClaudeCode.RateLimitCooldown,
		}),
	)

	publicServer := buildPublicServer(cfg, p, tp, logger)
	adminServer := buildAdminServer(cfg, tp, logger)

	// Start both servers in their own goroutines.
	go runServer(publicServer, "public", logger)
	go runServer(adminServer, "admin", logger)

	// Background sweeper: drops expired sessions from the in-memory
	// Claude store. Without this, sessions accumulate forever until
	// restart and the process leaks memory proportional to uptime.
	janitorCtx, stopJanitor := context.WithCancel(context.Background())
	go runMemoryJanitor(janitorCtx, claudeStore, memoryJanitorInterval, logger)

	// Block until interrupted, then drain in-flight streams.
	awaitShutdown(publicServer, adminServer, tp, stopJanitor, logger)
}

// newUpstreamTransport returns the shared transport for non-AWS providers.
// No per-request Client.Timeout: streaming responses can take minutes;
// we rely on context cancellation instead.
func newUpstreamTransport() *http.Transport {
	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          200,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
	}
}

// runMemoryJanitor periodically prunes expired session entries until ctx
// is cancelled. Errors are not possible -- the store is local and the
// only failure mode is contention, handled internally.
func runMemoryJanitor(ctx context.Context, store *memory.Store, interval time.Duration, logger *slog.Logger) {
	if store == nil || interval <= 0 {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	logger.Info("memory janitor started", slog.Duration("interval", interval))
	for {
		select {
		case <-ctx.Done():
			logger.Info("memory janitor stopped")
			return
		case <-t.C:
			store.CleanupExpired()
		}
	}
}

// buildPublicServer wires the inference proxy + observability middleware
// chain onto the public-facing port.
func buildPublicServer(cfg *config.AppConfig, p *proxy.Proxy, tp *telemetry.Provider, logger *slog.Logger) *http.Server {
	// Chain order (outermost first):
	//   CORS           -- answer browser preflight and expose optimizer stats
	//   otelhttp       -- create root span; extract W3C TraceContext from inbound headers
	//   RouteResolver  -- stamp matched ServeMux pattern onto ctx so
	//                     Logging and Metrics can label by bounded route
	//   RequestID      -- generate / honor X-Request-ID; attach to ctx
	//   Recovery       -- catch any panic from anything below; needs metrics
	//   Logging        -- access log: reads request_id, route, status, duration
	//   Metrics        -- HTTP counter + histogram + active-streams gauge
	//   (innermost)    -- proxy mux dispatches to handler
	handler := middleware.Chain(p.Mux(),
		middleware.CORS(middleware.CORSConfig{
			Enabled:               cfg.Server.CORSEnabled,
			AllowedOrigins:        cfg.Server.CORSAllowedOrigins,
			AllowChromeExtensions: cfg.Server.CORSAllowChromeExtensions,
			MaxAge:                cfg.Server.CORSMaxAge,
		}),
		// otelhttp uses the globally-registered TracerProvider that
		// telemetry.Init installed; no explicit binding needed.
		func(next http.Handler) http.Handler { return otelhttp.NewHandler(next, "gateway") },
		middleware.RouteResolver(p.Mux()),
		middleware.RequestID,
		middleware.Recovery(logger, tp.Metrics),
		middleware.Logging(logger, cfg.Server.TrustedProxies),
		middleware.Metrics(tp.Metrics),
	)

	return &http.Server{
		Addr:    publicAddr(cfg),
		Handler: handler,

		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout,
		ReadTimeout:       cfg.Server.ReadTimeout,
		// Streaming-first: zero write timeout. IdleTimeout still kills
		// fully silent connections.
		WriteTimeout: cfg.Server.WriteTimeout,
		IdleTimeout:  cfg.Server.IdleTimeout,
	}
}

func publicAddr(cfg *config.AppConfig) string {
	if cfg.Server.BindAddr != "" {
		return cfg.Server.BindAddr
	}
	return ":" + cfg.Server.Port
}

// buildAdminServer exposes /metrics, /healthz, and /readyz on a separate
// port. Operators should restrict access at the network layer (e.g.
// ServiceMonitor selectors, security groups).
func buildAdminServer(cfg *config.AppConfig, tp *telemetry.Provider, logger *slog.Logger) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", tp.PrometheusHandler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	})

	return &http.Server{
		Addr:              net.JoinHostPort(cfg.Server.AdminBindAddr, cfg.Server.AdminPort),
		Handler:           mux,
		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout,
		ReadTimeout:       cfg.Server.ReadTimeout,
	}
}

func runServer(s *http.Server, label string, logger *slog.Logger) {
	logger.Info("server listening", slog.String("server", label), slog.String("addr", s.Addr))
	if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server failure", slog.String("server", label), slog.Any("err", err))
		os.Exit(1)
	}
}

// awaitShutdown blocks on SIGINT/SIGTERM and drains both servers + the
// telemetry provider within shutdownGrace. Active streams get the full
// grace window to finish; anything past it is forcibly killed. The
// janitor goroutine is stopped before telemetry flushes so it doesn't
// log into a closed provider.
func awaitShutdown(public, admin *http.Server, tp *telemetry.Provider, stopJanitor context.CancelFunc, logger *slog.Logger) {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	logger.Info("shutdown signal received", slog.String("signal", sig.String()))

	ctx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()

	// Stop accepting new public traffic first; in-flight streams get the
	// grace window via Shutdown's documented semantics.
	if err := public.Shutdown(ctx); err != nil {
		logger.Error("public server shutdown", slog.Any("err", err))
	}
	if err := admin.Shutdown(ctx); err != nil {
		logger.Error("admin server shutdown", slog.Any("err", err))
	}
	if stopJanitor != nil {
		stopJanitor()
	}
	if err := tp.Shutdown(ctx); err != nil {
		logger.Error("telemetry shutdown", slog.Any("err", err))
	}

	logger.Info("gateway exited cleanly")
}
