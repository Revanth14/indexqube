package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/cache"
	"github.com/Revanth14/indexqube/gateway/internal/config"
	"github.com/Revanth14/indexqube/gateway/internal/domain"
	"github.com/Revanth14/indexqube/gateway/internal/governor"
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
		slog.String("port", cfg.Server.Port),
		slog.String("admin_port", cfg.Server.AdminPort),
	)

	// Provider adapters -> governor -> proxy. Adapters know how to call
	// upstream LLMs; the governor routes by provider tag and mediates
	// the response cache; the proxy handles HTTP framing and SSE.
	anthropicAdapter := anthropic.New(anthropic.WithLogger(logger))
	openaiAdapter := openai.New(openai.WithLogger(logger))
	azureAdapter := azure.New(azure.WithLogger(logger))
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
	p := proxy.New(gov,
		proxy.WithLogger(logger),
		proxy.WithMaxRequestSize(cfg.Governor.MaxRequestSize),
	)

	publicServer := buildPublicServer(cfg, p, tp, logger)
	adminServer := buildAdminServer(cfg, tp, logger)

	// Start both servers in their own goroutines.
	go runServer(publicServer, "public", logger)
	go runServer(adminServer, "admin", logger)

	// Block until interrupted, then drain in-flight streams.
	awaitShutdown(publicServer, adminServer, tp, logger)
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
		middleware.Logging(logger),
		middleware.Metrics(tp.Metrics),
	)

	return &http.Server{
		Addr:    ":" + cfg.Server.Port,
		Handler: handler,

		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout,
		ReadTimeout:       cfg.Server.ReadTimeout,
		// Streaming-first: zero write timeout. IdleTimeout still kills
		// fully silent connections.
		WriteTimeout: cfg.Server.WriteTimeout,
		IdleTimeout:  cfg.Server.IdleTimeout,
	}
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
		Addr:              ":" + cfg.Server.AdminPort,
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
// grace window to finish; anything past it is forcibly killed.
func awaitShutdown(public, admin *http.Server, tp *telemetry.Provider, logger *slog.Logger) {
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
	if err := tp.Shutdown(ctx); err != nil {
		logger.Error("telemetry shutdown", slog.Any("err", err))
	}

	logger.Info("gateway exited cleanly")
}
