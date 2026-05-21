// Package server exports the gateway entrypoint so it can be embedded by
// cmd/gateway (standalone daemon) and cmd/iq (wrapper binary).
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
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
	"github.com/Revanth14/indexqube/gateway/internal/sessions"
	"github.com/Revanth14/indexqube/gateway/internal/storage/supabase"
	"github.com/Revanth14/indexqube/gateway/internal/telemetry"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

const (
	memoryJanitorInterval = time.Minute
	shutdownGrace         = 15 * time.Second
)

// Run starts the gateway, reading all config from the environment, and blocks
// until ctx is cancelled or a fatal startup error occurs.
func Run(ctx context.Context) error {
	return run(ctx, nil)
}

// RunWithPublicListener starts the gateway with an already-bound public
// listener. This is useful for callers that need to reserve an ephemeral port
// before startup.
func RunWithPublicListener(ctx context.Context, publicListener net.Listener) error {
	return run(ctx, publicListener)
}

func run(ctx context.Context, publicListener net.Listener) error {
	bootLogger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	cfg, err := config.Load()
	if err != nil {
		bootLogger.Error("config load failed", slog.Any("err", err))
		return fmt.Errorf("config: %w", err)
	}

	tp, err := telemetry.Init(ctx, cfg.Telemetry)
	if err != nil {
		bootLogger.Error("telemetry init failed", slog.Any("err", err))
		return fmt.Errorf("telemetry: %w", err)
	}
	logger := tp.Logger
	logger.Info("config loaded",
		slog.String("env", cfg.Environment),
		slog.String("bind_addr", cfg.Server.BindAddr),
		slog.String("mode", cfg.ClaudeCode.Mode),
	)

	upstreamClient := &http.Client{Transport: newUpstreamTransport()}
	anthropicAdapter := anthropic.New(anthropic.WithLogger(logger), anthropic.WithHTTPClient(upstreamClient))
	openaiAdapter := openai.New(openai.WithLogger(logger), openai.WithHTTPClient(upstreamClient))
	azureAdapter := azure.New(azure.WithLogger(logger), azure.WithHTTPClient(upstreamClient))
	bedrockAdapter := bedrock.New(bedrock.WithLogger(logger), bedrock.WithRegion(cfg.AWS.Region))

	hist := governor.NewMemoryHistoryWithConfig(governor.MemoryHistoryConfig{
		MaxTenants:        cfg.Governor.HistoryMaxTenants,
		MaxFilesPerTenant: cfg.Governor.HistoryMaxFilesPerTenant,
		MaxFileBytes:      cfg.Governor.HistoryMaxFileBytes,
		MaxBytes:          cfg.Governor.HistoryMaxBytes,
		TTL:               cfg.Governor.HistoryTTL,
	})

	var c cache.Cache
	if cfg.Cache.Enabled && cfg.Supabase.DBURL != "" {
		pool, err := pgxpool.New(ctx, cfg.Supabase.DBURL)
		if err != nil {
			logger.Error("supabase pool failed", slog.Any("err", err))
			return fmt.Errorf("supabase: %w", err)
		}
		defer pool.Close()
		c = supabase.NewCache(pool, cfg.Cache.MaxEntryBytes)
		logger.Info("using supabase response cache")
	} else if cfg.Cache.Enabled {
		c = cache.NewMemoryCache(cache.MemoryConfig{
			MaxBytes: cfg.Cache.MaxBytes,
			TTL:      cfg.Cache.TTL,
		})
		logger.Info("using volatile in-memory storage")
	}

	projectMemory, err := governor.LoadProjectMemory(cfg.Governor.ProjectMemoryPath)
	if err != nil {
		logger.Error("project memory load failed", slog.Any("err", err))
		return fmt.Errorf("project memory: %w", err)
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
	}

	gov, err := governor.New(govOpts...)
	if err != nil {
		logger.Error("governor init failed", slog.Any("err", err))
		return fmt.Errorf("governor: %w", err)
	}
	claudeStore := memory.NewStore(cfg.ClaudeCode.SessionTTL)

	var bedrockClient *bedrockruntime.Client
	var bedrockModels []proxy.ModelEntry
	if cfg.ClaudeCode.BedrockEnabled {
		awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.ClaudeCode.BedrockRegion))
		if err != nil {
			logger.Error("failed to load AWS config", slog.Any("err", err))
			return fmt.Errorf("aws: %w", err)
		}
		bedrockClient = bedrockruntime.NewFromConfig(awsCfg)
		bedrockCfg := proxy.BedrockConfig{
			Enabled:     cfg.ClaudeCode.BedrockEnabled,
			Region:      cfg.ClaudeCode.BedrockRegion,
			ModelPrefix: cfg.ClaudeCode.BedrockModelPrefix,
			Client:      bedrockClient,
		}
		bedrockModels = proxy.FetchBedrockModels(ctx, bedrockCfg, logger)
		logger.Info("bedrock backend enabled",
			slog.String("region", cfg.ClaudeCode.BedrockRegion),
			slog.Int("models_available", len(bedrockModels)),
		)
	}

	// Optional usage telemetry. Prefer GatewayClient (relay) when
	// IQ_TELEMETRY_ENDPOINT is set; fall back to direct Supabase writes when
	// SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY are present (server deployments only).
	var usageTracker telemetry.Sink
	if endpoint := os.Getenv("IQ_TELEMETRY_ENDPOINT"); endpoint != "" {
		usageTracker = telemetry.NewGatewayClient(endpoint)
		logger.Info("gateway relay telemetry enabled", slog.String("endpoint", endpoint))
	} else if cfg.Supabase.URL != "" && cfg.Supabase.ServiceKey != "" {
		usageTracker = telemetry.NewSupabaseClient(cfg.Supabase.URL, cfg.Supabase.ServiceKey)
		logger.Info("supabase usage telemetry enabled")
	}

	agentSessions := telemetry.NewAgentSessionStore(0) // 4 h TTL default

	var sessionTracker *sessions.Tracker
	if home, err := os.UserHomeDir(); err == nil {
		dbDir := filepath.Join(home, ".indexqube")
		if err := os.MkdirAll(dbDir, 0o700); err == nil {
			t, err := sessions.Open(filepath.Join(dbDir, "sessions.db"), logger)
			if err != nil {
				logger.Warn("session tracker init failed; continuing without local persistence", slog.Any("err", err))
			} else {
				sessionTracker = t
				logger.Info("session tracker opened", slog.String("path", filepath.Join(dbDir, "sessions.db")))
			}
		}
	}

	p := proxy.New(gov,
		proxy.WithLogger(logger),
		proxy.WithMaxRequestSize(cfg.Governor.MaxRequestSize),
		proxy.WithMetrics(tp.Metrics),
		proxy.WithOptimizeTimeout(cfg.Governor.OptimizeTimeout),
		proxy.WithUsageTracker(usageTracker),
		proxy.WithAgentSessionStore(agentSessions),
		proxy.WithSessionPersist(sessionTracker),
		proxy.WithSupabaseStats(cfg.Supabase.URL, cfg.Supabase.ServiceKey),
		proxy.WithClaudeMessages(proxy.ClaudeMessagesConfig{
			Mode:                 cfg.ClaudeCode.Mode,
			DevToken:             cfg.ClaudeCode.DevToken,
			AnthropicAPIKey:      cfg.ClaudeCode.AnthropicAPIKey,
			AnthropicBaseURL:     cfg.ClaudeCode.AnthropicBaseURL,
			AnthropicVersion:     cfg.ClaudeCode.AnthropicVersion,
			EnableLogPruner:      cfg.ClaudeCode.EnableLogPruner,
			EnableBlockOptimizer: cfg.ClaudeCode.EnableBlockOptimizer,
			Optimizer: proxy.OptimizerConfig{
				MinSpanBytes:            cfg.ClaudeCode.OptMinSpanBytes,
				TargetChunkBytes:        cfg.ClaudeCode.OptTargetChunkBytes,
				MaxChunkBytes:           cfg.ClaudeCode.OptMaxChunkBytes,
				MinSavedTokens:          cfg.ClaudeCode.OptMinSavedTokens,
				EnableToolResultPruning: cfg.ClaudeCode.OptEnableToolResultPruning,
				EnableAssistantPruning:  cfg.ClaudeCode.OptEnableAssistantPruning,
				EnableSystemPruning:     cfg.ClaudeCode.OptEnableSystemPruning,
				Diagnostics:             cfg.ClaudeCode.OptDiagnostics,
			},
			Bedrock: proxy.BedrockConfig{
				Enabled:       cfg.ClaudeCode.BedrockEnabled,
				Region:        cfg.ClaudeCode.BedrockRegion,
				ModelPrefix:   cfg.ClaudeCode.BedrockModelPrefix,
				ModelOverride: cfg.ClaudeCode.BedrockModelOverride,
				Client:        bedrockClient,
				Models:        bedrockModels,
			},
			SessionStore: claudeStore,
			HTTPClient:   upstreamClient,
		}),
	)

	publicServer := buildPublicServer(cfg, p, tp, logger)
	adminServer := buildAdminServer(cfg, tp, logger)

	serverErr := make(chan error, 2)
	go func() {
		listenAddr := publicServer.Addr
		if publicListener != nil {
			listenAddr = publicListener.Addr().String()
		}
		logger.Info("public server listening", slog.String("addr", listenAddr))

		var err error
		if publicListener != nil {
			err = publicServer.Serve(publicListener)
		} else {
			err = publicServer.ListenAndServe()
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- fmt.Errorf("public server: %w", err)
		}
	}()
	go func() {
		logger.Info("admin server listening", slog.String("addr", adminServer.Addr))
		if err := adminServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- fmt.Errorf("admin server: %w", err)
		}
	}()

	janitorCtx, stopJanitor := context.WithCancel(context.Background())
	go runMemoryJanitor(janitorCtx, claudeStore, agentSessions, memoryJanitorInterval, logger)

	// Block until ctx is cancelled or a server fails fatally.
	var runErr error
	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-serverErr:
		runErr = err
		logger.Error("server failure", slog.Any("err", err))
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()

	if err := publicServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("public server shutdown", slog.Any("err", err))
	}
	if err := adminServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("admin server shutdown", slog.Any("err", err))
	}
	stopJanitor()
	if sessionTracker != nil {
		if err := sessionTracker.Close(); err != nil {
			logger.Error("session tracker close", slog.Any("err", err))
		}
	}
	if err := tp.Shutdown(shutdownCtx); err != nil {
		logger.Error("telemetry shutdown", slog.Any("err", err))
	}
	logger.Info("gateway exited cleanly")
	return runErr
}

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

func runMemoryJanitor(ctx context.Context, store *memory.Store, sessions *telemetry.AgentSessionStore, interval time.Duration, logger *slog.Logger) {
	if store == nil || interval <= 0 {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			store.CleanupExpired()
			if sessions != nil {
				sessions.CleanupExpired()
			}
		}
	}
}

func buildPublicServer(cfg *config.AppConfig, p *proxy.Proxy, tp *telemetry.Provider, logger *slog.Logger) *http.Server {
	handler := middleware.Chain(p.Mux(),
		middleware.CORS(middleware.CORSConfig{
			Enabled:               cfg.Server.CORSEnabled,
			AllowedOrigins:        cfg.Server.CORSAllowedOrigins,
			AllowChromeExtensions: cfg.Server.CORSAllowChromeExtensions,
			MaxAge:                cfg.Server.CORSMaxAge,
		}),
		func(next http.Handler) http.Handler { return otelhttp.NewHandler(next, "gateway") },
		middleware.RouteResolver(p.Mux()),
		middleware.RequestID,
		middleware.Recovery(logger, tp.Metrics),
		middleware.Logging(logger, cfg.Server.TrustedProxies),
		middleware.Metrics(tp.Metrics),
	)
	addr := cfg.Server.BindAddr
	if addr == "" {
		addr = ":" + cfg.Server.Port
	}
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout,
		ReadTimeout:       cfg.Server.ReadTimeout,
		WriteTimeout:      cfg.Server.WriteTimeout,
		IdleTimeout:       cfg.Server.IdleTimeout,
	}
}

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
