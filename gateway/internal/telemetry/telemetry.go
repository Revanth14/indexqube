// Package telemetry initializes and tears down the gateway's
// observability stack: OpenTelemetry traces, Prometheus metrics, and
// trace-aware structured logging.
//
// The package exposes a Provider that callers wire into HTTP middleware
// and graceful-shutdown paths. Application code outside this package
// should depend on the OTel API and slog only — never on the SDK or
// the Prometheus internals -- so that the observability backend can be
// swapped without touching business logic.
package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/trace"
)

// Sink is the minimal telemetry event sink used by the gateway and the iq wrapper.
// Implementations must be non-blocking and must never panic on the request path.
type Sink interface {
	Track(event UsageEvent)
}

// UsageEvent is the privacy-safe event shape persisted for product telemetry.
// It intentionally contains only metadata, counters, and aggregate optimizer stats.
// Do not add raw prompts, raw code, terminal output, provider keys, or file contents here.
type UsageEvent struct {
	MachineID            string         `json:"machine_id"`
	OsArch               string         `json:"os_arch"`
	IqVersion            string         `json:"iq_version"`
	CliAgent             string         `json:"cli_agent"`
	ModelTarget          string         `json:"model_target"`
	InputTokensAttempted int            `json:"input_tokens_attempted"`
	InputTokensSent      int            `json:"input_tokens_sent"`
	TokensSaved          int            `json:"tokens_saved"`
	ReductionRatio       float64        `json:"reduction_ratio"`
	BlocksAnalyzed       int            `json:"blocks_analyzed"`
	BlocksPruned         int            `json:"blocks_pruned"`
	ToolTypesSeen        []string       `json:"tool_types_seen"`
	SkipReasons          map[string]int `json:"skip_reasons"`
	TotalLatencyMs       int            `json:"total_latency_ms"`
	ProxyOverheadMs      float64        `json:"proxy_overhead_ms"`
	UpstreamStatus       int            `json:"upstream_status"`
}

// SupabaseClient writes telemetry events to Supabase through the REST API.
// This client is server-side only. Do not wire it into distributed client binaries.
type SupabaseClient struct {
	url        string
	serviceKey string
	httpClient *http.Client
}

// NewSupabaseClient constructs a server-side Supabase telemetry sink.
func NewSupabaseClient(url, serviceKey string) *SupabaseClient {
	return &SupabaseClient{
		url:        strings.TrimRight(url, "/"),
		serviceKey: serviceKey,
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}
}

// Track asynchronously persists a usage event. It is intentionally fire-and-forget
// so telemetry can never block Claude Code streaming or the gateway request path.
func (s *SupabaseClient) Track(event UsageEvent) {
	if s == nil || s.url == "" || s.serviceKey == "" {
		return
	}

	// Enterprise/privacy escape hatch.
	if os.Getenv("IQ_TELEMETRY") == "off" {
		return
	}

	go func() {
		body, err := json.Marshal(event)
		if err != nil {
			return
		}

		req, err := http.NewRequestWithContext(
			context.Background(),
			http.MethodPost,
			fmt.Sprintf("%s/rest/v1/usage_events", s.url),
			bytes.NewReader(body),
		)
		if err != nil {
			return
		}

		req.Header.Set("apikey", s.serviceKey)
		req.Header.Set("Authorization", "Bearer "+s.serviceKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Prefer", "return=minimal")

		resp, err := s.httpClient.Do(req)
		if err != nil {
			return
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
	}()
}

// Provider holds the wired observability primitives. Construct via Init,
// drain via Shutdown.
type Provider struct {
	Tracer   trace.Tracer
	Metrics  *Metrics
	Logger   *slog.Logger
	registry *prometheus.Registry

	shutdownFns []func(context.Context) error
}

// Init wires up tracer, meter, logger, and metrics registry from cfg.
//
// If cfg.OTLPEndpoint is empty, a no-op tracer is installed -- the rest
// of the codebase still calls Start/End and the calls are free.
//
// The returned Provider's Shutdown MUST be called before process exit
// so buffered spans are flushed.
func Init(ctx context.Context, cfg config.TelemetryConfig) (*Provider, error) {
	logger := buildLogger(cfg)

	tracerProvider, traceShutdown, err := buildTracerProvider(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("tracer provider: %w", err)
	}

	registry := prometheus.NewRegistry()
	if cfg.MetricsEnabled {
		registry.MustRegister(
			prometheus.NewGoCollector(),
			prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
		)
	}
	metrics := newMetrics(registry)

	p := &Provider{
		Tracer:   tracerProvider.Tracer(cfg.ServiceName),
		Metrics:  metrics,
		Logger:   logger,
		registry: registry,
	}
	if traceShutdown != nil {
		p.shutdownFns = append(p.shutdownFns, traceShutdown)
	}

	logger.Info("telemetry initialized",
		slog.String("service", cfg.ServiceName),
		slog.String("version", cfg.ServiceVersion),
		slog.Bool("traces_enabled", cfg.OTLPEndpoint != ""),
		slog.Bool("metrics_enabled", cfg.MetricsEnabled),
	)

	return p, nil
}

// PrometheusHandler returns an http.Handler exposing the gathered metrics
// in Prometheus exposition format. Mount on the admin port, NOT on the
// public inference port.
func (p *Provider) PrometheusHandler() http.Handler {
	return promhttp.HandlerFor(p.registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}

// Shutdown drains pending telemetry. It runs all shutdown functions and
// returns the first error, but always attempts every shutdown so a
// failing tracer doesn't strand other resources.
func (p *Provider) Shutdown(ctx context.Context) error {
	var firstErr error
	for _, fn := range p.shutdownFns {
		if err := fn(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func buildLogger(cfg config.TelemetryConfig) *slog.Logger {
	level := parseLogLevel(cfg.LogLevel)
	// Logs go to stderr so they never corrupt stdout when the gateway is
	// embedded inside the iq wrapper (where Claude Code owns stdout).
	base := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	})
	logger := slog.New(&tracingHandler{inner: base})
	slog.SetDefault(logger)
	return logger
}

func parseLogLevel(s string) slog.Level {
	switch s {
	case "debug", "DEBUG":
		return slog.LevelDebug
	case "warn", "WARN":
		return slog.LevelWarn
	case "error", "ERROR":
		return slog.LevelError
	case "off", "OFF", "none", "NONE":
		return slog.Level(100) // above all standard levels — effectively silent
	default:
		return slog.LevelInfo
	}
}
