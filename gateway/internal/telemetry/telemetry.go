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
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/Revanth14/indexqube/gateway/internal/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/trace"
)

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
	default:
		return slog.LevelInfo
	}
}
