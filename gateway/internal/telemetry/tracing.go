package telemetry

import (
	"context"
	"fmt"

	"github.com/Revanth14/indexqube/gateway/internal/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// buildTracerProvider returns a TracerProvider and an optional shutdown
// function. When cfg.OTLPEndpoint is empty, a noop provider is returned
// so trace calls are zero-cost.
func buildTracerProvider(ctx context.Context, cfg config.TelemetryConfig) (trace.TracerProvider, func(context.Context) error, error) {
	if cfg.OTLPEndpoint == "" {
		tp := tracenoop.NewTracerProvider()
		otel.SetTracerProvider(tp)
		// W3C TraceContext propagation is wired even in noop mode so
		// upstream-sent trace headers are still parsed and forwarded.
		otel.SetTextMapPropagator(propagation.TraceContext{})
		return tp, nil, nil
	}

	exporterOpts := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(cfg.OTLPEndpoint),
	}
	if cfg.OTLPInsecure {
		exporterOpts = append(exporterOpts, otlptracehttp.WithInsecure())
	}

	exporter, err := otlptracehttp.New(ctx, exporterOpts...)
	if err != nil {
		return nil, nil, fmt.Errorf("otlp exporter: %w", err)
	}

	res, err := sdkresource.New(ctx,
		sdkresource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceVersion(cfg.ServiceVersion),
		),
		sdkresource.WithTelemetrySDK(),
		sdkresource.WithProcess(),
		sdkresource.WithHost(),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp, tp.Shutdown, nil
}
