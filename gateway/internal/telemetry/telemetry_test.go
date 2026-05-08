package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Revanth14/indexqube/gateway/internal/config"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestInit_NoopWhenNoOTLPEndpoint(t *testing.T) {
	t.Parallel()
	p, err := Init(context.Background(), config.TelemetryConfig{
		ServiceName:    "test",
		MetricsEnabled: true,
		LogLevel:       "info",
	})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	defer p.Shutdown(context.Background())
	if p.Tracer == nil {
		t.Fatal("Tracer is nil")
	}
	if p.Metrics == nil {
		t.Fatal("Metrics is nil")
	}
}

func TestPrometheusHandler_ExposesGatewayMetrics(t *testing.T) {
	t.Parallel()
	p, err := Init(context.Background(), config.TelemetryConfig{
		ServiceName:    "test",
		MetricsEnabled: true,
	})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	defer p.Shutdown(context.Background())

	// Stir the pot so each metric has at least one sample.
	// (Histograms only export their series after the first observation.)
	p.Metrics.HTTPRequestsTotal.WithLabelValues("GET", "/healthz", "200").Inc()
	p.Metrics.HTTPDuration.WithLabelValues("GET", "/healthz").Observe(0.001)
	p.Metrics.PanicsTotal.Inc()
	p.Metrics.OptimizerRequests.WithLabelValues("optimize").Inc()
	p.Metrics.OptimizerBytes.WithLabelValues("optimize", "before").Add(100)
	p.Metrics.OptimizerBytes.WithLabelValues("optimize", "after").Add(25)
	p.Metrics.OptimizerTokens.WithLabelValues("optimize", "before").Add(25)
	p.Metrics.OptimizerTokens.WithLabelValues("optimize", "after").Add(7)
	p.Metrics.OptimizerBlocks.WithLabelValues("optimize", "seen").Add(1)
	p.Metrics.OptimizerBlocks.WithLabelValues("optimize", "pruned").Add(1)
	p.Metrics.OptimizerDiffs.WithLabelValues("optimize", "exact").Add(1)
	p.Metrics.OptimizerReduction.WithLabelValues("optimize").Observe(0.75)
	p.Metrics.StreamCancellations.Inc()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	p.PrometheusHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200", rec.Code)
	}
	body := rec.Body.String()
	wantSubstrings := []string{
		"iq_http_requests_total",
		`iq_http_requests_total{method="GET",route="/healthz",status="200"} 1`,
		"iq_runtime_panics_total 1",
		"iq_http_request_duration_seconds",
		"iq_stream_active",
		"iq_optimizer_requests_total",
		`iq_optimizer_bytes_total{source="optimize",stage="before"} 100`,
		`iq_optimizer_tokens_total{source="optimize",stage="after"} 7`,
		`iq_optimizer_blocks_total{result="pruned",source="optimize"} 1`,
		`iq_optimizer_diffs_total{mode="exact",source="optimize"} 1`,
		"iq_optimizer_reduction_ratio",
		"iq_stream_cancellations_total 1",
		"go_goroutines",             // from Go collector
		"process_cpu_seconds_total", // from process collector
	}
	for _, s := range wantSubstrings {
		if !strings.Contains(body, s) {
			t.Errorf("missing %q in /metrics output", s)
		}
	}
}

func TestTracingHandler_InjectsTraceIDsInsideSpan(t *testing.T) {
	t.Parallel()

	// Wire a real SDK tracer with an in-memory exporter so SpanContext.IsValid() is true.
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	prevTP := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prevTP)

	var buf bytes.Buffer
	base := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(&tracingHandler{inner: base})

	tracer := tp.Tracer("test")
	ctx, span := tracer.Start(context.Background(), "op")
	logger.InfoContext(ctx, "in span")
	span.End()
	logger.InfoContext(context.Background(), "out of span")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d log lines, want 2", len(lines))
	}

	var inSpan map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &inSpan); err != nil {
		t.Fatalf("parse line 0: %v", err)
	}
	if _, ok := inSpan["trace_id"]; !ok {
		t.Error("first log line missing trace_id (should be present inside span)")
	}
	if _, ok := inSpan["span_id"]; !ok {
		t.Error("first log line missing span_id")
	}

	var outSpan map[string]any
	if err := json.Unmarshal([]byte(lines[1]), &outSpan); err != nil {
		t.Fatalf("parse line 1: %v", err)
	}
	if _, ok := outSpan["trace_id"]; ok {
		t.Error("second log line should NOT have trace_id (no active span)")
	}
}

func TestTracingHandler_RedactsSensitiveAttrs(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	base := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(&tracingHandler{inner: base})

	logger.InfoContext(context.Background(), "secret check",
		slog.String("X-IQ-Provider-Key", "sk-provider1234567890"),
		slog.String("err", "Authorization: Bearer sk-bearer1234567890"),
		slog.Group("headers", slog.String("api-key", "sk-nested1234567890")),
	)

	line := buf.String()
	for _, leak := range []string{"sk-provider1234567890", "sk-bearer1234567890", "sk-nested1234567890"} {
		if strings.Contains(line, leak) {
			t.Fatalf("log leaked %q: %s", leak, line)
		}
	}
	if !strings.Contains(line, "[redacted]") {
		t.Fatalf("redaction marker missing: %s", line)
	}
}

func TestShutdown_IsIdempotentWithNoExporter(t *testing.T) {
	t.Parallel()
	p, err := Init(context.Background(), config.TelemetryConfig{ServiceName: "test"})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
}
