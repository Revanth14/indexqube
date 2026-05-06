package telemetry

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// tracingHandler decorates a slog.Handler so every log record made
// inside an active span automatically carries trace_id and span_id
// attributes. This is what closes the loop between traces and logs:
// jump from a Prometheus alert -> trace -> the exact log lines for
// that request.
//
// It is a thin wrapper -- no allocations on the hot path when the
// context has no span.
type tracingHandler struct {
	inner slog.Handler
}

func (h *tracingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *tracingHandler) Handle(ctx context.Context, r slog.Record) error {
	if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
		sc := span.SpanContext()
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.inner.Handle(ctx, r)
}

func (h *tracingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &tracingHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *tracingHandler) WithGroup(name string) slog.Handler {
	return &tracingHandler{inner: h.inner.WithGroup(name)}
}
