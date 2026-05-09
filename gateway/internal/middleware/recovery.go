package middleware

import (
	"log/slog"
	"net/http"
	"runtime/debug"

	"github.com/Revanth14/indexqube/gateway/internal/telemetry"
)

// Recovery returns middleware that catches panics from any downstream
// handler, logs them with full stack trace, and emits a generic JSON
// 500 error envelope so non-streaming clients receive a clean wire
// response instead of a hung connection.
//
// Mid-stream panics are detected via a wrapping ResponseWriter: once any
// bytes or headers have been committed to the wire, the recovery path
// only logs and bumps the metric. Writing a JSON envelope into a
// half-written SSE event would corrupt the stream; the http server tears
// the connection down instead, which is the correct bound on damage.
func Recovery(logger *slog.Logger, m *telemetry.Metrics) Middleware {
	const errBody = `{"error":{"type":"server_error","code":"internal_error","message":"internal server error"}}`
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			cw := &committingWriter{ResponseWriter: w}
			defer func() {
				rec := recover()
				if rec == nil {
					return
				}
				if m != nil {
					m.PanicsTotal.Inc()
				}
				logger.ErrorContext(r.Context(), "panic in handler",
					slog.Any("panic", rec),
					slog.String("stack", string(debug.Stack())),
					slog.String("request_id", RequestIDFromContext(r.Context())),
					slog.String("method", r.Method),
					slog.String("path", r.URL.Path),
					slog.Bool("response_committed", cw.committed),
				)
				if cw.committed {
					return
				}
				cw.Header().Set("Content-Type", "application/json")
				cw.WriteHeader(http.StatusInternalServerError)
				_, _ = cw.Write([]byte(errBody))
			}()
			next.ServeHTTP(cw, r)
		})
	}
}

// committingWriter wraps http.ResponseWriter to track whether any bytes
// or headers have been sent. It also forwards Flush so SSE handlers
// downstream still get the *http.response Flusher behavior.
type committingWriter struct {
	http.ResponseWriter
	committed bool
}

func (w *committingWriter) WriteHeader(code int) {
	w.committed = true
	w.ResponseWriter.WriteHeader(code)
}

func (w *committingWriter) Write(b []byte) (int, error) {
	w.committed = true
	return w.ResponseWriter.Write(b)
}

func (w *committingWriter) Flush() {
	w.committed = true
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *committingWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}
