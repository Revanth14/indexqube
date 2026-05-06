package middleware

import (
	"log/slog"
	"net/http"
	"runtime/debug"

	"github.com/Revanth14/indexqube/gateway/internal/telemetry"
)

// Recovery returns middleware that catches panics from any downstream
// handler, logs them with full stack trace, and emits a generic JSON
// 500 error envelope so streaming clients receive a clean wire response
// instead of a hung connection.
//
// If response headers were already committed (e.g. mid-stream panic),
// WriteHeader is a no-op; the connection will be torn down by the http
// server. There is no perfect recovery from a mid-stream panic -- the
// bound on damage is "this one connection" thanks to the per-request
// goroutine model.
func Recovery(logger *slog.Logger, m *telemetry.Metrics) Middleware {
	const errBody = `{"error":{"type":"server_error","code":"internal_error","message":"internal server error"}}`
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
				)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(errBody))
			}()
			next.ServeHTTP(w, r)
		})
	}
}
