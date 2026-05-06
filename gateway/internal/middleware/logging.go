package middleware

import (
	"log/slog"
	"net/http"
	"time"
)

// Logging returns middleware that emits one structured access log line
// per request at handler completion. Trace and span IDs are added
// automatically by the telemetry slog handler when present.
//
// Note: the log fires AFTER the wrapped handler returns. For long
// streams that means the line is delayed until the stream completes;
// this is the right tradeoff (it includes the final status and total
// bytes) but means access logs lag real-time. For per-request span
// data, use the trace exporter.
func Logging(logger *slog.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w}
			next.ServeHTTP(rec, r)

			dur := time.Since(start)
			status := rec.status
			if status == 0 {
				status = http.StatusOK
			}

			level := slog.LevelInfo
			switch {
			case status >= 500:
				level = slog.LevelError
			case status >= 400:
				level = slog.LevelWarn
			}

			logger.LogAttrs(r.Context(), level, "http request",
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.String("route", RouteFromContext(r.Context())),
				slog.Int("status", status),
				slog.Int64("bytes_out", rec.bytes),
				slog.Duration("duration", dur),
				slog.String("remote_addr", clientIP(r)),
				slog.String("request_id", RequestIDFromContext(r.Context())),
				slog.String("user_agent", r.UserAgent()),
			)
		})
	}
}

// clientIP returns the most plausible client IP. It prefers a single
// X-Forwarded-For value when behind a trusted proxy; otherwise falls
// back to RemoteAddr. We do NOT split CSV X-Forwarded-For chains here
// because that's an auth concern -- a future trusted-proxy middleware
// will canonicalize.
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return xff
	}
	return r.RemoteAddr
}
