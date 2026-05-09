package middleware

import (
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"
)

// Logging returns middleware that emits one structured access log line per
// request at handler completion. Trace and span IDs are added automatically
// by the telemetry slog handler when present.
//
// trustedProxies is a list of IP addresses or CIDR ranges (e.g. "10.0.0.0/8")
// from which X-Forwarded-For headers are trusted as the real client IP.
// When empty, r.RemoteAddr is always used directly.
//
// Note: the log fires AFTER the wrapped handler returns. For long streams that
// means the line is delayed until the stream completes; this is the right
// tradeoff (it includes the final status and total bytes) but means access
// logs lag real-time. For per-request span data, use the trace exporter.
func Logging(logger *slog.Logger, trustedProxies []string) Middleware {
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
				slog.String("remote_addr", clientIP(r, trustedProxies)),
				slog.String("request_id", RequestIDFromContext(r.Context())),
				slog.String("user_agent", r.UserAgent()),
			)
		})
	}
}

// clientIP returns the most plausible client IP. X-Forwarded-For is only
// trusted when the direct peer (r.RemoteAddr) is in trustedProxies; otherwise
// r.RemoteAddr is returned directly to prevent IP spoofing.
func clientIP(r *http.Request, trustedProxies []string) string {
	if len(trustedProxies) > 0 {
		remoteHost, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			remoteHost = r.RemoteAddr
		}
		if isTrustedProxy(remoteHost, trustedProxies) {
			if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
				// Take only the leftmost (original client) IP from a CSV chain.
				if first, _, found := strings.Cut(xff, ","); found {
					return strings.TrimSpace(first)
				}
				return strings.TrimSpace(xff)
			}
		}
	}
	return r.RemoteAddr
}

// isTrustedProxy reports whether host matches any entry in trustedProxies.
// Entries may be exact IP strings or CIDR ranges.
func isTrustedProxy(host string, trustedProxies []string) bool {
	ip := net.ParseIP(host)
	for _, trusted := range trustedProxies {
		if trusted == host {
			return true
		}
		if _, cidr, err := net.ParseCIDR(trusted); err == nil && ip != nil && cidr.Contains(ip) {
			return true
		}
	}
	return false
}
