package middleware

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/telemetry"
)

// Metrics returns middleware that records HTTP-level Prometheus metrics:
// request count by status, latency histogram, and active-streams gauge
// for streaming endpoints.
//
// The route label uses the matched ServeMux pattern (stamped onto ctx
// by RouteResolver, which MUST run earlier in the chain) instead of
// r.URL.Path to keep cardinality bounded. Without this, /metrics would
// see one time series per distinct URL -- catastrophic for the TSDB.
func Metrics(m *telemetry.Metrics) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			route := RouteFromContext(r.Context())
			if route == "" {
				// RouteResolver hasn't run, or this request didn't match a
				// registered pattern. Bucket as "unknown" to avoid leaking
				// raw URLs into label cardinality.
				route = "unknown"
			}

			if isStreamingRoute(route) {
				m.StreamsActive.Inc()
				defer m.StreamsActive.Dec()
			}

			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w}
			next.ServeHTTP(rec, r)

			status := rec.status
			if status == 0 {
				status = http.StatusOK
			}

			m.HTTPRequestsTotal.WithLabelValues(r.Method, route, strconv.Itoa(status)).Inc()
			m.HTTPDuration.WithLabelValues(r.Method, route).Observe(time.Since(start).Seconds())
		})
	}
}

// isStreamingRoute is a coarse classifier used to gate the streams-active
// gauge. It matches our registered streaming patterns regardless of
// method prefix ("POST /v1/chat/completions").
func isStreamingRoute(route string) bool {
	return strings.HasSuffix(route, "/v1/chat/completions") ||
		strings.HasSuffix(route, "/v1/messages")
}

// RouteResolver returns Middleware that, given the proxy's mux, looks up
// the matched ServeMux pattern for each request and stamps it onto the
// request context. This lets downstream middleware (Logging, Metrics)
// label by route -- a bounded label -- instead of by raw URL path.
//
// PLACEMENT: must run BEFORE any middleware that reads the route from
// context. In the standard chain, that's directly outside Metrics and
// Logging. mux.Handler() does pattern lookup only -- it does not invoke
// the actual handler -- so this is cheap.
func RouteResolver(mux *http.ServeMux) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, pattern := mux.Handler(r)
			ctx := withRoute(r.Context(), pattern)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
