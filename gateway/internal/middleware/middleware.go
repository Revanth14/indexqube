// Package middleware contains HTTP middleware that wires the
// observability stack (request IDs, panic recovery, access logging,
// Prometheus metrics) around the proxy handler.
//
// The middleware chain order in main.go is significant -- see
// Chain's docstring for the rule of thumb.
package middleware

import (
	"context"
	"net/http"
)

// ctxKey is an unexported type for context keys defined in this package,
// avoiding collisions with keys from other packages.
type ctxKey int

const (
	ctxKeyRequestID ctxKey = iota
	ctxKeyRoute
)

// Middleware is the standard wrapper signature.
type Middleware func(http.Handler) http.Handler

// Chain composes middlewares around a handler. The first middleware in
// the slice becomes the OUTERMOST layer (sees the request first, sees
// the response last). Order rule of thumb:
//
//	Chain(handler,
//	    Tracing,    // outermost: trace context spans everything
//	    Recovery,   // catches panics from everything below
//	    RequestID,  // request ID flows into logs and downstream
//	    Logging,    // sees final status from metrics layer
//	    Metrics,    // wraps response writer to capture status
//	)
func Chain(handler http.Handler, mws ...Middleware) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		handler = mws[i](handler)
	}
	return handler
}

// RequestIDFromContext returns the request ID stamped on ctx by RequestID.
// Returns "" if no middleware has run yet (e.g. in unit tests).
func RequestIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKeyRequestID).(string); ok {
		return v
	}
	return ""
}

// RouteFromContext returns the matched ServeMux pattern for the current
// request, e.g. "POST /v1/chat/completions". Returns "" if not yet set.
//
// This is used by the Metrics middleware to label time series by route
// instead of by raw URL (which would explode cardinality).
func RouteFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKeyRoute).(string); ok {
		return v
	}
	return ""
}

// withRequestID returns ctx with the given request ID stamped on it.
func withRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKeyRequestID, id)
}

// withRoute returns ctx with the matched route stamped on it.
func withRoute(ctx context.Context, route string) context.Context {
	return context.WithValue(ctx, ctxKeyRoute, route)
}

// statusRecorder is a thin response-writer wrapper that captures the
// final HTTP status and bytes-written count. It implements Unwrap so
// http.NewResponseController correctly walks past it to find the
// underlying flushable writer -- critical for SSE streaming.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	bytes       int64
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.status = code
		r.wroteHeader = true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.status = http.StatusOK
		r.wroteHeader = true
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += int64(n)
	return n, err
}

// Unwrap exposes the inner ResponseWriter so http.NewResponseController
// (Go 1.20+) can chain to its Flush / Hijack / SetWriteDeadline methods.
func (r *statusRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}
