package middleware

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Revanth14/indexqube/gateway/internal/config"
	"github.com/Revanth14/indexqube/gateway/internal/telemetry"
)

// helper: fresh telemetry provider with a private registry so tests don't
// fight over global Prometheus state.
func newTelemetry(t *testing.T) *telemetry.Provider {
	t.Helper()
	p, err := telemetry.Init(context.Background(), config.TelemetryConfig{
		ServiceName: "test",
	})
	if err != nil {
		t.Fatalf("telemetry.Init: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func TestRequestID_GeneratesWhenAbsent(t *testing.T) {
	t.Parallel()
	var capturedID string
	h := RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedID = RequestIDFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	h.ServeHTTP(rec, req)

	if capturedID == "" {
		t.Fatal("no request ID generated")
	}
	if got := rec.Header().Get(headerRequestID); got != capturedID {
		t.Errorf("response header X-Request-ID=%q, want %q", got, capturedID)
	}
	if len(capturedID) != 32 {
		t.Errorf("got id len=%d, want 32 (16 bytes hex)", len(capturedID))
	}
}

func TestRequestID_HonorsClientID(t *testing.T) {
	t.Parallel()
	const clientID = "abc-123_DEF"
	var captured string
	h := RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = RequestIDFromContext(r.Context())
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(headerRequestID, clientID)
	h.ServeHTTP(rec, req)

	if captured != clientID {
		t.Errorf("got %q, want %q", captured, clientID)
	}
}

func TestRequestID_RejectsUnsafeClientID(t *testing.T) {
	t.Parallel()
	var captured string
	h := RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = RequestIDFromContext(r.Context())
	}))

	for _, bad := range []string{
		"has spaces",
		"has\nnewline",
		"<script>",
		strings.Repeat("a", 65), // exceeds max len
	} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set(headerRequestID, bad)
		h.ServeHTTP(rec, req)
		if captured == bad {
			t.Errorf("unsafe id %q was honored", bad)
		}
		if captured == "" {
			t.Errorf("no fallback id generated for unsafe input %q", bad)
		}
	}
}

func TestRecovery_CatchesPanicAndEmitsJSON(t *testing.T) {
	t.Parallel()
	tp := newTelemetry(t)
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	h := Recovery(logger, tp.Metrics)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status=%d, want 500", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type=%q, want application/json", ct)
	}
	var env map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("response not JSON: %v", err)
	}
	if !strings.Contains(buf.String(), "panic") {
		t.Error("expected log to include 'panic'")
	}
}

func TestLogging_EmitsAccessLog(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	h := Chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte("hello"))
	}), RequestID, Logging(logger))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/foo", nil)
	h.ServeHTTP(rec, req)

	var rec0 map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec0); err != nil {
		t.Fatalf("log line not JSON: %v\nbody=%q", err, buf.String())
	}
	if rec0["method"] != "GET" {
		t.Errorf("method=%v", rec0["method"])
	}
	if rec0["path"] != "/foo" {
		t.Errorf("path=%v", rec0["path"])
	}
	if rec0["status"].(float64) != float64(http.StatusTeapot) {
		t.Errorf("status=%v", rec0["status"])
	}
	if rec0["bytes_out"].(float64) != 5 {
		t.Errorf("bytes_out=%v, want 5", rec0["bytes_out"])
	}
	if id, _ := rec0["request_id"].(string); id == "" {
		t.Error("request_id missing")
	}
}

func TestMetrics_RecordsCounterAndHistogram(t *testing.T) {
	t.Parallel()
	tp := newTelemetry(t)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /widgets", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	})

	// RouteResolver runs BEFORE Metrics so the route is stamped on ctx
	// when Metrics reads it. Order: RouteResolver -> Metrics -> mux.
	h := Chain(mux, RouteResolver(mux), Metrics(tp.Metrics))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/widgets", nil)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status=%d, want 202", rec.Code)
	}

	scrape := scrapeMetrics(t, tp)
	wantSubstrings := []string{
		`iq_http_requests_total{method="GET",route="GET /widgets",status="202"} 1`,
		`iq_http_request_duration_seconds_count{method="GET",route="GET /widgets"} 1`,
	}
	for _, s := range wantSubstrings {
		if !strings.Contains(scrape, s) {
			t.Errorf("missing %q in /metrics output\nfull body:\n%s", s, scrape)
		}
	}
}

func TestStatusRecorder_UnwrapEnablesFlushing(t *testing.T) {
	t.Parallel()
	// Verify that http.NewResponseController can still flush through our
	// wrapped writer. This is the regression that breaks SSE streaming
	// if Unwrap isn't implemented.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /stream", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("chunk1"))
		rc := http.NewResponseController(w)
		if err := rc.Flush(); err != nil {
			t.Errorf("flush failed: %v", err)
		}
	})
	tp := newTelemetry(t)
	h := Chain(mux, RouteResolver(mux), Metrics(tp.Metrics), Logging(slog.New(slog.NewTextHandler(io.Discard, nil))))

	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/stream")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "chunk1" {
		t.Errorf("body=%q, want chunk1", string(body))
	}
}

func TestRouteResolver_StampsPattern(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	var captured string
	mux.HandleFunc("POST /v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		captured = RouteFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})

	h := Chain(mux, RouteResolver(mux))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	h.ServeHTTP(rec, req)

	if captured != "POST /v1/chat/completions" {
		t.Errorf("got route=%q, want %q", captured, "POST /v1/chat/completions")
	}
}

// scrapeMetrics renders the Prometheus exposition for assertions.
func scrapeMetrics(t *testing.T, tp *telemetry.Provider) string {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	tp.PrometheusHandler().ServeHTTP(rec, req)
	return rec.Body.String()
}
