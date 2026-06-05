package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRateLimiterAllowed(t *testing.T) {
	rl := NewRateLimiter(1, 2, time.Minute)

	// First two requests succeed (burst = 2).
	if ok, _ := rl.Allowed("1.2.3.4"); !ok {
		t.Fatal("expected first request to succeed")
	}
	if ok, _ := rl.Allowed("1.2.3.4"); !ok {
		t.Fatal("expected second request to succeed")
	}

	// Third request fails.
	if ok, after := rl.Allowed("1.2.3.4"); ok {
		t.Fatalf("expected third request to fail, retryAfter=%f", after)
	}
}

func TestRateLimiterReplenish(t *testing.T) {
	rl := NewRateLimiter(10, 1, time.Minute)

	if ok, _ := rl.Allowed("1.2.3.4"); !ok {
		t.Fatal("expected first request to succeed")
	}
	if ok, _ := rl.Allowed("1.2.3.4"); ok {
		t.Fatal("expected second request to fail immediately")
	}

	time.Sleep(120 * time.Millisecond) // wait for 1 token at 10 tps

	if ok, _ := rl.Allowed("1.2.3.4"); !ok {
		t.Fatal("expected request to succeed after replenish")
	}
}

func TestRateLimitMiddlewareExemptsHealthz(t *testing.T) {
	mw := RateLimit(NewRateLimiter(1, 0, time.Minute), "/v1/", nil)
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.RemoteAddr = "192.168.1.1:1234"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for /healthz, got %d", rec.Code)
	}
}

func TestRateLimitMiddlewareExemptsLocalhost(t *testing.T) {
	mw := RateLimit(NewRateLimiter(1, 0, time.Minute), "/v1/", nil)
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/v1/messages", nil)
	req.RemoteAddr = "127.0.0.1:1234"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for localhost, got %d", rec.Code)
	}
}

func TestRateLimitMiddlewareReturns429(t *testing.T) {
	mw := RateLimit(NewRateLimiter(1, 0, time.Minute), "/v1/", nil)
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/v1/messages", nil)
	req.RemoteAddr = "192.168.1.1:1234"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("expected Retry-After header")
	}
}
