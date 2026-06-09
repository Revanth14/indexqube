package middleware

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// RateLimiter provides per-IP token-bucket rate limiting.
type RateLimiter struct {
	mu       sync.Mutex
	buckets  map[string]*bucket
	rate     float64 // tokens per second
	burst    int
	maxAge   time.Duration
	lastSeen map[string]time.Time
}

// bucket is a simple token bucket tracker.
type bucket struct {
	tokens    float64
	lastCheck time.Time
}

// NewRateLimiter creates a rate limiter with the given rate (req/sec), burst,
// and max age for stale bucket cleanup.
func NewRateLimiter(rate float64, burst int, maxAge time.Duration) *RateLimiter {
	rl := &RateLimiter{
		buckets:  make(map[string]*bucket),
		rate:     rate,
		burst:    burst,
		maxAge:   maxAge,
		lastSeen: make(map[string]time.Time),
	}
	go rl.cleanupLoop()
	return rl
}

// Allowed reports whether a request from the given IP is within the rate limit.
// It returns false and the seconds until the next token is available when
// the bucket is empty.
func (rl *RateLimiter) Allowed(ip string) (ok bool, retryAfter float64) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	rl.lastSeen[ip] = now

	b, exists := rl.buckets[ip]
	if !exists {
		b = &bucket{tokens: float64(rl.burst), lastCheck: now}
		rl.buckets[ip] = b
	}

	// Replenish tokens based on elapsed time.
	elapsed := now.Sub(b.lastCheck).Seconds()
	b.tokens += elapsed * rl.rate
	if b.tokens > float64(rl.burst) {
		b.tokens = float64(rl.burst)
	}
	b.lastCheck = now

	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}

	retryAfter = (1 - b.tokens) / rl.rate
	return false, retryAfter
}

// cleanupLoop removes stale buckets every maxAge interval.
func (rl *RateLimiter) cleanupLoop() {
	ticker := time.NewTicker(rl.maxAge)
	defer ticker.Stop()
	for range ticker.C {
		rl.mu.Lock()
		cutoff := time.Now().Add(-rl.maxAge)
		for ip, t := range rl.lastSeen {
			if t.Before(cutoff) {
				delete(rl.buckets, ip)
				delete(rl.lastSeen, ip)
			}
		}
		rl.mu.Unlock()
	}
}

// RateLimit returns middleware that rejects requests exceeding the per-IP rate
// with HTTP 429. Paths starting with prefix are rate-limited; others pass
// through. Localhost requests are exempt.
func RateLimit(limiter *RateLimiter, prefix string, trustedProxies []string) Middleware {
	if limiter == nil {
		return func(next http.Handler) http.Handler { return next }
	}
	isLoopback := checkLoopbackFunc(trustedProxies)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !strings.HasPrefix(r.URL.Path, prefix) {
				next.ServeHTTP(w, r)
				return
			}
			if isLoopback(r) {
				next.ServeHTTP(w, r)
				return
			}

			ip := clientIP(r, trustedProxies)
			ok, retryAfter := limiter.Allowed(ip)
			if ok {
				next.ServeHTTP(w, r)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", fmt.Sprintf("%.0f", retryAfter))
			w.WriteHeader(http.StatusTooManyRequests)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"error": map[string]interface{}{
					"type":    "rate_limit_error",
					"code":    "rate_limit_exceeded",
					"message": "too many requests; retry after " + fmt.Sprintf("%.0f", retryAfter) + " seconds",
				},
			})
		})
	}
}
