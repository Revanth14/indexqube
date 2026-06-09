package middleware

import (
	"encoding/json"
	"net"
	"net/http"
	"strings"
)

// Auth returns middleware that requires a valid Bearer token for requests
// from non-loopback addresses. Localhost (127.0.0.1, ::1) is exempt.
// trustedProxies is used to extract the real client IP from X-Forwarded-For.
func Auth(token string, trustedProxies []string) Middleware {
	if token == "" {
		return func(next http.Handler) http.Handler { return next }
	}

	isLoopback := checkLoopbackFunc(trustedProxies)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if isLoopback(r) {
				next.ServeHTTP(w, r)
				return
			}

			auth := strings.TrimSpace(r.Header.Get("Authorization"))
			got := strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
			if got != token {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				_ = json.NewEncoder(w).Encode(map[string]interface{}{
					"error": map[string]interface{}{
						"type":    "authentication_error",
						"code":    "missing_key",
						"message": "missing or invalid Bearer token",
					},
				})
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

func checkLoopbackFunc(trustedProxies []string) func(*http.Request) bool {
	return func(r *http.Request) bool {
		ip := resolveIP(r, trustedProxies)
		return ip.IsLoopback()
	}
}

func resolveIP(r *http.Request, trustedProxies []string) net.IP {
	ipStr := clientIP(r, trustedProxies)
	host, _, err := net.SplitHostPort(ipStr)
	if err != nil {
		host = ipStr
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return net.ParseIP("127.0.0.1")
	}
	return ip
}
