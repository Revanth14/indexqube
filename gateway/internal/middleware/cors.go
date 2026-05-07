package middleware

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// CORSConfig controls browser access to the public gateway API.
type CORSConfig struct {
	Enabled               bool
	AllowedOrigins        []string
	AllowChromeExtensions bool
	MaxAge                time.Duration
}

var (
	corsAllowedMethods = []string{"GET", "POST", "OPTIONS"}
	corsAllowedHeaders = []string{
		"Content-Type",
		"X-Request-ID",
		"X-IQ-Session-Key",
		"X-IQ-Project-Memory",
		"X-IQ-Context-Path",
		"X-IQ-Context-Lang",
		"X-IQ-Provider",
		"X-IQ-Provider-Key",
	}
	corsExposedHeaders = []string{
		"X-Request-ID",
		"X-IQ-Contract-Version",
		"X-IQ-Mode",
		"X-IQ-Blocks-Seen",
		"X-IQ-Blocks-Pruned",
		"X-IQ-Blocks-Skipped",
		"X-IQ-Bytes-Before",
		"X-IQ-Bytes-After",
		"X-IQ-Bytes-Saved",
		"X-IQ-Tokens-Before",
		"X-IQ-Tokens-After",
		"X-IQ-Tokens-Saved",
		"X-IQ-Reduction-Ratio",
		"X-IQ-Diff-Exact",
		"X-IQ-Diff-Fallback",
		"X-IQ-Skip-Reasons",
	}
)

// CORS allows trusted browser clients, including the local Chrome extension,
// to call the gateway while keeping response stats headers readable.
func CORS(cfg CORSConfig) Middleware {
	allowed := make(map[string]struct{}, len(cfg.AllowedOrigins))
	for _, origin := range cfg.AllowedOrigins {
		origin = strings.TrimSpace(origin)
		if origin != "" {
			allowed[origin] = struct{}{}
		}
	}

	return func(next http.Handler) http.Handler {
		if !cfg.Enabled {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := strings.TrimSpace(r.Header.Get("Origin"))
			if origin == "" {
				next.ServeHTTP(w, r)
				return
			}
			if !corsOriginAllowed(origin, allowed, cfg.AllowChromeExtensions) {
				if r.Method == http.MethodOptions {
					http.Error(w, "cors origin not allowed", http.StatusForbidden)
					return
				}
				next.ServeHTTP(w, r)
				return
			}

			writeCORSHeaders(w, origin, cfg.MaxAge)
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func corsOriginAllowed(origin string, allowed map[string]struct{}, allowChromeExtensions bool) bool {
	if _, ok := allowed[origin]; ok {
		return true
	}
	return allowChromeExtensions && strings.HasPrefix(origin, "chrome-extension://")
}

func writeCORSHeaders(w http.ResponseWriter, origin string, maxAge time.Duration) {
	h := w.Header()
	h.Set("Access-Control-Allow-Origin", origin)
	h.Set("Access-Control-Allow-Methods", strings.Join(corsAllowedMethods, ", "))
	h.Set("Access-Control-Allow-Headers", strings.Join(corsAllowedHeaders, ", "))
	h.Set("Access-Control-Expose-Headers", strings.Join(corsExposedHeaders, ", "))
	if maxAge > 0 {
		h.Set("Access-Control-Max-Age", strconv.Itoa(int(maxAge.Seconds())))
	}
	h.Add("Vary", "Origin")
}
