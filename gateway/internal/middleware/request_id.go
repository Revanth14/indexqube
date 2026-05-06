package middleware

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"regexp"
)

const (
	headerRequestID = "X-Request-ID"

	// requestIDMaxLen guards against attacker-supplied IDs blowing up
	// log lines. 64 chars is roomy enough for UUIDs and our own hex IDs.
	requestIDMaxLen = 64
)

// requestIDPattern accepts only safe characters for log/header injection.
var requestIDPattern = regexp.MustCompile(`^[a-zA-Z0-9_\-]+$`)

// RequestID is middleware that ensures every request has a unique ID,
// stamps it onto the request context, and echoes it in the response
// header so callers can correlate logs across the stack.
//
// Behavior:
//   - If the client supplied X-Request-ID and it passes the safety check,
//     it is honored and propagated.
//   - Otherwise a fresh 16-byte hex ID is generated.
//   - The chosen ID is always echoed back in X-Request-ID.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(headerRequestID)
		if !isSafeRequestID(id) {
			id = newRequestID()
		}
		w.Header().Set(headerRequestID, id)
		ctx := withRequestID(r.Context(), id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func isSafeRequestID(id string) bool {
	if id == "" || len(id) > requestIDMaxLen {
		return false
	}
	return requestIDPattern.MatchString(id)
}

func newRequestID() string {
	var b [16]byte
	// crypto/rand.Read on modern Go is documented to never return a
	// short read or error; a fallback is unnecessary.
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
