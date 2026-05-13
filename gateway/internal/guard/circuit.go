package guard

import (
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const maxCircuitKeys = 10_000

type CircuitBreaker struct {
	mu     sync.Mutex
	cfg    Config
	events map[string][]event
}

type event struct {
	At     time.Time
	Tokens int
}

func NewCircuitBreaker(cfg Config) *CircuitBreaker {
	return &CircuitBreaker{
		cfg:    cfg,
		events: make(map[string][]event),
	}
}

func (cb *CircuitBreaker) Check(sig RequestSignal) (decision Decision) {
	defer func() {
		if recover() != nil {
			decision = Decision{Allow: true, Reason: "guard_error"}
		}
	}()
	if cb == nil {
		return Decision{Allow: true, Reason: "disabled"}
	}
	if !cb.cfg.CircuitEnabled {
		return Decision{Allow: true, Reason: "disabled"}
	}
	if os.Getenv(envAllowRunaway) == "1" {
		return Decision{Allow: true, Reason: "override"}
	}
	if sig.AttemptedTokens < cb.cfg.CircuitMinAttemptedTokens {
		return Decision{Allow: true, Reason: "below_min_tokens"}
	}

	now := sig.Now
	if now.IsZero() {
		now = time.Now()
	}
	key := makeKey(sig)
	cutoff := now.Add(-cb.cfg.CircuitWindow)

	cb.mu.Lock()
	defer cb.mu.Unlock()

	events := pruneBefore(cb.events[key], cutoff)
	count := len(events) + 1
	if count > cb.cfg.CircuitMaxSimilarRequests {
		cb.events[key] = events
		return Decision{
			Allow:      false,
			StatusCode: http.StatusTooManyRequests,
			Reason:     "similar_large_requests",
			RetryAfter: cb.cfg.CircuitRetryAfter,
			Count:      count,
			Remaining:  0,
		}
	}

	events = append(events, event{At: now, Tokens: sig.AttemptedTokens})
	cb.events[key] = events
	if len(cb.events) > maxCircuitKeys {
		cb.pruneMap(cutoff)
	}

	remaining := cb.cfg.CircuitMaxSimilarRequests - count
	return Decision{
		Allow:     true,
		Warn:      remaining <= 3,
		Reason:    "allowed",
		Count:     count,
		Remaining: remaining,
	}
}

func (cb *CircuitBreaker) pruneMap(cutoff time.Time) {
	for key, events := range cb.events {
		events = pruneBefore(events, cutoff)
		if len(events) == 0 {
			delete(cb.events, key)
			if len(cb.events) <= maxCircuitKeys {
				return
			}
			continue
		}
		cb.events[key] = events
	}
}

func pruneBefore(events []event, cutoff time.Time) []event {
	if len(events) == 0 {
		return events
	}
	idx := 0
	for idx < len(events) && events[idx].At.Before(cutoff) {
		idx++
	}
	if idx == 0 {
		return events
	}
	if idx >= len(events) {
		return nil
	}
	return events[idx:]
}

func makeKey(sig RequestSignal) string {
	parts := []string{
		safePart(sig.MachineID),
		safePart(sig.SessionKey),
		safePart(sig.Route),
		safePart(sig.Model),
		safePart(sig.Fingerprint),
	}
	if sig.SessionKey == "" {
		parts[1] = "no-session"
	}
	return strings.Join(parts, "|")
}

func safePart(s string) string {
	if strings.TrimSpace(s) == "" {
		return "_"
	}
	return strings.TrimSpace(s)
}
