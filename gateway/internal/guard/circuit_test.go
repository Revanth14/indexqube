package guard

import (
	"testing"
	"time"
)

func TestAllowsBelowMinTokens(t *testing.T) {
	t.Parallel()
	cb := NewCircuitBreaker(Config{
		CircuitEnabled:            true,
		CircuitWindow:             time.Minute,
		CircuitMaxSimilarRequests: 2,
		CircuitMinAttemptedTokens: 100,
		CircuitRetryAfter:         time.Minute,
	})

	got := cb.Check(RequestSignal{
		MachineID:       "m",
		SessionKey:      "s",
		Route:           "/v1/messages",
		Model:           "claude",
		Fingerprint:     "fp",
		AttemptedTokens: 99,
		Now:             time.Unix(1, 0),
	})
	if !got.Allow || got.Reason != "below_min_tokens" {
		t.Fatalf("decision=%+v, want allow below_min_tokens", got)
	}
}

func TestBlocksAfterMaxSimilarRequests(t *testing.T) {
	t.Parallel()
	cb := NewCircuitBreaker(Config{
		CircuitEnabled:            true,
		CircuitWindow:             10 * time.Second,
		CircuitMaxSimilarRequests: 2,
		CircuitMinAttemptedTokens: 100,
		CircuitRetryAfter:         2 * time.Second,
	})
	base := time.Unix(10, 0)
	sig := RequestSignal{
		MachineID:       "m",
		SessionKey:      "s",
		Route:           "/v1/messages",
		Model:           "claude",
		Fingerprint:     "fp",
		AttemptedTokens: 1000,
	}

	first := cb.Check(withNow(sig, base))
	second := cb.Check(withNow(sig, base.Add(1*time.Second)))
	third := cb.Check(withNow(sig, base.Add(2*time.Second)))
	if !first.Allow || !second.Allow {
		t.Fatalf("first=%+v second=%+v, expected allowed", first, second)
	}
	if third.Allow || third.StatusCode != 429 || third.Reason != "similar_large_requests" {
		t.Fatalf("third=%+v, expected blocked 429 similar_large_requests", third)
	}
}

func TestWindowExpiryAllowsAgain(t *testing.T) {
	t.Parallel()
	cb := NewCircuitBreaker(Config{
		CircuitEnabled:            true,
		CircuitWindow:             1 * time.Second,
		CircuitMaxSimilarRequests: 1,
		CircuitMinAttemptedTokens: 100,
		CircuitRetryAfter:         2 * time.Second,
	})
	sig := RequestSignal{
		MachineID:       "m",
		SessionKey:      "s",
		Route:           "/v1/messages",
		Model:           "claude",
		Fingerprint:     "fp",
		AttemptedTokens: 1000,
	}

	first := cb.Check(withNow(sig, time.Unix(1, 0)))
	blocked := cb.Check(withNow(sig, time.Unix(1, 500*int64(time.Millisecond))))
	again := cb.Check(withNow(sig, time.Unix(8, 0)))
	if !first.Allow {
		t.Fatalf("first=%+v, expected allowed", first)
	}
	if blocked.Allow {
		t.Fatalf("blocked=%+v, expected blocked", blocked)
	}
	if !again.Allow {
		t.Fatalf("again=%+v, expected allowed after window expiry", again)
	}
}

func TestDifferentFingerprintsDoNotShareLimit(t *testing.T) {
	t.Parallel()
	cb := NewCircuitBreaker(Config{
		CircuitEnabled:            true,
		CircuitWindow:             time.Minute,
		CircuitMaxSimilarRequests: 1,
		CircuitMinAttemptedTokens: 100,
		CircuitRetryAfter:         time.Minute,
	})
	base := RequestSignal{
		MachineID:       "m",
		SessionKey:      "s",
		Route:           "/v1/messages",
		Model:           "claude",
		AttemptedTokens: 1000,
	}

	_ = cb.Check(withNow(withFingerprint(base, "fp-1"), time.Unix(1, 0)))
	secondKey := cb.Check(withNow(withFingerprint(base, "fp-2"), time.Unix(2, 0)))
	if !secondKey.Allow {
		t.Fatalf("second key decision=%+v, expected allowed", secondKey)
	}
}

func TestOverrideAllowsRunaway(t *testing.T) {
	t.Setenv(envAllowRunaway, "1")
	cb := NewCircuitBreaker(Config{
		CircuitEnabled:            true,
		CircuitWindow:             time.Minute,
		CircuitMaxSimilarRequests: 1,
		CircuitMinAttemptedTokens: 100,
		CircuitRetryAfter:         time.Minute,
	})
	sig := RequestSignal{
		MachineID:       "m",
		SessionKey:      "s",
		Route:           "/v1/messages",
		Model:           "claude",
		Fingerprint:     "fp",
		AttemptedTokens: 1000,
		Now:             time.Unix(1, 0),
	}

	got := cb.Check(sig)
	if !got.Allow || got.Reason != "override" {
		t.Fatalf("decision=%+v, want allow override", got)
	}
}

func withNow(sig RequestSignal, now time.Time) RequestSignal {
	sig.Now = now
	return sig
}

func withFingerprint(sig RequestSignal, fp string) RequestSignal {
	sig.Fingerprint = fp
	return sig
}
