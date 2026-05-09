package governor

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/domain"
)

// ErrCircuitOpen is returned by dispatch when a provider's circuit breaker is
// open (the provider has recently failed too many times). The governor treats
// this as a retryable error so it can fail over to a backup provider instead
// of waiting for the open circuit to probe.
var ErrCircuitOpen = errors.New("circuit breaker open")

// breakerState is the three-phase circuit breaker FSM.
type breakerState uint8

const (
	breakerClosed   breakerState = iota // normal — all requests allowed
	breakerOpen                         // failing — requests rejected immediately
	breakerHalfOpen                     // probing — one request allowed to test recovery
)

// breaker is a per-provider circuit breaker.
//
// State machine:
//
//	Closed ──(N consecutive failures)──► Open
//	Open   ──(openTimeout elapsed)──────► HalfOpen
//	HalfOpen ──(success)────────────────► Closed
//	HalfOpen ──(failure)────────────────► Open (reset timer)
type breaker struct {
	mu          sync.Mutex
	provider    domain.Provider
	state       breakerState
	failures    uint32
	maxFailures uint32
	openUntil   time.Time
	openTimeout time.Duration
	logger      *slog.Logger
}

func newBreaker(provider domain.Provider, maxFailures uint32, openTimeout time.Duration, logger *slog.Logger) *breaker {
	if maxFailures == 0 {
		maxFailures = 5
	}
	if openTimeout <= 0 {
		openTimeout = 30 * time.Second
	}
	return &breaker{
		provider:    provider,
		state:       breakerClosed,
		maxFailures: maxFailures,
		openTimeout: openTimeout,
		logger:      logger,
	}
}

// allow checks whether a call to the provider is permitted.
//
// Returns (true, done) when the request should proceed.
// The caller MUST invoke done(success) exactly once after the call completes —
// this is how the breaker learns whether the provider is healthy.
//
// Returns (false, nil) when the circuit is open; the caller must NOT invoke done.
func (b *breaker) allow() (bool, func(bool)) {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case breakerOpen:
		if time.Now().Before(b.openUntil) {
			// Still cooling down — fail fast.
			return false, nil
		}
		// Timeout elapsed: allow a single probe to test recovery.
		b.state = breakerHalfOpen
		b.logger.Warn("circuit breaker half-open: probing provider",
			slog.String("provider", string(b.provider)),
			slog.Duration("was_open_for", b.openTimeout),
		)
		return true, b.record

	case breakerHalfOpen:
		// Only one probe at a time; reject all concurrent requests.
		return false, nil

	default: // breakerClosed
		return true, b.record
	}
}

// record is the done callback passed to callers by allow.
func (b *breaker) record(success bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if success {
		if b.state == breakerHalfOpen {
			b.logger.Info("circuit breaker closed: provider recovered",
				slog.String("provider", string(b.provider)),
			)
		}
		b.failures = 0
		b.state = breakerClosed
		return
	}

	b.failures++
	if b.state == breakerHalfOpen || b.failures >= b.maxFailures {
		b.state = breakerOpen
		b.openUntil = time.Now().Add(b.openTimeout)
		b.logger.Warn("circuit breaker opened: provider suspended",
			slog.String("provider", string(b.provider)),
			slog.Uint64("consecutive_failures", uint64(b.failures)),
			slog.Duration("open_for", b.openTimeout),
			slog.Time("retry_after", b.openUntil),
		)
		b.failures = 0 // reset counter so next half-open probe starts fresh
	}
}

// circuitOpenError wraps ErrCircuitOpen with the provider name so logs
// can distinguish which provider was suspended.
func circuitOpenError(provider domain.Provider) error {
	return fmt.Errorf("provider %s: %w", provider, ErrCircuitOpen)
}
