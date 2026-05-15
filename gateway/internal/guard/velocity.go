package guard

import (
	"net/http"
	"os"
	"sync"
	"time"
)

type SpendEvent struct {
	At              time.Time
	EstimatedUSD    float64
	AttemptedTokens int
	SentTokens      int
}

type VelocityGuard struct {
	mu     sync.Mutex
	cfg    Config
	events map[string][]SpendEvent
}

func NewVelocityGuard(cfg Config) *VelocityGuard {
	return &VelocityGuard{
		cfg:    cfg,
		events: make(map[string][]SpendEvent),
	}
}

func (vg *VelocityGuard) Check(sig RequestSignal) Decision {
	if !vg.cfg.SpendVelocityEnabled {
		return Decision{Allow: true, Reason: "disabled"}
	}
	if os.Getenv(envDevMode) == "1" {
		return Decision{Allow: true, Reason: "dev_mode"}
	}
	if os.Getenv(envAllowOverBudget) == "1" || os.Getenv(envAllowRunaway) == "1" {
		return Decision{Allow: true, Reason: "override"}
	}

	key := sig.SessionKey
	if key == "" {
		key = "no-session"
	}
	now := sig.Now
	if now.IsZero() {
		now = time.Now()
	}
	cutoff := now.Add(-vg.cfg.SpendVelocityWindow)

	vg.mu.Lock()
	defer vg.mu.Unlock()

	events := pruneSpendEvents(vg.events[key], cutoff)
	
	events = append(events, SpendEvent{
		At:              now,
		EstimatedUSD:    sig.EstimatedCostUSD,
		AttemptedTokens: sig.AttemptedTokens,
		SentTokens:      sig.SentTokens,
	})
	vg.events[key] = events

	var sumUSD float64
	for _, e := range events {
		sumUSD += e.EstimatedUSD
	}

	if sumUSD >= vg.cfg.SpendVelocityBlockUSD {
		return Decision{
			Allow:      false,
			StatusCode: http.StatusTooManyRequests,
			Reason:     "velocity_exceeded",
		}
	}

	if sumUSD >= vg.cfg.SpendVelocityWarnUSD {
		return Decision{
			Allow:  true,
			Warn:   true,
			Reason: "velocity_warning",
		}
	}

	return Decision{Allow: true, Reason: "allowed"}
}

func pruneSpendEvents(events []SpendEvent, cutoff time.Time) []SpendEvent {
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
