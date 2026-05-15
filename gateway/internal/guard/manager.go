package guard

import (
	"net/http"
	"sync"
	"time"
)

// recentBlock records a blocked request so retries can be detected.
type recentBlock struct {
	tokenCount int64
	blockedAt  time.Time
}

type Manager struct {
	Circuit  *CircuitBreaker
	Budget   *BudgetGuard
	Velocity *VelocityGuard

	recentBlocksMu sync.Mutex
	recentBlocks   []recentBlock
}

func NewManager(cfg Config) *Manager {
	return &Manager{
		Circuit:      NewCircuitBreaker(cfg),
		Budget:       NewBudgetGuard(cfg),
		Velocity:     NewVelocityGuard(cfg),
		recentBlocks: make([]recentBlock, 0, 8),
	}
}

func (m *Manager) Check(sig RequestSignal) Decision {
	// Retry storm check must run before any guard so retries of a blocked
	// request are never counted toward budget or velocity metrics.
	if m.isRetryStorm(int64(sig.AttemptedTokens)) {
		return Decision{
			Allow:      false,
			StatusCode: http.StatusTooManyRequests,
			Reason:     "retry_storm",
		}
	}

	// Priority:
	// 1. budget exceeded  → block
	// 2. velocity exceeded → block
	// 3. circuit breaker  → block
	// 4. warnings         → allow with warning

	bd := m.Budget.Check(sig)
	if !bd.Allow {
		m.recordBlock(int64(sig.AttemptedTokens))
		return bd
	}

	vd := m.Velocity.Check(sig)
	if !vd.Allow {
		m.recordBlock(int64(sig.AttemptedTokens))
		vd.BudgetUSD = bd.BudgetUSD
		vd.SpentUSD = bd.SpentUSD
		vd.ProjectedUSD = bd.ProjectedUSD
		return vd
	}

	cd := m.Circuit.Check(sig)
	if !cd.Allow {
		m.recordBlock(int64(sig.AttemptedTokens))
		cd.BudgetUSD = bd.BudgetUSD
		cd.SpentUSD = bd.SpentUSD
		cd.ProjectedUSD = bd.ProjectedUSD
		return cd
	}

	// If any guard allowed with a warning, surface it.
	if bd.Warn {
		return bd
	}
	if vd.Warn {
		vd.BudgetUSD = bd.BudgetUSD
		vd.SpentUSD = bd.SpentUSD
		vd.ProjectedUSD = bd.ProjectedUSD
		return vd
	}
	if cd.Warn {
		cd.BudgetUSD = bd.BudgetUSD
		cd.SpentUSD = bd.SpentUSD
		cd.ProjectedUSD = bd.ProjectedUSD
		return cd
	}

	return bd
}

func (m *Manager) recordBlock(tokenCount int64) {
	m.recentBlocksMu.Lock()
	defer m.recentBlocksMu.Unlock()
	m.recentBlocks = append(m.recentBlocks, recentBlock{
		tokenCount: tokenCount,
		blockedAt:  time.Now(),
	})
}

// isRetryStorm returns true when the incoming request looks like a retry of a
// recently blocked request: same token count (±10%) within a 2-second window.
// It also prunes expired entries from the window as a side effect.
func (m *Manager) isRetryStorm(tokenCount int64) bool {
	m.recentBlocksMu.Lock()
	defer m.recentBlocksMu.Unlock()

	now := time.Now()
	live := m.recentBlocks[:0]
	storm := false

	for _, rb := range m.recentBlocks {
		if now.Sub(rb.blockedAt) >= 2*time.Second {
			continue // expired, drop it
		}
		live = append(live, rb)
		if rb.tokenCount > 0 {
			diff := float64(abs64(tokenCount-rb.tokenCount)) / float64(rb.tokenCount)
			if diff < 0.10 {
				storm = true
			}
		}
	}
	m.recentBlocks = live
	return storm
}

func abs64(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}
