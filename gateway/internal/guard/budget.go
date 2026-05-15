package guard

import (
	"net/http"
	"os"
	"sync"
)

type BudgetGuard struct {
	mu       sync.Mutex
	cfg      Config
	spentUSD map[string]float64
}

func NewBudgetGuard(cfg Config) *BudgetGuard {
	return &BudgetGuard{
		cfg:      cfg,
		spentUSD: make(map[string]float64),
	}
}

func (bg *BudgetGuard) Check(sig RequestSignal) Decision {
	if os.Getenv(envDevMode) == "1" || os.Getenv(envAllowOverBudget) == "1" || os.Getenv(envAllowRunaway) == "1" {
		return Decision{Allow: true, Reason: "override"}
	}

	key := sig.SessionKey
	if key == "" {
		key = "no-session"
	}

	bg.mu.Lock()
	spent := bg.spentUSD[key]
	projected := spent + sig.EstimatedCostUSD
	bg.spentUSD[key] = projected
	bg.mu.Unlock()

	budget := bg.cfg.SessionBudgetUSD
	decision := Decision{
		BudgetUSD:    budget,
		SpentUSD:     spent,
		ProjectedUSD: projected,
	}

	if projected >= budget*bg.cfg.BudgetHardRatio {
		decision.Allow = false
		decision.StatusCode = http.StatusTooManyRequests
		decision.Reason = "budget_exceeded"
		return decision
	}

	if projected >= budget*bg.cfg.BudgetWarnRatio {
		decision.Allow = true
		decision.Warn = true
		decision.Reason = "budget_warning"
		return decision
	}

	decision.Allow = true
	decision.Reason = "allowed"
	return decision
}
