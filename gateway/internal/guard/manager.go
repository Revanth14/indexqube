package guard

type Manager struct {
	Circuit  *CircuitBreaker
	Budget   *BudgetGuard
	Velocity *VelocityGuard
}

func NewManager(cfg Config) *Manager {
	return &Manager{
		Circuit:  NewCircuitBreaker(cfg),
		Budget:   NewBudgetGuard(cfg),
		Velocity: NewVelocityGuard(cfg),
	}
}

func (m *Manager) Check(sig RequestSignal) Decision {
	// Priority:
	// 1. override env -> allow (handled inside each guard, but we can fast-path if needed)
	// 2. budget exceeded -> block
	// 3. velocity exceeded -> block
	// 4. circuit breaker -> block
	// 5. warnings -> allow with warning

	bd := m.Budget.Check(sig)
	if !bd.Allow {
		return bd
	}

	vd := m.Velocity.Check(sig)
	if !vd.Allow {
		// Pass budget context along
		vd.BudgetUSD = bd.BudgetUSD
		vd.SpentUSD = bd.SpentUSD
		vd.ProjectedUSD = bd.ProjectedUSD
		return vd
	}

	cd := m.Circuit.Check(sig)
	if !cd.Allow {
		cd.BudgetUSD = bd.BudgetUSD
		cd.SpentUSD = bd.SpentUSD
		cd.ProjectedUSD = bd.ProjectedUSD
		return cd
	}

	// If any allowed with warning, return the warning
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

	return bd // all allowed, return the budget decision which has the budget fields
}
