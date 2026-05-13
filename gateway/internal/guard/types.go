package guard

import "time"

type RequestSignal struct {
	MachineID   string
	SessionKey  string
	Route       string
	Model       string
	Fingerprint string

	AttemptedTokens int
	SentTokens      int
	TokensSaved     int
	ReductionPct    float64

	BlocksAnalyzed int
	BlocksPruned   int

	Now time.Time

	EstimatedCostUSD float64
}

type Decision struct {
	Allow      bool
	Warn       bool
	StatusCode int
	Reason     string
	RetryAfter time.Duration
	Count      int
	Remaining  int

	// Add fields for Budget
	BudgetUSD    float64
	SpentUSD     float64
	ProjectedUSD float64
}
