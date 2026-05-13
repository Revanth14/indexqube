package guard

import (
	"os"
	"strconv"
	"time"
)

const (
	envCircuitEnabled    = "INDEXQUBE_CIRCUIT_BREAKER_ENABLED"
	envWindowSeconds     = "INDEXQUBE_CIRCUIT_WINDOW_SECONDS"
	envMaxSimilar        = "INDEXQUBE_CIRCUIT_MAX_SIMILAR"
	envMinTokens         = "INDEXQUBE_CIRCUIT_MIN_TOKENS"
	envRetryAfterSeconds = "INDEXQUBE_CIRCUIT_RETRY_AFTER_SECONDS"
	envAllowRunaway      = "IQ_ALLOW_RUNAWAY"

	// Budget Config
	envSessionBudgetUSD = "INDEXQUBE_SESSION_BUDGET_USD"
	envBudgetWarnRatio  = "INDEXQUBE_BUDGET_WARN_RATIO"
	envBudgetHardRatio  = "INDEXQUBE_BUDGET_HARD_RATIO"
	envAllowOverBudget  = "IQ_ALLOW_OVER_BUDGET"

	// Velocity Config
	envSpendVelocityEnabled     = "INDEXQUBE_SPEND_VELOCITY_ENABLED"
	envSpendVelocityWindowSecs  = "INDEXQUBE_SPEND_VELOCITY_WINDOW_SECONDS"
	envSpendVelocityWarnUSD     = "INDEXQUBE_SPEND_VELOCITY_WARN_USD"
	envSpendVelocityBlockUSD    = "INDEXQUBE_SPEND_VELOCITY_BLOCK_USD"
)

type Config struct {
	CircuitEnabled            bool
	CircuitWindow             time.Duration
	CircuitMaxSimilarRequests int
	CircuitMinAttemptedTokens int
	CircuitRetryAfter         time.Duration

	SessionBudgetUSD float64
	BudgetWarnRatio  float64
	BudgetHardRatio  float64

	SpendVelocityEnabled bool
	SpendVelocityWindow  time.Duration
	SpendVelocityWarnUSD float64
	SpendVelocityBlockUSD float64
}

func DefaultConfig() Config {
	return Config{
		CircuitEnabled:            true,
		CircuitWindow:             60 * time.Second,
		CircuitMaxSimilarRequests: 15,
		CircuitMinAttemptedTokens: 50_000,
		CircuitRetryAfter:         60 * time.Second,

		SessionBudgetUSD: 1.00,
		BudgetWarnRatio:  0.80,
		BudgetHardRatio:  1.00,

		SpendVelocityEnabled: true,
		SpendVelocityWindow:  60 * time.Second,
		SpendVelocityWarnUSD: 0.25,
		SpendVelocityBlockUSD: 0.75,
	}
}

func FromEnv() Config {
	cfg := DefaultConfig()
	
	// Circuit Breaker
	cfg.CircuitEnabled = envBool(envCircuitEnabled, cfg.CircuitEnabled)
	cfg.CircuitWindow = time.Duration(envInt(envWindowSeconds, int(cfg.CircuitWindow.Seconds()))) * time.Second
	cfg.CircuitMaxSimilarRequests = envInt(envMaxSimilar, cfg.CircuitMaxSimilarRequests)
	cfg.CircuitMinAttemptedTokens = envInt(envMinTokens, cfg.CircuitMinAttemptedTokens)
	cfg.CircuitRetryAfter = time.Duration(envInt(envRetryAfterSeconds, int(cfg.CircuitRetryAfter.Seconds()))) * time.Second

	// Budget
	cfg.SessionBudgetUSD = envFloat(envSessionBudgetUSD, cfg.SessionBudgetUSD)
	cfg.BudgetWarnRatio = envFloat(envBudgetWarnRatio, cfg.BudgetWarnRatio)
	cfg.BudgetHardRatio = envFloat(envBudgetHardRatio, cfg.BudgetHardRatio)

	// Velocity
	cfg.SpendVelocityEnabled = envBool(envSpendVelocityEnabled, cfg.SpendVelocityEnabled)
	cfg.SpendVelocityWindow = time.Duration(envInt(envSpendVelocityWindowSecs, int(cfg.SpendVelocityWindow.Seconds()))) * time.Second
	cfg.SpendVelocityWarnUSD = envFloat(envSpendVelocityWarnUSD, cfg.SpendVelocityWarnUSD)
	cfg.SpendVelocityBlockUSD = envFloat(envSpendVelocityBlockUSD, cfg.SpendVelocityBlockUSD)

	return cfg
}

func envInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func envFloat(key string, fallback float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseFloat(v, 64)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func envBool(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	switch v {
	case "1", "true", "TRUE", "True", "yes", "y":
		return true
	case "0", "false", "FALSE", "False", "no", "n":
		return false
	default:
		return fallback
	}
}
