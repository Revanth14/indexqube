package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// AppConfig is the root configuration struct holding all domain-specific configs.
type AppConfig struct {
	Environment string
	Server      ServerConfig
	Telemetry   TelemetryConfig
	Cache       CacheConfig
	Governor    GovernorConfig
	ClaudeCode  ClaudeCodeConfig
	AWS         AWSConfig
	Azure       AzureConfig
	Supabase    SupabaseConfig
}

type ServerConfig struct {
	BindAddr string
	Port     string
	// AdminBindAddr defaults to 127.0.0.1 so /metrics, /healthz, /readyz
	// aren't reachable from outside the host. Set to 0.0.0.0 only when a
	// scrape target needs in-cluster access.
	AdminBindAddr     string
	AdminPort         string
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	// WriteTimeout MUST be 0 for the streaming proxy. Setting any non-zero
	// value caps the maximum stream duration and breaks long generations.
	WriteTimeout              time.Duration
	IdleTimeout               time.Duration
	// AuthToken protects sensitive endpoints (/stats, /v1/agent-sessions,
	// /v1/diagnostics) when the gateway binds to a non-loopback address.
	// Empty disables auth entirely. Defaults to INDEXQUBE_AUTH_TOKEN,
	// falling back to INDEXQUBE_DEV_TOKEN for backwards compatibility.
	AuthToken                 string
	CORSEnabled               bool
	CORSAllowedOrigins        []string
	CORSAllowChromeExtensions bool
	CORSMaxAge                time.Duration
	// TrustedProxies is a list of IP addresses or CIDR ranges (e.g. "10.0.0.0/8")
	// from which X-Forwarded-For headers are trusted. Empty means no proxy is trusted.
	TrustedProxies []string
}

// TelemetryConfig drives observability initialization.
//
// OTLPEndpoint empty disables trace export (a no-op tracer is installed).
// MetricsEnabled controls Prometheus registration; the admin server
// always starts -- it serves /healthz and /readyz regardless.
type TelemetryConfig struct {
	ServiceName    string
	ServiceVersion string
	OTLPEndpoint   string
	OTLPInsecure   bool
	MetricsEnabled bool
	LogLevel       string // "debug" | "info" | "warn" | "error"
}

type GovernorConfig struct {
	// EgressReductionTarget is for observability metrics
	EgressReductionTarget float64
	// MaxTokensPerRequest acts as a circuit breaker
	MaxTokensPerRequest int
	// MaxRequestSize hard-caps the inbound prompt size to prevent OOM
	MaxRequestSize int64

	// PruneEnabled turns on code-block diff pruning in the governor (Path A+B).
	PruneEnabled bool
	// PruneMaxLines caps per-file line count for Myers diff; larger blocks skip pruning.
	PruneMaxLines int
	// ProjectMemoryPath points to indexqube_context.md. Missing files mean no static memory.
	ProjectMemoryPath string

	HistoryMaxTenants        int
	HistoryMaxFilesPerTenant int
	HistoryMaxFileBytes      int64
	HistoryMaxBytes          int64
	HistoryTTL               time.Duration

	// OptimizeTimeout caps Path A (/v1/optimize). Streaming requests are
	// intentionally unbounded; this only applies to the synchronous
	// prune+memory path.
	OptimizeTimeout time.Duration
}

type ClaudeCodeConfig struct {
	Mode                 string
	DevToken             string
	AnthropicAPIKey      string
	AnthropicBaseURL     string
	AnthropicVersion     string
	EnableLogPruner      bool
	EnableBlockOptimizer bool
	SessionTTL           time.Duration

	// Bedrock backend for /v1/messages (replaces direct Anthropic calls).
	BedrockEnabled       bool
	BedrockRegion        string
	BedrockModelPrefix   string // "us." for cross-region, "" for single-region
	BedrockModelOverride string // force a specific Bedrock model ID for all requests

	// Optimizer tuning (all have safe defaults when zero).
	OptMinSpanBytes            int
	OptTargetChunkBytes        int
	OptMaxChunkBytes           int
	OptMinSavedTokens          int
	OptEnableToolResultPruning bool
	OptEnableAssistantPruning  bool
	OptEnableSystemPruning     bool
	OptDiagnostics             bool
}

// CacheConfig drives the L1 in-memory response cache.
//
// Setting MaxBytes to 0 disables the cache entirely (the governor
// passes requests straight through to the adapter). Production
// deployments should size MaxBytes to a meaningful fraction of the
// container's memory budget.
type CacheConfig struct {
	Enabled           bool
	MaxBytes          int64
	TTL               time.Duration
	MaxEntryBytes     int64 // largest single response we'll cache; bigger -> tee abandons
	SemanticEnabled   bool
	SemanticThreshold float64
}

type AWSConfig struct {
	Region          string
	BedrockEndpoint string
	// Access keys are typically handled by AWS SDK's default credential chain,
	// but we can expose overrides if needed for BYO-Key multi-tenant setups.
}

type AzureConfig struct {
	Endpoint string
	APIKey   string
}

type SupabaseConfig struct {
	DBURL      string
	URL        string // REST API base URL (e.g. https://xyz.supabase.co) for telemetry
	ServiceKey string // service role key for telemetry inserts
}

// Load reads environment variables, applies defaults, and validates required fields.
func Load() (*AppConfig, error) {
	env := getEnvFirst([]string{"INDEXQUBE_ENV", "APP_ENV"}, "development")
	cfg := &AppConfig{
		Environment: env,
		Server: ServerConfig{
			BindAddr:          os.Getenv("INDEXQUBE_BIND_ADDR"),
			Port:              getEnvWithDefault("PORT", "8080"),
			AdminBindAddr:     getEnvWithDefault("ADMIN_BIND_ADDR", "127.0.0.1"),
			AdminPort:         getEnvWithDefault("ADMIN_PORT", "9100"),
			ReadHeaderTimeout: getEnvAsDuration("SERVER_READ_HEADER_TIMEOUT", 10*time.Second),
			ReadTimeout:       getEnvAsDuration("SERVER_READ_TIMEOUT", 30*time.Second),
			// Default 0: no per-request write deadline. Streaming-first design.
			// Slowloris is mitigated by ReadHeaderTimeout + IdleTimeout instead.
			WriteTimeout: getEnvAsDuration("SERVER_WRITE_TIMEOUT", 0),
			IdleTimeout:  getEnvAsDuration("SERVER_IDLE_TIMEOUT", 120*time.Second),
			AuthToken:    getEnvFirst([]string{"INDEXQUBE_AUTH_TOKEN", "INDEXQUBE_DEV_TOKEN"}, ""),
			CORSEnabled:  getEnvAsBool("CORS_ENABLED", true),
			CORSAllowedOrigins: getEnvAsCSV("CORS_ALLOWED_ORIGINS", []string{
				"http://localhost:3000",
				"http://localhost:5173",
				"http://localhost:8080",
			}),
			CORSAllowChromeExtensions: getEnvAsBool("CORS_ALLOW_CHROME_EXTENSIONS", env != "production"),
			CORSMaxAge:                getEnvAsDuration("CORS_MAX_AGE", 10*time.Minute),
			TrustedProxies:            getEnvAsCSV("TRUSTED_PROXIES", nil),
		},
		Telemetry: TelemetryConfig{
			ServiceName:    getEnvWithDefault("OTEL_SERVICE_NAME", "indexqube-gateway"),
			ServiceVersion: getEnvWithDefault("SERVICE_VERSION", "dev"),
			OTLPEndpoint:   os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
			OTLPInsecure:   getEnvAsBool("OTEL_EXPORTER_OTLP_INSECURE", true),
			MetricsEnabled: getEnvAsBool("METRICS_ENABLED", true),
			LogLevel:       getEnvFirst([]string{"INDEXQUBE_LOG_LEVEL", "LOG_LEVEL"}, "info"),
		},
		Governor: GovernorConfig{
			EgressReductionTarget:    getEnvAsFloat64("GOVERNOR_EGRESS_TARGET", 0.65),
			MaxTokensPerRequest:      getEnvAsInt("GOVERNOR_MAX_TOKENS", 8192),
			MaxRequestSize:           int64(getEnvAsIntFirst([]string{"INDEXQUBE_MAX_BODY_BYTES", "GOVERNOR_MAX_REQUEST_SIZE"}, 8<<20)),
			PruneEnabled:             getEnvAsBoolFirst([]string{"INDEXQUBE_ENABLE_BLOCK_OPTIMIZER", "GOVERNOR_PRUNE_ENABLED"}, true),
			PruneMaxLines:            getEnvAsInt("GOVERNOR_PRUNE_MAX_LINES", 8000),
			ProjectMemoryPath:        getEnvWithDefault("GOVERNOR_PROJECT_MEMORY_PATH", "indexqube_context.md"),
			HistoryMaxTenants:        getEnvAsInt("GOVERNOR_HISTORY_MAX_TENANTS", 1024),
			HistoryMaxFilesPerTenant: getEnvAsInt("GOVERNOR_HISTORY_MAX_FILES_PER_TENANT", 256),
			HistoryMaxFileBytes:      int64(getEnvAsInt("GOVERNOR_HISTORY_MAX_FILE_BYTES", 2<<20)),
			HistoryMaxBytes:          int64(getEnvAsInt("GOVERNOR_HISTORY_MAX_BYTES", 64<<20)),
			HistoryTTL:               getSessionTTL(),
			OptimizeTimeout:          getEnvAsDuration("GOVERNOR_OPTIMIZE_TIMEOUT", 30*time.Second),
		},
		ClaudeCode: ClaudeCodeConfig{
			Mode:                 getEnvWithDefault("INDEXQUBE_MODE", "observe"),
			DevToken:             os.Getenv("INDEXQUBE_DEV_TOKEN"),
			AnthropicAPIKey:      os.Getenv("ANTHROPIC_API_KEY"),
			AnthropicBaseURL:     getEnvFirst([]string{"INDEXQUBE_ANTHROPIC_BASE_URL", "ANTHROPIC_BASE_URL"}, "https://api.anthropic.com"),
			AnthropicVersion:     getEnvWithDefault("ANTHROPIC_VERSION", "2023-06-01"),
			EnableLogPruner:      getEnvAsBool("INDEXQUBE_ENABLE_LOG_PRUNER", false),
			EnableBlockOptimizer: getEnvAsBool("INDEXQUBE_ENABLE_BLOCK_OPTIMIZER", false),
			SessionTTL:           getSessionTTL(),

			BedrockEnabled:       getEnvAsBool("INDEXQUBE_BEDROCK_ENABLED", false),
			BedrockRegion:        getEnvWithDefault("INDEXQUBE_BEDROCK_REGION", "us-east-1"),
			BedrockModelPrefix:   getEnvWithDefault("INDEXQUBE_BEDROCK_MODEL_PREFIX", "us."),
			BedrockModelOverride: os.Getenv("INDEXQUBE_BEDROCK_MODEL_OVERRIDE"),

			OptMinSpanBytes:            getEnvAsInt("INDEXQUBE_OPT_MIN_SPAN_BYTES", 0),
			OptTargetChunkBytes:        getEnvAsInt("INDEXQUBE_OPT_TARGET_CHUNK_BYTES", 0),
			OptMaxChunkBytes:           getEnvAsInt("INDEXQUBE_OPT_MAX_CHUNK_BYTES", 0),
			OptMinSavedTokens:          getEnvAsInt("INDEXQUBE_OPT_MIN_SAVED_TOKENS", 0),
			OptEnableToolResultPruning: getEnvAsBool("INDEXQUBE_OPT_ENABLE_TOOL_RESULT_PRUNING", true),
			OptEnableAssistantPruning:  getEnvAsBool("INDEXQUBE_OPT_ENABLE_ASSISTANT_PRUNING", false),
			OptEnableSystemPruning:     getEnvAsBool("INDEXQUBE_OPT_ENABLE_SYSTEM_PRUNING", false),
			OptDiagnostics:             getEnvAsBool("INDEXQUBE_OPT_DIAGNOSTICS", false),
		},
		Cache: CacheConfig{
			Enabled:           getEnvAsBool("CACHE_ENABLED", true),
			MaxBytes:          int64(getEnvAsInt("CACHE_MAX_BYTES", 256<<20)), // 256 MiB
			TTL:               getEnvAsDuration("CACHE_TTL", 24*time.Hour),
			MaxEntryBytes:     int64(getEnvAsInt("CACHE_MAX_ENTRY_BYTES", 4<<20)), // 4 MiB
			SemanticEnabled:   getEnvAsBool("CACHE_SEMANTIC_ENABLED", false),
			SemanticThreshold: getEnvAsFloat64("CACHE_SEMANTIC_THRESHOLD", 0.95),
		},
		AWS: AWSConfig{
			Region:          getEnvWithDefault("AWS_REGION", "us-east-1"),
			BedrockEndpoint: os.Getenv("AWS_BEDROCK_ENDPOINT"),
		},
		Azure: AzureConfig{
			Endpoint: os.Getenv("AZURE_OPENAI_ENDPOINT"),
			APIKey:   os.Getenv("AZURE_OPENAI_API_KEY"),
		},
		Supabase: SupabaseConfig{
			DBURL:      os.Getenv("SUPABASE_DB_URL"),
			URL:        os.Getenv("SUPABASE_URL"),
			ServiceKey: os.Getenv("SUPABASE_SERVICE_ROLE_KEY"),
		},
	}

	if err := validate(cfg); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	return cfg, nil
}

// validate ensures all critical infrastructure variables are present.
func validate(cfg *AppConfig) error {
	switch cfg.ClaudeCode.Mode {
	case "observe", "dry_run", "optimize":
	default:
		return fmt.Errorf("INDEXQUBE_MODE must be observe, dry_run, or optimize, got %q", cfg.ClaudeCode.Mode)
	}
	// For local dev, we might bypass some checks, but production requires strict validation.
	if cfg.Environment == "production" {
		if cfg.Supabase.DBURL == "" {
			return errors.New("SUPABASE_DB_URL is required in production")
		}
		if cfg.Server.CORSAllowChromeExtensions {
			return errors.New("CORS_ALLOW_CHROME_EXTENSIONS must be false in production")
		}
	}
	return nil
}

// --- Helper Functions ---

func getEnvWithDefault(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func getEnvFirst(keys []string, fallback string) string {
	for _, key := range keys {
		if value, exists := os.LookupEnv(key); exists {
			return value
		}
	}
	return fallback
}

func getEnvAsIntFirst(keys []string, fallback int) int {
	for _, key := range keys {
		if valStr := os.Getenv(key); valStr != "" {
			val, err := strconv.Atoi(valStr)
			if err == nil {
				return val
			}
		}
	}
	return fallback
}

func getEnvAsBoolFirst(keys []string, fallback bool) bool {
	for _, key := range keys {
		if valStr := os.Getenv(key); valStr != "" {
			switch valStr {
			case "1", "true", "TRUE", "True", "yes", "y":
				return true
			case "0", "false", "FALSE", "False", "no", "n":
				return false
			}
		}
	}
	return fallback
}

func getSessionTTL() time.Duration {
	if minutes := getEnvAsInt("INDEXQUBE_SESSION_TTL_MINUTES", 0); minutes > 0 {
		return time.Duration(minutes) * time.Minute
	}
	return getEnvAsDuration("GOVERNOR_HISTORY_TTL", 2*time.Hour)
}

func getEnvAsInt(key string, fallback int) int {
	valStr := os.Getenv(key)
	if valStr == "" {
		return fallback
	}
	val, err := strconv.Atoi(valStr)
	if err != nil {
		return fallback
	}
	return val
}

func getEnvAsFloat64(key string, fallback float64) float64 {
	valStr := os.Getenv(key)
	if valStr == "" {
		return fallback
	}
	val, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return fallback
	}
	return val
}

func getEnvAsDuration(key string, fallback time.Duration) time.Duration {
	valStr := os.Getenv(key)
	if valStr == "" {
		return fallback
	}
	val, err := time.ParseDuration(valStr)
	if err != nil {
		return fallback
	}
	return val
}

func getEnvAsBool(key string, fallback bool) bool {
	valStr := os.Getenv(key)
	if valStr == "" {
		return fallback
	}
	switch valStr {
	case "1", "true", "TRUE", "True", "yes", "y":
		return true
	case "0", "false", "FALSE", "False", "no", "n":
		return false
	default:
		return fallback
	}
}

func getEnvAsCSV(key string, fallback []string) []string {
	valStr := os.Getenv(key)
	if valStr == "" {
		return fallback
	}
	parts := strings.Split(valStr, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	if len(out) == 0 {
		return fallback
	}
	return out
}
