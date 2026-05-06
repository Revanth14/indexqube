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
	AWS         AWSConfig
	Azure       AzureConfig
	Supabase    SupabaseConfig
}

type ServerConfig struct {
	Port              string
	AdminPort         string
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	// WriteTimeout MUST be 0 for the streaming proxy. Setting any non-zero
	// value caps the maximum stream duration and breaks long generations.
	WriteTimeout              time.Duration
	IdleTimeout               time.Duration
	CORSEnabled               bool
	CORSAllowedOrigins        []string
	CORSAllowChromeExtensions bool
	CORSMaxAge                time.Duration
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
}

// CacheConfig drives the L1 in-memory response cache.
//
// Setting MaxBytes to 0 disables the cache entirely (the governor
// passes requests straight through to the adapter). Production
// deployments should size MaxBytes to a meaningful fraction of the
// container's memory budget.
type CacheConfig struct {
	Enabled       bool
	MaxBytes      int64
	TTL           time.Duration
	MaxEntryBytes int64 // largest single response we'll cache; bigger -> tee abandons
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
	DBURL          string
	ServiceRoleKey string
}

// Load reads environment variables, applies defaults, and validates required fields.
func Load() (*AppConfig, error) {
	cfg := &AppConfig{
		Environment: getEnvWithDefault("APP_ENV", "development"),
		Server: ServerConfig{
			Port:              getEnvWithDefault("PORT", "8080"),
			AdminPort:         getEnvWithDefault("ADMIN_PORT", "9100"),
			ReadHeaderTimeout: getEnvAsDuration("SERVER_READ_HEADER_TIMEOUT", 10*time.Second),
			ReadTimeout:       getEnvAsDuration("SERVER_READ_TIMEOUT", 30*time.Second),
			// Default 0: no per-request write deadline. Streaming-first design.
			// Slowloris is mitigated by ReadHeaderTimeout + IdleTimeout instead.
			WriteTimeout: getEnvAsDuration("SERVER_WRITE_TIMEOUT", 0),
			IdleTimeout:  getEnvAsDuration("SERVER_IDLE_TIMEOUT", 120*time.Second),
			CORSEnabled:  getEnvAsBool("CORS_ENABLED", true),
			CORSAllowedOrigins: getEnvAsCSV("CORS_ALLOWED_ORIGINS", []string{
				"http://localhost:3000",
				"http://localhost:5173",
				"http://localhost:8080",
			}),
			CORSAllowChromeExtensions: getEnvAsBool("CORS_ALLOW_CHROME_EXTENSIONS", getEnvWithDefault("APP_ENV", "development") != "production"),
			CORSMaxAge:                getEnvAsDuration("CORS_MAX_AGE", 10*time.Minute),
		},
		Telemetry: TelemetryConfig{
			ServiceName:    getEnvWithDefault("OTEL_SERVICE_NAME", "indexqube-gateway"),
			ServiceVersion: getEnvWithDefault("SERVICE_VERSION", "dev"),
			OTLPEndpoint:   os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
			OTLPInsecure:   getEnvAsBool("OTEL_EXPORTER_OTLP_INSECURE", true),
			MetricsEnabled: getEnvAsBool("METRICS_ENABLED", true),
			LogLevel:       getEnvWithDefault("LOG_LEVEL", "info"),
		},
		Governor: GovernorConfig{
			EgressReductionTarget:    getEnvAsFloat64("GOVERNOR_EGRESS_TARGET", 0.65),
			MaxTokensPerRequest:      getEnvAsInt("GOVERNOR_MAX_TOKENS", 8192),
			MaxRequestSize:           int64(getEnvAsInt("GOVERNOR_MAX_REQUEST_SIZE", 8<<20)),
			PruneEnabled:             getEnvAsBool("GOVERNOR_PRUNE_ENABLED", true),
			PruneMaxLines:            getEnvAsInt("GOVERNOR_PRUNE_MAX_LINES", 8000),
			ProjectMemoryPath:        getEnvWithDefault("GOVERNOR_PROJECT_MEMORY_PATH", "indexqube_context.md"),
			HistoryMaxTenants:        getEnvAsInt("GOVERNOR_HISTORY_MAX_TENANTS", 1024),
			HistoryMaxFilesPerTenant: getEnvAsInt("GOVERNOR_HISTORY_MAX_FILES_PER_TENANT", 256),
			HistoryMaxFileBytes:      int64(getEnvAsInt("GOVERNOR_HISTORY_MAX_FILE_BYTES", 2<<20)),
			HistoryMaxBytes:          int64(getEnvAsInt("GOVERNOR_HISTORY_MAX_BYTES", 64<<20)),
			HistoryTTL:               getEnvAsDuration("GOVERNOR_HISTORY_TTL", 2*time.Hour),
		},
		Cache: CacheConfig{
			Enabled:       getEnvAsBool("CACHE_ENABLED", true),
			MaxBytes:      int64(getEnvAsInt("CACHE_MAX_BYTES", 256<<20)), // 256 MiB
			TTL:           getEnvAsDuration("CACHE_TTL", 24*time.Hour),
			MaxEntryBytes: int64(getEnvAsInt("CACHE_MAX_ENTRY_BYTES", 4<<20)), // 4 MiB
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
			DBURL:          os.Getenv("SUPABASE_DB_URL"),
			ServiceRoleKey: os.Getenv("SUPABASE_SERVICE_ROLE_KEY"),
		},
	}

	if err := validate(cfg); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	return cfg, nil
}

// validate ensures all critical infrastructure variables are present.
func validate(cfg *AppConfig) error {
	// For local dev, we might bypass some checks, but production requires strict validation.
	if cfg.Environment == "production" {
		if cfg.Supabase.DBURL == "" {
			return errors.New("SUPABASE_DB_URL is required in production")
		}
		// Add other strict production checks here
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
