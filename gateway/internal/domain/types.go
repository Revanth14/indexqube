// Package domain holds the canonical types that flow across package
// boundaries inside the gateway: request shapes, credentials, and the
// streaming sink port. It depends on nothing internal.
//
// Anything HTTP-, provider-, or storage-specific does NOT belong here.
package domain

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
)

// Provider is the upstream LLM provider tag carried on each request.
// The governor uses it to dispatch to a registered adapter.
type Provider string

const (
	ProviderAnthropic Provider = "anthropic"
	ProviderOpenAI    Provider = "openai"
	ProviderBedrock   Provider = "bedrock"
	ProviderAzure     Provider = "azure"
)

// IsValid reports whether p is a known provider tag.
func (p Provider) IsValid() bool {
	switch p {
	case ProviderAnthropic, ProviderOpenAI, ProviderBedrock, ProviderAzure:
		return true
	}
	return false
}

// Credential is a BYO upstream API key. The gateway never persists it.
type Credential struct {
	Provider Provider
	APIKey   string
}

// String implements fmt.Stringer. It intentionally redacts the API key so the
// struct is safe to pass to loggers, error formatters, or fmt.Sprintf.
func (c Credential) String() string {
	return fmt.Sprintf("Credential{Provider:%s,APIKey:[REDACTED]}", c.Provider)
}

// LogValue implements slog.LogValuer so that structured loggers (zerolog, slog)
// never emit the raw APIKey field even when the whole struct is logged.
func (c Credential) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("provider", string(c.Provider)),
		slog.String("api_key", "[REDACTED]"),
	)
}

// Message is the canonical chat message shape, lightly OpenAI-compatible.
// Multimodal content (image blocks, tool calls) is deferred to v2.
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// InferenceRequest is the canonical inference request shape consumed by
// the governor. Provider-specific translation (Anthropic system blocks,
// Bedrock variants, Azure deployment names) is the adapter's job.
type InferenceRequest struct {
	Model       string    `json:"model"`
	Messages    []Message `json:"messages"`
	MaxTokens   int       `json:"max_tokens,omitempty"`
	Temperature float64   `json:"temperature,omitempty"`
	Stream      bool      `json:"stream"`

	// Credential is populated from request headers, never from the JSON body.
	Credential Credential `json:"-"`

	// ProjectMemory is optional markdown injected as a leading system message
	// (indexqube_context). Populated from X-IQ-Project-Memory by the proxy.
	ProjectMemory string `json:"-"`

	// SessionKey scopes pruning history across requests when non-empty
	// (e.g. Chrome extension flow). Otherwise tenant defaults to a hash of
	// the upstream API key. From header X-IQ-Session-Key.
	SessionKey string `json:"-"`

	// AzureEndpoint is the full resource URL (e.g. https://res.openai.azure.com).
	// From header X-IQ-Azure-Endpoint.
	AzureEndpoint string `json:"-"`

	// AWSRegion is the target region for Bedrock (e.g. us-east-1).
	// From header X-IQ-AWS-Region.
	AWSRegion string `json:"-"`
}

// PruneStats summarizes one pruning pass for logs and /v1/optimize JSON.
type PruneStats struct {
	BlocksSeen     int            `json:"blocks_seen"`
	BlocksPruned   int            `json:"blocks_pruned"`
	BlocksSkipped  int            `json:"blocks_skipped"`
	SkipReasons    map[string]int `json:"skip_reasons,omitempty"`
	BytesBefore    int            `json:"bytes_before"`
	BytesAfter     int            `json:"bytes_after"`
	BytesSaved     int            `json:"bytes_saved"`
	TokensBefore   int            `json:"estimated_tokens_before"`
	TokensAfter    int            `json:"estimated_tokens_after"`
	TokensSaved    int            `json:"estimated_tokens_saved"`
	ReductionRatio float64        `json:"reduction_ratio"`
	DiffExact      int            `json:"diff_exact"`
	DiffFallback   int            `json:"diff_fallback"`
}

// Diagnostics is a privacy-safe gateway health snapshot. It must never contain
// raw prompt text, file paths, session keys, provider keys, or tenant hashes.
type Diagnostics struct {
	Status          string             `json:"status"`
	PruningEnabled  bool               `json:"pruning_enabled"`
	ContractVersion string             `json:"contract_version"`
	History         HistoryDiagnostics `json:"history"`
}

// HistoryDiagnostics reports bounded in-memory pruning pressure without
// exposing any tenant/session/file identifiers.
type HistoryDiagnostics struct {
	Tenants int   `json:"tenants"`
	Entries int   `json:"entries"`
	Bytes   int64 `json:"bytes"`
}

// TokenWriter is the streaming sink the proxy hands to the governor,
// which in turn hands it to the dispatched adapter. Implementations are
// NOT safe for concurrent use -- a single adapter goroutine owns it for
// the lifetime of one request.
type TokenWriter interface {
	// WriteData emits one SSE `data:` frame and flushes. The payload is
	// written verbatim; the caller chooses the on-wire shape.
	WriteData(data []byte) error

	// WriteEvent emits a named SSE event with a `data:` payload and flushes.
	WriteEvent(event string, data []byte) error

	// WriteDone emits the OpenAI-style sentinel `data: [DONE]`.
	WriteDone() error

	// Flush forces buffered bytes onto the wire. The Write* methods flush
	// implicitly; this is for callers writing raw payloads via the
	// underlying writer.
	Flush() error
}

// Embedder is the contract for generating semantic embeddings of text.
type Embedder interface {
	Embed(ctx context.Context, text string) ([]float32, error)
}

// ─── Provider errors ──────────────────────────────────────────────────────────

// ProviderError is a structured error returned by all provider adapters.
// It carries the upstream HTTP status code so the governor can make failover
// decisions without brittle string matching on error messages.
type ProviderError struct {
	Provider   Provider
	StatusCode int
	// Message is a sanitised, caller-safe description (no billing/account info).
	Message string
	// Cause preserves the raw upstream response for internal logging.
	Cause error
}

func (e *ProviderError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s error (HTTP %d): %s: %v", e.Provider, e.StatusCode, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s error (HTTP %d): %s", e.Provider, e.StatusCode, e.Message)
}

func (e *ProviderError) Unwrap() error { return e.Cause }

// IsRateLimit reports whether the provider returned 429 Too Many Requests.
func (e *ProviderError) IsRateLimit() bool { return e.StatusCode == http.StatusTooManyRequests }

// IsUnavailable reports whether the provider returned 503 Service Unavailable.
func (e *ProviderError) IsUnavailable() bool { return e.StatusCode == http.StatusServiceUnavailable }

// IsRetryable reports whether the governor should attempt a provider failover.
// Only rate-limit and unavailable responses warrant a cross-provider retry;
// all other errors (bad request, auth failure, context-too-long) are terminal.
func (e *ProviderError) IsRetryable() bool { return e.IsRateLimit() || e.IsUnavailable() }
