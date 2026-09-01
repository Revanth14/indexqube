package proxy

import (
	"encoding/json"
	"time"
)

const (
	claudeDefaultMode = "observe"
)

type anthropicMessagesRequest struct {
	Model    string             `json:"model"`
	Stream   bool               `json:"stream"`
	System   json.RawMessage    `json:"system,omitempty"`
	Messages []anthropicMessage `json:"messages"`
}

type anthropicMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

type claudeRequestShape struct {
	MessageCount      int
	ContentBlockCount int
	ToolResultCount   int
	LatestUserText    string
	SystemText        string
}

// claudeOptimizerStats holds per-request accounting populated by prepareClaudeBody.
// All byte/token fields reflect the state after optimization (or the original if skipped).
type claudeOptimizerStats struct {
	// Block/span counts (backward-compatible log field names).
	BlocksSeen   int `json:"blocks_seen"`
	BlocksNew    int `json:"blocks_new"`
	BlocksKnown  int `json:"blocks_known"`
	BlocksPruned int `json:"blocks_pruned"`

	// Byte/token accounting.
	BytesBefore   int `json:"bytes_before"`
	BytesAfter    int `json:"bytes_after"`
	BytesEligible int `json:"bytes_eligible"`
	BytesPruned   int `json:"bytes_pruned"`
	// KnownBytes is the byte total of every span that hit the session cache
	// (Seen returned true), independent of whether the span was subsequently
	// pruned or preserved by a protection rule. Invariant:
	//   KnownBytes == BytesPruned + PreservedInstructionBytes
	//                + PreservedLatestTurnBytes + PreservedLastOccurrenceBytes
	KnownBytes            int     `json:"known_bytes"`
	EstimatedTokensBefore int     `json:"estimated_tokens_before"`
	EstimatedTokensAfter  int     `json:"estimated_tokens_after"`
	EstimatedTokensSaved  int     `json:"estimated_tokens_saved"`
	ReductionRatio        float64 `json:"reduction_ratio"`

	// Per-class byte accounting (keyed by SpanClass* constants).
	ClassBytesSeen     map[string]int `json:"class_bytes_seen,omitempty"`
	ClassBytesEligible map[string]int `json:"class_bytes_eligible,omitempty"`
	ClassBytesPruned   map[string]int `json:"class_bytes_pruned,omitempty"`
	ClassSpansSeen     map[string]int `json:"class_spans_seen,omitempty"`
	ClassSpansPruned   map[string]int `json:"class_spans_pruned,omitempty"`

	// Preserve-reason counters.
	PreservedLatestTurnBytes     int `json:"preserved_latest_turn_bytes"`
	PreservedLatestTurnCount     int `json:"preserved_latest_turn_count"`
	PreservedSmallBytes          int `json:"preserved_small_bytes"`
	PreservedSmallCount          int `json:"preserved_small_count"`
	PreservedSystemBytes         int `json:"preserved_system_bytes"`
	PreservedSystemCount         int `json:"preserved_system_count"`
	PreservedToolUseBytes        int `json:"preserved_tool_use_bytes"`
	PreservedToolUseCount        int `json:"preserved_tool_use_count"`
	PreservedInstructionBytes    int `json:"preserved_instruction_bytes"`
	PreservedInstructionCount    int `json:"preserved_instruction_count"`
	PreservedLastOccurrenceBytes int `json:"preserved_last_occurrence_bytes"`
	PreservedLastOccurrenceCount int `json:"preserved_last_occurrence_count"`
	// PreservedCachePrefix counts spans left untouched because they sit inside
	// Anthropic's prompt-cache prefix (a cache_control breakpoint covers them).
	// Rewriting them would invalidate the cached prefix for the whole suffix,
	// costing far more than the bytes saved — so the optimizer preserves them.
	PreservedCachePrefixBytes int `json:"preserved_cache_prefix_bytes"`
	PreservedCachePrefixCount int `json:"preserved_cache_prefix_count"`

	// PreservedCacheFidelity is set when the whole body was forwarded byte-for-byte
	// because the client manages prompt caching (cache_control present). Any rewrite
	// would re-serialize the cached prefix and bust Anthropic's cache.
	PreservedCacheFidelity bool `json:"preserved_cache_fidelity"`

	// Size tracking.
	LargestSpanBytes   int `json:"largest_span_bytes"`
	LargestPrunedBytes int `json:"largest_pruned_bytes"`
}

type claudeStreamStats struct {
	Chunks        int
	OutputText    int
	OutputRawText string
	OutputTokens  int
	// Real upstream input accounting, captured from the message_start event's
	// usage object. These are measured ground truth (not byte estimates).
	// InputTokens excludes cached tokens; CacheReadInputTokens is context Anthropic
	// served from its prompt cache (≈10% cost, the latency win); CacheCreation is
	// context written into the cache this turn.
	InputTokens              int
	CacheReadInputTokens     int
	CacheCreationInputTokens int
	Status                   string
	StatusCode               int
	Cancelled                bool
	Completed                bool
	HasToolUse               bool
	Provider                 string // "anthropic" or "bedrock"
	UpstreamErr              string
	UpstreamErrorCode        string
	UpstreamErrorType        string
	UpstreamRequestID        string
	RetryAfter               time.Duration
}

type claudeUpstreamErrorMeta struct {
	StatusCode int
	Code       string
	Type       string
	Message    string
	RequestID  string
	RetryAfter time.Duration
}
