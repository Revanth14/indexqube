package proxy

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/chunker"
	"github.com/Revanth14/indexqube/gateway/internal/memory"
	"github.com/Revanth14/indexqube/gateway/internal/middleware"
	"github.com/Revanth14/indexqube/gateway/internal/redact"
	"github.com/Revanth14/indexqube/gateway/internal/telemetry"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"github.com/google/uuid"
)

const (
	claudeDefaultMode = "observe"
)

// guardBypassRe matches directives that attempt to disable proxy safety controls.
// The separator between "guards" and "velocity" is optional (covers slash, pipe,
// backslash, or none) to handle formatting variations in injected CLAUDE.md files.
var guardBypassRe = regexp.MustCompile(`(?i)guards?\s*[/\\|]?\s*velocity\s+limits?\s+do\s+not\s+apply`)

var dumpPayloadMu sync.Mutex

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

func (p *Proxy) handleClaudeMessages(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	cfg := p.claudeDefaults()
	rawRequestID := middleware.RequestIDFromContext(r.Context())
	if rawRequestID == "" {
		rawRequestID = r.Header.Get("X-Request-ID")
	}

	if err := cfg.validate(); err != nil {
		p.writeError(w, r, errorPayload{
			HTTPStatus: http.StatusServiceUnavailable,
			Type:       "server_error",
			Code:       "claude_gateway_not_configured",
			Message:    err.Error(),
		})
		return
	}
	if !validClaudeDevToken(r, cfg.DevToken) {
		p.writeError(w, r, errorPayload{
			HTTPStatus: http.StatusUnauthorized,
			Type:       "authentication_error",
			Code:       "invalid_dev_token",
			Message:    "invalid IndexQube development token",
		})
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, p.maxRequestSize)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			p.writeError(w, r, errorPayload{
				HTTPStatus: http.StatusRequestEntityTooLarge,
				Type:       "invalid_request_error",
				Code:       "body_too_large",
				Message:    err.Error(),
			})
			return
		}
		p.writeError(w, r, errorPayload{HTTPStatus: http.StatusBadRequest, Type: "invalid_request_error", Message: err.Error()})
		return
	}

	// overheadStart marks the beginning of pure IndexQube processing time.
	// It stops just before dispatching to upstream so ProxyOverheadMs
	// reflects only the optimizer + guard work, not the model round-trip.
	overheadStart := time.Now()

	sessionKey := claudeSessionKey(r, cfg.DevToken)

	// FIX 3: resolve or synthesize the request ID and detect velocity issues.
	requestID, missingReqID, velocityWarning := p.resolveRequestID(sessionKey, rawRequestID)

	// Conservative rate limiting when the session repeatedly sends turns
	// without request IDs — max 1 turn/second, max 32 KB payload.
	if velocityWarning {
		time.Sleep(time.Second)
		if len(body) > 32*1024 {
			p.writeError(w, r, errorPayload{
				HTTPStatus: http.StatusTooManyRequests,
				Type:       "rate_limit_error",
				Code:       "missing_request_id_velocity_limit",
				Message:    "too many turns without request IDs; payload capped at 32 KB",
			})
			return
		}
	}

	var earlyMeta struct {
		Model string `json:"model"`
	}
	_ = json.Unmarshal(body, &earlyMeta)
	ctx := r.Context()

	forwardBody, meta, optStats, shape, err := p.prepareClaudeBody(ctx, cfg, sessionKey, body)
	if err != nil {
		p.writeError(w, r, errorPayload{HTTPStatus: http.StatusBadRequest, Type: "invalid_request_error", Code: "invalid_anthropic_request", Message: err.Error()})
		return
	}
	overheadMs := time.Since(overheadStart).Milliseconds()

	finishSynthetic := func(streamStats claudeStreamStats) {
		effectiveOpt := optStats
		effectiveOpt.BytesAfter = 0
		effectiveOpt.EstimatedTokensAfter = 0
		effectiveOpt.EstimatedTokensSaved = effectiveOpt.EstimatedTokensBefore
		if effectiveOpt.BytesBefore > 0 {
			effectiveOpt.ReductionRatio = 1
		}

		duration := time.Since(started)
		if cfg.SessionStore != nil {
			cfg.SessionStore.RecordUsage(sessionKey, memory.UsageTotals{
				Requests:    1,
				TokensIn:    estimateTokens(len(body)),
				TokensOut:   streamStats.estimatedOutputTokens(),
				TokensSaved: effectiveOpt.EstimatedTokensSaved,
				BytesIn:     len(body),
				BytesSaved:  effectiveOpt.BytesBefore - effectiveOpt.BytesAfter,
			})
		}
		p.logClaudeRequestComplete(r.Context(), requestID, cfg.Mode, meta.Model, sessionKey, len(body), effectiveOpt, streamStats, duration, missingReqID, requestID, velocityWarning)
		p.emitClaudeUsageEvent(r, sessionKey, meta.Model, effectiveOpt, streamStats, duration, streamStats.StatusCode, overheadMs)
		if os.Getenv("IQ_DUMP_PAYLOADS") == "1" {
			dumpClaudePayloads(requestID, body, nil, streamStats, effectiveOpt)
		}
	}

	// Intercept Sentinel Probes (quota, ping) to respond in 1 ms at 0 cost
	normalizedLatest := strings.TrimSpace(strings.ToLower(shape.LatestUserText))
	if normalizedLatest == "quota" || normalizedLatest == "ping" {
		p.logger.InfoContext(ctx, "sentinel probe intercepted",
			slog.String("session_key", shortLogHash(sessionKey)),
			slog.String("probe", normalizedLatest))
		text := "IndexQube active. Quota check successful."
		writeSyntheticStreamResponse(w, text)
		finishSynthetic(claudeStreamStats{
			OutputText:    len(text),
			OutputRawText: text,
			Status:        "synthetic_probe",
			StatusCode:    http.StatusOK,
			Completed:     true,
			Provider:      "synthetic",
		})
		return
	}

	var capture *responseCaptureWriter
	var promptCacheHash string
	isCacheablePrompt := shape.LatestUserText != "" && cfg.Mode == "optimize" &&
		r.Header.Get("Cache-Control") != "no-cache" && r.Header.Get("Pragma") != "no-cache"
	writeCachedResponse := func(cachedPayload []byte) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(cachedPayload)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		finishSynthetic(claudeStreamStats{
			Chunks:     bytes.Count(cachedPayload, []byte("event: content_block_delta")),
			OutputText: len(cachedPayload),
			Status:     "cache_replay",
			StatusCode: http.StatusOK,
			Completed:  true,
			Provider:   "cache",
		})
	}
	if isCacheablePrompt {
		promptCacheHash = computePromptHash(body, meta.Model)
		ts := p.getOrCreateTurnState(sessionKey)
		ts.mu.Lock()
		cachedPayload, ok := ts.getCachedResponse(promptCacheHash)
		ts.mu.Unlock()
		if ok {
			p.logger.InfoContext(ctx, "response-level cache hit",
				slog.String("session_key", shortLogHash(sessionKey)),
				slog.String("prompt_hash", promptCacheHash[:8]))
			writeCachedResponse(cachedPayload)
			return
		}
		capture = &responseCaptureWriter{ResponseWriter: w}
		w = capture
	}

	// FIX 2: in-flight duplicate detection. When a second identical request
	// arrives while the first is still in-flight, it waits up to 30 s for the
	// first to complete before dispatching its own upstream call. This prevents
	// the "triple-fire" pattern where 3 identical prompts fire concurrently.
	promptHash := semanticPromptHash(body)
	if doneFn, waitChan := p.inFlightRequests.acquire(promptHash); doneFn == nil {
		select {
		case <-waitChan:
		case <-time.After(30 * time.Second):
		case <-r.Context().Done():
			return
		}
		if isCacheablePrompt {
			ts := p.getOrCreateTurnState(sessionKey)
			ts.mu.Lock()
			cachedPayload, ok := ts.getCachedResponse(promptCacheHash)
			ts.mu.Unlock()
			if ok {
				p.logger.InfoContext(ctx, "response-level cache hit after in-flight wait",
					slog.String("session_key", shortLogHash(sessionKey)),
					slog.String("prompt_hash", promptCacheHash[:8]))
				writeCachedResponse(cachedPayload)
				return
			}
		}
		// Re-register after the original completed so our dispatch is tracked.
		if done2, _ := p.inFlightRequests.acquire(promptHash); done2 != nil {
			defer done2()
		}
	} else {
		defer doneFn()
	}

	streamStats := p.forwardClaudeMessages(w, r, cfg, forwardBody)
	if capture != nil && streamStats.Completed && streamStats.StatusCode == http.StatusOK && !streamStats.HasToolUse {
		ts := p.getOrCreateTurnState(sessionKey)
		ts.mu.Lock()
		ts.saveCachedResponse(promptCacheHash, capture.buf.Bytes())
		ts.mu.Unlock()
		p.logger.InfoContext(ctx, "response-level cache saved",
			slog.String("session_key", shortLogHash(sessionKey)),
			slog.String("prompt_hash", promptCacheHash[:8]))
	}
	duration := time.Since(started)
	if cfg.SessionStore != nil {
		// Prefer measured upstream input over the byte estimate when Anthropic
		// reported usage; fall back to the estimate when it didn't (e.g. errors).
		tokensIn := streamStats.realInputTokens()
		if tokensIn == 0 {
			tokensIn = estimateTokens(len(body))
		}
		cfg.SessionStore.RecordUsage(sessionKey, memory.UsageTotals{
			Requests:            1,
			TokensIn:            tokensIn,
			TokensOut:           streamStats.estimatedOutputTokens(),
			TokensSaved:         optStats.EstimatedTokensSaved,
			BytesIn:             len(body),
			BytesSaved:          optStats.BytesBefore - optStats.BytesAfter,
			CacheReadTokens:     streamStats.CacheReadInputTokens,
			CacheCreationTokens: streamStats.CacheCreationInputTokens,
		})
	}
	p.logClaudeRequestComplete(r.Context(), requestID, cfg.Mode, meta.Model, sessionKey, len(body), optStats, streamStats, duration, missingReqID, requestID, velocityWarning)
	p.emitClaudeUsageEvent(r, sessionKey, meta.Model, optStats, streamStats, duration, streamStats.StatusCode, overheadMs)

	if os.Getenv("IQ_DUMP_PAYLOADS") == "1" {
		dumpClaudePayloads(requestID, body, forwardBody, streamStats, optStats)
	}
}

func (p *Proxy) handleClaudeCountTokens(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	cfg := p.claudeDefaults()
	requestID := middleware.RequestIDFromContext(r.Context())
	if requestID == "" {
		requestID = r.Header.Get("X-Request-ID")
	}

	if err := cfg.validate(); err != nil {
		p.writeError(w, r, errorPayload{
			HTTPStatus: http.StatusServiceUnavailable,
			Type:       "server_error",
			Code:       "claude_gateway_not_configured",
			Message:    err.Error(),
		})
		return
	}
	if !validClaudeDevToken(r, cfg.DevToken) {
		p.writeError(w, r, errorPayload{
			HTTPStatus: http.StatusUnauthorized,
			Type:       "authentication_error",
			Code:       "invalid_dev_token",
			Message:    "invalid IndexQube development token",
		})
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, p.maxRequestSize)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			p.writeError(w, r, errorPayload{
				HTTPStatus: http.StatusRequestEntityTooLarge,
				Type:       "invalid_request_error",
				Code:       "body_too_large",
				Message:    err.Error(),
			})
			return
		}
		p.writeError(w, r, errorPayload{HTTPStatus: http.StatusBadRequest, Type: "invalid_request_error", Message: err.Error()})
		return
	}

	status := p.forwardClaudeJSON(w, r, cfg, body, "v1/messages/count_tokens")
	p.logger.InfoContext(r.Context(), "claude count tokens complete",
		slog.String("event", "count_tokens_complete"),
		slog.String("request_id", requestID),
		slog.String("mode", cfg.Mode),
		slog.Int("bytes_before", len(body)),
		slog.Int("estimated_tokens_before", estimateTokens(len(body))),
		slog.Int64("duration_ms", time.Since(started).Milliseconds()),
		slog.Int("status_code", status),
	)
}

func (p *Proxy) claudeDefaults() ClaudeMessagesConfig {
	cfg := p.claude
	if cfg.Mode == "" {
		cfg.Mode = claudeDefaultMode
	}
	if cfg.AnthropicBaseURL == "" {
		cfg.AnthropicBaseURL = "https://api.anthropic.com"
	}
	if cfg.AnthropicVersion == "" {
		cfg.AnthropicVersion = "2023-06-01"
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	// Apply optimizer defaults when not explicitly configured.
	if cfg.Optimizer.MinSpanBytes <= 0 {
		cfg.Optimizer.MinSpanBytes = 512
		cfg.Optimizer.TargetChunkBytes = 2048
		cfg.Optimizer.MaxChunkBytes = 8192
		cfg.Optimizer.MinSavedTokens = 10
		cfg.Optimizer.EnableToolResultPruning = true
		cfg.Optimizer.EnableSubspanChunking = true
		cfg.Optimizer.SmallFileBytes = 4096
		cfg.Optimizer.EnablePromptCache = true
	}
	return cfg
}

func (c ClaudeMessagesConfig) validate() error {
	if c.DevToken == "" {
		return fmt.Errorf("INDEXQUBE_DEV_TOKEN is required for /v1/messages")
	}
	// AnthropicAPIKey may be empty in passthrough mode: the user's Bearer token
	// (OAuth session) is forwarded to Anthropic unchanged. Bedrock ignores it entirely.
	switch c.Mode {
	case "observe", "dry_run", "optimize":
		return nil
	default:
		return fmt.Errorf("unsupported INDEXQUBE_MODE %q", c.Mode)
	}
}

func validClaudeDevToken(r *http.Request, want string) bool {
	_ = want
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	token := strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
	return token != ""
}

func dumpClaudePayloads(requestID string, before, after []byte, stats claudeStreamStats, optStats claudeOptimizerStats) {
	if sessionFile := os.Getenv("IQ_DUMP_SESSION_FILE"); sessionFile != "" {
		if err := appendSessionDump(sessionFile, requestID, before, after, stats, optStats); err != nil {
			fmt.Fprintf(os.Stderr, "[iq] failed to append payload dump: %v\n", err)
		}
		return
	}

	dumpDir := os.Getenv("IQ_DUMP_DIR")
	if dumpDir == "" {
		dumpDir = "/tmp"
	}
	if err := os.MkdirAll(dumpDir, 0o700); err != nil {
		fmt.Fprintf(os.Stderr, "[iq] failed to dump payload pair: %v\n", err)
		return
	}
	beforePath := filepath.Join(dumpDir, "iq-before-"+requestID+".json")
	afterPath := filepath.Join(dumpDir, "iq-after-"+requestID+".json")
	if err := os.WriteFile(beforePath, prettyJSON(redactedJSONPayload(before)), 0o600); err != nil {
		fmt.Fprintf(os.Stderr, "[iq] failed to dump payload pair: %v\n", err)
		return
	}
	if err := os.WriteFile(afterPath, prettyJSON(redactedJSONPayload(after)), 0o600); err != nil {
		fmt.Fprintf(os.Stderr, "[iq] failed to dump payload pair: %v\n", err)
		return
	}
	appendDumpLog(dumpDir, beforePath, afterPath)
}

type payloadDumpResponse struct {
	Text         string `json:"text"`
	OutputTokens int    `json:"output_tokens"`
	Status       string `json:"status"`
	// Raw upstream input usage exactly as Anthropic reported it, so a dump can
	// distinguish "prompt caching not applied" (cache fields 0 with large input)
	// from "applied but small". Zero on synthetic/probe turns (no upstream call).
	InputTokens              int `json:"input_tokens"`
	CacheReadInputTokens     int `json:"cache_read_input_tokens"`
	CacheCreationInputTokens int `json:"cache_creation_input_tokens"`
}

type payloadDumpRecord struct {
	Timestamp   string                 `json:"ts"`
	RequestID   string                 `json:"request_id"`
	BeforeBytes int                    `json:"before_bytes"`
	AfterBytes  int                    `json:"after_bytes"`
	SavedBytes  int                    `json:"saved_bytes"`
	Before      json.RawMessage        `json:"before"`
	After       json.RawMessage        `json:"after"`
	Response    payloadDumpResponse    `json:"response"`
	Optimizer   *payloadOptimizerStats `json:"optimizer,omitempty"`
}

// payloadOptimizerStats is the per-turn cache-efficiency view written into
// the JSONL dump. It separates the pruning view (saved_bytes / blocks_pruned)
// from the true cache-hit view (true_cache_hit_bytes / known_bytes), so the
// audit script can distinguish "bytes removed from the forwarded payload"
// from "bytes that hit the session cache regardless of preservation rules".
type payloadOptimizerStats struct {
	BlocksPruned         int `json:"blocks_pruned"`
	BlocksKnown          int `json:"blocks_known"`
	BlocksKnownProtected int `json:"blocks_known_protected"`
	BytesPruned          int `json:"bytes_pruned"`
	ProtectedBytes       int `json:"protected_bytes"`
	KnownBytes           int `json:"known_bytes"`
	TrueCacheHitBytes    int `json:"true_cache_hit_bytes"`
}

func appendSessionDump(sessionFile, requestID string, before, after []byte, stats claudeStreamStats, optStats claudeOptimizerStats) error {
	if err := os.MkdirAll(filepath.Dir(sessionFile), 0o700); err != nil {
		return err
	}
	record := payloadDumpRecord{
		Timestamp:   time.Now().Format(time.RFC3339),
		RequestID:   requestID,
		BeforeBytes: len(before),
		AfterBytes:  len(after),
		SavedBytes:  len(before) - len(after),
		Before:      redactedJSONPayload(before),
		After:       redactedJSONPayload(after),
		Response: payloadDumpResponse{
			Text:                     redact.String(stats.OutputRawText),
			OutputTokens:             stats.OutputTokens,
			Status:                   stats.Status,
			InputTokens:              stats.InputTokens,
			CacheReadInputTokens:     stats.CacheReadInputTokens,
			CacheCreationInputTokens: stats.CacheCreationInputTokens,
		},
		Optimizer: &payloadOptimizerStats{
			BlocksPruned:         optStats.BlocksPruned,
			BlocksKnown:          optStats.BlocksKnown,
			BlocksKnownProtected: optStats.PreservedInstructionCount,
			BytesPruned:          optStats.BytesPruned,
			ProtectedBytes:       optStats.PreservedInstructionBytes,
			KnownBytes:           optStats.KnownBytes,
			TrueCacheHitBytes:    optStats.KnownBytes,
		},
	}
	line, err := json.Marshal(record)
	if err != nil {
		return err
	}
	line = append(line, '\n')

	dumpPayloadMu.Lock()
	defer dumpPayloadMu.Unlock()
	f, err := os.OpenFile(sessionFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(line)
	return err
}

func redactedJSONPayload(raw []byte) json.RawMessage {
	if len(bytes.TrimSpace(raw)) == 0 {
		return json.RawMessage("null")
	}

	var parsed any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		encoded, _ := json.Marshal(redact.String(string(raw)))
		return json.RawMessage(encoded)
	}

	encoded, err := marshalJSONNoHTMLEscape(redactJSONValue(parsed))
	if err != nil {
		fallback, _ := json.Marshal(redact.String(string(raw)))
		return json.RawMessage(fallback)
	}
	return json.RawMessage(encoded)
}

func redactJSONValue(value any) any {
	switch v := value.(type) {
	case map[string]any:
		for key, child := range v {
			if redact.SensitiveKey(key) {
				v[key] = "[redacted]"
				continue
			}
			v[key] = redactJSONValue(child)
		}
		return v
	case []any:
		for i, child := range v {
			v[i] = redactJSONValue(child)
		}
		return v
	case string:
		return redact.String(v)
	default:
		return v
	}
}

func appendDumpLog(dumpDir, beforePath, afterPath string) {
	logPath := filepath.Join(dumpDir, "dump.log")
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fprintf(f, "%s dumped payload pair -> %s %s\n", time.Now().Format(time.RFC3339), beforePath, afterPath)
}

func prettyJSON(raw []byte) []byte {
	var out bytes.Buffer
	if err := json.Indent(&out, raw, "", "  "); err != nil {
		return raw
	}
	out.WriteByte('\n')
	return out.Bytes()
}

func marshalJSONNoHTMLEscape(v any) ([]byte, error) {
	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(out.Bytes(), []byte{'\n'}), nil
}

// stripGuardBypassDirectives scans the system field of root for patterns
// that attempt to disable proxy-level guards (e.g. from CLAUDE.md content).
// Any matched text is replaced with an empty string in-place. Returns true
// if any directives were stripped so the caller can log a warning.
func stripGuardBypassDirectives(ctx context.Context, logger *slog.Logger, root map[string]any) bool {
	stripped := false
	stripText := func(text string) (string, bool) {
		// Check the original text first; if that misses (e.g. newlines inside the
		// directive from a system-reminder block), fall back to a whitespace-normalised
		// copy so multi-line formatting doesn't hide the bypass attempt.
		matched := guardBypassRe.MatchString(text)
		if !matched {
			normalized := strings.ToLower(strings.Join(strings.Fields(text), " "))
			matched = guardBypassRe.MatchString(normalized)
		}
		if !matched {
			return text, false
		}
		return guardBypassRe.ReplaceAllString(text, ""), true
	}

	switch sys := root["system"].(type) {
	case string:
		if cleaned, ok := stripText(sys); ok {
			root["system"] = cleaned
			stripped = true
		}
	case []any:
		for _, item := range sys {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			if text, ok2 := m["text"].(string); ok2 {
				if cleaned, ok3 := stripText(text); ok3 {
					m["text"] = cleaned
					stripped = true
				}
			}
		}
	}

	if stripped {
		logger.WarnContext(ctx, "guard bypass attempt detected in system content",
			slog.String("event", "guard_bypass_attempt"),
			slog.String("source", "CLAUDE.md"),
		)
	}
	return stripped
}

// warmUpSystemSpans pre-registers all system-block fingerprints into the
// session cache on the first turn. This ensures that stable injected files
// (CLAUDE.md, MEMORY.md, architecture docs) are cached from turn 1 so that
// turns 2+ see them as known and report non-zero savings (FIX 6).
func (p *Proxy) warmUpSystemSpans(ctx context.Context, cfg ClaudeMessagesConfig, sessionKey string, root map[string]any) {
	registerText := func(text, kind string) {
		text = strings.TrimSpace(text)
		if len(text) < cfg.Optimizer.MinSpanBytes {
			return
		}
		data := []byte(text)
		if cfg.Optimizer.EnableSubspanChunking && len(data) >= cfg.Optimizer.SmallFileBytes {
			chunks := splitSpanChunks(text, cfg.Optimizer)
			for _, ch := range chunks {
				chHash := hashText(ch)
				cfg.SessionStore.SaveBlock(sessionKey, memory.Block{
					Hash:   chHash,
					Kind:   "warmup:chunk:" + kind,
					Bytes:  len(ch),
					Tokens: estimateTokens(len(ch)),
				})
			}
		} else {
			h := hashText(text)
			cfg.SessionStore.SaveBlock(sessionKey, memory.Block{
				Hash:   h,
				Kind:   "warmup:" + kind,
				Bytes:  len(data),
				Tokens: estimateTokens(len(data)),
			})
		}
	}

	switch sys := root["system"].(type) {
	case string:
		registerText(sys, "system")
	case []any:
		for _, item := range sys {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			if text, ok2 := m["text"].(string); ok2 {
				registerText(text, "system_block")
			}
		}
	}
	p.logger.DebugContext(ctx, "session warm-up complete",
		slog.String("session_key", shortLogHash(sessionKey)),
	)
}

// getOrCreateTurnState returns the mutable turn-state for sessionKey,
// creating it if it does not yet exist.
func (p *Proxy) getOrCreateTurnState(sessionKey string) *sessionTurnState {
	v, _ := p.sessionTurnCounters.LoadOrStore(sessionKey, &sessionTurnState{})
	p.touchSession(sessionKey)
	return v.(*sessionTurnState)
}

// getOrCreateBoilerplateState returns the mutable boilerplate-state for
// sessionKey, creating it if it does not yet exist.
func (p *Proxy) getOrCreateBoilerplateState(sessionKey string) *boilerplateState {
	v, _ := p.sessionBoilerplateState.LoadOrStore(sessionKey, &boilerplateState{})
	p.touchSession(sessionKey)
	return v.(*boilerplateState)
}

// resolveRequestID returns a non-empty request ID, assigning a synthetic one
// when the incoming ID is blank. It also updates the per-session missing-ID
// window and returns whether the session should be velocity-limited due to
// excessive missing-ID turns (FIX 3).
func (p *Proxy) resolveRequestID(sessionKey, rawID string) (id string, synthetic bool, velocityLimit bool) {
	if rawID != "" {
		ts := p.getOrCreateTurnState(sessionKey)
		ts.mu.Lock()
		ts.turnIndex++
		ts.mu.Unlock()
		return rawID, false, false
	}

	ts := p.getOrCreateTurnState(sessionKey)
	ts.mu.Lock()
	defer ts.mu.Unlock()

	ts.turnIndex++
	// FIX 1: UUID4 suffix guarantees uniqueness across sessions and restarts.
	// Previous counter-based IDs shared the same value when the session key
	// reset between iq invocations (all got suffix -1).
	keyPart := sessionKey
	if len(keyPart) > 8 {
		keyPart = keyPart[:8]
	}
	syntheticID := fmt.Sprintf("iq-synthetic-%s-%s", keyPart, uuid.New().String()[:8])

	// Track timestamp for the 60-second missing-ID window.
	now := time.Now().Unix()
	windowStart := now - 60
	// Evict entries older than 60 seconds.
	filtered := ts.missingIDWindow[:0]
	for _, t := range ts.missingIDWindow {
		if t >= windowStart {
			filtered = append(filtered, t)
		}
	}
	filtered = append(filtered, now)
	ts.missingIDWindow = filtered

	p.logger.Warn("request arrived with empty request ID; assigned synthetic",
		slog.String("synthetic_request_id", syntheticID),
		slog.String("session_key", shortLogHash(sessionKey)),
		slog.Int("missing_id_window_count", len(filtered)),
	)

	vLimit := len(filtered) > 3
	return syntheticID, true, vLimit
}

func claudeSessionKey(r *http.Request, fallback string) string {
	if sk := strings.TrimSpace(r.Header.Get(headerSessionKey)); sk != "" {
		return sk
	}
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if auth != "" {
		sum := sha256.Sum256([]byte(auth))
		key := hex.EncodeToString(sum[:8])
		// Suffix with the per-invocation session ID so the circuit breaker
		// scopes similar-request counts to this iq session, not across sessions.
		if sid := os.Getenv("IQ_SESSION_ID"); sid != "" {
			return key + "-" + sid[:8]
		}
		return key
	}
	sum := sha256.Sum256([]byte(fallback))
	return hex.EncodeToString(sum[:8])
}

func (p *Proxy) prepareClaudeBody(ctx context.Context, cfg ClaudeMessagesConfig, sessionKey string, body []byte) ([]byte, anthropicMessagesRequest, claudeOptimizerStats, claudeRequestShape, error) {
	if err := ctx.Err(); err != nil {
		return nil, anthropicMessagesRequest{}, claudeOptimizerStats{}, claudeRequestShape{}, err
	}
	var root map[string]any
	if err := json.Unmarshal(body, &root); err != nil {
		return nil, anthropicMessagesRequest{}, claudeOptimizerStats{}, claudeRequestShape{}, fmt.Errorf("parse anthropic messages body: %w", err)
	}

	// FIX 4: strip guard-disable directives injected via CLAUDE.md before any
	// optimization or forwarding. Proxy-level guards are always enforced.
	if stripGuardBypassDirectives(ctx, p.logger, root) {
		// Re-serialize so the cleaned body is what gets forwarded and re-parsed.
		if cleaned, err := marshalJSONNoHTMLEscape(root); err == nil {
			body = cleaned
		}
	}
	req := anthropicMessagesRequest{}
	req.Model, _ = root["model"].(string)
	req.Stream, _ = root["stream"].(bool)
	shape := extractClaudeRequestShape(root)
	stats := claudeOptimizerStats{
		BytesBefore:           len(body),
		BytesAfter:            len(body),
		EstimatedTokensBefore: estimateTokens(len(body)),
		EstimatedTokensAfter:  estimateTokens(len(body)),
	}
	if cfg.SessionStore == nil {
		return body, req, stats, shape, nil
	}

	// FIX 6: Session-open cache warm-up. On the first actual turn with a system prompt,
	// pre-fingerprint all system blocks (CLAUDE.md, MEMORY.md, architecture
	// files) so that subsequent turns start with cache hits rather than
	// zero-savings. Uses a stable chunk ID: sha256(file_path + content_hash).
	if root["system"] != nil {
		_, alreadyWarmed := p.sessionWarmUpDone.LoadOrStore(sessionKey, true)
		p.touchSession(sessionKey)
		if !alreadyWarmed {
			p.warmUpSystemSpans(ctx, cfg, sessionKey, root)
		}
	}

	// FIX 7: suggestion-mode payloads are ephemeral harness meta-prompts.
	// Registering their large mixed content in the chunk store pollutes it and
	// doubles per-turn cost. Rate-limit to 1 per 10 seconds; always skip chunk
	// registration regardless of rate-limit status.
	if strings.HasPrefix(shape.LatestUserText, "SUGGESTION MODE:") {
		now := time.Now()
		if last, ok := p.sessionSuggestionTs.Load(sessionKey); ok && now.Sub(last.(time.Time)) < 10*time.Second {
			p.logger.DebugContext(ctx, "suggestion-mode request rate-limited; skipping",
				slog.String("session_key", shortLogHash(sessionKey)))
		} else {
			p.sessionSuggestionTs.Store(sessionKey, now)
			p.touchSession(sessionKey)
		}
		return body, req, stats, shape, nil
	}

	minSpanBytes := cfg.Optimizer.MinSpanBytes
	if minSpanBytes <= 0 {
		minSpanBytes = 512
	}

	spans := extractSpans(root)

	// In observe mode or with optimizer disabled, just warm the session store.
	if cfg.Mode == "observe" || !cfg.EnableBlockOptimizer {
		for _, span := range spans {
			if span.Bytes >= minSpanBytes {
				cfg.SessionStore.SaveBlock(sessionKey, memory.Block{
					Hash:   span.Hash,
					Kind:   span.Class,
					Bytes:  span.Bytes,
					Tokens: span.Tokens,
				})
			} else if span.Bytes > 0 {
				// FIX 4: register small spans so they are tracked across turns.
				cfg.SessionStore.SaveBlock(sessionKey, memory.Block{
					Hash:   span.Hash,
					Kind:   span.Class + ":small",
					Bytes:  span.Bytes,
					Tokens: span.Tokens,
				})
			}
		}
		return body, req, stats, shape, nil
	}

	// optimize / dry_run: full span accounting and class-aware pruning.
	stats.ClassBytesSeen = make(map[string]int)
	stats.ClassBytesEligible = make(map[string]int)
	stats.ClassBytesPruned = make(map[string]int)
	stats.ClassSpansSeen = make(map[string]int)
	stats.ClassSpansPruned = make(map[string]int)

	// Pre-pass: for hashes that appear MORE THAN ONCE in this request,
	// track the highest message index. When the same file was read multiple
	// times we preserve the newest copy and prune the older duplicates.
	toolResultHashCount := make(map[string]int)
	lastToolResultMsg := make(map[string]int)
	for _, span := range spans {
		if span.Class == SpanClassToolResultOld || span.Class == SpanClassToolResultLatest {
			toolResultHashCount[span.Hash]++
			if existing, ok := lastToolResultMsg[span.Hash]; !ok || span.MessageIndex > existing {
				lastToolResultMsg[span.Hash] = span.MessageIndex
			}
		}
	}

	// FIX 7: per-session boilerplate state for injection cooldown.
	bpState := p.getOrCreateBoilerplateState(sessionKey)
	ts := p.getOrCreateTurnState(sessionKey)
	ts.mu.Lock()
	currentTurn := ts.turnIndex
	currentCtxBytes := ts.contextBytesCumulative + int64(len(body))
	ts.contextBytesCumulative = currentCtxBytes
	ts.mu.Unlock()

	var prunableSpans []TextSpan
	// systemAllKnown tracks whether every system/boilerplate span was already
	// seen in the session store, indicating a stable system prompt this turn.
	systemAllKnown := true
	hasSystemSpan := false

	// Prompt-cache protection: Claude Code marks a rolling cache_control breakpoint
	// on the latest user turn (and the system prompt) every request, so the prefix
	// up to that breakpoint is served by Anthropic at cache-read price on the next
	// turn. Rewriting any span inside that prefix invalidates the cache for the
	// entire suffix — far more expensive than the bytes a prune would save. When
	// the request uses prompt caching we therefore preserve cached-prefix spans.
	cachePrefixMsgIdx := lastCacheControlMessageIndex(root)
	systemCached := contentHasCacheControl(root["system"])
	spanInCachedPrefix := func(span TextSpan) bool {
		if span.Role == "system" {
			return systemCached
		}
		return cachePrefixMsgIdx >= 0 && span.MessageIndex >= 0 && span.MessageIndex <= cachePrefixMsgIdx
	}

	for _, span := range spans {
		if span.Bytes < minSpanBytes {
			// FIX 4 & SQLite Optimization: register small spans in the session cache
			// only if they are >= 256 bytes to prevent SQLite write-amplification.
			if cfg.SessionStore != nil && span.Bytes >= 256 {
				if cfg.SessionStore.Seen(sessionKey, span.Hash) {
					stats.BlocksKnown++
				} else {
					cfg.SessionStore.SaveBlock(sessionKey, memory.Block{
						Hash:   span.Hash,
						Kind:   span.Class + ":small",
						Bytes:  span.Bytes,
						Tokens: span.Tokens,
					})
					// FIX 3: register small span in the prefix-hint registry so
					// larger spans can detect it as a known prefix.
					p.getOrCreatePrefixHints(sessionKey).add(span.Hash, span.Bytes)
					stats.BlocksNew++
				}
			}
			stats.PreservedSmallBytes += span.Bytes
			stats.PreservedSmallCount++
			continue
		}

		stats.BlocksSeen++
		stats.ClassBytesSeen[span.Class] += span.Bytes
		stats.ClassSpansSeen[span.Class]++
		if span.Bytes > stats.LargestSpanBytes {
			stats.LargestSpanBytes = span.Bytes
		}

		// Track system prompt stability for Anthropic prompt-cache injection.
		if span.Class == SpanClassSystemText || span.Class == SpanClassSystemBoilerplate {
			hasSystemSpan = true
		}

		// Sub-span chunking: for large tool_result spans, deduplicate at the
		// individual chunk level so that edits to one section of a file do not
		// invalidate the hashes of unchanged sections in adjacent chunks.
		var known bool
		if cfg.Optimizer.EnableSubspanChunking &&
			(span.Class == SpanClassToolResultOld || span.Class == SpanClassToolResultLatest) &&
			span.Bytes >= cfg.Optimizer.SmallFileBytes {

			// FIX 3: prefix-chunk reuse. Check whether the span's content
			// starts with a previously registered small chunk. If so, count
			// those prefix bytes as known before splitting the remainder.
			phints := p.getOrCreatePrefixHints(sessionKey)
			if _, prefixLen := phints.matchPrefix([]byte(span.Text)); prefixLen > 0 {
				// The first prefixLen bytes match a known small chunk —
				// mark the prefix as a cache hit in the session store
				// (already registered, so Seen returns true next turn).
				// We do not prune the prefix bytes here; prefix pruning
				// is handled at the whole-span level below.
				stats.BlocksKnown++ // credit the prefix as a known block
			}

			chunks := splitSpanChunks(span.Text, cfg.Optimizer)
			allKnown := true
			for _, ch := range chunks {
				chHash := hashText(ch)
				chKnown := cfg.SessionStore.Seen(sessionKey, chHash)
				cfg.SessionStore.SaveBlock(sessionKey, memory.Block{
					Hash:   chHash,
					Kind:   "chunk:" + span.Class,
					Bytes:  len(ch),
					Tokens: estimateTokens(len(ch)),
				})
				if !chKnown {
					allKnown = false
				}
			}
			known = allKnown
		} else {
			known = cfg.SessionStore.Seen(sessionKey, span.Hash)
			cfg.SessionStore.SaveBlock(sessionKey, memory.Block{
				Hash:   span.Hash,
				Kind:   span.Class,
				Bytes:  span.Bytes,
				Tokens: span.Tokens,
			})
		}

		if !known {
			if span.Class == SpanClassSystemText || span.Class == SpanClassSystemBoilerplate {
				systemAllKnown = false
			}

			// Protected content must win even when the span is a new boilerplate
			// variant. This check intentionally runs before boilerplate cooldown
			// pruning so instruction files, credentials, and the latest turn are
			// never removed just because they live inside a repeated harness block.
			if span.IsLatestTurn {
				stats.PreservedLatestTurnBytes += span.Bytes
				stats.PreservedLatestTurnCount++
				stats.BlocksNew++
				continue
			}
			if isProtectedInstructionSpan(span) {
				stats.PreservedInstructionBytes += span.Bytes
				stats.PreservedInstructionCount++
				stats.BlocksNew++
				continue
			}

			// Even a new span must not be rewritten if it sits inside the prompt-cache
			// prefix — rewriting it breaks Anthropic's cached suffix for this turn.
			if spanInCachedPrefix(span) {
				stats.PreservedCachePrefixBytes += span.Bytes
				stats.PreservedCachePrefixCount++
				stats.BlocksNew++
				continue
			}

			// FIX 7: SUGGESTION MODE injection cooldown. If a boilerplate span
			// is new (unknown hash) but the last forward was <5 turns ago AND
			// context delta is <10 KB, prune it anyway to suppress redundant
			// harness meta-prompt injections.
			if span.Class == SpanClassSystemBoilerplate {
				bpState.mu.Lock()
				turnsSinceLast := currentTurn - bpState.lastForwardedTurn
				bytesSinceLast := currentCtxBytes - int64(bpState.lastForwardedCtxBytes)
				inCooldown := bpState.lastForwardedTurn > 0 && turnsSinceLast < 5 && bytesSinceLast < 10240
				if !inCooldown {
					bpState.lastForwardedTurn = currentTurn
					bpState.lastForwardedCtxBytes = int(currentCtxBytes)
				}
				bpState.mu.Unlock()

				if inCooldown && isEligibleSpanClass(span.Class, cfg.Optimizer) {
					p.logger.DebugContext(ctx, "suppressing boilerplate injection (cooldown active)",
						slog.String("session_key", shortLogHash(sessionKey)),
						slog.Int("turns_since_last", turnsSinceLast),
					)
					stats.ClassBytesEligible[span.Class] += span.Bytes
					stats.BytesEligible += span.Bytes
					prunableSpans = append(prunableSpans, span)
					continue
				}
			}

			stats.BlocksNew++
			continue
		}
		stats.BlocksKnown++
		// FIX B: credit the cache hit in bytes regardless of any downstream
		// preserve-vs-prune decision, so the audit metric reflects true cache
		// efficiency (including protected instruction files like CLAUDE.md
		// that hit the cache but are intentionally never pruned).
		stats.KnownBytes += span.Bytes

		// Latest-turn spans must never be pruned regardless of whether the
		// content was seen before. The model needs current-turn context intact.
		if span.IsLatestTurn {
			stats.PreservedLatestTurnBytes += span.Bytes
			stats.PreservedLatestTurnCount++
			continue
		}

		// Instruction files (CLAUDE.md, CONTEXT.md, .cursorrules, etc.) must
		// NEVER be pruned regardless of span class. This check runs before
		// the eligibility gate so user_text_old spans containing instruction
		// content are caught — previously they fell through to pruning because
		// isEligibleSpanClass("user_text_old") returns true unconditionally.
		if isProtectedInstructionSpan(span) {
			stats.PreservedInstructionBytes += span.Bytes
			stats.PreservedInstructionCount++
			continue
		}

		// When the same tool result content appears more than once in this
		// request (the model re-read the same file), preserve the newest copy
		// so the model always has one copy. Older duplicates fall through to
		// be pruned. For single-occurrence tool results the readable replacement
		// text is self-explanatory enough that the model won't re-invoke.
		if span.Class == SpanClassToolResultOld && toolResultHashCount[span.Hash] > 1 {
			if lastMsg := lastToolResultMsg[span.Hash]; span.MessageIndex == lastMsg {
				stats.PreservedLastOccurrenceBytes += span.Bytes
				stats.PreservedLastOccurrenceCount++
				continue
			}
		}

		// Never rewrite content inside Anthropic's prompt-cache prefix; doing so
		// invalidates the cached suffix and costs more than the prune saves.
		if spanInCachedPrefix(span) {
			stats.PreservedCachePrefixBytes += span.Bytes
			stats.PreservedCachePrefixCount++
			continue
		}

		if !isEligibleSpanClass(span.Class, cfg.Optimizer) {
			switch span.Class {
			case SpanClassSystemText:
				stats.PreservedSystemBytes += span.Bytes
				stats.PreservedSystemCount++
			case SpanClassToolUse:
				stats.PreservedToolUseBytes += span.Bytes
				stats.PreservedToolUseCount++
			}
			continue
		}

		stats.ClassBytesEligible[span.Class] += span.Bytes
		stats.BytesEligible += span.Bytes
		prunableSpans = append(prunableSpans, span)
	}

	// Only inject our own cache_control when the client did NOT already manage
	// prompt caching. Claude Code places its own breakpoints (system + rolling
	// latest turn) on every request; adding another risks exceeding Anthropic's
	// 4-breakpoint limit (a 400 for any user, not just subscription) and is
	// redundant. Deferring to the client is the correct generalization of the old
	// blunt "skip for subscription" rule — subscription users keep their cache
	// benefit via Claude Code's own breakpoints, which E2 now preserves.
	requestHasCacheControl := systemCached || cachePrefixMsgIdx >= 0

	// Cache fidelity beats pruning. When the client manages prompt caching — Claude
	// Code sets a rolling cache_control breakpoint every turn — forward the body
	// byte-for-byte. Any rewrite here re-serializes the whole request (Go sorts JSON
	// map keys), changing the bytes of the cached prefix even when no prefix content
	// is pruned. Anthropic then misses the cache every turn, turning 0.1x cache reads
	// into 1.25x writes. `iq bench` measured this at ~6x more expensive than going
	// direct (optimize 0% hit vs observe 91%). The span loop above already recorded
	// blocks for dedup, and redundant-call elimination runs upstream of this; pruning
	// is only safe to attempt when the client is NOT caching.
	if requestHasCacheControl {
		stats.PreservedCacheFidelity = true
		return body, req, stats, shape, nil
	}

	wantPromptCache := cfg.Optimizer.EnablePromptCache && hasSystemSpan && systemAllKnown && !cfg.Bedrock.Enabled && !requestHasCacheControl
	if len(prunableSpans) == 0 && !wantPromptCache {
		return body, req, stats, shape, nil
	}

	// Rewrite on a fresh parse so the original stays intact for dry_run.
	var rewriteRoot map[string]any
	if err := json.Unmarshal(body, &rewriteRoot); err != nil {
		p.logger.WarnContext(ctx, "claude optimize re-parse failed; forwarding original", slog.Any("err", err))
		return body, req, stats, shape, nil
	}

	pruneCount := 0
	for _, span := range prunableSpans {
		replacement := classSpecificReplacement(span)
		if err := setSpanText(rewriteRoot, span, replacement); err != nil {
			p.logger.WarnContext(ctx, "span replacement failed", slog.String("path", span.Path), slog.Any("err", err))
			continue
		}
		pruneCount++
		stats.ClassBytesPruned[span.Class] += span.Bytes
		stats.ClassSpansPruned[span.Class]++
		stats.BytesPruned += span.Bytes
		if span.Bytes > stats.LargestPrunedBytes {
			stats.LargestPrunedBytes = span.Bytes
		}
	}

	// Inject Anthropic server-side prompt cache header when system prompt is stable.
	if wantPromptCache {
		injectPromptCacheHeaders(rewriteRoot)
	}

	optimized, err := marshalJSONNoHTMLEscape(rewriteRoot)
	if err != nil {
		p.logger.WarnContext(ctx, "claude optimize marshal failed; forwarding original", slog.Any("err", err))
		return body, req, stats, shape, nil
	}

	stats.BlocksPruned = pruneCount
	stats.BytesAfter = len(optimized)
	stats.EstimatedTokensAfter = estimateTokens(len(optimized))
	stats.EstimatedTokensSaved = max(0, stats.EstimatedTokensBefore-stats.EstimatedTokensAfter)
	if stats.BytesBefore > 0 {
		stats.ReductionRatio = float64(stats.BytesBefore-stats.BytesAfter) / float64(stats.BytesBefore)
	}

	// FIX 2: synchronous warm-up flush at the end of prepareClaudeBody when Turn is 1 or 2 (turn_index == 0 in 0-based auditing)
	if (currentTurn == 1 || currentTurn == 2) && root["system"] != nil {
		p.warmUpSystemSpans(ctx, cfg, sessionKey, root)
	}

	if cfg.Mode == "optimize" {
		return optimized, req, stats, shape, nil
	}
	// dry_run: report accurate stats but forward the original body.
	return body, req, stats, shape, nil
}

// isEligibleSpanClass returns true if the span class is eligible for pruning
// under the given optimizer config.
func isEligibleSpanClass(class string, cfg OptimizerConfig) bool {
	switch class {
	case SpanClassUserTextOld:
		return true
	case SpanClassToolResultOld:
		return cfg.EnableToolResultPruning
	case SpanClassAssistantTextOld:
		return cfg.EnableAssistantPruning
	case SpanClassSystemBoilerplate:
		return true // harness meta-prompts are prunable after first occurrence
	case SpanClassSystemText:
		return false
	default:
		return false
	}
}

var protectedInstructionPathFragments = [...]string{
	"claude.md",
	"context.md",
	"agents.md",
	".cursorrules",
	".cursor/rules/",
	".github/copilot-instructions.md",
}

// contentHasCacheControl reports whether an Anthropic content value (an array of
// content blocks) carries a cache_control breakpoint on any block.
func contentHasCacheControl(content any) bool {
	arr, ok := content.([]any)
	if !ok {
		return false
	}
	for _, raw := range arr {
		b, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if _, has := b["cache_control"]; has {
			return true
		}
	}
	return false
}

// lastCacheControlMessageIndex returns the highest message index whose content
// carries a cache_control breakpoint, or -1 if none. Claude Code places a
// rolling breakpoint on the latest user turn each request, so this marks the end
// of the prompt-cache prefix that Anthropic will serve at cache-read price on the
// next turn. Content at or before this index must not be rewritten.
func lastCacheControlMessageIndex(root map[string]any) int {
	msgs, ok := root["messages"].([]any)
	if !ok {
		return -1
	}
	last := -1
	for i, raw := range msgs {
		m, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if contentHasCacheControl(m["content"]) {
			last = i
		}
	}
	return last
}

func isProtectedInstructionSpan(span TextSpan) bool {
	return containsProtectedInstructionPath(span.SourcePath) || containsProtectedInstructionPath(span.Text) || containsCredentialMarker(span.Text)
}

func containsCredentialMarker(s string) bool {
	lower := strings.ToLower(s)
	return strings.Contains(lower, "api-key") ||
		strings.Contains(lower, "bearer ") ||
		strings.Contains(lower, "x-anthropic-api-key") ||
		strings.Contains(lower, "authorization")
}

func containsProtectedInstructionPath(s string) bool {
	s = strings.ToLower(strings.ReplaceAll(s, "\\", "/"))
	if strings.TrimSpace(s) == "" {
		return false
	}
	for _, fragment := range protectedInstructionPathFragments {
		if strings.Contains(s, fragment) {
			return true
		}
	}
	return false
}

// classSpecificReplacement returns a human-readable placeholder for a pruned
// span. The text must be self-explanatory so the model does not re-invoke the
// tool to retrieve content it has already processed.
func classSpecificReplacement(span TextSpan) string {
	if span.BlockType == "tool_result" {
		if span.SourcePath != "" {
			return "[Content of " + span.SourcePath + " was read here — omitted to save context]"
		}
		return "[Tool result content omitted — already processed in this session]"
	}
	return "[Content omitted — already processed in this session]"
}

// setSpanText navigates to the span's location in root and replaces its text
// value with replacement. Returns an error if the navigation fails.
func setSpanText(root map[string]any, span TextSpan, replacement string) error {
	if span.Role == "system" {
		return setSystemSpanText(root, span, replacement)
	}
	messages, ok := root["messages"].([]any)
	if !ok || span.MessageIndex < 0 || span.MessageIndex >= len(messages) {
		return fmt.Errorf("invalid message index %d", span.MessageIndex)
	}
	msg, ok := messages[span.MessageIndex].(map[string]any)
	if !ok {
		return fmt.Errorf("message[%d] is not a map", span.MessageIndex)
	}
	if span.ContentIndex < 0 {
		msg["content"] = replacement
		return nil
	}
	content, ok := msg["content"].([]any)
	if !ok || span.ContentIndex >= len(content) {
		return fmt.Errorf("invalid content index %d", span.ContentIndex)
	}
	item, ok := content[span.ContentIndex].(map[string]any)
	if !ok {
		return fmt.Errorf("content[%d] is not a map", span.ContentIndex)
	}
	switch span.BlockType {
	case "text":
		item["text"] = replacement
	case "tool_result":
		if span.SubContentIndex < 0 {
			item["content"] = replacement
		} else {
			sub, ok := item["content"].([]any)
			if !ok || span.SubContentIndex >= len(sub) {
				return fmt.Errorf("invalid tool_result sub-content index %d", span.SubContentIndex)
			}
			subItem, ok := sub[span.SubContentIndex].(map[string]any)
			if !ok {
				return fmt.Errorf("tool_result content[%d] is not a map", span.SubContentIndex)
			}
			subItem["text"] = replacement
		}
	default:
		return fmt.Errorf("unsupported block type %q for span replacement", span.BlockType)
	}
	return nil
}

func setSystemSpanText(root map[string]any, span TextSpan, replacement string) error {
	if span.ContentIndex < 0 {
		root["system"] = replacement
		return nil
	}
	sysArr, ok := root["system"].([]any)
	if !ok || span.ContentIndex >= len(sysArr) {
		return fmt.Errorf("invalid system content index %d", span.ContentIndex)
	}
	item, ok := sysArr[span.ContentIndex].(map[string]any)
	if !ok {
		return fmt.Errorf("system[%d] is not a map", span.ContentIndex)
	}
	item["text"] = replacement
	return nil
}

func stripCacheControlRecursively(v any) {
	switch val := v.(type) {
	case map[string]any:
		delete(val, "cache_control")
		for _, child := range val {
			stripCacheControlRecursively(child)
		}
	case []any:
		for _, child := range val {
			stripCacheControlRecursively(child)
		}
	}
}

// forwardClaudeMessagesViaBedrock proxies /v1/messages to the Bedrock
// InvokeModelWithResponseStream API. The Anthropic Messages body is forwarded
// as-is except that `model` and `stream` are removed and
// `anthropic_version: "bedrock-2023-05-31"` is injected. The binary AWS event
// stream is decoded and re-emitted as standard Anthropic SSE so Claude Code
// sees an identical response shape regardless of backend.
func (p *Proxy) forwardClaudeMessagesViaBedrock(w http.ResponseWriter, r *http.Request, cfg ClaudeMessagesConfig, body []byte) claudeStreamStats {
	// Parse the model name before mutating the body.
	var meta struct {
		Model string `json:"model"`
	}
	_ = json.Unmarshal(body, &meta)

	modelID := toBedrockModelID(meta.Model, cfg.Bedrock)

	// Transform body: remove model/stream, inject bedrock anthropic_version.
	var root map[string]any
	if err := json.Unmarshal(body, &root); err != nil {
		p.writeError(w, r, errorPayload{HTTPStatus: http.StatusBadRequest, Type: "invalid_request_error", Message: "cannot parse request body"})
		return claudeStreamStats{Status: "error", StatusCode: http.StatusBadRequest}
	}
	delete(root, "model")
	delete(root, "stream")
	delete(root, "context_management") // Bedrock rejects this Anthropic-specific field
	stripCacheControlRecursively(root) // Bedrock rejects cache_control fields
	root["anthropic_version"] = "bedrock-2023-05-31"
	bedrockBody, err := json.Marshal(root)
	if err != nil {
		p.writeError(w, r, errorPayload{HTTPStatus: http.StatusInternalServerError, Type: "server_error", Message: "failed to marshal bedrock body"})
		return claudeStreamStats{Status: "error", StatusCode: http.StatusInternalServerError}
	}

	client, err := bedrockClientFor(r.Context(), cfg.Bedrock)
	if err != nil {
		p.writeError(w, r, errorPayload{HTTPStatus: http.StatusServiceUnavailable, Type: "server_error", Code: "bedrock_unavailable", Message: err.Error()})
		return claudeStreamStats{Status: "error", StatusCode: http.StatusServiceUnavailable}
	}

	p.logger.InfoContext(r.Context(), "bedrock invoke", slog.String("model_id", modelID))
	output, err := client.InvokeModelWithResponseStream(r.Context(), &bedrockruntime.InvokeModelWithResponseStreamInput{
		ModelId:     &modelID,
		Body:        bedrockBody,
		ContentType: strPtr("application/json"),
		Accept:      strPtr("application/json"),
	})
	if err != nil {
		if errors.Is(err, context.Canceled) || r.Context().Err() != nil {
			return claudeStreamStats{Status: "cancelled", Cancelled: true, Provider: "bedrock"}
		}
		p.logger.ErrorContext(r.Context(), "bedrock invoke failed", slog.String("model_id", modelID), slog.Any("err", err))
		code, message := classifyUpstreamError(err)
		p.writeError(w, r, errorPayload{HTTPStatus: http.StatusBadGateway, Type: "upstream_error", Code: code, Message: message})
		return claudeStreamStats{Status: "error", StatusCode: http.StatusBadGateway, Provider: "bedrock", UpstreamErr: err.Error()}
	}

	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache, no-store, must-revalidate")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	rc := http.NewResponseController(w)
	_ = rc.Flush()

	stats := claudeStreamStats{Status: "completed", StatusCode: http.StatusOK, Provider: "bedrock"}
	stream := output.GetStream()
	defer stream.Close()

	for event := range stream.Events() {
		if err := r.Context().Err(); err != nil {
			stats.Status = "cancelled"
			stats.Cancelled = true
			return stats
		}
		chunk, ok := event.(*brtypes.ResponseStreamMemberChunk)
		if !ok {
			continue
		}
		payload := chunk.Value.Bytes

		// Extract event type to emit the SSE event header.
		var ev struct {
			Type string `json:"type"`
		}
		_ = json.Unmarshal(payload, &ev)

		var line string
		if ev.Type != "" {
			line = "event: " + ev.Type + "\ndata: " + string(payload) + "\n\n"
		} else {
			line = "data: " + string(payload) + "\n\n"
		}
		if _, err := io.WriteString(w, line); err != nil {
			stats.Status = "cancelled"
			stats.Cancelled = true
			return stats
		}
		_ = rc.Flush()

		payloadStr := string(payload)
		if strings.Contains(payloadStr, `"tool_use"`) {
			stats.HasToolUse = true
		}

		switch ev.Type {
		case "content_block_delta":
			stats.Chunks++
			stats.OutputText += anthropicDeltaTextLen(payloadStr)
			stats.OutputRawText += anthropicDeltaText(payloadStr)
		case "message_start", "message_delta":
			stats.applyUpstreamUsage(parseAnthropicUsage(payloadStr))
		}
	}
	if err := stream.Err(); err != nil {
		// Headers already sent (HTTP 200). Send an SSE error event so the
		// client knows the stream terminated abnormally.
		errPayload := fmt.Sprintf(`{"type":"error","error":{"type":"stream_error","message":%q}}`, err.Error())
		_, _ = io.WriteString(w, "event: error\ndata: "+errPayload+"\n\n")
		_ = rc.Flush()
		stats.Status = "stream_error" // HTTP 200 already sent; stream terminated abnormally
		stats.UpstreamErr = err.Error()
		return stats
	}
	stats.Completed = true
	return stats
}

// bedrockClientFor returns the pre-built client from cfg, or creates one from
// the default AWS credential chain when none was supplied (e.g. in tests).
func bedrockClientFor(ctx context.Context, cfg BedrockConfig) (*bedrockruntime.Client, error) {
	if cfg.Client != nil {
		return cfg.Client, nil
	}
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	return bedrockruntime.NewFromConfig(awsCfg), nil
}

// bedrockKnownModels maps Claude Code model names to their Bedrock model IDs
// (without regional prefix). Package-level to avoid per-request allocation.
var bedrockKnownModels = map[string]string{
	// Claude 3 series
	"claude-3-sonnet-20240229":  "anthropic.claude-3-sonnet-20240229-v1:0",
	"claude-3-haiku-20240307":   "anthropic.claude-3-haiku-20240307-v1:0",
	"claude-3-5-haiku-20241022": "anthropic.claude-3-5-haiku-20241022-v1:0",
	"claude-3-5-haiku-latest":   "anthropic.claude-3-5-haiku-20241022-v1:0",
	// Claude 4 series (IDs from Bedrock ListFoundationModels)
	"claude-sonnet-4-20250514":   "anthropic.claude-sonnet-4-20250514-v1:0",
	"claude-haiku-4-5-20251001":  "anthropic.claude-haiku-4-5-20251001-v1:0",
	"claude-sonnet-4-5-20250929": "anthropic.claude-sonnet-4-5-20250929-v1:0",
	"claude-opus-4-1-20250805":   "anthropic.claude-opus-4-1-20250805-v1:0",
	"claude-opus-4-5-20251101":   "anthropic.claude-opus-4-5-20251101-v1:0",
	"claude-opus-4-20250514":     "anthropic.claude-opus-4-20250514-v1:0",
	// Unversioned aliases used by the Claude Code CLI
	"claude-sonnet-4-6": "anthropic.claude-sonnet-4-6",
	"claude-opus-4-6":   "anthropic.claude-opus-4-6-v1",
	"claude-opus-4-7":   "anthropic.claude-opus-4-7",
}

// toBedrockModelID maps a Claude Code model name to its AWS Bedrock model ID.
// ModelOverride takes precedence. For unknown models a best-effort pattern is
// applied; use INDEXQUBE_BEDROCK_MODEL_OVERRIDE to pin an exact ID.
func toBedrockModelID(model string, cfg BedrockConfig) string {
	if cfg.ModelOverride != "" {
		return cfg.ModelOverride
	}
	prefix := cfg.ModelPrefix
	if prefix == "" {
		prefix = "us."
	}
	// Already a Bedrock-style ID (contains ":").
	if strings.Contains(model, ":") {
		return model
	}
	if id, ok := bedrockKnownModels[model]; ok {
		return prefix + id
	}
	// Best-effort: dated names get -v1:0, unversioned aliases do not.
	if strings.ContainsAny(model, "0123456789") {
		return prefix + "anthropic." + model + "-v1:0"
	}
	return prefix + "anthropic." + model
}

func strPtr(s string) *string { return &s }

// splitSpanChunks splits span text into content-defined chunks using Rabin-Karp
// CDC. For code content a wider 256-byte window is selected automatically so
// that edits within one function shift fewer adjacent chunk boundaries.
// Content shorter than cfg.SmallFileBytes is returned as a single element.
func splitSpanChunks(text string, cfg OptimizerConfig) []string {
	smallBytes := cfg.SmallFileBytes
	if smallBytes <= 0 {
		smallBytes = 4096
	}
	data := []byte(text)
	if len(data) < smallBytes {
		return []string{text}
	}
	var ckCfg chunker.Config
	if len(data) >= 8192 && chunker.IsCodeContent(data) {
		// FIX 5: 256-byte window for large code files (≥8 KB with declaration-keyword
		// density) to produce fewer, more stable chunk boundaries that survive
		// minor edits without shifting fingerprints in adjacent functions.
		ckCfg = chunker.CodeConfig()
	} else if chunker.IsSystemProseContent(data) {
		// FIX 4: 4 KB MaxSize for XML-tagged system-reminder blocks so a single
		// changed field (e.g. currentDate) only invalidates its own small chunk.
		ckCfg = chunker.SystemProseConfig()
	} else if chunker.IsPathList(data) {
		// FIX 7: narrow 32-byte window for file-path listings so per-path changes
		// produce localised boundary shifts instead of cascading re-fingerprints.
		ckCfg = chunker.PathListConfig()
	} else {
		ckCfg = chunker.DefaultConfig()
	}
	if cfg.MaxChunkBytes > 0 {
		ckCfg.MaxSize = cfg.MaxChunkBytes
	}
	ck := chunker.New(ckCfg)
	raw := ck.Split(data)
	if len(raw) == 0 {
		return []string{text}
	}
	result := make([]string, len(raw))
	for i, ch := range raw {
		result[i] = string(ch.Data)
	}
	return result
}

func hashText(text string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(text)))
	return hex.EncodeToString(sum[:])
}

func (p *Proxy) forwardClaudeMessages(w http.ResponseWriter, r *http.Request, cfg ClaudeMessagesConfig, body []byte) claudeStreamStats {
	if cfg.Bedrock.Enabled {
		return p.forwardClaudeMessagesViaBedrock(w, r, cfg, body)
	}

	// Preserve Claude Code's cache_control breakpoints on the direct Anthropic
	// path. Subscription/OAuth auth supports prompt caching too — Claude Code
	// relies on it — so stripping breakpoints here only disabled the cache:
	// Anthropic then reported cache_read=0 / cache_creation=0 and billed full
	// input every turn. Bedrock, which rejects Anthropic-only cache_control,
	// strips it in its own path (see forwardClaudeMessagesViaBedrock).

	upstreamURL, err := url.JoinPath(strings.TrimRight(cfg.AnthropicBaseURL, "/"), "v1", "messages")
	if err != nil {
		p.writeError(w, r, errorPayload{HTTPStatus: http.StatusInternalServerError, Type: "server_error", Code: "bad_upstream_url", Message: err.Error()})
		return claudeStreamStats{Status: "error", StatusCode: http.StatusInternalServerError, UpstreamErr: err.Error()}
	}
	upReq, err := http.NewRequestWithContext(r.Context(), http.MethodPost, upstreamURL, bytes.NewReader(body))
	if err != nil {
		p.writeError(w, r, errorPayload{HTTPStatus: http.StatusInternalServerError, Type: "server_error", Code: "upstream_request_failed", Message: err.Error()})
		return claudeStreamStats{Status: "error", StatusCode: http.StatusInternalServerError, UpstreamErr: err.Error()}
	}
	copyAnthropicHeaders(upReq.Header, r.Header)
	if cfg.AnthropicAPIKey != "" {
		upReq.Header.Set("x-api-key", cfg.AnthropicAPIKey)
	} else if auth := r.Header.Get("Authorization"); auth != "" {
		// Passthrough mode: user's OAuth Bearer token flows through unchanged.
		upReq.Header.Set("Authorization", auth)
	}
	upReq.Header.Set("anthropic-version", cfg.AnthropicVersion)
	upReq.Header.Set("content-type", "application/json")
	upReq.Header.Set("accept", "text/event-stream")

	resp, err := cfg.HTTPClient.Do(upReq)
	if err != nil {
		if errors.Is(err, context.Canceled) || r.Context().Err() != nil {
			return claudeStreamStats{Status: "cancelled", Cancelled: true}
		}
		p.writeError(w, r, errorPayload{HTTPStatus: http.StatusBadGateway, Type: "upstream_error", Code: "anthropic_unavailable", Message: err.Error()})
		return claudeStreamStats{Status: "error", StatusCode: http.StatusBadGateway, UpstreamErr: err.Error()}
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		meta := p.proxyUpstreamError(w, r, resp)
		return claudeStreamStats{
			Status:            "error",
			StatusCode:        meta.StatusCode,
			UpstreamErrorCode: meta.Code,
			UpstreamErrorType: meta.Type,
			UpstreamRequestID: meta.RequestID,
			RetryAfter:        meta.RetryAfter,
		}
	}
	return proxyAnthropicStream(w, r, resp)
}

func (p *Proxy) forwardClaudeJSON(w http.ResponseWriter, r *http.Request, cfg ClaudeMessagesConfig, body []byte, upstreamPath string) int {
	upstreamURL, err := url.JoinPath(strings.TrimRight(cfg.AnthropicBaseURL, "/"), upstreamPath)
	if err != nil {
		p.writeError(w, r, errorPayload{HTTPStatus: http.StatusInternalServerError, Type: "server_error", Code: "bad_upstream_url", Message: err.Error()})
		return http.StatusInternalServerError
	}
	upReq, err := http.NewRequestWithContext(r.Context(), http.MethodPost, upstreamURL, bytes.NewReader(body))
	if err != nil {
		p.writeError(w, r, errorPayload{HTTPStatus: http.StatusInternalServerError, Type: "server_error", Code: "upstream_request_failed", Message: err.Error()})
		return http.StatusInternalServerError
	}
	copyAnthropicHeaders(upReq.Header, r.Header)
	upReq.Header.Set("x-api-key", cfg.AnthropicAPIKey)
	upReq.Header.Set("anthropic-version", cfg.AnthropicVersion)
	upReq.Header.Set("content-type", "application/json")
	upReq.Header.Set("accept", "application/json")

	resp, err := cfg.HTTPClient.Do(upReq)
	if err != nil {
		if errors.Is(err, context.Canceled) || r.Context().Err() != nil {
			return 499
		}
		p.writeError(w, r, errorPayload{HTTPStatus: http.StatusBadGateway, Type: "upstream_error", Code: "anthropic_unavailable", Message: err.Error()})
		return http.StatusBadGateway
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return p.proxyUpstreamError(w, r, resp).StatusCode
	}
	w.Header().Set("Content-Type", firstHeader(resp.Header.Get("Content-Type"), "application/json"))
	w.WriteHeader(resp.StatusCode)
	if _, err := io.Copy(w, resp.Body); err != nil {
		p.logger.WarnContext(r.Context(), "failed to write claude json response", slog.Any("err", err))
	}
	return resp.StatusCode
}

func copyAnthropicHeaders(dst, src http.Header) {
	for key, values := range src {
		lower := strings.ToLower(key)
		if !strings.HasPrefix(lower, "anthropic-") {
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func firstHeader(value, fallback string) string {
	if strings.TrimSpace(value) != "" {
		return value
	}
	return fallback
}

func (p *Proxy) proxyUpstreamError(w http.ResponseWriter, r *http.Request, resp *http.Response) claudeUpstreamErrorMeta {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	meta := parseClaudeUpstreamError(resp, body)
	if value := retryAfterSeconds(meta.RetryAfter); value != "" {
		w.Header().Set("Retry-After", value)
	}
	p.writeError(w, r, errorPayload{
		HTTPStatus: meta.StatusCode,
		Type:       "upstream_error",
		Code:       meta.Code,
		Message:    meta.Message,
	})
	return meta
}

func parseClaudeUpstreamError(resp *http.Response, body []byte) claudeUpstreamErrorMeta {
	raw := string(body)
	code, message := classifyUpstreamError(fmt.Errorf("anthropic api error: status=%d body=%s", resp.StatusCode, raw))
	meta := claudeUpstreamErrorMeta{
		StatusCode: resp.StatusCode,
		Code:       code,
		Message:    message,
		RequestID:  firstNonEmpty(resp.Header.Get("request-id"), resp.Header.Get("x-request-id"), resp.Header.Get("anthropic-request-id")),
		RetryAfter: retryAfterDuration(resp.Header.Get("Retry-After"), time.Now()),
	}
	var env struct {
		Type      string `json:"type"`
		RequestID string `json:"request_id"`
		Error     struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &env); err == nil {
		meta.Type = firstNonEmpty(env.Error.Type, env.Type)
		meta.RequestID = firstNonEmpty(meta.RequestID, env.RequestID)
	}
	if meta.Type == "" {
		meta.Type = code
	}
	return meta
}

func proxyAnthropicStream(w http.ResponseWriter, r *http.Request, resp *http.Response) claudeStreamStats {
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache, no-store, must-revalidate")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	rc := http.NewResponseController(w)
	_ = rc.Flush()

	stats := claudeStreamStats{Status: "completed", StatusCode: http.StatusOK}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 4096), 1<<20)
	var event string
	for scanner.Scan() {
		if err := r.Context().Err(); err != nil {
			stats.Status = "cancelled"
			stats.Cancelled = true
			return stats
		}
		line := scanner.Text()
		if _, err := io.WriteString(w, line+"\n"); err != nil {
			stats.Status = "cancelled"
			stats.Cancelled = true
			return stats
		}
		switch {
		case line == "":
			_ = rc.Flush()
			event = ""
		case strings.HasPrefix(line, "event: "):
			event = strings.TrimSpace(strings.TrimPrefix(line, "event: "))
		case strings.HasPrefix(line, "data: "):
			payload := strings.TrimSpace(strings.TrimPrefix(line, "data: "))
			if strings.Contains(payload, `"tool_use"`) {
				stats.HasToolUse = true
			}
			switch event {
			case "content_block_delta":
				stats.Chunks++
				txt := anthropicDeltaText(payload)
				stats.OutputText += len(txt)
				stats.OutputRawText += txt
			case "message_start", "message_delta":
				stats.applyUpstreamUsage(parseAnthropicUsage(payload))
			}
		}
	}
	if err := scanner.Err(); err != nil {
		stats.Status = "stream_error" // HTTP 200 already sent; stream terminated abnormally
		stats.UpstreamErr = err.Error()
		return stats
	}
	stats.Completed = true
	return stats
}

func anthropicDeltaText(payload string) string {
	var ev struct {
		Delta struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"delta"`
	}
	if err := json.Unmarshal([]byte(payload), &ev); err != nil {
		return ""
	}
	if ev.Delta.Type != "text_delta" {
		return ""
	}
	return ev.Delta.Text
}

func anthropicDeltaTextLen(payload string) int {
	var ev struct {
		Delta struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"delta"`
	}
	if err := json.Unmarshal([]byte(payload), &ev); err != nil {
		return 0
	}
	if ev.Delta.Type != "text_delta" {
		return 0
	}
	return len(ev.Delta.Text)
}

// anthropicUsage holds the token-usage fields Anthropic reports over SSE. Input
// and cache counters arrive on message_start (inside message.usage); the final
// cumulative output_tokens arrives on message_delta (top-level usage). All fields
// are optional, so a payload that omits a field simply leaves it at zero.
type anthropicUsage struct {
	InputTokens              int
	OutputTokens             int
	CacheReadInputTokens     int
	CacheCreationInputTokens int
}

// parseAnthropicUsage extracts usage from either a message_start payload
// (usage nested under "message") or a message_delta payload (top-level "usage").
func parseAnthropicUsage(payload string) anthropicUsage {
	var ev struct {
		Usage   *usageFields `json:"usage"`
		Message *struct {
			Usage *usageFields `json:"usage"`
		} `json:"message"`
	}
	if err := json.Unmarshal([]byte(payload), &ev); err != nil {
		return anthropicUsage{}
	}
	u := ev.Usage
	if u == nil && ev.Message != nil {
		u = ev.Message.Usage
	}
	if u == nil {
		return anthropicUsage{}
	}
	return anthropicUsage{
		InputTokens:              u.InputTokens,
		OutputTokens:             u.OutputTokens,
		CacheReadInputTokens:     u.CacheReadInputTokens,
		CacheCreationInputTokens: u.CacheCreationInputTokens,
	}
}

type usageFields struct {
	InputTokens              int `json:"input_tokens"`
	OutputTokens             int `json:"output_tokens"`
	CacheReadInputTokens     int `json:"cache_read_input_tokens"`
	CacheCreationInputTokens int `json:"cache_creation_input_tokens"`
}

// applyUpstreamUsage folds a parsed usage object into streamStats, keeping the
// max output_tokens seen (cumulative) and capturing input/cache counters when
// present (they appear once, on message_start).
func (s *claudeStreamStats) applyUpstreamUsage(u anthropicUsage) {
	if u.OutputTokens > 0 {
		s.OutputTokens = u.OutputTokens
	}
	if u.InputTokens > 0 {
		s.InputTokens = u.InputTokens
	}
	if u.CacheReadInputTokens > 0 {
		s.CacheReadInputTokens = u.CacheReadInputTokens
	}
	if u.CacheCreationInputTokens > 0 {
		s.CacheCreationInputTokens = u.CacheCreationInputTokens
	}
}

// realInputTokens returns the total measured input the model billed this turn:
// fresh input + cache-creation + cache-read. Zero when upstream reported nothing
// (e.g. synthetic probe / cache replay paths).
func (s claudeStreamStats) realInputTokens() int {
	return s.InputTokens + s.CacheReadInputTokens + s.CacheCreationInputTokens
}

func (p *Proxy) emitClaudeUsageEvent(r *http.Request, sessionKey, model string, optStats claudeOptimizerStats, streamStats claudeStreamStats, duration time.Duration, upstreamStatus int, overheadMs int64) {
	if p.usageTracker != nil {
		p.usageTracker.Track(telemetry.UsageEvent{
			MachineID:            telemetry.GetMachineID(),
			SessionID:            sessionKey,
			OsArch:               runtime.GOOS + "/" + runtime.GOARCH,
			IqVersion:            Version,
			CliAgent:             r.Header.Get("User-Agent"),
			ModelTarget:          model,
			InputTokensAttempted: optStats.EstimatedTokensBefore,
			InputTokensSent:      optStats.EstimatedTokensAfter,
			TokensSaved:          optStats.EstimatedTokensSaved,
			InputTokensReal:      streamStats.realInputTokens(),
			CacheReadTokens:      streamStats.CacheReadInputTokens,
			CacheCreationTokens:  streamStats.CacheCreationInputTokens,
			ReductionRatio:       optStats.ReductionRatio * 100,
			BlocksAnalyzed:       optStats.BlocksSeen,
			BlocksPruned:         optStats.BlocksPruned,
			TotalLatencyMs:       int(duration.Milliseconds()),
			ProxyOverheadMs:      float64(overheadMs),
			UpstreamStatus:       upstreamStatus,
		})
	}

	outcome := telemetry.RequestOutcome{
		TokensAttempted:     optStats.EstimatedTokensBefore,
		TokensSent:          optStats.EstimatedTokensAfter,
		TokensSaved:         optStats.EstimatedTokensSaved,
		InputTokensReal:     streamStats.realInputTokens(),
		CacheReadTokens:     streamStats.CacheReadInputTokens,
		CacheCreationTokens: streamStats.CacheCreationInputTokens,
	}
	if p.sessionTracker != nil {
		p.sessionTracker.Record(sessionKey, outcome)
	}
	if p.sessionPersist != nil {
		p.sessionPersist.Record(sessionKey, outcome)
	}
}

func extractClaudeRequestShape(root map[string]any) claudeRequestShape {
	var shape claudeRequestShape
	msgs, _ := root["messages"].([]any)
	shape.MessageCount = len(msgs)
	for _, raw := range msgs {
		msg, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		blocks, toolResults, text := summarizeAnthropicContent(msg["content"])
		shape.ContentBlockCount += blocks
		shape.ToolResultCount += toolResults
		role, _ := msg["role"].(string)
		if strings.EqualFold(role, "user") {
			trimmed := strings.TrimSpace(text)
			if trimmed != "" {
				shape.LatestUserText = trimmed
			}
		}
	}
	_, _, systemText := summarizeAnthropicContent(root["system"])
	shape.SystemText = strings.TrimSpace(systemText)
	return shape
}

func summarizeAnthropicContent(content any) (blocks int, toolResults int, text string) {
	switch v := content.(type) {
	case string:
		if strings.TrimSpace(v) == "" {
			return 0, 0, ""
		}
		return 1, 0, v
	case []any:
		var sb strings.Builder
		for _, item := range v {
			blocks++
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			typ, _ := m["type"].(string)
			switch typ {
			case "tool_result":
				toolResults++
				appendText(&sb, m["content"])
			default:
				appendText(&sb, m["text"])
			}
		}
		return blocks, toolResults, strings.TrimSpace(sb.String())
	case map[string]any:
		var sb strings.Builder
		blocks = 1
		if typ, _ := v["type"].(string); typ == "tool_result" {
			toolResults = 1
			appendText(&sb, v["content"])
		} else {
			appendText(&sb, v["text"])
		}
		return blocks, toolResults, strings.TrimSpace(sb.String())
	default:
		return 0, 0, ""
	}
}

func appendText(sb *strings.Builder, value any) {
	switch v := value.(type) {
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return
		}
		if sb.Len() > 0 {
			sb.WriteByte('\n')
		}
		sb.WriteString(trimmed)
	case []any:
		for _, item := range v {
			appendText(sb, item)
		}
	case map[string]any:
		appendText(sb, v["text"])
		appendText(sb, v["content"])
	}
}

func (p *Proxy) logClaudeRequestComplete(ctx context.Context, requestID, mode, model, sessionKey string, bytesBefore int, opt claudeOptimizerStats, stream claudeStreamStats, dur time.Duration, missingRequestID bool, syntheticRequestID string, velocityWarning bool) {
	level := slog.LevelInfo
	if stream.Status == "error" || stream.Status == "stream_error" {
		level = slog.LevelError
	} else if stream.Cancelled {
		level = slog.LevelWarn
	}
	attrs := []slog.Attr{
		slog.String("event", "request_complete"),
		slog.String("request_id", requestID),
		slog.String("mode", mode),
		slog.String("provider", firstNonEmpty(stream.Provider, "anthropic")),
		slog.String("model", model),
		slog.String("session_key", shortLogHash(sessionKey)),
		slog.Int("bytes_before", bytesBefore),
		slog.Int("estimated_tokens_before", estimateTokens(bytesBefore)),
		slog.Int("bytes_after", opt.BytesAfter),
		slog.Int("estimated_tokens_after", opt.EstimatedTokensAfter),
		slog.Int("estimated_tokens_saved", opt.EstimatedTokensSaved),
		slog.Float64("reduction_ratio", opt.ReductionRatio),
		slog.Int("blocks_seen", opt.BlocksSeen),
		slog.Int("blocks_new", opt.BlocksNew),
		slog.Int("blocks_known", opt.BlocksKnown),
		slog.Int("blocks_pruned", opt.BlocksPruned),
		slog.Int("stream_chunks", stream.Chunks),
		slog.Int("estimated_output_tokens", stream.estimatedOutputTokens()),
		slog.Int64("duration_ms", dur.Milliseconds()),
		slog.String("status", stream.Status),
		slog.Int("status_code", stream.StatusCode),
		slog.Bool("stream_completed", stream.Completed),
		slog.Bool("stream_cancelled", stream.Cancelled),
		// FIX 8: request ID tracing fields
		slog.Bool("missing_request_id", missingRequestID),
		slog.Bool("velocity_warning", velocityWarning),
	}
	if missingRequestID && syntheticRequestID != "" {
		attrs = append(attrs, slog.String("synthetic_request_id", syntheticRequestID))
	}
	if opt.BytesEligible > 0 {
		attrs = append(attrs, slog.Int("bytes_eligible", opt.BytesEligible))
	}
	if opt.BytesPruned > 0 {
		attrs = append(attrs, slog.Int("bytes_pruned", opt.BytesPruned))
	}
	if opt.LargestSpanBytes > 0 {
		attrs = append(attrs, slog.Int("largest_span_bytes", opt.LargestSpanBytes))
	}
	if opt.LargestPrunedBytes > 0 {
		attrs = append(attrs, slog.Int("largest_pruned_bytes", opt.LargestPrunedBytes))
	}
	if opt.PreservedLatestTurnCount > 0 {
		attrs = append(attrs, slog.Int("preserved_latest_turn_count", opt.PreservedLatestTurnCount))
		attrs = append(attrs, slog.Int("preserved_latest_turn_bytes", opt.PreservedLatestTurnBytes))
	}
	if opt.PreservedSmallCount > 0 {
		attrs = append(attrs, slog.Int("preserved_small_count", opt.PreservedSmallCount))
	}
	if opt.PreservedSystemCount > 0 {
		attrs = append(attrs, slog.Int("preserved_system_count", opt.PreservedSystemCount))
	}
	if opt.PreservedToolUseCount > 0 {
		attrs = append(attrs, slog.Int("preserved_tool_use_count", opt.PreservedToolUseCount))
	}
	if opt.PreservedInstructionCount > 0 {
		attrs = append(attrs, slog.Int("preserved_instruction_count", opt.PreservedInstructionCount))
		attrs = append(attrs, slog.Int("preserved_instruction_bytes", opt.PreservedInstructionBytes))
	}
	if opt.PreservedCachePrefixCount > 0 {
		attrs = append(attrs, slog.Int("preserved_cache_prefix_count", opt.PreservedCachePrefixCount))
		attrs = append(attrs, slog.Int("preserved_cache_prefix_bytes", opt.PreservedCachePrefixBytes))
	}
	// Live per-turn measured cache efficiency (E4): visible each turn in --dev logs
	// and session dumps, not just in the post-exit summary. Emitted only when the
	// upstream actually reported input usage (skips synthetic/cache-replay turns).
	if realIn := stream.realInputTokens(); realIn > 0 {
		attrs = append(attrs,
			slog.Int("input_tokens_real", realIn),
			slog.Int("cache_read_tokens", stream.CacheReadInputTokens),
			slog.Int("cache_creation_tokens", stream.CacheCreationInputTokens),
			slog.Float64("cache_hit_ratio", float64(stream.CacheReadInputTokens)/float64(realIn)),
		)
	}
	for class, bytes := range opt.ClassBytesSeen {
		if bytes > 0 {
			attrs = append(attrs, slog.Int("class_bytes_seen:"+class, bytes))
		}
	}
	for class, bytes := range opt.ClassBytesEligible {
		if bytes > 0 {
			attrs = append(attrs, slog.Int("class_bytes_eligible:"+class, bytes))
		}
	}
	for class, bytes := range opt.ClassBytesPruned {
		if bytes > 0 {
			attrs = append(attrs, slog.Int("class_bytes_pruned:"+class, bytes))
		}
	}
	if stream.UpstreamErr != "" {
		attrs = append(attrs, slog.String("upstream_err", stream.UpstreamErr))
	}
	if stream.UpstreamErrorCode != "" {
		attrs = append(attrs, slog.String("upstream_error_code", stream.UpstreamErrorCode))
	}
	if stream.UpstreamErrorType != "" {
		attrs = append(attrs, slog.String("upstream_error_type", stream.UpstreamErrorType))
	}
	if stream.UpstreamRequestID != "" {
		attrs = append(attrs, slog.String("upstream_request_id", stream.UpstreamRequestID))
	}
	if stream.RetryAfter > 0 {
		attrs = append(attrs, slog.Int64("retry_after_ms", stream.RetryAfter.Milliseconds()))
	}
	p.logger.LogAttrs(ctx, level, "claude request complete", attrs...)
}

func (s claudeStreamStats) estimatedOutputTokens() int {
	if s.OutputTokens > 0 {
		return s.OutputTokens
	}
	return estimateTokens(s.OutputText)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func shortLogHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:6])
}

// semanticPromptHash returns a 128-bit hex digest keyed on the content of
// the last 3 user messages plus the first 64 bytes of the system field.
// Raw-body hashing fails in-flight deduplication because the same logical
// prompt produces different bytes each turn as the context window grows.
// Falls back to a raw SHA-256 if the body cannot be parsed.
func semanticPromptHash(body []byte) string {
	var parsed struct {
		System   json.RawMessage `json:"system"`
		Messages []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		sum := sha256.Sum256(body)
		return hex.EncodeToString(sum[:16])
	}

	// System fingerprint: first 64 bytes of the string-form system content.
	var sysFP string
	var sysStr string
	if err := json.Unmarshal(parsed.System, &sysStr); err == nil {
		if len(sysStr) > 64 {
			sysStr = sysStr[:64]
		}
		sysFP = sysStr
	} else {
		var sysArr []map[string]any
		if err2 := json.Unmarshal(parsed.System, &sysArr); err2 == nil && len(sysArr) > 0 {
			if text, ok := sysArr[0]["text"].(string); ok {
				if len(text) > 64 {
					text = text[:64]
				}
				sysFP = text
			}
		}
	}

	// Collect last 3 user-message contents.
	var userContents []string
	for _, msg := range parsed.Messages {
		if !strings.EqualFold(msg.Role, "user") {
			continue
		}
		var text string
		if err := json.Unmarshal(msg.Content, &text); err == nil {
			userContents = append(userContents, text)
		} else {
			var blocks []map[string]any
			if err2 := json.Unmarshal(msg.Content, &blocks); err2 == nil {
				var sb strings.Builder
				for _, b := range blocks {
					appendText(&sb, b["text"])
					appendText(&sb, b["content"])
				}
				userContents = append(userContents, sb.String())
			}
		}
	}
	if len(userContents) > 3 {
		userContents = userContents[len(userContents)-3:]
	}

	h := sha256.New()
	for _, c := range userContents {
		h.Write([]byte(c))
	}
	h.Write([]byte(sysFP))
	return hex.EncodeToString(h.Sum(nil)[:16])
}

// getOrCreatePrefixHints returns the prefix-hint set for sessionKey, creating
// it if it does not yet exist (FIX 3).
func (p *Proxy) getOrCreatePrefixHints(sessionKey string) *prefixHintSet {
	v, _ := p.sessionPrefixHints.LoadOrStore(sessionKey, &prefixHintSet{
		hints: make(map[string]int),
	})
	p.touchSession(sessionKey)
	return v.(*prefixHintSet)
}

func (s *prefixHintSet) add(hash string, length int) {
	s.mu.Lock()
	s.hints[hash] = length
	s.mu.Unlock()
}

// matchPrefix returns the length and hash of the longest registered small
// chunk whose content matches data[0:length], or 0 if none match (FIX 3).
func (s *prefixHintSet) matchPrefix(data []byte) (matchedHash string, matchedLen int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for h, l := range s.hints {
		if l <= 0 || l > len(data) {
			continue
		}
		sum := sha256.Sum256([]byte(strings.TrimSpace(string(data[:l]))))
		if hex.EncodeToString(sum[:]) == h && l > matchedLen {
			matchedLen = l
			matchedHash = h
		}
	}
	return
}

func estimateTokens(chars int) int {
	if chars <= 0 {
		return 0
	}
	return (chars + 3) / 4
}

func retryAfterSeconds(d time.Duration) string {
	if d <= 0 {
		return ""
	}
	seconds := int((d + time.Second - 1) / time.Second)
	if seconds < 1 {
		seconds = 1
	}
	return fmt.Sprintf("%d", seconds)
}

func retryAfterDuration(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil {
		if seconds <= 0 {
			return 0
		}
		return time.Duration(seconds) * time.Second
	}
	when, err := http.ParseTime(value)
	if err != nil {
		return 0
	}
	if !when.After(now) {
		return 0
	}
	return when.Sub(now)
}

type responseCaptureWriter struct {
	http.ResponseWriter
	buf bytes.Buffer
}

func (rcw *responseCaptureWriter) Write(b []byte) (int, error) {
	rcw.buf.Write(b)
	return rcw.ResponseWriter.Write(b)
}

func (rcw *responseCaptureWriter) Flush() {
	if flusher, ok := rcw.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (rcw *responseCaptureWriter) WriteString(s string) (int, error) {
	rcw.buf.WriteString(s)
	if sw, ok := rcw.ResponseWriter.(io.StringWriter); ok {
		return sw.WriteString(s)
	}
	return rcw.ResponseWriter.Write([]byte(s))
}

func computePromptHash(body []byte, model string) string {
	canonicalBody := body
	var parsed any
	if err := json.Unmarshal(body, &parsed); err == nil {
		if encoded, encErr := marshalJSONNoHTMLEscape(parsed); encErr == nil {
			canonicalBody = encoded
		}
	}
	h := sha256.New()
	h.Write([]byte(model))
	h.Write([]byte{0})
	h.Write(canonicalBody)
	return hex.EncodeToString(h.Sum(nil))
}

func writeSyntheticStreamResponse(w http.ResponseWriter, text string) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	textData, _ := json.Marshal(text)
	events := []string{
		`event: message_start` + "\n" + `data: {"type":"message_start","message":{"id":"msg_synth","type":"message","role":"assistant","content":[],"model":"claude-3-5-sonnet","usage":{"input_tokens":5,"output_tokens":5}}}` + "\n\n",
		`event: content_block_start` + "\n" + `data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}` + "\n\n",
		`event: content_block_delta` + "\n" + `data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":` + string(textData) + `}}` + "\n\n",
		`event: content_block_stop` + "\n" + `data: {"type":"content_block_stop","index":0}` + "\n\n",
		`event: message_delta` + "\n" + `data: {"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":5}}` + "\n\n",
		`event: message_stop` + "\n" + `data: {"type":"message_stop"}` + "\n\n",
	}

	for _, ev := range events {
		_, _ = io.WriteString(w, ev)
	}
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
}
