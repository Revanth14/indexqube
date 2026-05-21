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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/memory"
	"github.com/Revanth14/indexqube/gateway/internal/middleware"
	"github.com/Revanth14/indexqube/gateway/internal/telemetry"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
)

const (
	claudeDefaultMode = "observe"
)

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
	BytesBefore           int     `json:"bytes_before"`
	BytesAfter            int     `json:"bytes_after"`
	BytesEligible         int     `json:"bytes_eligible"`
	BytesPruned           int     `json:"bytes_pruned"`
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
	PreservedLatestTurnBytes  int `json:"preserved_latest_turn_bytes"`
	PreservedLatestTurnCount  int `json:"preserved_latest_turn_count"`
	PreservedSmallBytes       int `json:"preserved_small_bytes"`
	PreservedSmallCount       int `json:"preserved_small_count"`
	PreservedSystemBytes      int `json:"preserved_system_bytes"`
	PreservedSystemCount      int `json:"preserved_system_count"`
	PreservedToolUseBytes     int `json:"preserved_tool_use_bytes"`
	PreservedToolUseCount     int `json:"preserved_tool_use_count"`
	PreservedInstructionBytes    int `json:"preserved_instruction_bytes"`
	PreservedInstructionCount    int `json:"preserved_instruction_count"`
	PreservedLastOccurrenceBytes int `json:"preserved_last_occurrence_bytes"`
	PreservedLastOccurrenceCount int `json:"preserved_last_occurrence_count"`

	// Size tracking.
	LargestSpanBytes   int `json:"largest_span_bytes"`
	LargestPrunedBytes int `json:"largest_pruned_bytes"`
}

type claudeStreamStats struct {
	Chunks            int
	OutputText        int
	OutputTokens      int
	Status            string
	StatusCode        int
	Cancelled         bool
	Completed         bool
	Provider          string // "anthropic" or "bedrock"
	UpstreamErr       string
	UpstreamErrorCode string
	UpstreamErrorType string
	UpstreamRequestID string
	RetryAfter        time.Duration
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

	// overheadStart marks the beginning of pure IndexQube processing time.
	// It stops just before dispatching to upstream so ProxyOverheadMs
	// reflects only the optimizer + guard work, not the model round-trip.
	overheadStart := time.Now()

	sessionKey := claudeSessionKey(r, cfg.DevToken)
	var earlyMeta struct {
		Model string `json:"model"`
	}
	_ = json.Unmarshal(body, &earlyMeta)

	forwardBody, meta, optStats, shape, err := p.prepareClaudeBody(r.Context(), cfg, sessionKey, body)
	if err != nil {
		p.writeError(w, r, errorPayload{HTTPStatus: http.StatusBadRequest, Type: "invalid_request_error", Code: "invalid_anthropic_request", Message: err.Error()})
		return
	}
	if os.Getenv("IQ_DUMP_PAYLOADS") == "1" {
		dumpClaudePayloads(requestID, body, forwardBody)
	}
	_ = shape

	overheadMs := time.Since(overheadStart).Milliseconds()
	streamStats := p.forwardClaudeMessages(w, r, cfg, forwardBody)
	duration := time.Since(started)
	if cfg.SessionStore != nil {
		cfg.SessionStore.RecordUsage(sessionKey, memory.UsageTotals{
			Requests:    1,
			TokensIn:    estimateTokens(len(body)),
			TokensOut:   streamStats.estimatedOutputTokens(),
			TokensSaved: optStats.EstimatedTokensSaved,
			BytesIn:     len(body),
			BytesSaved:  optStats.BytesBefore - optStats.BytesAfter,
		})
	}
	p.logClaudeRequestComplete(r.Context(), requestID, cfg.Mode, meta.Model, sessionKey, len(body), optStats, streamStats, duration)
	p.emitClaudeUsageEvent(r, sessionKey, meta.Model, optStats, duration, streamStats.StatusCode, overheadMs)
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
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	token := strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
	return token != ""
}

func dumpClaudePayloads(requestID string, before, after []byte) {
	if sessionFile := os.Getenv("IQ_DUMP_SESSION_FILE"); sessionFile != "" {
		if err := appendSessionDump(sessionFile, requestID, before, after); err != nil {
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
	if err := os.WriteFile(beforePath, prettyJSON(before), 0o600); err != nil {
		fmt.Fprintf(os.Stderr, "[iq] failed to dump payload pair: %v\n", err)
		return
	}
	if err := os.WriteFile(afterPath, prettyJSON(after), 0o600); err != nil {
		fmt.Fprintf(os.Stderr, "[iq] failed to dump payload pair: %v\n", err)
		return
	}
	appendDumpLog(dumpDir, beforePath, afterPath)
}

type payloadDumpRecord struct {
	Timestamp   string          `json:"ts"`
	RequestID   string          `json:"request_id"`
	BeforeBytes int             `json:"before_bytes"`
	AfterBytes  int             `json:"after_bytes"`
	SavedBytes  int             `json:"saved_bytes"`
	Before      json.RawMessage `json:"before"`
	After       json.RawMessage `json:"after"`
}

func appendSessionDump(sessionFile, requestID string, before, after []byte) error {
	if err := os.MkdirAll(filepath.Dir(sessionFile), 0o700); err != nil {
		return err
	}
	record := payloadDumpRecord{
		Timestamp:   time.Now().Format(time.RFC3339),
		RequestID:   requestID,
		BeforeBytes: len(before),
		AfterBytes:  len(after),
		SavedBytes:  len(before) - len(after),
		Before:      json.RawMessage(before),
		After:       json.RawMessage(after),
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

	var prunableSpans []TextSpan

	for _, span := range spans {
		if span.Bytes < minSpanBytes {
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

		known := cfg.SessionStore.Seen(sessionKey, span.Hash)
		cfg.SessionStore.SaveBlock(sessionKey, memory.Block{
			Hash:   span.Hash,
			Kind:   span.Class,
			Bytes:  span.Bytes,
			Tokens: span.Tokens,
		})

		if !known {
			stats.BlocksNew++
			continue
		}
		stats.BlocksKnown++

		// Latest-turn spans must never be pruned regardless of whether the
		// content was seen before. The model needs current-turn context intact.
		if span.IsLatestTurn {
			stats.PreservedLatestTurnBytes += span.Bytes
			stats.PreservedLatestTurnCount++
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

		if isProtectedInstructionSpan(span) {
			stats.PreservedInstructionBytes += span.Bytes
			stats.PreservedInstructionCount++
			continue
		}

		stats.ClassBytesEligible[span.Class] += span.Bytes
		stats.BytesEligible += span.Bytes
		prunableSpans = append(prunableSpans, span)
	}

	if len(prunableSpans) == 0 {
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

func isProtectedInstructionSpan(span TextSpan) bool {
	return containsProtectedInstructionPath(span.SourcePath) || containsProtectedInstructionPath(span.Text)
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

		switch ev.Type {
		case "content_block_delta":
			stats.Chunks++
			stats.OutputText += anthropicDeltaTextLen(string(payload))
		case "message_delta":
			if tokens := anthropicUsageOutputTokens(string(payload)); tokens > 0 {
				stats.OutputTokens = tokens
			}
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

func hashText(text string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(text)))
	return hex.EncodeToString(sum[:])
}

func (p *Proxy) forwardClaudeMessages(w http.ResponseWriter, r *http.Request, cfg ClaudeMessagesConfig, body []byte) claudeStreamStats {
	if cfg.Bedrock.Enabled {
		return p.forwardClaudeMessagesViaBedrock(w, r, cfg, body)
	}
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
			switch event {
			case "content_block_delta":
				stats.Chunks++
				stats.OutputText += anthropicDeltaTextLen(payload)
			case "message_delta":
				if tokens := anthropicUsageOutputTokens(payload); tokens > 0 {
					stats.OutputTokens = tokens
				}
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

func anthropicUsageOutputTokens(payload string) int {
	var ev struct {
		Usage struct {
			OutputTokens int `json:"output_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal([]byte(payload), &ev); err != nil {
		return 0
	}
	return ev.Usage.OutputTokens
}

func (p *Proxy) emitClaudeUsageEvent(r *http.Request, sessionKey, model string, optStats claudeOptimizerStats, duration time.Duration, upstreamStatus int, overheadMs int64) {
	if p.usageTracker != nil {
		p.usageTracker.Track(telemetry.UsageEvent{
			MachineID:            telemetry.GetMachineID(),
			OsArch:               runtime.GOOS + "/" + runtime.GOARCH,
			IqVersion:            Version,
			CliAgent:             r.Header.Get("User-Agent"),
			ModelTarget:          model,
			InputTokensAttempted: optStats.EstimatedTokensBefore,
			InputTokensSent:      optStats.EstimatedTokensAfter,
			TokensSaved:          optStats.EstimatedTokensSaved,
			ReductionRatio:       optStats.ReductionRatio * 100,
			BlocksAnalyzed:       optStats.BlocksSeen,
			BlocksPruned:         optStats.BlocksPruned,
			TotalLatencyMs:       int(duration.Milliseconds()),
			ProxyOverheadMs:      float64(overheadMs),
			UpstreamStatus:       upstreamStatus,
		})
	}

	outcome := telemetry.RequestOutcome{
		TokensAttempted: optStats.EstimatedTokensBefore,
		TokensSent:      optStats.EstimatedTokensAfter,
		TokensSaved:     optStats.EstimatedTokensSaved,
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

func (p *Proxy) logClaudeRequestComplete(ctx context.Context, requestID, mode, model, sessionKey string, bytesBefore int, opt claudeOptimizerStats, stream claudeStreamStats, dur time.Duration) {
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
