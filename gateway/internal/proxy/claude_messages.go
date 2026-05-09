package proxy

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/memory"
	"github.com/Revanth14/indexqube/gateway/internal/middleware"
)

const (
	claudeDefaultMode       = "observe"
	claudeMinBlockBytes     = 768
	claudeTargetChunkBytes  = 2048
	claudeProtectLastN      = 4
	claudeMinMessageBytes   = 200
	claudeReplacementFormat = "[iq:repeated ref:%s]"
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

type claudeOptimizerStats struct {
	BlocksSeen            int            `json:"blocks_seen"`
	BlocksNew             int            `json:"blocks_new"`
	BlocksKnown           int            `json:"blocks_known"`
	BlocksPruned          int            `json:"blocks_pruned"`
	MessagesPruned        int            `json:"messages_pruned"`
	BlockKinds            map[string]int `json:"block_kinds,omitempty"`
	BytesBefore           int            `json:"bytes_before"`
	BytesAfter            int            `json:"bytes_after"`
	EstimatedTokensBefore int            `json:"estimated_tokens_before"`
	EstimatedTokensAfter  int            `json:"estimated_tokens_after"`
	EstimatedTokensSaved  int            `json:"estimated_tokens_saved"`
	ReductionRatio        float64        `json:"reduction_ratio"`
}

type claudeStreamStats struct {
	Chunks            int
	OutputText        int
	OutputTokens      int
	Status            string
	StatusCode        int
	Cancelled         bool
	Completed         bool
	UpstreamErr       string
	UpstreamErrorCode string
	UpstreamErrorType string
	UpstreamRequestID string
	RetryAfter        time.Duration
	CircuitOpen       bool
	CircuitCooldown   time.Duration
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

	sessionKey := claudeSessionKey(r, cfg.DevToken)
	var earlyMeta struct {
		Model string `json:"model"`
	}
	_ = json.Unmarshal(body, &earlyMeta)
	if cooldown, ok := p.claudeCooldowns.Get("anthropic", earlyMeta.Model, time.Now()); ok {
		remaining := time.Until(cooldown.Until)
		if value := retryAfterSeconds(remaining); value != "" {
			w.Header().Set("Retry-After", value)
		}
		p.writeError(w, r, errorPayload{
			HTTPStatus: http.StatusTooManyRequests,
			Type:       "upstream_error",
			Code:       "provider_rate_limited",
			Message:    "Provider is cooling down after a recent rate limit. Retry shortly or switch model.",
		})
		streamStats := claudeStreamStats{
			Status:            "error",
			StatusCode:        http.StatusTooManyRequests,
			UpstreamErrorCode: firstNonEmpty(cooldown.UpstreamCode, "provider_rate_limited"),
			UpstreamErrorType: cooldown.UpstreamType,
			UpstreamRequestID: cooldown.UpstreamRequestID,
			RetryAfter:        remaining,
			CircuitOpen:       true,
			CircuitCooldown:   remaining,
		}
		duration := time.Since(started)
		if cfg.SessionStore != nil {
			cfg.SessionStore.RecordUsage(sessionKey, memory.UsageTotals{
				Requests: 1,
				TokensIn: estimateTokens(len(body)),
				BytesIn:  len(body),
			})
		}
		p.logClaudeRequestComplete(r.Context(), requestID, cfg.Mode, earlyMeta.Model, sessionKey, len(body), claudeOptimizerStats{
			BytesBefore:           len(body),
			BytesAfter:            len(body),
			EstimatedTokensBefore: estimateTokens(len(body)),
			EstimatedTokensAfter:  estimateTokens(len(body)),
		}, streamStats, duration)
		return
	}

	forwardBody, meta, optStats, err := p.prepareClaudeBody(r.Context(), cfg, sessionKey, body)
	if err != nil {
		p.writeError(w, r, errorPayload{HTTPStatus: http.StatusBadRequest, Type: "invalid_request_error", Code: "invalid_anthropic_request", Message: err.Error()})
		return
	}

	streamStats := p.forwardClaudeMessages(w, r, cfg, forwardBody)
	if streamStats.StatusCode == http.StatusTooManyRequests {
		entry := p.claudeCooldowns.Open("anthropic", meta.Model, http.StatusTooManyRequests, streamStats.upstreamMeta(), cfg.RateLimitCooldown, time.Now())
		streamStats.CircuitCooldown = time.Until(entry.Until)
	}
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
	if cfg.RateLimitCooldown <= 0 {
		cfg.RateLimitCooldown = 30 * time.Second
	}
	return cfg
}

func (c ClaudeMessagesConfig) validate() error {
	if c.DevToken == "" {
		return fmt.Errorf("INDEXQUBE_DEV_TOKEN is required for /v1/messages")
	}
	if c.AnthropicAPIKey == "" {
		return fmt.Errorf("ANTHROPIC_API_KEY is required for /v1/messages")
	}
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
	if token == "" || want == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(want)) == 1
}

func claudeSessionKey(r *http.Request, fallback string) string {
	if sk := strings.TrimSpace(r.Header.Get(headerSessionKey)); sk != "" {
		return sk
	}
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if auth != "" {
		sum := sha256.Sum256([]byte(auth))
		return hex.EncodeToString(sum[:8])
	}
	sum := sha256.Sum256([]byte(fallback))
	return hex.EncodeToString(sum[:8])
}

func (p *Proxy) prepareClaudeBody(ctx context.Context, cfg ClaudeMessagesConfig, sessionKey string, body []byte) ([]byte, anthropicMessagesRequest, claudeOptimizerStats, error) {
	if err := ctx.Err(); err != nil {
		return nil, anthropicMessagesRequest{}, claudeOptimizerStats{}, err
	}
	// Single parse — map[string]any serves block extraction and rewriting.
	// Typed fields needed downstream (Model, Stream) are pulled from the map.
	var root map[string]any
	if err := json.Unmarshal(body, &root); err != nil {
		return nil, anthropicMessagesRequest{}, claudeOptimizerStats{}, fmt.Errorf("parse anthropic messages body: %w", err)
	}
	req := anthropicMessagesRequest{}
	req.Model, _ = root["model"].(string)
	req.Stream, _ = root["stream"].(bool)
	stats := claudeOptimizerStats{
		BytesBefore:           len(body),
		BytesAfter:            len(body),
		EstimatedTokensBefore: estimateTokens(len(body)),
		EstimatedTokensAfter:  estimateTokens(len(body)),
	}
	if cfg.SessionStore == nil {
		return body, req, stats, nil
	}

	blocks := extractClaudeBlocksFromRoot(root)
	if cfg.Mode == "observe" {
		for _, block := range blocks {
			cfg.SessionStore.SaveBlock(sessionKey, block)
		}
		return body, req, stats, nil
	}
	if cfg.Mode == "optimize" && !cfg.EnableBlockOptimizer {
		for _, block := range blocks {
			cfg.SessionStore.SaveBlock(sessionKey, block)
		}
		return body, req, stats, nil
	}
	stats.BlocksSeen = len(blocks)
	stats.BlockKinds = make(map[string]int, 4)
	prunable := make(map[string]bool, len(blocks))
	for _, block := range blocks {
		stats.BlockKinds[block.Kind]++
		if cfg.SessionStore.Seen(sessionKey, block.Hash) {
			prunable[block.Hash] = true
			stats.BlocksKnown++
		} else {
			stats.BlocksNew++
		}
	}

	// Save all blocks for future deduplication.
	for _, block := range blocks {
		cfg.SessionStore.SaveBlock(sessionKey, block)
	}

	// Always re-parse so the rewrite mutates a fresh tree and the original
	// body stays intact for dry_run mode.
	var rewriteRoot map[string]any
	if err := json.Unmarshal(body, &rewriteRoot); err != nil {
		p.logger.WarnContext(ctx, "claude optimize re-parse failed; forwarding original", slog.Any("err", err))
		return body, req, stats, nil
	}

	// Phase 1: Message-level dedup — compact old conversation turns whose
	// full content was already seen in a prior request. This catches the
	// many small messages that individually fall below claudeMinBlockBytes
	// but collectively dominate Claude Code's repeated payload.
	msgPruned := pruneOldMessages(rewriteRoot, cfg.SessionStore, sessionKey)
	stats.MessagesPruned = msgPruned

	// Phase 2: Chunk-level dedup — replace remaining repeated text spans
	// within surviving messages.
	var chunkPruned int
	if len(prunable) > 0 {
		chunkPruned = applyChunkRewrite(rewriteRoot, prunable)
	}

	optimized, err := json.Marshal(rewriteRoot)
	if err != nil {
		p.logger.WarnContext(ctx, "claude optimize marshal failed; forwarding original", slog.Any("err", err))
		return body, req, stats, nil
	}

	stats.BlocksPruned = chunkPruned
	stats.BytesAfter = len(optimized)
	stats.EstimatedTokensAfter = estimateTokens(len(optimized))
	stats.EstimatedTokensSaved = max(0, stats.EstimatedTokensBefore-stats.EstimatedTokensAfter)
	if stats.BytesBefore > 0 {
		stats.ReductionRatio = float64(stats.BytesBefore-stats.BytesAfter) / float64(stats.BytesBefore)
	}

	if cfg.Mode == "optimize" {
		return optimized, req, stats, nil
	}
	return body, req, stats, nil
}

// pruneOldMessages compacts old conversation turns whose full serialized
// content was already seen in a prior request. It protects the last N
// messages to preserve the current turn context. For user messages with
// tool_result blocks, only the tool_result content is replaced (keeping
// tool_use_id intact). For assistant messages, tool_use blocks are kept
// (they're referenced by subsequent tool_results) and text is compacted.
func pruneOldMessages(root map[string]any, store *memory.Store, sessionKey string) int {
	messages, ok := root["messages"].([]any)
	if !ok || len(messages) <= claudeProtectLastN {
		return 0
	}

	pruned := 0
	cutoff := len(messages) - claudeProtectLastN

	for i := 0; i < cutoff; i++ {
		msg, ok := messages[i].(map[string]any)
		if !ok {
			continue
		}

		contentJSON, err := json.Marshal(msg["content"])
		if err != nil || len(contentJSON) < claudeMinMessageBytes {
			continue
		}

		msgHash := "msg:" + hashText(string(contentJSON))
		if !store.Seen(sessionKey, msgHash) {
			continue
		}

		// This message content was seen in a prior request — compact it.
		role, _ := msg["role"].(string)
		msg["content"] = compactMessageContent(msg["content"], role, msgHash)
		pruned++
	}

	return pruned
}

// compactMessageContent replaces the bulk of a message's content with a
// short marker while preserving structural elements the API requires.
func compactMessageContent(content any, role, hash string) any {
	marker := fmt.Sprintf("[iq:prior %s turn ref:%s]", role, hash[4:16])

	switch c := content.(type) {
	case string:
		return marker
	case []any:
		if role == "assistant" {
			// Keep tool_use blocks (subsequent tool_results reference them by ID).
			// Replace text blocks with the compact marker.
			kept := make([]any, 0, len(c))
			addedMarker := false
			for _, rawItem := range c {
				item, ok := rawItem.(map[string]any)
				if !ok {
					continue
				}
				typ, _ := item["type"].(string)
				if typ == "tool_use" {
					kept = append(kept, item)
				} else if !addedMarker {
					kept = append(kept, map[string]any{"type": "text", "text": marker})
					addedMarker = true
				}
			}
			if len(kept) == 0 {
				return marker
			}
			return kept
		}
		// For user messages: keep tool_result wrappers (tool_use_id) but
		// replace their content with the compact marker.
		for _, rawItem := range c {
			item, ok := rawItem.(map[string]any)
			if !ok {
				continue
			}
			typ, _ := item["type"].(string)
			switch typ {
			case "tool_result":
				item["content"] = marker
			case "text":
				item["text"] = marker
			}
		}
		return c
	default:
		return marker
	}
}

// applyChunkRewrite runs the chunk-level span-preserving rewrite on the
// already-parsed root. Returns the number of chunks replaced.
func applyChunkRewrite(root map[string]any, repeated map[string]bool) int {
	totalPruned := 0
	if sys := root["system"]; sys != nil {
		rewritten, n := rewriteAnthropicContentValue(sys, repeated)
		if n > 0 {
			root["system"] = rewritten
			totalPruned += n
		}
	}
	messages, ok := root["messages"].([]any)
	if !ok {
		return totalPruned
	}
	for _, rawMsg := range messages {
		msg, ok := rawMsg.(map[string]any)
		if !ok {
			continue
		}
		rewritten, n := rewriteAnthropicContentValue(msg["content"], repeated)
		if n > 0 {
			msg["content"] = rewritten
			totalPruned += n
		}
	}
	return totalPruned
}

func extractClaudeBlocksFromRoot(root map[string]any) []memory.Block {
	var blocks []memory.Block
	appendText := func(kind, text string) {
		for _, chunk := range splitStableTextChunks(text) {
			if len(chunk) < claudeMinBlockBytes {
				continue
			}
			sum := sha256.Sum256([]byte(chunk))
			blocks = append(blocks, memory.Block{
				Hash:   hex.EncodeToString(sum[:]),
				Kind:   kind,
				Bytes:  len(chunk),
				Tokens: estimateTokens(len(chunk)),
			})
		}
	}
	for _, text := range textValuesFromAny(root["system"]) {
		appendText("system", text)
	}
	if messages, ok := root["messages"].([]any); ok {
		for i, rawMsg := range messages {
			msg, ok := rawMsg.(map[string]any)
			if !ok {
				continue
			}
			role, _ := msg["role"].(string)
			// Chunk-level blocks from text values.
			for _, text := range textValuesFromAny(msg["content"]) {
				appendText("message:"+role, text)
			}
			// Message-level block: hash the full serialized content so we
			// can deduplicate entire old messages, including ones with many
			// small content blocks that individually fall below the chunk
			// threshold.
			contentJSON, err := json.Marshal(msg["content"])
			if err == nil && len(contentJSON) >= claudeMinMessageBytes {
				sum := sha256.Sum256(contentJSON)
				blocks = append(blocks, memory.Block{
					Hash:   "msg:" + hex.EncodeToString(sum[:]),
					Kind:   fmt.Sprintf("message_full:%s:%d", role, i),
					Bytes:  len(contentJSON),
					Tokens: estimateTokens(len(contentJSON)),
				})
			}
		}
	}
	return blocks
}

func textValuesFromAny(v any) []string {
	if v == nil {
		return nil
	}
	if s, ok := v.(string); ok {
		return []string{s}
	}
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	var out []string
	for _, item := range arr {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		typ, _ := m["type"].(string)
		switch typ {
		case "text":
			if text, ok := m["text"].(string); ok {
				out = append(out, text)
			}
		case "tool_result":
			// tool_result content can be a string or an array of content blocks.
			out = append(out, textValuesFromAny(m["content"])...)
		}
	}
	return out
}

func splitStableTextChunks(text string) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	// Small-to-medium blocks are hashed as a single unit. This threshold
	// must be low enough that large user messages with appended instructions
	// still get chunked so the shared prefix chunks can be deduplicated.
	if len(text) <= 8192 {
		return []string{text}
	}
	lines := strings.Split(text, "\n")
	var chunks []string
	var b strings.Builder
	for _, line := range lines {
		if b.Len()+len(line)+1 > claudeTargetChunkBytes && b.Len() >= claudeMinBlockBytes {
			chunks = append(chunks, strings.TrimSpace(b.String()))
			b.Reset()
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	if strings.TrimSpace(b.String()) != "" {
		chunks = append(chunks, strings.TrimSpace(b.String()))
	}
	return chunks
}




// rewriteAnthropicContentValue replaces repeated text spans within a content
// value. Returns the (possibly rewritten) content and the number of chunks
// actually replaced.
func rewriteAnthropicContentValue(content any, repeated map[string]bool) (any, int) {
	switch c := content.(type) {
	case string:
		rewritten, n := replaceRepeatedSpans(c, repeated)
		if n > 0 {
			return rewritten, n
		}
		return content, 0
	case []any:
		total := 0
		for _, rawItem := range c {
			item, ok := rawItem.(map[string]any)
			if !ok {
				continue
			}
			typ, _ := item["type"].(string)
			switch typ {
			case "text":
				text, ok := item["text"].(string)
				if !ok || len(text) < claudeMinBlockBytes {
					continue
				}
				rewritten, n := replaceRepeatedSpans(text, repeated)
				if n > 0 {
					item["text"] = rewritten
					total += n
				}
			case "tool_result":
				// Recurse into tool_result content (string or content array).
				rewritten, n := rewriteAnthropicContentValue(item["content"], repeated)
				if n > 0 {
					item["content"] = rewritten
					total += n
				}
			}
		}
		return c, total
	default:
		return content, 0
	}
}

// replaceRepeatedSpans replaces repeated text chunks in the original text
// without altering the whitespace or formatting of non-repeated content.
// For text <= 8KB (single chunk), it replaces the whole value if matched.
// For text > 8KB, it locates chunk boundaries within the original text and
// replaces only the byte spans of matched chunks.
// Returns the result text and the number of chunks replaced.
func replaceRepeatedSpans(text string, repeated map[string]bool) (string, int) {
	trimmed := strings.TrimSpace(text)
	if len(trimmed) < claudeMinBlockBytes {
		return text, 0
	}

	// Single-chunk fast path: the whole value is one hash unit.
	if len(trimmed) <= 8192 {
		hash := hashText(trimmed)
		if repeated[hash] {
			return fmt.Sprintf(claudeReplacementFormat, hash[:12]), 1
		}
		return text, 0
	}

	// Multi-chunk path: walk lines, build chunks with byte offsets within
	// the trimmed region, and replace only matched spans.
	trimStart := strings.Index(text, trimmed[:1])
	if trimStart < 0 {
		trimStart = 0
	}

	type span struct {
		start int    // byte offset in original text
		end   int    // exclusive byte offset in original text
		hash  string // SHA-256 of trimmed chunk content
	}

	var spans []span
	lines := strings.Split(trimmed, "\n")
	var b strings.Builder
	chunkStartInTrimmed := 0
	posInTrimmed := 0

	for _, line := range lines {
		if b.Len()+len(line)+1 > claudeTargetChunkBytes && b.Len() >= claudeMinBlockBytes {
			chunk := strings.TrimSpace(b.String())
			if len(chunk) >= claudeMinBlockBytes {
				spans = append(spans, span{
					start: trimStart + chunkStartInTrimmed,
					end:   trimStart + posInTrimmed,
					hash:  hashText(chunk),
				})
			}
			chunkStartInTrimmed = posInTrimmed
			b.Reset()
		}
		b.WriteString(line)
		b.WriteByte('\n')
		posInTrimmed += len(line) + 1
	}
	if chunk := strings.TrimSpace(b.String()); len(chunk) >= claudeMinBlockBytes {
		spans = append(spans, span{
			start: trimStart + chunkStartInTrimmed,
			end:   trimStart + len(trimmed),
			hash:  hashText(chunk),
		})
	}

	if len(spans) == 0 {
		return text, 0
	}

	// Build result by replacing only matched spans, preserving everything else.
	var out strings.Builder
	out.Grow(len(text))
	cursor := 0
	count := 0
	for _, s := range spans {
		if !repeated[s.hash] {
			continue
		}
		out.WriteString(text[cursor:s.start])
		out.WriteString(fmt.Sprintf(claudeReplacementFormat, s.hash[:12]))
		cursor = s.end
		count++
	}
	if count == 0 {
		return text, 0
	}
	out.WriteString(text[cursor:])
	return out.String(), count
}

func hashText(text string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(text)))
	return hex.EncodeToString(sum[:])
}

func (p *Proxy) forwardClaudeMessages(w http.ResponseWriter, r *http.Request, cfg ClaudeMessagesConfig, body []byte) claudeStreamStats {
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
	upReq.Header.Set("x-api-key", cfg.AnthropicAPIKey)
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
		stats.Status = "error"
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

func (p *Proxy) logClaudeRequestComplete(ctx context.Context, requestID, mode, model, sessionKey string, bytesBefore int, opt claudeOptimizerStats, stream claudeStreamStats, dur time.Duration) {
	level := slog.LevelInfo
	if stream.Status == "error" {
		level = slog.LevelError
	} else if stream.Cancelled {
		level = slog.LevelWarn
	}
	attrs := []slog.Attr{
		slog.String("event", "request_complete"),
		slog.String("request_id", requestID),
		slog.String("mode", mode),
		slog.String("provider", "anthropic"),
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
		slog.Int("messages_pruned", opt.MessagesPruned),
		slog.Int("stream_chunks", stream.Chunks),
		slog.Int("estimated_output_tokens", stream.estimatedOutputTokens()),
		slog.Int64("duration_ms", dur.Milliseconds()),
		slog.String("status", stream.Status),
		slog.Int("status_code", stream.StatusCode),
		slog.Bool("stream_completed", stream.Completed),
		slog.Bool("stream_cancelled", stream.Cancelled),
	}
	for kind, n := range opt.BlockKinds {
		attrs = append(attrs, slog.Int("kind:"+kind, n))
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
	if stream.CircuitOpen {
		attrs = append(attrs, slog.Bool("circuit_open", true))
	}
	if stream.CircuitCooldown > 0 {
		attrs = append(attrs, slog.Int64("circuit_cooldown_ms", stream.CircuitCooldown.Milliseconds()))
	}
	p.logger.LogAttrs(ctx, level, "claude request complete", attrs...)
}

func (s claudeStreamStats) estimatedOutputTokens() int {
	if s.OutputTokens > 0 {
		return s.OutputTokens
	}
	return estimateTokens(s.OutputText)
}

func (s claudeStreamStats) upstreamMeta() claudeUpstreamErrorMeta {
	return claudeUpstreamErrorMeta{
		StatusCode: s.StatusCode,
		Code:       s.UpstreamErrorCode,
		Type:       s.UpstreamErrorType,
		RequestID:  s.UpstreamRequestID,
		RetryAfter: s.RetryAfter,
	}
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

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
