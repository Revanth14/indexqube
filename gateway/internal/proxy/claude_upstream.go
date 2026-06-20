package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

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
	applyAnthropicAuthHeaders(upReq.Header, r.Header, cfg)
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
	applyAnthropicAuthHeaders(upReq.Header, r.Header, cfg)
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

func applyAnthropicAuthHeaders(dst, src http.Header, cfg ClaudeMessagesConfig) {
	if cfg.AnthropicAPIKey != "" {
		dst.Set("x-api-key", cfg.AnthropicAPIKey)
		return
	}
	if apiKey := strings.TrimSpace(src.Get("x-api-key")); apiKey != "" {
		dst.Set("x-api-key", apiKey)
		return
	}
	if auth := strings.TrimSpace(src.Get("Authorization")); auth != "" {
		// Passthrough mode: user's OAuth Bearer token flows through unchanged.
		dst.Set("Authorization", auth)
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
