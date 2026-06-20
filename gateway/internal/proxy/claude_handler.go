package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/memory"
	"github.com/Revanth14/indexqube/gateway/internal/middleware"
)

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
