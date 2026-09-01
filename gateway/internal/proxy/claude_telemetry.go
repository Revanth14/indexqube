package proxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/telemetry"
)

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
