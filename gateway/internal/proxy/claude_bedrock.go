package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
)

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
