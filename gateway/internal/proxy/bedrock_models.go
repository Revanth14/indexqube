package proxy

import (
	"context"
	"log/slog"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/bedrock"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
)

// FetchBedrockModels calls ListFoundationModels, filters for Anthropic Claude
// models, deduplicates by Claude model name, and returns a sorted ModelEntry
// slice ready to serve from /v1/models.
func FetchBedrockModels(ctx context.Context, cfg BedrockConfig, logger *slog.Logger) []ModelEntry {
	if cfg.Client == nil {
		return nil
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		logger.Warn("bedrock management client init failed, model list unavailable", slog.Any("err", err))
		return nil
	}
	bc := bedrock.NewFromConfig(awsCfg)

	resp, err := bc.ListFoundationModels(ctx, &bedrock.ListFoundationModelsInput{})
	if err != nil {
		logger.Warn("ListFoundationModels failed, model list unavailable", slog.Any("err", err))
		return nil
	}

	seen := map[string]bool{}
	var entries []ModelEntry
	for _, m := range resp.ModelSummaries {
		if m.ModelId == nil || m.ModelName == nil {
			continue
		}
		id := *m.ModelId
		// Only Claude models
		if !strings.Contains(id, "anthropic.claude") {
			continue
		}
		// Skip context-window variants (e.g. :28k, :200k) — they'd duplicate the base model
		if isContextWindowVariant(id) {
			continue
		}
		// Convert Bedrock ID to a Claude model name (what Claude Code sends in requests)
		claudeName := bedrockIDToClaudeModel(id)
		if claudeName == "" || seen[claudeName] {
			continue
		}
		seen[claudeName] = true
		desc := ""
		if m.ModelName != nil {
			desc = *m.ModelName + " (Bedrock)"
		}
		entries = append(entries, ModelEntry{ID: claudeName, Description: desc})
	}
	return entries
}

// bedrockIDToClaudeModel converts a Bedrock model ID like
// "anthropic.claude-3-5-haiku-20241022-v1:0" → "claude-3-5-haiku-20241022"
// and "anthropic.claude-sonnet-4-6" → "claude-sonnet-4-6".
func bedrockIDToClaudeModel(id string) string {
	// Strip regional prefix if present (e.g. "us.", "eu.")
	if dot := strings.Index(id, "."); dot != -1 && !strings.HasPrefix(id[dot+1:], "claude") {
		id = id[dot+1:]
	}
	// Strip "anthropic." prefix
	id = strings.TrimPrefix(id, "anthropic.")
	// Strip ":0" revision suffix
	if i := strings.LastIndex(id, ":"); i != -1 {
		id = id[:i]
	}
	// Strip version suffix "-v1", "-v2", etc.
	if i := strings.LastIndex(id, "-v"); i != -1 {
		rest := id[i+2:]
		allDigits := len(rest) > 0
		for _, c := range rest {
			if c < '0' || c > '9' {
				allDigits = false
				break
			}
		}
		if allDigits {
			id = id[:i]
		}
	}
	return id
}

// isContextWindowVariant returns true for IDs with a context-window suffix
// like ":28k" or ":200k" after the revision (e.g. "...-v1:0:28k").
func isContextWindowVariant(id string) bool {
	// Count colons — base IDs have one ("...:0"), variants have two ("...:0:28k")
	return strings.Count(id, ":") >= 2
}
