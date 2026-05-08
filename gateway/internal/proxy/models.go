package proxy

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
)

type modelEntry struct {
	ID          string `json:"id"`
	Description string `json:"description,omitempty"`
}

var modelCatalog = map[string][]modelEntry{
	"anthropic": {
		{ID: "claude-opus-4-7", Description: "Most capable"},
		{ID: "claude-sonnet-4-6", Description: "Balanced (recommended)"},
		{ID: "claude-haiku-4-5", Description: "Fast and affordable"},
		{ID: "claude-3-5-sonnet-20241022", Description: "Claude 3.5 Sonnet"},
		{ID: "claude-3-5-haiku-20241022", Description: "Claude 3.5 Haiku"},
		{ID: "claude-3-opus-20240229", Description: "Claude 3 Opus"},
	},
	"openai": {
		{ID: "gpt-4o", Description: "Fastest GPT-4 class (recommended)"},
		{ID: "gpt-4o-mini", Description: "Affordable GPT-4 class"},
		{ID: "gpt-4-turbo", Description: "GPT-4 Turbo"},
		{ID: "gpt-4", Description: "GPT-4"},
		{ID: "o1", Description: "OpenAI o1 reasoning"},
		{ID: "o1-mini", Description: "OpenAI o1 mini"},
		{ID: "o3-mini", Description: "OpenAI o3 mini"},
		{ID: "gpt-3.5-turbo", Description: "GPT-3.5 Turbo"},
	},
	"azure": {
		{ID: "gpt-4o", Description: "Azure GPT-4o deployment"},
		{ID: "gpt-4-turbo", Description: "Azure GPT-4 Turbo deployment"},
		{ID: "gpt-4", Description: "Azure GPT-4 deployment"},
		{ID: "gpt-35-turbo", Description: "Azure GPT-3.5 Turbo deployment"},
	},
	"bedrock": {
		{ID: "anthropic.claude-opus-4-7", Description: "Claude Opus 4.7 on Bedrock"},
		{ID: "anthropic.claude-sonnet-4-6", Description: "Claude Sonnet 4.6 on Bedrock"},
		{ID: "anthropic.claude-haiku-4-5-20251001-v1:0", Description: "Claude Haiku 4.5 on Bedrock"},
		{ID: "anthropic.claude-3-5-sonnet-20241022-v2:0", Description: "Claude 3.5 Sonnet on Bedrock"},
		{ID: "anthropic.claude-3-5-haiku-20241022-v1:0", Description: "Claude 3.5 Haiku on Bedrock"},
		{ID: "anthropic.claude-3-opus-20240229-v1:0", Description: "Claude 3 Opus on Bedrock"},
		{ID: "anthropic.claude-3-sonnet-20240229-v1:0", Description: "Claude 3 Sonnet on Bedrock"},
	},
}

func (p *Proxy) handleModels(w http.ResponseWriter, r *http.Request) {
	provider := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("provider")))

	type response struct {
		Object string       `json:"object"`
		Data   []modelEntry `json:"data"`
	}

	var models []modelEntry
	if provider != "" {
		models = modelCatalog[provider]
	} else {
		for _, entries := range modelCatalog {
			models = append(models, entries...)
		}
	}
	if models == nil {
		models = []modelEntry{}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(response{Object: "list", Data: models}); err != nil {
		p.logger.ErrorContext(r.Context(), "models encode failed", slog.Any("err", err))
	}
}
