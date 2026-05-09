package proxy

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"sort"
	"strings"
)

// ModelEntry is an exported model descriptor returned by /v1/models.
type ModelEntry struct {
	ID          string `json:"id"`
	Description string `json:"description,omitempty"`
}

var modelCatalog = map[string][]ModelEntry{
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
}

func (p *Proxy) handleModels(w http.ResponseWriter, r *http.Request) {
	provider := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("provider")))

	type response struct {
		Object string       `json:"object"`
		Data   []ModelEntry `json:"data"`
	}

	// When Bedrock is enabled and models were fetched at startup, return those
	// for the "anthropic" provider (Claude Code always queries provider=anthropic).
	if p.claude.Bedrock.Enabled && len(p.claude.Bedrock.Models) > 0 {
		if provider == "" || provider == "anthropic" || provider == "bedrock" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(response{Object: "list", Data: p.claude.Bedrock.Models})
			return
		}
	}

	var models []ModelEntry
	if provider != "" {
		models = modelCatalog[provider]
	} else {
		for _, entries := range modelCatalog {
			models = append(models, entries...)
		}
		sort.Slice(models, func(i, j int) bool { return models[i].ID < models[j].ID })
	}
	if models == nil {
		models = []ModelEntry{}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(response{Object: "list", Data: models}); err != nil {
		p.logger.ErrorContext(r.Context(), "models encode failed", slog.Any("err", err))
	}
}
