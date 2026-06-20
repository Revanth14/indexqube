package proxy

import (
	"fmt"
	"net/http"
	"strings"
)

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
	if strings.TrimSpace(r.Header.Get("x-api-key")) != "" {
		return true
	}
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if !strings.HasPrefix(strings.ToLower(auth), "bearer ") {
		return false
	}
	token := strings.TrimSpace(auth[len("Bearer "):])
	return token != ""
}
