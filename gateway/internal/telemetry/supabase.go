package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
)

// SupabaseClient sends usage telemetry to Supabase REST API. All writes are
// fire-and-forget goroutines — the critical request path is never blocked.
type SupabaseClient struct {
	url        string
	serviceKey string
	httpClient *http.Client
}

// NewSupabaseClient returns a wired client.
func NewSupabaseClient(url, serviceKey string) *SupabaseClient {
	return &SupabaseClient{
		url:        url,
		serviceKey: serviceKey,
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}
}

// UsageEvent captures per-request optimizer statistics. Fields are intentionally
// non-identifying: no prompts, file paths, API keys, or session identifiers.
type UsageEvent struct {
	MachineID            string         `json:"machine_id"`
	OsArch               string         `json:"os_arch"`
	IqVersion            string         `json:"iq_version"`
	CliAgent             string         `json:"cli_agent"`
	ModelTarget          string         `json:"model_target"`
	InputTokensAttempted int            `json:"input_tokens_attempted"`
	InputTokensSent      int            `json:"input_tokens_sent"`
	TokensSaved          int            `json:"tokens_saved"`
	ReductionRatio       float64        `json:"reduction_ratio"`
	BlocksAnalyzed       int            `json:"blocks_analyzed"`
	BlocksPruned         int            `json:"blocks_pruned"`
	ToolTypesSeen        []string       `json:"tool_types_seen,omitempty"`
	SkipReasons          map[string]int `json:"skip_reasons,omitempty"`
	TotalLatencyMs       int            `json:"total_latency_ms"`
	ProxyOverheadMs      float64        `json:"proxy_overhead_ms"`
	UpstreamStatus       int            `json:"upstream_status"`
}

// Track fires a UsageEvent to Supabase in a background goroutine. Never blocks.
// Set IQ_TELEMETRY=off to opt out entirely.
func (s *SupabaseClient) Track(event UsageEvent) {
	if os.Getenv("IQ_TELEMETRY") == "off" {
		return
	}
	go func() {
		body, err := json.Marshal(event)
		if err != nil {
			return
		}
		req, err := http.NewRequestWithContext(
			context.Background(),
			http.MethodPost,
			fmt.Sprintf("%s/rest/v1/usage_events", s.url),
			bytes.NewReader(body),
		)
		if err != nil {
			return
		}
		req.Header.Set("apikey", s.serviceKey)
		req.Header.Set("Authorization", "Bearer "+s.serviceKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Prefer", "return=minimal")
		resp, err := s.httpClient.Do(req)
		if err != nil {
			return
		}
		resp.Body.Close()
	}()
}
