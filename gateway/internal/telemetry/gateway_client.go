package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

// GatewayClient sends telemetry events to a deployed IndexQube gateway's
// /v1/telemetry endpoint. Use this in the iq binary so that Supabase
// credentials never need to be baked into the distributed binary.
type GatewayClient struct {
	endpoint   string
	httpClient *http.Client
}

// NewGatewayClient returns a client that POSTs events to endpoint/v1/telemetry.
func NewGatewayClient(endpoint string) *GatewayClient {
	return &GatewayClient{
		endpoint:   strings.TrimRight(endpoint, "/"),
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}
}

// Track ships a UsageEvent to the gateway in a background goroutine. Never blocks.
func (g *GatewayClient) Track(event UsageEvent) {
	if g == nil || g.endpoint == "" {
		return
	}
	if !Enabled() {
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
			g.endpoint+"/v1/telemetry",
			bytes.NewReader(body),
		)
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := g.httpClient.Do(req)
		if err != nil {
			return
		}
		resp.Body.Close()
	}()
}

func (g *GatewayClient) TrackReliability(event ReliabilityEvent) {
	if g == nil || g.endpoint == "" || !Enabled() {
		return
	}
	go func() {
		body, err := json.Marshal(event)
		if err != nil {
			return
		}
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
			g.endpoint+"/v1/reliability", bytes.NewReader(body))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := g.httpClient.Do(req)
		if err != nil {
			return
		}
		resp.Body.Close()
	}()
}
