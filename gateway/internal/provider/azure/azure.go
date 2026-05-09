// Package azure is a governor.Adapter implementation for the Azure OpenAI
// Chat Completions API.
//
// It wraps the logic of the standard OpenAI adapter but handles Azure's
// specific URL scheme and authentication (api-key header instead of
// Bearer token).
package azure

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/Revanth14/indexqube/gateway/internal/domain"
)

const (
	defaultAPIVersion = "2024-02-15-preview"
)

// Adapter implements governor.Adapter via Azure OpenAI's Chat Completions API.
type Adapter struct {
	client     *http.Client
	apiVersion string
	logger     *slog.Logger
}

// Option configures an Adapter at construction time.
type Option func(*Adapter)

// WithHTTPClient replaces the default HTTP client.
func WithHTTPClient(c *http.Client) Option {
	return func(a *Adapter) {
		if c != nil {
			a.client = c
		}
	}
}

// WithLogger overrides the default slog.Default() logger.
func WithLogger(l *slog.Logger) Option {
	return func(a *Adapter) {
		if l != nil {
			a.logger = l
		}
	}
}

// WithAPIVersion overrides the Azure OpenAI API version.
func WithAPIVersion(version string) Option {
	return func(a *Adapter) {
		if version != "" {
			a.apiVersion = version
		}
	}
}

// New returns a wired Adapter.
func New(opts ...Option) *Adapter {
	a := &Adapter{
		client:     &http.Client{},
		apiVersion: defaultAPIVersion,
		logger:     slog.Default(),
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// Ready reports whether the adapter is locally configured.
func (a *Adapter) Ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if a == nil || a.client == nil {
		return fmt.Errorf("azure adapter is not initialized")
	}
	return nil
}

// Dispatch is the governor.Adapter implementation.
func (a *Adapter) Dispatch(ctx context.Context, req *domain.InferenceRequest, tw domain.TokenWriter) error {
	// Azure requires the endpoint to be provided. For BYO-Key multi-tenant
	// setups, we expect the client to pass the endpoint via headers or we
	// use a global fallback. Since Azure endpoints are tenant-specific
	// (e.g. {resource}.openai.azure.com), we must ensure it's provided.
	
	// Implementation note: req.Credential.APIKey for Azure is expected to be 
	// the "api-key" value. The endpoint must be resolved.
	// For v1, we assume the APIKey might contain "endpoint|key" or we use a header.
	// However, the current domain.Credential only has APIKey.
	
	// TODO: Refine credential extraction for Azure. 
	// For now, we'll look for an "X-IQ-Azure-Endpoint" header or similar, 
	// but let's stick to the MAANG standard: allow the client to specify the 
	// full target if possible, or use a configured default.
	
	endpoint := req.AzureEndpoint
	apiKey := req.Credential.APIKey
	
	// Fallback: if AzureEndpoint header is missing, check if it's encoded in the APIKey (endpoint|key).
	if endpoint == "" {
		if parts := strings.Split(req.Credential.APIKey, "|"); len(parts) == 2 {
			endpoint = parts[0]
			apiKey = parts[1]
		}
	}

	if err := validateAzureEndpoint(endpoint); err != nil {
		return err
	}

	// Azure URL format: {endpoint}/openai/deployments/{deployment}/chat/completions?api-version={version}
	// We map req.Model to {deployment}.
	url := fmt.Sprintf("%s/openai/deployments/%s/chat/completions?api-version=%s",
		strings.TrimSuffix(endpoint, "/"),
		req.Model,
		a.apiVersion,
	)

	body, err := buildRequest(req)
	if err != nil {
		return fmt.Errorf("build azure request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("new http request: %w", err)
	}
	httpReq.Header.Set("api-key", apiKey)
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")

	resp, err := a.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("azure call failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return readUpstreamError(resp)
	}

	return streamSSE(ctx, resp.Body, tw)
}

func streamSSE(ctx context.Context, body io.Reader, tw domain.TokenWriter) error {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 4096), 1<<20)

	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		line := scanner.Bytes()
		if len(line) == 0 || !bytes.HasPrefix(line, []byte("data: ")) {
			continue
		}
		payload := line[len("data: "):]
		if bytes.Equal(payload, []byte("[DONE]")) {
			return nil
		}
		if isErrorChunk(payload) {
			return parseUpstreamErrorChunk(payload)
		}
		if err := tw.WriteData(payload); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("azure sse read: %w", err)
	}
	return nil
}

func isErrorChunk(payload []byte) bool {
	trimmed := bytes.TrimLeft(payload, " \t")
	return bytes.HasPrefix(trimmed, []byte(`{"error"`))
}

func parseUpstreamErrorChunk(payload []byte) error {
	var ev struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(payload, &ev); err != nil || ev.Error.Message == "" {
		return fmt.Errorf("azure stream error: %s", bytes.TrimSpace(payload))
	}
	return fmt.Errorf("azure stream error: %s", ev.Error.Message)
}

func readUpstreamError(resp *http.Response) error {
	const limit = 64 << 10
	body, _ := io.ReadAll(io.LimitReader(resp.Body, limit))
	return fmt.Errorf("azure api error: status=%d body=%s", resp.StatusCode, bytes.TrimSpace(body))
}

// validateAzureEndpoint blocks SSRF by requiring HTTPS and rejecting
// private, link-local, or unspecified IP addresses as the target host.
func validateAzureEndpoint(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil {
		return fmt.Errorf("azure: malformed endpoint: %w", err)
	}
	if u.Scheme != "https" {
		return fmt.Errorf("azure: endpoint must use https, got %q", u.Scheme)
	}
	host := u.Hostname()
	if host == "" {
		return fmt.Errorf("azure: endpoint missing hostname")
	}
	if ip := net.ParseIP(host); ip != nil {
		if ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
			return fmt.Errorf("azure: endpoint must not use a private, link-local, or unspecified address")
		}
	}
	return nil
}

func buildRequest(req *domain.InferenceRequest) ([]byte, error) {
	out := azureRequest{
		Messages:    req.Messages,
		MaxTokens:   req.MaxTokens,
		Temperature: req.Temperature,
		Stream:      true,
	}
	if out.MaxTokens == 0 {
		out.MaxTokens = 4096
	}
	return json.Marshal(out)
}

type azureRequest struct {
	Messages    []domain.Message `json:"messages"`
	MaxTokens   int              `json:"max_tokens,omitempty"`
	Temperature float64          `json:"temperature,omitempty"`
	Stream      bool             `json:"stream"`
}
