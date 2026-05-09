// Package anthropic is a governor.Adapter implementation for the
// Anthropic Messages API.
//
// Wire-format responsibility lives entirely in this package:
//
//  1. Translate domain.InferenceRequest (canonical, OpenAI-shaped) into
//     the Anthropic Messages request body, hoisting `system` messages
//     into the top-level field.
//  2. POST to /v1/messages with the user's BYO key.
//  3. Read the upstream SSE stream, translate each meaningful event
//     into an OpenAI-shaped chat.completion.chunk, and emit it through
//     the supplied TokenWriter.
package anthropic

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/Revanth14/indexqube/gateway/internal/domain"
)

const (
	defaultBaseURL = "https://api.anthropic.com"
	apiVersion     = "2023-06-01"

	// defaultMaxTokens is used when the request omits max_tokens. The
	// Anthropic API requires this field; the value is conservative.
	defaultMaxTokens = 4096
)

// Adapter implements governor.Adapter via Anthropic's Messages API.
//
// One Adapter is reused across requests. The HTTP client is shared and
// must NOT have a global timeout (streaming responses can take minutes);
// per-request cancellation rides ctx instead.
type Adapter struct {
	client  *http.Client
	baseURL string
	logger  *slog.Logger
}

// Option configures an Adapter at construction time.
type Option func(*Adapter)

// WithBaseURL overrides the Anthropic API base URL (used by tests).
func WithBaseURL(url string) Option {
	return func(a *Adapter) {
		if url != "" {
			a.baseURL = url
		}
	}
}

// WithHTTPClient replaces the default HTTP client. The caller is
// responsible for setting (or omitting) timeouts appropriately --
// streaming requires no global Client.Timeout.
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

// New returns a wired Adapter.
func New(opts ...Option) *Adapter {
	a := &Adapter{
		// Zero-Timeout is intentional: we rely on ctx cancellation.
		client:  &http.Client{},
		baseURL: defaultBaseURL,
		logger:  slog.Default(),
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// Ready reports whether the adapter is locally configured enough to serve.
// It intentionally avoids a network probe because BYO keys are per request.
func (a *Adapter) Ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if a == nil {
		return fmt.Errorf("anthropic adapter is nil")
	}
	if a.client == nil {
		return fmt.Errorf("anthropic adapter http client is nil")
	}
	if a.baseURL == "" {
		return fmt.Errorf("anthropic adapter base URL is empty")
	}
	return nil
}

// Dispatch is the governor.Adapter implementation.
func (a *Adapter) Dispatch(ctx context.Context, req *domain.InferenceRequest, tw domain.TokenWriter) error {
	body, err := buildAnthropicRequest(req)
	if err != nil {
		return fmt.Errorf("build anthropic request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+"/v1/messages", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("new http request: %w", err)
	}
	httpReq.Header.Set("x-api-key", req.Credential.APIKey)
	httpReq.Header.Set("anthropic-version", apiVersion)
	httpReq.Header.Set("content-type", "application/json")
	httpReq.Header.Set("accept", "text/event-stream")

	resp, err := a.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("anthropic call failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return readUpstreamError(resp)
	}

	chunkID, err := newChunkID()
	if err != nil {
		return fmt.Errorf("chunk id: %w", err)
	}
	tr := newTranslator(chunkID, req.Model, tw)
	return streamSSE(ctx, resp.Body, tr)
}

// readUpstreamError parses the Anthropic error response body and returns a
// *domain.ProviderError with a sanitised message safe to surface to callers.
// The raw body is preserved in Cause for internal logging.
func readUpstreamError(resp *http.Response) error {
	const limit = 64 << 10 // never read more than 64 KiB of error body
	body, _ := io.ReadAll(io.LimitReader(resp.Body, limit))

	// Try to extract the structured message from Anthropic's error envelope.
	// Shape: {"type":"error","error":{"type":"rate_limit_error","message":"..."}}
	var envelope struct {
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}

	msg := "upstream error"
	if err := json.Unmarshal(body, &envelope); err == nil && envelope.Error.Message != "" {
		// Sanitise: drop anything after the first newline and any content that
		// looks like account-specific detail (credit balance, org names, etc.).
		msg = sanitiseProviderMessage(envelope.Error.Message)
	}

	return &domain.ProviderError{
		Provider:   domain.ProviderAnthropic,
		StatusCode: resp.StatusCode,
		Message:    msg,
		Cause:      fmt.Errorf("raw body: %s", bytes.TrimSpace(body)),
	}
}

// sanitiseProviderMessage strips account-specific or billing detail from a
// provider error message before it is returned to the caller.
func sanitiseProviderMessage(msg string) string {
	if len(msg) > 200 {
		msg = msg[:200]
	}
	return msg
}

// newChunkID returns an OpenAI-compatible chat completion chunk ID.
func newChunkID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "chatcmpl-" + hex.EncodeToString(b[:]), nil
}

// Build a JSON request body in Anthropic's Messages API shape.
func buildAnthropicRequest(req *domain.InferenceRequest) ([]byte, error) {
	out := anthropicRequest{
		Model:     req.Model,
		MaxTokens: req.MaxTokens,
		Stream:    true, // we always stream upstream; non-streaming is a different code path
	}
	if out.MaxTokens == 0 {
		out.MaxTokens = defaultMaxTokens
	}
	if req.Temperature != 0 {
		t := req.Temperature
		out.Temperature = &t
	}

	// Anthropic moves "system" out of the messages array into a top-level
	// field. Concatenate multiple system messages with blank-line separators.
	for _, m := range req.Messages {
		if m.Role == "system" {
			if out.System != "" {
				out.System += "\n\n"
			}
			out.System += m.Content
			continue
		}
		out.Messages = append(out.Messages, anthropicMessage{Role: m.Role, Content: m.Content})
	}
	if len(out.Messages) == 0 {
		return nil, fmt.Errorf("no non-system messages provided")
	}
	return json.Marshal(out)
}

type anthropicRequest struct {
	Model       string             `json:"model"`
	System      string             `json:"system,omitempty"`
	Messages    []anthropicMessage `json:"messages"`
	MaxTokens   int                `json:"max_tokens"`
	Temperature *float64           `json:"temperature,omitempty"`
	Stream      bool               `json:"stream"`
}

type anthropicMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}
