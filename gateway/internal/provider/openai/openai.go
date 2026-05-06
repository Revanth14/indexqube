// Package openai is a governor.Adapter implementation for the OpenAI
// Chat Completions API.
//
// Unlike the Anthropic adapter, this one is a near-passthrough: OpenAI's
// streaming wire format IS our canonical wire format (the proxy speaks
// OpenAI-shaped chat.completion.chunk). We forward each `data:` payload
// verbatim through the TokenWriter, stop on `data: [DONE]` without
// forwarding it (the proxy emits the sentinel on clean return), and
// detect upstream-emitted error chunks by their JSON shape.
package openai

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/Revanth14/indexqube/gateway/internal/domain"
)

const (
	defaultBaseURL = "https://api.openai.com"

	// defaultMaxTokens is used when the request omits max_tokens.
	// OpenAI does NOT require max_tokens (unlike Anthropic) -- if 0
	// is sent, the server picks a sensible cap. We still set a
	// circuit-breaker default so a runaway model can't burn budget.
	defaultMaxTokens = 4096
)

// Adapter implements governor.Adapter via OpenAI's Chat Completions API.
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

// WithBaseURL overrides the OpenAI API base URL (used by tests, also
// usable for OpenAI-compatible endpoints like Azure-OpenAI in proxy
// mode or self-hosted vLLM/llama.cpp servers).
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
		return fmt.Errorf("openai adapter is nil")
	}
	if a.client == nil {
		return fmt.Errorf("openai adapter http client is nil")
	}
	if a.baseURL == "" {
		return fmt.Errorf("openai adapter base URL is empty")
	}
	return nil
}

// Dispatch is the governor.Adapter implementation.
func (a *Adapter) Dispatch(ctx context.Context, req *domain.InferenceRequest, tw domain.TokenWriter) error {
	body, err := buildRequest(req)
	if err != nil {
		return fmt.Errorf("build openai request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("new http request: %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+req.Credential.APIKey)
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")

	resp, err := a.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("openai call failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return readUpstreamError(resp)
	}

	return streamSSE(ctx, resp.Body, tw)
}

// streamSSE reads OpenAI's `data: <json>\n\n` SSE frames and forwards
// each payload verbatim through tw.WriteData -- except:
//
//   - `data: [DONE]` terminates the stream cleanly without forwarding.
//   - Payloads beginning with `{"error"` are parsed and returned as an
//     adapter error so the governor's tee abandons the cache write and
//     the proxy emits an SSE error event.
func streamSSE(ctx context.Context, body io.Reader, tw domain.TokenWriter) error {
	scanner := bufio.NewScanner(body)
	// Default token size is 64 KiB; bump to 1 MiB so a single chunk
	// (large tool-call payloads in particular) cannot truncate.
	scanner.Buffer(make([]byte, 0, 4096), 1<<20)

	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		line := scanner.Bytes()
		if len(line) == 0 || !bytes.HasPrefix(line, []byte("data: ")) {
			// Empty lines (frame separators) and non-data lines (comments,
			// future event:/id:/retry: fields) are ignored.
			continue
		}
		payload := line[len("data: "):]

		if bytes.Equal(payload, []byte("[DONE]")) {
			return nil
		}

		// Detect upstream-emitted error frames. Cheap prefix check after
		// trimming whitespace; OpenAI's error chunks always start with
		// the literal `{"error"` byte sequence.
		if isErrorChunk(payload) {
			return parseUpstreamErrorChunk(payload)
		}

		if err := tw.WriteData(payload); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("openai sse read: %w", err)
	}
	return nil
}

func isErrorChunk(payload []byte) bool {
	trimmed := bytes.TrimLeft(payload, " \t")
	return bytes.HasPrefix(trimmed, []byte(`{"error"`))
}

// parseUpstreamErrorChunk extracts a structured error from an OpenAI
// error frame. If parsing fails, the raw payload becomes the error message.
func parseUpstreamErrorChunk(payload []byte) error {
	var ev struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(payload, &ev); err != nil || ev.Error.Message == "" {
		return fmt.Errorf("openai stream error: %s", bytes.TrimSpace(payload))
	}
	if ev.Error.Type != "" {
		return fmt.Errorf("openai stream error: %s: %s", ev.Error.Type, ev.Error.Message)
	}
	return fmt.Errorf("openai stream error: %s", ev.Error.Message)
}

// readUpstreamError consumes the upstream error body from a non-200
// response and wraps it as a concise error suitable for emission to
// the client through the proxy's SSE error frame.
func readUpstreamError(resp *http.Response) error {
	const limit = 64 << 10 // never read more than 64 KiB of error body
	body, _ := io.ReadAll(io.LimitReader(resp.Body, limit))
	return fmt.Errorf("openai api error: status=%d body=%s", resp.StatusCode, bytes.TrimSpace(body))
}

// buildRequest serializes the canonical InferenceRequest into OpenAI's
// Chat Completions request body. OpenAI's shape is essentially our
// canonical shape -- system messages stay inline, no translation.
func buildRequest(req *domain.InferenceRequest) ([]byte, error) {
	out := openAIRequest{
		Model:     req.Model,
		Messages:  req.Messages,
		MaxTokens: req.MaxTokens,
		Stream:    true, // we always stream upstream
	}
	if out.MaxTokens == 0 {
		out.MaxTokens = defaultMaxTokens
	}
	if req.Temperature != 0 {
		t := req.Temperature
		out.Temperature = &t
	}
	return json.Marshal(out)
}

type openAIRequest struct {
	Model       string           `json:"model"`
	Messages    []domain.Message `json:"messages"`
	MaxTokens   int              `json:"max_tokens,omitempty"`
	Temperature *float64         `json:"temperature,omitempty"`
	Stream      bool             `json:"stream"`
}
