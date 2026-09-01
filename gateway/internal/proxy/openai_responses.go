package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/Revanth14/indexqube/gateway/internal/domain"
)

const defaultOpenAIResponsesBaseURL = "https://api.openai.com"

type responsesTextRef struct {
	role string
	text string
	set  func(string)
}

func (p *Proxy) handleOpenAIResponses(w http.ResponseWriter, r *http.Request) {
	cred, err := extractCredential(r)
	if err != nil {
		p.writeError(w, r, mapParseError(err))
		return
	}
	if cred.Provider != domain.ProviderOpenAI {
		p.writeError(w, r, errorPayload{
			HTTPStatus: http.StatusBadRequest,
			Type:       "invalid_request_error",
			Code:       "unsupported_provider",
			Message:    "/v1/responses currently supports OpenAI-compatible bearer auth",
		})
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, p.maxRequestSize)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		p.writeError(w, r, errorPayload{
			HTTPStatus: http.StatusBadRequest,
			Type:       "invalid_request_error",
			Message:    err.Error(),
		})
		return
	}

	var body map[string]any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&body); err != nil {
		p.writeError(w, r, errorPayload{
			HTTPStatus: http.StatusBadRequest,
			Type:       "invalid_request_error",
			Message:    fmt.Sprintf("invalid request body: %v", err),
		})
		return
	}
	if _, ok := body["model"].(string); !ok {
		p.writeError(w, r, errorPayload{
			HTTPStatus: http.StatusBadRequest,
			Type:       "invalid_request_error",
			Code:       "missing_model",
			Message:    "model field is required",
		})
		return
	}

	tenant := domain.ResolveTenantKey(r.Header.Get(headerSessionKey), cred.APIKey)
	if err := p.optimizeResponsesBody(r, body, tenant); err != nil {
		p.writeError(w, r, errorPayload{
			HTTPStatus: http.StatusInternalServerError,
			Type:       "server_error",
			Code:       "optimize_failed",
			Message:    err.Error(),
		})
		return
	}

	out, err := json.Marshal(body)
	if err != nil {
		p.writeError(w, r, errorPayload{
			HTTPStatus: http.StatusInternalServerError,
			Type:       "server_error",
			Code:       "marshal_failed",
			Message:    err.Error(),
		})
		return
	}

	upReq, err := http.NewRequestWithContext(
		r.Context(),
		http.MethodPost,
		openAIResponsesBaseURL()+"/v1/responses",
		bytes.NewReader(out),
	)
	if err != nil {
		p.writeError(w, r, errorPayload{
			HTTPStatus: http.StatusInternalServerError,
			Type:       "server_error",
			Message:    err.Error(),
		})
		return
	}
	copyOpenAIRequestHeaders(upReq.Header, r.Header)
	upReq.Header.Set("Authorization", "Bearer "+cred.APIKey)
	upReq.Header.Set("Content-Type", "application/json")
	if upReq.Header.Get("Accept") == "" {
		upReq.Header.Set("Accept", "text/event-stream")
	}

	client := p.claude.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(upReq)
	if err != nil {
		p.writeError(w, r, errorPayload{
			HTTPStatus: http.StatusBadGateway,
			Type:       "server_error",
			Code:       "upstream_error",
			Message:    err.Error(),
		})
		return
	}
	defer resp.Body.Close()

	copyOpenAIResponseHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	if isStreamingResponsesRequest(body, resp) {
		p.copyStreamingResponse(w, resp.Body)
		return
	}
	_, _ = io.Copy(w, resp.Body)
}

func (p *Proxy) optimizeResponsesBody(r *http.Request, body map[string]any, tenant string) error {
	input, ok := body["input"]
	if !ok {
		return nil
	}
	projectMemory := r.Header.Get(headerProjectMemory)
	if s, ok := input.(string); ok {
		msgs, _, err := p.governor.Optimize(r.Context(), tenant, []domain.Message{{Role: "user", Content: s}}, projectMemory)
		if err != nil {
			return err
		}
		if text, ok := lastMessageContent(msgs, "user"); ok {
			body["input"] = text
		}
		return nil
	}

	refs := collectResponsesTextRefs(input)
	if len(refs) == 0 {
		return nil
	}
	msgs := make([]domain.Message, 0, len(refs))
	for _, ref := range refs {
		role := ref.role
		if role == "" {
			role = "user"
		}
		msgs = append(msgs, domain.Message{Role: role, Content: ref.text})
	}
	optimized, _, err := p.governor.Optimize(r.Context(), tenant, msgs, projectMemory)
	if err != nil {
		return err
	}
	if len(optimized) != len(refs) {
		return nil
	}
	for i := range refs {
		refs[i].set(optimized[i].Content)
	}
	return nil
}

func collectResponsesTextRefs(input any) []responsesTextRef {
	items, ok := input.([]any)
	if !ok {
		return nil
	}
	refs := make([]responsesTextRef, 0, len(items))
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		role, _ := m["role"].(string)
		switch content := m["content"].(type) {
		case string:
			msg := m
			refs = append(refs, responsesTextRef{
				role: role,
				text: content,
				set:  func(s string) { msg["content"] = s },
			})
		case []any:
			if len(content) != 1 {
				continue
			}
			block, ok := content[0].(map[string]any)
			if !ok {
				continue
			}
			text, ok := block["text"].(string)
			if !ok {
				continue
			}
			typ, _ := block["type"].(string)
			if typ != "" && typ != "input_text" && typ != "output_text" {
				continue
			}
			textBlock := block
			refs = append(refs, responsesTextRef{
				role: role,
				text: text,
				set:  func(s string) { textBlock["text"] = s },
			})
		}
	}
	return refs
}

func lastMessageContent(msgs []domain.Message, role string) (string, bool) {
	for i := len(msgs) - 1; i >= 0; i-- {
		if strings.EqualFold(msgs[i].Role, role) {
			return msgs[i].Content, true
		}
	}
	return "", false
}

func openAIResponsesBaseURL() string {
	base := strings.TrimRight(os.Getenv("INDEXQUBE_OPENAI_BASE_URL"), "/")
	if base == "" {
		base = defaultOpenAIResponsesBaseURL
	}
	return strings.TrimSuffix(base, "/v1")
}

func copyOpenAIRequestHeaders(dst, src http.Header) {
	for name, values := range src {
		canonical := http.CanonicalHeaderKey(name)
		lower := strings.ToLower(canonical)
		if lower == "authorization" || lower == "content-length" || lower == "host" {
			continue
		}
		if strings.HasPrefix(lower, "openai-") || strings.HasPrefix(lower, "x-stainless-") || canonical == "Accept" {
			for _, value := range values {
				dst.Add(canonical, value)
			}
		}
	}
}

func copyOpenAIResponseHeaders(dst, src http.Header) {
	for name, values := range src {
		lower := strings.ToLower(name)
		if lower == "content-length" || lower == "connection" || lower == "transfer-encoding" {
			continue
		}
		for _, value := range values {
			dst.Add(name, value)
		}
	}
}

func isStreamingResponsesRequest(body map[string]any, resp *http.Response) bool {
	if stream, ok := body["stream"].(bool); ok && stream {
		return true
	}
	return strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream")
}

func (p *Proxy) copyStreamingResponse(w http.ResponseWriter, body io.Reader) {
	flusher, _ := w.(http.Flusher)
	buf := make([]byte, 32<<10)
	for {
		n, err := body.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if err != nil {
			return
		}
	}
}
