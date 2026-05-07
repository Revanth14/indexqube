package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/Revanth14/indexqube/gateway/internal/domain"
)

const defaultRawContextPath = "indexqube/raw_context.txt"

func (p *Proxy) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// handleReady is the readiness probe. It gates on the governor's local
// readiness contract without probing tenant-scoped upstream credentials.
func (p *Proxy) handleReady(w http.ResponseWriter, r *http.Request) {
	if err := p.governor.Ready(r.Context()); err != nil {
		p.writeError(w, r, errorPayload{
			HTTPStatus: http.StatusServiceUnavailable,
			Type:       "server_error",
			Code:       "not_ready",
			Message:    err.Error(),
		})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ready"}`))
}

func (p *Proxy) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	req, err := p.parseInferenceRequest(w, r)
	if err != nil {
		p.writeError(w, r, mapParseError(err))
		return
	}

	if !req.Stream {
		// Non-streaming buffered responses are explicitly out of scope for v1.
		// The whole architectural premise is streaming-first.
		p.writeError(w, r, errorPayload{
			HTTPStatus: http.StatusBadRequest,
			Type:       "invalid_request_error",
			Code:       "stream_required",
			Message:    "non-streaming requests are not supported; set stream:true",
		})
		return
	}

	p.streamThroughGovernor(w, r, req)
}

type optimizeRequestBody struct {
	Messages      []domain.Message `json:"messages"`
	SessionKey    string           `json:"session_key,omitempty"`
	SessionID     string           `json:"session_id,omitempty"`
	ProjectMemory string           `json:"project_memory,omitempty"`
	Prompt        string           `json:"prompt,omitempty"`
	ContextText   string           `json:"context_text,omitempty"`
	ContextPath   string           `json:"context_path,omitempty"`
	ContextLang   string           `json:"context_lang,omitempty"`
}

type optimizeResponseBody struct {
	Messages []domain.Message  `json:"messages"`
	Stats    domain.PruneStats `json:"stats"`
}

// handleOptimize exposes Path A (Chrome pre-processor): prune + memory
// injection without contacting any LLM provider.
func (p *Proxy) handleOptimize(w http.ResponseWriter, r *http.Request) {
	if isRawOptimizeRequest(r) {
		p.handleOptimizeText(w, r)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, p.maxRequestSize)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var body optimizeRequestBody
	if err := dec.Decode(&body); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			p.writeError(w, r, errorPayload{
				HTTPStatus: http.StatusRequestEntityTooLarge,
				Type:       "invalid_request_error",
				Code:       "body_too_large",
				Message:    err.Error(),
			})
			return
		}
		p.writeError(w, r, errorPayload{HTTPStatus: http.StatusBadRequest, Type: "invalid_request_error", Message: err.Error()})
		return
	}
	if len(body.Messages) == 0 {
		body.Messages = messagesFromOptimizePrompt(body.Prompt, body.ContextText, body.ContextPath, body.ContextLang)
		if len(body.Messages) == 0 {
			p.writeError(w, r, errorPayload{HTTPStatus: http.StatusBadRequest, Type: "invalid_request_error", Code: "empty_messages", Message: "messages required"})
			return
		}
	}
	sk := body.SessionKey
	if sk == "" {
		sk = body.SessionID
	}
	if sk == "" {
		sk = r.Header.Get(headerSessionKey)
	}
	pm := body.ProjectMemory
	if pm == "" {
		pm = r.Header.Get(headerProjectMemory)
	}
	tenant := ""
	if sk != "" {
		tenant = domain.ResolveTenantKey(sk, "")
	}

	msgs, stats, err := p.governor.Optimize(r.Context(), tenant, body.Messages, pm)
	if err != nil {
		p.writeError(w, r, errorPayload{HTTPStatus: http.StatusInternalServerError, Type: "server_error", Message: err.Error()})
		return
	}
	writeOptimizeStatsHeaders(w, stats)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(optimizeResponseBody{Messages: msgs, Stats: stats}); err != nil {
		p.logger.ErrorContext(r.Context(), "optimize encode failed", slog.Any("err", err))
	}
}

// handleOptimizeText is the Chrome-extension flow: POST raw prompt text,
// receive raw optimized prompt text. Stats ride response headers.
func (p *Proxy) handleOptimizeText(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, p.maxRequestSize)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			p.writeError(w, r, errorPayload{
				HTTPStatus: http.StatusRequestEntityTooLarge,
				Type:       "invalid_request_error",
				Code:       "body_too_large",
				Message:    err.Error(),
			})
			return
		}
		p.writeError(w, r, errorPayload{HTTPStatus: http.StatusBadRequest, Type: "invalid_request_error", Message: err.Error()})
		return
	}

	content := normalizeRawOptimizeText(string(raw), r.Header.Get(headerContextPath), r.Header.Get(headerContextLang))
	if strings.TrimSpace(content) == "" {
		p.writeError(w, r, errorPayload{HTTPStatus: http.StatusBadRequest, Type: "invalid_request_error", Code: "empty_prompt", Message: "raw prompt text required"})
		return
	}

	tenant := ""
	if sk := r.Header.Get(headerSessionKey); sk != "" {
		tenant = domain.ResolveTenantKey(sk, "")
	}
	msgs, stats, err := p.governor.Optimize(
		r.Context(),
		tenant,
		[]domain.Message{{Role: "user", Content: content}},
		r.Header.Get(headerProjectMemory),
	)
	if err != nil {
		p.writeError(w, r, errorPayload{HTTPStatus: http.StatusInternalServerError, Type: "server_error", Message: err.Error()})
		return
	}

	writeOptimizeStatsHeaders(w, stats)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte(renderOptimizedText(msgs))); err != nil {
		p.logger.ErrorContext(r.Context(), "optimize text write failed", slog.Any("err", err))
	}
}

func isRawOptimizeRequest(r *http.Request) bool {
	ct := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Type")))
	return strings.HasPrefix(ct, "text/plain")
}

func messagesFromOptimizePrompt(prompt, contextText, contextPath, contextLang string) []domain.Message {
	prompt = strings.TrimSpace(prompt)
	contextText = strings.TrimSpace(contextText)
	if prompt == "" && contextText == "" {
		return nil
	}
	content := prompt
	if contextText != "" {
		contextText = ensureFencedContext(contextText, contextPath, contextLang)
	}
	if prompt != "" && contextText != "" {
		content += "\n\n" + contextText
	} else if contextText != "" {
		content = contextText
	}
	return []domain.Message{{Role: "user", Content: content}}
}

func normalizeRawOptimizeText(content, contextPath, contextLang string) string {
	content = strings.TrimSpace(content)
	if content == "" || containsFence(content) {
		return content
	}
	contextPath = strings.TrimSpace(contextPath)
	contextLang = strings.TrimSpace(contextLang)
	if isLegacyBrowserPromptContext(contextPath, contextLang) && !looksLikeCodeText(content) {
		return content
	}
	if contextPath != "" || contextLang != "" || looksLikeCodeText(content) {
		return ensureFencedContext(content, contextPath, contextLang)
	}
	return content
}

func isLegacyBrowserPromptContext(contextPath, contextLang string) bool {
	return contextPath == "browser-prompt.txt" && (contextLang == "" || contextLang == "txt")
}

func ensureFencedContext(content, contextPath, contextLang string) string {
	content = strings.TrimSpace(content)
	if content == "" || containsFence(content) {
		return content
	}
	path := normalizeContextPath(contextPath)
	lang := normalizeContextLang(contextLang, path, content)
	return "```" + lang + " " + path + "\n" + content + "\n```"
}

func containsFence(s string) bool {
	return strings.Contains(s, "```")
}

func normalizeContextPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return defaultRawContextPath
	}
	path = strings.ReplaceAll(path, "\\", "/")
	path = strings.TrimPrefix(path, "/")
	if path == "" || strings.Contains(path, "```") || strings.ContainsAny(path, "\r\n\t ") {
		return defaultRawContextPath
	}
	return path
}

func normalizeContextLang(lang, path, content string) string {
	lang = strings.TrimSpace(lang)
	if lang != "" && !strings.ContainsAny(lang, "` \t\r\n") {
		return lang
	}
	lowerPath := strings.ToLower(path)
	switch {
	case strings.HasSuffix(lowerPath, ".go"):
		return "go"
	case strings.HasSuffix(lowerPath, ".ts"):
		return "ts"
	case strings.HasSuffix(lowerPath, ".tsx"):
		return "tsx"
	case strings.HasSuffix(lowerPath, ".js"):
		return "js"
	case strings.HasSuffix(lowerPath, ".jsx"):
		return "jsx"
	case strings.HasSuffix(lowerPath, ".py"):
		return "py"
	case strings.HasSuffix(lowerPath, ".rs"):
		return "rs"
	case strings.HasSuffix(lowerPath, ".java"):
		return "java"
	case strings.HasSuffix(lowerPath, ".sql"):
		return "sql"
	case strings.HasSuffix(lowerPath, ".json"):
		return "json"
	case strings.HasSuffix(lowerPath, ".yaml"), strings.HasSuffix(lowerPath, ".yml"):
		return "yaml"
	}
	if looksLikeGo(content) {
		return "go"
	}
	return "txt"
}

func looksLikeCodeText(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	if looksLikeGo(s) {
		return true
	}
	codeSignals := []string{"function ", "const ", "let ", "var ", "class ", "import ", "export ", "return ", "{", "}", "=>", ":=", "def ", "SELECT ", "select "}
	hits := 0
	for _, signal := range codeSignals {
		if strings.Contains(s, signal) {
			hits++
		}
	}
	return hits >= 2 || (strings.Contains(s, "\n") && hits >= 1)
}

func looksLikeGo(s string) bool {
	return strings.Contains(s, "func ") ||
		strings.Contains(s, "package ") ||
		strings.Contains(s, " := ") ||
		strings.Contains(s, "interface {") ||
		strings.Contains(s, "struct {")
}

func writeOptimizeStatsHeaders(w http.ResponseWriter, stats domain.PruneStats) {
	w.Header().Set("X-IQ-Blocks-Seen", strconv.Itoa(stats.BlocksSeen))
	w.Header().Set("X-IQ-Blocks-Pruned", strconv.Itoa(stats.BlocksPruned))
	w.Header().Set("X-IQ-Blocks-Skipped", strconv.Itoa(stats.BlocksSkipped))
	w.Header().Set("X-IQ-Bytes-Before", strconv.Itoa(stats.BytesBefore))
	w.Header().Set("X-IQ-Bytes-After", strconv.Itoa(stats.BytesAfter))
	w.Header().Set("X-IQ-Tokens-Before", strconv.Itoa(stats.TokensBefore))
	w.Header().Set("X-IQ-Tokens-After", strconv.Itoa(stats.TokensAfter))
	w.Header().Set("X-IQ-Reduction-Ratio", strconv.FormatFloat(stats.ReductionRatio, 'f', 6, 64))
	w.Header().Set("X-IQ-Diff-Exact", strconv.Itoa(stats.DiffExact))
	w.Header().Set("X-IQ-Diff-Fallback", strconv.Itoa(stats.DiffFallback))
}

func renderOptimizedText(msgs []domain.Message) string {
	parts := make([]string, 0, len(msgs))
	for _, msg := range msgs {
		content := strings.TrimSpace(msg.Content)
		if content != "" {
			parts = append(parts, content)
		}
	}
	return strings.Join(parts, "\n\n")
}

func (p *Proxy) streamThroughGovernor(w http.ResponseWriter, r *http.Request, req *domain.InferenceRequest) {
	sw, err := newSSEWriter(w)
	if err != nil {
		// Headers haven't been committed yet (newSSEWriter failed before any
		// successful frame flush in practice), so a JSON 500 is still safe.
		p.logger.ErrorContext(r.Context(), "sse writer init failed", slog.Any("err", err))
		p.writeError(w, r, errorPayload{
			HTTPStatus: http.StatusInternalServerError,
			Type:       "server_error",
			Code:       "stream_init_failed",
			Message:    "streaming not supported on this connection",
		})
		return
	}

	if err := p.governor.Stream(r.Context(), req, sw); err != nil {
		// Client hung up -- socket is gone, don't try to write to it.
		if errors.Is(err, context.Canceled) {
			p.logger.InfoContext(r.Context(), "client disconnected mid-stream")
			return
		}

		// Best-effort error frame. If this write also fails, the client is
		// already gone; we log and return.
		errBytes, mErr := json.Marshal(errorEnvelope{Error: errorPayload{
			Type:    "upstream_error",
			Message: err.Error(),
		}})
		if mErr == nil {
			if wErr := sw.WriteEvent("error", errBytes); wErr != nil {
				p.logger.WarnContext(r.Context(), "failed to emit sse error frame", slog.Any("err", wErr))
			}
		}
		p.logger.ErrorContext(r.Context(), "governor stream failed", slog.Any("err", err))
		return
	}

	// Clean termination -- emit OpenAI sentinel.
	if err := sw.WriteDone(); err != nil {
		p.logger.WarnContext(r.Context(), "failed to write [DONE] sentinel", slog.Any("err", err))
	}
}

// --- error envelope ---

type errorPayload struct {
	HTTPStatus int    `json:"-"`
	Type       string `json:"type"`
	Code       string `json:"code,omitempty"`
	Message    string `json:"message"`
}

type errorEnvelope struct {
	Error errorPayload `json:"error"`
}

func mapParseError(err error) errorPayload {
	switch {
	case errors.Is(err, errMissingProvider):
		return errorPayload{HTTPStatus: http.StatusBadRequest, Type: "invalid_request_error", Code: "missing_provider", Message: err.Error()}
	case errors.Is(err, errUnknownProvider):
		return errorPayload{HTTPStatus: http.StatusBadRequest, Type: "invalid_request_error", Code: "unknown_provider", Message: err.Error()}
	case errors.Is(err, errMissingKey):
		return errorPayload{HTTPStatus: http.StatusUnauthorized, Type: "authentication_error", Code: "missing_key", Message: err.Error()}
	case errors.Is(err, errBodyTooLarge):
		return errorPayload{HTTPStatus: http.StatusRequestEntityTooLarge, Type: "invalid_request_error", Code: "body_too_large", Message: err.Error()}
	case errors.Is(err, errMissingModel):
		return errorPayload{HTTPStatus: http.StatusBadRequest, Type: "invalid_request_error", Code: "missing_model", Message: err.Error()}
	case errors.Is(err, errEmptyMessages):
		return errorPayload{HTTPStatus: http.StatusBadRequest, Type: "invalid_request_error", Code: "empty_messages", Message: err.Error()}
	default:
		return errorPayload{HTTPStatus: http.StatusBadRequest, Type: "invalid_request_error", Message: err.Error()}
	}
}

func (p *Proxy) writeError(w http.ResponseWriter, r *http.Request, payload errorPayload) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(payload.HTTPStatus)
	if err := json.NewEncoder(w).Encode(errorEnvelope{Error: payload}); err != nil {
		p.logger.ErrorContext(r.Context(), "failed to encode error response", slog.Any("err", err))
	}
}
