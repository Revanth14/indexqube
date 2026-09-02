package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/Revanth14/indexqube/gateway/internal/domain"
	"github.com/Revanth14/indexqube/gateway/internal/sessions"
	"github.com/Revanth14/indexqube/gateway/internal/telemetry"
)

const defaultRawContextPath = "indexqube/raw_context.txt"
const optimizeContractVersion = "v1"

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

func (p *Proxy) handleDiagnostics(w http.ResponseWriter, r *http.Request) {
	diag, err := p.governor.Diagnostics(r.Context())
	if err != nil {
		p.writeError(w, r, errorPayload{
			HTTPStatus: http.StatusInternalServerError,
			Type:       "server_error",
			Code:       "diagnostics_failed",
			Message:    err.Error(),
		})
		return
	}
	if diag.Status == "" {
		diag.Status = "ok"
	}
	diag.ContractVersion = optimizeContractVersion
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(diag); err != nil {
		p.logger.ErrorContext(r.Context(), "diagnostics encode failed", slog.Any("err", err))
	}
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
	Version              string            `json:"version"`
	Mode                 string            `json:"mode"`
	Messages             []domain.Message  `json:"messages"`
	Text                 string            `json:"text,omitempty"`
	Stats                domain.PruneStats `json:"stats"`
	BytesSaved           int               `json:"bytes_saved"`
	EstimatedTokensSaved int               `json:"estimated_tokens_saved"`
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
	if err := validateSessionKey(sk); err != nil {
		p.writeError(w, r, errorPayload{HTTPStatus: http.StatusBadRequest, Type: "invalid_request_error", Code: "invalid_session_key", Message: err.Error()})
		return
	}
	pm := body.ProjectMemory
	if pm == "" {
		pm = r.Header.Get(headerProjectMemory)
	}
	tenant := ""
	if sk != "" {
		tenant = domain.ResolveTenantKey(sk, "")
	}

	ctx, cancel := p.optimizeContext(r.Context())
	defer cancel()
	msgs, stats, err := p.governor.Optimize(ctx, tenant, body.Messages, pm)
	if err != nil {
		p.writeOptimizeError(w, r, err)
		return
	}
	bodyOut := newOptimizeResponse(msgs, stats, tenant != "")
	writeOptimizeStatsHeaders(w, bodyOut)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(bodyOut); err != nil {
		p.logger.ErrorContext(r.Context(), "optimize encode failed", slog.Any("err", err))
	}
}

// optimizeContext applies the configured per-request cap to /v1/optimize.
// Returns the request context unchanged if no cap is set.
func (p *Proxy) optimizeContext(parent context.Context) (context.Context, context.CancelFunc) {
	if p.optimizeTimeout <= 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, p.optimizeTimeout)
}

// writeOptimizeError maps governor errors to client status. Deadline
// exceeded (the optimize timeout firing) becomes 504 so callers can
// distinguish "we gave up" from a real 5xx.
func (p *Proxy) writeOptimizeError(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, context.DeadlineExceeded) {
		p.writeError(w, r, errorPayload{
			HTTPStatus: http.StatusGatewayTimeout,
			Type:       "server_error",
			Code:       "optimize_timeout",
			Message:    "optimize exceeded configured timeout",
		})
		return
	}
	p.writeError(w, r, errorPayload{HTTPStatus: http.StatusInternalServerError, Type: "server_error", Message: err.Error()})
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
	ctx, cancel := p.optimizeContext(r.Context())
	defer cancel()
	msgs, stats, err := p.governor.Optimize(
		ctx,
		tenant,
		[]domain.Message{{Role: "user", Content: content}},
		r.Header.Get(headerProjectMemory),
	)
	if err != nil {
		p.writeOptimizeError(w, r, err)
		return
	}

	bodyOut := newOptimizeResponse(msgs, stats, tenant != "")
	writeOptimizeStatsHeaders(w, bodyOut)
	if wantsOptimizeJSON(r) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(bodyOut); err != nil {
			p.logger.ErrorContext(r.Context(), "optimize text json encode failed", slog.Any("err", err))
		}
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte(bodyOut.Text)); err != nil {
		p.logger.ErrorContext(r.Context(), "optimize text write failed", slog.Any("err", err))
	}
}

func isRawOptimizeRequest(r *http.Request) bool {
	ct := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Type")))
	return strings.HasPrefix(ct, "text/plain")
}

func wantsOptimizeJSON(r *http.Request) bool {
	return strings.Contains(strings.ToLower(r.Header.Get("Accept")), "application/json")
}

func newOptimizeResponse(msgs []domain.Message, stats domain.PruneStats, hasSession bool) optimizeResponseBody {
	return optimizeResponseBody{
		Version:              optimizeContractVersion,
		Mode:                 optimizeMode(stats, hasSession),
		Messages:             msgs,
		Text:                 renderOptimizedText(msgs),
		Stats:                stats,
		BytesSaved:           stats.BytesSaved,
		EstimatedTokensSaved: stats.TokensSaved,
	}
}

func optimizeMode(stats domain.PruneStats, hasSession bool) string {
	if !hasSession {
		return "stateless"
	}
	if stats.BlocksPruned > 0 {
		if stats.DiffExact+stats.DiffFallback > 0 {
			return "diff"
		}
		return "unchanged"
	}
	if stats.BlocksSkipped > 0 {
		return "skipped"
	}
	return "warmup"
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
		if contextPath == "" && contextLang == "" {
			if normalized, ok := normalizeBrowserPromptCode(content); ok {
				return normalized
			}
		}
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
	if path == "" || strings.Contains(path, "```") || strings.ContainsAny(path, "\r\n\t ") || containsTraversal(path) {
		return defaultRawContextPath
	}
	return path
}

func containsTraversal(path string) bool {
	parts := strings.Split(path, "/")
	for _, p := range parts {
		if p == ".." {
			return true
		}
	}
	return false
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

func writeOptimizeStatsHeaders(w http.ResponseWriter, body optimizeResponseBody) {
	stats := body.Stats
	w.Header().Set("X-IQ-Contract-Version", body.Version)
	w.Header().Set("X-IQ-Mode", body.Mode)
	w.Header().Set("X-IQ-Blocks-Seen", strconv.Itoa(stats.BlocksSeen))
	w.Header().Set("X-IQ-Blocks-Pruned", strconv.Itoa(stats.BlocksPruned))
	w.Header().Set("X-IQ-Blocks-Skipped", strconv.Itoa(stats.BlocksSkipped))
	w.Header().Set("X-IQ-Bytes-Before", strconv.Itoa(stats.BytesBefore))
	w.Header().Set("X-IQ-Bytes-After", strconv.Itoa(stats.BytesAfter))
	w.Header().Set("X-IQ-Bytes-Saved", strconv.Itoa(stats.BytesSaved))
	w.Header().Set("X-IQ-Tokens-Before", strconv.Itoa(stats.TokensBefore))
	w.Header().Set("X-IQ-Tokens-After", strconv.Itoa(stats.TokensAfter))
	w.Header().Set("X-IQ-Tokens-Saved", strconv.Itoa(stats.TokensSaved))
	w.Header().Set("X-IQ-Reduction-Ratio", strconv.FormatFloat(stats.ReductionRatio, 'f', 6, 64))
	w.Header().Set("X-IQ-Diff-Exact", strconv.Itoa(stats.DiffExact))
	w.Header().Set("X-IQ-Diff-Fallback", strconv.Itoa(stats.DiffFallback))
	if skipReasons := formatSkipReasons(stats.SkipReasons); skipReasons != "" {
		w.Header().Set("X-IQ-Skip-Reasons", skipReasons)
	}
}

func formatSkipReasons(reasons map[string]int) string {
	if len(reasons) == 0 {
		return ""
	}
	keys := make([]string, 0, len(reasons))
	for reason, n := range reasons {
		if reason != "" && n > 0 {
			keys = append(keys, reason)
		}
	}
	if len(keys) == 0 {
		return ""
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, reason := range keys {
		parts = append(parts, reason+"="+strconv.Itoa(reasons[reason]))
	}
	return strings.Join(parts, ",")
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
		p.logger.ErrorContext(r.Context(), "sse writer init failed", slog.Any("err", err))
		p.writeError(w, r, errorPayload{
			HTTPStatus: http.StatusInternalServerError,
			Type:       "server_error",
			Code:       "stream_init_failed",
			Message:    "streaming not supported on this connection",
		})
		return
	}

	ctx := r.Context()
	if p.streamTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.streamTimeout)
		defer cancel()
	}

	if err := p.governor.Stream(ctx, req, sw); err != nil {
		// Client hung up -- socket is gone, don't try to write to it.
		// Two paths lead here:
		//   A) The adapter detected ctx.Err() and returned context.Canceled.
		//   B) A write to the client socket failed (broken pipe / connection
		//      reset). By the time we land here the Go HTTP server has already
		//      cancelled r.Context(), so r.Context().Err() is non-nil.
		if errors.Is(err, context.Canceled) || r.Context().Err() != nil {
			p.logger.InfoContext(r.Context(), "client disconnected mid-stream")
			if p.metrics != nil {
				p.metrics.StreamCancellations.Inc()
			}
			return
		}

		// Best-effort error frame. If this write also fails, the client is
		// already gone; we log and return.
		safePayload := upstreamErrorPayload(err)
		if p.metrics != nil {
			p.metrics.StreamErrors.WithLabelValues(safePayload.Code, string(req.Credential.Provider), req.Model).Inc()
		}
		errBytes, mErr := json.Marshal(errorEnvelope{Error: safePayload})
		if mErr == nil {
			if wErr := sw.WriteEvent("error", errBytes); wErr != nil {
				p.logger.WarnContext(r.Context(), "failed to emit sse error frame", slog.Any("err", wErr))
			}
		}
		p.logger.ErrorContext(r.Context(), "governor stream failed",
			slog.String("error_code", safePayload.Code),
			slog.String("err", safeLogError(err)),
		)
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
	payload = safeErrorPayload(payload)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(payload.HTTPStatus)
	if err := json.NewEncoder(w).Encode(errorEnvelope{Error: payload}); err != nil {
		p.logger.ErrorContext(r.Context(), "failed to encode error response", slog.Any("err", err))
	}
}

// handleAgentSessions returns agent session data for the iq ui dashboard.
// Live in-memory sessions come from sessionTracker; durable history (surviving
// restarts) comes from sessionPersist (SQLite). Both sources are included so
// the dashboard shows the full picture without requiring a process restart.
func (p *Proxy) handleAgentSessions(w http.ResponseWriter, _ *http.Request) {
	type response struct {
		Sessions      []telemetry.AgentSession `json:"sessions"`
		KillLog       []telemetry.KillEvent    `json:"kill_log"`
		TotalSessions int                      `json:"total_sessions"`
		TotalKills    int                      `json:"total_kills"`
	}

	var resp response

	// Live sessions from the current process (fast, always up-to-date).
	if p.sessionTracker != nil {
		resp.Sessions = p.sessionTracker.Snapshot()
		resp.KillLog = p.sessionTracker.KillLog()
	}

	// Merge historical data from SQLite — sessions from previous runs that
	// are no longer in the in-memory store, and the durable kill log.
	if p.sessionPersist != nil {
		seen := make(map[string]bool, len(resp.Sessions))
		for _, s := range resp.Sessions {
			seen[s.SessionID] = true
		}
		if rows, err := p.sessionPersist.Sessions(); err == nil {
			for _, row := range rows {
				if !seen[row.SessionID] {
					resp.Sessions = append(resp.Sessions, sessions.ToAgentSession(row))
				}
			}
		}
		// Kill log: prefer SQLite (survives restarts) over in-memory.
		if kills, err := p.sessionPersist.KillLog(); err == nil {
			resp.KillLog = make([]telemetry.KillEvent, len(kills))
			for i, k := range kills {
				resp.KillLog[i] = sessions.ToKillEvent(k)
			}
		}
	}

	if resp.Sessions == nil {
		resp.Sessions = []telemetry.AgentSession{}
	}
	if resp.KillLog == nil {
		resp.KillLog = []telemetry.KillEvent{}
	}
	resp.TotalSessions = len(resp.Sessions)
	for _, s := range resp.Sessions {
		resp.TotalKills += s.KillEvents
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// handleStats returns aggregate gateway metrics for the public landing page.
// When a Supabase stats handler is configured it returns global all-time totals
// from the usage_events table (cached 5 min). Otherwise it falls back to local
// SQLite + in-memory session data for the current process.
func (p *Proxy) handleStats(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Prefer Supabase for global totals (server deployments).
	if p.supabaseStats != nil {
		stats, err := p.supabaseStats.get()
		if err != nil {
			http.Error(w, `{"error":"stats unavailable"}`, http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Cache-Control", "public, max-age=300")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(stats)
		return
	}

	// Fallback: aggregate from local SQLite + in-memory store.
	type statsResp struct {
		SessionsTotal      int64 `json:"sessions_total"`
		TokensAttempted    int64 `json:"tokens_attempted"`
		TokensDeduplicated int64 `json:"tokens_deduplicated"`
		RequestsTotal      int64 `json:"requests_total"`
	}

	var resp statsResp
	seen := make(map[string]bool)

	if p.sessionPersist != nil {
		if rows, err := p.sessionPersist.Sessions(); err == nil {
			for _, row := range rows {
				seen[row.SessionID] = true
				resp.SessionsTotal++
				resp.TokensAttempted += row.TokensAttempted
				resp.TokensDeduplicated += row.TokensDeduplicated
				resp.RequestsTotal += row.RequestsTotal
			}
		}
	}

	if p.sessionTracker != nil {
		for _, s := range p.sessionTracker.Snapshot() {
			if !seen[s.SessionID] {
				resp.SessionsTotal++
				resp.TokensAttempted += s.TokensAttempted
				resp.TokensDeduplicated += s.TokensDeduplicated
				resp.RequestsTotal += int64(s.RequestsTotal)
			}
		}
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// handleTelemetry accepts a UsageEvent from the iq binary and forwards it to
// the configured telemetry sink (Supabase). This keeps Supabase credentials
// server-side only — the distributed iq binary never sees them.
func (p *Proxy) handleTelemetry(w http.ResponseWriter, r *http.Request) {
	if p.usageTracker == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1<<16) // 64 KiB max
	var event telemetry.UsageEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		p.writeError(w, r, errorPayload{
			HTTPStatus: http.StatusBadRequest,
			Type:       "invalid_request_error",
			Code:       "invalid_body",
			Message:    "could not decode telemetry event",
		})
		return
	}

	p.usageTracker.Track(event)
	w.WriteHeader(http.StatusNoContent)
}

func (p *Proxy) handleReliabilityTelemetry(w http.ResponseWriter, r *http.Request) {
	sink, ok := p.usageTracker.(telemetry.ReliabilitySink)
	if !ok || sink == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<14)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var event telemetry.ReliabilityEvent
	if err := decoder.Decode(&event); err != nil {
		p.writeError(w, r, errorPayload{
			HTTPStatus: http.StatusBadRequest, Type: "invalid_request_error", Code: "invalid_body",
			Message: "could not decode aggregate reliability event",
		})
		return
	}
	sink.TrackReliability(event)
	w.WriteHeader(http.StatusNoContent)
}
