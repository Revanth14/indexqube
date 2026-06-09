package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/memory"
	"github.com/Revanth14/indexqube/gateway/internal/telemetry"
)

func TestParseAnthropicUsage(t *testing.T) {
	t.Parallel()

	// message_start carries input + cache counters nested under "message".
	start := `{"type":"message_start","message":{"id":"msg_1","usage":{"input_tokens":120,"cache_read_input_tokens":4000,"cache_creation_input_tokens":80,"output_tokens":1}}}`
	got := parseAnthropicUsage(start)
	if got.InputTokens != 120 || got.CacheReadInputTokens != 4000 || got.CacheCreationInputTokens != 80 {
		t.Fatalf("message_start usage = %+v, want input=120 cache_read=4000 cache_creation=80", got)
	}

	// message_delta carries the final cumulative output_tokens at the top level.
	delta := `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":256}}`
	got = parseAnthropicUsage(delta)
	if got.OutputTokens != 256 {
		t.Fatalf("message_delta output_tokens = %d, want 256", got.OutputTokens)
	}

	// Folding both into stats should keep input/cache from start and output from delta.
	var stats claudeStreamStats
	stats.applyUpstreamUsage(parseAnthropicUsage(start))
	stats.applyUpstreamUsage(parseAnthropicUsage(delta))
	if stats.InputTokens != 120 || stats.CacheReadInputTokens != 4000 || stats.CacheCreationInputTokens != 80 {
		t.Fatalf("folded input/cache = %+v, want input=120 cache_read=4000 cache_creation=80", stats)
	}
	if stats.OutputTokens != 256 {
		t.Fatalf("folded output_tokens = %d, want 256", stats.OutputTokens)
	}
	if got := stats.realInputTokens(); got != 120+4000+80 {
		t.Fatalf("realInputTokens = %d, want %d", got, 120+4000+80)
	}

	// Malformed payload must not panic and yields a zero value.
	if got := parseAnthropicUsage("not json"); (got != anthropicUsage{}) {
		t.Fatalf("malformed payload usage = %+v, want zero", got)
	}
}

// TestClaudeMessages_CapturesRealUpstreamCacheTokens verifies that the
// cache_read / cache_creation / input tokens reported by the upstream
// message_start event are recorded against the session (visible on
// /v1/agent-sessions) rather than discarded in favor of byte estimates.
func TestClaudeMessages_CapturesRealUpstreamCacheTokens(t *testing.T) {
	t.Parallel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: message_start\n")
		_, _ = io.WriteString(w, `data: {"type":"message_start","message":{"id":"msg_1","model":"claude-sonnet-4-6","usage":{"input_tokens":50,"cache_read_input_tokens":9000,"cache_creation_input_tokens":100,"output_tokens":1}}}`+"\n\n")
		_, _ = io.WriteString(w, "event: content_block_delta\n")
		_, _ = io.WriteString(w, `data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"hi"}}`+"\n\n")
		_, _ = io.WriteString(w, "event: message_delta\n")
		_, _ = io.WriteString(w, `data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":42}}`+"\n\n")
		_, _ = io.WriteString(w, "event: message_stop\n")
		_, _ = io.WriteString(w, `data: {"type":"message_stop"}`+"\n\n")
	}))
	t.Cleanup(upstream.Close)

	p := New(&fakeGovernor{},
		WithAgentSessionStore(telemetry.NewAgentSessionStore(time.Hour)),
		WithClaudeMessages(ClaudeMessagesConfig{
			Mode:             "observe",
			DevToken:         "iq-dev-local",
			AnthropicAPIKey:  "sk-ant-test",
			AnthropicBaseURL: upstream.URL,
			AnthropicVersion: "2023-06-01",
			SessionStore:     memory.NewStore(time.Hour),
		}),
	)
	srv := httptest.NewServer(p.Handler())
	t.Cleanup(srv.Close)

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/messages", strings.NewReader(`{"model":"claude-sonnet-4-6","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer iq-dev-local")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/messages: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want 200", resp.StatusCode)
	}

	resp, err = http.Get(srv.URL + "/v1/agent-sessions")
	if err != nil {
		t.Fatalf("GET /v1/agent-sessions: %v", err)
	}
	defer resp.Body.Close()
	var body struct {
		Sessions []struct {
			CacheReadTokens     int64 `json:"cache_read_tokens"`
			CacheCreationTokens int64 `json:"cache_creation_tokens"`
			InputTokensReal     int64 `json:"input_tokens_real"`
		} `json:"sessions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.Sessions) != 1 {
		t.Fatalf("sessions len=%d, want 1", len(body.Sessions))
	}
	s := body.Sessions[0]
	if s.CacheReadTokens != 9000 {
		t.Fatalf("cache_read_tokens=%d, want 9000 (real upstream value, not byte estimate)", s.CacheReadTokens)
	}
	if s.CacheCreationTokens != 100 {
		t.Fatalf("cache_creation_tokens=%d, want 100", s.CacheCreationTokens)
	}
	if s.InputTokensReal != 50+9000+100 {
		t.Fatalf("input_tokens_real=%d, want %d", s.InputTokensReal, 50+9000+100)
	}
}

// TestClaudeMessages_PromptCachePrefixIsNotPruned verifies the optimizer does
// NOT rewrite content that sits inside Anthropic's prompt-cache prefix. Claude
// Code marks a cache_control breakpoint on the latest turn each request; pruning
// an older block before that breakpoint would invalidate the cached suffix and
// cost far more than the bytes saved. The same payload without a breakpoint is
// pruned (see TestClaudeMessages_OptimizeStillPrunesOrdinaryToolResult), so this
// isolates the cache-protection behavior.
func TestClaudeMessages_PromptCachePrefixIsNotPruned(t *testing.T) {
	t.Parallel()
	p := New(&fakeGovernor{})
	cfg := ClaudeMessagesConfig{
		Mode:                 "optimize",
		EnableBlockOptimizer: true,
		SessionStore:         memory.NewStore(time.Hour),
		Optimizer: OptimizerConfig{
			MinSpanBytes:            512,
			EnableToolResultPruning: true,
		},
	}
	// Two identical tool results (older at messages[1], newer at messages[3]).
	// The latest turn carries a cache_control breakpoint, so the whole prefix
	// up to messages[4] is cacheable and must be preserved verbatim.
	fileBody := strings.Repeat("ordinary source code output line\n", 80)
	body := []byte(fmt.Sprintf(
		`{"model":"claude-sonnet-4-6","messages":[`+
			`{"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"Read","input":{"file_path":"/repo/src/main.go"}}]},`+
			`{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":%q}]},`+
			`{"role":"assistant","content":[{"type":"tool_use","id":"t2","name":"Read","input":{"file_path":"/repo/src/main.go"}}]},`+
			`{"role":"user","content":[{"type":"tool_result","tool_use_id":"t2","content":%q}]},`+
			`{"role":"user","content":[{"type":"text","text":"latest turn","cache_control":{"type":"ephemeral"}}]}]}`,
		fileBody, fileBody,
	))

	// First request warms the session cache; second would prune the older
	// duplicate if it were not inside the cache prefix.
	if _, _, _, _, err := p.prepareClaudeBody(context.Background(), cfg, "cacheprefix-test", body); err != nil {
		t.Fatalf("first prepare: %v", err)
	}
	forward, _, stats, _, err := p.prepareClaudeBody(context.Background(), cfg, "cacheprefix-test", body)
	if err != nil {
		t.Fatalf("second prepare: %v", err)
	}

	if strings.Contains(string(forward), "omitted") {
		t.Fatalf("cached-prefix content must not be rewritten, body=%s", forward)
	}
	if stats.BlocksPruned != 0 {
		t.Fatalf("blocks_pruned=%d, want 0 (cache prefix protected); stats=%+v", stats.BlocksPruned, stats)
	}
	if stats.PreservedCachePrefixCount < 1 {
		t.Fatalf("preserved_cache_prefix_count=%d, want >=1; stats=%+v", stats.PreservedCachePrefixCount, stats)
	}
}

// TestClaudeMessages_CacheControlRequestForwardedByteIdentical verifies that when
// the client manages prompt caching, the optimizer forwards the body byte-for-byte
// — even when it contains an older duplicate tool_result that sits AFTER the cache
// breakpoint and would otherwise be pruned. Re-marshaling such a request reorders
// JSON keys in the cached prefix and busts Anthropic's cache (measured ~6x worse
// than direct), so byte fidelity must win.
func TestClaudeMessages_CacheControlRequestForwardedByteIdentical(t *testing.T) {
	t.Parallel()
	p := New(&fakeGovernor{})
	cfg := ClaudeMessagesConfig{
		Mode:                 "optimize",
		EnableBlockOptimizer: true,
		SessionStore:         memory.NewStore(time.Hour),
		Optimizer: OptimizerConfig{
			MinSpanBytes:            512,
			EnableToolResultPruning: true,
		},
	}
	// Breakpoint on messages[1]. The same tool_result content appears at messages
	// [1], [3], [5]; the copy at [3] is an older duplicate AFTER the breakpoint and
	// is NOT the last occurrence, so without the cache-fidelity short-circuit the
	// optimizer would prune it and re-marshal the whole body.
	fileBody := strings.Repeat("ordinary source code output line\n", 80)
	body := []byte(fmt.Sprintf(
		`{"model":"claude-sonnet-4-6","messages":[`+
			`{"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"Read","input":{"file_path":"/repo/a.go"}}]},`+
			`{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":%q,"cache_control":{"type":"ephemeral"}}]},`+
			`{"role":"assistant","content":[{"type":"tool_use","id":"t2","name":"Read","input":{"file_path":"/repo/a.go"}}]},`+
			`{"role":"user","content":[{"type":"tool_result","tool_use_id":"t2","content":%q}]},`+
			`{"role":"assistant","content":[{"type":"tool_use","id":"t3","name":"Read","input":{"file_path":"/repo/a.go"}}]},`+
			`{"role":"user","content":[{"type":"tool_result","tool_use_id":"t3","content":%q}]},`+
			`{"role":"user","content":[{"type":"text","text":"continue"}]}]}`,
		fileBody, fileBody, fileBody,
	))

	// Warm the session so the duplicate is "known" and would be eligible to prune.
	if _, _, _, _, err := p.prepareClaudeBody(context.Background(), cfg, "fidelity-test", body); err != nil {
		t.Fatalf("first prepare: %v", err)
	}
	forward, _, stats, _, err := p.prepareClaudeBody(context.Background(), cfg, "fidelity-test", body)
	if err != nil {
		t.Fatalf("second prepare: %v", err)
	}
	if string(forward) != string(body) {
		t.Fatalf("cache_control request must be forwarded byte-identical (no re-marshal); got:\n%s", forward)
	}
	if stats.BlocksPruned != 0 {
		t.Fatalf("blocks_pruned=%d, want 0 (cache fidelity)", stats.BlocksPruned)
	}
	if !stats.PreservedCacheFidelity {
		t.Fatalf("PreservedCacheFidelity=false, want true")
	}
}

// promptCacheCfg builds an optimize-mode config with prompt-cache injection on.
func promptCacheCfg() ClaudeMessagesConfig {
	return ClaudeMessagesConfig{
		Mode:                 "optimize",
		EnableBlockOptimizer: true,
		SessionStore:         memory.NewStore(time.Hour),
		Optimizer: OptimizerConfig{
			MinSpanBytes:      512,
			EnablePromptCache: true,
		},
	}
}

// TestPromptCache_InjectsWhenClientHasNone verifies IndexQube adds a cache_control
// breakpoint to a stable system prompt when the client supplied none.
func TestPromptCache_InjectsWhenClientHasNone(t *testing.T) {
	t.Parallel()
	p := New(&fakeGovernor{})
	cfg := promptCacheCfg()
	systemText := strings.Repeat("You are a careful assistant. ", 40) // >512B, stable
	body := []byte(fmt.Sprintf(
		`{"model":"claude-sonnet-4-6","system":%q,"messages":[{"role":"user","content":"hello"}]}`,
		systemText,
	))

	// Turn 1 warms the system span; turn 2 sees it as known/stable → injects.
	if _, _, _, _, err := p.prepareClaudeBody(context.Background(), cfg, "inject-test", body); err != nil {
		t.Fatalf("first prepare: %v", err)
	}
	forward, _, _, _, err := p.prepareClaudeBody(context.Background(), cfg, "inject-test", body)
	if err != nil {
		t.Fatalf("second prepare: %v", err)
	}
	if !strings.Contains(string(forward), "cache_control") || !strings.Contains(string(forward), "ephemeral") {
		t.Fatalf("expected injected cache_control on stable system prompt, body=%s", forward)
	}
}

// TestPromptCache_DefersWhenClientManagesCaching verifies IndexQube does NOT add
// its own cache_control when the request already carries a breakpoint (Claude Code
// manages caching). Adding another risks exceeding Anthropic's 4-breakpoint limit.
func TestPromptCache_DefersWhenClientManagesCaching(t *testing.T) {
	t.Parallel()
	p := New(&fakeGovernor{})
	cfg := promptCacheCfg()
	systemText := strings.Repeat("You are a careful assistant. ", 40)
	// The client put its own cache_control on the latest user turn; system is a
	// bare string. IndexQube must leave system alone (no second breakpoint).
	body := []byte(fmt.Sprintf(
		`{"model":"claude-sonnet-4-6","system":%q,"messages":[{"role":"user","content":[{"type":"text","text":"hello","cache_control":{"type":"ephemeral"}}]}]}`,
		systemText,
	))

	if _, _, _, _, err := p.prepareClaudeBody(context.Background(), cfg, "defer-test", body); err != nil {
		t.Fatalf("first prepare: %v", err)
	}
	forward, _, _, _, err := p.prepareClaudeBody(context.Background(), cfg, "defer-test", body)
	if err != nil {
		t.Fatalf("second prepare: %v", err)
	}
	// Exactly the one breakpoint the client sent — IndexQube added none.
	if got := strings.Count(string(forward), "cache_control"); got != 1 {
		t.Fatalf("cache_control count=%d, want 1 (client's only; no extra injected); body=%s", got, forward)
	}
}

// TestClaudeMessages_SubscriptionAuthPreservesCacheControl verifies that for
// subscription/OAuth auth (sk-ant-oat…) the proxy forwards Claude Code's
// cache_control breakpoints to Anthropic instead of stripping them. Stripping
// them disabled prompt caching entirely — Anthropic reported cache_read=0 /
// cache_creation=0 and billed full input every turn, the opposite of saving.
func TestClaudeMessages_SubscriptionAuthPreservesCacheControl(t *testing.T) {
	t.Parallel()
	bodyCh := make(chan []byte, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		bodyCh <- b
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: message_start\n")
		_, _ = io.WriteString(w, `data: {"type":"message_start","message":{"id":"msg_1","model":"claude-sonnet-4-6","usage":{"input_tokens":10,"cache_read_input_tokens":5000,"cache_creation_input_tokens":0,"output_tokens":1}}}`+"\n\n")
		_, _ = io.WriteString(w, "event: message_stop\n")
		_, _ = io.WriteString(w, `data: {"type":"message_stop"}`+"\n\n")
	}))
	t.Cleanup(upstream.Close)

	// Passthrough mode: no API key, so the subscription bearer flows upstream.
	p := New(&fakeGovernor{},
		WithClaudeMessages(ClaudeMessagesConfig{
			Mode:             "observe",
			DevToken:         "iq-dev-local",
			AnthropicBaseURL: upstream.URL,
			AnthropicVersion: "2023-06-01",
			SessionStore:     memory.NewStore(time.Hour),
		}),
	)
	srv := httptest.NewServer(p.Handler())
	t.Cleanup(srv.Close)

	// A cache_control breakpoint on the latest turn, exactly as Claude Code sends.
	reqBody := `{"model":"claude-sonnet-4-6","stream":true,"messages":[{"role":"user","content":[{"type":"text","text":"hello","cache_control":{"type":"ephemeral"}}]}]}`
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/messages", strings.NewReader(reqBody))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer sk-ant-oat-test-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/messages: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want 200", resp.StatusCode)
	}

	gotBody := <-bodyCh
	if !strings.Contains(string(gotBody), "cache_control") {
		t.Fatalf("upstream body must retain cache_control for subscription auth, got: %s", gotBody)
	}
}
