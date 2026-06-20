package proxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/chunker"
	"github.com/Revanth14/indexqube/gateway/internal/memory"
)

// guardBypassRe matches directives that attempt to disable proxy safety controls.
// The separator between "guards" and "velocity" is optional (covers slash, pipe,
// backslash, or none) to handle formatting variations in injected CLAUDE.md files.
var guardBypassRe = regexp.MustCompile(`(?i)guards?\s*[/\\|]?\s*velocity\s+limits?\s+do\s+not\s+apply`)

// stripGuardBypassDirectives scans the system field of root for patterns
// that attempt to disable proxy-level guards (e.g. from CLAUDE.md content).
// Any matched text is replaced with an empty string in-place. Returns true
// if any directives were stripped so the caller can log a warning.
func stripGuardBypassDirectives(ctx context.Context, logger *slog.Logger, root map[string]any) bool {
	stripped := false
	stripText := func(text string) (string, bool) {
		// Check the original text first; if that misses (e.g. newlines inside the
		// directive from a system-reminder block), fall back to a whitespace-normalised
		// copy so multi-line formatting doesn't hide the bypass attempt.
		matched := guardBypassRe.MatchString(text)
		if !matched {
			normalized := strings.ToLower(strings.Join(strings.Fields(text), " "))
			matched = guardBypassRe.MatchString(normalized)
		}
		if !matched {
			return text, false
		}
		return guardBypassRe.ReplaceAllString(text, ""), true
	}

	switch sys := root["system"].(type) {
	case string:
		if cleaned, ok := stripText(sys); ok {
			root["system"] = cleaned
			stripped = true
		}
	case []any:
		for _, item := range sys {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			if text, ok2 := m["text"].(string); ok2 {
				if cleaned, ok3 := stripText(text); ok3 {
					m["text"] = cleaned
					stripped = true
				}
			}
		}
	}

	if stripped {
		logger.WarnContext(ctx, "guard bypass attempt detected in system content",
			slog.String("event", "guard_bypass_attempt"),
			slog.String("source", "CLAUDE.md"),
		)
	}
	return stripped
}

// warmUpSystemSpans pre-registers all system-block fingerprints into the
// session cache on the first turn. This ensures that stable injected files
// (CLAUDE.md, MEMORY.md, architecture docs) are cached from turn 1 so that
// turns 2+ see them as known and report non-zero savings (FIX 6).
func (p *Proxy) warmUpSystemSpans(ctx context.Context, cfg ClaudeMessagesConfig, sessionKey string, root map[string]any) {
	registerText := func(text, kind string) {
		text = strings.TrimSpace(text)
		if len(text) < cfg.Optimizer.MinSpanBytes {
			return
		}
		data := []byte(text)
		if cfg.Optimizer.EnableSubspanChunking && len(data) >= cfg.Optimizer.SmallFileBytes {
			chunks := splitSpanChunks(text, cfg.Optimizer)
			for _, ch := range chunks {
				chHash := hashText(ch)
				cfg.SessionStore.SaveBlock(sessionKey, memory.Block{
					Hash:   chHash,
					Kind:   "warmup:chunk:" + kind,
					Bytes:  len(ch),
					Tokens: estimateTokens(len(ch)),
				})
			}
		} else {
			h := hashText(text)
			cfg.SessionStore.SaveBlock(sessionKey, memory.Block{
				Hash:   h,
				Kind:   "warmup:" + kind,
				Bytes:  len(data),
				Tokens: estimateTokens(len(data)),
			})
		}
	}

	switch sys := root["system"].(type) {
	case string:
		registerText(sys, "system")
	case []any:
		for _, item := range sys {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			if text, ok2 := m["text"].(string); ok2 {
				registerText(text, "system_block")
			}
		}
	}
	p.logger.DebugContext(ctx, "session warm-up complete",
		slog.String("session_key", shortLogHash(sessionKey)),
	)
}

func (p *Proxy) prepareClaudeBody(ctx context.Context, cfg ClaudeMessagesConfig, sessionKey string, body []byte) ([]byte, anthropicMessagesRequest, claudeOptimizerStats, claudeRequestShape, error) {
	if err := ctx.Err(); err != nil {
		return nil, anthropicMessagesRequest{}, claudeOptimizerStats{}, claudeRequestShape{}, err
	}
	var root map[string]any
	if err := json.Unmarshal(body, &root); err != nil {
		return nil, anthropicMessagesRequest{}, claudeOptimizerStats{}, claudeRequestShape{}, fmt.Errorf("parse anthropic messages body: %w", err)
	}

	// FIX 4: strip guard-disable directives injected via CLAUDE.md before any
	// optimization or forwarding. Proxy-level guards are always enforced.
	if stripGuardBypassDirectives(ctx, p.logger, root) {
		// Re-serialize so the cleaned body is what gets forwarded and re-parsed.
		if cleaned, err := marshalJSONNoHTMLEscape(root); err == nil {
			body = cleaned
		}
	}
	req := anthropicMessagesRequest{}
	req.Model, _ = root["model"].(string)
	req.Stream, _ = root["stream"].(bool)
	shape := extractClaudeRequestShape(root)
	stats := claudeOptimizerStats{
		BytesBefore:           len(body),
		BytesAfter:            len(body),
		EstimatedTokensBefore: estimateTokens(len(body)),
		EstimatedTokensAfter:  estimateTokens(len(body)),
	}
	if cfg.SessionStore == nil {
		return body, req, stats, shape, nil
	}

	// FIX 6: Session-open cache warm-up. On the first actual turn with a system prompt,
	// pre-fingerprint all system blocks (CLAUDE.md, MEMORY.md, architecture
	// files) so that subsequent turns start with cache hits rather than
	// zero-savings. Uses a stable chunk ID: sha256(file_path + content_hash).
	if root["system"] != nil {
		_, alreadyWarmed := p.sessionWarmUpDone.LoadOrStore(sessionKey, true)
		p.touchSession(sessionKey)
		if !alreadyWarmed {
			p.warmUpSystemSpans(ctx, cfg, sessionKey, root)
		}
	}

	// FIX 7: suggestion-mode payloads are ephemeral harness meta-prompts.
	// Registering their large mixed content in the chunk store pollutes it and
	// doubles per-turn cost. Rate-limit to 1 per 10 seconds; always skip chunk
	// registration regardless of rate-limit status.
	if strings.HasPrefix(shape.LatestUserText, "SUGGESTION MODE:") {
		now := time.Now()
		if last, ok := p.sessionSuggestionTs.Load(sessionKey); ok && now.Sub(last.(time.Time)) < 10*time.Second {
			p.logger.DebugContext(ctx, "suggestion-mode request rate-limited; skipping",
				slog.String("session_key", shortLogHash(sessionKey)))
		} else {
			p.sessionSuggestionTs.Store(sessionKey, now)
			p.touchSession(sessionKey)
		}
		return body, req, stats, shape, nil
	}

	minSpanBytes := cfg.Optimizer.MinSpanBytes
	if minSpanBytes <= 0 {
		minSpanBytes = 512
	}

	spans := extractSpans(root)

	// In observe mode or with optimizer disabled, just warm the session store.
	if cfg.Mode == "observe" || !cfg.EnableBlockOptimizer {
		for _, span := range spans {
			if span.Bytes >= minSpanBytes {
				cfg.SessionStore.SaveBlock(sessionKey, memory.Block{
					Hash:   span.Hash,
					Kind:   span.Class,
					Bytes:  span.Bytes,
					Tokens: span.Tokens,
				})
			} else if span.Bytes > 0 {
				// FIX 4: register small spans so they are tracked across turns.
				cfg.SessionStore.SaveBlock(sessionKey, memory.Block{
					Hash:   span.Hash,
					Kind:   span.Class + ":small",
					Bytes:  span.Bytes,
					Tokens: span.Tokens,
				})
			}
		}
		return body, req, stats, shape, nil
	}

	// optimize / dry_run: full span accounting and class-aware pruning.
	stats.ClassBytesSeen = make(map[string]int)
	stats.ClassBytesEligible = make(map[string]int)
	stats.ClassBytesPruned = make(map[string]int)
	stats.ClassSpansSeen = make(map[string]int)
	stats.ClassSpansPruned = make(map[string]int)

	// Pre-pass: for hashes that appear MORE THAN ONCE in this request,
	// track the highest message index. When the same file was read multiple
	// times we preserve the newest copy and prune the older duplicates.
	toolResultHashCount := make(map[string]int)
	lastToolResultMsg := make(map[string]int)
	for _, span := range spans {
		if span.Class == SpanClassToolResultOld || span.Class == SpanClassToolResultLatest {
			toolResultHashCount[span.Hash]++
			if existing, ok := lastToolResultMsg[span.Hash]; !ok || span.MessageIndex > existing {
				lastToolResultMsg[span.Hash] = span.MessageIndex
			}
		}
	}

	// FIX 7: per-session boilerplate state for injection cooldown.
	bpState := p.getOrCreateBoilerplateState(sessionKey)
	ts := p.getOrCreateTurnState(sessionKey)
	ts.mu.Lock()
	currentTurn := ts.turnIndex
	currentCtxBytes := ts.contextBytesCumulative + int64(len(body))
	ts.contextBytesCumulative = currentCtxBytes
	ts.mu.Unlock()

	var prunableSpans []TextSpan
	// systemAllKnown tracks whether every system/boilerplate span was already
	// seen in the session store, indicating a stable system prompt this turn.
	systemAllKnown := true
	hasSystemSpan := false

	// Prompt-cache protection: Claude Code marks a rolling cache_control breakpoint
	// on the latest user turn (and the system prompt) every request, so the prefix
	// up to that breakpoint is served by Anthropic at cache-read price on the next
	// turn. Rewriting any span inside that prefix invalidates the cache for the
	// entire suffix — far more expensive than the bytes a prune would save. When
	// the request uses prompt caching we therefore preserve cached-prefix spans.
	cachePrefixMsgIdx := lastCacheControlMessageIndex(root)
	systemCached := contentHasCacheControl(root["system"])
	spanInCachedPrefix := func(span TextSpan) bool {
		if span.Role == "system" {
			return systemCached
		}
		return cachePrefixMsgIdx >= 0 && span.MessageIndex >= 0 && span.MessageIndex <= cachePrefixMsgIdx
	}

	for _, span := range spans {
		if span.Bytes < minSpanBytes {
			// FIX 4 & SQLite Optimization: register small spans in the session cache
			// only if they are >= 256 bytes to prevent SQLite write-amplification.
			if cfg.SessionStore != nil && span.Bytes >= 256 {
				if cfg.SessionStore.Seen(sessionKey, span.Hash) {
					stats.BlocksKnown++
				} else {
					cfg.SessionStore.SaveBlock(sessionKey, memory.Block{
						Hash:   span.Hash,
						Kind:   span.Class + ":small",
						Bytes:  span.Bytes,
						Tokens: span.Tokens,
					})
					// FIX 3: register small span in the prefix-hint registry so
					// larger spans can detect it as a known prefix.
					p.getOrCreatePrefixHints(sessionKey).add(span.Hash, span.Bytes)
					stats.BlocksNew++
				}
			}
			stats.PreservedSmallBytes += span.Bytes
			stats.PreservedSmallCount++
			continue
		}

		stats.BlocksSeen++
		stats.ClassBytesSeen[span.Class] += span.Bytes
		stats.ClassSpansSeen[span.Class]++
		if span.Bytes > stats.LargestSpanBytes {
			stats.LargestSpanBytes = span.Bytes
		}

		// Track system prompt stability for Anthropic prompt-cache injection.
		if span.Class == SpanClassSystemText || span.Class == SpanClassSystemBoilerplate {
			hasSystemSpan = true
		}

		// Sub-span chunking: for large tool_result spans, deduplicate at the
		// individual chunk level so that edits to one section of a file do not
		// invalidate the hashes of unchanged sections in adjacent chunks.
		var known bool
		if cfg.Optimizer.EnableSubspanChunking &&
			(span.Class == SpanClassToolResultOld || span.Class == SpanClassToolResultLatest) &&
			span.Bytes >= cfg.Optimizer.SmallFileBytes {

			// FIX 3: prefix-chunk reuse. Check whether the span's content
			// starts with a previously registered small chunk. If so, count
			// those prefix bytes as known before splitting the remainder.
			phints := p.getOrCreatePrefixHints(sessionKey)
			if _, prefixLen := phints.matchPrefix([]byte(span.Text)); prefixLen > 0 {
				// The first prefixLen bytes match a known small chunk —
				// mark the prefix as a cache hit in the session store
				// (already registered, so Seen returns true next turn).
				// We do not prune the prefix bytes here; prefix pruning
				// is handled at the whole-span level below.
				stats.BlocksKnown++ // credit the prefix as a known block
			}

			chunks := splitSpanChunks(span.Text, cfg.Optimizer)
			allKnown := true
			for _, ch := range chunks {
				chHash := hashText(ch)
				chKnown := cfg.SessionStore.Seen(sessionKey, chHash)
				cfg.SessionStore.SaveBlock(sessionKey, memory.Block{
					Hash:   chHash,
					Kind:   "chunk:" + span.Class,
					Bytes:  len(ch),
					Tokens: estimateTokens(len(ch)),
				})
				if !chKnown {
					allKnown = false
				}
			}
			known = allKnown
		} else {
			known = cfg.SessionStore.Seen(sessionKey, span.Hash)
			cfg.SessionStore.SaveBlock(sessionKey, memory.Block{
				Hash:   span.Hash,
				Kind:   span.Class,
				Bytes:  span.Bytes,
				Tokens: span.Tokens,
			})
		}

		if !known {
			if span.Class == SpanClassSystemText || span.Class == SpanClassSystemBoilerplate {
				systemAllKnown = false
			}

			// Protected content must win even when the span is a new boilerplate
			// variant. This check intentionally runs before boilerplate cooldown
			// pruning so instruction files, credentials, and the latest turn are
			// never removed just because they live inside a repeated harness block.
			if span.IsLatestTurn {
				stats.PreservedLatestTurnBytes += span.Bytes
				stats.PreservedLatestTurnCount++
				stats.BlocksNew++
				continue
			}
			if isProtectedInstructionSpan(span) {
				stats.PreservedInstructionBytes += span.Bytes
				stats.PreservedInstructionCount++
				stats.BlocksNew++
				continue
			}

			// Even a new span must not be rewritten if it sits inside the prompt-cache
			// prefix — rewriting it breaks Anthropic's cached suffix for this turn.
			if spanInCachedPrefix(span) {
				stats.PreservedCachePrefixBytes += span.Bytes
				stats.PreservedCachePrefixCount++
				stats.BlocksNew++
				continue
			}

			// FIX 7: SUGGESTION MODE injection cooldown. If a boilerplate span
			// is new (unknown hash) but the last forward was <5 turns ago AND
			// context delta is <10 KB, prune it anyway to suppress redundant
			// harness meta-prompt injections.
			if span.Class == SpanClassSystemBoilerplate {
				bpState.mu.Lock()
				turnsSinceLast := currentTurn - bpState.lastForwardedTurn
				bytesSinceLast := currentCtxBytes - int64(bpState.lastForwardedCtxBytes)
				inCooldown := bpState.lastForwardedTurn > 0 && turnsSinceLast < 5 && bytesSinceLast < 10240
				if !inCooldown {
					bpState.lastForwardedTurn = currentTurn
					bpState.lastForwardedCtxBytes = int(currentCtxBytes)
				}
				bpState.mu.Unlock()

				if inCooldown && isEligibleSpanClass(span.Class, cfg.Optimizer) {
					p.logger.DebugContext(ctx, "suppressing boilerplate injection (cooldown active)",
						slog.String("session_key", shortLogHash(sessionKey)),
						slog.Int("turns_since_last", turnsSinceLast),
					)
					stats.ClassBytesEligible[span.Class] += span.Bytes
					stats.BytesEligible += span.Bytes
					prunableSpans = append(prunableSpans, span)
					continue
				}
			}

			stats.BlocksNew++
			continue
		}
		stats.BlocksKnown++
		// FIX B: credit the cache hit in bytes regardless of any downstream
		// preserve-vs-prune decision, so the audit metric reflects true cache
		// efficiency (including protected instruction files like CLAUDE.md
		// that hit the cache but are intentionally never pruned).
		stats.KnownBytes += span.Bytes

		// Latest-turn spans must never be pruned regardless of whether the
		// content was seen before. The model needs current-turn context intact.
		if span.IsLatestTurn {
			stats.PreservedLatestTurnBytes += span.Bytes
			stats.PreservedLatestTurnCount++
			continue
		}

		// Instruction files (CLAUDE.md, CONTEXT.md, .cursorrules, etc.) must
		// NEVER be pruned regardless of span class. This check runs before
		// the eligibility gate so user_text_old spans containing instruction
		// content are caught — previously they fell through to pruning because
		// isEligibleSpanClass("user_text_old") returns true unconditionally.
		if isProtectedInstructionSpan(span) {
			stats.PreservedInstructionBytes += span.Bytes
			stats.PreservedInstructionCount++
			continue
		}

		// When the same tool result content appears more than once in this
		// request (the model re-read the same file), preserve the newest copy
		// so the model always has one copy. Older duplicates fall through to
		// be pruned. For single-occurrence tool results the readable replacement
		// text is self-explanatory enough that the model won't re-invoke.
		if span.Class == SpanClassToolResultOld && toolResultHashCount[span.Hash] > 1 {
			if lastMsg := lastToolResultMsg[span.Hash]; span.MessageIndex == lastMsg {
				stats.PreservedLastOccurrenceBytes += span.Bytes
				stats.PreservedLastOccurrenceCount++
				continue
			}
		}

		// Never rewrite content inside Anthropic's prompt-cache prefix; doing so
		// invalidates the cached suffix and costs more than the prune saves.
		if spanInCachedPrefix(span) {
			stats.PreservedCachePrefixBytes += span.Bytes
			stats.PreservedCachePrefixCount++
			continue
		}

		if !isEligibleSpanClass(span.Class, cfg.Optimizer) {
			switch span.Class {
			case SpanClassSystemText:
				stats.PreservedSystemBytes += span.Bytes
				stats.PreservedSystemCount++
			case SpanClassToolUse:
				stats.PreservedToolUseBytes += span.Bytes
				stats.PreservedToolUseCount++
			}
			continue
		}

		stats.ClassBytesEligible[span.Class] += span.Bytes
		stats.BytesEligible += span.Bytes
		prunableSpans = append(prunableSpans, span)
	}

	// Only inject our own cache_control when the client did NOT already manage
	// prompt caching. Claude Code places its own breakpoints (system + rolling
	// latest turn) on every request; adding another risks exceeding Anthropic's
	// 4-breakpoint limit (a 400 for any user, not just subscription) and is
	// redundant. Deferring to the client is the correct generalization of the old
	// blunt "skip for subscription" rule — subscription users keep their cache
	// benefit via Claude Code's own breakpoints, which E2 now preserves.
	requestHasCacheControl := systemCached || cachePrefixMsgIdx >= 0

	// Cache fidelity beats pruning. When the client manages prompt caching — Claude
	// Code sets a rolling cache_control breakpoint every turn — forward the body
	// byte-for-byte. Any rewrite here re-serializes the whole request (Go sorts JSON
	// map keys), changing the bytes of the cached prefix even when no prefix content
	// is pruned. Anthropic then misses the cache every turn, turning 0.1x cache reads
	// into 1.25x writes. `iq bench` measured this at ~6x more expensive than going
	// direct (optimize 0% hit vs observe 91%). The span loop above already recorded
	// blocks for dedup, and redundant-call elimination runs upstream of this; pruning
	// is only safe to attempt when the client is NOT caching.
	if requestHasCacheControl {
		stats.PreservedCacheFidelity = true
		return body, req, stats, shape, nil
	}

	wantPromptCache := cfg.Optimizer.EnablePromptCache && hasSystemSpan && systemAllKnown && !cfg.Bedrock.Enabled && !requestHasCacheControl
	if len(prunableSpans) == 0 && !wantPromptCache {
		return body, req, stats, shape, nil
	}

	// Rewrite on a fresh parse so the original stays intact for dry_run.
	var rewriteRoot map[string]any
	if err := json.Unmarshal(body, &rewriteRoot); err != nil {
		p.logger.WarnContext(ctx, "claude optimize re-parse failed; forwarding original", slog.Any("err", err))
		return body, req, stats, shape, nil
	}

	pruneCount := 0
	for _, span := range prunableSpans {
		replacement := classSpecificReplacement(span)
		if err := setSpanText(rewriteRoot, span, replacement); err != nil {
			p.logger.WarnContext(ctx, "span replacement failed", slog.String("path", span.Path), slog.Any("err", err))
			continue
		}
		pruneCount++
		stats.ClassBytesPruned[span.Class] += span.Bytes
		stats.ClassSpansPruned[span.Class]++
		stats.BytesPruned += span.Bytes
		if span.Bytes > stats.LargestPrunedBytes {
			stats.LargestPrunedBytes = span.Bytes
		}
	}

	// Inject Anthropic server-side prompt cache header when system prompt is stable.
	if wantPromptCache {
		injectPromptCacheHeaders(rewriteRoot)
	}

	optimized, err := marshalJSONNoHTMLEscape(rewriteRoot)
	if err != nil {
		p.logger.WarnContext(ctx, "claude optimize marshal failed; forwarding original", slog.Any("err", err))
		return body, req, stats, shape, nil
	}

	stats.BlocksPruned = pruneCount
	stats.BytesAfter = len(optimized)
	stats.EstimatedTokensAfter = estimateTokens(len(optimized))
	stats.EstimatedTokensSaved = max(0, stats.EstimatedTokensBefore-stats.EstimatedTokensAfter)
	if stats.BytesBefore > 0 {
		stats.ReductionRatio = float64(stats.BytesBefore-stats.BytesAfter) / float64(stats.BytesBefore)
	}

	// FIX 2: synchronous warm-up flush at the end of prepareClaudeBody when Turn is 1 or 2 (turn_index == 0 in 0-based auditing)
	if (currentTurn == 1 || currentTurn == 2) && root["system"] != nil {
		p.warmUpSystemSpans(ctx, cfg, sessionKey, root)
	}

	if cfg.Mode == "optimize" {
		return optimized, req, stats, shape, nil
	}
	// dry_run: report accurate stats but forward the original body.
	return body, req, stats, shape, nil
}

// isEligibleSpanClass returns true if the span class is eligible for pruning
// under the given optimizer config.
func isEligibleSpanClass(class string, cfg OptimizerConfig) bool {
	switch class {
	case SpanClassUserTextOld:
		return true
	case SpanClassToolResultOld:
		return cfg.EnableToolResultPruning
	case SpanClassAssistantTextOld:
		return cfg.EnableAssistantPruning
	case SpanClassSystemBoilerplate:
		return true // harness meta-prompts are prunable after first occurrence
	case SpanClassSystemText:
		return false
	default:
		return false
	}
}

var protectedInstructionPathFragments = [...]string{
	"claude.md",
	"context.md",
	"agents.md",
	".cursorrules",
	".cursor/rules/",
	".github/copilot-instructions.md",
}

// contentHasCacheControl reports whether an Anthropic content value (an array of
// content blocks) carries a cache_control breakpoint on any block.
func contentHasCacheControl(content any) bool {
	arr, ok := content.([]any)
	if !ok {
		return false
	}
	for _, raw := range arr {
		b, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if _, has := b["cache_control"]; has {
			return true
		}
	}
	return false
}

// lastCacheControlMessageIndex returns the highest message index whose content
// carries a cache_control breakpoint, or -1 if none. Claude Code places a
// rolling breakpoint on the latest user turn each request, so this marks the end
// of the prompt-cache prefix that Anthropic will serve at cache-read price on the
// next turn. Content at or before this index must not be rewritten.
func lastCacheControlMessageIndex(root map[string]any) int {
	msgs, ok := root["messages"].([]any)
	if !ok {
		return -1
	}
	last := -1
	for i, raw := range msgs {
		m, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if contentHasCacheControl(m["content"]) {
			last = i
		}
	}
	return last
}

func isProtectedInstructionSpan(span TextSpan) bool {
	return containsProtectedInstructionPath(span.SourcePath) || containsProtectedInstructionPath(span.Text) || containsCredentialMarker(span.Text)
}

func containsCredentialMarker(s string) bool {
	lower := strings.ToLower(s)
	return strings.Contains(lower, "api-key") ||
		strings.Contains(lower, "bearer ") ||
		strings.Contains(lower, "x-anthropic-api-key") ||
		strings.Contains(lower, "authorization")
}

func containsProtectedInstructionPath(s string) bool {
	s = strings.ToLower(strings.ReplaceAll(s, "\\", "/"))
	if strings.TrimSpace(s) == "" {
		return false
	}
	for _, fragment := range protectedInstructionPathFragments {
		if strings.Contains(s, fragment) {
			return true
		}
	}
	return false
}

// classSpecificReplacement returns a human-readable placeholder for a pruned
// span. The text must be self-explanatory so the model does not re-invoke the
// tool to retrieve content it has already processed.
func classSpecificReplacement(span TextSpan) string {
	if span.BlockType == "tool_result" {
		if span.SourcePath != "" {
			return "[Content of " + span.SourcePath + " was read here — omitted to save context]"
		}
		return "[Tool result content omitted — already processed in this session]"
	}
	return "[Content omitted — already processed in this session]"
}

// setSpanText navigates to the span's location in root and replaces its text
// value with replacement. Returns an error if the navigation fails.
func setSpanText(root map[string]any, span TextSpan, replacement string) error {
	if span.Role == "system" {
		return setSystemSpanText(root, span, replacement)
	}
	messages, ok := root["messages"].([]any)
	if !ok || span.MessageIndex < 0 || span.MessageIndex >= len(messages) {
		return fmt.Errorf("invalid message index %d", span.MessageIndex)
	}
	msg, ok := messages[span.MessageIndex].(map[string]any)
	if !ok {
		return fmt.Errorf("message[%d] is not a map", span.MessageIndex)
	}
	if span.ContentIndex < 0 {
		msg["content"] = replacement
		return nil
	}
	content, ok := msg["content"].([]any)
	if !ok || span.ContentIndex >= len(content) {
		return fmt.Errorf("invalid content index %d", span.ContentIndex)
	}
	item, ok := content[span.ContentIndex].(map[string]any)
	if !ok {
		return fmt.Errorf("content[%d] is not a map", span.ContentIndex)
	}
	switch span.BlockType {
	case "text":
		item["text"] = replacement
	case "tool_result":
		if span.SubContentIndex < 0 {
			item["content"] = replacement
		} else {
			sub, ok := item["content"].([]any)
			if !ok || span.SubContentIndex >= len(sub) {
				return fmt.Errorf("invalid tool_result sub-content index %d", span.SubContentIndex)
			}
			subItem, ok := sub[span.SubContentIndex].(map[string]any)
			if !ok {
				return fmt.Errorf("tool_result content[%d] is not a map", span.SubContentIndex)
			}
			subItem["text"] = replacement
		}
	default:
		return fmt.Errorf("unsupported block type %q for span replacement", span.BlockType)
	}
	return nil
}

func setSystemSpanText(root map[string]any, span TextSpan, replacement string) error {
	if span.ContentIndex < 0 {
		root["system"] = replacement
		return nil
	}
	sysArr, ok := root["system"].([]any)
	if !ok || span.ContentIndex >= len(sysArr) {
		return fmt.Errorf("invalid system content index %d", span.ContentIndex)
	}
	item, ok := sysArr[span.ContentIndex].(map[string]any)
	if !ok {
		return fmt.Errorf("system[%d] is not a map", span.ContentIndex)
	}
	item["text"] = replacement
	return nil
}

// splitSpanChunks splits span text into content-defined chunks using Rabin-Karp
// CDC. For code content a wider 256-byte window is selected automatically so
// that edits within one function shift fewer adjacent chunk boundaries.
// Content shorter than cfg.SmallFileBytes is returned as a single element.
func splitSpanChunks(text string, cfg OptimizerConfig) []string {
	smallBytes := cfg.SmallFileBytes
	if smallBytes <= 0 {
		smallBytes = 4096
	}
	data := []byte(text)
	if len(data) < smallBytes {
		return []string{text}
	}
	var ckCfg chunker.Config
	if len(data) >= 8192 && chunker.IsCodeContent(data) {
		// FIX 5: 256-byte window for large code files (≥8 KB with declaration-keyword
		// density) to produce fewer, more stable chunk boundaries that survive
		// minor edits without shifting fingerprints in adjacent functions.
		ckCfg = chunker.CodeConfig()
	} else if chunker.IsSystemProseContent(data) {
		// FIX 4: 4 KB MaxSize for XML-tagged system-reminder blocks so a single
		// changed field (e.g. currentDate) only invalidates its own small chunk.
		ckCfg = chunker.SystemProseConfig()
	} else if chunker.IsPathList(data) {
		// FIX 7: narrow 32-byte window for file-path listings so per-path changes
		// produce localised boundary shifts instead of cascading re-fingerprints.
		ckCfg = chunker.PathListConfig()
	} else {
		ckCfg = chunker.DefaultConfig()
	}
	if cfg.MaxChunkBytes > 0 {
		ckCfg.MaxSize = cfg.MaxChunkBytes
	}
	ck := chunker.New(ckCfg)
	raw := ck.Split(data)
	if len(raw) == 0 {
		return []string{text}
	}
	result := make([]string, len(raw))
	for i, ch := range raw {
		result[i] = string(ch.Data)
	}
	return result
}

func hashText(text string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(text)))
	return hex.EncodeToString(sum[:])
}
