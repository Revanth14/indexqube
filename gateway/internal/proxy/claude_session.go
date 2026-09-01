package proxy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
)

// getOrCreateTurnState returns the mutable turn-state for sessionKey,
// creating it if it does not yet exist.
func (p *Proxy) getOrCreateTurnState(sessionKey string) *sessionTurnState {
	v, _ := p.sessionTurnCounters.LoadOrStore(sessionKey, &sessionTurnState{})
	p.touchSession(sessionKey)
	return v.(*sessionTurnState)
}

// getOrCreateBoilerplateState returns the mutable boilerplate-state for
// sessionKey, creating it if it does not yet exist.
func (p *Proxy) getOrCreateBoilerplateState(sessionKey string) *boilerplateState {
	v, _ := p.sessionBoilerplateState.LoadOrStore(sessionKey, &boilerplateState{})
	p.touchSession(sessionKey)
	return v.(*boilerplateState)
}

// resolveRequestID returns a non-empty request ID, assigning a synthetic one
// when the incoming ID is blank. It also updates the per-session missing-ID
// window and returns whether the session should be velocity-limited due to
// excessive missing-ID turns (FIX 3).
func (p *Proxy) resolveRequestID(sessionKey, rawID string) (id string, synthetic bool, velocityLimit bool) {
	if rawID != "" {
		ts := p.getOrCreateTurnState(sessionKey)
		ts.mu.Lock()
		ts.turnIndex++
		ts.mu.Unlock()
		return rawID, false, false
	}

	ts := p.getOrCreateTurnState(sessionKey)
	ts.mu.Lock()
	defer ts.mu.Unlock()

	ts.turnIndex++
	// FIX 1: UUID4 suffix guarantees uniqueness across sessions and restarts.
	// Previous counter-based IDs shared the same value when the session key
	// reset between iq invocations (all got suffix -1).
	keyPart := sessionKey
	if len(keyPart) > 8 {
		keyPart = keyPart[:8]
	}
	syntheticID := fmt.Sprintf("iq-synthetic-%s-%s", keyPart, uuid.New().String()[:8])

	// Track timestamp for the 60-second missing-ID window.
	now := time.Now().Unix()
	windowStart := now - 60
	// Evict entries older than 60 seconds.
	filtered := ts.missingIDWindow[:0]
	for _, t := range ts.missingIDWindow {
		if t >= windowStart {
			filtered = append(filtered, t)
		}
	}
	filtered = append(filtered, now)
	ts.missingIDWindow = filtered

	p.logger.Warn("request arrived with empty request ID; assigned synthetic",
		slog.String("synthetic_request_id", syntheticID),
		slog.String("session_key", shortLogHash(sessionKey)),
		slog.Int("missing_id_window_count", len(filtered)),
	)

	vLimit := len(filtered) > 3
	return syntheticID, true, vLimit
}

func claudeSessionKey(r *http.Request, fallback string) string {
	if sk := strings.TrimSpace(r.Header.Get(headerSessionKey)); sk != "" {
		return sk
	}
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if auth != "" {
		sum := sha256.Sum256([]byte(auth))
		key := hex.EncodeToString(sum[:8])
		// Suffix with the per-invocation session ID so the circuit breaker
		// scopes similar-request counts to this iq session, not across sessions.
		if sid := os.Getenv("IQ_SESSION_ID"); sid != "" {
			if len(sid) > 8 {
				sid = sid[:8]
			}
			return key + "-" + sid
		}
		return key
	}
	sum := sha256.Sum256([]byte(fallback))
	return hex.EncodeToString(sum[:8])
}

// semanticPromptHash returns a 128-bit hex digest keyed on the content of
// the last 3 user messages plus the first 64 bytes of the system field.
// Raw-body hashing fails in-flight deduplication because the same logical
// prompt produces different bytes each turn as the context window grows.
// Falls back to a raw SHA-256 if the body cannot be parsed.
func semanticPromptHash(body []byte) string {
	var parsed struct {
		System   json.RawMessage `json:"system"`
		Messages []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		sum := sha256.Sum256(body)
		return hex.EncodeToString(sum[:16])
	}

	// System fingerprint: first 64 bytes of the string-form system content.
	var sysFP string
	var sysStr string
	if err := json.Unmarshal(parsed.System, &sysStr); err == nil {
		if len(sysStr) > 64 {
			sysStr = sysStr[:64]
		}
		sysFP = sysStr
	} else {
		var sysArr []map[string]any
		if err2 := json.Unmarshal(parsed.System, &sysArr); err2 == nil && len(sysArr) > 0 {
			if text, ok := sysArr[0]["text"].(string); ok {
				if len(text) > 64 {
					text = text[:64]
				}
				sysFP = text
			}
		}
	}

	// Collect last 3 user-message contents.
	var userContents []string
	for _, msg := range parsed.Messages {
		if !strings.EqualFold(msg.Role, "user") {
			continue
		}
		var text string
		if err := json.Unmarshal(msg.Content, &text); err == nil {
			userContents = append(userContents, text)
		} else {
			var blocks []map[string]any
			if err2 := json.Unmarshal(msg.Content, &blocks); err2 == nil {
				var sb strings.Builder
				for _, b := range blocks {
					appendText(&sb, b["text"])
					appendText(&sb, b["content"])
				}
				userContents = append(userContents, sb.String())
			}
		}
	}
	if len(userContents) > 3 {
		userContents = userContents[len(userContents)-3:]
	}

	h := sha256.New()
	for _, c := range userContents {
		h.Write([]byte(c))
	}
	h.Write([]byte(sysFP))
	return hex.EncodeToString(h.Sum(nil)[:16])
}

// getOrCreatePrefixHints returns the prefix-hint set for sessionKey, creating
// it if it does not yet exist (FIX 3).
func (p *Proxy) getOrCreatePrefixHints(sessionKey string) *prefixHintSet {
	v, _ := p.sessionPrefixHints.LoadOrStore(sessionKey, &prefixHintSet{
		hints: make(map[string]int),
	})
	p.touchSession(sessionKey)
	return v.(*prefixHintSet)
}

func (s *prefixHintSet) add(hash string, length int) {
	s.mu.Lock()
	s.hints[hash] = length
	s.mu.Unlock()
}

// matchPrefix returns the length and hash of the longest registered small
// chunk whose content matches data[0:length], or 0 if none match (FIX 3).
func (s *prefixHintSet) matchPrefix(data []byte) (matchedHash string, matchedLen int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for h, l := range s.hints {
		if l <= 0 || l > len(data) {
			continue
		}
		sum := sha256.Sum256([]byte(strings.TrimSpace(string(data[:l]))))
		if hex.EncodeToString(sum[:]) == h && l > matchedLen {
			matchedLen = l
			matchedHash = h
		}
	}
	return
}
