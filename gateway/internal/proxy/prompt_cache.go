package proxy

// injectPromptCacheHeaders adds Anthropic's cache_control: {type:"ephemeral"}
// marker to the last system content block. When the system prompt is identical
// to the prior turn, Anthropic's server-side prompt cache covers the stable
// prefix and only charges cache-read tokens (≈10% of normal input token cost)
// for all subsequent turns that share the same prefix.
//
// Call this only when the system spans are all "known" (seen in the session
// store), which indicates the system prompt has not changed since the last turn.
func injectPromptCacheHeaders(root map[string]any) {
	sys, ok := root["system"]
	if !ok {
		return
	}
	cacheCtrl := map[string]any{"type": "ephemeral"}
	switch v := sys.(type) {
	case string:
		if len(v) == 0 {
			return
		}
		// Promote string system to array form so we can attach cache_control.
		root["system"] = []any{
			map[string]any{
				"type":          "text",
				"text":          v,
				"cache_control": cacheCtrl,
			},
		}
	case []any:
		if len(v) == 0 {
			return
		}
		last, ok := v[len(v)-1].(map[string]any)
		if !ok {
			return
		}
		last["cache_control"] = cacheCtrl
	}
}
