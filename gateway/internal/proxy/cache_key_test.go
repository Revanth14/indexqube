package proxy

import (
	"testing"
)

// TestComputePromptHashContextAware verifies that the response replay cache
// key includes tool-result context, not just the latest user text.
func TestComputePromptHashContextAware(t *testing.T) {
	// Two requests with the same latest user text but different prior
	// tool-result content.
	bodyA := []byte(`{
		"model": "claude-3",
		"messages": [
			{"role": "user", "content": "hello"},
			{"role": "user", "content": [{"type": "tool_result", "content": "data-from-file-A"}]}
		]
	}`)
	bodyB := []byte(`{
		"model": "claude-3",
		"messages": [
			{"role": "user", "content": "hello"},
			{"role": "user", "content": [{"type": "tool_result", "content": "data-from-file-B"}]}
		]
	}`)

	hA := computePromptHash(bodyA, "claude-3")
	hB := computePromptHash(bodyB, "claude-3")

	if hA == hB {
		t.Fatalf("expected different cache keys for different tool results, got %q and %q", hA, hB)
	}

	// Same request should produce the same hash.
	hA2 := computePromptHash(bodyA, "claude-3")
	if hA != hA2 {
		t.Fatalf("expected same cache key for identical request, got %q and %q", hA, hA2)
	}
}

func TestComputePromptHashUsesFullRequestBeyondSemanticWindow(t *testing.T) {
	bodyA := []byte(`{
		"model": "claude-3",
		"messages": [
			{"role": "user", "content": "old context A"},
			{"role": "user", "content": "same recent 1"},
			{"role": "user", "content": "same recent 2"},
			{"role": "user", "content": "same recent 3"}
		]
	}`)
	bodyB := []byte(`{
		"model": "claude-3",
		"messages": [
			{"role": "user", "content": "old context B"},
			{"role": "user", "content": "same recent 1"},
			{"role": "user", "content": "same recent 2"},
			{"role": "user", "content": "same recent 3"}
		]
	}`)

	if semanticPromptHash(bodyA) != semanticPromptHash(bodyB) {
		t.Fatal("test setup expected semantic hash to ignore the older context")
	}
	if computePromptHash(bodyA, "claude-3") == computePromptHash(bodyB, "claude-3") {
		t.Fatal("response replay hash must include older context outside the semantic dedupe window")
	}
}

func TestComputePromptHashCanonicalizesJSONKeyOrder(t *testing.T) {
	bodyA := []byte(`{"model":"claude-3","messages":[{"role":"user","content":"hello"}]}`)
	bodyB := []byte(`{"messages":[{"content":"hello","role":"user"}],"model":"claude-3"}`)

	if computePromptHash(bodyA, "claude-3") != computePromptHash(bodyB, "claude-3") {
		t.Fatal("expected logically identical request JSON to produce the same replay hash")
	}
}
