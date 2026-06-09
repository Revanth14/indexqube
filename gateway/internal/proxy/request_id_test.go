package proxy

import (
	"strings"
	"testing"
)

func TestResolveRequestIDUsesUUIDFormat(t *testing.T) {
	p := New(&fakeGovernor{})
	id, synthetic, _ := p.resolveRequestID("sess123", "")
	if !synthetic {
		t.Fatal("expected synthetic ID for empty rawID")
	}
	if !strings.HasPrefix(id, "iq-synthetic-") {
		t.Fatalf("expected iq-synthetic- prefix, got %q", id)
	}
	// Should contain an 8-char hex suffix from uuid.New().String()[:8].
	parts := strings.Split(id, "-")
	lastPart := parts[len(parts)-1]
	if len(lastPart) != 8 {
		t.Fatalf("expected 8-char hex suffix, got %q (len=%d)", lastPart, len(lastPart))
	}
}

func TestProvidedRequestIDPreserved(t *testing.T) {
	p := New(&fakeGovernor{})
	id, synthetic, _ := p.resolveRequestID("sess123", "client-req-42")
	if synthetic {
		t.Fatal("expected provided ID to be preserved")
	}
	if id != "client-req-42" {
		t.Fatalf("expected client-req-42, got %q", id)
	}
}
