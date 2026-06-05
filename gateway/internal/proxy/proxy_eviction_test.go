package proxy

import (
	"testing"
	"time"
)

func TestSessionEviction(t *testing.T) {
	p := New(&fakeGovernor{})
	p.Start()
	defer p.Stop()

	// Seed some session state.
	p.sessionTurnCounters.Store("old-session", &sessionTurnState{})
	p.sessionLastUsed.Store("old-session", time.Now().Add(-2*time.Hour))

	p.sessionWarmUpDone.Store("fresh-session", true)
	p.sessionLastUsed.Store("fresh-session", time.Now())

	// Manually trigger eviction with a 1-minute max idle for testing.
	p.evictStaleSessions()

	// old-session should be gone.
	if _, ok := p.sessionTurnCounters.Load("old-session"); ok {
		t.Fatal("old-session should have been evicted")
	}

	// fresh-session should remain.
	if _, ok := p.sessionWarmUpDone.Load("fresh-session"); !ok {
		t.Fatal("fresh-session should still exist")
	}
}

func TestTouchSessionUpdatesLastUsed(t *testing.T) {
	p := New(&fakeGovernor{})
	p.Start()
	defer p.Stop()

	p.touchSession("test-key")

	val, ok := p.sessionLastUsed.Load("test-key")
	if !ok {
		t.Fatal("expected last-used entry after touch")
	}
	lastUsed := val.(time.Time)
	if time.Since(lastUsed) > time.Second {
		t.Fatal("expected recently updated last-used time")
	}
}
