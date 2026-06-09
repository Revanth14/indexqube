package proxy

import (
	"context"
	"log/slog"
	"time"
)

const sessionCleanupInterval = time.Minute
const sessionMaxIdle = time.Hour

// Start begins background goroutines (session TTL eviction).
func (p *Proxy) Start() {
	p.cleanupCtx, p.cleanupCancel = context.WithCancel(context.Background())
	p.cleanupDone = make(chan struct{})
	go p.cleanupLoop()
}

// Stop cleanly shuts down background goroutines.
func (p *Proxy) Stop() {
	if p.cleanupCancel != nil {
		p.cleanupCancel()
	}
	if p.cleanupDone != nil {
		<-p.cleanupDone
	}
}

func (p *Proxy) touchSession(key string) {
	p.sessionLastUsed.Store(key, time.Now())
}

func (p *Proxy) cleanupLoop() {
	defer close(p.cleanupDone)
	ticker := time.NewTicker(sessionCleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.evictStaleSessions()
		case <-p.cleanupCtx.Done():
			return
		}
	}
}

func (p *Proxy) evictStaleSessions() {
	cutoff := time.Now().Add(-sessionMaxIdle)
	var evicted int

	p.sessionLastUsed.Range(func(key, value interface{}) bool {
		lastUsed := value.(time.Time)
		if lastUsed.Before(cutoff) {
			sk := key.(string)
			p.sessionTurnCounters.Delete(sk)
			p.sessionWarmUpDone.Delete(sk)
			p.sessionBoilerplateState.Delete(sk)
			p.sessionPrefixHints.Delete(sk)
			p.sessionSuggestionTs.Delete(sk)
			p.sessionLastUsed.Delete(sk)
			evicted++
		}
		return true
	})

	if evicted > 0 {
		p.logger.Debug("session eviction complete",
			slog.Int("evicted", evicted),
			slog.Duration("max_idle", sessionMaxIdle))
	}
}
