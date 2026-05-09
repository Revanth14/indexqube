package proxy

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const maxProviderCooldown = 5 * time.Minute

type providerCooldowns struct {
	mu      sync.Mutex
	entries map[string]providerCooldown
}

type providerCooldown struct {
	Until             time.Time
	Provider          string
	Model             string
	StatusCode        int
	UpstreamCode      string
	UpstreamType      string
	UpstreamRequestID string
}

func newProviderCooldowns() *providerCooldowns {
	return &providerCooldowns{entries: make(map[string]providerCooldown)}
}

func (c *providerCooldowns) Get(provider, model string, now time.Time) (providerCooldown, bool) {
	if c == nil {
		return providerCooldown{}, false
	}
	key := providerCooldownKey(provider, model)
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if !ok {
		return providerCooldown{}, false
	}
	if !now.Before(entry.Until) {
		delete(c.entries, key)
		return providerCooldown{}, false
	}
	return entry, true
}

func (c *providerCooldowns) Open(provider, model string, statusCode int, meta claudeUpstreamErrorMeta, fallback time.Duration, now time.Time) providerCooldown {
	if c == nil {
		return providerCooldown{}
	}
	cooldown := meta.RetryAfter
	if cooldown <= 0 {
		cooldown = fallback
	}
	if cooldown <= 0 {
		cooldown = 30 * time.Second
	}
	if cooldown > maxProviderCooldown {
		cooldown = maxProviderCooldown
	}
	entry := providerCooldown{
		Until:             now.Add(cooldown),
		Provider:          provider,
		Model:             model,
		StatusCode:        statusCode,
		UpstreamCode:      meta.Code,
		UpstreamType:      meta.Type,
		UpstreamRequestID: meta.RequestID,
	}
	c.mu.Lock()
	c.entries[providerCooldownKey(provider, model)] = entry
	c.mu.Unlock()
	return entry
}

func providerCooldownKey(provider, model string) string {
	return strings.ToLower(strings.TrimSpace(provider)) + ":" + strings.TrimSpace(model)
}

func retryAfterDuration(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil {
		if seconds <= 0 {
			return 0
		}
		return time.Duration(seconds) * time.Second
	}
	when, err := http.ParseTime(value)
	if err != nil {
		return 0
	}
	if !when.After(now) {
		return 0
	}
	return when.Sub(now)
}

func retryAfterSeconds(d time.Duration) string {
	if d <= 0 {
		return ""
	}
	seconds := int((d + time.Second - 1) / time.Second)
	if seconds < 1 {
		seconds = 1
	}
	return fmt.Sprintf("%d", seconds)
}
