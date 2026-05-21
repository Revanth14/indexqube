package proxy

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// StatsResponse is the shape returned by GET /stats.
type StatsResponse struct {
	TokensAttempted    int64 `json:"tokens_attempted"`
	TokensDeduplicated int64 `json:"tokens_deduplicated"`
	RequestsTotal      int64 `json:"requests_total"`
	SessionsTotal      int64 `json:"sessions_total"`
}

// StatsHandler serves aggregated usage stats from Supabase, cached for 5 min.
type StatsHandler struct {
	supabaseURL string
	serviceKey  string
	httpClient  *http.Client

	mu       sync.RWMutex
	cached   *StatsResponse
	cachedAt time.Time
	ttl      time.Duration
}

// NewStatsHandler returns a handler that queries Supabase and caches results.
// supabaseURL is the project URL (e.g. https://xyz.supabase.co).
func NewStatsHandler(supabaseURL, serviceKey string) *StatsHandler {
	return &StatsHandler{
		supabaseURL: supabaseURL,
		serviceKey:  serviceKey,
		httpClient:  &http.Client{Timeout: 10 * time.Second},
		ttl:         5 * time.Minute,
	}
}

func (h *StatsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	stats, err := h.get()
	if err != nil {
		http.Error(w, `{"error":"stats unavailable"}`, http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "public, max-age=300")
	_ = json.NewEncoder(w).Encode(stats)
}

func (h *StatsHandler) get() (*StatsResponse, error) {
	h.mu.RLock()
	if h.cached != nil && time.Since(h.cachedAt) < h.ttl {
		stats := *h.cached
		h.mu.RUnlock()
		return &stats, nil
	}
	h.mu.RUnlock()

	fresh, err := h.fetch()
	if err != nil {
		// Return stale cache on error rather than failing.
		h.mu.RLock()
		stale := h.cached
		h.mu.RUnlock()
		if stale != nil {
			return stale, nil
		}
		return nil, err
	}

	h.mu.Lock()
	h.cached = fresh
	h.cachedAt = time.Now()
	h.mu.Unlock()
	return fresh, nil
}

func (h *StatsHandler) fetch() (*StatsResponse, error) {
	url := fmt.Sprintf("%s/rest/v1/rpc/get_public_stats", h.supabaseURL)
	req, err := http.NewRequest(http.MethodPost, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("apikey", h.serviceKey)
	req.Header.Set("Authorization", "Bearer "+h.serviceKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("supabase rpc status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var stats StatsResponse
	if err := json.Unmarshal(body, &stats); err != nil {
		return nil, fmt.Errorf("parse stats: %w", err)
	}
	return &stats, nil
}
