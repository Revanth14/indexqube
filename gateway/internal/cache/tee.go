package cache

import (
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/domain"
)

// Compile-time assertion that *Tee satisfies the domain TokenWriter port.
var _ domain.TokenWriter = (*Tee)(nil)

// Tee wraps a domain.TokenWriter so that every WriteData chunk flowing
// through to the live client is ALSO captured into a buffer for caching.
//
// Capture is abandoned, and any captured bytes are released, when ANY of
// the following happen:
//
//   - WriteEvent is called (errors and other out-of-band frames must not
//     be cached as if they were the response)
//   - Captured size exceeds maxCaptureBytes (response too large to cache)
//   - The Tee owner explicitly calls Abandon()
//
// On clean adapter return, the owner calls Entry to mint a cache.Entry
// from the buffered chunks. If capture was abandoned at any point,
// Entry returns (nil, false) and the response is NOT cached -- the
// stream the client received is unaffected.
type Tee struct {
	inner            domain.TokenWriter
	maxCaptureBytes  int64
	captured         [][]byte
	capturedBytes    int64
	abandoned        bool
}

// NewTee returns a Tee wrapping inner. If maxCaptureBytes <= 0 the cap
// is disabled and capture grows without bound (caller is responsible
// for not handing the Tee unbounded streams).
func NewTee(inner domain.TokenWriter, maxCaptureBytes int64) *Tee {
	return &Tee{
		inner:           inner,
		maxCaptureBytes: maxCaptureBytes,
	}
}

// WriteData captures the chunk (until abandoned) and forwards.
//
// We copy the slice because callers commonly write into a sync.Pool
// buffer and reuse it. Holding the original would race with the pool.
func (t *Tee) WriteData(data []byte) error {
	if !t.abandoned {
		if t.maxCaptureBytes > 0 && t.capturedBytes+int64(len(data)) > t.maxCaptureBytes {
			t.Abandon()
		} else {
			cp := make([]byte, len(data))
			copy(cp, data)
			t.captured = append(t.captured, cp)
			t.capturedBytes += int64(len(data))
		}
	}
	return t.inner.WriteData(data)
}

// WriteEvent forwards the event and abandons capture. Events represent
// out-of-band frames (errors today, possibly tool-call updates later)
// that should not be replayed verbatim from cache.
func (t *Tee) WriteEvent(event string, data []byte) error {
	t.Abandon()
	return t.inner.WriteEvent(event, data)
}

// WriteDone forwards the [DONE] sentinel without capturing it. Cache
// replays let the proxy emit [DONE] on clean return, same as live calls.
func (t *Tee) WriteDone() error {
	return t.inner.WriteDone()
}

// Flush forwards a flush.
func (t *Tee) Flush() error {
	return t.inner.Flush()
}

// Abandon discards the capture buffer and ensures Entry returns (nil, false).
func (t *Tee) Abandon() {
	t.abandoned = true
	t.captured = nil
	t.capturedBytes = 0
}

// Entry returns a cache.Entry built from the captured chunks, or
// (nil, false) if capture was abandoned or no data was captured.
func (t *Tee) Entry(provider domain.Provider, model string) (*Entry, bool) {
	if t.abandoned || len(t.captured) == 0 {
		return nil, false
	}
	return &Entry{
		Provider:  provider,
		Model:     model,
		Chunks:    t.captured,
		CreatedAt: time.Now(),
	}, true
}
