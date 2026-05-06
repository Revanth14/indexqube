package proxy

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/Revanth14/indexqube/gateway/internal/domain"
)

// Compile-time assertion that *sseWriter satisfies the domain TokenWriter port.
var _ domain.TokenWriter = (*sseWriter)(nil)

const (
	sseDoneSentinel = "[DONE]"
	sseDataPrefix   = "data: "
	sseEventPrefix  = "event: "
	sseLineEnd      = "\n"
	sseFrameEnd     = "\n\n"

	// initialBufCap and maxRetainedBufCap bound the sync.Pool buffer sizes.
	// Retaining buffers larger than maxRetainedBufCap defeats the pool's
	// purpose and risks ratcheting steady-state memory.
	initialBufCap     = 4 << 10
	maxRetainedBufCap = 64 << 10
)

var bufPool = sync.Pool{
	New: func() any {
		b := bytes.NewBuffer(make([]byte, 0, initialBufCap))
		return b
	},
}

func acquireBuf() *bytes.Buffer {
	b := bufPool.Get().(*bytes.Buffer)
	b.Reset()
	return b
}

func releaseBuf(b *bytes.Buffer) {
	if b.Cap() > maxRetainedBufCap {
		return
	}
	bufPool.Put(b)
}

// sseWriter is the canonical TokenWriter implementation backed by an
// http.ResponseWriter and an http.ResponseController for flushing.
type sseWriter struct {
	w  http.ResponseWriter
	rc *http.ResponseController
}

// newSSEWriter sets streaming-friendly headers, performs an initial flush
// to push them onto the wire (so clients see TTFB immediately), and returns
// a TokenWriter ready for frame writes.
func newSSEWriter(w http.ResponseWriter) (*sseWriter, error) {
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache, no-store, must-revalidate")
	h.Set("Connection", "keep-alive")
	// Disable nginx / CDN response buffering on intermediaries.
	h.Set("X-Accel-Buffering", "no")

	sw := &sseWriter{w: w, rc: http.NewResponseController(w)}
	if err := sw.rc.Flush(); err != nil {
		if errors.Is(err, http.ErrNotSupported) {
			return nil, fmt.Errorf("response writer does not support flushing: %w", err)
		}
		return nil, fmt.Errorf("initial flush failed: %w", err)
	}
	return sw, nil
}

func (s *sseWriter) WriteData(data []byte) error {
	buf := acquireBuf()
	defer releaseBuf(buf)
	buf.WriteString(sseDataPrefix)
	buf.Write(data)
	buf.WriteString(sseFrameEnd)
	if _, err := s.w.Write(buf.Bytes()); err != nil {
		return err
	}
	return s.rc.Flush()
}

func (s *sseWriter) WriteEvent(event string, data []byte) error {
	buf := acquireBuf()
	defer releaseBuf(buf)
	buf.WriteString(sseEventPrefix)
	buf.WriteString(event)
	buf.WriteString(sseLineEnd)
	buf.WriteString(sseDataPrefix)
	buf.Write(data)
	buf.WriteString(sseFrameEnd)
	if _, err := s.w.Write(buf.Bytes()); err != nil {
		return err
	}
	return s.rc.Flush()
}

func (s *sseWriter) WriteDone() error {
	return s.WriteData([]byte(sseDoneSentinel))
}

func (s *sseWriter) Flush() error {
	return s.rc.Flush()
}
