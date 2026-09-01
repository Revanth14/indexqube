package proxy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
)

type responseCaptureWriter struct {
	http.ResponseWriter
	buf bytes.Buffer
}

func (rcw *responseCaptureWriter) Write(b []byte) (int, error) {
	rcw.buf.Write(b)
	return rcw.ResponseWriter.Write(b)
}

func (rcw *responseCaptureWriter) Flush() {
	if flusher, ok := rcw.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (rcw *responseCaptureWriter) WriteString(s string) (int, error) {
	rcw.buf.WriteString(s)
	if sw, ok := rcw.ResponseWriter.(io.StringWriter); ok {
		return sw.WriteString(s)
	}
	return rcw.ResponseWriter.Write([]byte(s))
}

func computePromptHash(body []byte, model string) string {
	canonicalBody := body
	var parsed any
	if err := json.Unmarshal(body, &parsed); err == nil {
		if encoded, encErr := marshalJSONNoHTMLEscape(parsed); encErr == nil {
			canonicalBody = encoded
		}
	}
	h := sha256.New()
	h.Write([]byte(model))
	h.Write([]byte{0})
	h.Write(canonicalBody)
	return hex.EncodeToString(h.Sum(nil))
}

func writeSyntheticStreamResponse(w http.ResponseWriter, text string) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	textData, _ := json.Marshal(text)
	events := []string{
		`event: message_start` + "\n" + `data: {"type":"message_start","message":{"id":"msg_synth","type":"message","role":"assistant","content":[],"model":"claude-3-5-sonnet","usage":{"input_tokens":5,"output_tokens":5}}}` + "\n\n",
		`event: content_block_start` + "\n" + `data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}` + "\n\n",
		`event: content_block_delta` + "\n" + `data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":` + string(textData) + `}}` + "\n\n",
		`event: content_block_stop` + "\n" + `data: {"type":"content_block_stop","index":0}` + "\n\n",
		`event: message_delta` + "\n" + `data: {"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":5}}` + "\n\n",
		`event: message_stop` + "\n" + `data: {"type":"message_stop"}` + "\n\n",
	}

	for _, ev := range events {
		_, _ = io.WriteString(w, ev)
	}
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
}

func estimateTokens(chars int) int {
	if chars <= 0 {
		return 0
	}
	return (chars + 3) / 4
}
