package anthropic

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/domain"
)

// streamSSE is a minimal Server-Sent Events parser specialized for
// Anthropic's stream format. Each event is one or more lines terminated
// by a blank line. We only care about `event:` and `data:` lines; comments
// (`:` prefix) and id/retry fields are skipped.
//
// Multi-line `data:` payloads (concatenated by newline) are not supported
// in v1 -- Anthropic's stream emits one `data:` per event in practice.
func streamSSE(ctx context.Context, body io.Reader, tr *translator) error {
	scanner := bufio.NewScanner(body)
	// Default token size is 64 KiB; bump to 1 MiB so a single event payload
	// (e.g. a large content_block_delta) cannot truncate.
	scanner.Buffer(make([]byte, 0, 4096), 1<<20)

	var event string
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		line := scanner.Bytes()
		switch {
		case len(line) == 0:
			event = ""
		case bytes.HasPrefix(line, []byte("event: ")):
			event = string(line[len("event: "):])
		case bytes.HasPrefix(line, []byte("data: ")):
			payload := line[len("data: "):]
			if err := tr.handle(event, payload); err != nil {
				return err
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("anthropic sse read: %w", err)
	}
	return nil
}

// translator turns Anthropic SSE events into OpenAI-shaped chunks and
// emits them through a TokenWriter.
//
// State: it remembers whether the role frame has been emitted, and which
// stop_reason to forward in the final chunk.
type translator struct {
	chunkID    string
	created    int64
	model      string
	tw         domain.TokenWriter
	roleSent   bool
	stopReason string
}

func newTranslator(chunkID, model string, tw domain.TokenWriter) *translator {
	return &translator{
		chunkID: chunkID,
		created: time.Now().Unix(),
		model:   model,
		tw:      tw,
	}
}

func (t *translator) handle(event string, payload []byte) error {
	switch event {
	case "content_block_start":
		// Emit the OpenAI role frame on first content block.
		if !t.roleSent {
			t.roleSent = true
			return t.emit(openAIDelta{Role: "assistant"}, "")
		}
		return nil

	case "content_block_delta":
		var ev struct {
			Delta struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"delta"`
		}
		if err := json.Unmarshal(payload, &ev); err != nil {
			// Malformed delta: skip rather than abort the stream.
			return nil
		}
		if ev.Delta.Type != "text_delta" || ev.Delta.Text == "" {
			return nil
		}
		// Defensive: if upstream skipped content_block_start, still emit role first.
		if !t.roleSent {
			t.roleSent = true
			if err := t.emit(openAIDelta{Role: "assistant"}, ""); err != nil {
				return err
			}
		}
		return t.emit(openAIDelta{Content: ev.Delta.Text}, "")

	case "message_delta":
		// Captures stop_reason; the actual finish frame is emitted on message_stop.
		var ev struct {
			Delta struct {
				StopReason string `json:"stop_reason"`
			} `json:"delta"`
		}
		if err := json.Unmarshal(payload, &ev); err != nil {
			return nil
		}
		if ev.Delta.StopReason != "" {
			t.stopReason = mapStopReason(ev.Delta.StopReason)
		}
		return nil

	case "message_stop":
		reason := t.stopReason
		if reason == "" {
			reason = "stop"
		}
		return t.emit(openAIDelta{}, reason)

	case "error":
		var ev struct {
			Error struct {
				Type    string `json:"type"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(payload, &ev); err == nil && ev.Error.Message != "" {
			return fmt.Errorf("anthropic stream error: %s: %s", ev.Error.Type, ev.Error.Message)
		}
		return fmt.Errorf("anthropic stream error: %s", payload)

	default:
		// message_start, content_block_stop, ping, and unknown events: ignore.
		return nil
	}
}

func (t *translator) emit(delta openAIDelta, finishReason string) error {
	choice := openAIChoice{Index: 0, Delta: delta}
	if finishReason != "" {
		fr := finishReason
		choice.FinishReason = &fr
	}
	chunk := openAIChunk{
		ID:      t.chunkID,
		Object:  "chat.completion.chunk",
		Created: t.created,
		Model:   t.model,
		Choices: []openAIChoice{choice},
	}
	b, err := json.Marshal(chunk)
	if err != nil {
		return err
	}
	return t.tw.WriteData(b)
}

// mapStopReason translates Anthropic stop_reason into OpenAI finish_reason.
// Mapping reference:
//
//	end_turn      -> stop
//	max_tokens    -> length
//	stop_sequence -> stop
//	tool_use      -> tool_calls (deferred; falls back to "stop" until tools are wired)
func mapStopReason(s string) string {
	switch s {
	case "end_turn", "stop_sequence":
		return "stop"
	case "max_tokens":
		return "length"
	case "tool_use":
		return "tool_calls"
	default:
		return "stop"
	}
}

// --- OpenAI chunk shape ---

type openAIChunk struct {
	ID      string         `json:"id"`
	Object  string         `json:"object"`
	Created int64          `json:"created"`
	Model   string         `json:"model"`
	Choices []openAIChoice `json:"choices"`
}

type openAIChoice struct {
	Index        int         `json:"index"`
	Delta        openAIDelta `json:"delta"`
	FinishReason *string     `json:"finish_reason"`
}

type openAIDelta struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}
