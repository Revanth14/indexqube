package proxy

import (
	"encoding/json"
	"fmt"
	"strings"
)

// SpanClass constants identify the semantic type of a text span.
const (
	// SpanClassSystemText identifies content from the system prompt; never pruned.
	SpanClassSystemText          = "system_text"
	SpanClassUserTextLatest      = "user_text_latest"
	SpanClassUserTextOld         = "user_text_old"
	SpanClassAssistantTextLatest = "assistant_text_latest"
	SpanClassAssistantTextOld    = "assistant_text_old"
	SpanClassToolResultLatest    = "tool_result_latest"
	SpanClassToolResultOld       = "tool_result_old"
	SpanClassToolUse             = "tool_use"
	SpanClassUnknownText         = "unknown_text"
)

// TextSpan is one text-bearing unit extracted from an Anthropic messages request.
// It carries location, classification, and hash — but never persisted raw text.
type TextSpan struct {
	Path            string // dot-path for diagnostics (e.g. "messages[2].content[0].text")
	Role            string // "user", "assistant", "system"
	BlockType       string // "text", "tool_result", "tool_use", or ""
	Class           string // one of SpanClass* constants
	MessageIndex    int    // -1 for system
	ContentIndex    int    // -1 for plain string content
	SubContentIndex int    // index within tool_result content array; -1 for string content or N/A
	Text            string // original text value (used for hashing only; never stored in memory store)
	Hash            string // sha256 of TrimSpace(Text)
	Bytes           int
	Tokens          int
	IsLatestTurn    bool
}

// extractSpans walks an Anthropic messages request root and returns all
// text-bearing spans with classification. Both the latest user and latest
// assistant turns are detected for preservation policy.
func extractSpans(root map[string]any) []TextSpan {
	var spans []TextSpan

	latestUserIdx := -1
	latestAssistantIdx := -1
	if messages, ok := root["messages"].([]any); ok {
		for i := len(messages) - 1; i >= 0; i-- {
			msg, ok := messages[i].(map[string]any)
			if !ok {
				continue
			}
			role, _ := msg["role"].(string)
			role = strings.ToLower(strings.TrimSpace(role))
			if role == "user" && latestUserIdx < 0 {
				latestUserIdx = i
			}
			if role == "assistant" && latestAssistantIdx < 0 {
				latestAssistantIdx = i
			}
			if latestUserIdx >= 0 && latestAssistantIdx >= 0 {
				break
			}
		}
	}

	if sys := root["system"]; sys != nil {
		spans = append(spans, extractSystemSpans(sys)...)
	}

	if messages, ok := root["messages"].([]any); ok {
		for i, rawMsg := range messages {
			msg, ok := rawMsg.(map[string]any)
			if !ok {
				continue
			}
			role, _ := msg["role"].(string)
			role = strings.ToLower(strings.TrimSpace(role))
			isLatest := i == latestUserIdx || i == latestAssistantIdx
			spans = append(spans, extractMessageSpans(msg["content"], role, i, isLatest)...)
		}
	}

	return spans
}

func extractSystemSpans(sys any) []TextSpan {
	switch v := sys.(type) {
	case string:
		if strings.TrimSpace(v) != "" {
			return []TextSpan{{
				Path:         "system",
				Role:         "system",
				Class:        SpanClassSystemText,
				MessageIndex: -1,
				ContentIndex: -1,
				Text:         v,
				Hash:         hashText(v),
				Bytes:        len(v),
				Tokens:       estimateTokens(len(v)),
			}}
		}
	case []any:
		var spans []TextSpan
		for i, item := range v {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			if typ, _ := m["type"].(string); typ == "text" {
				if text, ok := m["text"].(string); ok && strings.TrimSpace(text) != "" {
					spans = append(spans, TextSpan{
						Path:         fmt.Sprintf("system[%d].text", i),
						Role:         "system",
						BlockType:    "text",
						Class:        SpanClassSystemText,
						MessageIndex: -1,
						ContentIndex: i,
						Text:         text,
						Hash:         hashText(text),
						Bytes:        len(text),
						Tokens:       estimateTokens(len(text)),
					})
				}
			}
		}
		return spans
	}
	return nil
}

func extractMessageSpans(content any, role string, msgIdx int, isLatest bool) []TextSpan {
	var spans []TextSpan
	switch c := content.(type) {
	case string:
		if strings.TrimSpace(c) != "" {
			spans = append(spans, TextSpan{
				Path:            fmt.Sprintf("messages[%d].content", msgIdx),
				Role:            role,
				Class:           classifyTextSpan(role, "", isLatest),
				MessageIndex:    msgIdx,
				ContentIndex:    -1,
				SubContentIndex: -1,
				Text:            c,
				Hash:            hashText(c),
				Bytes:           len(c),
				Tokens:          estimateTokens(len(c)),
				IsLatestTurn:    isLatest,
			})
		}
	case []any:
		for i, rawItem := range c {
			item, ok := rawItem.(map[string]any)
			if !ok {
				continue
			}
			typ, _ := item["type"].(string)
			switch typ {
			case "text":
				if text, ok := item["text"].(string); ok && strings.TrimSpace(text) != "" {
					spans = append(spans, TextSpan{
						Path:            fmt.Sprintf("messages[%d].content[%d].text", msgIdx, i),
						Role:            role,
						BlockType:       "text",
						Class:           classifyTextSpan(role, "text", isLatest),
						MessageIndex:    msgIdx,
						ContentIndex:    i,
						SubContentIndex: -1,
						Text:            text,
						Hash:            hashText(text),
						Bytes:           len(text),
						Tokens:          estimateTokens(len(text)),
						IsLatestTurn:    isLatest,
					})
				}
			case "tool_result":
				spans = append(spans, extractToolResultSpans(item["content"], role, msgIdx, i, isLatest)...)
			case "tool_use":
				b, _ := json.Marshal(item)
				spans = append(spans, TextSpan{
					Path:         fmt.Sprintf("messages[%d].content[%d]", msgIdx, i),
					Role:         role,
					BlockType:    "tool_use",
					Class:        SpanClassToolUse,
					MessageIndex: msgIdx,
					ContentIndex: i,
					Hash:         hashText(string(b)),
					Bytes:        len(b),
					Tokens:       estimateTokens(len(b)),
					IsLatestTurn: isLatest,
				})
			}
		}
	}
	return spans
}

func extractToolResultSpans(content any, role string, msgIdx, contentIdx int, isLatest bool) []TextSpan {
	class := classifyTextSpan(role, "tool_result", isLatest)
	switch c := content.(type) {
	case string:
		if strings.TrimSpace(c) != "" {
			return []TextSpan{{
				Path:            fmt.Sprintf("messages[%d].content[%d].content", msgIdx, contentIdx),
				Role:            role,
				BlockType:       "tool_result",
				Class:           class,
				MessageIndex:    msgIdx,
				ContentIndex:    contentIdx,
				SubContentIndex: -1,
				Text:            c,
				Hash:            hashText(c),
				Bytes:           len(c),
				Tokens:          estimateTokens(len(c)),
				IsLatestTurn:    isLatest,
			}}
		}
	case []any:
		var spans []TextSpan
		for i, rawItem := range c {
			item, ok := rawItem.(map[string]any)
			if !ok {
				continue
			}
			if typ, _ := item["type"].(string); typ == "text" {
				if text, ok := item["text"].(string); ok && strings.TrimSpace(text) != "" {
					spans = append(spans, TextSpan{
						Path:            fmt.Sprintf("messages[%d].content[%d].content[%d].text", msgIdx, contentIdx, i),
						Role:            role,
						BlockType:       "tool_result",
						Class:           class,
						MessageIndex:    msgIdx,
						ContentIndex:    contentIdx,
						SubContentIndex: i,
						Text:            text,
						Hash:            hashText(text),
						Bytes:           len(text),
						Tokens:          estimateTokens(len(text)),
						IsLatestTurn:    isLatest,
					})
				}
			}
		}
		return spans
	}
	return nil
}

// classifyTextSpan returns the SpanClass* constant for a given role/blockType/isLatest.
func classifyTextSpan(role, blockType string, isLatest bool) string {
	switch role {
	case "system":
		return SpanClassSystemText
	case "user":
		if blockType == "tool_result" {
			if isLatest {
				return SpanClassToolResultLatest
			}
			return SpanClassToolResultOld
		}
		if isLatest {
			return SpanClassUserTextLatest
		}
		return SpanClassUserTextOld
	case "assistant":
		if blockType == "tool_use" {
			return SpanClassToolUse
		}
		if isLatest {
			return SpanClassAssistantTextLatest
		}
		return SpanClassAssistantTextOld
	default:
		return SpanClassUnknownText
	}
}
