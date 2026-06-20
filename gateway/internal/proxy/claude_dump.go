package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/redact"
)

var dumpPayloadMu sync.Mutex

func dumpClaudePayloads(requestID string, before, after []byte, stats claudeStreamStats, optStats claudeOptimizerStats) {
	if sessionFile := os.Getenv("IQ_DUMP_SESSION_FILE"); sessionFile != "" {
		if err := appendSessionDump(sessionFile, requestID, before, after, stats, optStats); err != nil {
			fmt.Fprintf(os.Stderr, "[iq] failed to append payload dump: %v\n", err)
		}
		return
	}

	dumpDir := os.Getenv("IQ_DUMP_DIR")
	if dumpDir == "" {
		dumpDir = "/tmp"
	}
	if err := os.MkdirAll(dumpDir, 0o700); err != nil {
		fmt.Fprintf(os.Stderr, "[iq] failed to dump payload pair: %v\n", err)
		return
	}
	beforePath := filepath.Join(dumpDir, "iq-before-"+requestID+".json")
	afterPath := filepath.Join(dumpDir, "iq-after-"+requestID+".json")
	if err := os.WriteFile(beforePath, prettyJSON(redactedJSONPayload(before)), 0o600); err != nil {
		fmt.Fprintf(os.Stderr, "[iq] failed to dump payload pair: %v\n", err)
		return
	}
	if err := os.WriteFile(afterPath, prettyJSON(redactedJSONPayload(after)), 0o600); err != nil {
		fmt.Fprintf(os.Stderr, "[iq] failed to dump payload pair: %v\n", err)
		return
	}
	appendDumpLog(dumpDir, beforePath, afterPath)
}

type payloadDumpResponse struct {
	Text         string `json:"text"`
	OutputTokens int    `json:"output_tokens"`
	Status       string `json:"status"`
	// Raw upstream input usage exactly as Anthropic reported it, so a dump can
	// distinguish "prompt caching not applied" (cache fields 0 with large input)
	// from "applied but small". Zero on synthetic/probe turns (no upstream call).
	InputTokens              int `json:"input_tokens"`
	CacheReadInputTokens     int `json:"cache_read_input_tokens"`
	CacheCreationInputTokens int `json:"cache_creation_input_tokens"`
}

type payloadDumpRecord struct {
	Timestamp   string                 `json:"ts"`
	RequestID   string                 `json:"request_id"`
	BeforeBytes int                    `json:"before_bytes"`
	AfterBytes  int                    `json:"after_bytes"`
	SavedBytes  int                    `json:"saved_bytes"`
	Before      json.RawMessage        `json:"before"`
	After       json.RawMessage        `json:"after"`
	Response    payloadDumpResponse    `json:"response"`
	Optimizer   *payloadOptimizerStats `json:"optimizer,omitempty"`
}

// payloadOptimizerStats is the per-turn cache-efficiency view written into
// the JSONL dump. It separates the pruning view (saved_bytes / blocks_pruned)
// from the true cache-hit view (true_cache_hit_bytes / known_bytes), so the
// audit script can distinguish "bytes removed from the forwarded payload"
// from "bytes that hit the session cache regardless of preservation rules".
type payloadOptimizerStats struct {
	BlocksPruned         int `json:"blocks_pruned"`
	BlocksKnown          int `json:"blocks_known"`
	BlocksKnownProtected int `json:"blocks_known_protected"`
	BytesPruned          int `json:"bytes_pruned"`
	ProtectedBytes       int `json:"protected_bytes"`
	KnownBytes           int `json:"known_bytes"`
	TrueCacheHitBytes    int `json:"true_cache_hit_bytes"`
}

func appendSessionDump(sessionFile, requestID string, before, after []byte, stats claudeStreamStats, optStats claudeOptimizerStats) error {
	if err := os.MkdirAll(filepath.Dir(sessionFile), 0o700); err != nil {
		return err
	}
	record := payloadDumpRecord{
		Timestamp:   time.Now().Format(time.RFC3339),
		RequestID:   requestID,
		BeforeBytes: len(before),
		AfterBytes:  len(after),
		SavedBytes:  len(before) - len(after),
		Before:      redactedJSONPayload(before),
		After:       redactedJSONPayload(after),
		Response: payloadDumpResponse{
			Text:                     redact.String(stats.OutputRawText),
			OutputTokens:             stats.OutputTokens,
			Status:                   stats.Status,
			InputTokens:              stats.InputTokens,
			CacheReadInputTokens:     stats.CacheReadInputTokens,
			CacheCreationInputTokens: stats.CacheCreationInputTokens,
		},
		Optimizer: &payloadOptimizerStats{
			BlocksPruned:         optStats.BlocksPruned,
			BlocksKnown:          optStats.BlocksKnown,
			BlocksKnownProtected: optStats.PreservedInstructionCount,
			BytesPruned:          optStats.BytesPruned,
			ProtectedBytes:       optStats.PreservedInstructionBytes,
			KnownBytes:           optStats.KnownBytes,
			TrueCacheHitBytes:    optStats.KnownBytes,
		},
	}
	line, err := json.Marshal(record)
	if err != nil {
		return err
	}
	line = append(line, '\n')

	dumpPayloadMu.Lock()
	defer dumpPayloadMu.Unlock()
	f, err := os.OpenFile(sessionFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(line)
	return err
}

func redactedJSONPayload(raw []byte) json.RawMessage {
	if len(bytes.TrimSpace(raw)) == 0 {
		return json.RawMessage("null")
	}

	var parsed any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		encoded, _ := json.Marshal(redact.String(string(raw)))
		return json.RawMessage(encoded)
	}

	encoded, err := marshalJSONNoHTMLEscape(redactJSONValue(parsed))
	if err != nil {
		fallback, _ := json.Marshal(redact.String(string(raw)))
		return json.RawMessage(fallback)
	}
	return json.RawMessage(encoded)
}

func redactJSONValue(value any) any {
	switch v := value.(type) {
	case map[string]any:
		for key, child := range v {
			if redact.SensitiveKey(key) {
				v[key] = "[redacted]"
				continue
			}
			v[key] = redactJSONValue(child)
		}
		return v
	case []any:
		for i, child := range v {
			v[i] = redactJSONValue(child)
		}
		return v
	case string:
		return redact.String(v)
	default:
		return v
	}
}

func appendDumpLog(dumpDir, beforePath, afterPath string) {
	logPath := filepath.Join(dumpDir, "dump.log")
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fprintf(f, "%s dumped payload pair -> %s %s\n", time.Now().Format(time.RFC3339), beforePath, afterPath)
}

func prettyJSON(raw []byte) []byte {
	var out bytes.Buffer
	if err := json.Indent(&out, raw, "", "  "); err != nil {
		return raw
	}
	out.WriteByte('\n')
	return out.Bytes()
}

func marshalJSONNoHTMLEscape(v any) ([]byte, error) {
	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(out.Bytes(), []byte{'\n'}), nil
}
