package proxy

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Revanth14/indexqube/gateway/internal/redact"
)

func TestDumpRedaction(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "iq-dump-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sessionFile := filepath.Join(tmpDir, "dump.jsonl")

	before := []byte(`{"messages":[{"role":"user","content":"Use key: sk-proj-abc1234567890"}],"system":"Authorization: Bearer sk-ant-test123"}`)
	after := []byte(`{"messages":[{"role":"user","content":"Use key: sk-proj-abc1234567890"}]}`)

	stats := claudeStreamStats{OutputRawText: "done", OutputTokens: 10, Status: "ok"}
	opt := claudeOptimizerStats{}

	if err := appendSessionDump(sessionFile, "req-1", before, after, stats, opt); err != nil {
		t.Fatalf("appendSessionDump failed: %v", err)
	}

	data, err := os.ReadFile(sessionFile)
	if err != nil {
		t.Fatalf("read dump failed: %v", err)
	}

	if strings.Contains(string(data), "sk-proj-abc1234567890") {
		t.Fatal("dump should not contain raw API key")
	}
	if strings.Contains(string(data), "sk-ant-test123") {
		t.Fatal("dump should not contain bearer token")
	}
	if !strings.Contains(string(data), redact.String("sk-proj-abc1234567890")) {
		t.Fatal("dump should contain redacted marker")
	}
	for i, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if !json.Valid([]byte(line)) {
			t.Fatalf("dump line %d is not valid JSON: %s", i+1, line)
		}
	}
}

func TestDumpRedactionPreservesJSONLForEscapedCredentialExamples(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "iq-dump-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sessionFile := filepath.Join(tmpDir, "dump.jsonl")
	before := []byte(`{"messages":[{"role":"user","content":"normal prompt"}],"authorization":"Bearer sk-ant-beforesecret1234567890"}`)
	after := []byte(`{"messages":[{"role":"user","content":"normal prompt"}]}`)
	stats := claudeStreamStats{
		OutputRawText: "A span ending with `\\\"Authorization: sk-proj-responsesecret1234567890\\\"` should not corrupt JSON.\n```go\n\".cursor/rules/\",\n```",
		OutputTokens:  12,
		Status:        "completed",
	}

	if err := appendSessionDump(sessionFile, "req-escaped", before, after, stats, claudeOptimizerStats{}); err != nil {
		t.Fatalf("appendSessionDump failed: %v", err)
	}

	data, err := os.ReadFile(sessionFile)
	if err != nil {
		t.Fatalf("read dump failed: %v", err)
	}
	if strings.Contains(string(data), "sk-ant-beforesecret1234567890") || strings.Contains(string(data), "sk-proj-responsesecret1234567890") {
		t.Fatalf("dump leaked secret material: %s", data)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 1 {
		t.Fatalf("dump lines=%d, want 1", len(lines))
	}
	if !json.Valid([]byte(lines[0])) {
		t.Fatalf("dump line is not valid JSON: %s", lines[0])
	}

	var rec payloadDumpRecord
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("unmarshal dump line: %v", err)
	}
	if rec.Response.Text == "" || !strings.Contains(rec.Response.Text, "[redacted") {
		t.Fatalf("response text was not redacted as expected: %q", rec.Response.Text)
	}
	if !json.Valid(rec.Before) || !json.Valid(rec.After) {
		t.Fatalf("embedded payloads must stay valid JSON: before=%s after=%s", rec.Before, rec.After)
	}
}

func TestDumpRedactionPrettyFiles(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "iq-dump-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	os.Setenv("IQ_DUMP_DIR", tmpDir)
	defer os.Unsetenv("IQ_DUMP_DIR")
	os.Unsetenv("IQ_DUMP_SESSION_FILE")

	before := []byte(`{"system":"Authorization: Bearer abc-secret"}`)
	after := []byte(`{"system":"redacted"}`)
	stats := claudeStreamStats{OutputRawText: "done", OutputTokens: 5, Status: "ok"}
	opt := claudeOptimizerStats{}

	dumpClaudePayloads("req-2", before, after, stats, opt)

	beforePath := filepath.Join(tmpDir, "iq-before-req-2.json")
	data, err := os.ReadFile(beforePath)
	if err != nil {
		t.Fatalf("read before dump failed: %v", err)
	}
	if strings.Contains(string(data), "abc-secret") {
		t.Fatal("pretty dump should not contain raw bearer token")
	}
	if !json.Valid(data) {
		t.Fatalf("pretty dump should remain valid JSON: %s", data)
	}
}
