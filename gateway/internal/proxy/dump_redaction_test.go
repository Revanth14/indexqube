package proxy

import (
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
}
