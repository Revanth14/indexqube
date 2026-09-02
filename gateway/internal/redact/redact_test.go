package redact

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestString_RedactsKnownSecretShapes(t *testing.T) {
	t.Parallel()

	input := strings.Join([]string{
		"Authorization: Bearer sk-proj-abc1234567890",
		"X-IQ-Provider-Key: sk-ant-api03-abc1234567890",
		`"api_key":"sk-live-abc1234567890"`,
		"ghp_abc1234567890abcdef",
		"AKIA1234567890ABCDEF",
		"xoxb-123456789012-abcdefghijklmnopqrst",
		"eyJabc1234567890.eyJdef1234567890.signature1234567890",
		"-----BEGIN PRIVATE KEY-----\nabc123\n-----END PRIVATE KEY-----",
		`"aws_secret_access_key":"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"`,
		`azure_api_key=abcdefghijklmnopqrstuvwxyz123456`,
	}, "\n")

	got := String(input)
	for _, leak := range []string{
		"sk-proj-abc1234567890",
		"sk-ant-api03-abc1234567890",
		"sk-live-abc1234567890",
		"ghp_abc1234567890abcdef",
		"AKIA1234567890ABCDEF",
		"xoxb-123456789012-abcdefghijklmnopqrst",
		"eyJabc1234567890.eyJdef1234567890.signature1234567890",
		"abc123",
		"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		"abcdefghijklmnopqrstuvwxyz123456",
	} {
		if strings.Contains(got, leak) {
			t.Fatalf("redacted string leaked %q: %s", leak, got)
		}
	}
	if !strings.Contains(got, "[redacted") {
		t.Fatalf("redacted markers missing: %s", got)
	}
}

func TestValueForKey_RedactsSensitiveKeys(t *testing.T) {
	t.Parallel()

	for _, key := range []string{"Authorization", "X-IQ-Provider-Key", "x-api-key", "api-key"} {
		if got := ValueForKey(key, "plain-secret"); got != "[redacted]" {
			t.Fatalf("%s redaction=%q, want [redacted]", key, got)
		}
	}

	if got := ValueForKey("message", "hello sk-test123456789"); strings.Contains(got, "sk-test123456789") {
		t.Fatalf("message redaction leaked secret: %q", got)
	}
}

func TestTruncatePreservesUTF8(t *testing.T) {
	got := Truncate("a🙂b🙂c", 8)
	if !utf8.ValidString(got) || len(got) > 8 || !strings.HasSuffix(got, "...") {
		t.Fatalf("truncated=%q bytes=%d", got, len(got))
	}
}
