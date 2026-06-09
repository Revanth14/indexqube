package proxy

import (
	"strings"
	"testing"
)

func TestValidateSessionKey(t *testing.T) {
	if err := validateSessionKey(""); err != nil {
		t.Fatalf("empty session key should be valid: %v", err)
	}
	if err := validateSessionKey("short"); err != nil {
		t.Fatalf("short session key should be valid: %v", err)
	}
	long := strings.Repeat("a", 256)
	if err := validateSessionKey(long); err != nil {
		t.Fatalf("256-byte session key should be valid: %v", err)
	}
	tooLong := strings.Repeat("a", 257)
	if err := validateSessionKey(tooLong); err == nil {
		t.Fatal("257-byte session key should be invalid")
	}
}

func TestNormalizeContextPathBlocksTraversal(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"foo/bar", "foo/bar"},
		{"../etc/passwd", defaultRawContextPath},
		{"foo/../bar", defaultRawContextPath},
		{"foo/bar/..", defaultRawContextPath},
		{"", defaultRawContextPath},
		{"ok.txt", "ok.txt"},
	}
	for _, c := range cases {
		got := normalizeContextPath(c.input)
		if got != c.want {
			t.Errorf("normalizeContextPath(%q) = %q, want %q", c.input, got, c.want)
		}
	}
}
