// Package redact centralizes best-effort secret redaction for logs and
// client-facing error text. It intentionally favors false positives over
// leaking credentials.
package redact

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

const marker = "[redacted]"

var stringRules = []struct {
	re   *regexp.Regexp
	repl string
}{
	{regexp.MustCompile(`(?i)(Authorization:\s*Bearer\s+)[^\s,}]+`), `${1}` + marker},
	{regexp.MustCompile(`(?i)((?:X-IQ-Provider-Key|x-api-key|api-key):\s*)[^\s,}]+`), `${1}` + marker},
	{regexp.MustCompile(`(?i)("?(?:x-iq-provider-key|authorization|x-api-key|api-key|provider_key|api_key)"?\s*[:=]\s*"?)[^"\s,}]+("?)`), `${1}` + marker + `${2}`},
	{regexp.MustCompile(`\bsk-[A-Za-z0-9_-]{8,}\b`), `[redacted-api-key]`},
	{regexp.MustCompile(`\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9_]{8,}\b`), `[redacted-github-token]`},
	{regexp.MustCompile(`\bgithub_pat_[A-Za-z0-9_]{8,}\b`), `[redacted-github-token]`},
	{regexp.MustCompile(`\bAKIA[0-9A-Z]{16}\b`), `[redacted-aws-key]`},
	{regexp.MustCompile(`\bxox[baprs]-[A-Za-z0-9-]{20,}\b`), `[redacted-slack-token]`},
	{regexp.MustCompile(`\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b`), `[redacted-jwt]`},
	{regexp.MustCompile(`(?is)-----BEGIN (?:[A-Z ]+ )?PRIVATE KEY-----.*?-----END (?:[A-Z ]+ )?PRIVATE KEY-----`), `[redacted-private-key]`},
	// AWS secret access keys in key-value context
	{regexp.MustCompile(`(?i)("?(?:aws[_-]secret[_-]access[_-]key|secret[_-]access[_-]key)"?\s*[:=]\s*"?)[A-Za-z0-9/+=]{20,}("?)`), `${1}` + marker + `${2}`},
	// Azure API / subscription keys in key-value context
	{regexp.MustCompile(`(?i)("?(?:ocp-apim-subscription-key|azure[_-]openai[_-]api[_-]key|azure[_-]api[_-]key)"?\s*[:=]\s*"?)[A-Za-z0-9]{20,}("?)`), `${1}` + marker + `${2}`},
}

// String redacts common API key and bearer-token shapes from arbitrary text.
func String(value string) string {
	out := value
	for _, rule := range stringRules {
		out = rule.re.ReplaceAllString(out, rule.repl)
	}
	return out
}

// ValueForKey redacts the entire value when the attribute/header key itself is
// sensitive, otherwise it redacts known secret shapes inside the value.
func ValueForKey(key, value string) string {
	if SensitiveKey(key) {
		return marker
	}
	return String(value)
}

// SensitiveKey reports whether a log attribute or HTTP header key should never
// expose its value.
func SensitiveKey(key string) bool {
	normalized := strings.NewReplacer("-", "", "_", "", " ", "").Replace(strings.ToLower(key))
	switch normalized {
	case "authorization", "xiqproviderkey", "xapikey", "apikey", "providerkey", "providerapikey",
		"awssecretaccesskey", "secretaccesskey",
		"azureapikey", "azureopenaiapikey", "ocpapimsubscriptionkey":
		return true
	default:
		return false
	}
}

// Truncate keeps UI/log-safe messages bounded.
func Truncate(value string, max int) string {
	if max <= 0 || len(value) <= max {
		return value
	}
	if max <= 3 {
		return validPrefix(value, max)
	}
	return validPrefix(value, max-3) + "..."
}

func validPrefix(value string, max int) string {
	if max >= len(value) {
		return value
	}
	for max > 0 && !utf8.ValidString(value[:max]) {
		max--
	}
	return value[:max]
}
