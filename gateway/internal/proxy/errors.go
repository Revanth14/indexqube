package proxy

import (
	"context"
	"errors"
	"regexp"
	"strconv"
	"strings"

	"github.com/Revanth14/indexqube/gateway/internal/redact"
)

const maxClientErrorMessage = 700

var providerStatusPattern = regexp.MustCompile(`(?i)status[=:\s]+(\d{3})`)

func upstreamErrorPayload(err error) errorPayload {
	code, message := classifyUpstreamError(err)
	return errorPayload{
		Type:    "upstream_error",
		Code:    code,
		Message: message,
	}
}

func safeErrorPayload(payload errorPayload) errorPayload {
	payload.Message = safeClientMessage(payload.Message)
	return payload
}

func safeClientMessage(message string) string {
	clean := strings.Join(strings.Fields(message), " ")
	if clean == "" {
		clean = "request failed"
	}
	return redact.Truncate(redact.String(clean), maxClientErrorMessage)
}

func safeLogError(err error) string {
	if err == nil {
		return ""
	}
	return safeClientMessage(err.Error())
}

func classifyUpstreamError(err error) (string, string) {
	if err == nil {
		return "provider_error", "Provider request failed."
	}
	if errors.Is(err, context.Canceled) {
		return "request_cancelled", "Request was cancelled."
	}

	raw := err.Error()
	lower := strings.ToLower(raw)
	status := providerHTTPStatus(raw)

	switch {
	case status == 401 || status == 403 || strings.Contains(lower, "invalid key") || strings.Contains(lower, "api key"):
		return "provider_key_invalid", "Provider rejected the API key. Update your provider key and retry."
	case status == 429 || strings.Contains(lower, "rate limit"):
		return "provider_rate_limited", "Provider rate-limited this request. Try again later or switch provider/model."
	case status == 402 || strings.Contains(lower, "insufficient_quota") || strings.Contains(lower, "quota") || strings.Contains(lower, "balance") || strings.Contains(lower, "credit"):
		return "provider_balance_exhausted", "Provider account has insufficient quota or balance."
	case status == 408 || status == 504 || strings.Contains(lower, "timeout") || strings.Contains(lower, "deadline exceeded"):
		return "provider_timeout", "Provider request timed out."
	case status >= 500 || strings.Contains(lower, "unavailable") || strings.Contains(lower, "overloaded"):
		return "provider_unavailable", "Provider is unavailable or overloaded. Try again shortly."
	case status == 400 && (strings.Contains(lower, "context_length") || strings.Contains(lower, "context length") ||
		strings.Contains(lower, "maximum context") || strings.Contains(lower, "token limit") ||
		strings.Contains(lower, "too large") || strings.Contains(lower, "request too large")):
		return "gateway_context_too_large", "Request exceeds the provider context window. Reduce context size and retry."
	default:
		return "provider_error", "Provider request failed. Check gateway logs with the request ID."
	}
}

func providerHTTPStatus(message string) int {
	match := providerStatusPattern.FindStringSubmatch(message)
	if len(match) != 2 {
		return 0
	}
	status, err := strconv.Atoi(match[1])
	if err != nil {
		return 0
	}
	return status
}
