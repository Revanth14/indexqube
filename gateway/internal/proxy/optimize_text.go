package proxy

import "github.com/Revanth14/indexqube/gateway/internal/domain"

// NormalizeRawOptimizeText applies the same raw browser prompt normalization
// used by POST /v1/optimize before the governor sees a text/plain body.
func NormalizeRawOptimizeText(content, contextPath, contextLang string) string {
	return normalizeRawOptimizeText(content, contextPath, contextLang)
}

// RenderOptimizedText renders optimized messages exactly like the text/plain
// Path A response body.
func RenderOptimizedText(msgs []domain.Message) string {
	return renderOptimizedText(msgs)
}
