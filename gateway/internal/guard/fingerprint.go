package guard

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

type FingerprintInput struct {
	Route             string
	Model             string
	MessageCount      int
	ContentBlockCount int
	ToolResultCount   int
	AttemptedTokens   int
	BlocksAnalyzed    int
	BlocksPruned      int
	LatestUserText    string
	SystemText        string
}

func BuildFingerprint(input FingerprintInput) string {
	normalized := strings.Join([]string{
		fmt.Sprintf("route=%s", normalize(input.Route)),
		fmt.Sprintf("model=%s", normalize(input.Model)),
		fmt.Sprintf("messages=%s", countBucket(input.MessageCount)),
		fmt.Sprintf("content_blocks=%s", countBucket(input.ContentBlockCount)),
		fmt.Sprintf("tool_results=%s", countBucket(input.ToolResultCount)),
		fmt.Sprintf("tokens=%s", tokenBucket(input.AttemptedTokens)),
		fmt.Sprintf("blocks_analyzed=%s", countBucket(input.BlocksAnalyzed)),
		fmt.Sprintf("blocks_pruned=%s", countBucket(input.BlocksPruned)),
		fmt.Sprintf("latest=%s", hashPrefix(input.LatestUserText)),
		fmt.Sprintf("system=%s", hashPrefix(input.SystemText)),
	}, "\n")
	sum := sha256.Sum256([]byte(normalized))
	return hex.EncodeToString(sum[:])
}

func normalize(s string) string {
	v := strings.TrimSpace(strings.ToLower(s))
	if v == "" {
		return "unknown"
	}
	return v
}

func hashPrefix(s string) string {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return "empty"
	}
	sum := sha256.Sum256([]byte(trimmed))
	return hex.EncodeToString(sum[:])[:12]
}

func countBucket(n int) string {
	switch {
	case n <= 0:
		return "0"
	case n == 1:
		return "1"
	case n <= 3:
		return "2-3"
	case n <= 10:
		return "4-10"
	case n <= 25:
		return "11-25"
	case n <= 50:
		return "26-50"
	case n <= 100:
		return "51-100"
	default:
		return "100+"
	}
}

func tokenBucket(n int) string {
	switch {
	case n < 10_000:
		return "0-10k"
	case n < 25_000:
		return "10k-25k"
	case n < 50_000:
		return "25k-50k"
	case n < 100_000:
		return "50k-100k"
	case n < 200_000:
		return "100k-200k"
	default:
		return "200k+"
	}
}
