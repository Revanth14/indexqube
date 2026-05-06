package domain

import (
	"crypto/sha256"
	"encoding/hex"
)

// ResolveTenantKey picks the pruning-history namespace. SessionKey wins when
// set (Chrome Path A); otherwise we hash the upstream API key (Path B BYO);
// otherwise a shared anonymous bucket (no isolation — avoid in production).
func ResolveTenantKey(sessionKey, apiKey string) string {
	if sessionKey != "" {
		sum := sha256.Sum256([]byte(sessionKey))
		return "sess:" + hex.EncodeToString(sum[:])
	}
	if apiKey != "" {
		sum := sha256.Sum256([]byte(apiKey))
		return "key:" + hex.EncodeToString(sum[:])
	}
	return "anon"
}
