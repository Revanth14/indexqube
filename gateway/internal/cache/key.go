package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/Revanth14/indexqube/gateway/internal/domain"
)

// keyMaterial is the canonical, deterministically-marshaled view of an
// inference request used as the body of the cache key hash. Field tags
// are short to keep the marshaled blob compact -- it never leaves
// memory, but smaller is faster to hash.
//
// Excluded fields and why:
//   - Stream: cache stores chunks; replay shape is identical regardless.
//   - Credential.APIKey: hashed separately so it forms the tenant
//     boundary without entering the marshaled JSON (defense in depth).
//   - Credential.Provider: included via Provider field below.
type keyMaterial struct {
	Provider       string           `json:"p"`
	Model          string           `json:"m"`
	MaxTokens      int              `json:"mt,omitempty"`
	Temperature    float64          `json:"t,omitempty"`
	Messages       []domain.Message `json:"msgs"`
	MemFingerprint string           `json:"mf,omitempty"` // SHA-256 hex of ProjectMemory when set
}

// DeriveKey is the deterministic key derivation used for both reads and
// writes. The same logical request produces the same Key on every host.
//
// Tenant scoping: the API key bytes are hashed BEFORE the request body,
// so two callers with different keys get different cache slots even
// when message content is byte-identical. This is the "BYO-Key implies
// per-key cache namespace" property.
func DeriveKey(req *domain.InferenceRequest) (Key, error) {
	memFP := ""
	if req.ProjectMemory != "" {
		sum := sha256.Sum256([]byte(req.ProjectMemory))
		memFP = hex.EncodeToString(sum[:])
	}
	body, err := json.Marshal(keyMaterial{
		Provider:       string(req.Credential.Provider),
		Model:          req.Model,
		MaxTokens:      req.MaxTokens,
		Temperature:    req.Temperature,
		Messages:       req.Messages,
		MemFingerprint: memFP,
	})
	if err != nil {
		return "", err
	}
	h := sha256.New()
	h.Write([]byte(req.Credential.APIKey))
	h.Write([]byte{0x00}) // separator: prevents (key="ab"+body="cd") == (key="a"+body="bcd")
	h.Write(body)
	return Key(hex.EncodeToString(h.Sum(nil))), nil
}
