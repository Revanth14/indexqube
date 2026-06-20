// Package contextopt contains provider-neutral context optimization helpers.
package contextopt

import "strings"

var protectedInstructionPathFragments = [...]string{
	"claude.md",
	"context.md",
	"agents.md",
	".cursorrules",
	".cursor/rules/",
	".github/copilot-instructions.md",
}

// IsProtectedContent reports whether a span should be preserved regardless of
// optimization policy because it contains project instructions or credentials.
func IsProtectedContent(sourcePath, text string) bool {
	return ContainsProtectedInstructionPath(sourcePath) ||
		ContainsProtectedInstructionPath(text) ||
		ContainsCredentialMarker(text)
}

// ContainsCredentialMarker reports whether text appears to contain auth
// material. These spans must never be replaced with references.
func ContainsCredentialMarker(s string) bool {
	lower := strings.ToLower(s)
	return strings.Contains(lower, "api-key") ||
		strings.Contains(lower, "bearer ") ||
		strings.Contains(lower, "x-anthropic-api-key") ||
		strings.Contains(lower, "authorization")
}

// ContainsProtectedInstructionPath reports whether text or a path references
// known agent instruction files that must remain visible to the model.
func ContainsProtectedInstructionPath(s string) bool {
	s = strings.ToLower(strings.ReplaceAll(s, "\\", "/"))
	if strings.TrimSpace(s) == "" {
		return false
	}
	for _, fragment := range protectedInstructionPathFragments {
		if strings.Contains(s, fragment) {
			return true
		}
	}
	return false
}
