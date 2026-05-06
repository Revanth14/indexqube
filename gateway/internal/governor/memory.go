package governor

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/Revanth14/indexqube/gateway/internal/domain"
)

const projectMemoryTitle = "# IndexQube project memory (indexqube_context)"

// LoadProjectMemory reads the static project-memory markdown file. Missing
// files are treated as empty memory so local development does not require one.
func LoadProjectMemory(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		return "", fmt.Errorf("load project memory %q: %w", path, err)
	}
	return strings.TrimSpace(string(b)), nil
}

// MergeProjectMemory combines the gateway's static project memory with any
// request-scoped memory supplied by a client. Static rules come first so they
// frame the project; request memory can add short-lived session detail.
func MergeProjectMemory(staticMemory, requestMemory string) string {
	staticMemory = strings.TrimSpace(staticMemory)
	requestMemory = strings.TrimSpace(requestMemory)
	switch {
	case staticMemory == "" && requestMemory == "":
		return ""
	case staticMemory == "":
		return "## Request memory\n\n" + requestMemory
	case requestMemory == "":
		return "## Static project rules\n\n" + staticMemory
	default:
		return "## Static project rules\n\n" + staticMemory + "\n\n## Request memory\n\n" + requestMemory
	}
}

// InjectProjectMemory prepends merged project memory as a leading system
// instruction. Message order after the system instruction is preserved.
func InjectProjectMemory(msgs []domain.Message, mem string) []domain.Message {
	mem = strings.TrimSpace(mem)
	if mem == "" {
		return msgs
	}
	sys := domain.Message{
		Role:    "system",
		Content: projectMemoryTitle + "\n\n" + mem,
	}
	out := make([]domain.Message, 0, len(msgs)+1)
	out = append(out, sys)
	out = append(out, msgs...)
	return out
}
