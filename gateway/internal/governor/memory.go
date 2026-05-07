package governor

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/Revanth14/indexqube/gateway/internal/domain"
)

const projectMemoryTitle = "# IndexQube project memory (indexqube_context)"

// LoadProjectMemory reads the static project-memory markdown. It supports
// both a single file or a directory. If path is a directory, it merges all
// .md files in alphabetical order with section headers. Missing paths are
// treated as empty memory.
func LoadProjectMemory(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", nil
	}

	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		return "", fmt.Errorf("stat project memory path %q: %w", path, err)
	}

	if !info.IsDir() {
		b, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("read project memory file %q: %w", path, err)
		}
		return strings.TrimSpace(string(b)), nil
	}

	// It's a directory. Merge all .md files.
	var files []string
	err = filepath.WalkDir(path, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.ToLower(filepath.Ext(p)) == ".md" {
			files = append(files, p)
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("walk project memory dir %q: %w", path, err)
	}

	sort.Strings(files)

	var sb strings.Builder
	for i, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return "", fmt.Errorf("read project memory file %q: %w", f, err)
		}
		content := strings.TrimSpace(string(b))
		if content == "" {
			continue
		}

		if i > 0 {
			sb.WriteString("\n\n---\n\n")
		}

		// Add a header for each file to maintain "hierarchy" in the final prompt
		rel, _ := filepath.Rel(path, f)
		sb.WriteString(fmt.Sprintf("## File: %s\n\n", rel))
		sb.WriteString(content)
	}

	return strings.TrimSpace(sb.String()), nil
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
