// Package workspace owns repository identity, observable workspace snapshots,
// and exclusive write guards. Git and the filesystem remain authoritative;
// snapshots only record what IndexQube observed at a turn boundary.
package workspace

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/taskstore"
)

const maxStoredDiff = 256 << 10
const maxStatusSummary = 64 << 10

type Identity struct {
	ID   string
	Root string
}

func Resolve(ctx context.Context, path string) (Identity, error) {
	if strings.TrimSpace(path) == "" {
		return Identity{}, fmt.Errorf("workspace: empty path")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return Identity{}, fmt.Errorf("workspace: absolute path: %w", err)
	}
	rootRaw, err := gitOutput(ctx, abs, "rev-parse", "--show-toplevel")
	if err != nil {
		return Identity{}, fmt.Errorf("workspace: resolve git root: %w", err)
	}
	root := strings.TrimSpace(string(rootRaw))
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return Identity{}, fmt.Errorf("workspace: resolve symlinks: %w", err)
	}
	root, err = filepath.Abs(root)
	if err != nil {
		return Identity{}, fmt.Errorf("workspace: canonical path: %w", err)
	}
	sum := sha256.Sum256([]byte(root))
	return Identity{ID: "ws_" + hex.EncodeToString(sum[:16]), Root: root}, nil
}

func Capture(ctx context.Context, identity Identity, taskID, turnID, phase string) (taskstore.WorkspaceSnapshot, error) {
	head := optionalGitOutput(ctx, identity.Root, "rev-parse", "HEAD")
	branch := optionalGitOutput(ctx, identity.Root, "symbolic-ref", "--short", "HEAD")
	if branch == "" {
		branch = "HEAD"
	}
	staged, err := gitOutput(ctx, identity.Root, "diff", "--binary", "--cached", "--no-ext-diff")
	if err != nil {
		return taskstore.WorkspaceSnapshot{}, fmt.Errorf("workspace: staged diff: %w", err)
	}
	unstaged, err := gitOutput(ctx, identity.Root, "diff", "--binary", "--no-ext-diff")
	if err != nil {
		return taskstore.WorkspaceSnapshot{}, fmt.Errorf("workspace: unstaged diff: %w", err)
	}
	untracked, err := untrackedManifest(ctx, identity.Root)
	if err != nil {
		return taskstore.WorkspaceSnapshot{}, err
	}
	status, err := gitOutput(ctx, identity.Root, "status", "--porcelain=v1", "--untracked-files=all")
	if err != nil {
		return taskstore.WorkspaceSnapshot{}, fmt.Errorf("workspace: status: %w", err)
	}

	stagedHash := hashBytes(staged)
	unstagedHash := hashBytes(unstaged)
	untrackedHash := hashBytes(untracked)
	fingerprint := hashStrings(head, branch, stagedHash, unstagedHash, untrackedHash)
	diff := append(append([]byte("STAGED\n"), staged...), []byte("\nUNSTAGED\n")...)
	diff = append(diff, unstaged...)

	return taskstore.WorkspaceSnapshot{
		ID:            taskstore.NewID("snap"),
		TaskID:        taskID,
		TurnID:        turnID,
		Phase:         phase,
		WorkspaceID:   identity.ID,
		HeadCommit:    head,
		Branch:        branch,
		StagedHash:    stagedHash,
		UnstagedHash:  unstagedHash,
		UntrackedHash: untrackedHash,
		Fingerprint:   fingerprint,
		StatusSummary: boundedString(status, maxStatusSummary),
		BoundedDiff:   boundedString(diff, maxStoredDiff),
		CapturedAt:    time.Now().UTC(),
	}, nil
}

func gitOutput(ctx context.Context, root string, args ...string) ([]byte, error) {
	cmdArgs := append([]string{"-C", root}, args...)
	cmd := exec.CommandContext(ctx, "git", cmdArgs...)
	cmd.Env = append(os.Environ(), "GIT_OPTIONAL_LOCKS=0")
	out, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return nil, fmt.Errorf("git %s: %s", strings.Join(args, " "), strings.TrimSpace(string(exitErr.Stderr)))
		}
		return nil, err
	}
	return out, nil
}

func optionalGitOutput(ctx context.Context, root string, args ...string) string {
	out, err := gitOutput(ctx, root, args...)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func untrackedManifest(ctx context.Context, root string) ([]byte, error) {
	raw, err := gitOutput(ctx, root, "ls-files", "--others", "--exclude-standard", "-z")
	if err != nil {
		return nil, fmt.Errorf("workspace: untracked files: %w", err)
	}
	parts := bytes.Split(raw, []byte{0})
	paths := make([]string, 0, len(parts))
	for _, part := range parts {
		if len(part) > 0 {
			paths = append(paths, string(part))
		}
	}
	sort.Strings(paths)
	var manifest bytes.Buffer
	for _, rel := range paths {
		path, err := safeWorkspacePath(root, rel)
		if err != nil {
			return nil, err
		}
		info, err := os.Lstat(path)
		if err != nil {
			return nil, fmt.Errorf("workspace: stat untracked %q: %w", rel, err)
		}
		var digest string
		switch {
		case info.Mode()&os.ModeSymlink != 0:
			target, err := os.Readlink(path)
			if err != nil {
				return nil, err
			}
			digest = hashBytes([]byte("symlink:" + target))
		case info.Mode().IsRegular():
			digest, err = hashFile(path)
			if err != nil {
				return nil, err
			}
		default:
			digest = hashBytes([]byte(info.Mode().String()))
		}
		fmt.Fprintf(&manifest, "%s\x00%s\x00", filepath.ToSlash(rel), digest)
	}
	return manifest.Bytes(), nil
}

func safeWorkspacePath(root, rel string) (string, error) {
	clean := filepath.Clean(rel)
	if filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("workspace: unsafe path %q", rel)
	}
	return filepath.Join(root, clean), nil
}

func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("workspace: hash %q: %w", path, err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("workspace: hash %q: %w", path, err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func hashBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func hashStrings(values ...string) string {
	h := sha256.New()
	for _, value := range values {
		_, _ = io.WriteString(h, value)
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func boundedString(data []byte, limit int) string {
	if len(data) <= limit {
		return string(data)
	}
	return string(data[:limit]) + "\n[indexqube: truncated]\n"
}
