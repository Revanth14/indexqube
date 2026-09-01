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
	snapshotID := taskstore.NewID("snap")
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
	statusZ, err := gitOutput(ctx, identity.Root, "status", "--porcelain=v1", "-z", "--untracked-files=all")
	if err != nil {
		return taskstore.WorkspaceSnapshot{}, fmt.Errorf("workspace: file states: %w", err)
	}
	files, err := captureFileStates(ctx, identity.Root, snapshotID, taskID, turnID, statusZ)
	if err != nil {
		return taskstore.WorkspaceSnapshot{}, err
	}

	stagedHash := hashBytes(staged)
	unstagedHash := hashBytes(unstaged)
	untrackedHash := hashBytes(untracked)
	fingerprint := hashStrings(head, branch, stagedHash, unstagedHash, untrackedHash)
	diff := append(append([]byte("STAGED\n"), staged...), []byte("\nUNSTAGED\n")...)
	diff = append(diff, unstaged...)

	return taskstore.WorkspaceSnapshot{
		ID:            snapshotID,
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
		Files:         files,
	}, nil
}

func captureFileStates(ctx context.Context, root, snapshotID, taskID, turnID string, raw []byte) ([]taskstore.WorkspaceFileState, error) {
	indexByPath, err := indexEntries(ctx, root)
	if err != nil {
		return nil, err
	}
	states := make([]taskstore.WorkspaceFileState, 0)
	for len(raw) > 0 {
		end := bytes.IndexByte(raw, 0)
		if end < 0 {
			return nil, fmt.Errorf("workspace: malformed porcelain status")
		}
		entry := raw[:end]
		raw = raw[end+1:]
		if len(entry) < 4 || entry[2] != ' ' {
			return nil, fmt.Errorf("workspace: malformed porcelain entry")
		}
		indexStatus := statusCode(entry[0])
		worktreeStatus := statusCode(entry[1])
		path := string(entry[3:])
		originalPath := ""
		if entry[0] == 'R' || entry[0] == 'C' || entry[1] == 'R' || entry[1] == 'C' {
			originalEnd := bytes.IndexByte(raw, 0)
			if originalEnd < 0 {
				return nil, fmt.Errorf("workspace: malformed rename entry")
			}
			originalPath = string(raw[:originalEnd])
			raw = raw[originalEnd+1:]
		}
		path = filepath.ToSlash(filepath.Clean(path))
		if _, err := safeWorkspacePath(root, path); err != nil {
			return nil, err
		}
		if originalPath != "" {
			originalPath = filepath.ToSlash(filepath.Clean(originalPath))
			if _, err := safeWorkspacePath(root, originalPath); err != nil {
				return nil, err
			}
		}
		indexEntry := indexByPath[path]
		worktreeFingerprint, err := worktreePathFingerprint(root, path)
		if err != nil {
			return nil, err
		}
		states = append(states, taskstore.WorkspaceFileState{
			SnapshotID: snapshotID, TaskID: taskID, TurnID: turnID, Path: path, OriginalPath: originalPath,
			IndexStatus: indexStatus, WorktreeStatus: worktreeStatus,
			Fingerprint: hashStrings(path, originalPath, indexStatus, worktreeStatus, indexEntry, worktreeFingerprint),
		})
	}
	sort.Slice(states, func(i, j int) bool { return states[i].Path < states[j].Path })
	return states, nil
}

func indexEntries(ctx context.Context, root string) (map[string]string, error) {
	raw, err := gitOutput(ctx, root, "ls-files", "--stage", "-z")
	if err != nil {
		return nil, fmt.Errorf("workspace: index entries: %w", err)
	}
	entries := make(map[string]string)
	for len(raw) > 0 {
		end := bytes.IndexByte(raw, 0)
		if end < 0 {
			return nil, fmt.Errorf("workspace: malformed index entry")
		}
		entry := raw[:end]
		raw = raw[end+1:]
		tab := bytes.IndexByte(entry, '\t')
		if tab < 0 {
			continue
		}
		path := filepath.ToSlash(filepath.Clean(string(entry[tab+1:])))
		entries[path] = string(entry[:tab])
	}
	return entries, nil
}

func statusCode(value byte) string {
	if value == ' ' {
		return ""
	}
	return string(value)
}

func worktreePathFingerprint(root, rel string) (string, error) {
	path, err := safeWorkspacePath(root, rel)
	if err != nil {
		return "", err
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return "missing", nil
	}
	if err != nil {
		return "", fmt.Errorf("workspace: stat %q: %w", rel, err)
	}
	switch {
	case info.Mode()&os.ModeSymlink != 0:
		target, err := os.Readlink(path)
		if err != nil {
			return "", err
		}
		return hashStrings("symlink", target), nil
	case info.Mode().IsRegular():
		digest, err := hashFile(path)
		if err != nil {
			return "", err
		}
		return hashStrings(info.Mode().String(), digest), nil
	default:
		return hashStrings(info.Mode().String(), fmt.Sprint(info.Size())), nil
	}
}

// DiffFileStates derives the authoritative net Git-visible mutation for one
// turn. It detects changes relative to a dirty baseline by comparing per-path
// fingerprints, not merely porcelain status letters.
func DiffFileStates(before, after taskstore.WorkspaceSnapshot) []taskstore.WorkspaceFileDelta {
	beforeByPath := make(map[string]taskstore.WorkspaceFileState, len(before.Files))
	afterByPath := make(map[string]taskstore.WorkspaceFileState, len(after.Files))
	for _, state := range before.Files {
		beforeByPath[state.Path] = state
	}
	for _, state := range after.Files {
		afterByPath[state.Path] = state
	}
	sourceBeforeByPath := make(map[string]taskstore.WorkspaceFileState)
	consumedBeforePaths := make(map[string]struct{})
	for _, state := range after.Files {
		if state.OriginalPath == "" {
			continue
		}
		if source, ok := beforeByPath[state.OriginalPath]; ok {
			sourceBeforeByPath[state.Path] = source
			if strings.Contains(state.IndexStatus+state.WorktreeStatus, "R") {
				consumedBeforePaths[state.OriginalPath] = struct{}{}
			}
		}
	}
	paths := make(map[string]struct{}, len(beforeByPath)+len(afterByPath))
	for path := range beforeByPath {
		paths[path] = struct{}{}
	}
	for path := range afterByPath {
		paths[path] = struct{}{}
	}
	deltas := make([]taskstore.WorkspaceFileDelta, 0)
	for path := range paths {
		beforeState, hadBefore := beforeByPath[path]
		afterState, hasAfter := afterByPath[path]
		if !hasAfter {
			if _, consumed := consumedBeforePaths[path]; consumed {
				continue
			}
		}
		if !hadBefore {
			if source, ok := sourceBeforeByPath[path]; ok {
				beforeState = source
				hadBefore = true
			}
		}
		if hadBefore && hasAfter && beforeState.Fingerprint == afterState.Fingerprint {
			continue
		}
		beforeFingerprint := ""
		if hadBefore {
			beforeFingerprint = beforeState.Fingerprint
		}
		afterFingerprint := ""
		previousPath := ""
		if hasAfter {
			afterFingerprint = afterState.Fingerprint
			previousPath = afterState.OriginalPath
		}
		deltas = append(deltas, taskstore.WorkspaceFileDelta{
			ID: taskstore.NewID("delta"), TaskID: after.TaskID, TurnID: after.TurnID, Path: path,
			PreviousPath: previousPath, Operation: fileDeltaOperation(beforeState, hadBefore, afterState, hasAfter),
			BeforeFingerprint: beforeFingerprint, AfterFingerprint: afterFingerprint, RecordedAt: time.Now().UTC(),
		})
	}
	sort.Slice(deltas, func(i, j int) bool {
		if deltas[i].Path == deltas[j].Path {
			return deltas[i].PreviousPath < deltas[j].PreviousPath
		}
		return deltas[i].Path < deltas[j].Path
	})
	return deltas
}

func fileDeltaOperation(before taskstore.WorkspaceFileState, hadBefore bool, after taskstore.WorkspaceFileState, hasAfter bool) string {
	if hasAfter {
		status := after.IndexStatus + after.WorktreeStatus
		switch {
		case strings.ContainsAny(status, "R") || after.OriginalPath != "":
			return "renamed"
		case strings.ContainsAny(status, "C"):
			return "copied"
		case strings.ContainsAny(status, "D"):
			return "deleted"
		case !hadBefore && (strings.ContainsAny(status, "A?") || after.IndexStatus == "?" || after.WorktreeStatus == "?"):
			return "added"
		default:
			return "modified"
		}
	}
	if before.IndexStatus == "?" || before.WorktreeStatus == "?" || strings.ContainsAny(before.IndexStatus+before.WorktreeStatus, "A") {
		return "deleted"
	}
	return "reverted"
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
