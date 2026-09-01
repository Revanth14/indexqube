package workspace

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestCaptureUsesDirtyWorkspaceAsBaseline(t *testing.T) {
	root := initRepo(t)
	tracked := filepath.Join(root, "tracked.txt")
	if err := os.WriteFile(tracked, []byte("user dirty change\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "existing-untracked.txt"), []byte("baseline\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	identity, err := Resolve(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	before, err := Capture(context.Background(), identity, "task", "turn", "pre")
	if err != nil {
		t.Fatal(err)
	}
	added := filepath.Join(root, "agent-file.txt")
	if err := os.WriteFile(added, []byte("agent\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	after, err := Capture(context.Background(), identity, "task", "turn", "post")
	if err != nil {
		t.Fatal(err)
	}
	if before.Fingerprint == after.Fingerprint {
		t.Fatal("agent-introduced untracked file did not change fingerprint")
	}
	deltas := DiffFileStates(before, after)
	if len(deltas) != 1 || deltas[0].Path != "agent-file.txt" || deltas[0].Operation != "added" {
		t.Fatalf("deltas=%+v", deltas)
	}
	if err := os.Remove(added); err != nil {
		t.Fatal(err)
	}
	restored, err := Capture(context.Background(), identity, "task", "turn", "restored")
	if err != nil {
		t.Fatal(err)
	}
	if restored.Fingerprint != before.Fingerprint {
		t.Fatalf("restored fingerprint=%s want baseline=%s", restored.Fingerprint, before.Fingerprint)
	}
}

func TestDiffFileStatesTracksDirtyBaselineContentChange(t *testing.T) {
	root := initRepo(t)
	tracked := filepath.Join(root, "tracked.txt")
	if err := os.WriteFile(tracked, []byte("user dirty baseline\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "existing-untracked.txt"), []byte("keep me\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	identity, err := Resolve(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	before, err := Capture(context.Background(), identity, "task", "turn", "pre")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tracked, []byte("user dirty baseline plus agent edit\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	after, err := Capture(context.Background(), identity, "task", "turn", "post")
	if err != nil {
		t.Fatal(err)
	}
	deltas := DiffFileStates(before, after)
	if len(deltas) != 1 || deltas[0].Path != "tracked.txt" || deltas[0].Operation != "modified" {
		t.Fatalf("deltas=%+v", deltas)
	}
}

func TestDiffFileStatesClassifiesDeleteAndRename(t *testing.T) {
	for _, tc := range []struct {
		name          string
		mutate        func(*testing.T, string)
		wantPath      string
		wantPrevious  string
		wantOperation string
	}{
		{
			name: "delete", wantPath: "tracked.txt", wantOperation: "deleted",
			mutate: func(t *testing.T, root string) {
				if err := os.Remove(filepath.Join(root, "tracked.txt")); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "rename", wantPath: "renamed file.txt", wantPrevious: "tracked.txt", wantOperation: "renamed",
			mutate: func(t *testing.T, root string) {
				runGit(t, root, "mv", "tracked.txt", "renamed file.txt")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := initRepo(t)
			identity, err := Resolve(context.Background(), root)
			if err != nil {
				t.Fatal(err)
			}
			before, err := Capture(context.Background(), identity, "task", "turn", "pre")
			if err != nil {
				t.Fatal(err)
			}
			tc.mutate(t, root)
			after, err := Capture(context.Background(), identity, "task", "turn", "post")
			if err != nil {
				t.Fatal(err)
			}
			deltas := DiffFileStates(before, after)
			if len(deltas) != 1 || deltas[0].Path != tc.wantPath || deltas[0].PreviousPath != tc.wantPrevious || deltas[0].Operation != tc.wantOperation {
				t.Fatalf("deltas=%+v", deltas)
			}
		})
	}
}

func TestDiffFileStatesDirtyBaselineRenameIsOneDelta(t *testing.T) {
	root := initRepo(t)
	if err := os.WriteFile(filepath.Join(root, "tracked.txt"), []byte("dirty before rename\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	identity, err := Resolve(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	before, err := Capture(context.Background(), identity, "task", "turn", "pre")
	if err != nil {
		t.Fatal(err)
	}
	runGit(t, root, "mv", "tracked.txt", "renamed.txt")
	after, err := Capture(context.Background(), identity, "task", "turn", "post")
	if err != nil {
		t.Fatal(err)
	}
	deltas := DiffFileStates(before, after)
	if len(deltas) != 1 || deltas[0].Path != "renamed.txt" || deltas[0].PreviousPath != "tracked.txt" || deltas[0].Operation != "renamed" {
		t.Fatalf("deltas=%+v", deltas)
	}
}

func TestDiffFileStatesDetectsStagedDirtyBaselineChange(t *testing.T) {
	root := initRepo(t)
	tracked := filepath.Join(root, "tracked.txt")
	if err := os.WriteFile(tracked, []byte("first staged baseline\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, root, "add", "tracked.txt")
	identity, err := Resolve(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	before, err := Capture(context.Background(), identity, "task", "turn", "pre")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tracked, []byte("second staged value\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, root, "add", "tracked.txt")
	after, err := Capture(context.Background(), identity, "task", "turn", "post")
	if err != nil {
		t.Fatal(err)
	}
	deltas := DiffFileStates(before, after)
	if len(deltas) != 1 || deltas[0].Path != "tracked.txt" || deltas[0].Operation != "modified" {
		t.Fatalf("deltas=%+v", deltas)
	}
}

func initRepo(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	runGit(t, root, "init", "-q")
	runGit(t, root, "config", "user.email", "test@indexqube.local")
	runGit(t, root, "config", "user.name", "IndexQube Test")
	if err := os.WriteFile(filepath.Join(root, "tracked.txt"), []byte("committed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, root, "add", "tracked.txt")
	runGit(t, root, "commit", "-q", "-m", "initial")
	return root
}

func runGit(t *testing.T, root string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", root}, args...)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
}
