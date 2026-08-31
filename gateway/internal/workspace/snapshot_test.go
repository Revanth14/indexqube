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
