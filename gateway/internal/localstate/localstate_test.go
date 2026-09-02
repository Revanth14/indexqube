package localstate

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDirPrefersIndexQubeHome(t *testing.T) {
	want := filepath.Join(t.TempDir(), "custom-state")
	t.Setenv("INDEXQUBE_HOME", want)
	t.Setenv("HOME", filepath.Join(t.TempDir(), "ignored-home"))

	got, err := Dir()
	if err != nil {
		t.Fatalf("Dir: %v", err)
	}
	if got != want {
		t.Fatalf("Dir = %q, want %q", got, want)
	}
}

func TestEnsureUsesDefaultHome(t *testing.T) {
	home := t.TempDir()
	t.Setenv("INDEXQUBE_HOME", "")
	t.Setenv("HOME", home)

	got, err := Ensure()
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	want := filepath.Join(home, ".indexqube")
	if got != want {
		t.Fatalf("Ensure = %q, want %q", got, want)
	}
	info, err := os.Stat(want)
	if err != nil {
		t.Fatalf("stat state dir: %v", err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("state dir mode = %o, want 700", info.Mode().Perm())
	}
}

func TestEnsureRepairsExistingDirectoryPermissions(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("INDEXQUBE_HOME", dir)
	if _, err := Ensure(); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("state dir mode=%o, want 700", info.Mode().Perm())
	}
}

func TestEnsureRejectsSymbolicLinkStateDirectory(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "state")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	t.Setenv("INDEXQUBE_HOME", link)
	if _, err := Ensure(); err == nil {
		t.Fatal("Ensure accepted symbolic-link state directory")
	}
}

func TestEnsureRejectsFilesystemRoot(t *testing.T) {
	t.Setenv("INDEXQUBE_HOME", string(filepath.Separator))
	if _, err := Ensure(); err == nil {
		t.Fatal("Ensure accepted filesystem root as state directory")
	}
}
