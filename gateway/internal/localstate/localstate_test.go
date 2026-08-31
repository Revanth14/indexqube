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
