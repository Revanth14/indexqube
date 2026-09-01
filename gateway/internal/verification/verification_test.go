package verification

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDetectGoChecksUsesNearestChangedModule(t *testing.T) {
	root := t.TempDir()
	module := filepath.Join(root, "services", "api")
	if err := os.MkdirAll(filepath.Join(module, "internal"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(module, "go.mod"), []byte("module example.com/api\n\ngo 1.22\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	checks := detectGoChecks(root, []string{"services/api/internal/handler.go", "README.md"})
	if len(checks) != 1 {
		t.Fatalf("checks=%+v", checks)
	}
	if checks[0].cwd != "services/api" || checks[0].command != "go test -mod=readonly ./..." {
		t.Fatalf("check=%+v", checks[0])
	}
}

func TestLocalVerifierRecordsPassedAndFailedGoChecks(t *testing.T) {
	for _, tc := range []struct {
		name       string
		testSource string
		want       Status
	}{
		{name: "passed", testSource: "package sample\nimport \"testing\"\nfunc TestValue(t *testing.T) {}\n", want: StatusVerified},
		{name: "failed", testSource: "package sample\nimport \"testing\"\nfunc TestValue(t *testing.T) { t.Fatal(\"nope\") }\n", want: StatusFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/sample\n\ngo 1.22\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "sample.go"), []byte("package sample\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "sample_test.go"), []byte(tc.testSource), 0o600); err != nil {
				t.Fatal(err)
			}
			verifier := NewLocalVerifier()
			verifier.Timeout = 20 * time.Second
			result := verifier.Verify(context.Background(), Request{
				Workspace: root, ChangedPaths: []string{"sample.go"},
			})
			if result.Status != tc.want || len(result.Checks) != 1 {
				t.Fatalf("result=%+v", result)
			}
			if result.Checks[0].ExitCode == nil {
				t.Fatalf("check=%+v", result.Checks[0])
			}
		})
	}
}

func TestLocalVerifierSkipsUnsupportedChanges(t *testing.T) {
	result := NewLocalVerifier().Verify(context.Background(), Request{
		Workspace: t.TempDir(), ChangedPaths: []string{"README.md"},
	})
	if result.Status != StatusSkipped || len(result.Checks) != 0 || result.Summary == "" {
		t.Fatalf("result=%+v", result)
	}
}

func TestDetectGoChecksRejectsSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "go.mod"), []byte("module example.com/outside\n\ngo 1.22\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outside, "escape.go"), []byte("package outside\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "linked")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if checks := detectGoChecks(root, []string{"linked/escape.go"}); len(checks) != 0 {
		t.Fatalf("checks escaped workspace: %+v", checks)
	}
}
