package verification

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	if mode := os.Getenv("INDEXQUBE_VERIFICATION_HELPER"); mode != "" {
		if mode == "sleep" {
			time.Sleep(30 * time.Second)
		}
		fmt.Printf("helper=%s offline=%s cargo_target=%s\n", filepath.Base(os.Args[0]), os.Getenv("npm_config_offline"), os.Getenv("CARGO_TARGET_DIR"))
		os.Exit(0)
	}
	os.Exit(m.Run())
}

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

func TestConfiguredRecipeOverridesAutomaticDetection(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/recipe\n\ngo 1.22\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "sample.go"), []byte("package sample\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	writeRecipe(t, root, `{
  "version": 1,
  "checks": [{
    "name": "Toolchain check",
    "kind": "build",
    "command": ["go", "version"],
    "paths": ["sample.go"],
    "timeout_seconds": 10
  }]
}`)

	result := NewLocalVerifier().Verify(context.Background(), Request{
		Workspace: root, ChangedPaths: []string{"sample.go"},
	})
	if result.Status != StatusVerified || len(result.Checks) != 1 {
		t.Fatalf("result=%+v", result)
	}
	if result.Checks[0].Name != "Toolchain check" || result.Checks[0].Kind != "build" || result.Checks[0].Command != "go version" {
		t.Fatalf("check=%+v", result.Checks[0])
	}
}

func TestConfiguredRecipePathFiltering(t *testing.T) {
	root := t.TempDir()
	writeRecipe(t, root, `{
  "version": 1,
  "checks": [{"name": "Frontend tests", "command": ["npm", "test"], "cwd": ".", "paths": ["web"]}]
}`)
	result := NewLocalVerifier().Verify(context.Background(), Request{
		Workspace: root, ChangedPaths: []string{"README.md"},
	})
	if result.Status != StatusSkipped || len(result.Checks) != 0 || !strings.Contains(result.Summary, "no configured verification recipe matched") {
		t.Fatalf("result=%+v", result)
	}
}

func TestConfiguredRecipeChangedDuringTurnFailsClosed(t *testing.T) {
	root := t.TempDir()
	writeRecipe(t, root, `{"version":1,"checks":[{"name":"Tests","command":["go","version"]}]}`)
	result := NewLocalVerifier().Verify(context.Background(), Request{
		Workspace: root, ChangedPaths: []string{RecipePath},
	})
	if result.Status != StatusFailed || len(result.Checks) != 1 || result.Checks[0].Kind != "configuration" ||
		!strings.Contains(result.Checks[0].Output, "changed during this turn") || result.Checks[0].ExitCode != nil {
		t.Fatalf("result=%+v", result)
	}
}

func TestUntrackedConfiguredRecipeFailsClosed(t *testing.T) {
	root := t.TempDir()
	runRecipeGit(t, root, "init", "-q")
	dir := filepath.Join(root, ".indexqube")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "verification.json"), []byte(`{"version":1,"checks":[{"name":"Tests","command":["go","version"]}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	result := NewLocalVerifier().Verify(context.Background(), Request{
		Workspace: root, ChangedPaths: []string{"README.md"},
	})
	if result.Status != StatusFailed || len(result.Checks) != 1 || !strings.Contains(result.Checks[0].Output, "must be Git-tracked") {
		t.Fatalf("result=%+v", result)
	}
}

func TestConfiguredRecipeValidationFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name     string
		document string
		want     string
	}{
		{name: "unknown field", document: `{"version":1,"unknown":true,"checks":[{"name":"Tests","command":["go","version"]}]}`, want: "unknown field"},
		{name: "shell", document: `{"version":1,"checks":[{"name":"Tests","command":["sh","-c","true"]}]}`, want: "cannot invoke a shell"},
		{name: "absolute executable", document: `{"version":1,"checks":[{"name":"Tests","command":["/bin/true"]}]}`, want: "workspace-relative"},
		{name: "protected environment", document: `{"version":1,"checks":[{"name":"Tests","command":["go","version"],"env":{"GOPROXY":"https://proxy.example"}}]}`, want: "protected variable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeRecipe(t, root, tc.document)
			result := NewLocalVerifier().Verify(context.Background(), Request{
				Workspace: root, ChangedPaths: []string{"README.md"},
			})
			if result.Status != StatusFailed || len(result.Checks) != 1 || !strings.Contains(result.Checks[0].Output, tc.want) {
				t.Fatalf("result=%+v", result)
			}
		})
	}
}

func TestConfiguredRecipeTimeoutIsBounded(t *testing.T) {
	root := t.TempDir()
	binDir := t.TempDir()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(binary, filepath.Join(binDir, "slow-helper")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("INDEXQUBE_VERIFICATION_HELPER", "sleep")
	writeRecipe(t, root, `{"version":1,"checks":[{"name":"Bounded check","command":["slow-helper"],"timeout_seconds":1}]}`)

	started := time.Now()
	result := NewLocalVerifier().Verify(context.Background(), Request{
		Workspace: root, ChangedPaths: []string{"README.md"},
	})
	if result.Status != StatusFailed || len(result.Checks) != 1 ||
		!strings.Contains(result.Checks[0].Output, "timed out after 1s") || time.Since(started) > 5*time.Second {
		t.Fatalf("elapsed=%s result=%+v", time.Since(started), result)
	}
}

func TestConfiguredRecipeRejectsSymlinkedCWDOutsideWorkspace(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "outside")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	writeRecipe(t, root, `{"version":1,"checks":[{"name":"Tests","command":["go","version"],"cwd":"outside"}]}`)
	result := NewLocalVerifier().Verify(context.Background(), Request{
		Workspace: root, ChangedPaths: []string{"README.md"},
	})
	if result.Status != StatusFailed || len(result.Checks) != 1 || !strings.Contains(result.Checks[0].Output, "outside the workspace") {
		t.Fatalf("result=%+v", result)
	}
}

func TestDetectNodeCheckUsesDeclaredPackageManager(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "package.json"), []byte(`{
  "packageManager": "pnpm@10.0.0",
  "scripts": {"test": "vitest run"}
}`), 0o600); err != nil {
		t.Fatal(err)
	}
	checks, err := detectNodeChecks(root, []string{"src/component.tsx"})
	if err != nil || len(checks) != 1 {
		t.Fatalf("checks=%+v err=%v", checks, err)
	}
	if checks[0].command != "pnpm test" || checks[0].name != "Node tests" || checks[0].env["npm_config_offline"] != "true" {
		t.Fatalf("check=%+v", checks[0])
	}
}

func TestDetectNodeCheckSkipsPackageWithoutTestScript(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "package.json"), []byte(`{"scripts":{"lint":"eslint ."}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	checks, err := detectNodeChecks(root, []string{"src/index.js"})
	if err != nil || len(checks) != 0 {
		t.Fatalf("checks=%+v err=%v", checks, err)
	}
}

func TestMalformedDetectedProjectFailsPlanning(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "package.json"), []byte(`{"scripts":`), 0o600); err != nil {
		t.Fatal(err)
	}
	result := NewLocalVerifier().Verify(context.Background(), Request{
		Workspace: root, ChangedPaths: []string{"src/index.js"},
	})
	if result.Status != StatusFailed || len(result.Checks) != 1 || result.Checks[0].Kind != "detection" ||
		result.Checks[0].Name != "Automatic verification detection" || !strings.Contains(result.Checks[0].Output, "package.json") {
		t.Fatalf("result=%+v", result)
	}
}

func TestDetectPythonCheckRequiresPytestSignal(t *testing.T) {
	for _, tc := range []struct {
		name      string
		pyproject string
		want      int
	}{
		{name: "configured", pyproject: "[tool.pytest.ini_options]\naddopts = \"-q\"\n", want: 1},
		{name: "not configured", pyproject: "[project]\nname = \"sample\"\n", want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "pyproject.toml"), []byte(tc.pyproject), 0o600); err != nil {
				t.Fatal(err)
			}
			checks, err := detectPythonChecks(root, []string{"src/sample.py"})
			if err != nil || len(checks) != tc.want {
				t.Fatalf("checks=%+v err=%v", checks, err)
			}
			if tc.want == 1 && (checks[0].command != "python3 -m pytest -p no:cacheprovider" || checks[0].env["PYTHONDONTWRITEBYTECODE"] != "1") {
				t.Fatalf("check=%+v", checks[0])
			}
		})
	}
}

func TestDetectPythonCheckUsesUVOffline(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "pyproject.toml"), []byte("[tool.pytest.ini_options]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "uv.lock"), []byte("version = 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	checks, err := detectPythonChecks(root, []string{"sample.py"})
	if err != nil || len(checks) != 1 || checks[0].command != "uv run --offline python -m pytest -p no:cacheprovider" {
		t.Fatalf("checks=%+v err=%v", checks, err)
	}
}

func TestDetectRustCheckUsesLockedOfflineModeAndTemporaryTarget(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "Cargo.toml"), []byte("[package]\nname = \"sample\"\nversion = \"0.1.0\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "Cargo.lock"), []byte("version = 4\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	checks, err := detectRustChecks(root, []string{"src/lib.rs"})
	if err != nil || len(checks) != 1 {
		t.Fatalf("checks=%+v err=%v", checks, err)
	}
	if checks[0].command != "cargo test --locked --offline" || !checks[0].temporaryTarget || checks[0].env["CARGO_NET_OFFLINE"] != "true" {
		t.Fatalf("check=%+v", checks[0])
	}
}

func TestPlanChecksCombinesDetectedEcosystemsDeterministically(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "frontend"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/mixed\n\ngo 1.22\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "frontend", "package.json"), []byte(`{"scripts":{"test":"node --test"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	checks, _, err := planChecks(root, []string{"main.go", "frontend/index.ts"})
	if err != nil || len(checks) != 2 {
		t.Fatalf("checks=%+v err=%v", checks, err)
	}
	if checks[0].cwd != "." || checks[1].cwd != "frontend" {
		t.Fatalf("checks not deterministic: %+v", checks)
	}
}

func TestLocalVerifierRunsDetectedNodePythonAndRustChecks(t *testing.T) {
	root := t.TempDir()
	for _, dir := range []string{"node", "python", "rust"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "node", "package.json"), []byte(`{"scripts":{"test":"node --test"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "python", "pyproject.toml"), []byte("[tool.pytest.ini_options]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "rust", "Cargo.toml"), []byte("[package]\nname = \"sample\"\nversion = \"0.1.0\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	binDir := t.TempDir()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"npm", "python3", "cargo"} {
		if err := os.Symlink(binary, filepath.Join(binDir, name)); err != nil {
			t.Skipf("symlink unavailable: %v", err)
		}
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("INDEXQUBE_VERIFICATION_HELPER", "1")

	verifier := NewLocalVerifier()
	verifier.Timeout = 10 * time.Second
	result := verifier.Verify(context.Background(), Request{
		Workspace: root,
		ChangedPaths: []string{
			"node/index.ts", "python/sample.py", "rust/src/lib.rs",
		},
	})
	if result.Status != StatusVerified || len(result.Checks) != 3 {
		t.Fatalf("result=%+v", result)
	}
	for _, check := range result.Checks {
		if check.ExitCode == nil || *check.ExitCode != 0 || !strings.Contains(check.Output, "helper=") {
			t.Fatalf("check=%+v", check)
		}
		if strings.HasPrefix(check.Name, "Node") && !strings.Contains(check.Output, "offline=true") {
			t.Fatalf("node offline environment missing: %+v", check)
		}
		if strings.HasPrefix(check.Name, "Rust") && !strings.Contains(check.Output, "cargo_target=") {
			t.Fatalf("rust temporary target missing: %+v", check)
		}
	}
}

func writeRecipe(t *testing.T, root, document string) {
	t.Helper()
	dir := filepath.Join(root, ".indexqube")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "verification.json"), []byte(document), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, ".git")); os.IsNotExist(err) {
		runRecipeGit(t, root, "init", "-q")
	}
	runRecipeGit(t, root, "add", "-f", RecipePath)
}

func runRecipeGit(t *testing.T, root string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", root}, args...)...)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v: %s", args, err, output)
	}
}
