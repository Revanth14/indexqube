// Package verification detects and runs conservative, task-scoped repository
// checks after a coding agent reports success.
package verification

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type Status string

const (
	StatusVerified Status = "verified"
	StatusFailed   Status = "verification_failed"
	StatusSkipped  Status = "verification_skipped"
)

type CheckStatus string

const (
	CheckPassed CheckStatus = "passed"
	CheckFailed CheckStatus = "failed"
)

type Request struct {
	Workspace    string
	ChangedPaths []string
	Guard        ProcessGuard
}

type ProcessGuard interface {
	PrepareCommand(*exec.Cmd) error
}

type CheckResult struct {
	Name        string
	Kind        string
	Command     string
	CWD         string
	Status      CheckStatus
	ExitCode    *int
	Output      string
	StartedAt   time.Time
	CompletedAt time.Time
}

type Result struct {
	Status      Status
	Summary     string
	StartedAt   time.Time
	CompletedAt time.Time
	Checks      []CheckResult
}

type Verifier interface {
	Verify(context.Context, Request) Result
}

type LocalVerifier struct {
	Timeout     time.Duration
	OutputLimit int
}

func NewLocalVerifier() *LocalVerifier {
	return &LocalVerifier{Timeout: 2 * time.Minute, OutputLimit: 64 << 10}
}

type checkSpec struct {
	name    string
	kind    string
	command string
	args    []string
	dir     string
	cwd     string
}

func (v *LocalVerifier) Verify(ctx context.Context, request Request) Result {
	started := time.Now().UTC()
	result := Result{Status: StatusSkipped, StartedAt: started}
	checks := detectGoChecks(request.Workspace, request.ChangedPaths)
	if len(checks) == 0 {
		result.Summary = "no supported project verification detected for the changed paths"
		result.CompletedAt = time.Now().UTC()
		return result
	}

	timeout := v.Timeout
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	outputLimit := v.OutputLimit
	if outputLimit <= 0 {
		outputLimit = 64 << 10
	}
	result.Status = StatusVerified
	for _, check := range checks {
		checkStarted := time.Now().UTC()
		checkCtx, cancel := context.WithTimeout(ctx, timeout)
		cmd := exec.CommandContext(checkCtx, check.args[0], check.args[1:]...)
		cmd.Dir = check.dir
		// Detected verification must not fetch dependencies or rewrite module
		// metadata. A cold dependency cache therefore fails visibly instead of
		// turning a local check into an implicit network operation.
		cmd.Env = offlineGoEnv()
		output := &boundedBuffer{limit: outputLimit}
		cmd.Stdout = output
		cmd.Stderr = output
		var err error
		if request.Guard != nil {
			err = request.Guard.PrepareCommand(cmd)
		}
		if err == nil {
			err = cmd.Run()
		}
		cancel()

		exitCode := 0
		status := CheckPassed
		if err != nil {
			status = CheckFailed
			exitCode = -1
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) {
				exitCode = exitErr.ExitCode()
			}
			if errors.Is(checkCtx.Err(), context.DeadlineExceeded) {
				output.appendMessage(fmt.Sprintf("verification timed out after %s", timeout))
			} else if !errors.As(err, &exitErr) {
				output.appendMessage(err.Error())
			}
			result.Status = StatusFailed
		}
		code := exitCode
		result.Checks = append(result.Checks, CheckResult{
			Name: check.name, Kind: check.kind, Command: check.command, CWD: check.cwd,
			Status: status, ExitCode: &code, Output: output.String(),
			StartedAt: checkStarted, CompletedAt: time.Now().UTC(),
		})
	}
	result.CompletedAt = time.Now().UTC()
	if result.Status == StatusVerified {
		result.Summary = fmt.Sprintf("%d verification check(s) passed", len(result.Checks))
	} else {
		failed := 0
		for _, check := range result.Checks {
			if check.Status == CheckFailed {
				failed++
			}
		}
		result.Summary = fmt.Sprintf("%d of %d verification check(s) failed", failed, len(result.Checks))
	}
	return result
}

func offlineGoEnv() []string {
	base := os.Environ()
	env := make([]string, 0, len(base)+3)
	for _, entry := range base {
		if strings.HasPrefix(entry, "GOPROXY=") || strings.HasPrefix(entry, "GOSUMDB=") ||
			strings.HasPrefix(entry, "GOTOOLCHAIN=") {
			continue
		}
		env = append(env, entry)
	}
	return append(env, "GOPROXY=off", "GOSUMDB=off", "GOTOOLCHAIN=local")
}

func detectGoChecks(workspace string, changedPaths []string) []checkSpec {
	root, err := filepath.Abs(workspace)
	if err != nil {
		return nil
	}
	realRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return nil
	}
	moduleDirs := make(map[string]struct{})
	for _, changed := range changedPaths {
		clean := filepath.Clean(filepath.FromSlash(changed))
		if clean == "." || filepath.IsAbs(clean) || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
			continue
		}
		base := filepath.Base(clean)
		if filepath.Ext(base) != ".go" && base != "go.mod" && base != "go.sum" {
			continue
		}
		start := filepath.Join(root, clean)
		if info, statErr := os.Stat(start); statErr == nil && info.IsDir() {
			// Keep directory paths as-is.
		} else {
			start = filepath.Dir(start)
		}
		if moduleDir := nearestGoModule(root, start); moduleDir != "" {
			realModuleDir, err := filepath.EvalSymlinks(moduleDir)
			if err != nil {
				continue
			}
			rel, err := filepath.Rel(realRoot, realModuleDir)
			if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
				continue
			}
			moduleDirs[realModuleDir] = struct{}{}
		}
	}
	dirs := make([]string, 0, len(moduleDirs))
	for dir := range moduleDirs {
		dirs = append(dirs, dir)
	}
	sort.Strings(dirs)
	checks := make([]checkSpec, 0, len(dirs))
	for _, dir := range dirs {
		cwd, err := filepath.Rel(realRoot, dir)
		if err != nil || strings.HasPrefix(cwd, "..") {
			continue
		}
		if cwd == "." {
			cwd = "."
		}
		name := "Go tests"
		if cwd != "." {
			name += " (" + filepath.ToSlash(cwd) + ")"
		}
		checks = append(checks, checkSpec{
			name: name, kind: "test", command: "go test -mod=readonly ./...",
			args: []string{"go", "test", "-mod=readonly", "./..."}, dir: dir, cwd: filepath.ToSlash(cwd),
		})
	}
	return checks
}

func nearestGoModule(root, start string) string {
	root = filepath.Clean(root)
	current := filepath.Clean(start)
	for {
		rel, err := filepath.Rel(root, current)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return ""
		}
		if info, err := os.Stat(filepath.Join(current, "go.mod")); err == nil && !info.IsDir() {
			return current
		}
		if current == root {
			return ""
		}
		parent := filepath.Dir(current)
		if parent == current {
			return ""
		}
		current = parent
	}
}

type boundedBuffer struct {
	buf       bytes.Buffer
	limit     int
	truncated bool
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	original := len(p)
	if b.buf.Len() < b.limit {
		remaining := b.limit - b.buf.Len()
		if len(p) > remaining {
			p = p[:remaining]
			b.truncated = true
		}
		_, _ = b.buf.Write(p)
	} else if len(p) > 0 {
		b.truncated = true
	}
	return original, nil
}

func (b *boundedBuffer) appendMessage(message string) {
	if strings.TrimSpace(message) == "" {
		return
	}
	if b.buf.Len() > 0 && !strings.HasSuffix(b.buf.String(), "\n") {
		_, _ = b.Write([]byte("\n"))
	}
	_, _ = b.Write([]byte(message))
}

func (b *boundedBuffer) String() string {
	value := b.buf.String()
	if b.truncated {
		value += "\n[indexqube: verification output truncated]"
	}
	return value
}
