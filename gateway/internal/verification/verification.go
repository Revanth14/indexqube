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
	"sort"
	"strings"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/securityaudit"
)

type Status string

const (
	StatusVerified Status = "verified"
	StatusWarnings Status = "verified_with_warnings"
	StatusFailed   Status = "verification_failed"
	StatusSkipped  Status = "verification_skipped"
)

type CheckStatus string

const (
	CheckPassed  CheckStatus = "passed"
	CheckWarning CheckStatus = "warning"
	CheckFailed  CheckStatus = "failed"
)

type ChangeEvidence struct {
	BeforeDiff       string
	AfterDiff        string
	CurrentFilePaths []string
	DiffTruncated    bool
}

type Request struct {
	Workspace    string
	ChangedPaths []string
	Change       *ChangeEvidence
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
	Findings    []securityaudit.Finding
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
	name            string
	kind            string
	command         string
	args            []string
	dir             string
	cwd             string
	env             map[string]string
	timeout         time.Duration
	temporaryTarget bool
}

func (v *LocalVerifier) Verify(ctx context.Context, request Request) Result {
	started := time.Now().UTC()
	result := Result{Status: StatusSkipped, StartedAt: started}
	checks, skipSummary, err := planChecks(request.Workspace, request.ChangedPaths)
	if err != nil {
		now := time.Now().UTC()
		name := "Verification planning"
		kind := "detection"
		command := ""
		var planErr *planningError
		if errors.As(err, &planErr) {
			name = planErr.name
			kind = planErr.kind
			command = planErr.command
		}
		result.Status = StatusFailed
		result.Summary = "verification planning failed"
		result.Checks = []CheckResult{{
			Name: name, Kind: kind, Command: command, CWD: ".", Status: CheckFailed, Output: err.Error(),
			StartedAt: now, CompletedAt: now,
		}}
		result.CompletedAt = now
		return result
	}
	if len(checks) == 0 && (request.Change == nil || len(request.ChangedPaths) == 0) {
		result.Summary = skipSummary
		if result.Summary == "" {
			result.Summary = "no supported project verification detected for the changed paths"
		}
		result.CompletedAt = time.Now().UTC()
		return result
	}

	defaultTimeout := v.Timeout
	if defaultTimeout <= 0 {
		defaultTimeout = 2 * time.Minute
	}
	outputLimit := v.OutputLimit
	if outputLimit <= 0 {
		outputLimit = 64 << 10
	}
	result.Status = StatusVerified
	for _, check := range checks {
		checkStarted := time.Now().UTC()
		var runErr error
		timeout := check.timeout
		if timeout <= 0 {
			timeout = defaultTimeout
		}
		checkCtx, cancel := context.WithTimeout(ctx, timeout)
		output := &boundedBuffer{limit: outputLimit}
		exitCode := 0
		status := CheckPassed

		env := mergeEnvironment(os.Environ(), check.env)
		cleanup := func() {}
		if check.temporaryTarget {
			var target string
			target, runErr = os.MkdirTemp("", "indexqube-verification-")
			if runErr == nil {
				env = mergeEnvironment(env, map[string]string{"CARGO_TARGET_DIR": target})
				cleanup = func() { _ = os.RemoveAll(target) }
			}
		}

		if runErr == nil {
			cmd := exec.CommandContext(checkCtx, check.args[0], check.args[1:]...)
			cmd.WaitDelay = 2 * time.Second
			prepareVerificationProcess(cmd)
			cmd.Dir = check.dir
			cmd.Env = env
			cmd.Stdout = output
			cmd.Stderr = output
			if request.Guard != nil {
				runErr = request.Guard.PrepareCommand(cmd)
			}
			if runErr == nil {
				runErr = cmd.Run()
			}
		}
		cancel()
		cleanup()

		if runErr != nil {
			status = CheckFailed
			exitCode = -1
			var exitErr *exec.ExitError
			if errors.As(runErr, &exitErr) {
				exitCode = exitErr.ExitCode()
			}
			if errors.Is(checkCtx.Err(), context.DeadlineExceeded) {
				output.appendMessage(fmt.Sprintf("verification timed out after %s", timeout))
			} else if !errors.As(runErr, &exitErr) {
				output.appendMessage(runErr.Error())
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
	if request.Change != nil && len(request.ChangedPaths) > 0 {
		result.Checks = append(result.Checks, securityCheck(request))
	}
	result.CompletedAt = time.Now().UTC()
	failed := 0
	warnings := 0
	for _, check := range result.Checks {
		switch check.Status {
		case CheckFailed:
			failed++
		case CheckWarning:
			warnings++
		}
	}
	switch {
	case failed > 0:
		result.Status = StatusFailed
		result.Summary = fmt.Sprintf("%d of %d verification check(s) failed", failed, len(result.Checks))
	case warnings > 0:
		result.Status = StatusWarnings
		result.Summary = fmt.Sprintf("%d verification check(s) completed with %d warning(s)", len(result.Checks), warnings)
	default:
		result.Status = StatusVerified
		result.Summary = fmt.Sprintf("%d verification check(s) passed", len(result.Checks))
	}
	return result
}

func mergeEnvironment(base []string, overrides map[string]string) []string {
	if len(overrides) == 0 {
		return append([]string(nil), base...)
	}
	keys := make([]string, 0, len(overrides))
	for key := range overrides {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	blocked := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		blocked[key] = struct{}{}
	}
	env := make([]string, 0, len(base)+len(overrides))
	for _, entry := range base {
		key, _, found := strings.Cut(entry, "=")
		if _, replace := blocked[key]; !found || !replace {
			env = append(env, entry)
		}
	}
	for _, key := range keys {
		env = append(env, key+"="+overrides[key])
	}
	return env
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
