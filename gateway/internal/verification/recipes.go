package verification

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	RecipePath          = ".indexqube/verification.json"
	maxRecipeFileBytes  = 64 << 10
	maxConfiguredChecks = 16
	maxRecipeArgs       = 64
	maxRecipeArgBytes   = 4096
	maxRecipeTotalBytes = 16 << 10
	maxRecipeTimeout    = 10 * time.Minute
)

var recipeKind = map[string]struct{}{
	"build": {}, "custom": {}, "lint": {}, "security": {}, "test": {}, "typecheck": {},
}

var environmentName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type recipeDocument struct {
	Version int      `json:"version"`
	Checks  []recipe `json:"checks"`
}

type recipe struct {
	Name           string            `json:"name"`
	Kind           string            `json:"kind,omitempty"`
	Command        []string          `json:"command"`
	CWD            string            `json:"cwd,omitempty"`
	Paths          []string          `json:"paths,omitempty"`
	TimeoutSeconds int               `json:"timeout_seconds,omitempty"`
	Env            map[string]string `json:"env,omitempty"`
}

func configuredChecks(workspace string, changedPaths []string) ([]checkSpec, bool, string, error) {
	root, realRoot, err := workspaceRoots(workspace)
	if err != nil {
		return nil, false, "", err
	}
	for _, changed := range normalizedChangedPaths(changedPaths) {
		if changed == RecipePath {
			return nil, true, "", fmt.Errorf("%s changed during this turn; review it before allowing automatic execution", RecipePath)
		}
	}

	configPath := filepath.Join(root, filepath.FromSlash(RecipePath))
	info, err := os.Lstat(configPath)
	if os.IsNotExist(err) {
		return nil, false, "", nil
	}
	if err != nil {
		return nil, true, "", fmt.Errorf("read %s: %w", RecipePath, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, true, "", fmt.Errorf("%s must be a regular file, not a symlink or special file", RecipePath)
	}
	if info.Size() > maxRecipeFileBytes {
		return nil, true, "", fmt.Errorf("%s exceeds the %d-byte limit", RecipePath, maxRecipeFileBytes)
	}
	realConfigPath, err := filepath.EvalSymlinks(configPath)
	if err != nil || !pathWithin(realRoot, realConfigPath) {
		return nil, true, "", fmt.Errorf("%s must resolve inside the workspace", RecipePath)
	}
	tracked, err := recipeIsTracked(root)
	if err != nil {
		return nil, true, "", fmt.Errorf("verify %s trust: %w", RecipePath, err)
	}
	if !tracked {
		return nil, true, "", fmt.Errorf("%s must be Git-tracked before IndexQube will execute it", RecipePath)
	}

	file, err := os.Open(configPath)
	if err != nil {
		return nil, true, "", fmt.Errorf("open %s: %w", RecipePath, err)
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, maxRecipeFileBytes+1))
	if err != nil {
		return nil, true, "", fmt.Errorf("read %s: %w", RecipePath, err)
	}
	if len(raw) > maxRecipeFileBytes {
		return nil, true, "", fmt.Errorf("%s exceeds the %d-byte limit", RecipePath, maxRecipeFileBytes)
	}
	var document recipeDocument
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		return nil, true, "", fmt.Errorf("parse %s: %w", RecipePath, err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, true, "", fmt.Errorf("parse %s: %w", RecipePath, err)
	}
	if document.Version != 1 {
		return nil, true, "", fmt.Errorf("%s version must be 1", RecipePath)
	}
	if len(document.Checks) == 0 || len(document.Checks) > maxConfiguredChecks {
		return nil, true, "", fmt.Errorf("%s must define between 1 and %d checks", RecipePath, maxConfiguredChecks)
	}

	changed := normalizedChangedPaths(changedPaths)
	checks := make([]checkSpec, 0, len(document.Checks))
	for index, configured := range document.Checks {
		check, matches, err := validateRecipe(realRoot, index, configured, changed)
		if err != nil {
			return nil, true, "", err
		}
		if matches {
			checks = append(checks, check)
		}
	}
	if len(checks) == 0 {
		return nil, true, "no configured verification recipe matched the changed paths", nil
	}
	return checks, true, "", nil
}

func validateRecipe(realRoot string, index int, configured recipe, changedPaths []string) (checkSpec, bool, error) {
	location := fmt.Sprintf("%s checks[%d]", RecipePath, index)
	name := strings.TrimSpace(configured.Name)
	if name == "" || len(name) > 120 {
		return checkSpec{}, false, fmt.Errorf("%s name must be between 1 and 120 characters", location)
	}
	kind := strings.ToLower(strings.TrimSpace(configured.Kind))
	if kind == "" {
		kind = "custom"
	}
	if _, ok := recipeKind[kind]; !ok {
		return checkSpec{}, false, fmt.Errorf("%s kind %q is not supported", location, configured.Kind)
	}
	if len(configured.Command) == 0 || len(configured.Command) > maxRecipeArgs {
		return checkSpec{}, false, fmt.Errorf("%s command must contain between 1 and %d arguments", location, maxRecipeArgs)
	}
	total := 0
	args := make([]string, len(configured.Command))
	for argIndex, arg := range configured.Command {
		if arg == "" || strings.ContainsRune(arg, '\x00') || len(arg) > maxRecipeArgBytes {
			return checkSpec{}, false, fmt.Errorf("%s command[%d] is empty, contains NUL, or exceeds %d bytes", location, argIndex, maxRecipeArgBytes)
		}
		total += len(arg)
		args[argIndex] = arg
	}
	if total > maxRecipeTotalBytes {
		return checkSpec{}, false, fmt.Errorf("%s command exceeds the %d-byte total limit", location, maxRecipeTotalBytes)
	}
	if filepath.IsAbs(args[0]) {
		return checkSpec{}, false, fmt.Errorf("%s executable must be a PATH name or workspace-relative path", location)
	}
	switch strings.ToLower(filepath.Base(args[0])) {
	case "bash", "cmd", "cmd.exe", "dash", "fish", "ksh", "powershell", "powershell.exe", "pwsh", "sh", "zsh":
		return checkSpec{}, false, fmt.Errorf("%s cannot invoke a shell; use an argv command or a reviewed workspace script", location)
	}

	cwd, dir, err := resolveRecipeCWD(realRoot, configured.CWD)
	if err != nil {
		return checkSpec{}, false, fmt.Errorf("%s cwd: %w", location, err)
	}
	if strings.ContainsAny(args[0], `/\`) {
		executable := filepath.Join(dir, filepath.FromSlash(args[0]))
		realExecutable, err := filepath.EvalSymlinks(executable)
		if err != nil {
			return checkSpec{}, false, fmt.Errorf("%s executable: %w", location, err)
		}
		if !pathWithin(realRoot, realExecutable) {
			return checkSpec{}, false, fmt.Errorf("%s executable resolves outside the workspace", location)
		}
		info, err := os.Stat(realExecutable)
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
			return checkSpec{}, false, fmt.Errorf("%s executable must be an executable regular file", location)
		}
	}

	paths := make([]string, 0, len(configured.Paths))
	for pathIndex, prefix := range configured.Paths {
		normalized, ok := normalizeRelativePath(prefix)
		if !ok {
			return checkSpec{}, false, fmt.Errorf("%s paths[%d] must be workspace-relative and cannot escape", location, pathIndex)
		}
		paths = append(paths, normalized)
	}
	if len(paths) > 0 && !matchesPathPrefix(changedPaths, paths) {
		return checkSpec{}, false, nil
	}

	if configured.TimeoutSeconds < 0 || configured.TimeoutSeconds > int(maxRecipeTimeout/time.Second) {
		return checkSpec{}, false, fmt.Errorf("%s timeout_seconds must be between 1 and %.0f when set", location, maxRecipeTimeout.Seconds())
	}
	timeout := time.Duration(configured.TimeoutSeconds) * time.Second
	env := commonOfflineEnvironment()
	if len(configured.Env) > 32 {
		return checkSpec{}, false, fmt.Errorf("%s env cannot contain more than 32 entries", location)
	}
	for key, value := range configured.Env {
		if !environmentName.MatchString(key) || strings.ContainsRune(value, '\x00') || len(value) > maxRecipeArgBytes {
			return checkSpec{}, false, fmt.Errorf("%s env contains an invalid name or value", location)
		}
		if protectedRecipeEnvironment(key) {
			return checkSpec{}, false, fmt.Errorf("%s env cannot override protected variable %q", location, key)
		}
		env[key] = value
	}

	return checkSpec{
		name: name, kind: kind, command: displayCommand(args), args: args,
		dir: dir, cwd: cwd, env: env, timeout: timeout,
	}, true, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err == io.EOF {
		return nil
	} else if err != nil {
		return err
	}
	return fmt.Errorf("contains more than one JSON value")
}

func recipeIsTracked(root string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", "-C", root, "ls-files", "--error-unmatch", "--", RecipePath)
	cmd.Env = withoutGitEnvironmentOverrides(os.Environ())
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	err := cmd.Run()
	if err == nil {
		return true, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return false, nil
	}
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	return false, err
}

func withoutGitEnvironmentOverrides(base []string) []string {
	env := make([]string, 0, len(base))
	for _, entry := range base {
		key, _, found := strings.Cut(entry, "=")
		if found && strings.HasPrefix(strings.ToUpper(key), "GIT_") {
			continue
		}
		env = append(env, entry)
	}
	return env
}

func resolveRecipeCWD(realRoot, configured string) (string, string, error) {
	if strings.TrimSpace(configured) == "" {
		configured = "."
	}
	cwd, ok := normalizeRelativePath(configured)
	if !ok {
		return "", "", fmt.Errorf("must be workspace-relative and cannot escape")
	}
	dir := filepath.Join(realRoot, filepath.FromSlash(cwd))
	realDir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return "", "", err
	}
	if !pathWithin(realRoot, realDir) {
		return "", "", fmt.Errorf("resolves outside the workspace")
	}
	info, err := os.Stat(realDir)
	if err != nil || !info.IsDir() {
		return "", "", fmt.Errorf("must resolve to an existing directory")
	}
	return cwd, realDir, nil
}

func matchesPathPrefix(changedPaths, prefixes []string) bool {
	for _, changed := range changedPaths {
		for _, prefix := range prefixes {
			if prefix == "." || changed == prefix || strings.HasPrefix(changed, strings.TrimSuffix(prefix, "/")+"/") {
				return true
			}
		}
	}
	return false
}

func protectedRecipeEnvironment(key string) bool {
	upper := strings.ToUpper(key)
	if upper == "PATH" || upper == "HOME" || upper == "TMPDIR" || strings.HasPrefix(upper, "INDEXQUBE_") ||
		strings.HasPrefix(upper, "DYLD_") || strings.HasPrefix(upper, "LD_") {
		return true
	}
	switch upper {
	case "BASH_ENV", "CARGO_NET_OFFLINE", "CI", "ENV", "GIT_CONFIG", "GIT_CONFIG_COUNT", "GOPROXY", "GOSUMDB", "GOTOOLCHAIN",
		"NODE_OPTIONS", "PIP_DISABLE_PIP_VERSION_CHECK", "PIP_NO_INDEX", "PYTHONHOME", "PYTHONPATH", "RUSTC_WRAPPER", "UV_OFFLINE",
		"YARN_ENABLE_NETWORK", "YARN_ENABLE_TELEMETRY":
		return true
	}
	if strings.HasPrefix(upper, "NPM_CONFIG_") {
		return true
	}
	return false
}

func displayCommand(args []string) string {
	displayed := make([]string, len(args))
	for index, arg := range args {
		if strings.IndexFunc(arg, func(r rune) bool {
			return r <= ' ' || strings.ContainsRune(`"'\\`, r)
		}) >= 0 {
			displayed[index] = strconv.Quote(arg)
		} else {
			displayed[index] = arg
		}
	}
	return strings.Join(displayed, " ")
}
