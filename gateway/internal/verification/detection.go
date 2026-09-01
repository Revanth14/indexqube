package verification

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const maxDetectionFileBytes = 1 << 20

func planChecks(workspace string, changedPaths []string) ([]checkSpec, string, error) {
	if _, _, err := workspaceRoots(workspace); err != nil {
		return nil, "", &planningError{
			name: "Automatic verification detection", kind: "detection", err: err,
		}
	}
	configured, found, summary, err := configuredChecks(workspace, changedPaths)
	if err != nil {
		return nil, "", &planningError{
			name: "Verification recipe", kind: "configuration", command: RecipePath, err: err,
		}
	}
	if found {
		return configured, summary, err
	}

	checks := make([]checkSpec, 0)
	for _, detector := range []func(string, []string) ([]checkSpec, error){
		detectGoChecksWithError,
		detectNodeChecks,
		detectPythonChecks,
		detectRustChecks,
	} {
		detected, err := detector(workspace, changedPaths)
		if err != nil {
			return nil, "", &planningError{
				name: "Automatic verification detection", kind: "detection", err: err,
			}
		}
		checks = append(checks, detected...)
	}
	sort.Slice(checks, func(i, j int) bool {
		if checks[i].cwd != checks[j].cwd {
			return checks[i].cwd < checks[j].cwd
		}
		if checks[i].kind != checks[j].kind {
			return checks[i].kind < checks[j].kind
		}
		return checks[i].name < checks[j].name
	})
	return checks, "no supported project verification detected for the changed paths", nil
}

type planningError struct {
	name    string
	kind    string
	command string
	err     error
}

func (e *planningError) Error() string { return e.err.Error() }

func (e *planningError) Unwrap() error { return e.err }

func detectGoChecks(workspace string, changedPaths []string) []checkSpec {
	checks, _ := detectGoChecksWithError(workspace, changedPaths)
	return checks
}

func detectGoChecksWithError(workspace string, changedPaths []string) ([]checkSpec, error) {
	root, realRoot, err := workspaceRoots(workspace)
	if err != nil {
		return nil, nil
	}
	moduleDirs := make(map[string]struct{})
	for _, changed := range normalizedChangedPaths(changedPaths) {
		base := filepath.Base(changed)
		if filepath.Ext(base) != ".go" && base != "go.mod" && base != "go.sum" && base != "go.work" && base != "go.work.sum" {
			continue
		}
		start := startDirectory(root, changed)
		moduleDir := nearestMarkerDirectory(root, start, "go.mod")
		if moduleDir == "" {
			continue
		}
		realModuleDir, ok := safeProjectDirectory(realRoot, moduleDir)
		if !ok {
			continue
		}
		moduleDirs[realModuleDir] = struct{}{}
	}
	return checksForDirectories(realRoot, moduleDirs, func(dir, cwd string) checkSpec {
		name := namedCheck("Go tests", cwd)
		args := []string{"go", "test", "-mod=readonly", "./..."}
		return checkSpec{
			name: name, kind: "test", command: displayCommand(args), args: args, dir: dir, cwd: cwd,
			env: mapWith(commonOfflineEnvironment(), map[string]string{
				"GOPROXY": "off", "GOSUMDB": "off", "GOTOOLCHAIN": "local",
			}),
		}
	}), nil
}

func detectNodeChecks(workspace string, changedPaths []string) ([]checkSpec, error) {
	root, realRoot, err := workspaceRoots(workspace)
	if err != nil {
		return nil, nil
	}
	packageDirs := make(map[string]struct{})
	for _, changed := range normalizedChangedPaths(changedPaths) {
		if !isNodeChange(changed) {
			continue
		}
		moduleDir := nearestMarkerDirectory(root, startDirectory(root, changed), "package.json")
		if moduleDir == "" {
			continue
		}
		realModuleDir, ok := safeProjectDirectory(realRoot, moduleDir)
		if ok {
			packageDirs[realModuleDir] = struct{}{}
		}
	}

	return checksForDirectoriesWithError(realRoot, packageDirs, func(dir, cwd string) (checkSpec, bool, error) {
		raw, err := readSmallRegularFile(filepath.Join(dir, "package.json"), maxDetectionFileBytes)
		if err != nil {
			return checkSpec{}, false, fmt.Errorf("inspect Node project at %s: %w", cwd, err)
		}
		var manifest struct {
			Scripts        map[string]string `json:"scripts"`
			PackageManager string            `json:"packageManager"`
		}
		if err := json.Unmarshal(raw, &manifest); err != nil {
			return checkSpec{}, false, fmt.Errorf("parse %s/package.json: %w", cwd, err)
		}
		if strings.TrimSpace(manifest.Scripts["test"]) == "" {
			return checkSpec{}, false, nil
		}
		manager := nodePackageManager(realRoot, dir, manifest.PackageManager)
		args := []string{manager, "test"}
		if manager == "bun" {
			args = []string{"bun", "run", "test"}
		}
		return checkSpec{
			name: namedCheck("Node tests", cwd), kind: "test", command: displayCommand(args),
			args: args, dir: dir, cwd: cwd,
			env: mapWith(commonOfflineEnvironment(), map[string]string{
				"NODE_ENV": "test", "npm_config_audit": "false", "npm_config_fund": "false",
				"npm_config_offline": "true", "npm_config_update_notifier": "false",
				"YARN_ENABLE_NETWORK": "0", "YARN_ENABLE_TELEMETRY": "0",
			}),
		}, true, nil
	})
}

func detectPythonChecks(workspace string, changedPaths []string) ([]checkSpec, error) {
	root, realRoot, err := workspaceRoots(workspace)
	if err != nil {
		return nil, nil
	}
	projectDirs := make(map[string]struct{})
	for _, changed := range normalizedChangedPaths(changedPaths) {
		if !isPythonChange(changed) {
			continue
		}
		projectDir := nearestPythonProject(root, startDirectory(root, changed))
		if projectDir == "" {
			continue
		}
		realProjectDir, ok := safeProjectDirectory(realRoot, projectDir)
		if ok {
			projectDirs[realProjectDir] = struct{}{}
		}
	}

	return checksForDirectoriesWithError(realRoot, projectDirs, func(dir, cwd string) (checkSpec, bool, error) {
		configured, err := pythonUsesPytest(dir)
		if err != nil {
			return checkSpec{}, false, fmt.Errorf("inspect Python project at %s: %w", cwd, err)
		}
		if !configured {
			return checkSpec{}, false, nil
		}
		args := []string{"python3", "-m", "pytest", "-p", "no:cacheprovider"}
		if regularFileExists(filepath.Join(dir, "uv.lock")) {
			args = []string{"uv", "run", "--offline", "python", "-m", "pytest", "-p", "no:cacheprovider"}
		} else if regularFileExists(filepath.Join(dir, "poetry.lock")) {
			args = []string{"poetry", "run", "python", "-m", "pytest", "-p", "no:cacheprovider"}
		} else if regularFileExists(filepath.Join(dir, "pdm.lock")) {
			args = []string{"pdm", "run", "python", "-m", "pytest", "-p", "no:cacheprovider"}
		}
		return checkSpec{
			name: namedCheck("Python tests", cwd), kind: "test", command: displayCommand(args),
			args: args, dir: dir, cwd: cwd,
			env: mapWith(commonOfflineEnvironment(), map[string]string{
				"PIP_DISABLE_PIP_VERSION_CHECK": "1", "PIP_NO_INDEX": "1",
				"PYTHONDONTWRITEBYTECODE": "1", "UV_OFFLINE": "1",
			}),
		}, true, nil
	})
}

func detectRustChecks(workspace string, changedPaths []string) ([]checkSpec, error) {
	root, realRoot, err := workspaceRoots(workspace)
	if err != nil {
		return nil, nil
	}
	crateDirs := make(map[string]struct{})
	for _, changed := range normalizedChangedPaths(changedPaths) {
		if !isRustChange(changed) {
			continue
		}
		crateDir := nearestMarkerDirectory(root, startDirectory(root, changed), "Cargo.toml")
		if crateDir == "" {
			continue
		}
		realCrateDir, ok := safeProjectDirectory(realRoot, crateDir)
		if ok {
			crateDirs[realCrateDir] = struct{}{}
		}
	}

	return checksForDirectories(realRoot, crateDirs, func(dir, cwd string) checkSpec {
		args := []string{"cargo", "test", "--offline"}
		if nearestMarkerDirectory(realRoot, dir, "Cargo.lock") != "" {
			args = []string{"cargo", "test", "--locked", "--offline"}
		}
		return checkSpec{
			name: namedCheck("Rust tests", cwd), kind: "test", command: displayCommand(args),
			args: args, dir: dir, cwd: cwd, temporaryTarget: true,
			env: mapWith(commonOfflineEnvironment(), map[string]string{
				"CARGO_INCREMENTAL": "0", "CARGO_NET_OFFLINE": "true",
			}),
		}
	}), nil
}

func workspaceRoots(workspace string) (string, string, error) {
	root, err := filepath.Abs(workspace)
	if err != nil {
		return "", "", fmt.Errorf("resolve verification workspace: %w", err)
	}
	root = filepath.Clean(root)
	realRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", "", fmt.Errorf("resolve verification workspace: %w", err)
	}
	info, err := os.Stat(realRoot)
	if err != nil || !info.IsDir() {
		return "", "", fmt.Errorf("verification workspace must be an existing directory")
	}
	return root, realRoot, nil
}

func normalizedChangedPaths(paths []string) []string {
	unique := make(map[string]struct{}, len(paths))
	for _, candidate := range paths {
		if normalized, ok := normalizeRelativePath(candidate); ok {
			unique[normalized] = struct{}{}
		}
	}
	result := make([]string, 0, len(unique))
	for path := range unique {
		result = append(result, path)
	}
	sort.Strings(result)
	return result
}

func normalizeRelativePath(candidate string) (string, bool) {
	if strings.TrimSpace(candidate) == "" || filepath.IsAbs(candidate) {
		return "", false
	}
	clean := filepath.Clean(filepath.FromSlash(candidate))
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) || filepath.IsAbs(clean) {
		return "", false
	}
	return filepath.ToSlash(clean), true
}

func startDirectory(root, changed string) string {
	path := filepath.Join(root, filepath.FromSlash(changed))
	if info, err := os.Stat(path); err == nil && info.IsDir() {
		return path
	}
	return filepath.Dir(path)
}

func nearestMarkerDirectory(root, start string, markers ...string) string {
	root = filepath.Clean(root)
	current := filepath.Clean(start)
	for {
		if !pathWithin(root, current) {
			return ""
		}
		for _, marker := range markers {
			if regularFileExists(filepath.Join(current, marker)) {
				return current
			}
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

func safeProjectDirectory(realRoot, candidate string) (string, bool) {
	realCandidate, err := filepath.EvalSymlinks(candidate)
	if err != nil || !pathWithin(realRoot, realCandidate) {
		return "", false
	}
	info, err := os.Stat(realCandidate)
	return realCandidate, err == nil && info.IsDir()
}

func pathWithin(root, candidate string) bool {
	rel, err := filepath.Rel(filepath.Clean(root), filepath.Clean(candidate))
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func regularFileExists(path string) bool {
	info, err := os.Lstat(path)
	return err == nil && info.Mode().IsRegular()
}

func readSmallRegularFile(path string, limit int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("must be a regular file")
	}
	if info.Size() > limit {
		return nil, fmt.Errorf("exceeds the %d-byte inspection limit", limit)
	}
	return os.ReadFile(path)
}

func checksForDirectories(realRoot string, directories map[string]struct{}, build func(string, string) checkSpec) []checkSpec {
	checks, _ := checksForDirectoriesWithError(realRoot, directories, func(dir, cwd string) (checkSpec, bool, error) {
		return build(dir, cwd), true, nil
	})
	return checks
}

func checksForDirectoriesWithError(realRoot string, directories map[string]struct{}, build func(string, string) (checkSpec, bool, error)) ([]checkSpec, error) {
	dirs := make([]string, 0, len(directories))
	for dir := range directories {
		dirs = append(dirs, dir)
	}
	sort.Strings(dirs)
	checks := make([]checkSpec, 0, len(dirs))
	for _, dir := range dirs {
		cwd, err := filepath.Rel(realRoot, dir)
		if err != nil || cwd == ".." || strings.HasPrefix(cwd, ".."+string(filepath.Separator)) {
			continue
		}
		cwd = filepath.ToSlash(cwd)
		check, include, err := build(dir, cwd)
		if err != nil {
			return nil, err
		}
		if include {
			checks = append(checks, check)
		}
	}
	return checks, nil
}

func namedCheck(name, cwd string) string {
	if cwd == "." {
		return name
	}
	return name + " (" + cwd + ")"
}

func commonOfflineEnvironment() map[string]string {
	return map[string]string{
		"CI": "1", "NO_COLOR": "1",
		"CARGO_NET_OFFLINE": "true", "GOPROXY": "off", "GOSUMDB": "off", "GOTOOLCHAIN": "local",
		"PIP_DISABLE_PIP_VERSION_CHECK": "1", "PIP_NO_INDEX": "1", "UV_OFFLINE": "1",
		"npm_config_audit": "false", "npm_config_fund": "false", "npm_config_offline": "true",
		"YARN_ENABLE_NETWORK": "0", "YARN_ENABLE_TELEMETRY": "0",
	}
}

func mapWith(base, extra map[string]string) map[string]string {
	result := make(map[string]string, len(base)+len(extra))
	for key, value := range base {
		result[key] = value
	}
	for key, value := range extra {
		result[key] = value
	}
	return result
}

func isNodeChange(path string) bool {
	base := filepath.Base(path)
	switch strings.ToLower(filepath.Ext(base)) {
	case ".js", ".jsx", ".mjs", ".cjs", ".ts", ".tsx", ".mts", ".cts", ".vue", ".svelte":
		return true
	}
	switch base {
	case "package.json", "package-lock.json", "npm-shrinkwrap.json", "pnpm-lock.yaml", "yarn.lock", "bun.lock", "bun.lockb":
		return true
	}
	return false
}

func nodePackageManager(root, dir, declared string) string {
	if name := strings.ToLower(strings.TrimSpace(strings.SplitN(declared, "@", 2)[0])); name == "npm" || name == "pnpm" || name == "yarn" || name == "bun" {
		return name
	}
	current := filepath.Clean(dir)
	for pathWithin(root, current) {
		for _, candidate := range []struct {
			file    string
			manager string
		}{{"pnpm-lock.yaml", "pnpm"}, {"yarn.lock", "yarn"}, {"bun.lock", "bun"}, {"bun.lockb", "bun"}, {"package-lock.json", "npm"}, {"npm-shrinkwrap.json", "npm"}} {
			if regularFileExists(filepath.Join(current, candidate.file)) {
				return candidate.manager
			}
		}
		if current == root {
			break
		}
		current = filepath.Dir(current)
	}
	return "npm"
}

func isPythonChange(path string) bool {
	base := filepath.Base(path)
	if strings.EqualFold(filepath.Ext(base), ".py") || strings.HasPrefix(base, "requirements") && strings.HasSuffix(base, ".txt") {
		return true
	}
	switch base {
	case "pyproject.toml", "pytest.ini", "setup.cfg", "tox.ini", "uv.lock", "poetry.lock", "pdm.lock":
		return true
	}
	return false
}

func nearestPythonProject(root, start string) string {
	current := filepath.Clean(start)
	for pathWithin(root, current) {
		for _, marker := range []string{"pyproject.toml", "pytest.ini", "setup.cfg", "tox.ini"} {
			if regularFileExists(filepath.Join(current, marker)) {
				return current
			}
		}
		if current == root {
			break
		}
		current = filepath.Dir(current)
	}
	return ""
}

func pythonUsesPytest(dir string) (bool, error) {
	for _, name := range []string{"pytest.ini", "pyproject.toml", "setup.cfg", "tox.ini"} {
		path := filepath.Join(dir, name)
		if !regularFileExists(path) {
			continue
		}
		raw, err := readSmallRegularFile(path, maxDetectionFileBytes)
		if err != nil {
			return false, err
		}
		lower := strings.ToLower(string(raw))
		if name == "pytest.ini" || strings.Contains(lower, "pytest") {
			return true, nil
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		name := entry.Name()
		if regularDirEntry(entry) && strings.HasPrefix(name, "requirements") && strings.HasSuffix(name, ".txt") {
			raw, err := readSmallRegularFile(filepath.Join(dir, name), maxDetectionFileBytes)
			if err != nil {
				return false, err
			}
			if strings.Contains(strings.ToLower(string(raw)), "pytest") {
				return true, nil
			}
		}
	}
	return false, nil
}

func isRustChange(path string) bool {
	base := filepath.Base(path)
	return strings.EqualFold(filepath.Ext(base), ".rs") || base == "Cargo.toml" || base == "Cargo.lock"
}

func regularDirEntry(entry fs.DirEntry) bool {
	if entry.Type().IsRegular() {
		return true
	}
	info, err := entry.Info()
	return err == nil && info.Mode().IsRegular()
}
