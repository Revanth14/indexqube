package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	setupManifestFile = "setup.json"

	claudeSetupStart = "# >>> indexqube claude setup >>>"
	claudeSetupEnd   = "# <<< indexqube claude setup <<<"

	codexTopStart      = "# >>> indexqube codex top >>>"
	codexTopEnd        = "# <<< indexqube codex top <<<"
	codexProviderStart = "# >>> indexqube codex provider >>>"
	codexProviderEnd   = "# <<< indexqube codex provider <<<"
)

type setupManifest struct {
	Version int          `json:"version"`
	Entries []setupEntry `json:"entries"`
}

type setupEntry struct {
	Agent     string    `json:"agent"`
	Path      string    `json:"path"`
	Backup    string    `json:"backup,omitempty"`
	Existed   bool      `json:"existed"`
	CreatedAt time.Time `json:"created_at"`
}

func runSetup(args []string) {
	addr, rest, err := parseSetupArgs(args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "iq: setup failed: %v\n", err)
		os.Exit(2)
	}
	agents := normalizeSetupAgents(rest, true)
	if len(agents) == 0 {
		fmt.Fprintln(os.Stderr, "  [iq] no supported agents detected; try `iq setup claude` or `iq setup codex`")
		return
	}
	if err := setupAgents(agents, normalizeDaemonAddr(addr)); err != nil {
		fmt.Fprintf(os.Stderr, "iq: setup failed: %v\n", err)
		os.Exit(1)
	}
}

func runUnsetup(args []string) {
	fs := flag.NewFlagSet("unsetup", flag.ExitOnError)
	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}
	agents := normalizeSetupAgents(fs.Args(), false)
	if err := unsetupAgents(agents); err != nil {
		fmt.Fprintf(os.Stderr, "iq: unsetup failed: %v\n", err)
		os.Exit(1)
	}
}

func setupAgents(agents []string, addr string) error {
	if addr == "" {
		addr = defaultDaemonAddr
	}
	for _, agent := range agents {
		switch agent {
		case "claude":
			if err := setupClaude(addr); err != nil {
				return err
			}
			fmt.Fprintln(os.Stderr, "  [iq] claude configured")
		case "codex":
			if err := setupCodex(addr); err != nil {
				return err
			}
			fmt.Fprintln(os.Stderr, "  [iq] codex configured")
		default:
			return fmt.Errorf("unsupported agent %q", agent)
		}
	}
	return nil
}

func parseSetupArgs(args []string) (string, []string, error) {
	addr := defaultDaemonAddr
	rest := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--addr" || arg == "-addr":
			if i+1 >= len(args) {
				return "", nil, fmt.Errorf("%s requires a value", arg)
			}
			addr = args[i+1]
			i++
		case strings.HasPrefix(arg, "--addr="):
			addr = strings.TrimPrefix(arg, "--addr=")
		case strings.HasPrefix(arg, "-addr="):
			addr = strings.TrimPrefix(arg, "-addr=")
		default:
			rest = append(rest, arg)
		}
	}
	return addr, rest, nil
}

func unsetupAgents(agents []string) error {
	manifest, err := readSetupManifest()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			fmt.Fprintln(os.Stderr, "  [iq] no setup backups found")
			return nil
		}
		return err
	}
	want := make(map[string]bool)
	for _, agent := range agents {
		want[agent] = true
	}
	remaining := make([]setupEntry, 0, len(manifest.Entries))
	for i := len(manifest.Entries) - 1; i >= 0; i-- {
		entry := manifest.Entries[i]
		if len(want) > 0 && !want[entry.Agent] {
			remaining = append(remaining, entry)
			continue
		}
		if err := restoreSetupEntry(entry); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "  [iq] restored %s config: %s\n", entry.Agent, entry.Path)
	}
	reverseEntries(remaining)
	manifest.Entries = remaining
	return writeSetupManifest(manifest)
}

func normalizeSetupAgents(args []string, detect bool) []string {
	if len(args) > 0 {
		seen := map[string]bool{}
		var agents []string
		for _, raw := range args {
			agent := strings.ToLower(strings.TrimSpace(raw))
			if agent == "all" {
				return []string{"claude", "codex"}
			}
			if agent == "" || seen[agent] {
				continue
			}
			seen[agent] = true
			agents = append(agents, agent)
		}
		return agents
	}
	if !detect {
		return nil
	}
	var agents []string
	if _, err := exec.LookPath("claude"); err == nil {
		agents = append(agents, "claude")
	}
	if _, err := exec.LookPath("codex"); err == nil {
		agents = append(agents, "codex")
	}
	return agents
}

func setupClaude(addr string) error {
	path, err := claudeShellConfigPath()
	if err != nil {
		return err
	}
	content, mode, existed, err := readOptionalFile(path)
	if err != nil {
		return err
	}
	next := patchClaudeShellConfig(string(content), daemonURL(addr))
	return backupAndWrite("claude", path, []byte(next), mode, existed)
}

func setupCodex(addr string) error {
	path, err := codexConfigPath()
	if err != nil {
		return err
	}
	content, mode, existed, err := readOptionalFile(path)
	if err != nil {
		return err
	}
	next := patchCodexConfig(string(content), daemonURL(addr)+"/v1")
	return backupAndWrite("codex", path, []byte(next), mode, existed)
}

func patchClaudeShellConfig(content, baseURL string) string {
	block := strings.Join([]string{
		claudeSetupStart,
		fmt.Sprintf("export ANTHROPIC_BASE_URL=%q", baseURL),
		claudeSetupEnd,
	}, "\n")
	content = removeManagedBlock(content, claudeSetupStart, claudeSetupEnd)
	return appendBlock(content, block)
}

func patchCodexConfig(content, baseURL string) string {
	content = removeManagedBlock(content, codexTopStart, codexTopEnd)
	content = removeManagedBlock(content, codexProviderStart, codexProviderEnd)
	content = removeTableSection(content, "[model_providers.indexqube]")

	topBlock := strings.Join([]string{
		codexTopStart,
		`model_provider = "indexqube"`,
		codexTopEnd,
	}, "\n")
	content = replaceOrInsertTopLevelKey(content, "model_provider", topBlock)

	providerBlock := strings.Join([]string{
		codexProviderStart,
		`[model_providers.indexqube]`,
		`name = "IndexQube local gateway"`,
		fmt.Sprintf("base_url = %q", baseURL),
		`wire_api = "responses"`,
		`requires_openai_auth = true`,
		codexProviderEnd,
	}, "\n")
	return appendBlock(content, providerBlock)
}

func backupAndWrite(agent, path string, next []byte, mode os.FileMode, existed bool) error {
	current, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err == nil && string(current) == string(next) {
		return nil
	}
	entry := setupEntry{
		Agent:     agent,
		Path:      path,
		Existed:   existed,
		CreatedAt: time.Now().UTC(),
	}
	if existed {
		backupPath, err := writeSetupBackup(agent, path, current)
		if err != nil {
			return err
		}
		entry.Backup = backupPath
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	if mode == 0 {
		mode = 0o600
	}
	if err := os.WriteFile(path, next, mode); err != nil {
		return err
	}
	return appendSetupEntry(entry)
}

func restoreSetupEntry(entry setupEntry) error {
	if !entry.Existed {
		return os.Remove(entry.Path)
	}
	raw, err := os.ReadFile(entry.Backup)
	if err != nil {
		return err
	}
	info, err := os.Stat(entry.Backup)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(entry.Path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(entry.Path, raw, info.Mode())
}

func writeSetupBackup(agent, path string, content []byte) (string, error) {
	home, err := indexQubeHome()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(home, "backups", time.Now().Format("20060102-150405"))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	name := sanitizeBackupName(agent + "-" + filepath.Base(path))
	backupPath := filepath.Join(dir, name)
	return backupPath, os.WriteFile(backupPath, content, 0o600)
}

func appendSetupEntry(entry setupEntry) error {
	manifest, err := readSetupManifest()
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if manifest.Version == 0 {
		manifest.Version = 1
	}
	manifest.Entries = append(manifest.Entries, entry)
	return writeSetupManifest(manifest)
}

func setupManifestPath() (string, error) {
	home, err := indexQubeHome()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, setupManifestFile), nil
}

func readSetupManifest() (setupManifest, error) {
	path, err := setupManifestPath()
	if err != nil {
		return setupManifest{}, err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return setupManifest{}, err
	}
	var manifest setupManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return setupManifest{}, err
	}
	return manifest, nil
}

func writeSetupManifest(manifest setupManifest) error {
	path, err := setupManifestPath()
	if err != nil {
		return err
	}
	if manifest.Version == 0 {
		manifest.Version = 1
	}
	sort.SliceStable(manifest.Entries, func(i, j int) bool {
		return manifest.Entries[i].CreatedAt.Before(manifest.Entries[j].CreatedAt)
	})
	raw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return os.WriteFile(path, raw, 0o600)
}

func readOptionalFile(path string) ([]byte, os.FileMode, bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, 0o600, false, nil
		}
		return nil, 0, false, err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, false, err
	}
	return raw, info.Mode(), true, nil
}

func claudeShellConfigPath() (string, error) {
	if path := strings.TrimSpace(os.Getenv("IQ_CLAUDE_SHELL_RC")); path != "" {
		return path, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	shell := filepath.Base(os.Getenv("SHELL"))
	switch shell {
	case "bash":
		return filepath.Join(home, ".bashrc"), nil
	case "zsh", "":
		return filepath.Join(home, ".zshrc"), nil
	default:
		return filepath.Join(home, ".profile"), nil
	}
}

func codexConfigPath() (string, error) {
	if path := strings.TrimSpace(os.Getenv("IQ_CODEX_CONFIG")); path != "" {
		return path, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".codex", "config.toml"), nil
}

func codexConfigHasIndexQube() bool {
	path, err := codexConfigPath()
	if err != nil {
		return false
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	return strings.Contains(string(raw), codexProviderStart)
}

func claudeShellHasIndexQube() bool {
	path, err := claudeShellConfigPath()
	if err != nil {
		return false
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	return strings.Contains(string(raw), claudeSetupStart)
}

func removeManagedBlock(content, start, end string) string {
	for {
		s := strings.Index(content, start)
		if s < 0 {
			return content
		}
		e := strings.Index(content[s:], end)
		if e < 0 {
			return content
		}
		e = s + e + len(end)
		for e < len(content) && (content[e] == '\n' || content[e] == '\r') {
			e++
		}
		content = strings.TrimRight(content[:s], "\r\n") + "\n" + content[e:]
		content = strings.TrimLeft(content, "\r\n")
	}
}

func removeTableSection(content, tableHeader string) string {
	lines := splitLines(content)
	var out []string
	for i := 0; i < len(lines); {
		if strings.TrimSpace(lines[i]) != tableHeader {
			out = append(out, lines[i])
			i++
			continue
		}
		i++
		for i < len(lines) {
			trimmed := strings.TrimSpace(lines[i])
			if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
				break
			}
			i++
		}
	}
	return strings.Join(out, "\n")
}

func replaceOrInsertTopLevelKey(content, key, block string) string {
	lines := splitLines(content)
	firstTable := len(lines)
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
			firstTable = i
			break
		}
	}
	prefix := key + " "
	for i := 0; i < firstTable; i++ {
		trimmed := strings.TrimSpace(lines[i])
		if strings.HasPrefix(trimmed, prefix) || strings.HasPrefix(trimmed, key+"=") {
			next := append([]string{}, lines[:i]...)
			next = append(next, strings.Split(block, "\n")...)
			next = append(next, lines[i+1:]...)
			return strings.Join(next, "\n")
		}
	}
	insert := strings.Split(block, "\n")
	next := append([]string{}, lines[:firstTable]...)
	if len(next) > 0 && strings.TrimSpace(next[len(next)-1]) != "" {
		next = append(next, "")
	}
	next = append(next, insert...)
	if firstTable < len(lines) {
		next = append(next, "")
		next = append(next, lines[firstTable:]...)
	}
	return strings.Join(next, "\n")
}

func appendBlock(content, block string) string {
	content = strings.TrimRight(content, "\r\n")
	if content == "" {
		return block + "\n"
	}
	return content + "\n\n" + block + "\n"
}

func splitLines(s string) []string {
	s = strings.TrimRight(s, "\r\n")
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}

func sanitizeBackupName(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '.', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return "config"
	}
	return b.String()
}

func reverseEntries(entries []setupEntry) {
	for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
		entries[i], entries[j] = entries[j], entries[i]
	}
}
