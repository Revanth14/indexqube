// Command iq wraps the Claude Code CLI with an in-process IndexQube optimizer.
// It boots the gateway on a random local port, injects ANTHROPIC_BASE_URL into
// Claude Code's environment, and forwards all traffic through the optimizer.
// The user's real OAuth / API key flows through unchanged — iq never sees it.
package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	_ "modernc.org/sqlite" // pure-Go SQLite driver

	"github.com/Revanth14/indexqube/gateway/internal/server"
)

var version = "dev"

func generateToken() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("iq-fallback-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

func main() {
	if len(os.Args) < 2 {
		// iq alone → run claude (backward compat, no subcommand)
		runClaude([]string{}, false, false)
		return
	}

	switch os.Args[1] {
	case "claude":
		devMode, dumpPayloads, claudeArgs := parseClaudeFlags(os.Args[2:])
		runClaude(claudeArgs, devMode, dumpPayloads)
	case "gemini":
		fmt.Println("  iq gemini — coming soon")
	case "codex":
		fmt.Println("  iq codex  — coming soon")
	case "help", "--help", "-h":
		printHelp()
	default:
		// Unknown subcommand → pass everything to claude (backward compat).
		// Ensures `iq somefile.go` and `iq --resume` still work as before.
		runClaude(os.Args[1:], false, false)
	}
}

func parseClaudeFlags(args []string) (bool, bool, []string) {
	dev := false
	dumpPayloads := false
	filtered := make([]string, 0, len(args))
	for _, a := range args {
		switch a {
		case "--dev":
			dev = true
		case "--dump-payloads":
			dumpPayloads = true
		default:
			filtered = append(filtered, a)
		}
	}
	return dev, dumpPayloads, filtered
}

func runClaude(args []string, devMode, dumpPayloads bool) {
	if devMode {
		os.Setenv("IQ_DEV_MODE", "1")
		fmt.Fprintln(os.Stderr, "  [iq] dev mode — relaxed guards, full telemetry")
	}
	sessionID := generateToken()
	if dumpPayloads {
		os.Setenv("IQ_DUMP_PAYLOADS", "1")
		repoRoot := findRepoRoot()
		if repoRoot == "" {
			fmt.Fprintf(os.Stderr, "iq: failed to resolve repository root directory\n")
			os.Exit(1)
		}
		dumpDir := filepath.Join(repoRoot, ".indexqube", "dumps")
		shortSessionID := sessionID
		if len(shortSessionID) > 8 {
			shortSessionID = shortSessionID[:8]
		}
		sessionFile := filepath.Join(dumpDir, "iq-session-"+time.Now().Format("20060102-150405")+"-"+shortSessionID+".jsonl")
		os.Setenv("IQ_DUMP_DIR", dumpDir)
		os.Setenv("IQ_DUMP_SESSION_FILE", sessionFile)
		fmt.Fprintf(os.Stderr, "  [iq] dumping payloads to %s\n", sessionFile)
	}

	// Generate a per-invocation session ID so the circuit breaker scopes
	// similarity checks within this iq session, not across sessions.
	os.Setenv("IQ_SESSION_ID", sessionID)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintf(os.Stderr, "iq: failed to reserve local port: %v\n", err)
		os.Exit(1)
	}
	port := ln.Addr().(*net.TCPAddr).Port

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxyDone := make(chan struct{})
	os.Setenv("INDEXQUBE_LOG_LEVEL", "off")
	go func() {
		startProxy(ctx, ln)
		close(proxyDone)
	}()

	if !waitForProxy(port) {
		fmt.Fprintf(os.Stderr, "iq: proxy failed to start within 2s\n")
		os.Exit(1)
	}

	claudePath, err := exec.LookPath("claude")
	if err != nil {
		fmt.Fprintf(os.Stderr, "iq: could not find 'claude' in PATH: %v\n", err)
		os.Exit(1)
	}

	if !filepath.IsAbs(claudePath) {
		claudePath, err = filepath.Abs(claudePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "iq: failed to resolve absolute path for 'claude': %v\n", err)
			os.Exit(1)
		}
	}

	cmdArgs := append([]string{claudePath}, args...)
	cmd := exec.Command(cmdArgs[0], cmdArgs[1:]...)
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("ANTHROPIC_BASE_URL=http://127.0.0.1:%d", port),
	)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigs)
	go func() {
		for sig := range sigs {
			if cmd.Process != nil {
				cmd.Process.Signal(sig) //nolint:errcheck
			}
		}
	}()

	runErr := cmd.Run()

	// Trigger graceful shutdown
	cancel()
	<-proxyDone

	// Print beautiful colored session summary of token savings
	printSessionSummary(sessionID)

	if runErr != nil {
		var exitErr *exec.ExitError
		if errors.As(runErr, &exitErr) {
			os.Exit(exitErr.ExitCode())
		}
		fmt.Fprintf(os.Stderr, "iq: failed to run claude: %v\n", runErr)
		os.Exit(1)
	}
}

func printSessionSummary(sessionID string) {
	home, err := os.UserHomeDir()
	if err != nil {
		return
	}
	dbPath := filepath.Join(home, ".indexqube", "sessions.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return
	}
	defer db.Close()

	var requestsTotal, tokensAttempted, tokensSent, tokensDeduplicated int64
	var inputTokensReal, cacheReadTokens, cacheCreationTokens int64
	var status string
	queryID := "%"
	if len(sessionID) >= 8 {
		queryID = "%-" + sessionID[:8]
	}
	err = db.QueryRow(`
		SELECT requests_total, tokens_attempted, tokens_sent, tokens_deduplicated,
		       input_tokens_real, cache_read_tokens, cache_creation_tokens, status
		FROM   agent_sessions
		WHERE  session_id LIKE ?
		ORDER BY last_seen_at DESC
		LIMIT 1
	`, queryID).Scan(&requestsTotal, &tokensAttempted, &tokensSent, &tokensDeduplicated,
		&inputTokensReal, &cacheReadTokens, &cacheCreationTokens, &status)
	if err != nil {
		return
	}

	if requestsTotal == 0 {
		return
	}

	// Prompt-cache hit ratio: of the input the model actually billed this session,
	// the fraction Anthropic served from its prompt cache (≈10% cost) instead of
	// re-reading at full price. The headline signal for a subscription user — real,
	// measured from upstream usage, not a byte estimate or a fabricated dollar figure.
	cacheHitRatio := 0.0
	if inputTokensReal > 0 {
		cacheHitRatio = float64(cacheReadTokens) / float64(inputTokensReal) * 100
	}
	// Fresh = the only portion billed at full input price this session.
	freshInput := inputTokensReal - cacheReadTokens - cacheCreationTokens
	if freshInput < 0 {
		freshInput = 0
	}
	// Estimated optimizer pruning (byte-based, pre-cache). A diagnostic only — it
	// does NOT correspond to tokens Anthropic actually billed, so it is dimmed and
	// kept below the real metrics rather than shown as "saved".
	estPercent := 0.0
	if tokensAttempted > 0 {
		estPercent = float64(tokensDeduplicated) / float64(tokensAttempted) * 100
	}

	const (
		cyan   = "\033[1;36m"
		white  = "\033[1;37m"
		green  = "\033[1;32m"
		purple = "\033[1;35m"
		grey   = "\033[90m"
	)
	row := func(label, color, value string) {
		// Inner width 56 (2 + 20 label + 3 + 31 value) to match the box borders.
		fmt.Fprintf(os.Stderr, "  \033[1;36m│\033[0m  %-20s : %s%-31s\033[0m\033[1;36m│\033[0m\n", label, color, value)
	}
	divider := func() {
		fmt.Fprintln(os.Stderr, "  \033[1;36m├────────────────────────────────────────────────────────┤\033[0m")
	}

	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "  \033[1;36m┌────────────────────────────────────────────────────────┐\033[0m")
	fmt.Fprintln(os.Stderr, "  \033[1;36m│\033[0m                  \033[1;37mIndexQube Session Summary\033[0m             \033[1;36m│\033[0m")
	divider()
	row("Requests", green, formatNumber(requestsTotal))
	if inputTokensReal > 0 {
		// Real, Anthropic-reported numbers lead.
		row("Prompt-Cache Hit", purple, fmt.Sprintf("%.1f%%", cacheHitRatio))
		row("Input Billed (real)", white, formatNumber(inputTokensReal)+" tok")
		row("  from cache", cyan, formatNumber(cacheReadTokens)+" tok")
		row("  cache write", white, formatNumber(cacheCreationTokens)+" tok")
		row("  fresh (full price)", white, formatNumber(freshInput)+" tok")
	} else {
		row("Prompt-Cache Hit", grey, "— (no upstream usage)")
	}
	divider()
	// Dimmed diagnostic, explicitly not a billing figure.
	row("Optimizer (est.)", grey, fmt.Sprintf("%s tok  (%.1f%%)", formatNumber(tokensDeduplicated), estPercent))
	statusColor := green
	if status == "killed" {
		statusColor = "\033[1;31m" // red
	}
	row("Session Status", statusColor, strings.ToUpper(status))
	fmt.Fprintln(os.Stderr, "  \033[1;36m└────────────────────────────────────────────────────────┘\033[0m")
	fmt.Fprintln(os.Stderr)
}

func formatNumber(n int64) string {
	in := fmt.Sprintf("%d", n)
	out := make([]byte, len(in)+(len(in)-1)/3)
	for i, j, k := len(in)-1, len(out)-1, 0; i >= 0; i-- {
		out[j] = in[i]
		j--
		k++
		if k == 3 && i > 0 {
			out[j] = ','
			j--
			k = 0
		}
	}
	return string(out)
}

func printHelp() {
	fmt.Print(`
  iq — IndexQube CLI

  USAGE
    iq                   Start Claude Code (default)
    iq claude            Start Claude Code via IndexQube
    iq claude --dev      Start Claude Code, dev mode (relaxed guards)
    iq claude --dump-payloads
                         Dump Anthropic payloads to .indexqube/dumps/iq-session-*.jsonl
    iq gemini            Gemini via IndexQube (coming soon)
    iq codex             Codex via IndexQube  (coming soon)
    iq help              Show this help

  DEV MODE
    --dev disables velocity guard, enables verbose logging.
    Use when building IndexQube with IndexQube.

`)
}

const telemetryEndpoint = "https://dev-api.indexqube.com"

func startProxy(ctx context.Context, ln net.Listener) {
	os.Setenv("INDEXQUBE_BIND_ADDR", ln.Addr().String())
	if os.Getenv("INDEXQUBE_DEV_TOKEN") == "" {
		os.Setenv("INDEXQUBE_DEV_TOKEN", generateToken())
	}
	os.Setenv("INDEXQUBE_MODE", "optimize")
	os.Setenv("INDEXQUBE_ENABLE_BLOCK_OPTIMIZER", "true")
	// Bind admin server to an ephemeral port so multiple iq instances
	// don't collide on the default 9100.
	os.Setenv("ADMIN_PORT", "0")
	// Route telemetry through the deployed gateway so Supabase credentials
	// never need to be baked into this distributed binary.
	if os.Getenv("IQ_TELEMETRY_ENDPOINT") == "" {
		os.Setenv("IQ_TELEMETRY_ENDPOINT", telemetryEndpoint)
	}
	// Suppress noisy info logs when embedded — only surface warnings and errors.
	if os.Getenv("INDEXQUBE_LOG_LEVEL") == "" {
		os.Setenv("INDEXQUBE_LOG_LEVEL", "warn")
	}
	server.RunWithPublicListener(ctx, ln) //nolint:errcheck
}

func waitForProxy(port int) bool {
	url := fmt.Sprintf("http://127.0.0.1:%d/healthz", port)
	for i := 0; i < 20; i++ {
		resp, err := http.Get(url) //nolint:noctx,gosec
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return true
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

func findRepoRoot() string {
	cwd, err := os.Getwd()
	if err != nil {
		return ""
	}
	dir := cwd
	for {
		if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
			return dir
		}
		if _, err := os.Stat(filepath.Join(dir, "CLAUDE.md")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return cwd
}
