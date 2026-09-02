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
	"flag"
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

	claudebackend "github.com/Revanth14/indexqube/gateway/internal/agent/claude"
	"github.com/Revanth14/indexqube/gateway/internal/agent/fake"
	"github.com/Revanth14/indexqube/gateway/internal/localstate"
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
	case "__fake-agent":
		os.Exit(fake.RunHelper(os.Stdin, os.Stdout, os.Stderr))
	case "__claude-permission-mcp":
		os.Exit(claudebackend.RunPermissionMCP(os.Args[2:], os.Stdin, os.Stdout, os.Stderr))
	case "task":
		runTask(os.Args[2:])
	case "tasks":
		runTasks(os.Args[2:])
	case "approvals":
		runApprovals(os.Args[2:])
	case "approve":
		runApprove(os.Args[2:], "approve")
	case "deny":
		runApprove(os.Args[2:], "deny")
	case "cancel":
		runCancel(os.Args[2:])
	case "continue":
		runContinue(os.Args[2:])
	case "claude":
		devMode, dumpPayloads, claudeArgs := parseClaudeFlags(os.Args[2:])
		runClaude(claudeArgs, devMode, dumpPayloads)
	case "start":
		runStart(os.Args[2:])
	case "daemon":
		runDaemon(os.Args[2:])
	case "stop":
		runStop(os.Args[2:])
	case "status":
		runStatus(os.Args[2:])
	case "logs":
		runLogs(os.Args[2:])
	case "doctor":
		runDoctor(os.Args[2:])
	case "setup":
		runSetup(os.Args[2:])
	case "unsetup":
		runUnsetup(os.Args[2:])
	case "audit":
		runAudit(os.Args[2:])
	case "bench":
		runBench(os.Args[2:])
	case "gemini":
		fmt.Println("  iq gemini — coming soon")
	case "codex":
		fmt.Println("  iq codex  — use `iq start` and `iq setup codex`")
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
	// Honor a caller-provided session ID (used by `iq bench` to locate each
	// arm's recorded metrics afterward); otherwise generate one.
	sessionID := os.Getenv("IQ_SESSION_ID")
	if sessionID == "" {
		sessionID = generateToken()
	}
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

	// Print the colored session summary unless suppressed (e.g. by `iq bench`,
	// which prints its own side-by-side comparison instead).
	if os.Getenv("IQ_NO_SUMMARY") == "" {
		printSessionSummary(sessionID)
	}

	if runErr != nil {
		var exitErr *exec.ExitError
		if errors.As(runErr, &exitErr) {
			os.Exit(exitErr.ExitCode())
		}
		fmt.Fprintf(os.Stderr, "iq: failed to run claude: %v\n", runErr)
		os.Exit(1)
	}
}

// sessionMetrics holds the real, Anthropic-reported usage recorded for one
// session, plus the byte-estimate diagnostic, read from sessions.db.
type sessionMetrics struct {
	requests           int64
	tokensAttempted    int64
	tokensDeduplicated int64
	inputReal          int64
	cacheRead          int64
	cacheCreation      int64
	status             string
}

func sessionsDBPath() string {
	home, err := localstate.Dir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, "sessions.db")
}

// readSessionMetrics loads the most recent agent_sessions row whose ID matches
// the given session (the recorded ID is suffixed with sessionID[:8]).
func readSessionMetrics(dbPath, sessionID string) (sessionMetrics, bool) {
	var m sessionMetrics
	if dbPath == "" {
		return m, false
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return m, false
	}
	defer db.Close()
	queryID := "%"
	if len(sessionID) >= 8 {
		queryID = "%-" + sessionID[:8]
	}
	err = db.QueryRow(`
		SELECT requests_total, tokens_attempted, tokens_deduplicated,
		       input_tokens_real, cache_read_tokens, cache_creation_tokens, status
		FROM   agent_sessions
		WHERE  session_id LIKE ?
		ORDER BY last_seen_at DESC
		LIMIT 1
	`, queryID).Scan(&m.requests, &m.tokensAttempted, &m.tokensDeduplicated,
		&m.inputReal, &m.cacheRead, &m.cacheCreation, &m.status)
	if err != nil {
		return m, false
	}
	return m, true
}

func printSessionSummary(sessionID string) {
	m, ok := readSessionMetrics(sessionsDBPath(), sessionID)
	if !ok || m.requests == 0 {
		return
	}
	requestsTotal := m.requests
	tokensAttempted := m.tokensAttempted
	tokensDeduplicated := m.tokensDeduplicated
	inputTokensReal := m.inputReal
	cacheReadTokens := m.cacheRead
	cacheCreationTokens := m.cacheCreation
	status := m.status

	// Prompt-cache reuse ratio: of the input Anthropic accounted for this session,
	// the fraction served from its prompt cache instead of freshly processed. This
	// is the primary subscription-session signal: real upstream usage, not a byte
	// estimate or a fabricated dollar figure.
	cacheHitRatio := 0.0
	if inputTokensReal > 0 {
		cacheHitRatio = float64(cacheReadTokens) / float64(inputTokensReal) * 100
	}
	// Fresh input is the portion that was neither served from cache nor written
	// into the cache this session.
	freshInput := inputTokensReal - cacheReadTokens - cacheCreationTokens
	if freshInput < 0 {
		freshInput = 0
	}
	// Estimated optimizer pruning (byte-based, pre-cache). A diagnostic only — it
	// does NOT correspond to Anthropic's upstream usage accounting, so it is dimmed and
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
	fmt.Fprintln(os.Stderr, "  \033[1;36m│\033[0m                 \033[1;37mIndexQube · Claude Session\033[0m             \033[1;36m│\033[0m")
	divider()
	row("Requests", green, formatNumber(requestsTotal))
	if inputTokensReal > 0 {
		// Real, Anthropic-reported numbers lead.
		row("Prompt-cache reuse", purple, fmt.Sprintf("%.1f%%", cacheHitRatio))
		row("Fresh input pressure", white, fmt.Sprintf("%.1f%%", float64(freshInput)/float64(inputTokensReal)*100))
		row("Upstream input", white, formatNumber(inputTokensReal)+" tok")
		row("  cache reuse", cyan, formatNumber(cacheReadTokens)+" tok")
		row("  cache write", white, formatNumber(cacheCreationTokens)+" tok")
		row("  fresh input", white, formatNumber(freshInput)+" tok")
	} else {
		row("Prompt-cache reuse", grey, "— (no upstream usage)")
		row("Fresh input pressure", grey, "—")
	}
	divider()
	// Dimmed diagnostic, explicitly not an upstream usage figure.
	row("Optimizer estimate", grey, fmt.Sprintf("%s tok  (%.1f%%)", formatNumber(tokensDeduplicated), estPercent))
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

// runBench drives the same prompt through two arms of the local proxy — an
// `optimize` arm (proxy) and an `observe` arm (direct passthrough) — both using
// the user's real subscription/OAuth auth (no API key), then prints the real
// Anthropic-reported usage side by side. This is the proxy-vs-direct A/B.
func runBench(args []string) {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	prompt := fs.String("prompt", "List the files in the current directory, then stop.", "prompt used to drive both arms")
	cooldown := fs.Duration("cooldown", 6*time.Minute, "wait between arms so Anthropic's prompt cache (5m TTL) expires for an uncontaminated read; below 5m the comparison is contaminated")
	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}

	self, err := os.Executable()
	if err != nil || self == "" {
		self = os.Args[0]
	}

	// Optimize first (cold cache), direct second. With no cooldown the 2nd arm
	// may read the 1st arm's cache, which inflates the DIRECT column — a
	// conservative bias for any proxy-savings claim.
	type arm struct {
		label     string
		mode      string
		sessionID string
		m         sessionMetrics
	}
	arms := []*arm{
		{label: "proxy (optimize)", mode: "optimize"},
		{label: "direct (observe)", mode: "observe"},
	}

	for i, a := range arms {
		a.sessionID = generateToken()
		fmt.Fprintf(os.Stderr, "\n  [iq bench] arm %d/%d — %s: claude -p %q\n", i+1, len(arms), a.label, *prompt)
		cmd := exec.Command(self, "claude", "-p", *prompt) //nolint:gosec
		cmd.Env = append(os.Environ(),
			"INDEXQUBE_MODE="+a.mode,
			"IQ_SESSION_ID="+a.sessionID,
			"IQ_NO_SUMMARY=1",
		)
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "  [iq bench] arm %q exited with error: %v\n", a.label, err)
		}
		if i < len(arms)-1 && *cooldown > 0 {
			fmt.Fprintf(os.Stderr, "  [iq bench] cooldown %s (waiting for Anthropic cache TTL)…\n", *cooldown)
			time.Sleep(*cooldown)
		}
	}

	dbPath := sessionsDBPath()
	for _, a := range arms {
		a.m, _ = readSessionMetrics(dbPath, a.sessionID)
	}
	// arms[0]=proxy, arms[1]=direct.
	printBenchComparison(arms[1].label, arms[1].m, arms[0].label, arms[0].m, *cooldown)
}

// anthropicCacheTTL is Anthropic's prompt-cache lifetime. A bench cooldown below
// this lets the second arm read the first arm's still-warm cache, contaminating
// the comparison (it freeloads, looking artificially cheap).
const anthropicCacheTTL = 5 * time.Minute

func benchCacheRatio(m sessionMetrics) float64 {
	if m.inputReal <= 0 {
		return 0
	}
	return float64(m.cacheRead) / float64(m.inputReal) * 100
}

func benchFreshInput(m sessionMetrics) int64 {
	f := m.inputReal - m.cacheRead - m.cacheCreation
	if f < 0 {
		f = 0
	}
	return f
}

// benchEffectiveInput weights input by Anthropic's standard prompt-cache
// multipliers (cache read ≈0.1×, cache write ≈1.25×, fresh =1.0×) so the
// comparison reflects subscription usage pressure. Raw token sums hide that a
// cache write has much more impact than a cache read.
func benchEffectiveInput(m sessionMetrics) int64 {
	return int64(float64(m.cacheRead)*0.1 + float64(m.cacheCreation)*1.25 + float64(benchFreshInput(m)))
}

func signedNumber(n int64) string {
	switch {
	case n > 0:
		return "+" + formatNumber(n)
	case n < 0:
		return "-" + formatNumber(-n)
	default:
		return "0"
	}
}

func printBenchComparison(directLabel string, d sessionMetrics, proxyLabel string, p sessionMetrics, cooldown time.Duration) {
	const (
		green = "\033[1;32m"
		red   = "\033[1;31m"
		grey  = "\033[90m"
		bold  = "\033[1;37m"
		reset = "\033[0m"
	)
	w := os.Stderr
	fmt.Fprintln(w)
	fmt.Fprintf(w, "  %sIndexQube A/B — %s vs %s%s\n", bold, proxyLabel, directLabel, reset)
	fmt.Fprintln(w, "  ──────────────────────────────────────────────────────────────")

	if d.requests == 0 || p.requests == 0 {
		fmt.Fprintf(w, "  %s⚠ one arm recorded no requests (direct=%d, proxy=%d) — cannot compare.%s\n", red, d.requests, p.requests, reset)
		fmt.Fprintln(w, "  Check that `claude -p` ran successfully for both arms.")
		fmt.Fprintln(w)
		return
	}

	fmt.Fprintf(w, "  %-22s %14s %14s %14s\n", "Metric", "Direct", "Proxy", "Δ proxy−direct")

	// row prints one metric. lowerBetter colors a negative delta green.
	row := func(label string, dv, pv int64, lowerBetter bool) {
		delta := pv - dv
		col := grey
		if delta != 0 && lowerBetter {
			if delta < 0 {
				col = green
			} else {
				col = red
			}
		}
		fmt.Fprintf(w, "  %-22s %14s %14s %s%14s%s\n",
			label, formatNumber(dv), formatNumber(pv), col, signedNumber(delta), reset)
	}

	row("Requests", d.requests, p.requests, false)
	row("Real input (tok)", d.inputReal, p.inputReal, true)
	row("  cache read", d.cacheRead, p.cacheRead, false)
	row("  cache write", d.cacheCreation, p.cacheCreation, false)
	row("  fresh input", benchFreshInput(d), benchFreshInput(p), true)

	dr, pr := benchCacheRatio(d), benchCacheRatio(p)
	rcol := grey
	if pr-dr > 0.05 {
		rcol = green
	} else if dr-pr > 0.05 {
		rcol = red
	}
	fmt.Fprintf(w, "  %-22s %13.1f%% %13.1f%% %s%+13.1f%s\n",
		"Cache-hit ratio", dr, pr, rcol, pr-dr, "pp"+reset)

	// The bottom line: usage-weighted input. This is what reflects subscription
	// usage pressure (read×0.1, write×1.25, fresh×1.0).
	row("Usage-weighted input", benchEffectiveInput(d), benchEffectiveInput(p), true)

	fmt.Fprintln(w, "  ──────────────────────────────────────────────────────────────")

	de, pe := benchEffectiveInput(d), benchEffectiveInput(p)
	switch {
	case cooldown < anthropicCacheTTL:
		// The second arm (direct) ran while the first arm's cache was still warm
		// and read it, so direct looks artificially cheap. Don't print a verdict.
		fmt.Fprintf(w, "  %s⚠ contaminated: cooldown %s < %s cache TTL — the 2nd arm read the 1st arm's\n    warm cache, biasing the comparison against the proxy. Re-run with --cooldown 6m.%s\n", red, cooldown, anthropicCacheTTL, reset)
		fmt.Fprintf(w, "  %seach arm's own cache-hit ratio is still valid: proxy=%.1f%%, direct=%.1f%%%s\n", grey, pr, dr, reset)
	case pe < de:
		pct := float64(de-pe) / float64(de) * 100
		fmt.Fprintf(w, "  %s✓ Proxy's usage-weighted input is %s lower than direct (%.1f%%).%s\n", green, formatNumber(de-pe), pct, reset)
	case pe > de:
		pct := float64(pe-de) / float64(de) * 100
		fmt.Fprintf(w, "  %s✗ Proxy's usage-weighted input is %s HIGHER than direct (%.1f%%) — investigate.%s\n", red, formatNumber(pe-de), pct, reset)
	default:
		fmt.Fprintf(w, "  %s≈ Proxy and direct have equal usage-weighted input.%s\n", grey, reset)
	}
	fmt.Fprintf(w, "  %susage weights: cache read×0.1, write×1.25, fresh×1.0%s\n", grey, reset)
	fmt.Fprintln(w)
}

func printHelp() {
	fmt.Print(`
  iq — IndexQube CLI

  USAGE
    iq                   Start Claude Code (default)
    iq task [flags] TEXT Create a durable agent task (--backend fake|codex|claude, --write)
    iq tasks             List durable tasks
    iq task status TASK  Inspect canonical task and latest-turn state
    iq task show TASK    Show durable turns, commands, files, routes, and snapshots
    iq approvals          List pending durable approval requests
    iq approve APPROVAL   Approve one waiting backend action
    iq deny APPROVAL      Deny one waiting backend action
    iq cancel TASK         Durably request cancellation; safe to retry
    iq task close TASK     Close an idle task
    iq task reopen TASK    Reopen a closed task or acknowledge needs_attention
    iq continue TASK TEXT Continue a task; recover if its native session is lost
    iq claude            Start Claude Code via IndexQube
    iq claude --dev      Start Claude Code, dev mode (relaxed guards)
    iq claude --dump-payloads
                         Dump Anthropic payloads to .indexqube/dumps/iq-session-*.jsonl
    iq start             Start the local IndexQube daemon on 127.0.0.1:17373
    iq stop              Stop the local daemon
    iq status            Show daemon status
    iq logs              Print recent daemon logs
    iq doctor            Check daemon and supported agent setup
    iq setup [agent...]  Configure supported agents (claude, codex)
    iq unsetup [agent...] Restore IndexQube setup backups
    iq audit [latest]     Write a local agent security report from the latest dump
    iq bench             Proxy-vs-direct A/B: runs the same prompt in optimize
                         and observe modes, prints real Anthropic usage side by side
                         (flags: --prompt "...", --cooldown 6m)
    iq gemini            Gemini via IndexQube (coming soon)
    iq codex             Use iq setup codex
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
	// Respect an explicit mode (e.g. `iq bench` runs an observe arm and an
	// optimize arm); default to optimize for normal `iq claude`.
	if os.Getenv("INDEXQUBE_MODE") == "" {
		os.Setenv("INDEXQUBE_MODE", "optimize")
	}
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
