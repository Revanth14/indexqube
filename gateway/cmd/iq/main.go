// Command iq wraps the Claude Code CLI with an in-process IndexQube optimizer.
// It boots the gateway on a random local port, injects ANTHROPIC_BASE_URL into
// Claude Code's environment, and forwards all traffic through the optimizer.
// The user's real OAuth / API key flows through unchanged — iq never sees it.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

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
		cwd, err := os.Getwd()
		if err != nil {
			fmt.Fprintf(os.Stderr, "iq: failed to resolve dump directory: %v\n", err)
			os.Exit(1)
		}
		dumpDir := filepath.Join(cwd, ".indexqube", "dumps")
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

	os.Setenv("INDEXQUBE_LOG_LEVEL", "off")
	go startProxy(ctx, ln)

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
		fmt.Fprintf(os.Stderr, "iq: 'claude' path is not absolute: %s\n", claudePath)
		os.Exit(1)
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
		sig := <-sigs
		if cmd.Process != nil {
			cmd.Process.Signal(sig) //nolint:errcheck
		}
	}()

	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			os.Exit(exitErr.ExitCode())
		}
		fmt.Fprintf(os.Stderr, "iq: failed to run claude: %v\n", err)
		os.Exit(1)
	}
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
