// Command iq wraps the Claude Code CLI with an in-process IndexQube optimizer.
// It boots the gateway on a random local port, injects ANTHROPIC_BASE_URL into
// Claude Code's environment, and forwards all traffic through the optimizer.
// The user's real OAuth / API key flows through unchanged — iq never sees it.
package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/server"
)

var version = "dev"

func main() {
	go checkForUpdate()

	port, err := getFreePort()
	if err != nil {
		fmt.Fprintf(os.Stderr, "iq: failed to find free port: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go startProxy(ctx, port)

	if !waitForProxy(port) {
		fmt.Fprintf(os.Stderr, "iq: proxy failed to start within 2s\n")
		os.Exit(1)
	}

	args := append([]string{"claude"}, os.Args[1:]...)
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("ANTHROPIC_BASE_URL=http://127.0.0.1:%d", port),
	)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		if cmd.Process != nil {
			cmd.Process.Signal(sig) //nolint:errcheck
		}
	}()

	cmd.Run() //nolint:errcheck
}

func startProxy(ctx context.Context, port int) {
	os.Setenv("INDEXQUBE_BIND_ADDR", fmt.Sprintf("127.0.0.1:%d", port))
	os.Setenv("INDEXQUBE_DEV_TOKEN", "iq-local")
	os.Setenv("INDEXQUBE_MODE", "optimize")
	os.Setenv("INDEXQUBE_ENABLE_BLOCK_OPTIMIZER", "true")
	// Suppress noisy info logs when embedded — only surface warnings and errors.
	if os.Getenv("INDEXQUBE_LOG_LEVEL") == "" {
		os.Setenv("INDEXQUBE_LOG_LEVEL", "warn")
	}
	server.Run(ctx) //nolint:errcheck
}

func getFreePort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port, nil
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
