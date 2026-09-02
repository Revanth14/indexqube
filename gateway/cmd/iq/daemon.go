package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
	claudebackend "github.com/Revanth14/indexqube/gateway/internal/agent/claude"
	codexbackend "github.com/Revanth14/indexqube/gateway/internal/agent/codex"
	"github.com/Revanth14/indexqube/gateway/internal/agent/fake"
	"github.com/Revanth14/indexqube/gateway/internal/control"
	"github.com/Revanth14/indexqube/gateway/internal/localstate"
	"github.com/Revanth14/indexqube/gateway/internal/orchestrator"
	"github.com/Revanth14/indexqube/gateway/internal/server"
	"github.com/Revanth14/indexqube/gateway/internal/taskstore"
	"github.com/Revanth14/indexqube/gateway/internal/workspace"
)

const (
	defaultDaemonAddr  = "127.0.0.1:17373"
	defaultControlAddr = "127.0.0.1:17374"
	daemonStateFile    = "daemon.json"
	defaultLogLines    = 80
)

type daemonState struct {
	PID         int       `json:"pid"`
	Addr        string    `json:"addr"`
	URL         string    `json:"url"`
	ControlAddr string    `json:"control_addr"`
	ControlURL  string    `json:"control_url"`
	LogPath     string    `json:"log_path,omitempty"`
	StartedAt   time.Time `json:"started_at"`
	Version     string    `json:"version"`
}

func runStart(args []string) {
	fs := flag.NewFlagSet("start", flag.ExitOnError)
	addr := fs.String("addr", defaultDaemonAddr, "loopback address for the local daemon")
	controlAddr := fs.String("control-addr", defaultControlAddr, "loopback address for the task control API")
	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}
	if err := startDaemonWithControl(*addr, *controlAddr); err != nil {
		fmt.Fprintf(os.Stderr, "iq: start failed: %v\n", err)
		os.Exit(1)
	}
}

func runDaemon(args []string) {
	fs := flag.NewFlagSet("daemon", flag.ExitOnError)
	addr := fs.String("addr", defaultDaemonAddr, "loopback address for the local daemon")
	controlAddr := fs.String("control-addr", defaultControlAddr, "loopback address for the task control API")
	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}
	if err := runDaemonForegroundWithControl(*addr, *controlAddr); err != nil {
		fmt.Fprintf(os.Stderr, "iq: daemon exited: %v\n", err)
		os.Exit(1)
	}
}

func runStop(args []string) {
	fs := flag.NewFlagSet("stop", flag.ExitOnError)
	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}
	if err := stopDaemon(); err != nil {
		fmt.Fprintf(os.Stderr, "iq: stop failed: %v\n", err)
		os.Exit(1)
	}
}

func runStatus(args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}
	printDaemonStatus(os.Stdout)
}

func runLogs(args []string) {
	fs := flag.NewFlagSet("logs", flag.ExitOnError)
	lines := fs.Int("lines", defaultLogLines, "number of recent log lines to print")
	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}
	if err := printDaemonLogs(os.Stdout, *lines); err != nil {
		fmt.Fprintf(os.Stderr, "iq: logs failed: %v\n", err)
		os.Exit(1)
	}
}

func runDoctor(args []string) {
	fs := flag.NewFlagSet("doctor", flag.ExitOnError)
	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}
	printDoctor(os.Stdout)
}

func startDaemon(addr string) error {
	return startDaemonWithControl(addr, defaultControlAddr)
}

func startDaemonWithControl(addr, controlAddr string) error {
	addr = normalizeDaemonAddr(addr)
	controlAddr = normalizeDaemonAddr(controlAddr)
	if addr == "" {
		return fmt.Errorf("empty daemon address")
	}
	if !isLoopbackAddr(addr) || !isLoopbackAddr(controlAddr) {
		return fmt.Errorf("daemon and control addresses must be loopback, got %q and %q", addr, controlAddr)
	}
	if isDaemonHealthy(addr) && isControlHealthy(controlAddr) {
		fmt.Fprintf(os.Stderr, "  [iq] daemon already running at %s\n", daemonURL(addr))
		return nil
	}
	if isDaemonHealthy(addr) {
		return fmt.Errorf("daemon at %s is running without its control API", daemonURL(addr))
	}

	home, err := indexQubeHome()
	if err != nil {
		return err
	}
	logDir := filepath.Join(home, "logs")
	if err := os.MkdirAll(logDir, 0o700); err != nil {
		return fmt.Errorf("create log dir: %w", err)
	}
	logPath := filepath.Join(logDir, "daemon-"+time.Now().Format("20060102-150405")+".log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("open log file: %w", err)
	}
	defer logFile.Close()

	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve executable: %w", err)
	}
	cmd := exec.Command(exe, "daemon", "--addr", addr, "--control-addr", controlAddr) //nolint:gosec
	cmd.Env = daemonEnv(os.Environ(), addr)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("spawn daemon: %w", err)
	}

	st := daemonState{
		PID:         cmd.Process.Pid,
		Addr:        addr,
		URL:         daemonURL(addr),
		ControlAddr: controlAddr,
		ControlURL:  daemonURL(controlAddr),
		LogPath:     logPath,
		StartedAt:   time.Now().UTC(),
		Version:     version,
	}
	if err := writeDaemonState(st); err != nil {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		return err
	}
	if !waitForDaemon(addr, controlAddr, 5*time.Second) {
		// The child never became usable, so terminate and reap it before
		// returning. Leaving it detached here can leak a failed daemon or zombie.
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		removeDaemonStateForPID(cmd.Process.Pid)
		return fmt.Errorf("daemon did not become healthy; see %s", logPath)
	}
	_ = cmd.Process.Release()
	fmt.Fprintf(os.Stderr, "  [iq] daemon started at %s\n", daemonURL(addr))
	fmt.Fprintf(os.Stderr, "  [iq] logs: %s\n", logPath)
	return nil
}

func runDaemonForeground(addr string) error {
	return runDaemonForegroundWithControl(addr, defaultControlAddr)
}

func runDaemonForegroundWithControl(addr, controlAddr string) error {
	addr = normalizeDaemonAddr(addr)
	controlAddr = normalizeDaemonAddr(controlAddr)
	if addr == "" {
		return fmt.Errorf("empty daemon address")
	}
	if !isLoopbackAddr(addr) || !isLoopbackAddr(controlAddr) {
		return fmt.Errorf("daemon and control addresses must be loopback, got %q and %q", addr, controlAddr)
	}

	os.Setenv("INDEXQUBE_BIND_ADDR", addr)
	if os.Getenv("INDEXQUBE_MODE") == "" {
		os.Setenv("INDEXQUBE_MODE", "optimize")
	}
	os.Setenv("INDEXQUBE_ENABLE_BLOCK_OPTIMIZER", "true")
	if os.Getenv("ADMIN_PORT") == "" {
		os.Setenv("ADMIN_PORT", "0")
	}
	if os.Getenv("INDEXQUBE_LOG_LEVEL") == "" {
		os.Setenv("INDEXQUBE_LOG_LEVEL", "warn")
	}
	if os.Getenv("IQ_TELEMETRY") == "" {
		os.Setenv("IQ_TELEMETRY", "off")
	}

	home, err := indexQubeHome()
	if err != nil {
		return err
	}
	store, err := taskstore.Open(filepath.Join(home, "tasks.db"))
	if err != nil {
		return err
	}
	defer store.Close()
	locks, err := workspace.NewLockManager(filepath.Join(home, "locks"), store, fmt.Sprintf("daemon_%d", os.Getpid()))
	if err != nil {
		return err
	}
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve executable: %w", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	runner := agent.NewRunner()
	claudePath, _ := exec.LookPath("claude")
	codexPath, _ := exec.LookPath("codex")
	service, err := orchestrator.NewService(runCtx, store, locks, orchestrator.NewRegistry(
		fake.New(runner, exe), claudebackend.New(runner, claudePath), codexbackend.New(runner, codexPath),
	))
	if err != nil {
		return err
	}
	reconciliation, err := service.ReconcileInterrupted(runCtx)
	if err != nil {
		return fmt.Errorf("reconcile interrupted tasks: %w", err)
	}
	if reconciliation.Recovered > 0 || reconciliation.NeedsAttention > 0 {
		fmt.Fprintf(os.Stderr, "indexqube: reconciled interrupted tasks: recoverable=%d needs_attention=%d\n",
			reconciliation.Recovered, reconciliation.NeedsAttention)
	}
	controlListener, err := net.Listen("tcp", controlAddr)
	if err != nil {
		return fmt.Errorf("listen on control API %s: %w", controlAddr, err)
	}
	controlToken, err := rotateControlCredential()
	if err != nil {
		controlListener.Close()
		return err
	}
	controlServer := &http.Server{Handler: control.NewHandler(service, controlToken), ReadHeaderTimeout: 5 * time.Second}

	prior, _ := readDaemonState()
	logPath := ""
	if prior.PID == os.Getpid() && prior.Addr == addr {
		logPath = prior.LogPath
	}
	_ = writeDaemonState(daemonState{
		PID:         os.Getpid(),
		Addr:        addr,
		URL:         daemonURL(addr),
		ControlAddr: controlAddr,
		ControlURL:  daemonURL(controlAddr),
		LogPath:     logPath,
		StartedAt:   time.Now().UTC(),
		Version:     version,
	})

	defer removeDaemonStateForPID(os.Getpid())

	proxyDone := make(chan error, 1)
	controlDone := make(chan error, 1)
	go func() { proxyDone <- server.Run(runCtx) }()
	go func() {
		err := controlServer.Serve(controlListener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		controlDone <- err
	}()

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-proxyDone:
	case runErr = <-controlDone:
	}
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = controlServer.Shutdown(shutdownCtx)
	shutdownCancel()
	service.Wait()
	return runErr
}

func stopDaemon() error {
	st, err := readDaemonState()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			fmt.Fprintln(os.Stderr, "  [iq] daemon is not running")
			return nil
		}
		return err
	}
	addr := normalizeDaemonAddr(st.Addr)
	if addr == "" {
		addr = defaultDaemonAddr
	}
	if !isDaemonHealthy(addr) {
		removeDaemonStateForPID(st.PID)
		fmt.Fprintln(os.Stderr, "  [iq] daemon state was stale; cleaned up")
		return nil
	}
	proc, err := os.FindProcess(st.PID)
	if err != nil {
		return fmt.Errorf("find process %d: %w", st.PID, err)
	}
	if err := proc.Signal(syscall.SIGTERM); err != nil {
		return fmt.Errorf("signal process %d: %w", st.PID, err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if !isDaemonHealthy(addr) {
			removeDaemonStateForPID(st.PID)
			fmt.Fprintln(os.Stderr, "  [iq] daemon stopped")
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("daemon did not stop within 5s")
}

func printDaemonStatus(w io.Writer) {
	st, err := readDaemonState()
	if err != nil {
		fmt.Fprintf(w, "IndexQube daemon: stopped\n")
		fmt.Fprintf(w, "Default URL: %s\n", daemonURL(defaultDaemonAddr))
		return
	}
	addr := normalizeDaemonAddr(st.Addr)
	if addr == "" {
		addr = defaultDaemonAddr
	}
	if isDaemonHealthy(addr) {
		fmt.Fprintf(w, "IndexQube daemon: running\n")
	} else {
		fmt.Fprintf(w, "IndexQube daemon: stopped (stale state)\n")
	}
	fmt.Fprintf(w, "URL: %s\n", daemonURL(addr))
	fmt.Fprintf(w, "PID: %d\n", st.PID)
	if !st.StartedAt.IsZero() {
		fmt.Fprintf(w, "Started: %s\n", st.StartedAt.Local().Format(time.RFC3339))
	}
	if st.LogPath != "" {
		fmt.Fprintf(w, "Logs: %s\n", st.LogPath)
	}
	if st.ControlURL != "" {
		fmt.Fprintf(w, "Control API: %s\n", st.ControlURL)
	}
}

func printDaemonLogs(w io.Writer, lines int) error {
	if lines <= 0 {
		lines = defaultLogLines
	}
	st, err := readDaemonState()
	if err != nil {
		return fmt.Errorf("daemon state not found")
	}
	if st.LogPath == "" {
		return fmt.Errorf("daemon log path not recorded")
	}
	f, err := os.Open(st.LogPath)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, line := range tailLines(f, lines) {
		fmt.Fprintln(w, line)
	}
	return nil
}

func printDoctor(w io.Writer) {
	fmt.Fprintln(w, "IndexQube doctor")
	fmt.Fprintln(w, "---------------")
	st, err := readDaemonState()
	addr := defaultDaemonAddr
	if err == nil && st.Addr != "" {
		addr = normalizeDaemonAddr(st.Addr)
	}
	if isDaemonHealthy(addr) {
		fmt.Fprintf(w, "daemon: ok (%s)\n", daemonURL(addr))
	} else {
		fmt.Fprintf(w, "daemon: not running (%s)\n", daemonURL(addr))
	}
	printBinaryCheck(w, "claude")
	printBinaryCheck(w, "codex")
	printBinaryCheck(w, "gemini")
	if codexConfigHasIndexQube() {
		fmt.Fprintln(w, "codex setup: configured")
	} else {
		fmt.Fprintln(w, "codex setup: not configured")
	}
	if claudeShellHasIndexQube() {
		fmt.Fprintln(w, "claude setup: configured")
	} else {
		fmt.Fprintln(w, "claude setup: not configured")
	}
}

func printBinaryCheck(w io.Writer, name string) {
	path, err := exec.LookPath(name)
	if err != nil {
		fmt.Fprintf(w, "%s: not found\n", name)
		return
	}
	fmt.Fprintf(w, "%s: %s\n", name, path)
}

func daemonEnv(base []string, addr string) []string {
	env := envMap(base)
	env["INDEXQUBE_BIND_ADDR"] = addr
	env["INDEXQUBE_MODE"] = defaultString(env["INDEXQUBE_MODE"], "optimize")
	env["INDEXQUBE_ENABLE_BLOCK_OPTIMIZER"] = "true"
	env["ADMIN_PORT"] = defaultString(env["ADMIN_PORT"], "0")
	env["INDEXQUBE_LOG_LEVEL"] = defaultString(env["INDEXQUBE_LOG_LEVEL"], "warn")
	if env["IQ_TELEMETRY"] == "" {
		env["IQ_TELEMETRY"] = "off"
	}
	return flattenEnv(env)
}

func envMap(list []string) map[string]string {
	out := make(map[string]string, len(list))
	for _, kv := range list {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		out[k] = v
	}
	return out
}

func flattenEnv(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	return out
}

func defaultString(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

func indexQubeHome() (string, error) {
	return localstate.Ensure()
}

func daemonStatePath() (string, error) {
	home, err := indexQubeHome()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, daemonStateFile), nil
}

func readDaemonState() (daemonState, error) {
	path, err := daemonStatePath()
	if err != nil {
		return daemonState{}, err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return daemonState{}, err
	}
	var st daemonState
	if err := json.Unmarshal(raw, &st); err != nil {
		return daemonState{}, err
	}
	return st, nil
}

func writeDaemonState(st daemonState) error {
	path, err := daemonStatePath()
	if err != nil {
		return err
	}
	raw, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return os.WriteFile(path, raw, 0o600)
}

func removeDaemonStateForPID(pid int) {
	st, err := readDaemonState()
	if err != nil {
		return
	}
	if st.PID == pid {
		if path, err := daemonStatePath(); err == nil {
			_ = os.Remove(path)
		}
	}
}

func daemonURL(addr string) string {
	addr = normalizeDaemonAddr(addr)
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		return strings.TrimRight(addr, "/")
	}
	return "http://" + addr
}

func normalizeDaemonAddr(addr string) string {
	addr = strings.TrimSpace(addr)
	addr = strings.TrimPrefix(addr, "http://")
	addr = strings.TrimPrefix(addr, "https://")
	addr = strings.TrimRight(addr, "/")
	return addr
}

func isLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func isDaemonHealthy(addr string) bool {
	client := &http.Client{Timeout: 300 * time.Millisecond}
	resp, err := client.Get(daemonURL(addr) + "/healthz") //nolint:noctx
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func isControlHealthy(addr string) bool {
	token, err := readControlCredential()
	if err != nil {
		return false
	}
	return isControlHealthyWithToken(addr, token)
}

func isControlHealthyWithToken(addr, token string) bool {
	client := &http.Client{Timeout: 300 * time.Millisecond}
	req, err := http.NewRequest(http.MethodGet, daemonURL(addr)+"/control/healthz", nil) //nolint:noctx
	if err != nil {
		return false
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK && resp.Header.Get(control.AuthContractHeader) == control.AuthContractValue
}

func waitForDaemon(addr, controlAddr string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if isDaemonHealthy(addr) && isControlHealthy(controlAddr) {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

func waitForProxyAddr(addr string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if isDaemonHealthy(addr) {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

func tailLines(r io.Reader, n int) []string {
	if n <= 0 {
		return nil
	}
	sc := bufio.NewScanner(r)
	buf := make([]string, 0, n)
	for sc.Scan() {
		if len(buf) == n {
			copy(buf, buf[1:])
			buf[n-1] = sc.Text()
			continue
		}
		buf = append(buf, sc.Text())
	}
	return buf
}
