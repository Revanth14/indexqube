package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"
)

var dashboardHTTPClient = &http.Client{Timeout: 10 * time.Second}
var dashboardOpener = openDashboardURL

func runDashboard(args []string) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runDashboardCommand(ctx, args, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "iq: dashboard failed: %v\n", err)
		os.Exit(1)
	}
}

func runDashboardCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("dashboard", flag.ContinueOnError)
	fs.SetOutput(stderr)
	workspaceArg := fs.String("workspace", ".", "Git workspace")
	noOpen := fs.Bool("no-open", false, "print the one-time dashboard URL instead of opening a browser")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("usage: iq dashboard [--workspace PATH] [--no-open]")
	}
	workspace, err := resolveUIWorkspace(*workspaceArg)
	if err != nil {
		return err
	}
	controlURL, err := resolveControlURL()
	if err != nil {
		return err
	}
	body, err := json.Marshal(map[string]string{"workspace": workspace})
	if err != nil {
		return err
	}
	request, err := newControlRequest(ctx, http.MethodPost, controlURL+"/control/v1/dashboard-sessions", bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := dashboardHTTPClient.Do(request)
	if err != nil {
		return fmt.Errorf("create dashboard session: %w", err)
	}
	defer response.Body.Close()
	if err := verifyControlResponse(response); err != nil {
		return err
	}
	if response.StatusCode != http.StatusCreated {
		return responseError("create dashboard session", response)
	}
	var result struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode dashboard session: %w", err)
	}
	if err := validateDashboardURL(controlURL, result.URL); err != nil {
		return err
	}
	if *noOpen {
		fmt.Fprintln(stdout, result.URL)
		return nil
	}
	if err := dashboardOpener(result.URL); err != nil {
		fmt.Fprintf(stdout, "Open this one-time local URL:\n%s\n", result.URL)
		return fmt.Errorf("open browser: %w", err)
	}
	fmt.Fprintln(stdout, "Opened the local IndexQube dashboard.")
	return nil
}

func validateDashboardURL(controlURL, rawURL string) error {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return fmt.Errorf("invalid dashboard URL: %w", err)
	}
	wantOrigin, err := validateControlURL(controlURL)
	if err != nil {
		return err
	}
	gotOrigin := (&url.URL{Scheme: parsed.Scheme, Host: parsed.Host}).String()
	if gotOrigin != wantOrigin || parsed.Path != "/control/ui/" || parsed.Fragment != "" || parsed.Query().Get("ticket") == "" || len(parsed.Query()) != 1 {
		return fmt.Errorf("daemon returned an unsafe dashboard URL")
	}
	return nil
}

func openDashboardURL(rawURL string) error {
	var command *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		command = exec.Command("open", rawURL)
	case "linux":
		command = exec.Command("xdg-open", rawURL)
	default:
		return fmt.Errorf("automatic browser opening is not supported on %s", runtime.GOOS)
	}
	if err := command.Start(); err != nil {
		return err
	}
	return command.Process.Release()
}
