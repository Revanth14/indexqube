package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
	"github.com/Revanth14/indexqube/gateway/internal/taskstore"
)

func runTask(args []string) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if len(args) > 0 && args[0] == "status" {
		if err := runTaskStatus(ctx, args[1:], os.Stdout); err != nil {
			fmt.Fprintf(os.Stderr, "iq: task status failed: %v\n", err)
			os.Exit(1)
		}
		return
	}
	if err := runTaskCommand(ctx, args, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "iq: task failed: %v\n", err)
		os.Exit(1)
	}
}

func runTaskStatus(ctx context.Context, args []string, out io.Writer) error {
	if len(args) != 1 || strings.TrimSpace(args[0]) == "" {
		return fmt.Errorf("usage: iq task status TASK")
	}
	controlURL, err := resolveControlURL()
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, controlURL+"/control/v1/tasks/"+strings.TrimSpace(args[0])+"/state", nil)
	if err != nil {
		return err
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return fmt.Errorf("get task state: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return responseError("get task state", resp)
	}
	var state taskstore.TaskState
	if err := json.NewDecoder(resp.Body).Decode(&state); err != nil {
		return fmt.Errorf("decode task state: %w", err)
	}
	fmt.Fprintf(out, "Task: %s\nStatus: %s\nBackend: %s\nPermission: %s\nWorkspace: %s\n",
		state.Task.ID, state.Task.Status, state.Task.PreferredBackend, state.Task.Permission, state.Task.WorkspacePath)
	if state.LatestTurn != nil {
		fmt.Fprintf(out, "Latest turn: %d (%s)\n", state.LatestTurn.Sequence, state.LatestTurn.Status)
		if state.LatestTurn.ErrorCode != "" {
			fmt.Fprintf(out, "Error: %s: %s\n", state.LatestTurn.ErrorCode, state.LatestTurn.ErrorMessage)
		}
	}
	if state.Session != nil {
		fmt.Fprintf(out, "Native session: %s (%s)\n", state.Session.NativeSessionID, state.Session.Status)
	}
	return nil
}

func runTaskCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("task", flag.ContinueOnError)
	fs.SetOutput(stderr)
	provider := fs.String("provider", string(agent.BackendFake), "agent backend (fake or codex read-only)")
	workspacePath := fs.String("workspace", "", "Git workspace (default: current directory)")
	write := fs.Bool("write", false, "grant workspace-write permission")
	if err := fs.Parse(args); err != nil {
		return err
	}
	prompt := strings.TrimSpace(strings.Join(fs.Args(), " "))
	if prompt == "" {
		return fmt.Errorf("prompt is required")
	}
	if *workspacePath == "" {
		cwd, err := os.Getwd()
		if err != nil {
			return fmt.Errorf("resolve current directory: %w", err)
		}
		*workspacePath = cwd
	}
	permission := agent.PermissionReadOnly
	if *write {
		permission = agent.PermissionWrite
	}
	controlURL, err := resolveControlURL()
	if err != nil {
		return err
	}
	body, err := json.Marshal(map[string]any{
		"workspace":  *workspacePath,
		"prompt":     prompt,
		"provider":   *provider,
		"permission": permission,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, controlURL+"/control/v1/tasks", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return fmt.Errorf("create task: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		return responseError("create task", resp)
	}
	var task taskstore.Task
	if err := json.NewDecoder(resp.Body).Decode(&task); err != nil {
		return fmt.Errorf("decode task: %w", err)
	}
	fmt.Fprintf(stderr, "  [iq] task %s via %s\n", task.ID, task.PreferredBackend)

	if err := streamTaskEvents(ctx, controlURL, task.ID, 0, stdout, stderr); err != nil {
		if errors.Is(err, context.Canceled) {
			cancelTask(controlURL, task.ID)
		}
		return err
	}
	return nil
}

func runContinue(args []string) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runContinueCommand(ctx, args, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "iq: continue failed: %v\n", err)
		os.Exit(1)
	}
}

func runContinueCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: iq continue TASK PROMPT")
	}
	taskID := strings.TrimSpace(args[0])
	prompt := strings.TrimSpace(strings.Join(args[1:], " "))
	if taskID == "" || prompt == "" {
		return fmt.Errorf("task ID and prompt are required")
	}
	controlURL, err := resolveControlURL()
	if err != nil {
		return err
	}
	body, err := json.Marshal(map[string]string{"prompt": prompt})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, controlURL+"/control/v1/tasks/"+taskID+"/turns", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return fmt.Errorf("continue task: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		return responseError("continue task", resp)
	}
	var result struct {
		Task          taskstore.Task `json:"task"`
		AfterSequence int64          `json:"after_sequence"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode continuation: %w", err)
	}
	fmt.Fprintf(stderr, "  [iq] continuing %s via %s\n", result.Task.ID, result.Task.PreferredBackend)
	if err := streamTaskEvents(ctx, controlURL, result.Task.ID, result.AfterSequence, stdout, stderr); err != nil {
		if errors.Is(err, context.Canceled) {
			cancelTask(controlURL, result.Task.ID)
		}
		return err
	}
	return nil
}

func resolveControlURL() (string, error) {
	if explicit := strings.TrimRight(strings.TrimSpace(os.Getenv("INDEXQUBE_CONTROL_URL")), "/"); explicit != "" {
		return explicit, nil
	}
	if st, err := readDaemonState(); err == nil {
		addr := normalizeDaemonAddr(st.ControlAddr)
		if addr == "" {
			addr = defaultControlAddr
		}
		if isControlHealthy(addr) {
			return daemonURL(addr), nil
		}
	}
	if err := startDaemon(defaultDaemonAddr); err != nil {
		return "", err
	}
	st, err := readDaemonState()
	if err != nil {
		return "", err
	}
	addr := normalizeDaemonAddr(st.ControlAddr)
	if addr == "" {
		addr = defaultControlAddr
	}
	return daemonURL(addr), nil
}

func streamTaskEvents(ctx context.Context, controlURL, taskID string, after int64, stdout, stderr io.Writer) error {
	eventsURL := fmt.Sprintf("%s/control/v1/tasks/%s/events?after=%d", controlURL, taskID, after)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, eventsURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return fmt.Errorf("stream task: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return responseError("stream task", resp)
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 1<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var event agent.Event
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
			return fmt.Errorf("decode event: %w", err)
		}
		switch event.Type {
		case agent.EventAssistantDelta:
			if event.Message != nil {
				fmt.Fprint(stdout, event.Message.Text)
			}
		case agent.EventAssistantMessage:
			if event.Message != nil {
				fmt.Fprintln(stdout, event.Message.Text)
			}
		case agent.EventFileChanged:
			if event.File != nil {
				fmt.Fprintf(stderr, "  [iq] %s %s\n", event.File.Operation, event.File.Path)
			}
		case agent.EventCompleted:
			return nil
		case agent.EventCancelled:
			return context.Canceled
		case agent.EventError:
			if event.Result != nil && event.Result.Error != "" {
				return errors.New(event.Result.Error)
			}
			return fmt.Errorf("backend failed")
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return fmt.Errorf("event stream ended before a terminal event")
}

func cancelTask(controlURL, taskID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, controlURL+"/control/v1/tasks/"+taskID+"/cancel", nil)
	if err != nil {
		return
	}
	resp, err := (&http.Client{Timeout: 2 * time.Second}).Do(req)
	if err == nil {
		resp.Body.Close()
	}
}

func responseError(action string, resp *http.Response) error {
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 16*1024))
	text := strings.TrimSpace(string(raw))
	if text == "" {
		text = http.StatusText(resp.StatusCode)
	}
	return fmt.Errorf("%s: HTTP %d: %s", action, resp.StatusCode, text)
}
