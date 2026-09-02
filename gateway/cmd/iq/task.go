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
	if len(args) > 0 {
		switch args[0] {
		case "status":
			if err := runTaskStatus(ctx, args[1:], os.Stdout); err != nil {
				fmt.Fprintf(os.Stderr, "iq: task status failed: %v\n", err)
				os.Exit(1)
			}
			return
		case "show":
			if err := runTaskShow(ctx, args[1:], os.Stdout); err != nil {
				fmt.Fprintf(os.Stderr, "iq: task show failed: %v\n", err)
				os.Exit(1)
			}
			return
		case "close", "reopen":
			if err := runTaskLifecycleCommand(ctx, args[1:], args[0], os.Stdout); err != nil {
				fmt.Fprintf(os.Stderr, "iq: task %s failed: %v\n", args[0], err)
				os.Exit(1)
			}
			return
		case "pin", "unpin":
			if err := runTaskPinCommand(ctx, args[1:], args[0], os.Stdout); err != nil {
				fmt.Fprintf(os.Stderr, "iq: task %s failed: %v\n", args[0], err)
				os.Exit(1)
			}
			return
		}
	}
	if err := runTaskCommand(ctx, args, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "iq: task failed: %v\n", err)
		os.Exit(1)
	}
}

func runTasks(args []string) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runTasksCommand(ctx, args, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "iq: tasks failed: %v\n", err)
		os.Exit(1)
	}
}

func runTasksCommand(ctx context.Context, args []string, out, errOut io.Writer) error {
	fs := flag.NewFlagSet("tasks", flag.ContinueOnError)
	fs.SetOutput(errOut)
	limit := fs.Int("limit", 50, "maximum tasks to show")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("usage: iq tasks [--limit N]")
	}
	controlURL, err := resolveControlURL()
	if err != nil {
		return err
	}
	req, err := newControlRequest(ctx, http.MethodGet, fmt.Sprintf("%s/control/v1/tasks?limit=%d", controlURL, *limit), nil)
	if err != nil {
		return err
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return fmt.Errorf("list tasks: %w", err)
	}
	defer resp.Body.Close()
	if err := verifyControlResponse(resp); err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return responseError("list tasks", resp)
	}
	var result struct {
		Tasks []taskstore.Task `json:"tasks"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode tasks: %w", err)
	}
	if len(result.Tasks) == 0 {
		fmt.Fprintln(out, "No tasks.")
		return nil
	}
	fmt.Fprintln(out, "TASK\tSTATUS\tBACKEND\tPERMISSION\tUPDATED\tGOAL")
	for _, task := range result.Tasks {
		fmt.Fprintf(out, "%s\t%s\t%s\t%s\t%s\t%s\n", task.ID, task.Status, task.PreferredBackend,
			task.Permission, task.UpdatedAt.Local().Format("2006-01-02 15:04"), oneLine(task.OriginalGoal, 80))
	}
	return nil
}

func runTaskStatus(ctx context.Context, args []string, out io.Writer) error {
	if len(args) != 1 || strings.TrimSpace(args[0]) == "" {
		return fmt.Errorf("usage: iq task status TASK")
	}
	controlURL, err := resolveControlURL()
	if err != nil {
		return err
	}
	req, err := newControlRequest(ctx, http.MethodGet, controlURL+"/control/v1/tasks/"+strings.TrimSpace(args[0])+"/state", nil)
	if err != nil {
		return err
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return fmt.Errorf("get task state: %w", err)
	}
	defer resp.Body.Close()
	if err := verifyControlResponse(resp); err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return responseError("get task state", resp)
	}
	var state taskstore.TaskState
	if err := json.NewDecoder(resp.Body).Decode(&state); err != nil {
		return fmt.Errorf("decode task state: %w", err)
	}
	fmt.Fprintf(out, "Task: %s\nStatus: %s\nBackend: %s\nPermission: %s\nWorkspace: %s\n",
		state.Task.ID, state.Task.Status, state.Task.PreferredBackend, state.Task.Permission, state.Task.WorkspacePath)
	if state.BackendPin != nil {
		fmt.Fprintf(out, "Routing: pinned to %s\n", state.BackendPin.Backend)
	} else {
		fmt.Fprintln(out, "Routing: unpinned")
	}
	if state.LatestTurn != nil {
		fmt.Fprintf(out, "Latest turn: %d (%s)\n", state.LatestTurn.Sequence, state.LatestTurn.Status)
		if state.LatestTurn.ErrorCode != "" {
			fmt.Fprintf(out, "Error: %s: %s\n", state.LatestTurn.ErrorCode, state.LatestTurn.ErrorMessage)
		}
	}
	if state.Cancellation != nil {
		fmt.Fprintf(out, "Cancellation: %s (%s)\n", state.Cancellation.ID, state.Cancellation.Status)
	}
	if state.Session != nil {
		fmt.Fprintf(out, "Native session: %s (%s)\n", state.Session.NativeSessionID, state.Session.Status)
	}
	return nil
}

func runTaskShow(ctx context.Context, args []string, out io.Writer) error {
	if len(args) != 1 || strings.TrimSpace(args[0]) == "" {
		return fmt.Errorf("usage: iq task show TASK")
	}
	controlURL, err := resolveControlURL()
	if err != nil {
		return err
	}
	taskID := strings.TrimSpace(args[0])
	req, err := newControlRequest(ctx, http.MethodGet, controlURL+"/control/v1/tasks/"+taskID+"/evidence", nil)
	if err != nil {
		return err
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return fmt.Errorf("get task evidence: %w", err)
	}
	defer resp.Body.Close()
	if err := verifyControlResponse(resp); err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return responseError("get task evidence", resp)
	}
	var evidence taskstore.TaskEvidence
	if err := json.NewDecoder(resp.Body).Decode(&evidence); err != nil {
		return fmt.Errorf("decode task evidence: %w", err)
	}
	renderTaskEvidence(out, evidence)
	return nil
}

func renderTaskEvidence(out io.Writer, evidence taskstore.TaskEvidence) {
	fmt.Fprintf(out, "Task: %s\nStatus: %s\nBackend: %s\nPermission: %s\nWorkspace: %s\nGoal: %s\n",
		evidence.Task.ID, evidence.Task.Status, evidence.Task.PreferredBackend, evidence.Task.Permission,
		evidence.Task.WorkspacePath, evidence.Task.OriginalGoal)
	if evidence.BackendPin != nil {
		fmt.Fprintf(out, "Routing: pinned to %s\n", evidence.BackendPin.Backend)
	} else {
		fmt.Fprintln(out, "Routing: unpinned")
	}
	if len(evidence.Turns) > 0 {
		fmt.Fprintln(out, "\nTurns:")
		for _, turn := range evidence.Turns {
			fmt.Fprintf(out, "  %d. %s — %s\n", turn.Sequence, turn.Status, oneLine(turn.UserMessage, 120))
			if turn.AssistantMessage != "" {
				fmt.Fprintf(out, "     %s\n", oneLine(turn.AssistantMessage, 160))
			}
			if turn.ErrorCode != "" {
				fmt.Fprintf(out, "     error: %s: %s\n", turn.ErrorCode, oneLine(turn.ErrorMessage, 160))
			}
		}
	}
	if len(evidence.Commands) > 0 {
		fmt.Fprintln(out, "\nCommands:")
		for _, command := range evidence.Commands {
			exit := ""
			if command.ExitCode != nil {
				exit = fmt.Sprintf(" exit=%d", *command.ExitCode)
			}
			fmt.Fprintf(out, "  [%s%s] %s\n", command.Status, exit, oneLine(command.Command, 200))
		}
	}
	if len(evidence.Files) > 0 {
		fmt.Fprintln(out, "\nFiles changed (workspace-authoritative):")
		for _, file := range evidence.Files {
			previous := ""
			if file.PreviousPath != "" {
				previous = " (from " + file.PreviousPath + ")"
			}
			fmt.Fprintf(out, "  %s %s%s\n", file.Operation, file.Path, previous)
		}
	}
	if evidence.EvidenceMismatch {
		fmt.Fprintln(out, "\nAttention: agent file events do not match the authoritative workspace delta.")
		if len(evidence.ReportedFiles) > 0 {
			fmt.Fprintln(out, "Agent-reported files:")
			for _, file := range evidence.ReportedFiles {
				fmt.Fprintf(out, "  %s %s\n", file.Operation, file.Path)
			}
		}
	}
	if len(evidence.Approvals) > 0 {
		fmt.Fprintln(out, "\nApprovals:")
		for _, approval := range evidence.Approvals {
			decision := ""
			if approval.Decision != "" {
				decision = " (" + string(approval.Decision) + ")"
			}
			fmt.Fprintf(out, "  %s — %s %s%s: %s\n", approval.ID, approval.Status, approval.Kind,
				decision, approvalSummary(approval))
		}
	}
	if len(evidence.Cancellations) > 0 {
		fmt.Fprintln(out, "\nCancellations:")
		for _, cancellation := range evidence.Cancellations {
			fmt.Fprintf(out, "  %s — turn %s: %s\n", cancellation.ID, cancellation.TurnID, cancellation.Status)
		}
	}
	if len(evidence.VerificationRuns) > 0 {
		fmt.Fprintln(out, "\nVerification:")
		for _, run := range evidence.VerificationRuns {
			fmt.Fprintf(out, "  %s — %s\n", run.Status, oneLine(run.Summary, 180))
			for _, check := range run.Checks {
				exit := ""
				if check.ExitCode != nil {
					exit = fmt.Sprintf(" exit=%d", *check.ExitCode)
				}
				location := ""
				if check.CWD != "" {
					location = " (cwd " + check.CWD + ")"
				}
				command := check.Command
				if command == "" {
					command = check.Name
				}
				label := check.Name
				if label == "" {
					label = command
				} else if command != "" && command != label {
					label += " — " + command
				}
				fmt.Fprintf(out, "    [%s%s] %s%s\n", check.Status, exit, oneLine(label, 240), location)
				if check.Output != "" {
					fmt.Fprintf(out, "      %s\n", oneLine(check.Output, 180))
				}
				for _, finding := range check.Findings {
					location := finding.Path
					if location != "" && finding.Line > 0 {
						location += fmt.Sprintf(":%d", finding.Line)
					}
					if location == "" {
						location = finding.Source
					}
					fmt.Fprintf(out, "      - %s %s at %s: %s\n", finding.Severity, finding.RuleID,
						oneLine(location, 120), oneLine(finding.Evidence, 160))
				}
			}
		}
	}
	if len(evidence.Routes) > 0 {
		fmt.Fprintln(out, "\nRoute attempts:")
		for _, route := range evidence.Routes {
			failure := ""
			if route.FailureClass != "" {
				failure = " failure=" + string(route.FailureClass)
			}
			if route.FallbackEligible {
				failure += " fallback-eligible"
			}
			fmt.Fprintf(out, "  %s #%d — %s (%s)%s\n", route.Backend, route.Ordinal, route.Status, route.DecisionReason, failure)
		}
	}
	if len(evidence.Handoffs) > 0 {
		fmt.Fprintln(out, "\nHandoffs:")
		for _, handoff := range evidence.Handoffs {
			fmt.Fprintf(out, "  %s — %s -> %s (turn %s)\n", handoff.ID, handoff.FromBackend, handoff.ToBackend, handoff.TurnID)
		}
	}
	fmt.Fprintf(out, "\nEvidence: %d snapshots, %d events, %d native sessions\n",
		len(evidence.Snapshots), len(evidence.Events), len(evidence.Sessions))
}

func runTaskCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("task", flag.ContinueOnError)
	fs.SetOutput(stderr)
	backend := fs.String("backend", "", "agent backend (fake, codex, or claude)")
	provider := fs.String("provider", "", "deprecated alias for --backend")
	workspacePath := fs.String("workspace", "", "Git workspace (default: current directory)")
	write := fs.Bool("write", false, "grant workspace-write permission")
	pin := fs.Bool("pin", false, "pin all continuations to the selected backend")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *backend != "" && *provider != "" && *backend != *provider {
		return fmt.Errorf("--backend and deprecated --provider alias disagree")
	}
	backendID := *backend
	if backendID == "" {
		backendID = *provider
	}
	if backendID == "" {
		backendID = string(agent.BackendFake)
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
		"workspace":   *workspacePath,
		"prompt":      prompt,
		"backend":     backendID,
		"permission":  permission,
		"pin_backend": *pin,
	})
	if err != nil {
		return err
	}
	req, err := newControlRequest(ctx, http.MethodPost, controlURL+"/control/v1/tasks", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return fmt.Errorf("create task: %w", err)
	}
	defer resp.Body.Close()
	if err := verifyControlResponse(resp); err != nil {
		return err
	}
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
	req, err := newControlRequest(ctx, http.MethodPost, controlURL+"/control/v1/tasks/"+taskID+"/turns", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return fmt.Errorf("continue task: %w", err)
	}
	defer resp.Body.Close()
	if err := verifyControlResponse(resp); err != nil {
		return err
	}
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
		return validateControlURL(explicit)
	}
	if st, err := readDaemonState(); err == nil {
		addr := normalizeDaemonAddr(st.ControlAddr)
		if addr == "" {
			addr = defaultControlAddr
		}
		if isControlHealthy(addr) {
			return validateControlURL(daemonURL(addr))
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
	return validateControlURL(daemonURL(addr))
}

func streamTaskEvents(ctx context.Context, controlURL, taskID string, after int64, stdout, stderr io.Writer) error {
	eventsURL := fmt.Sprintf("%s/control/v1/tasks/%s/events?after=%d", controlURL, taskID, after)
	req, err := newControlRequest(ctx, http.MethodGet, eventsURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return fmt.Errorf("stream task: %w", err)
	}
	defer resp.Body.Close()
	if err := verifyControlResponse(resp); err != nil {
		return err
	}
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
				changes := event.File.Changes
				if len(changes) == 0 && event.File.Path != "" {
					changes = []agent.FileChange{{Path: event.File.Path, Operation: event.File.Operation}}
				}
				for _, change := range changes {
					fmt.Fprintf(stderr, "  [iq] %s %s\n", change.Operation, change.Path)
				}
			}
		case agent.EventCommandFinished:
			if event.Command != nil {
				fmt.Fprintf(stderr, "  [iq] command %s: %s\n", event.Command.Status, oneLine(event.Command.Command, 160))
			}
		case agent.EventApprovalRequested:
			if event.Approval != nil {
				detail := event.Approval.Command
				if detail == "" && event.Approval.NetworkHost != "" {
					detail = event.Approval.NetworkProtocol + "://" + event.Approval.NetworkHost
				}
				if detail == "" {
					detail = event.Approval.GrantRoot
				}
				if detail == "" {
					detail = event.Approval.Reason
				}
				fmt.Fprintf(stderr, "  [iq] approval required %s (%s): %s\n", event.Approval.ApprovalID,
					event.Approval.Kind, oneLine(detail, 160))
				fmt.Fprintf(stderr, "  [iq] run: iq approve %s  (or iq deny %s)\n",
					event.Approval.ApprovalID, event.Approval.ApprovalID)
			}
		case agent.EventApprovalResolved:
			if event.Approval != nil {
				fmt.Fprintf(stderr, "  [iq] approval %s: %s\n", event.Approval.ApprovalID, event.Approval.Status)
			}
		case agent.EventVerificationCompleted:
			status := event.Metadata["verification_status"]
			summary := ""
			if event.Message != nil {
				summary = oneLine(event.Message.Text, 180)
			}
			fmt.Fprintf(stderr, "  [iq] verification %s: %s\n", status, summary)
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

func oneLine(value string, limit int) string {
	value = strings.Join(strings.Fields(value), " ")
	if limit > 0 && len(value) > limit {
		return value[:limit] + "…"
	}
	return value
}

func cancelTask(controlURL, taskID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := newControlRequest(ctx, http.MethodPost, controlURL+"/control/v1/tasks/"+taskID+"/cancel", nil)
	if err != nil {
		return
	}
	resp, err := (&http.Client{Timeout: 2 * time.Second}).Do(req)
	if err == nil {
		_ = verifyControlResponse(resp)
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
