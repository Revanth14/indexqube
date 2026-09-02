package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"golang.org/x/term"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
	"github.com/Revanth14/indexqube/gateway/internal/taskstore"
)

const uiRefreshInterval = time.Second

type uiView string

const (
	uiViewOverview     uiView = "overview"
	uiViewConversation uiView = "conversation"
	uiViewEvidence     uiView = "evidence"
)

type uiSnapshot struct {
	workspace  string
	tasks      []taskstore.Task
	selectedID string
	evidence   *taskstore.TaskEvidence
	approvals  []taskstore.Approval
	backends   []agent.BackendHealth
	view       uiView
	input      string
	status     string
}

type uiClient struct {
	controlURL string
	http       *http.Client
}

type uiApp struct {
	client         *uiClient
	workspace      string
	defaultBackend agent.BackendID
	defaultWrite   bool
	tasks          []taskstore.Task
	selected       int
	selectedID     string
	evidence       *taskstore.TaskEvidence
	approvals      []taskstore.Approval
	backends       []agent.BackendHealth
	view           uiView
	input          []rune
	inputUTF8      []byte
	escapeState    int
	status         string
}

func runUI(args []string) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runUICommand(ctx, args, os.Stdin, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "iq: ui failed: %v\n", err)
		os.Exit(1)
	}
}

func runUICommand(ctx context.Context, args []string, stdin, stdout *os.File, stderr io.Writer) error {
	fs := flag.NewFlagSet("ui", flag.ContinueOnError)
	fs.SetOutput(stderr)
	workspaceArg := fs.String("workspace", ".", "Git workspace")
	backend := fs.String("backend", "", "default backend (codex or claude)")
	write := fs.Bool("write", false, "make newly created tasks workspace-write")
	taskID := fs.String("task", "", "attach to a task")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("usage: iq ui [--workspace PATH] [--task TASK] [--backend BACKEND] [--write]")
	}
	if !term.IsTerminal(int(stdin.Fd())) || !term.IsTerminal(int(stdout.Fd())) {
		return errors.New("an interactive terminal is required")
	}
	workspacePath, err := resolveUIWorkspace(*workspaceArg)
	if err != nil {
		return err
	}
	controlURL, err := resolveControlURL()
	if err != nil {
		return err
	}
	app := &uiApp{
		client:    &uiClient{controlURL: controlURL, http: &http.Client{Timeout: 10 * time.Second}},
		workspace: workspacePath, defaultBackend: agent.BackendID(strings.TrimSpace(*backend)), defaultWrite: *write,
		selectedID: strings.TrimSpace(*taskID), view: uiViewOverview,
		status: "Type a request to start or continue a task. /help shows commands.",
	}
	if app.defaultBackend != "" && app.defaultBackend != agent.BackendCodex && app.defaultBackend != agent.BackendClaude {
		return fmt.Errorf("default backend must be codex or claude")
	}
	if err := app.refresh(ctx); err != nil {
		return err
	}
	oldState, err := term.MakeRaw(int(stdin.Fd()))
	if err != nil {
		return fmt.Errorf("enter terminal raw mode: %w", err)
	}
	defer term.Restore(int(stdin.Fd()), oldState) //nolint:errcheck
	fmt.Fprint(stdout, "\x1b[?25l")
	defer fmt.Fprint(stdout, "\x1b[?25h\x1b[0m\r\nDetached. Running tasks continue in the daemon.\r\n")

	inputBytes := make(chan byte, 32)
	inputErrors := make(chan error, 1)
	go func() {
		buffer := make([]byte, 1)
		for {
			_, readErr := stdin.Read(buffer)
			if readErr != nil {
				inputErrors <- readErr
				return
			}
			inputBytes <- buffer[0]
		}
	}()
	ticker := time.NewTicker(uiRefreshInterval)
	defer ticker.Stop()
	app.draw(stdout)
	for {
		select {
		case <-ctx.Done():
			return nil
		case readErr := <-inputErrors:
			if errors.Is(readErr, io.EOF) {
				return nil
			}
			return readErr
		case <-ticker.C:
			if err := app.refresh(ctx); err != nil {
				app.status = "Refresh failed: " + err.Error()
			}
			app.draw(stdout)
		case value := <-inputBytes:
			detach, submit := app.handleByte(value)
			if detach {
				return nil
			}
			if submit {
				line := strings.TrimSpace(string(app.input))
				app.input = nil
				if line != "" {
					detach = app.executeCommand(ctx, line)
					if detach {
						return nil
					}
				}
				if err := app.refresh(ctx); err != nil {
					app.status = "Refresh failed: " + err.Error()
				}
			}
			app.draw(stdout)
		}
	}
}

func (a *uiApp) handleByte(value byte) (detach, submit bool) {
	if a.escapeState == 1 {
		if value == '[' {
			a.escapeState = 2
		} else {
			a.escapeState = 0
		}
		return false, false
	}
	if a.escapeState == 2 {
		switch value {
		case 'A':
			a.moveSelection(-1)
		case 'B':
			a.moveSelection(1)
		}
		a.escapeState = 0
		return false, false
	}
	switch value {
	case 3, 4: // Ctrl-C and Ctrl-D detach; cancellation is always explicit.
		return true, false
	case 13, 10:
		return false, true
	case 8, 127:
		if len(a.input) > 0 {
			a.input = a.input[:len(a.input)-1]
		}
	case 27:
		a.escapeState = 1
	case 14: // Ctrl-N
		a.moveSelection(1)
	case 16: // Ctrl-P
		a.moveSelection(-1)
	default:
		if value >= 32 && value < 127 {
			a.input = append(a.input, rune(value))
		} else if value >= 128 {
			a.inputUTF8 = append(a.inputUTF8, value)
			if utf8.FullRune(a.inputUTF8) {
				r, size := utf8.DecodeRune(a.inputUTF8)
				if r != utf8.RuneError || size > 1 {
					a.input = append(a.input, r)
				}
				a.inputUTF8 = a.inputUTF8[:0]
			}
		}
	}
	return false, false
}

func (a *uiApp) draw(out *os.File) {
	width, height, err := term.GetSize(int(out.Fd()))
	if err != nil || width <= 0 || height <= 0 {
		width, height = 120, 36
	}
	snapshot := uiSnapshot{
		workspace: a.workspace, tasks: a.tasks, selectedID: a.selectedID, evidence: a.evidence,
		approvals: a.approvals, backends: a.backends, view: a.view, input: string(a.input), status: a.status,
	}
	fmt.Fprint(out, "\x1b[H\x1b[2J", renderUIScreen(snapshot, width, height))
}

func (a *uiApp) refresh(ctx context.Context) error {
	tasks, err := a.client.tasks(ctx)
	if err != nil {
		return err
	}
	a.tasks = a.tasks[:0]
	for _, task := range tasks {
		if sameWorkspace(task.WorkspacePath, a.workspace) {
			a.tasks = append(a.tasks, task)
		}
	}
	if a.selectedID == "" && len(a.tasks) > 0 {
		a.selectedID = a.tasks[0].ID
	}
	a.syncSelection()
	a.backends, _ = a.client.backends(ctx)
	allApprovals, _ := a.client.approvals(ctx)
	a.approvals = a.approvals[:0]
	workspaceTasks := make(map[string]bool, len(a.tasks))
	for _, task := range a.tasks {
		workspaceTasks[task.ID] = true
	}
	for _, approval := range allApprovals {
		if workspaceTasks[approval.TaskID] {
			a.approvals = append(a.approvals, approval)
		}
	}
	a.evidence = nil
	if a.selectedID != "" {
		evidence, fetchErr := a.client.evidence(ctx, a.selectedID)
		if fetchErr != nil {
			return fetchErr
		}
		a.evidence = &evidence
	}
	return nil
}

func (a *uiApp) syncSelection() {
	for index := range a.tasks {
		if a.tasks[index].ID == a.selectedID {
			a.selected = index
			return
		}
	}
	if len(a.tasks) == 0 {
		a.selected, a.selectedID = 0, ""
		return
	}
	a.selected = 0
	a.selectedID = a.tasks[0].ID
}

func (a *uiApp) moveSelection(delta int) {
	if len(a.tasks) == 0 {
		return
	}
	a.selected = (a.selected + delta + len(a.tasks)) % len(a.tasks)
	a.selectedID = a.tasks[a.selected].ID
	a.status = fmt.Sprintf("Selected task %s", shortID(a.selectedID))
}

func (a *uiApp) executeCommand(ctx context.Context, line string) bool {
	if !strings.HasPrefix(line, "/") {
		a.submitPrompt(ctx, line, false, "", false)
		return false
	}
	name, rest, _ := strings.Cut(strings.TrimPrefix(line, "/"), " ")
	name, rest = strings.ToLower(strings.TrimSpace(name)), strings.TrimSpace(rest)
	switch name {
	case "q", "quit", "detach":
		return true
	case "help", "?":
		a.status = "/new [backend] [read|write] PROMPT | /select N|ID | /view overview|conversation|evidence | /approve [ID] | /deny [ID] | /cancel | /handoff BACKEND [PROMPT] | /refresh | /quit"
	case "refresh":
		a.status = "Refreshed canonical task state."
	case "view":
		view := uiView(strings.ToLower(rest))
		if view != uiViewOverview && view != uiViewConversation && view != uiViewEvidence {
			a.status = "View must be overview, conversation, or evidence."
		} else {
			a.view, a.status = view, "Showing "+string(view)+"."
		}
	case "select":
		a.selectTask(rest)
	case "new":
		a.newCommand(ctx, rest)
	case "approve", "deny":
		a.decideApproval(ctx, name, rest)
	case "cancel":
		if a.selectedID == "" {
			a.status = "No task is selected."
		} else if err := a.client.cancel(ctx, a.selectedID); err != nil {
			a.status = "Cancel failed: " + err.Error()
		} else {
			a.status = "Cancellation requested."
		}
	case "handoff":
		a.handoff(ctx, rest)
	case "close", "reopen", "pin", "unpin":
		if a.selectedID == "" {
			a.status = "No task is selected."
		} else if err := a.client.taskAction(ctx, a.selectedID, name); err != nil {
			a.status = strings.Title(name) + " failed: " + err.Error() //nolint:staticcheck -- compact UI label
		} else {
			a.status = strings.Title(name) + " completed." //nolint:staticcheck -- compact UI label
		}
	default:
		a.status = "Unknown command /" + name + ". Type /help."
	}
	return false
}

func (a *uiApp) submitPrompt(ctx context.Context, prompt string, forceNew bool, backend agent.BackendID, write bool) {
	if !forceNew && a.evidence != nil && a.evidence.Task.Status == taskstore.TaskOpen {
		if err := a.client.continueTask(ctx, a.evidence.Task.ID, prompt); err != nil {
			a.status = "Continue failed: " + err.Error()
		} else {
			a.status = "Turn started. The UI remains attached."
		}
		return
	}
	if !forceNew && a.evidence != nil && (a.evidence.Task.Status == taskstore.TaskRunning || a.evidence.Task.Status == taskstore.TaskAwaitingApproval) {
		a.status = "The selected task is active. Select another task or wait for it to finish."
		return
	}
	if backend == "" {
		backend = a.defaultBackend
	}
	if backend == "" {
		backend = a.preferredAvailableBackend()
	}
	if backend == "" {
		a.status = "No compatible Codex or Claude backend is available. Run iq doctor."
		return
	}
	if !forceNew {
		write = a.defaultWrite
	}
	task, err := a.client.createTask(ctx, a.workspace, backend, prompt, write)
	if err != nil {
		a.status = "Start failed: " + err.Error()
		return
	}
	a.selectedID = task.ID
	a.status = fmt.Sprintf("Started %s task %s.", backend, shortID(task.ID))
}

func (a *uiApp) newCommand(ctx context.Context, rest string) {
	fields := strings.Fields(rest)
	backend := a.defaultBackend
	write := a.defaultWrite
	consumed := 0
	for consumed < len(fields) {
		switch strings.ToLower(fields[consumed]) {
		case "codex", "claude":
			backend = agent.BackendID(strings.ToLower(fields[consumed]))
		case "write":
			write = true
		case "read", "readonly", "read-only":
			write = false
		default:
			goto parsed
		}
		consumed++
	}
parsed:
	prompt := strings.Join(fields[consumed:], " ")
	if prompt == "" {
		a.status = "Usage: /new [codex|claude] [read|write] PROMPT"
		return
	}
	a.submitPrompt(ctx, prompt, true, backend, write)
}

func (a *uiApp) selectTask(selector string) {
	selector = strings.TrimSpace(selector)
	if number, err := strconv.Atoi(selector); err == nil && number >= 1 && number <= len(a.tasks) {
		a.selected, a.selectedID = number-1, a.tasks[number-1].ID
		a.status = "Selected task " + shortID(a.selectedID)
		return
	}
	var match string
	for _, task := range a.tasks {
		if task.ID == selector || strings.HasPrefix(task.ID, selector) || strings.HasPrefix(shortID(task.ID), selector) {
			if match != "" {
				a.status = "Task selector is ambiguous."
				return
			}
			match = task.ID
		}
	}
	if match == "" {
		a.status = "Task not found in this workspace."
		return
	}
	a.selectedID = match
	a.syncSelection()
	a.status = "Selected task " + shortID(match)
}

func (a *uiApp) decideApproval(ctx context.Context, decision, selector string) {
	candidates := make([]taskstore.Approval, 0)
	for _, approval := range a.approvals {
		if a.selectedID == "" || approval.TaskID == a.selectedID {
			candidates = append(candidates, approval)
		}
	}
	approvalID := strings.TrimSpace(selector)
	if approvalID == "" {
		if len(candidates) != 1 {
			a.status = fmt.Sprintf("Specify an approval ID (%d pending for this selection).", len(candidates))
			return
		}
		approvalID = candidates[0].ID
	} else {
		for _, approval := range candidates {
			if strings.HasPrefix(approval.ID, approvalID) {
				approvalID = approval.ID
				break
			}
		}
	}
	if err := a.client.decide(ctx, approvalID, decision); err != nil {
		a.status = strings.Title(decision) + " failed: " + err.Error() //nolint:staticcheck -- compact UI label
	} else {
		a.status = "Approval " + decision + " decision persisted."
	}
}

func (a *uiApp) handoff(ctx context.Context, rest string) {
	if a.selectedID == "" {
		a.status = "No task is selected."
		return
	}
	backendText, prompt, _ := strings.Cut(strings.TrimSpace(rest), " ")
	backend := agent.BackendID(strings.ToLower(backendText))
	if backend != agent.BackendCodex && backend != agent.BackendClaude {
		a.status = "Usage: /handoff codex|claude [PROMPT]"
		return
	}
	if err := a.client.handoff(ctx, a.selectedID, backend, strings.TrimSpace(prompt)); err != nil {
		a.status = "Handoff failed: " + err.Error()
	} else {
		a.status = "Canonical handoff started on " + string(backend) + "."
	}
}

func (a *uiApp) preferredAvailableBackend() agent.BackendID {
	for _, preferred := range []agent.BackendID{agent.BackendCodex, agent.BackendClaude} {
		for _, health := range a.backends {
			if health.Backend == preferred && health.Status == agent.HealthAvailable {
				return preferred
			}
		}
	}
	return ""
}

func (c *uiClient) tasks(ctx context.Context) ([]taskstore.Task, error) {
	var result struct {
		Tasks []taskstore.Task `json:"tasks"`
	}
	err := c.do(ctx, http.MethodGet, "/control/v1/tasks?limit=100", nil, http.StatusOK, &result)
	return result.Tasks, err
}

func (c *uiClient) evidence(ctx context.Context, taskID string) (taskstore.TaskEvidence, error) {
	var result taskstore.TaskEvidence
	err := c.do(ctx, http.MethodGet, "/control/v1/tasks/"+taskID+"/evidence", nil, http.StatusOK, &result)
	return result, err
}

func (c *uiClient) approvals(ctx context.Context) ([]taskstore.Approval, error) {
	var result struct {
		Approvals []taskstore.Approval `json:"approvals"`
	}
	err := c.do(ctx, http.MethodGet, "/control/v1/approvals?status=pending&limit=100", nil, http.StatusOK, &result)
	return result.Approvals, err
}

func (c *uiClient) backends(ctx context.Context) ([]agent.BackendHealth, error) {
	var result struct {
		Backends []agent.BackendHealth `json:"backends"`
	}
	err := c.do(ctx, http.MethodGet, "/control/v1/backends", nil, http.StatusOK, &result)
	return result.Backends, err
}

func (c *uiClient) createTask(ctx context.Context, workspace string, backend agent.BackendID, prompt string, write bool) (taskstore.Task, error) {
	permission := agent.PermissionReadOnly
	if write {
		permission = agent.PermissionWrite
	}
	var result taskstore.Task
	err := c.do(ctx, http.MethodPost, "/control/v1/tasks", map[string]any{
		"workspace": workspace, "backend": backend, "permission": permission, "prompt": prompt,
	}, http.StatusAccepted, &result)
	return result, err
}

func (c *uiClient) continueTask(ctx context.Context, taskID, prompt string) error {
	return c.do(ctx, http.MethodPost, "/control/v1/tasks/"+taskID+"/turns", map[string]string{"prompt": prompt}, http.StatusAccepted, nil)
}

func (c *uiClient) decide(ctx context.Context, approvalID, decision string) error {
	return c.do(ctx, http.MethodPost, "/control/v1/approvals/"+approvalID+"/decision", map[string]string{"decision": decision}, http.StatusOK, nil)
}

func (c *uiClient) cancel(ctx context.Context, taskID string) error {
	return c.do(ctx, http.MethodPost, "/control/v1/tasks/"+taskID+"/cancel", nil, http.StatusOK, nil)
}

func (c *uiClient) handoff(ctx context.Context, taskID string, backend agent.BackendID, prompt string) error {
	return c.do(ctx, http.MethodPost, "/control/v1/tasks/"+taskID+"/handoffs", map[string]any{
		"to_backend": backend, "prompt": prompt,
	}, http.StatusAccepted, nil)
}

func (c *uiClient) taskAction(ctx context.Context, taskID, action string) error {
	return c.do(ctx, http.MethodPost, "/control/v1/tasks/"+taskID+"/"+action, nil, http.StatusOK, nil)
}

func (c *uiClient) do(ctx context.Context, method, path string, body any, wantStatus int, target any) error {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(raw)
	}
	request, err := newControlRequest(ctx, method, c.controlURL+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.http.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if err := verifyControlResponse(response); err != nil {
		return err
	}
	if response.StatusCode != wantStatus {
		return responseError("control request", response)
	}
	if target != nil {
		if err := json.NewDecoder(response.Body).Decode(target); err != nil {
			return err
		}
	}
	return nil
}

func resolveUIWorkspace(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		path = "."
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	command := exec.Command("git", "-C", abs, "rev-parse", "--show-toplevel")
	if output, commandErr := command.Output(); commandErr == nil {
		abs = strings.TrimSpace(string(output))
	}
	if resolved, evalErr := filepath.EvalSymlinks(abs); evalErr == nil {
		abs = resolved
	}
	return filepath.Clean(abs), nil
}

func sameWorkspace(left, right string) bool {
	leftResolved, leftErr := filepath.EvalSymlinks(left)
	rightResolved, rightErr := filepath.EvalSymlinks(right)
	if leftErr == nil {
		left = leftResolved
	}
	if rightErr == nil {
		right = rightResolved
	}
	return filepath.Clean(left) == filepath.Clean(right)
}

func renderUIScreen(snapshot uiSnapshot, width, height int) string {
	if width < 60 || height < 16 {
		return "IndexQube needs a terminal at least 60x16. Resize to continue."
	}
	health := make([]string, 0, len(snapshot.backends))
	for _, backend := range snapshot.backends {
		if backend.Backend == agent.BackendFake {
			continue
		}
		label := string(backend.Backend) + ":" + string(backend.Status)
		if backend.Version != "" {
			label += " " + backend.Version
		}
		health = append(health, terminalText(label))
	}
	header := fitTerminalLine(" IndexQube | "+filepath.Base(snapshot.workspace)+" | "+strings.Join(health, " | "), width)
	bodyHeight := height - 4
	leftWidth := 34
	if width < 92 {
		leftWidth = 25
	}
	rightWidth := width - leftWidth - 3
	left := renderTaskPane(snapshot, leftWidth, bodyHeight)
	right := renderDetailPane(snapshot, rightWidth, bodyHeight)
	lines := []string{header, strings.Repeat("-", width)}
	for row := 0; row < bodyHeight; row++ {
		lines = append(lines, padTerminalLine(lineAt(left, row), leftWidth)+" | "+fitTerminalLine(lineAt(right, row), rightWidth))
	}
	status := fitTerminalLine(" "+terminalText(snapshot.status), width)
	prompt := fitTerminalLine(" > "+terminalText(snapshot.input), width)
	lines = append(lines, status, prompt)
	return strings.Join(lines, "\r\n")
}

func renderTaskPane(snapshot uiSnapshot, width, height int) []string {
	lines := []string{fitTerminalLine(" TASKS (Ctrl-N/P)", width)}
	for index, task := range snapshot.tasks {
		marker := " "
		if task.ID == snapshot.selectedID {
			marker = ">"
		}
		line := fmt.Sprintf("%s%d %-8s %-6s %s", marker, index+1, task.Status, task.PreferredBackend, terminalText(task.OriginalGoal))
		lines = append(lines, fitTerminalLine(line, width))
		if len(lines) >= height {
			break
		}
	}
	if len(snapshot.tasks) == 0 {
		lines = append(lines, " No tasks yet.", " Type a request below.")
	}
	return lines
}

func renderDetailPane(snapshot uiSnapshot, width, height int) []string {
	if snapshot.evidence == nil {
		return []string{" NO TASK SELECTED", "", "Type a request to create a task."}
	}
	evidence := snapshot.evidence
	lines := []string{fitTerminalLine(fmt.Sprintf(" %s | %s | %s | %s", shortID(evidence.Task.ID), evidence.Task.Status,
		evidence.Task.PreferredBackend, evidence.Task.Permission), width)}
	switch snapshot.view {
	case uiViewConversation:
		lines = append(lines, " CONVERSATION")
		for _, turn := range evidence.Turns {
			lines = append(lines, wrapTerminal(fmt.Sprintf("You %d: %s", turn.Sequence, turn.UserMessage), width)...)
			if turn.AssistantMessage != "" {
				lines = append(lines, wrapTerminal(string(evidence.Task.PreferredBackend)+": "+turn.AssistantMessage, width)...)
			}
			if turn.ErrorMessage != "" {
				lines = append(lines, wrapTerminal("Error: "+turn.ErrorCode+": "+turn.ErrorMessage, width)...)
			}
		}
	case uiViewEvidence:
		lines = append(lines, evidenceLines(*evidence, width)...)
	default:
		lines = append(lines, " CONVERSATION")
		start := len(evidence.Turns) - 3
		if start < 0 {
			start = 0
		}
		for _, turn := range evidence.Turns[start:] {
			lines = append(lines, fitTerminalLine(fmt.Sprintf(" You %d: %s", turn.Sequence, terminalText(turn.UserMessage)), width))
			if turn.AssistantMessage != "" {
				lines = append(lines, fitTerminalLine(" Agent: "+terminalText(turn.AssistantMessage), width))
			}
		}
		pending := 0
		for _, approval := range snapshot.approvals {
			if approval.TaskID == evidence.Task.ID {
				if pending == 0 {
					lines = append(lines, "", " APPROVALS")
				}
				pending++
				lines = append(lines, fitTerminalLine(" ! "+shortID(approval.ID)+" "+approvalSummary(approval), width))
			}
		}
		lines = append(lines, "", fmt.Sprintf(" EVIDENCE | files %d | commands %d | verification %d | routes %d | handoffs %d",
			len(evidence.Files), len(evidence.Commands), len(evidence.VerificationRuns), len(evidence.Routes), len(evidence.Handoffs)))
		lines = append(lines, recentEvidenceLines(*evidence, width)...)
	}
	if len(lines) > height {
		lines = lines[len(lines)-height:]
	}
	return lines
}

func recentEvidenceLines(evidence taskstore.TaskEvidence, width int) []string {
	lines := make([]string, 0)
	if len(evidence.VerificationRuns) > 0 {
		run := evidence.VerificationRuns[len(evidence.VerificationRuns)-1]
		lines = append(lines, fitTerminalLine(" Verify: "+string(run.Status)+" - "+terminalText(run.Summary), width))
	}
	if len(evidence.Files) > 0 {
		file := evidence.Files[len(evidence.Files)-1]
		lines = append(lines, fitTerminalLine(" File: "+file.Operation+" "+terminalText(file.Path), width))
	}
	if len(evidence.Commands) > 0 {
		command := evidence.Commands[len(evidence.Commands)-1]
		lines = append(lines, fitTerminalLine(" Cmd: ["+command.Status+"] "+terminalText(command.Command), width))
	}
	if len(evidence.Routes) > 0 {
		route := evidence.Routes[len(evidence.Routes)-1]
		lines = append(lines, fitTerminalLine(fmt.Sprintf(" Route: #%d %s %s (%s)", route.Ordinal, route.Backend, route.Status, route.DecisionReason), width))
	}
	if len(evidence.Handoffs) > 0 {
		handoff := evidence.Handoffs[len(evidence.Handoffs)-1]
		lines = append(lines, fitTerminalLine(fmt.Sprintf(" Handoff: %s -> %s", handoff.FromBackend, handoff.ToBackend), width))
	}
	return lines
}

func evidenceLines(evidence taskstore.TaskEvidence, width int) []string {
	lines := []string{" FILES"}
	for _, file := range evidence.Files {
		lines = append(lines, fitTerminalLine("  "+file.Operation+" "+terminalText(file.Path), width))
	}
	lines = append(lines, " COMMANDS")
	for _, command := range evidence.Commands {
		lines = append(lines, fitTerminalLine("  ["+command.Status+"] "+terminalText(command.Command), width))
	}
	lines = append(lines, " VERIFICATION")
	for _, run := range evidence.VerificationRuns {
		lines = append(lines, fitTerminalLine("  "+string(run.Status)+" "+terminalText(run.Summary), width))
		for _, check := range run.Checks {
			lines = append(lines, fitTerminalLine("    ["+string(check.Status)+"] "+terminalText(check.Name), width))
			for _, finding := range check.Findings {
				lines = append(lines, fitTerminalLine("      "+finding.Severity+" "+finding.RuleID+" "+terminalText(finding.Path), width))
			}
		}
	}
	lines = append(lines, " ROUTES")
	for _, route := range evidence.Routes {
		lines = append(lines, fitTerminalLine(fmt.Sprintf("  #%d %s %s (%s)", route.Ordinal, route.Backend, route.Status, route.DecisionReason), width))
	}
	lines = append(lines, " HANDOFFS")
	for _, handoff := range evidence.Handoffs {
		lines = append(lines, fitTerminalLine(fmt.Sprintf("  %s -> %s", handoff.FromBackend, handoff.ToBackend), width))
	}
	return lines
}

func terminalText(value string) string {
	return strings.Map(func(r rune) rune {
		if r < 32 || r == 127 {
			return ' '
		}
		return r
	}, strings.TrimSpace(value))
}

func fitTerminalLine(value string, width int) string {
	value = terminalText(value)
	if width <= 0 {
		return ""
	}
	runes := []rune(value)
	if len(runes) > width {
		if width == 1 {
			return "…"
		}
		return string(runes[:width-1]) + "…"
	}
	return value
}

func padTerminalLine(value string, width int) string {
	value = fitTerminalLine(value, width)
	padding := width - utf8.RuneCountInString(value)
	if padding < 0 {
		padding = 0
	}
	return value + strings.Repeat(" ", padding)
}

func wrapTerminal(value string, width int) []string {
	value = terminalText(value)
	if value == "" {
		return []string{""}
	}
	words := strings.Fields(value)
	lines := make([]string, 0)
	line := ""
	for _, word := range words {
		if line == "" {
			line = word
			continue
		}
		if utf8.RuneCountInString(line)+1+utf8.RuneCountInString(word) <= width {
			line += " " + word
			continue
		}
		lines = append(lines, fitTerminalLine(line, width))
		line = word
	}
	if line != "" {
		lines = append(lines, fitTerminalLine(line, width))
	}
	return lines
}

func lineAt(lines []string, index int) string {
	if index < 0 || index >= len(lines) {
		return ""
	}
	return lines[index]
}

func shortID(value string) string {
	if index := strings.IndexByte(value, '_'); index >= 0 && len(value)-index-1 > 8 {
		return value[:index+1] + value[index+1:index+9]
	}
	if len(value) > 14 {
		return value[:14]
	}
	return value
}
