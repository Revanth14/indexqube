// Package control exposes the local-only daemon API used by IndexQube CLI
// clients. It is deliberately separate from the externally bindable model
// proxy surface.
package control

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
	"github.com/Revanth14/indexqube/gateway/internal/orchestrator"
	"github.com/Revanth14/indexqube/gateway/internal/taskstore"
	"github.com/Revanth14/indexqube/gateway/internal/workspace"
)

type Handler struct {
	service   *orchestrator.Service
	mux       *http.ServeMux
	token     string
	dashboard *dashboardSessions
}

func NewHandler(service *orchestrator.Service, token string) *Handler {
	if strings.TrimSpace(token) == "" {
		panic("control API token must not be empty")
	}
	h := &Handler{service: service, mux: http.NewServeMux(), token: token, dashboard: newDashboardSessions()}
	h.mux.HandleFunc("GET /control/healthz", h.health)
	h.mux.HandleFunc("POST /control/v1/dashboard-sessions", h.createDashboardSession)
	h.mux.HandleFunc("GET /control/v1/dashboard-context", h.dashboardContext)
	h.mux.HandleFunc("GET /control/v1/backends", h.backends)
	h.mux.HandleFunc("GET /control/v1/approvals", h.listApprovals)
	h.mux.HandleFunc("POST /control/v1/approvals/{approvalID}/decision", h.decideApproval)
	h.mux.HandleFunc("GET /control/v1/tasks", h.listTasks)
	h.mux.HandleFunc("POST /control/v1/tasks", h.createTask)
	h.mux.HandleFunc("GET /control/v1/tasks/{taskID}", h.getTask)
	h.mux.HandleFunc("GET /control/v1/tasks/{taskID}/state", h.getTaskState)
	h.mux.HandleFunc("GET /control/v1/tasks/{taskID}/evidence", h.getTaskEvidence)
	h.mux.HandleFunc("POST /control/v1/tasks/{taskID}/turns", h.continueTask)
	h.mux.HandleFunc("POST /control/v1/tasks/{taskID}/handoffs", h.handoffTask)
	h.mux.HandleFunc("POST /control/v1/tasks/{taskID}/cancel", h.cancelTask)
	h.mux.HandleFunc("POST /control/v1/tasks/{taskID}/close", h.closeTask)
	h.mux.HandleFunc("POST /control/v1/tasks/{taskID}/reopen", h.reopenTask)
	h.mux.HandleFunc("POST /control/v1/tasks/{taskID}/pin", h.pinTask)
	h.mux.HandleFunc("POST /control/v1/tasks/{taskID}/unpin", h.unpinTask)
	h.mux.HandleFunc("GET /control/v1/tasks/{taskID}/events", h.taskEvents)
	return h
}

type continueTaskRequest struct {
	Prompt         string `json:"prompt"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

type handoffTaskRequest struct {
	ToBackend      agent.BackendID `json:"to_backend"`
	Prompt         string          `json:"prompt,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
}

func (h *Handler) handoffTask(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var body handoffTaskRequest
	if err := dec.Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err)
		return
	}
	after, err := h.service.LatestEventSequence(r.Context(), r.PathValue("taskID"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "state_error", err)
		return
	}
	result, err := h.service.HandoffTask(r.Context(), orchestrator.HandoffTaskInput{
		TaskID: r.PathValue("taskID"), ToBackend: body.ToBackend, Prompt: body.Prompt, IdempotencyKey: body.IdempotencyKey,
	})
	if errors.Is(err, taskstore.ErrTaskNotFound) {
		writeError(w, http.StatusNotFound, "task_not_found", err)
		return
	}
	if errors.Is(err, workspace.ErrWorkspaceLocked) {
		writeError(w, http.StatusConflict, "workspace_busy", err)
		return
	}
	if err != nil {
		writeError(w, http.StatusConflict, "handoff_rejected", err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"task": result.Task, "handoff": result.Handoff, "after_sequence": after,
	})
}

func (h *Handler) continueTask(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var body continueTaskRequest
	if err := dec.Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err)
		return
	}
	after, err := h.service.LatestEventSequence(r.Context(), r.PathValue("taskID"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "state_error", err)
		return
	}
	task, err := h.service.ContinueTask(r.Context(), orchestrator.ContinueTaskInput{
		TaskID: r.PathValue("taskID"), Prompt: body.Prompt, IdempotencyKey: body.IdempotencyKey,
	})
	if errors.Is(err, workspace.ErrWorkspaceLocked) {
		writeError(w, http.StatusConflict, "workspace_busy", err)
		return
	}
	if err != nil {
		writeError(w, http.StatusConflict, "continuation_rejected", err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"task": task, "after_sequence": after})
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set(AuthContractHeader, AuthContractValue)
	if strings.HasPrefix(r.URL.Path, "/control/ui") {
		h.serveDashboard(w, r)
		return
	}
	bearer := authenticate(h.token, r)
	dashboard := h.authenticateDashboard(r)
	if !bearer && !dashboard {
		writeUnauthorized(w)
		return
	}
	if dashboard && !bearer && r.Method != http.MethodGet && !validDashboardMutation(r) {
		writeError(w, http.StatusForbidden, "dashboard_csrf_rejected", errors.New("dashboard mutation requires same-origin proof"))
		return
	}
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) backends(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"backends": h.service.Backends(r.Context())})
}

type createTaskRequest struct {
	Workspace      string               `json:"workspace"`
	Prompt         string               `json:"prompt"`
	Backend        agent.BackendID      `json:"backend"`
	Provider       agent.BackendID      `json:"provider,omitempty"`
	Permission     agent.PermissionMode `json:"permission"`
	PinBackend     bool                 `json:"pin_backend,omitempty"`
	IdempotencyKey string               `json:"idempotency_key,omitempty"`
}

func (h *Handler) createTask(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var body createTaskRequest
	if err := dec.Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err)
		return
	}
	if body.Backend != "" && body.Provider != "" && body.Backend != body.Provider {
		writeError(w, http.StatusBadRequest, "invalid_request", fmt.Errorf("backend and deprecated provider alias disagree"))
		return
	}
	task, err := h.service.StartTask(r.Context(), orchestrator.StartTaskInput{
		Workspace: body.Workspace, Prompt: body.Prompt, Backend: body.Backend, Provider: body.Provider,
		Permission: body.Permission, PinBackend: body.PinBackend, IdempotencyKey: body.IdempotencyKey,
	})
	if errors.Is(err, workspace.ErrWorkspaceLocked) {
		writeError(w, http.StatusConflict, "workspace_busy", err)
		return
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, "task_rejected", err)
		return
	}
	w.Header().Set("Location", "/control/v1/tasks/"+task.ID)
	writeJSON(w, http.StatusAccepted, task)
}

func (h *Handler) listTasks(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("limit")))
	tasks, err := h.service.Tasks(r.Context(), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "state_error", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tasks": tasks})
}

func (h *Handler) listApprovals(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("limit")))
	status := taskstore.ApprovalStatus(strings.TrimSpace(r.URL.Query().Get("status")))
	approvals, err := h.service.Approvals(r.Context(), strings.TrimSpace(r.URL.Query().Get("task_id")), status, limit)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"approvals": approvals})
}

type approvalDecisionRequest struct {
	Decision string `json:"decision"`
}

func (h *Handler) decideApproval(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var body approvalDecisionRequest
	if err := dec.Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err)
		return
	}
	approval, err := h.service.DecideApproval(r.Context(), r.PathValue("approvalID"), body.Decision)
	if err != nil {
		writeError(w, http.StatusConflict, "approval_rejected", err)
		return
	}
	writeJSON(w, http.StatusOK, approval)
}

func (h *Handler) getTask(w http.ResponseWriter, r *http.Request) {
	task, ok, err := h.service.Task(r.Context(), r.PathValue("taskID"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "state_error", err)
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "task_not_found", fmt.Errorf("task not found"))
		return
	}
	writeJSON(w, http.StatusOK, task)
}

func (h *Handler) getTaskState(w http.ResponseWriter, r *http.Request) {
	state, ok, err := h.service.TaskState(r.Context(), r.PathValue("taskID"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "state_error", err)
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "task_not_found", fmt.Errorf("task not found"))
		return
	}
	writeJSON(w, http.StatusOK, state)
}

func (h *Handler) getTaskEvidence(w http.ResponseWriter, r *http.Request) {
	evidence, ok, err := h.service.TaskEvidence(r.Context(), r.PathValue("taskID"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "state_error", err)
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "task_not_found", fmt.Errorf("task not found"))
		return
	}
	writeJSON(w, http.StatusOK, evidence)
}

func (h *Handler) cancelTask(w http.ResponseWriter, r *http.Request) {
	result, err := h.service.Cancel(r.Context(), r.PathValue("taskID"))
	if errors.Is(err, taskstore.ErrTaskNotFound) {
		writeError(w, http.StatusNotFound, "task_not_found", err)
		return
	}
	if errors.Is(err, taskstore.ErrTaskNotActive) {
		writeError(w, http.StatusConflict, "task_not_running", err)
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "state_error", err)
		return
	}
	status := http.StatusAccepted
	if result.Cancellation.Status == taskstore.CancellationCompleted {
		status = http.StatusOK
	}
	writeJSON(w, status, result)
}

func (h *Handler) closeTask(w http.ResponseWriter, r *http.Request) {
	h.transitionTask(w, r, true)
}

func (h *Handler) reopenTask(w http.ResponseWriter, r *http.Request) {
	h.transitionTask(w, r, false)
}

func (h *Handler) pinTask(w http.ResponseWriter, r *http.Request) {
	h.setTaskPin(w, r, true)
}

func (h *Handler) unpinTask(w http.ResponseWriter, r *http.Request) {
	h.setTaskPin(w, r, false)
}

func (h *Handler) setTaskPin(w http.ResponseWriter, r *http.Request, pinned bool) {
	result, err := h.service.SetTaskBackendPin(r.Context(), r.PathValue("taskID"), pinned)
	if errors.Is(err, taskstore.ErrTaskNotFound) {
		writeError(w, http.StatusNotFound, "task_not_found", err)
		return
	}
	if errors.Is(err, taskstore.ErrTaskActive) {
		writeError(w, http.StatusConflict, "task_active", err)
		return
	}
	if err != nil {
		writeError(w, http.StatusConflict, "pin_rejected", err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *Handler) transitionTask(w http.ResponseWriter, r *http.Request, closeTask bool) {
	var result orchestrator.TaskTransitionResult
	var err error
	if closeTask {
		result, err = h.service.CloseTask(r.Context(), r.PathValue("taskID"))
	} else {
		result, err = h.service.ReopenTask(r.Context(), r.PathValue("taskID"))
	}
	if errors.Is(err, taskstore.ErrTaskNotFound) {
		writeError(w, http.StatusNotFound, "task_not_found", err)
		return
	}
	if errors.Is(err, taskstore.ErrTaskActive) {
		writeError(w, http.StatusConflict, "task_active", err)
		return
	}
	if err != nil {
		writeError(w, http.StatusConflict, "transition_rejected", err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *Handler) taskEvents(w http.ResponseWriter, r *http.Request) {
	taskID := r.PathValue("taskID")
	if _, ok, err := h.service.Task(r.Context(), taskID); err != nil {
		writeError(w, http.StatusInternalServerError, "state_error", err)
		return
	} else if !ok {
		writeError(w, http.StatusNotFound, "task_not_found", fmt.Errorf("task not found"))
		return
	}
	after := parseSequence(r)
	live, unsubscribe := h.service.Subscribe(taskID)
	defer unsubscribe()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming_unsupported", fmt.Errorf("streaming unsupported"))
		return
	}

	replay, err := h.service.EventsAfter(r.Context(), taskID, after)
	if err != nil {
		writeSSEError(w, flusher, err)
		return
	}
	last := after
	for _, event := range replay {
		if event.Sequence <= last {
			continue
		}
		if err := writeEvent(w, flusher, event); err != nil {
			return
		}
		last = event.Sequence
		if isTerminal(event.Type) {
			return
		}
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case event := <-live:
			if event.Sequence <= last {
				continue
			}
			if err := writeEvent(w, flusher, event); err != nil {
				return
			}
			last = event.Sequence
			if isTerminal(event.Type) {
				return
			}
		}
	}
}

func parseSequence(r *http.Request) int64 {
	raw := strings.TrimSpace(r.URL.Query().Get("after"))
	if raw == "" {
		raw = strings.TrimSpace(r.Header.Get("Last-Event-ID"))
	}
	value, _ := strconv.ParseInt(raw, 10, 64)
	if value < 0 {
		return 0
	}
	return value
}

func writeEvent(w http.ResponseWriter, flusher http.Flusher, event agent.Event) error {
	raw, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", event.Sequence, event.Type, raw); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}

func writeSSEError(w http.ResponseWriter, flusher http.Flusher, err error) {
	raw, _ := json.Marshal(map[string]string{"error": err.Error()})
	_, _ = fmt.Fprintf(w, "event: error\ndata: %s\n\n", raw)
	flusher.Flush()
}

func isTerminal(typ agent.EventType) bool {
	return typ == agent.EventCompleted || typ == agent.EventError || typ == agent.EventCancelled
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code string, err error) {
	writeJSON(w, status, map[string]string{"code": code, "error": err.Error()})
}
