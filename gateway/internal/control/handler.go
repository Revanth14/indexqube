// Package control exposes the local-only daemon API used by IndexQube CLI
// clients. It is deliberately separate from the externally bindable model
// proxy surface.
package control

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/Revanth14/indexqube/gateway/internal/agent"
	"github.com/Revanth14/indexqube/gateway/internal/orchestrator"
)

type Handler struct {
	service *orchestrator.Service
	mux     *http.ServeMux
}

func NewHandler(service *orchestrator.Service) *Handler {
	h := &Handler{service: service, mux: http.NewServeMux()}
	h.mux.HandleFunc("GET /control/healthz", h.health)
	h.mux.HandleFunc("GET /control/v1/backends", h.backends)
	h.mux.HandleFunc("POST /control/v1/tasks", h.createTask)
	h.mux.HandleFunc("GET /control/v1/tasks/{taskID}", h.getTask)
	h.mux.HandleFunc("GET /control/v1/tasks/{taskID}/state", h.getTaskState)
	h.mux.HandleFunc("POST /control/v1/tasks/{taskID}/turns", h.continueTask)
	h.mux.HandleFunc("POST /control/v1/tasks/{taskID}/cancel", h.cancelTask)
	h.mux.HandleFunc("GET /control/v1/tasks/{taskID}/events", h.taskEvents)
	return h
}

type continueTaskRequest struct {
	Prompt         string `json:"prompt"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
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
	if err != nil {
		writeError(w, http.StatusConflict, "continuation_rejected", err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"task": task, "after_sequence": after})
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
	Provider       agent.BackendID      `json:"provider"`
	Permission     agent.PermissionMode `json:"permission"`
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
	task, err := h.service.StartTask(r.Context(), orchestrator.StartTaskInput{
		Workspace: body.Workspace, Prompt: body.Prompt, Provider: body.Provider,
		Permission: body.Permission, IdempotencyKey: body.IdempotencyKey,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, "task_rejected", err)
		return
	}
	w.Header().Set("Location", "/control/v1/tasks/"+task.ID)
	writeJSON(w, http.StatusAccepted, task)
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

func (h *Handler) cancelTask(w http.ResponseWriter, r *http.Request) {
	if !h.service.Cancel(r.PathValue("taskID")) {
		writeError(w, http.StatusConflict, "task_not_running", fmt.Errorf("task is not running"))
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "cancelling"})
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
