package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/orchestrator"
)

var lifecycleHTTPClient = &http.Client{Timeout: 10 * time.Second}

func runCancel(args []string) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runCancelCommand(ctx, args, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "iq: cancel failed: %v\n", err)
		os.Exit(1)
	}
}

func runCancelCommand(ctx context.Context, args []string, out io.Writer) error {
	if len(args) != 1 || strings.TrimSpace(args[0]) == "" {
		return fmt.Errorf("usage: iq cancel TASK")
	}
	taskID := strings.TrimSpace(args[0])
	controlURL, err := resolveControlURL()
	if err != nil {
		return err
	}
	req, err := newControlRequest(ctx, http.MethodPost,
		controlURL+"/control/v1/tasks/"+taskID+"/cancel", nil)
	if err != nil {
		return err
	}
	resp, err := lifecycleHTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("cancel task: %w", err)
	}
	defer resp.Body.Close()
	if err := verifyControlResponse(resp); err != nil {
		return err
	}
	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		return responseError("cancel task", resp)
	}
	var result orchestrator.CancelTaskResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode cancellation: %w", err)
	}
	fmt.Fprintf(out, "Task %s: cancellation %s\n", result.Task.ID, result.Cancellation.Status)
	return nil
}

func runTaskLifecycleCommand(ctx context.Context, args []string, action string, out io.Writer) error {
	if action != "close" && action != "reopen" {
		return fmt.Errorf("unsupported task action %q", action)
	}
	if len(args) != 1 || strings.TrimSpace(args[0]) == "" {
		return fmt.Errorf("usage: iq task %s TASK", action)
	}
	taskID := strings.TrimSpace(args[0])
	controlURL, err := resolveControlURL()
	if err != nil {
		return err
	}
	req, err := newControlRequest(ctx, http.MethodPost,
		controlURL+"/control/v1/tasks/"+taskID+"/"+action, nil)
	if err != nil {
		return err
	}
	resp, err := lifecycleHTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("%s task: %w", action, err)
	}
	defer resp.Body.Close()
	if err := verifyControlResponse(resp); err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return responseError(action+" task", resp)
	}
	var result orchestrator.TaskTransitionResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode task transition: %w", err)
	}
	suffix := ""
	if !result.Changed {
		suffix = " (unchanged)"
	}
	fmt.Fprintf(out, "Task %s: %s%s\n", result.Task.ID, result.Task.Status, suffix)
	return nil
}

func runTaskPinCommand(ctx context.Context, args []string, action string, out io.Writer) error {
	if action != "pin" && action != "unpin" {
		return fmt.Errorf("unsupported task pin action %q", action)
	}
	if len(args) != 1 || strings.TrimSpace(args[0]) == "" {
		return fmt.Errorf("usage: iq task %s TASK", action)
	}
	taskID := strings.TrimSpace(args[0])
	controlURL, err := resolveControlURL()
	if err != nil {
		return err
	}
	req, err := newControlRequest(ctx, http.MethodPost, controlURL+"/control/v1/tasks/"+taskID+"/"+action, nil)
	if err != nil {
		return err
	}
	resp, err := lifecycleHTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("%s task backend: %w", action, err)
	}
	defer resp.Body.Close()
	if err := verifyControlResponse(resp); err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return responseError(action+" task backend", resp)
	}
	var result orchestrator.TaskPinResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode task pin: %w", err)
	}
	suffix := ""
	if !result.Changed {
		suffix = " (unchanged)"
	}
	if result.BackendPin == nil {
		fmt.Fprintf(out, "Task %s: backend unpinned%s\n", result.Task.ID, suffix)
	} else {
		fmt.Fprintf(out, "Task %s: pinned to %s%s\n", result.Task.ID, result.BackendPin.Backend, suffix)
	}
	return nil
}
