package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

var handoffHTTPClient = &http.Client{Timeout: 10 * time.Second}
var handoffEventStream = streamTaskEvents

func runHandoff(args []string) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runHandoffCommand(ctx, args, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "iq: handoff failed: %v\n", err)
		os.Exit(1)
	}
}

func runHandoffCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	taskID, destination, prompt, err := parseHandoffArgs(args)
	if err != nil {
		return err
	}
	controlURL, err := resolveControlURL()
	if err != nil {
		return err
	}
	body, err := json.Marshal(map[string]any{"to_backend": destination, "prompt": prompt})
	if err != nil {
		return err
	}
	req, err := newControlRequest(ctx, http.MethodPost, controlURL+"/control/v1/tasks/"+taskID+"/handoffs", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := handoffHTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("handoff task: %w", err)
	}
	defer resp.Body.Close()
	if err := verifyControlResponse(resp); err != nil {
		return err
	}
	if resp.StatusCode != http.StatusAccepted {
		return responseError("handoff task", resp)
	}
	var result struct {
		Task          taskstore.Task    `json:"task"`
		Handoff       taskstore.Handoff `json:"handoff"`
		AfterSequence int64             `json:"after_sequence"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode handoff: %w", err)
	}
	fmt.Fprintf(stderr, "  [iq] handoff %s: %s -> %s\n", result.Handoff.ID,
		result.Handoff.FromBackend, result.Handoff.ToBackend)
	if err := handoffEventStream(ctx, controlURL, result.Task.ID, result.AfterSequence, stdout, stderr); err != nil {
		if errors.Is(err, context.Canceled) {
			cancelTask(controlURL, result.Task.ID)
		}
		return err
	}
	return nil
}

func parseHandoffArgs(args []string) (string, agent.BackendID, string, error) {
	var destination string
	positionals := make([]string, 0, len(args))
	for index := 0; index < len(args); index++ {
		switch {
		case args[index] == "--to":
			if index+1 >= len(args) {
				return "", "", "", fmt.Errorf("--to requires a backend")
			}
			destination = strings.TrimSpace(args[index+1])
			index++
		case strings.HasPrefix(args[index], "--to="):
			destination = strings.TrimSpace(strings.TrimPrefix(args[index], "--to="))
		case strings.HasPrefix(args[index], "-"):
			return "", "", "", fmt.Errorf("unknown handoff flag %q", args[index])
		default:
			positionals = append(positionals, args[index])
		}
	}
	if len(positionals) == 0 || strings.TrimSpace(positionals[0]) == "" || destination == "" {
		return "", "", "", fmt.Errorf("usage: iq handoff TASK --to BACKEND [PROMPT]")
	}
	prompt := strings.TrimSpace(strings.Join(positionals[1:], " "))
	return strings.TrimSpace(positionals[0]), agent.BackendID(destination), prompt, nil
}
