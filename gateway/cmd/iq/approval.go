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
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/taskstore"
)

var approvalHTTPClient = &http.Client{Timeout: 10 * time.Second}

func runApprovals(args []string) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runApprovalsCommand(ctx, args, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "iq: approvals failed: %v\n", err)
		os.Exit(1)
	}
}

func runApprove(args []string, decision string) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runApprovalDecisionCommand(ctx, args, decision, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "iq: %s failed: %v\n", decision, err)
		os.Exit(1)
	}
}

func runApprovalsCommand(ctx context.Context, args []string, out, errOut io.Writer) error {
	fs := flag.NewFlagSet("approvals", flag.ContinueOnError)
	fs.SetOutput(errOut)
	taskID := fs.String("task", "", "only approvals for one task")
	all := fs.Bool("all", false, "include resolved approvals")
	limit := fs.Int("limit", 50, "maximum approvals to show")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("usage: iq approvals [--task TASK] [--all] [--limit N]")
	}
	controlURL, err := resolveControlURL()
	if err != nil {
		return err
	}
	query := url.Values{}
	query.Set("limit", fmt.Sprintf("%d", *limit))
	if strings.TrimSpace(*taskID) != "" {
		query.Set("task_id", strings.TrimSpace(*taskID))
	}
	if !*all {
		query.Set("status", string(taskstore.ApprovalPending))
	}
	req, err := newControlRequest(ctx, http.MethodGet, controlURL+"/control/v1/approvals?"+query.Encode(), nil)
	if err != nil {
		return err
	}
	resp, err := approvalHTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("list approvals: %w", err)
	}
	defer resp.Body.Close()
	if err := verifyControlResponse(resp); err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return responseError("list approvals", resp)
	}
	var result struct {
		Approvals []taskstore.Approval `json:"approvals"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode approvals: %w", err)
	}
	if len(result.Approvals) == 0 {
		fmt.Fprintln(out, "No approvals.")
		return nil
	}
	fmt.Fprintln(out, "APPROVAL\tSTATUS\tKIND\tBACKEND\tTASK\tREQUEST")
	for _, approval := range result.Approvals {
		fmt.Fprintf(out, "%s\t%s\t%s\t%s\t%s\t%s\n", approval.ID, approval.Status, approval.Kind,
			approval.Backend, approval.TaskID, approvalSummary(approval))
	}
	return nil
}

func runApprovalDecisionCommand(ctx context.Context, args []string, decision string, out io.Writer) error {
	if len(args) != 1 || strings.TrimSpace(args[0]) == "" {
		return fmt.Errorf("usage: iq %s APPROVAL", decision)
	}
	approvalID := strings.TrimSpace(args[0])
	controlURL, err := resolveControlURL()
	if err != nil {
		return err
	}
	body, err := json.Marshal(map[string]string{"decision": decision})
	if err != nil {
		return err
	}
	req, err := newControlRequest(ctx, http.MethodPost,
		controlURL+"/control/v1/approvals/"+approvalID+"/decision", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := approvalHTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("decide approval: %w", err)
	}
	defer resp.Body.Close()
	if err := verifyControlResponse(resp); err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return responseError("decide approval", resp)
	}
	var approval taskstore.Approval
	if err := json.NewDecoder(resp.Body).Decode(&approval); err != nil {
		return fmt.Errorf("decode approval: %w", err)
	}
	fmt.Fprintf(out, "Approval %s: %s\n", approval.ID, approval.Status)
	return nil
}

func approvalSummary(approval taskstore.Approval) string {
	if approval.NetworkHost != "" {
		value := approval.NetworkHost
		if approval.NetworkProtocol != "" {
			value = approval.NetworkProtocol + "://" + value
		}
		return oneLine("network access to "+value, 120)
	}
	if approval.Command != "" {
		return oneLine(approval.Command, 120)
	}
	if approval.GrantRoot != "" {
		return oneLine("write access under "+approval.GrantRoot, 120)
	}
	if approval.Reason != "" {
		return oneLine(approval.Reason, 120)
	}
	return string(approval.Kind)
}
