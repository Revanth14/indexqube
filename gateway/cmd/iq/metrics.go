package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/taskstore"
)

var metricsHTTPClient = &http.Client{Timeout: 10 * time.Second}

func runMetrics(args []string) {
	if err := runMetricsCommand(context.Background(), args, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "iq: metrics failed: %v\n", err)
		os.Exit(1)
	}
}

func runMetricsCommand(ctx context.Context, args []string, out, errOut io.Writer) error {
	fs := flag.NewFlagSet("metrics", flag.ContinueOnError)
	fs.SetOutput(errOut)
	jsonOutput := fs.Bool("json", false, "emit machine-readable JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("usage: iq metrics [--json]")
	}
	controlURL, err := resolveControlURL()
	if err != nil {
		return err
	}
	req, err := newControlRequest(ctx, http.MethodGet, controlURL+"/control/v1/metrics", nil)
	if err != nil {
		return err
	}
	resp, err := metricsHTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := verifyControlResponse(resp); err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return responseError("get reliability metrics", resp)
	}
	var metrics taskstore.ReliabilityMetrics
	if err := json.NewDecoder(resp.Body).Decode(&metrics); err != nil {
		return err
	}
	if *jsonOutput {
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(metrics)
	}
	fmt.Fprintln(out, "IndexQube reliability metrics (local aggregate)")
	fmt.Fprintf(out, "Tasks: %d | turns: %d succeeded, %d failed, %d cancelled (%d total)\n",
		metrics.TasksTotal, metrics.TurnsSucceeded, metrics.TurnsFailed, metrics.TurnsCancelled, metrics.TurnsTotal)
	fmt.Fprintf(out, "Successful latency: p50 %s | p95 %s | average %s | samples %d\n",
		formatMetricDuration(metrics.SuccessfulLatency.P50MS), formatMetricDuration(metrics.SuccessfulLatency.P95MS),
		formatMetricDuration(metrics.SuccessfulLatency.AverageMS), metrics.SuccessfulLatency.Count)
	fmt.Fprintf(out, "Handoffs: %d | automatic fallbacks: %d\n", metrics.Handoffs, metrics.AutomaticFallbacks)
	fmt.Fprintf(out, "Crash recoveries: %d | needing attention: %d\n", metrics.CrashRecoveries, metrics.CrashRecoveriesAttention)
	fmt.Fprintf(out, "Verified without manual switching: %d\n", metrics.VerifiedWithoutManualSwitch)
	keys := make([]string, 0, len(metrics.VerificationOutcomes))
	for key := range metrics.VerificationOutcomes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) != 0 {
		fmt.Fprint(out, "Verification:")
		for _, key := range keys {
			fmt.Fprintf(out, " %s=%d", key, metrics.VerificationOutcomes[key])
		}
		fmt.Fprintln(out)
	}
	return nil
}

func formatMetricDuration(milliseconds int64) string {
	return (time.Duration(milliseconds) * time.Millisecond).Round(time.Millisecond).String()
}
