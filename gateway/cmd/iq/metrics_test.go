package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestRunMetricsCommandRendersAggregateControlView(t *testing.T) {
	token := installControlTestCredential(t)
	oldClient := metricsHTTPClient
	t.Cleanup(func() { metricsHTTPClient = oldClient })
	metricsHTTPClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet || request.URL.Path != "/control/v1/metrics" {
			t.Fatalf("request=%s %s", request.Method, request.URL.Path)
		}
		if request.Header.Get("Authorization") != "Bearer "+token {
			t.Fatal("missing control credential")
		}
		body := `{"tasks_total":4,"turns_total":5,"turns_succeeded":3,"turns_failed":1,"turns_cancelled":1,"successful_latency":{"count":3,"average_ms":1500,"p50_ms":1000,"p95_ms":2500,"max_ms":2500},"handoffs":1,"automatic_fallbacks":2,"verification_outcomes":{"verified":2},"crash_recoveries":1,"crash_recoveries_needing_attention":1,"verified_without_manual_switch":2}`
		return &http.Response{StatusCode: http.StatusOK, Header: authenticatedControlHeader(), Body: io.NopCloser(strings.NewReader(body))}, nil
	})}
	t.Setenv("INDEXQUBE_CONTROL_URL", "http://127.0.0.1:17374")
	var out, errOut bytes.Buffer
	if err := runMetricsCommand(context.Background(), nil, &out, &errOut); err != nil {
		t.Fatalf("metrics: %v (%s)", err, errOut.String())
	}
	for _, want := range []string{"Tasks: 4", "p50 1s", "Handoffs: 1", "automatic fallbacks: 2", "needing attention: 1", "verified=2"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("missing %q:\n%s", want, out.String())
		}
	}
}
