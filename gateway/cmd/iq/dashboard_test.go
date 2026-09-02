package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestRunDashboardCreatesWorkspaceScopedTicketWithoutExposingBearer(t *testing.T) {
	token := installControlTestCredential(t)
	oldClient, oldOpener := dashboardHTTPClient, dashboardOpener
	opened := ""
	dashboardOpener = func(rawURL string) error { opened = rawURL; return nil }
	dashboardHTTPClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodPost || request.URL.Path != "/control/v1/dashboard-sessions" || request.Header.Get("Authorization") != "Bearer "+token {
			t.Fatalf("request=%s %s auth=%q", request.Method, request.URL.Path, request.Header.Get("Authorization"))
		}
		var body map[string]string
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil || body["workspace"] == "" {
			t.Fatalf("body=%v err=%v", body, err)
		}
		payload := `{"url":"http://127.0.0.1:17374/control/ui/?ticket=one-time-ticket"}`
		return &http.Response{StatusCode: http.StatusCreated, Header: authenticatedControlHeader(), Body: io.NopCloser(strings.NewReader(payload)), Request: request}, nil
	})}
	t.Cleanup(func() { dashboardHTTPClient, dashboardOpener = oldClient, oldOpener })
	t.Setenv("INDEXQUBE_CONTROL_URL", "http://127.0.0.1:17374")
	var stdout, stderr bytes.Buffer
	if err := runDashboardCommand(context.Background(), []string{"--workspace", "."}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if opened == "" || strings.Contains(opened, token) || !strings.Contains(stdout.String(), "Opened") {
		t.Fatalf("opened=%q stdout=%q", opened, stdout.String())
	}

	opened = ""
	stdout.Reset()
	if err := runDashboardCommand(context.Background(), []string{"--no-open"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if opened != "" || !strings.Contains(stdout.String(), "one-time-ticket") || strings.Contains(stdout.String(), token) {
		t.Fatalf("opened=%q stdout=%q", opened, stdout.String())
	}
}

func TestValidateDashboardURLRejectsWrongOriginAndCredentialShapes(t *testing.T) {
	controlURL := "http://127.0.0.1:17374"
	if err := validateDashboardURL(controlURL, controlURL+"/control/ui/?ticket=t"); err != nil {
		t.Fatal(err)
	}
	for _, rawURL := range []string{
		"http://127.0.0.1:9999/control/ui/?ticket=t",
		controlURL + "/control/ui/",
		controlURL + "/control/ui/?ticket=t&token=secret",
		controlURL + "/control/ui/?ticket=t#fragment",
	} {
		if err := validateDashboardURL(controlURL, rawURL); err == nil {
			t.Fatalf("accepted %q", rawURL)
		}
	}
}
