package telemetry

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestTelemetryRequiresAffirmativeOptIn(t *testing.T) {
	for _, setting := range []string{"", "off", "false", "0", "surprise"} {
		t.Run("off_"+setting, func(t *testing.T) {
			t.Setenv("IQ_TELEMETRY", setting)
			if Enabled() {
				t.Fatalf("setting %q enabled telemetry", setting)
			}
		})
	}
	for _, setting := range []string{"on", "true", "1", "yes", "enabled"} {
		t.Run("on_"+setting, func(t *testing.T) {
			t.Setenv("IQ_TELEMETRY", setting)
			if !Enabled() {
				t.Fatalf("setting %q did not enable telemetry", setting)
			}
		})
	}
}

func TestGatewayReliabilityEventIsAggregateOnlyAndOptIn(t *testing.T) {
	requests := make(chan []byte, 1)
	client := NewGatewayClient("https://telemetry.example")
	client.httpClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodPost || request.URL.String() != "https://telemetry.example/v1/reliability" {
			t.Fatalf("request=%s %s", request.Method, request.URL)
		}
		raw, _ := io.ReadAll(request.Body)
		requests <- raw
		return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(strings.NewReader(""))}, nil
	})}
	event := ReliabilityEvent{MachineID: "anonymous", IQVersion: "v1", OSArch: "test/test", GeneratedAt: time.Now().UTC(), TasksTotal: 3}
	t.Setenv("IQ_TELEMETRY", "off")
	client.TrackReliability(event)
	select {
	case <-requests:
		t.Fatal("disabled telemetry sent a request")
	case <-time.After(30 * time.Millisecond):
	}
	t.Setenv("IQ_TELEMETRY", "on")
	client.TrackReliability(event)
	select {
	case raw := <-requests:
		var object map[string]any
		if err := json.Unmarshal(raw, &object); err != nil {
			t.Fatal(err)
		}
		for _, forbidden := range []string{"prompt", "path", "command", "task_id", "workspace", "output", "provider_key"} {
			if _, exists := object[forbidden]; exists {
				t.Fatalf("reliability event exposed forbidden field %q", forbidden)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("enabled telemetry did not send a request")
	}
}
