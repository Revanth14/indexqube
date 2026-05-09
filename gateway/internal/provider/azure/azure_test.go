package azure

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Revanth14/indexqube/gateway/internal/domain"
)

type recordingWriter struct {
	frames [][]byte
}

func (r *recordingWriter) WriteData(data []byte) error {
	cp := make([]byte, len(data))
	copy(cp, data)
	r.frames = append(r.frames, cp)
	return nil
}
func (r *recordingWriter) WriteEvent(_ string, _ []byte) error { return nil }
func (r *recordingWriter) WriteDone() error                    { return nil }
func (r *recordingWriter) Flush() error                        { return nil }

func TestValidateAzureEndpoint(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		endpoint string
		wantErr  bool
	}{
		{"https://myresource.openai.azure.com", false},
		{"https://contoso.cognitiveservices.azure.com", false},
		{"http://myresource.openai.azure.com", true},  // no https
		{"", true},                                     // empty
		{"ftp://example.com", true},                   // wrong scheme
		{"https://", true},                             // missing host
		{"https://10.0.0.1", true},                    // private IP
		{"https://192.168.1.1", true},                 // private IP
		{"https://172.16.0.1", true},                  // private IP
		{"https://169.254.169.254", true},             // link-local (AWS metadata)
	} {
		err := validateAzureEndpoint(tc.endpoint)
		if tc.wantErr && err == nil {
			t.Errorf("validateAzureEndpoint(%q): expected error, got nil", tc.endpoint)
		}
		if !tc.wantErr && err != nil {
			t.Errorf("validateAzureEndpoint(%q): unexpected error: %v", tc.endpoint, err)
		}
	}
}

func TestDispatch_HappyPath(t *testing.T) {
	t.Parallel()

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("api-key") != "test-key" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if !strings.Contains(r.URL.Path, "/deployments/gpt-4/chat/completions") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer srv.Close()

	a := New(WithHTTPClient(srv.Client()))
	rec := &recordingWriter{}
	req := &domain.InferenceRequest{
		Model:         "gpt-4",
		AzureEndpoint: srv.URL,
		Messages:      []domain.Message{{Role: "user", Content: "hi"}},
		Credential: domain.Credential{
			Provider: domain.ProviderAzure,
			APIKey:   "test-key",
		},
	}

	if err := a.Dispatch(context.Background(), req, rec); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	if len(rec.frames) != 1 {
		t.Errorf("got %d frames, want 1", len(rec.frames))
	}
	if !strings.Contains(string(rec.frames[0]), "hello") {
		t.Errorf("frame content mismatch: %s", string(rec.frames[0]))
	}
}
