package proxy

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/Revanth14/indexqube/gateway/internal/domain"
)

const (
	headerProvider      = "X-IQ-Provider"
	headerKey           = "X-IQ-Provider-Key"
	headerProjectMemory = "X-IQ-Project-Memory"
	headerSessionKey    = "X-IQ-Session-Key"
	headerContextPath   = "X-IQ-Context-Path"
	headerContextLang   = "X-IQ-Context-Lang"
	headerAzureEndpoint = "X-IQ-Azure-Endpoint"
	headerAWSRegion     = "X-IQ-AWS-Region"
)

func validateSessionKey(sk string) error {
	if len(sk) == 0 {
		return nil // optional
	}
	if len(sk) > 256 {
		return fmt.Errorf("session key too long: %d bytes (max 256)", len(sk))
	}
	return nil
}

var (
	errMissingProvider = errors.New("missing X-IQ-Provider header")
	errUnknownProvider = errors.New("unknown provider in X-IQ-Provider header")
	errMissingKey      = errors.New("missing X-IQ-Provider-Key header")
	errBodyTooLarge    = errors.New("request body exceeds size limit")
	errEmptyMessages   = errors.New("messages array must not be empty")
	errMissingModel    = errors.New("model field is required")
)

// parseInferenceRequest extracts BYO-Key headers, body-decodes the canonical
// request, and runs minimal semantic validation. It does not contact the
// governor or any upstream.
func (p *Proxy) parseInferenceRequest(w http.ResponseWriter, r *http.Request) (*domain.InferenceRequest, error) {
	cred, err := extractCredential(r)
	if err != nil {
		return nil, err
	}

	r.Body = http.MaxBytesReader(w, r.Body, p.maxRequestSize)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	var req domain.InferenceRequest
	if err := dec.Decode(&req); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return nil, fmt.Errorf("%w: limit=%d", errBodyTooLarge, maxErr.Limit)
		}
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if strings.TrimSpace(req.Model) == "" {
		return nil, errMissingModel
	}
	if len(req.Messages) == 0 {
		return nil, errEmptyMessages
	}

	req.Credential = cred
	req.ProjectMemory = r.Header.Get(headerProjectMemory)
	req.SessionKey = r.Header.Get(headerSessionKey)
	if err := validateSessionKey(req.SessionKey); err != nil {
		return nil, err
	}
	req.AzureEndpoint = r.Header.Get(headerAzureEndpoint)
	req.AWSRegion = r.Header.Get(headerAWSRegion)
	return &req, nil
}

func extractCredential(r *http.Request) (domain.Credential, error) {
	raw := strings.ToLower(strings.TrimSpace(r.Header.Get(headerProvider)))
	if raw != "" {
		p := domain.Provider(raw)
		if !p.IsValid() {
			return domain.Credential{}, fmt.Errorf("%w: %q", errUnknownProvider, raw)
		}
		k := strings.TrimSpace(r.Header.Get(headerKey))
		if k == "" {
			return domain.Credential{}, errMissingKey
		}
		return domain.Credential{Provider: p, APIKey: k}, nil
	}

	if key := bearerToken(r.Header.Get("Authorization")); key != "" {
		return domain.Credential{Provider: domain.ProviderOpenAI, APIKey: key}, nil
	}
	if key := strings.TrimSpace(r.Header.Get("api-key")); key != "" {
		return domain.Credential{Provider: domain.ProviderAzure, APIKey: key}, nil
	}
	return domain.Credential{}, errMissingProvider
}

func bearerToken(auth string) string {
	auth = strings.TrimSpace(auth)
	if auth == "" {
		return ""
	}
	prefix := "bearer "
	if !strings.HasPrefix(strings.ToLower(auth), prefix) {
		return ""
	}
	return strings.TrimSpace(auth[len(prefix):])
}
