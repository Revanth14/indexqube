package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	controlapi "github.com/Revanth14/indexqube/gateway/internal/control"
)

const (
	controlCredentialFile    = "control-auth.json"
	controlCredentialVersion = 1
)

type controlCredential struct {
	Version int    `json:"version"`
	Token   string `json:"token"`
}

func controlCredentialPath() (string, error) {
	home, err := indexQubeHome()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, controlCredentialFile), nil
}

// rotateControlCredential creates a credential scoped to one daemon lifetime.
// The token is persisted only in the owner-only state directory and is never
// passed through argv or the environment.
func rotateControlCredential() (string, error) {
	rawToken := make([]byte, 32)
	if _, err := rand.Read(rawToken); err != nil {
		return "", fmt.Errorf("generate control API credential: %w", err)
	}
	credential := controlCredential{
		Version: controlCredentialVersion,
		Token:   base64.RawURLEncoding.EncodeToString(rawToken),
	}
	raw, err := json.Marshal(credential)
	if err != nil {
		return "", err
	}
	raw = append(raw, '\n')
	path, err := controlCredentialPath()
	if err != nil {
		return "", err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".control-auth-*")
	if err != nil {
		return "", fmt.Errorf("create control API credential: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return "", err
	}
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		return "", err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return "", fmt.Errorf("install control API credential: %w", err)
	}
	return credential.Token, nil
}

func readControlCredential() (string, error) {
	path, err := controlCredentialPath()
	if err != nil {
		return "", err
	}
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", errors.New("control API credential is missing; restart the IndexQube daemon")
		}
		return "", err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return "", errors.New("control API credential has unsafe permissions; restart the IndexQube daemon")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	var credential controlCredential
	if err := json.Unmarshal(raw, &credential); err != nil {
		return "", errors.New("control API credential is invalid; restart the IndexQube daemon")
	}
	if credential.Version != controlCredentialVersion || strings.TrimSpace(credential.Token) == "" {
		return "", errors.New("control API credential is incompatible; restart the IndexQube daemon")
	}
	return credential.Token, nil
}

func validateControlURL(rawURL string) (string, error) {
	parsed, err := url.Parse(strings.TrimRight(strings.TrimSpace(rawURL), "/"))
	if err != nil {
		return "", fmt.Errorf("invalid control API URL: %w", err)
	}
	if parsed.Scheme != "http" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != "" {
		return "", errors.New("control API URL must be an HTTP origin on numeric loopback")
	}
	host := parsed.Hostname()
	if parsed.Port() == "" {
		return "", errors.New("control API URL must include a port")
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return "", errors.New("control API URL must use a numeric loopback address")
	}
	return strings.TrimRight(parsed.String(), "/"), nil
}

func newControlRequest(ctx context.Context, method, rawURL string, body io.Reader) (*http.Request, error) {
	if _, err := validateControlURL(controlOrigin(rawURL)); err != nil {
		return nil, err
	}
	token, err := readControlCredential()
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, rawURL, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	return req, nil
}

func verifyControlResponse(resp *http.Response) error {
	if resp.Header.Get(controlapi.AuthContractHeader) != controlapi.AuthContractValue {
		return errors.New("control API does not enforce the supported authentication contract; restart the IndexQube daemon")
	}
	return nil
}

func controlOrigin(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	return (&url.URL{Scheme: parsed.Scheme, User: parsed.User, Host: parsed.Host}).String()
}
