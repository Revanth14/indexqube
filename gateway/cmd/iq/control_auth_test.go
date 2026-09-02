package main

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	controlapi "github.com/Revanth14/indexqube/gateway/internal/control"
)

func authenticatedControlHeader() http.Header {
	header := make(http.Header)
	header.Set(controlapi.AuthContractHeader, controlapi.AuthContractValue)
	return header
}

func installControlTestCredential(t *testing.T) string {
	t.Helper()
	t.Setenv("INDEXQUBE_HOME", filepath.Join(t.TempDir(), "state"))
	token, err := rotateControlCredential()
	if err != nil {
		t.Fatalf("rotateControlCredential: %v", err)
	}
	return token
}

func TestControlCredentialRotatesAndStaysOwnerOnly(t *testing.T) {
	first := installControlTestCredential(t)
	path, err := controlCredentialPath()
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("credential mode=%v, want regular 0600", info.Mode())
	}
	second, err := rotateControlCredential()
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("credential did not rotate")
	}
	loaded, err := readControlCredential()
	if err != nil {
		t.Fatal(err)
	}
	if loaded != second {
		t.Fatal("loaded credential is not the latest rotation")
	}
}

func TestReadControlCredentialRejectsUnsafePermissions(t *testing.T) {
	installControlTestCredential(t)
	path, _ := controlCredentialPath()
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readControlCredential(); err == nil || !strings.Contains(err.Error(), "unsafe permissions") {
		t.Fatalf("readControlCredential error=%v", err)
	}
}

func TestNewControlRequestInjectsCredentialOnlyForNumericLoopback(t *testing.T) {
	token := installControlTestCredential(t)
	req, err := newControlRequest(context.Background(), http.MethodGet, "http://127.0.0.1:17374/control/healthz", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := req.Header.Get("Authorization"); got != "Bearer "+token {
		t.Fatal("request did not receive the current bearer credential")
	}
	for _, rawURL := range []string{
		"http://localhost:17374/control/healthz",
		"http://192.0.2.1:17374/control/healthz",
		"https://127.0.0.1:17374/control/healthz",
		"http://user@127.0.0.1:17374/control/healthz",
	} {
		if _, err := newControlRequest(context.Background(), http.MethodGet, rawURL, nil); err == nil {
			t.Fatalf("newControlRequest(%q) succeeded", rawURL)
		}
	}
}

func TestVerifyControlResponseRejectsLegacyUnauthenticatedDaemon(t *testing.T) {
	if err := verifyControlResponse(&http.Response{Header: make(http.Header)}); err == nil || !strings.Contains(err.Error(), "restart") {
		t.Fatalf("verifyControlResponse error=%v", err)
	}
	if err := verifyControlResponse(&http.Response{Header: authenticatedControlHeader()}); err != nil {
		t.Fatal(err)
	}
}
