// Package localstate resolves the directory used for IndexQube's local-only
// state. INDEXQUBE_HOME is the explicit override used by the CLI, daemon,
// tests, and isolated installations.
package localstate

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

// Dir returns the configured IndexQube state directory without creating it.
func Dir() (string, error) {
	if dir := strings.TrimSpace(os.Getenv("INDEXQUBE_HOME")); dir != "" {
		return dir, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(home) == "" {
		return "", errors.New("user home directory is empty")
	}
	return filepath.Join(home, ".indexqube"), nil
}

// Ensure returns Dir and creates it with owner-only permissions when needed.
func Ensure() (string, error) {
	dir, err := Dir()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	return dir, nil
}
