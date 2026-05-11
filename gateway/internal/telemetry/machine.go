package telemetry

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var (
	machineID     string
	machineIDOnce sync.Once
)

type machineIDFile struct {
	ID string `json:"machine_id"`
}

// GetMachineID returns a stable anonymous identifier for this machine,
// created on first call and persisted to ~/.indexqube/telemetry.json.
func GetMachineID() string {
	machineIDOnce.Do(func() {
		machineID = loadOrCreateMachineID()
	})
	return machineID
}

func loadOrCreateMachineID() string {
	homeDir, err := os.UserHomeDir()
	if err != nil || strings.TrimSpace(homeDir) == "" {
		return generateEphemeralMachineID()
	}

	dir := filepath.Join(homeDir, ".indexqube")
	path := filepath.Join(dir, "telemetry.json")

	if data, err := os.ReadFile(path); err == nil {
		var f machineIDFile
		if json.Unmarshal(data, &f) == nil && strings.TrimSpace(f.ID) != "" {
			return f.ID
		}
	}

	id := generateEphemeralMachineID()

	if err := os.MkdirAll(dir, 0700); err != nil {
		return id
	}

	data, err := json.Marshal(machineIDFile{ID: id})
	if err != nil {
		return id
	}

	if err := os.WriteFile(path, data, 0600); err != nil {
		return id
	}

	return id
}

func generateEphemeralMachineID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "unknown"
	}
	return hex.EncodeToString(b)
}
