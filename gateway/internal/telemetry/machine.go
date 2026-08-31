package telemetry

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/Revanth14/indexqube/gateway/internal/localstate"
)

var (
	machineID     string
	machineIDOnce sync.Once
)

type machineIDFile struct {
	ID string `json:"machine_id"`
}

// GetMachineID returns a stable anonymous identifier for this machine,
// created on first call and persisted to INDEXQUBE_HOME/telemetry.json (or
// ~/.indexqube/telemetry.json when no override is configured).
func GetMachineID() string {
	machineIDOnce.Do(func() {
		machineID = loadOrCreateMachineID()
	})
	return machineID
}

func loadOrCreateMachineID() string {
	dir, err := localstate.Ensure()
	if err != nil || strings.TrimSpace(dir) == "" {
		return generateEphemeralMachineID()
	}

	path := filepath.Join(dir, "telemetry.json")

	if data, err := os.ReadFile(path); err == nil {
		var f machineIDFile
		if json.Unmarshal(data, &f) == nil && strings.TrimSpace(f.ID) != "" {
			return f.ID
		}
	}

	id := generateEphemeralMachineID()

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
