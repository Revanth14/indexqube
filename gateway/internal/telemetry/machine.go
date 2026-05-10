package telemetry

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
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
	dir := filepath.Join(os.Getenv("HOME"), ".indexqube")
	path := filepath.Join(dir, "telemetry.json")

	if data, err := os.ReadFile(path); err == nil {
		var f machineIDFile
		if json.Unmarshal(data, &f) == nil && f.ID != "" {
			return f.ID
		}
	}

	b := make([]byte, 16)
	rand.Read(b) //nolint:errcheck
	id := hex.EncodeToString(b)

	os.MkdirAll(dir, 0700)  //nolint:errcheck
	data, _ := json.Marshal(machineIDFile{ID: id})
	os.WriteFile(path, data, 0600) //nolint:errcheck

	return id
}
