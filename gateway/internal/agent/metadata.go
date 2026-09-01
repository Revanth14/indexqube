package agent

import "strings"

const maxMetadataEntries = 16
const maxMetadataKeyBytes = 64
const maxMetadataValueBytes = 256

var allowedMetadata = map[string]bool{
	"native_session_id": true,
	"native_event_id":   true,
	"backend_version":   true,
	"model":             true,
	"write_epoch":       true,
	"error_code":        true,
}

func NormalizeMetadata(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string]string)
	for key, value := range input {
		if len(out) >= maxMetadataEntries || !allowedMetadata[key] {
			continue
		}
		key = boundText(strings.TrimSpace(key), maxMetadataKeyBytes)
		value = boundText(value, maxMetadataValueBytes)
		if key != "" {
			out[key] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func boundText(value string, max int) string {
	if len(value) <= max {
		return value
	}
	return value[:max]
}
