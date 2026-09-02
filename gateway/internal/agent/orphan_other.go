//go:build !unix

package agent

import (
	"fmt"
	"time"
)

func TerminateRecordedProcess(ProcessInfo, time.Duration) (bool, error) {
	return false, fmt.Errorf("orphan process cleanup is unsupported on this platform")
}
