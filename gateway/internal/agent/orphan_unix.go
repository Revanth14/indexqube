//go:build unix

package agent

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// TerminateRecordedProcess kills a recorded process group only after proving
// that the live process still carries IndexQube's high-entropy environment
// token. A reused PID therefore cannot target an unrelated process.
func TerminateRecordedProcess(process ProcessInfo, grace time.Duration) (bool, error) {
	match, running, err := recordedProcessMatches(process)
	if err != nil || !running || !match {
		return false, err
	}
	if err := syscall.Kill(-process.PID, syscall.SIGINT); err != nil && !errors.Is(err, syscall.ESRCH) {
		return false, err
	}
	if waitForPIDExit(process.PID, grace) {
		return true, nil
	}
	if err := syscall.Kill(-process.PID, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return false, err
	}
	if waitForPIDExit(process.PID, grace) {
		return true, nil
	}
	if err := syscall.Kill(-process.PID, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return false, err
	}
	return true, nil
}

func recordedProcessMatches(process ProcessInfo) (match, running bool, err error) {
	if process.PID <= 0 || strings.TrimSpace(process.Token) == "" {
		return false, false, fmt.Errorf("invalid recorded process identity")
	}
	if err := syscall.Kill(process.PID, syscall.Signal(0)); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return false, false, nil
		}
		return false, false, err
	}
	needle := []byte("INDEXQUBE_PROCESS_TOKEN=" + process.Token)
	if runtime.GOOS == "linux" {
		raw, err := os.ReadFile("/proc/" + strconv.Itoa(process.PID) + "/environ")
		if err != nil {
			return false, true, err
		}
		for _, value := range bytes.Split(raw, []byte{0}) {
			if bytes.Equal(value, needle) {
				return true, true, nil
			}
		}
		return false, true, nil
	}
	raw, err := exec.Command("ps", "eww", "-p", strconv.Itoa(process.PID), "-o", "command=").Output() //nolint:gosec
	if err != nil {
		return false, true, err
	}
	return bytes.Contains(raw, needle), true, nil
}

func waitForPIDExit(pid int, grace time.Duration) bool {
	deadline := time.Now().Add(grace)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(pid, syscall.Signal(0)); errors.Is(err, syscall.ESRCH) {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}
