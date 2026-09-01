//go:build unix

package agent

import (
	"os"
	"os/exec"
	"syscall"
	"time"
)

func platformConfigureProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func terminateProcess(cmd *exec.Cmd, grace time.Duration) {
	if cmd.Process == nil {
		return
	}
	pid := cmd.Process.Pid
	_ = syscall.Kill(-pid, syscall.SIGINT)
	if waitForProcessExit(cmd.Process, grace) {
		return
	}
	_ = syscall.Kill(-pid, syscall.SIGTERM)
	if waitForProcessExit(cmd.Process, grace) {
		return
	}
	_ = syscall.Kill(-pid, syscall.SIGKILL)
}

func waitForProcessExit(process *os.Process, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if err := process.Signal(syscall.Signal(0)); err != nil {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}
