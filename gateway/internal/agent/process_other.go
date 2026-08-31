//go:build !unix

package agent

import (
	"os/exec"
	"time"
)

func platformConfigureProcess(_ *exec.Cmd) {}

func terminateProcess(cmd *exec.Cmd, _ time.Duration) {
	if cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
}
