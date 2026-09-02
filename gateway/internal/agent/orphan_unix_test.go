//go:build unix

package agent

import (
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestTerminateRecordedProcessRequiresExactEnvironmentToken(t *testing.T) {
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	token := "orphan-test-token-0123456789"
	cmd := exec.Command(binary, "-test.run=TestRunnerProcessHelper")
	cmd.Env = append(os.Environ(), "INDEXQUBE_RUNNER_HELPER=sleep", "INDEXQUBE_PROCESS_TOKEN="+token)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		select {
		case <-done:
		case <-time.After(time.Second):
		}
	})

	wrong := ProcessInfo{PID: cmd.Process.Pid, Token: token + "-wrong"}
	if terminated, err := TerminateRecordedProcess(wrong, 50*time.Millisecond); err != nil || terminated {
		if err != nil && strings.Contains(err.Error(), "operation not permitted") {
			t.Skip("process environment inspection is blocked by the test sandbox")
		}
		t.Fatalf("wrong token terminated=%v err=%v", terminated, err)
	}
	if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("process did not survive wrong token: %v", err)
	}
	correct := ProcessInfo{PID: cmd.Process.Pid, Token: token}
	if terminated, err := TerminateRecordedProcess(correct, 50*time.Millisecond); err != nil || !terminated {
		t.Fatalf("correct token terminated=%v err=%v", terminated, err)
	}
}
