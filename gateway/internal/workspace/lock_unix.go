//go:build unix

package workspace

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
)

func platformTryLock(file *os.File) error {
	err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
		return ErrWorkspaceLocked
	}
	if err != nil {
		return fmt.Errorf("workspace: flock: %w", err)
	}
	return nil
}

func platformUnlock(file *os.File) error {
	return syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
}

func platformPrepareCommand(file *os.File, cmd *exec.Cmd) error {
	cmd.ExtraFiles = append(cmd.ExtraFiles, file)
	fd := 3 + len(cmd.ExtraFiles) - 1
	cmd.Env = append(cmd.Env, fmt.Sprintf("INDEXQUBE_WORKSPACE_LOCK_FD=%d", fd))
	return nil
}
