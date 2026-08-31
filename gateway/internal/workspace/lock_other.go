//go:build !unix

package workspace

import (
	"fmt"
	"os"
	"os/exec"
)

func platformTryLock(_ *os.File) error {
	return fmt.Errorf("workspace: OS locking is not implemented on this platform")
}

func platformUnlock(_ *os.File) error { return nil }

func platformPrepareCommand(_ *os.File, _ *exec.Cmd) error { return nil }
