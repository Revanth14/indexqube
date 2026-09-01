//go:build !unix

package verification

import "os/exec"

func prepareVerificationProcess(_ *exec.Cmd) {}
