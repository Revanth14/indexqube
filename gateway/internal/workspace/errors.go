package workspace

import (
	"errors"
	"fmt"
)

var ErrWorkspaceLocked = errors.New("workspace is already held by another writer")

// WorkspaceLockedError identifies the in-process writer that owns a canonical
// Git workspace. Holder IDs are empty when the OS lock is owned by another
// daemon and its durable identity cannot be trusted after a crash.
type WorkspaceLockedError struct {
	WorkspaceID string
	TaskID      string
	TurnID      string
}

func (e *WorkspaceLockedError) Error() string {
	if e.TaskID != "" {
		return fmt.Sprintf("workspace %s is already held by task %s (turn %s)", e.WorkspaceID, e.TaskID, e.TurnID)
	}
	return fmt.Sprintf("workspace %s is already held by another daemon or inherited agent process", e.WorkspaceID)
}

func (e *WorkspaceLockedError) Unwrap() error { return ErrWorkspaceLocked }
