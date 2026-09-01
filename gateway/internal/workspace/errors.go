package workspace

import "errors"

var ErrWorkspaceLocked = errors.New("workspace is already held by another writer")
