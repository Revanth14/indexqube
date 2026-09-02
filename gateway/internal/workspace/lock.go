package workspace

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

type EpochStore interface {
	BeginWriteEpoch(context.Context, string, string, string, string, time.Time) (uint64, error)
	ReleaseWriteEpoch(context.Context, string, uint64, time.Time) error
}

type LockManager struct {
	dir    string
	store  EpochStore
	owner  string
	mu     sync.Mutex
	active map[string]activeWriter
}

type activeWriter struct {
	taskID string
	turnID string
}

func NewLockManager(dir string, store EpochStore, owner string) (*LockManager, error) {
	if store == nil {
		return nil, fmt.Errorf("workspace: nil epoch store")
	}
	if owner == "" {
		return nil, fmt.Errorf("workspace: empty owner")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("workspace: create lock dir: %w", err)
	}
	return &LockManager{dir: dir, store: store, owner: owner, active: make(map[string]activeWriter)}, nil
}

type WriteGuard struct {
	manager     *LockManager
	workspaceID string
	epoch       uint64
	file        *os.File
	mu          sync.Mutex
	released    bool
}

func (m *LockManager) AcquireWrite(ctx context.Context, workspaceID, taskID, turnID string) (*WriteGuard, error) {
	m.mu.Lock()
	if holder, ok := m.active[workspaceID]; ok {
		m.mu.Unlock()
		return nil, &WorkspaceLockedError{WorkspaceID: workspaceID, TaskID: holder.taskID, TurnID: holder.turnID}
	}
	m.active[workspaceID] = activeWriter{taskID: taskID, turnID: turnID}
	m.mu.Unlock()
	failed := true
	defer func() {
		if failed {
			m.clearActive(workspaceID)
		}
	}()

	path := filepath.Join(m.dir, workspaceID+".lock")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("workspace: open lock: %w", err)
	}
	if err := platformTryLock(f); err != nil {
		f.Close()
		if err == ErrWorkspaceLocked {
			return nil, &WorkspaceLockedError{WorkspaceID: workspaceID}
		}
		return nil, err
	}
	epoch, err := m.store.BeginWriteEpoch(ctx, workspaceID, taskID, turnID, m.owner, time.Now().UTC())
	if err != nil {
		_ = platformUnlock(f)
		f.Close()
		return nil, fmt.Errorf("workspace: begin fencing epoch: %w", err)
	}
	failed = false
	return &WriteGuard{manager: m, workspaceID: workspaceID, epoch: epoch, file: f}, nil
}

func (g *WriteGuard) Epoch() uint64 { return g.epoch }

func (g *WriteGuard) PrepareCommand(cmd *exec.Cmd) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.released {
		return fmt.Errorf("workspace: write guard already released")
	}
	return platformPrepareCommand(g.file, cmd)
}

func (g *WriteGuard) Release(ctx context.Context) error {
	g.mu.Lock()
	if g.released {
		g.mu.Unlock()
		return nil
	}
	g.released = true
	g.mu.Unlock()

	epochErr := g.manager.store.ReleaseWriteEpoch(ctx, g.workspaceID, g.epoch, time.Now().UTC())
	unlockErr := platformUnlock(g.file)
	closeErr := g.file.Close()
	g.manager.clearActive(g.workspaceID)
	if epochErr != nil {
		return fmt.Errorf("workspace: release epoch: %w", epochErr)
	}
	if unlockErr != nil {
		return fmt.Errorf("workspace: unlock: %w", unlockErr)
	}
	if closeErr != nil {
		return fmt.Errorf("workspace: close lock: %w", closeErr)
	}
	return nil
}

func (m *LockManager) clearActive(workspaceID string) {
	m.mu.Lock()
	delete(m.active, workspaceID)
	m.mu.Unlock()
}
