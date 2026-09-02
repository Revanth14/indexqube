package workspace

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/Revanth14/indexqube/gateway/internal/taskstore"
)

func TestWriteLockExcludesSecondWriterAndIncrementsEpoch(t *testing.T) {
	store, err := taskstore.Open(filepath.Join(t.TempDir(), "tasks.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	manager, err := NewLockManager(filepath.Join(t.TempDir(), "locks"), store, "daemon-test")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	first, err := manager.AcquireWrite(ctx, "ws_1", "task_1", "turn_1")
	if err != nil {
		t.Fatal(err)
	}
	if first.Epoch() != 1 {
		t.Fatalf("first epoch=%d want 1", first.Epoch())
	}
	if _, err := manager.AcquireWrite(ctx, "ws_1", "task_2", "turn_2"); !errors.Is(err, ErrWorkspaceLocked) {
		t.Fatalf("second writer error=%v want ErrWorkspaceLocked", err)
	} else {
		var conflict *WorkspaceLockedError
		if !errors.As(err, &conflict) || conflict.TaskID != "task_1" || conflict.TurnID != "turn_1" {
			t.Fatalf("conflict=%+v error=%v", conflict, err)
		}
	}
	if err := first.Release(ctx); err != nil {
		t.Fatal(err)
	}
	second, err := manager.AcquireWrite(ctx, "ws_1", "task_2", "turn_2")
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release(ctx)
	if second.Epoch() != 2 {
		t.Fatalf("second epoch=%d want 2", second.Epoch())
	}
}
