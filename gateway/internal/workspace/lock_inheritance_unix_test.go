//go:build unix

package workspace

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/taskstore"
)

func TestInheritedLockHelper(t *testing.T) {
	if os.Getenv("INDEXQUBE_LOCK_HELPER") != "1" {
		return
	}
	fd, err := strconv.Atoi(os.Getenv("INDEXQUBE_WORKSPACE_LOCK_FD"))
	if err != nil {
		os.Exit(2)
	}
	file := os.NewFile(uintptr(fd), "workspace-lock")
	if file == nil {
		os.Exit(3)
	}
	if _, err := file.Stat(); err != nil {
		os.Exit(4)
	}
	fmt.Println("ready")
	for {
		time.Sleep(time.Second)
	}
}

func TestChildKeepsWriteLockAfterParentDescriptorCloses(t *testing.T) {
	state := t.TempDir()
	store, err := taskstore.Open(filepath.Join(state, "tasks.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	lockDir := filepath.Join(state, "locks")
	manager, err := NewLockManager(lockDir, store, "daemon-a")
	if err != nil {
		t.Fatal(err)
	}
	guard, err := manager.AcquireWrite(context.Background(), "ws_inherited", "task_1", "turn_1")
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "-test.run=TestInheritedLockHelper")
	cmd.Env = append(os.Environ(), "INDEXQUBE_LOCK_HELPER=1")
	if err := guard.PrepareCommand(cmd); err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	})
	ready := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		if scanner.Scan() {
			ready <- scanner.Text()
		}
	}()
	select {
	case line := <-ready:
		if line != "ready" {
			t.Fatalf("helper output=%q", line)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("helper did not inherit lock descriptor")
	}

	// This is the descriptor state left by an abrupt daemon death: the parent
	// copy closes without LOCK_UN, while the child still owns the inherited open
	// file description.
	if err := guard.file.Close(); err != nil {
		t.Fatal(err)
	}
	manager.clearActive("ws_inherited")

	other, err := NewLockManager(lockDir, store, "daemon-b")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.AcquireWrite(context.Background(), "ws_inherited", "task_2", "turn_2"); !errors.Is(err, ErrWorkspaceLocked) {
		t.Fatalf("second writer error=%v want ErrWorkspaceLocked", err)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("killed helper unexpectedly exited successfully")
	}
	cmd.Process = nil

	second, err := other.AcquireWrite(context.Background(), "ws_inherited", "task_2", "turn_2")
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release(context.Background())
	if second.Epoch() != 2 {
		t.Fatalf("new ownership epoch=%d want 2", second.Epoch())
	}
}
