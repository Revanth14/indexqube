package cache

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/Revanth14/indexqube/gateway/internal/domain"
)

func TestLSMCache_GetMissOnEmpty(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "iq-lsm-cache-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	c, err := NewLSMCache(tmpDir, 1024)
	if err != nil {
		t.Fatalf("NewLSMCache failed: %v", err)
	}
	defer c.Close()

	_, hit, err := c.Get(context.Background(), "missing")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if hit {
		t.Error("expected miss on empty cache")
	}
}

func TestLSMCache_PutThenGet(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "iq-lsm-cache-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	c, err := NewLSMCache(tmpDir, 1024)
	if err != nil {
		t.Fatalf("NewLSMCache failed: %v", err)
	}
	defer c.Close()

	want := &Entry{
		Provider:  domain.ProviderAnthropic,
		Model:     "claude-3-5-sonnet",
		Chunks:    [][]byte{[]byte("first chunk"), []byte("second chunk")},
		CreatedAt: time.Now().UTC(),
	}

	if err := c.Put(context.Background(), "test-key", want); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	got, hit, err := c.Get(context.Background(), "test-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !hit {
		t.Fatal("expected cache hit")
	}

	if got.Provider != want.Provider {
		t.Errorf("provider mismatch: got %v, want %v", got.Provider, want.Provider)
	}
	if got.Model != want.Model {
		t.Errorf("model mismatch: got %s, want %s", got.Model, want.Model)
	}
	if len(got.Chunks) != len(want.Chunks) {
		t.Errorf("chunks len mismatch: got %d, want %d", len(got.Chunks), len(want.Chunks))
	}
	if string(got.Chunks[0]) != string(want.Chunks[0]) {
		t.Errorf("first chunk mismatch: got %s, want %s", got.Chunks[0], want.Chunks[0])
	}
}

func TestLSMCache_PutTooLargeRejected(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "iq-lsm-cache-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	c, err := NewLSMCache(tmpDir, 10) // extremely small max size
	if err != nil {
		t.Fatalf("NewLSMCache failed: %v", err)
	}
	defer c.Close()

	entry := &Entry{
		Provider:  domain.ProviderAnthropic,
		Model:     "claude-3-5-sonnet",
		Chunks:    [][]byte{[]byte("too long payload data")},
		CreatedAt: time.Now(),
	}

	err = c.Put(context.Background(), "too-large", entry)
	if !errors.Is(err, ErrEntryTooLarge) {
		t.Errorf("expected ErrEntryTooLarge, got: %v", err)
	}
}

func TestLSMCache_PersistenceAcrossCloses(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "iq-lsm-cache-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Phase 1: Open, write entry, then close
	c1, err := NewLSMCache(tmpDir, 1024)
	if err != nil {
		t.Fatalf("c1 NewLSMCache failed: %v", err)
	}

	want := &Entry{
		Provider:  domain.ProviderAnthropic,
		Model:     "claude-3-5-sonnet",
		Chunks:    [][]byte{[]byte("durable persistent cache chunk data")},
		CreatedAt: time.Now().UTC(),
	}

	if err := c1.Put(context.Background(), "persistent-key", want); err != nil {
		t.Fatalf("c1 Put failed: %v", err)
	}
	c1.Close()

	// Phase 2: Re-open in the same directory, read entry, then close
	c2, err := NewLSMCache(tmpDir, 1024)
	if err != nil {
		t.Fatalf("c2 NewLSMCache failed: %v", err)
	}
	defer c2.Close()

	got, hit, err := c2.Get(context.Background(), "persistent-key")
	if err != nil {
		t.Fatalf("c2 Get failed: %v", err)
	}
	if !hit {
		t.Fatal("expected hit from persistent L2 cache")
	}

	if string(got.Chunks[0]) != string(want.Chunks[0]) {
		t.Errorf("chunk data corrupted: got %s, want %s", got.Chunks[0], want.Chunks[0])
	}
}
