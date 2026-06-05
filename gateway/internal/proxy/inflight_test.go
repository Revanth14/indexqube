package proxy

import (
	"testing"
	"time"
)

func TestInFlightTrackerAcquireAndRelease(t *testing.T) {
	tr := newInFlightTracker()

	done1, wait1 := tr.acquire("hash-a")
	if done1 == nil {
		t.Fatal("first arrival should get doneFn")
	}
	if wait1 != nil {
		t.Fatal("first arrival should not get waitChan")
	}

	// Duplicate should wait.
	done2, wait2 := tr.acquire("hash-a")
	if done2 != nil {
		t.Fatal("duplicate should not get doneFn")
	}
	if wait2 == nil {
		t.Fatal("duplicate should get waitChan")
	}

	// First caller releases.
	done1()

	select {
	case <-wait2:
		// expected
	case <-time.After(time.Second):
		t.Fatal("duplicate should have been notified")
	}

	// After release, a new acquire should succeed again.
	done3, wait3 := tr.acquire("hash-a")
	if done3 == nil {
		t.Fatal("post-release acquire should get doneFn")
	}
	if wait3 != nil {
		t.Fatal("post-release acquire should not get waitChan")
	}
	done3()
}

func TestInFlightTrackerDifferentHashes(t *testing.T) {
	tr := newInFlightTracker()

	done1, wait1 := tr.acquire("hash-a")
	if done1 == nil {
		t.Fatal("first arrival should get doneFn")
	}
	if wait1 != nil {
		t.Fatal("first arrival should not get waitChan")
	}

	// Different hash should acquire independently.
	done2, wait2 := tr.acquire("hash-b")
	if done2 == nil {
		t.Fatal("different hash should get doneFn")
	}
	if wait2 != nil {
		t.Fatal("different hash should not get waitChan")
	}

	done1()
	done2()
}

func TestInFlightTrackerCleanupOnPanic(t *testing.T) {
	tr := newInFlightTracker()

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic")
			}
		}()
		done1, _ := tr.acquire("panic-hash")
		defer done1()
		panic("boom")
	}()

	// After panic recovery, the entry should be cleaned up.
	done2, wait2 := tr.acquire("panic-hash")
	if done2 == nil {
		t.Fatal("post-panic acquire should get doneFn")
	}
	if wait2 != nil {
		t.Fatal("post-panic acquire should not get waitChan")
	}
	done2()
}
