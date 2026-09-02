package agent

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"
)

func TestRunnerProcessHelper(t *testing.T) {
	mode := os.Getenv("INDEXQUBE_RUNNER_HELPER")
	if mode == "" {
		return
	}
	if mode == "event" {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]string{"type": "completed"})
		os.Exit(0)
	}
	if mode == "batch" {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]string{"type": "batch"})
		os.Exit(0)
	}
	if mode == "sleep" {
		time.Sleep(30 * time.Second)
		os.Exit(0)
	}
	if mode == "interactive-exit" {
		_, _ = os.Stdout.WriteString("{\"method\":\"approval\"}\n")
		os.Exit(0)
	}
}

func TestRunnerStreamsBatchDecodedEvents(t *testing.T) {
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	var got []EventType
	_, err = NewRunner().Run(context.Background(), ProcessSpec{
		Path: binary, Args: []string{"-test.run=TestRunnerProcessHelper"}, Env: []string{"INDEXQUBE_RUNNER_HELPER=batch"},
	}, nil, EventBatchDecoderFunc(func([]byte) ([]Event, error) {
		return []Event{{Type: EventToolStarted}, {Type: EventToolFinished}}, nil
	}), EventSinkFunc(func(_ context.Context, event Event) error {
		got = append(got, event.Type)
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != EventToolStarted || got[1] != EventToolFinished {
		t.Fatalf("events=%v", got)
	}
}

func TestInteractiveRunnerWakesBlockedHandlerWhenChildExits(t *testing.T) {
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	_, err = NewRunner().RunInteractive(context.Background(), ProcessSpec{
		Path: binary, Args: []string{"-test.run=TestRunnerProcessHelper"}, Env: []string{"INDEXQUBE_RUNNER_HELPER=interactive-exit"},
	}, nil, nil, func(ctx context.Context, _ []byte, _ func([]byte) error) (bool, error) {
		<-ctx.Done()
		return false, ctx.Err()
	})
	if err == nil {
		t.Fatal("expected interactive protocol error")
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("interactive child exit took %s", elapsed)
	}
}

func TestRunnerStreamsDecodedEvents(t *testing.T) {
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runner := NewRunner()
	var got int
	_, err = runner.Run(context.Background(), ProcessSpec{
		Path: binary, Args: []string{"-test.run=TestRunnerProcessHelper"}, Env: []string{"INDEXQUBE_RUNNER_HELPER=event"},
	}, nil, EventDecoderFunc(func(line []byte) (Event, bool, error) {
		return Event{Type: EventCompleted}, true, nil
	}), EventSinkFunc(func(context.Context, Event) error {
		got++
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if got != 1 {
		t.Fatalf("events=%d want 1", got)
	}
}

type recordingProcessObserver struct {
	mu      sync.Mutex
	started []ProcessInfo
	exited  []int
}

func (o *recordingProcessObserver) ProcessStarted(_ context.Context, process ProcessInfo) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.started = append(o.started, process)
	return nil
}

func (o *recordingProcessObserver) ProcessExited(_ context.Context, pid int) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.exited = append(o.exited, pid)
	return nil
}

func TestRunnerRegistersAndClearsSupervisedProcess(t *testing.T) {
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	observer := &recordingProcessObserver{}
	runner := NewRunner()
	runner.Observer = observer
	_, err = runner.Run(context.Background(), ProcessSpec{
		Path: binary, Args: []string{"-test.run=TestRunnerProcessHelper"}, Env: []string{"INDEXQUBE_RUNNER_HELPER=event"},
		TaskID: "task_observed", TurnID: "turn_observed",
	}, nil, EventDecoderFunc(func([]byte) (Event, bool, error) {
		return Event{Type: EventCompleted}, true, nil
	}), EventSinkFunc(func(context.Context, Event) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if len(observer.started) != 1 || len(observer.exited) != 1 {
		t.Fatalf("started=%+v exited=%v", observer.started, observer.exited)
	}
	process := observer.started[0]
	if process.PID <= 0 || process.Token == "" || process.TaskID != "task_observed" || process.TurnID != "turn_observed" || observer.exited[0] != process.PID {
		t.Fatalf("process=%+v exited=%v", process, observer.exited)
	}
}

func TestRunnerCancelsProcessGroup(t *testing.T) {
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runner := NewRunner()
	runner.CancelGrace = 50 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err = runner.Run(ctx, ProcessSpec{
		Path: binary, Args: []string{"-test.run=TestRunnerProcessHelper"}, Env: []string{"INDEXQUBE_RUNNER_HELPER=sleep"},
	}, nil, EventDecoderFunc(func([]byte) (Event, bool, error) { return Event{}, false, nil }), EventSinkFunc(func(context.Context, Event) error { return nil }))
	if err == nil {
		t.Fatal("expected cancellation error")
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("cancellation took %s", elapsed)
	}
}
