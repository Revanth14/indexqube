package agent

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"
)

const maxAgentEventBytes = 1 << 20
const maxAgentStderrBytes = 64 << 10

type ProcessSpec struct {
	Path  string
	Args  []string
	Dir   string
	Env   []string
	Stdin []byte
}

type EventDecoder interface {
	Decode([]byte) (Event, bool, error)
}

type EventDecoderFunc func([]byte) (Event, bool, error)

func (f EventDecoderFunc) Decode(line []byte) (Event, bool, error) { return f(line) }

// InteractiveLineHandler processes one JSONL message from a long-lived child.
// send is safe to call while handling the message. Returning done stops the
// supervised process after the protocol has emitted its terminal notification.
type InteractiveLineHandler func(ctx context.Context, line []byte, send func([]byte) error) (done bool, err error)

type ProcessResult struct {
	ExitCode int
	Stderr   string
}

type ProcessError struct {
	ExitCode int
	Stderr   string
}

func (e *ProcessError) Error() string {
	if e.Stderr == "" {
		return fmt.Sprintf("agent process exited with code %d", e.ExitCode)
	}
	return fmt.Sprintf("agent process exited with code %d: %s", e.ExitCode, e.Stderr)
}

type Runner struct {
	CancelGrace time.Duration
}

func NewRunner() *Runner {
	return &Runner{CancelGrace: 2 * time.Second}
}

func (r *Runner) Run(ctx context.Context, spec ProcessSpec, guard ProcessGuard, decoder EventDecoder, sink EventSink) (ProcessResult, error) {
	if spec.Path == "" {
		return ProcessResult{}, fmt.Errorf("agent: empty process path")
	}
	if decoder == nil || sink == nil {
		return ProcessResult{}, fmt.Errorf("agent: decoder and sink are required")
	}
	cmd := exec.Command(spec.Path, spec.Args...)
	cmd.Dir = spec.Dir
	cmd.Env = append(os.Environ(), spec.Env...)
	cmd.Stdin = bytes.NewReader(spec.Stdin)
	platformConfigureProcess(cmd)
	if guard != nil {
		if err := guard.PrepareCommand(cmd); err != nil {
			return ProcessResult{}, err
		}
	}
	stdout, stdoutWriter, err := os.Pipe()
	if err != nil {
		return ProcessResult{}, fmt.Errorf("agent: create stdout pipe: %w", err)
	}
	cmd.Stdout = stdoutWriter
	stderr := &boundedBuffer{max: maxAgentStderrBytes}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		stdout.Close()
		stdoutWriter.Close()
		return ProcessResult{}, fmt.Errorf("agent: start: %w", err)
	}
	// The child owns its duplicate of the write descriptor. Closing the parent
	// copy means the reader receives EOF exactly when the child exits; unlike
	// Cmd.StdoutPipe, Cmd.Wait cannot close the reader before buffered JSONL is
	// drained.
	_ = stdoutWriter.Close()

	decodeErr := make(chan error, 1)
	go func() {
		defer stdout.Close()
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 64<<10), maxAgentEventBytes)
		for scanner.Scan() {
			event, ok, err := decoder.Decode(append([]byte(nil), scanner.Bytes()...))
			if err != nil {
				decodeErr <- err
				return
			}
			if ok {
				if err := sink.Publish(ctx, event); err != nil {
					decodeErr <- err
					return
				}
			}
		}
		decodeErr <- scanner.Err()
	}()

	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }()

	var processErr error
	select {
	case <-ctx.Done():
		terminateProcess(cmd, r.cancelGrace())
		processErr = <-waitErr
		<-decodeErr
		return ProcessResult{ExitCode: exitCode(processErr), Stderr: stderr.String()}, ctx.Err()
	case err := <-decodeErr:
		if err != nil {
			terminateProcess(cmd, r.cancelGrace())
			<-waitErr
			return ProcessResult{ExitCode: -1, Stderr: stderr.String()}, fmt.Errorf("agent: decode events: %w", err)
		}
		processErr = <-waitErr
	case processErr = <-waitErr:
		decode := <-decodeErr
		if decode != nil {
			return ProcessResult{ExitCode: exitCode(processErr), Stderr: stderr.String()}, fmt.Errorf("agent: decode events: %w", decode)
		}
	}

	result := ProcessResult{ExitCode: exitCode(processErr), Stderr: stderr.String()}
	if processErr != nil {
		var exitErr *exec.ExitError
		if errors.As(processErr, &exitErr) {
			return result, &ProcessError{ExitCode: result.ExitCode, Stderr: result.Stderr}
		}
		return result, processErr
	}
	return result, nil
}

// RunInteractive supervises a bidirectional JSONL child such as Codex App
// Server. It retains the same process-group cancellation, bounded stderr, and
// inherited workspace guard semantics as Run.
func (r *Runner) RunInteractive(ctx context.Context, spec ProcessSpec, guard ProcessGuard, initialLines [][]byte, handler InteractiveLineHandler) (ProcessResult, error) {
	if spec.Path == "" {
		return ProcessResult{}, fmt.Errorf("agent: empty process path")
	}
	if handler == nil {
		return ProcessResult{}, fmt.Errorf("agent: interactive line handler is required")
	}
	handlerCtx, cancelHandler := context.WithCancel(ctx)
	defer cancelHandler()
	cmd := exec.Command(spec.Path, spec.Args...)
	cmd.Dir = spec.Dir
	cmd.Env = append(os.Environ(), spec.Env...)
	platformConfigureProcess(cmd)
	if guard != nil {
		if err := guard.PrepareCommand(cmd); err != nil {
			return ProcessResult{}, err
		}
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return ProcessResult{}, fmt.Errorf("agent: create stdin pipe: %w", err)
	}
	stdout, stdoutWriter, err := os.Pipe()
	if err != nil {
		stdin.Close()
		return ProcessResult{}, fmt.Errorf("agent: create stdout pipe: %w", err)
	}
	cmd.Stdout = stdoutWriter
	stderr := &boundedBuffer{max: maxAgentStderrBytes}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		stdin.Close()
		stdout.Close()
		stdoutWriter.Close()
		return ProcessResult{}, fmt.Errorf("agent: start: %w", err)
	}
	_ = stdoutWriter.Close()

	var writeMu sync.Mutex
	send := func(line []byte) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		if len(line) == 0 {
			return fmt.Errorf("agent: refusing empty interactive message")
		}
		if _, err := stdin.Write(append(append([]byte(nil), line...), '\n')); err != nil {
			return fmt.Errorf("agent: write interactive message: %w", err)
		}
		return nil
	}
	for _, line := range initialLines {
		if err := send(line); err != nil {
			terminateProcess(cmd, r.cancelGrace())
			_ = cmd.Wait()
			stdin.Close()
			stdout.Close()
			return ProcessResult{ExitCode: -1, Stderr: stderr.String()}, err
		}
	}

	type scanResult struct {
		done bool
		err  error
	}
	scanDone := make(chan scanResult, 1)
	go func() {
		defer stdout.Close()
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 64<<10), maxAgentEventBytes)
		for scanner.Scan() {
			done, err := handler(handlerCtx, append([]byte(nil), scanner.Bytes()...), send)
			if err != nil || done {
				scanDone <- scanResult{done: done, err: err}
				return
			}
		}
		scanDone <- scanResult{err: scanner.Err()}
	}()
	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }()

	finish := func(processErr error) (ProcessResult, error) {
		result := ProcessResult{ExitCode: exitCode(processErr), Stderr: stderr.String()}
		if processErr == nil {
			return result, nil
		}
		var exitErr *exec.ExitError
		if errors.As(processErr, &exitErr) {
			return result, &ProcessError{ExitCode: result.ExitCode, Stderr: result.Stderr}
		}
		return result, processErr
	}

	select {
	case <-ctx.Done():
		cancelHandler()
		terminateProcess(cmd, r.cancelGrace())
		processErr := <-waitErr
		<-scanDone
		stdin.Close()
		return ProcessResult{ExitCode: exitCode(processErr), Stderr: stderr.String()}, ctx.Err()
	case scanned := <-scanDone:
		if scanned.done {
			terminateProcess(cmd, r.cancelGrace())
			<-waitErr
			stdin.Close()
			return ProcessResult{ExitCode: 0, Stderr: stderr.String()}, scanned.err
		}
		if scanned.err != nil {
			terminateProcess(cmd, r.cancelGrace())
			<-waitErr
			stdin.Close()
			return ProcessResult{ExitCode: -1, Stderr: stderr.String()}, fmt.Errorf("agent: interactive protocol: %w", scanned.err)
		}
		processErr := <-waitErr
		stdin.Close()
		return finish(processErr)
	case processErr := <-waitErr:
		// If the child dies while a line handler is blocked on a user
		// approval, wake it before waiting for the scanner goroutine.
		cancelHandler()
		scanned := <-scanDone
		stdin.Close()
		if scanned.err != nil {
			return ProcessResult{ExitCode: exitCode(processErr), Stderr: stderr.String()}, fmt.Errorf("agent: interactive protocol: %w", scanned.err)
		}
		if scanned.done {
			return ProcessResult{ExitCode: 0, Stderr: stderr.String()}, nil
		}
		return finish(processErr)
	}
}

func (r *Runner) cancelGrace() time.Duration {
	if r.CancelGrace <= 0 {
		return 2 * time.Second
	}
	return r.CancelGrace
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return -1
}

type boundedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
	max int
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	want := len(p)
	remaining := b.max - b.buf.Len()
	if remaining > 0 {
		if len(p) > remaining {
			p = p[:remaining]
		}
		_, _ = b.buf.Write(p)
	}
	return want, nil
}

func (b *boundedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

var _ io.Writer = (*boundedBuffer)(nil)
