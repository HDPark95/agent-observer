package codexworker

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"agent-observer/internal/messaging"
	"agent-observer/internal/workers"
)

func TestDeadlineInterruptsUnreadTurnInput(t *testing.T) {
	w := fixtureWorker(t, "blocked-turn-input", rejectSend)
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	input := observeWrites(w, ctx)
	// '<' is a valid message rune that JSON expands to six bytes, exceeding
	// common stdin pipe capacities while staying within the public body limit.
	body := strings.Repeat("<", messaging.MaxBodyRunes)
	started := time.Now()
	receipt, err := boundedRun(t, w, ctx, workers.Input{ID: "input", Source: "ux", Body: body})
	if !errors.Is(err, context.DeadlineExceeded) || receipt.ExecutionID != "" {
		t.Fatalf("Run = %#v, %v; want deadline with unconfirmed turn", receipt, err)
	}
	input.assertBlocked(t)
	assertStoppedWorker(t, w, started)
}

func TestDeadlineInterruptsUnreadToolResponse(t *testing.T) {
	called := false
	w := fixtureWorker(t, "blocked-tool-response", func(context.Context, workers.Outbound) (workers.SendReceipt, error) {
		called = true
		return workers.SendReceipt{ID: "reply", Target: "dx", State: strings.Repeat("accepted ", 256<<10)}, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	input := observeWrites(w, ctx)
	started := time.Now()
	receipt, err := boundedRun(t, w, ctx, workers.Input{ID: "input", Source: "ux", Body: "Review."})
	if !called || !errors.Is(err, context.DeadlineExceeded) || receipt.ExecutionID != "turn-1" {
		t.Fatalf("Run = %#v, %v, send called = %v; want accepted turn and deadline", receipt, err, called)
	}
	input.assertBlocked(t)
	assertStoppedWorker(t, w, started)
}

func TestDeadlineInterruptsUnreadSetupInput(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cfg := workers.RuntimeConfig{Executable: executable, CWD: t.TempDir(), Instructions: strings.Repeat("read-only ", 256<<10)}
	w, err := launch(context.Background(), cfg, rejectSend, func(ctx context.Context) *exec.Cmd {
		return exec.CommandContext(ctx, executable, "-test.run=^TestProtocolHelper$", "--", "blocked-setup-input")
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	// The setup deadline is deliberately independent of the process lifetime,
	// just like Start's 30-second setup context and its parent process context.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	input := observeWrites(w, ctx)
	started := time.Now()
	done := make(chan error, 1)
	go func() { done <- w.initialize(ctx, cfg) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("initialize = %v; want deadline", err)
		}
	case <-time.After(3 * time.Second):
		_ = w.Close()
		<-done
		t.Fatal("setup write did not stop at its deadline")
	}
	input.assertBlocked(t)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	assertStoppedWorker(t, w, started)
}

func TestCompletedRequestDoesNotKeepCancellationCallback(t *testing.T) {
	w := fixtureWorker(t, "early-completion", rejectSend)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := w.Run(ctx, workers.Input{ID: "first", Source: "ux", Body: "Review."}); err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err := w.Run(context.Background(), workers.Input{ID: "second", Source: "ux", Body: "Review again."}); err != nil {
		t.Fatalf("completed request cancellation closed worker: %v", err)
	}
}

func assertStoppedWorker(t *testing.T, w *Worker, started time.Time) {
	t.Helper()
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Errorf("deadline took %v to interrupt blocked pipe", elapsed)
	}
	select {
	case <-w.wait:
	default:
		t.Error("worker process was not reaped before Run returned")
	}
	receipt, err := w.Run(context.Background(), workers.Input{ID: "later", Source: "ux", Body: "Must not run."})
	if err == nil || receipt.ExecutionID != "" {
		t.Errorf("closed worker accepted another turn: %#v, %v", receipt, err)
	}
}

func boundedRun(t *testing.T, w *Worker, ctx context.Context, message workers.Input) (workers.Receipt, error) {
	t.Helper()
	type result struct {
		receipt workers.Receipt
		err     error
	}
	done := make(chan result, 1)
	go func() {
		receipt, err := w.Run(ctx, message)
		done <- result{receipt, err}
	}()
	select {
	case result := <-done:
		return result.receipt, result.err
	case <-time.After(3 * time.Second):
		_ = w.Close()
		<-done
		t.Fatal("Run did not stop at its deadline")
		return workers.Receipt{}, nil
	}
}

type observedInput struct {
	io.WriteCloser
	ctx       context.Context
	blocked   atomic.Bool
	premature atomic.Bool
}

func observeWrites(w *Worker, ctx context.Context) *observedInput {
	input := &observedInput{WriteCloser: w.input, ctx: ctx}
	w.input = input
	return input
}

func (input *observedInput) Write(data []byte) (int, error) {
	n, err := input.WriteCloser.Write(data)
	if len(data) > 64<<10 {
		if input.ctx.Err() == nil {
			input.premature.Store(true)
		} else {
			input.blocked.Store(true)
		}
	}
	return n, err
}

func (input *observedInput) assertBlocked(t *testing.T) {
	t.Helper()
	if !input.blocked.Load() || input.premature.Load() {
		t.Fatal("fixture did not exercise a large protocol write blocked until cancellation")
	}
}
