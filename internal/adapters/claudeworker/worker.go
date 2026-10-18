package claudeworker

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"agent-observer/internal/workers"
)

// Worker owns one streaming subprocess and a narrowly scoped MCP endpoint.
// Run serializes inputs; Close interrupts the process without taking runMu.
type Worker struct {
	runMu     sync.Mutex
	ctx       context.Context
	cancel    context.CancelFunc
	input     io.WriteCloser
	frames    <-chan frameResult
	wait      <-chan struct{}
	closeOnce sync.Once
	bridge    *messageBridge
	dir       string
	sessionID string
}

// Start launches a fresh streaming process. Claude emits initialization after
// the first input, so Run validates its session and tool policy before acceptance.
func Start(ctx context.Context, cfg workers.RuntimeConfig, send workers.SendFunc) (*Worker, error) {
	return start(ctx, cfg, send, func(ctx context.Context, args []string) *exec.Cmd {
		return exec.CommandContext(ctx, cfg.Executable, args...)
	})
}

func start(ctx context.Context, cfg workers.RuntimeConfig, send workers.SendFunc, command func(context.Context, []string) *exec.Cmd) (*Worker, error) {
	if !filepath.IsAbs(cfg.Executable) || !filepath.IsAbs(cfg.CWD) || send == nil {
		return nil, errors.New("claude worker requires an absolute executable, working directory, and sender")
	}
	info, err := os.Stat(cfg.CWD)
	if err != nil || !info.IsDir() {
		return nil, errors.New("claude worker working directory is unavailable")
	}
	processCtx, cancel := context.WithCancel(ctx)
	bridge, err := newMessageBridge(processCtx, send)
	if err != nil {
		cancel()
		return nil, err
	}
	dir, args, err := prepareConfig(cfg, bridge)
	if err != nil {
		cancel()
		bridge.close()
		return nil, err
	}
	w := &Worker{ctx: processCtx, cancel: cancel, bridge: bridge, dir: dir}
	if err := w.launch(command(processCtx, args), cfg.CWD); err != nil {
		w.shutdown()
		return nil, err
	}
	go func() { <-processCtx.Done(); w.shutdown() }()
	return w, nil
}

func (w *Worker) launch(cmd *exec.Cmd, cwd string) error {
	cmd.Dir = cwd
	cmd.Stderr = io.Discard // Provider diagnostics can contain prompts or credentials.
	cmd.Cancel = func() error {
		err := cmd.Process.Signal(os.Interrupt)
		if err != nil && !errors.Is(err, os.ErrProcessDone) {
			return cmd.Process.Kill()
		}
		return err
	}
	cmd.WaitDelay = 2 * time.Second
	input, err := cmd.StdinPipe()
	if err != nil {
		return errors.New("open claude worker input")
	}
	w.input = input
	output, err := cmd.StdoutPipe()
	if err != nil {
		return errors.New("open claude worker output")
	}
	if err := cmd.Start(); err != nil {
		_ = output.Close()
		return errors.New("start claude worker process")
	}
	frames := make(chan frameResult, 64)
	wait := make(chan struct{})
	w.frames, w.wait = frames, wait
	go func() {
		stop := context.AfterFunc(w.ctx, func() { _ = output.Close() })
		defer stop()
		readFrames(w.ctx, output, frames)
		_ = cmd.Wait() // Drain StdoutPipe before Wait closes it.
		close(wait)
		// Run must drain buffered acknowledgments before observing EOF. Canceling
		// the lifetime here would discard an accepted identity on fast exit.
	}()
	return nil
}

func (w *Worker) shutdown() {
	w.closeOnce.Do(func() {
		w.cancel()
		if w.input != nil {
			_ = w.input.Close()
		}
		w.bridge.close()
		_ = os.RemoveAll(w.dir) // Only the private directory created by prepareConfig.
	})
}

// Close releases the private bridge/configuration and reaps the owned process.
func (w *Worker) Close() error {
	w.shutdown()
	select {
	case <-w.wait:
		return nil
	case <-time.After(5 * time.Second):
		return errors.New("claude worker shutdown is still pending")
	}
}

// Run waits for acknowledgment and completion of one serialized input. The
// execution receipt is our input UUID acknowledged by Claude, not a native turn ID.
func (w *Worker) Run(ctx context.Context, msg workers.Input) (receipt workers.Receipt, err error) {
	w.runMu.Lock()
	defer w.runMu.Unlock()
	receipt = workers.Receipt{ConversationID: w.sessionID}
	if msg.ID == "" || msg.Source == "" || msg.Body == "" {
		return receipt, errors.New("claude worker message requires identity, source, and body")
	}
	if err := ctx.Err(); err != nil {
		return receipt, err
	}
	if w.ctx.Err() != nil {
		return receipt, errors.New("claude worker is closed")
	}
	runCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(w.ctx, cancel)
	defer stop()
	defer cancel()
	shutdownDone := make(chan struct{})
	stopShutdown := context.AfterFunc(runCtx, func() {
		defer close(shutdownDone)
		w.shutdown()
	})
	defer func() {
		if !stopShutdown() {
			<-shutdownDone
			if err == nil {
				err = runCtx.Err()
			}
		}
	}()
	uuid, err := newUUID()
	if err != nil {
		return receipt, err
	}
	text := fmt.Sprintf("Observer message ID: %s\nFrom: %s\n\n%s", msg.ID, msg.Source, msg.Body)
	active := w.bridge.begin(runCtx)
	defer w.bridge.end(active)
	if err := w.write(runCtx, userInput(uuid, w.sessionID, text)); err != nil {
		_ = w.Close()
		return receipt, err
	}
	receipt, err = w.awaitResult(runCtx, msg, uuid, text, active)
	if runCtx.Err() != nil {
		err = runCtx.Err()
	}
	if err != nil {
		_ = w.Close()
	}
	return receipt, err
}

func newUUID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", errors.New("create claude worker input identity")
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", value[:4], value[4:6], value[6:8], value[8:10], value[10:]), nil
}
