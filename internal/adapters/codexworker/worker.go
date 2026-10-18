// Package codexworker runs ordinary, Observer-owned Codex app-server threads.
// It never resumes or injects input into a thread created by another host.
package codexworker

import (
	"context"
	"encoding/json"
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

// Worker owns one child process and one newly created thread. Run serializes
// turns; Close can interrupt an active turn and permanently closes the worker.
type Worker struct {
	mu        sync.Mutex
	ctx       context.Context
	cancel    context.CancelFunc
	input     io.WriteCloser
	frames    <-chan wireResult
	wait      <-chan struct{}
	closeOnce sync.Once
	nextID    int
	threadID  string
	send      workers.SendFunc
	pending   []wireMessage
}

// Start launches the supported stdio app-server protocol and creates a new
// read-only thread. It does not accept an existing thread identity.
func Start(ctx context.Context, cfg workers.RuntimeConfig, send workers.SendFunc) (*Worker, error) {
	return start(ctx, cfg, send, func(ctx context.Context) *exec.Cmd {
		return workerCommand(ctx, cfg.Executable)
	})
}

// Managed workers communicate through their identity-bound dynamic tool. Do not
// give children the coordinator's plugin controls or native delegation tools.
func workerCommand(ctx context.Context, executable string) *exec.Cmd {
	return exec.CommandContext(ctx, executable, "app-server", "--disable", "plugins", "--disable", "multi_agent")
}

func start(ctx context.Context, cfg workers.RuntimeConfig, send workers.SendFunc, command func(context.Context) *exec.Cmd) (*Worker, error) {
	if !filepath.IsAbs(cfg.Executable) || !filepath.IsAbs(cfg.CWD) || send == nil {
		return nil, errors.New("codex worker requires an absolute executable, working directory, and sender")
	}
	info, err := os.Stat(cfg.CWD)
	if err != nil || !info.IsDir() {
		return nil, errors.New("codex worker working directory is unavailable")
	}
	w, err := launch(ctx, cfg, send, command)
	if err != nil {
		return nil, err
	}
	setupCtx, setupCancel := context.WithTimeout(ctx, 30*time.Second)
	defer setupCancel()
	if err := w.initialize(setupCtx, cfg); err != nil {
		w.Close()
		return nil, err
	}
	return w, nil
}

func launch(ctx context.Context, cfg workers.RuntimeConfig, send workers.SendFunc, command func(context.Context) *exec.Cmd) (*Worker, error) {
	processCtx, cancel := context.WithCancel(ctx)
	cmd := command(processCtx)
	cmd.Dir = cfg.CWD
	cmd.Stderr = io.Discard // Transport diagnostics may contain user data.
	// Let app-server stop its owned tool/MCP children before WaitDelay falls
	// back to killing this process. Never signal an unrelated process group.
	cmd.Cancel = func() error {
		err := cmd.Process.Signal(os.Interrupt)
		if err != nil && !errors.Is(err, os.ErrProcessDone) {
			return cmd.Process.Kill() // Interrupt is unsupported on some platforms.
		}
		return err
	}
	cmd.WaitDelay = 2 * time.Second
	input, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return nil, errors.New("open codex worker input")
	}
	output, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		input.Close()
		return nil, errors.New("open codex worker output")
	}
	if err := cmd.Start(); err != nil {
		cancel()
		input.Close()
		output.Close()
		return nil, errors.New("start codex worker process")
	}
	frames := make(chan wireResult, 64)
	wait := make(chan struct{})
	w := &Worker{ctx: processCtx, cancel: cancel, input: input, frames: frames, wait: wait, send: send}
	go func() {
		stopClosing := context.AfterFunc(processCtx, func() { _ = output.Close() })
		defer stopClosing()
		readFrames(processCtx, output, frames)
		// StdoutPipe must be drained before Wait closes it.
		_ = cmd.Wait()
		close(wait)
	}()
	return w, nil
}

func (w *Worker) initialize(ctx context.Context, cfg workers.RuntimeConfig) error {
	_, err := w.request(ctx, "initialize", map[string]any{
		"clientInfo":   map[string]string{"name": "agent-observer", "version": "1"},
		"capabilities": map[string]bool{"experimentalApi": true},
	})
	if err != nil {
		return err
	}
	if err := w.write(ctx, map[string]any{"method": "initialized"}); err != nil {
		return err
	}
	result, err := w.request(ctx, "thread/start", map[string]any{
		"cwd": cfg.CWD, "runtimeWorkspaceRoots": []string{cfg.CWD},
		"sandbox": "read-only", "approvalPolicy": "untrusted", "approvalsReviewer": "user",
		"developerInstructions": workerInstructions(cfg),
		"dynamicTools":          []any{sendTool()},
	})
	if err != nil {
		return err
	}
	var response struct {
		Thread struct{ ID string } `json:"thread"`
	}
	if json.Unmarshal(result, &response) != nil || response.Thread.ID == "" {
		return errors.New("codex worker returned no thread identity")
	}
	w.threadID = response.Thread.ID
	return nil
}

// Close stops the worker process, including any pending turn. It is idempotent.
func (w *Worker) Close() error {
	w.shutdown()
	select {
	case <-w.wait:
		return nil
	case <-time.After(5 * time.Second):
		return errors.New("codex worker shutdown is still pending")
	}
}

// shutdown can run from a cancellation callback while the caller owns mu.
// It interrupts owned I/O without waiting for that caller or the child to exit.
func (w *Worker) shutdown() {
	w.closeOnce.Do(func() {
		w.cancel()
		_ = w.input.Close()
	})
}

// Run submits one message and waits for the matching provider turn to finish.
// Cancellation closes the worker, preventing a timed-out turn from accepting
// further messages. The manager decides whether a later worker may be created.
func (w *Worker) Run(ctx context.Context, msg workers.Input) (workers.Receipt, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	receipt := workers.Receipt{ConversationID: w.threadID}
	if msg.ID == "" || msg.Source == "" || msg.Body == "" {
		return receipt, errors.New("codex worker message requires identity, source, and body")
	}
	runCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(w.ctx, cancel)
	defer stop()
	defer cancel()
	text := fmt.Sprintf("Observer message ID: %s\nFrom: %s\n\n%s", msg.ID, msg.Source, msg.Body)
	result, err := w.request(runCtx, "turn/start", map[string]any{
		"threadId": w.threadID,
		"input":    []any{map[string]string{"type": "text", "text": text}},
	})
	if err != nil {
		w.Close()
		return receipt, err
	}
	var response struct {
		Turn turn `json:"turn"`
	}
	if json.Unmarshal(result, &response) != nil || response.Turn.ID == "" {
		w.Close()
		return receipt, errors.New("codex worker returned no turn identity")
	}
	receipt.ExecutionID = response.Turn.ID
	if msg.OnAccepted != nil {
		if err := msg.OnAccepted(receipt); err != nil {
			w.Close()
			return receipt, errors.New("persist codex worker delivery receipt")
		}
	}
	if response.Turn.Status != "inProgress" {
		receipt, err = completedReceipt(receipt, response.Turn)
	} else {
		receipt, err = w.awaitTurn(runCtx, receipt)
	}
	if err != nil {
		w.Close()
	}
	return receipt, err
}

func sendTool() map[string]any {
	spec := workers.SendToolSpec()
	spec["type"] = "function"
	return spec
}
