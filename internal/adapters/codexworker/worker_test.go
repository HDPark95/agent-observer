package codexworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"agent-observer/internal/workers"
)

func fixtureWorker(t *testing.T, scenario string, send workers.SendFunc) *Worker {
	t.Helper()
	return fixtureWorkerAt(t, t.TempDir(), scenario, send)
}

func fixtureWorkerAt(t *testing.T, cwd, scenario string, send workers.SendFunc) *Worker {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cfg := workers.RuntimeConfig{
		Executable: executable, CWD: cwd, Instructions: "Review the assigned work.",
		WorkerID: "managed:ux", Peers: []workers.Peer{{ID: "managed:dx", Name: "DX"}},
	}
	w, err := start(context.Background(), cfg, send, func(ctx context.Context) *exec.Cmd {
		return exec.CommandContext(ctx, executable, "-test.run=^TestProtocolHelper$", "--", scenario)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	})
	return w
}

func TestWorkerSendsWithoutPollingAndKeepsOneOwnedThread(t *testing.T) {
	var calls [][3]string
	accepted := false
	w := fixtureWorker(t, "conversation", func(_ context.Context, message workers.Outbound) (workers.SendReceipt, error) {
		if !accepted {
			t.Error("tool ran before the provider receipt was persisted")
		}
		calls = append(calls, [3]string{message.Target, message.Body, message.ID})
		return workers.SendReceipt{ID: message.ID, Target: message.Target, State: "queued"}, nil
	})
	var executor workers.Runtime = w
	for i := 1; i <= 2; i++ {
		accepted = false
		receipt, err := executor.Run(context.Background(), workers.Input{
			ID: "input", Source: "ux", Body: "Review the message UI.",
			OnAccepted: func(receipt workers.Receipt) error {
				if receipt.ConversationID != "owned-thread" || receipt.ExecutionID != fmt.Sprintf("turn-%d", i) || receipt.Output != "" {
					t.Errorf("unexpected acceptance receipt: %#v", receipt)
				}
				accepted = true
				return nil
			},
		})
		if err != nil || receipt.Output != "검토 완료" || receipt.ConversationID != w.threadID {
			t.Fatalf("Run = %#v, %v", receipt, err)
		}
	}
	want := [][3]string{{"dx", "본문 그대로 전달해요", "review-1"}, {"dx", "본문 그대로 전달해요", "review-2"}}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("outbound calls = %#v, want %#v", calls, want)
	}
}

func TestCompletionCanArriveBeforeStartResponse(t *testing.T) {
	w := fixtureWorker(t, "early-completion", rejectSend)
	receipt, err := w.Run(context.Background(), workers.Input{ID: "input", Source: "ux", Body: "Review."})
	if err != nil || receipt.Output != "early result" || receipt.ExecutionID != "turn-1" {
		t.Fatalf("Run = %#v, %v", receipt, err)
	}
}

func TestAcceptedTurnIdentitySurvivesDisconnect(t *testing.T) {
	w := fixtureWorker(t, "disconnect", rejectSend)
	receipt, err := w.Run(context.Background(), workers.Input{ID: "input", Source: "ux", Body: "Review."})
	if err == nil || receipt.ConversationID != "owned-thread" || receipt.ExecutionID != "turn-1" {
		t.Fatalf("Run = %#v, %v", receipt, err)
	}
}

func TestReceiptFailureStopsWorker(t *testing.T) {
	w := fixtureWorker(t, "wait", rejectSend)
	receipt, err := w.Run(context.Background(), workers.Input{
		ID: "input", Source: "ux", Body: "Review.",
		OnAccepted: func(workers.Receipt) error { return errors.New("private failure text") },
	})
	if err == nil || strings.Contains(err.Error(), "private failure") || receipt.ExecutionID != "turn-1" {
		t.Fatalf("Run = %#v, %v", receipt, err)
	}
	select {
	case <-w.wait:
	default:
		t.Fatal("worker process is still running after receipt failure")
	}
}

func TestCancellationStopsWorkerAndPreservesReceipt(t *testing.T) {
	w := fixtureWorker(t, "wait", rejectSend)
	ctx, cancel := context.WithCancel(context.Background())
	receipt, err := w.Run(ctx, workers.Input{
		ID: "input", Source: "ux", Body: "Review.",
		OnAccepted: func(workers.Receipt) error { cancel(); return nil },
	})
	if !errors.Is(err, context.Canceled) || receipt.ExecutionID != "turn-1" {
		t.Fatalf("Run = %#v, %v", receipt, err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestProviderErrorDoesNotExposeMessageText(t *testing.T) {
	w := fixtureWorker(t, "rpc-error", rejectSend)
	_, err := w.Run(context.Background(), workers.Input{ID: "input", Source: "ux", Body: "Review."})
	if err == nil || !strings.Contains(err.Error(), "-32602") || strings.Contains(err.Error(), "private") {
		t.Fatalf("unexpected provider error: %v", err)
	}
}

func TestInterruptedTurnIsNotReportedComplete(t *testing.T) {
	w := fixtureWorker(t, "interrupted", rejectSend)
	receipt, err := w.Run(context.Background(), workers.Input{ID: "input", Source: "ux", Body: "Review."})
	if err == nil || receipt.ExecutionID != "turn-1" {
		t.Fatalf("Run = %#v, %v", receipt, err)
	}
}

func TestCloseAllowsChildGracefulCleanup(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("os.Interrupt is not supported on Windows")
	}
	dir := t.TempDir()
	w := fixtureWorkerAt(t, dir, "graceful-close", rejectSend)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "graceful-cleanup")); err != nil {
		t.Fatalf("child did not run interrupt cleanup: %v", err)
	}
}

func TestCloseEscalatesWhenChildIgnoresInterrupt(t *testing.T) {
	w := fixtureWorker(t, "ignore-close", rejectSend)
	if err := w.Close(); err != nil {
		t.Fatalf("bounded shutdown failed: %v", err)
	}
	select {
	case <-w.wait:
	default:
		t.Fatal("child was not reaped")
	}
}

func rejectSend(context.Context, workers.Outbound) (workers.SendReceipt, error) {
	return workers.SendReceipt{}, errors.New("unexpected outbound message")
}

// TestProtocolHelper is a child-process fixture for the public app-server wire
// protocol. It never invokes Codex, changes credentials, or substitutes a hook.
func TestProtocolHelper(t *testing.T) {
	if len(os.Args) < 3 || os.Args[len(os.Args)-2] != "--" {
		return
	}
	scenario := os.Args[len(os.Args)-1]
	interrupts := make(chan os.Signal, 1)
	if scenario == "graceful-close" || scenario == "ignore-close" {
		signal.Notify(interrupts, os.Interrupt)
		defer signal.Stop(interrupts)
	}
	decoder := json.NewDecoder(os.Stdin)
	encoder := json.NewEncoder(os.Stdout)
	read := func() map[string]json.RawMessage {
		var message map[string]json.RawMessage
		if err := decoder.Decode(&message); err != nil {
			if scenario == "graceful-close" {
				select {
				case <-interrupts:
					if os.WriteFile("graceful-cleanup", []byte("done"), 0600) != nil {
						os.Exit(1)
					}
				case <-time.After(3 * time.Second):
					os.Exit(1)
				}
			}
			if scenario == "ignore-close" {
				for {
					time.Sleep(time.Hour)
				}
			}
			os.Exit(0)
		}
		return message
	}
	write := func(message any) {
		if encoder.Encode(message) != nil {
			os.Exit(1)
		}
	}
	reply := func(message map[string]json.RawMessage, result any) {
		write(map[string]any{"id": message["id"], "result": result})
	}
	require := func(ok bool) {
		if !ok {
			os.Exit(2)
		}
	}
	message := read()
	require(string(message["method"]) == `"initialize"`)
	reply(message, map[string]any{})
	message = read()
	require(string(message["method"]) == `"initialized"`)
	if scenario == "blocked-setup-input" {
		for {
			time.Sleep(time.Hour)
		}
	}
	message = read()
	require(string(message["method"]) == `"thread/start"`)
	var params map[string]json.RawMessage
	require(json.Unmarshal(message["params"], &params) == nil)
	require(string(params["sandbox"]) == `"read-only"` && string(params["approvalPolicy"]) == `"untrusted"`)
	require(params["model"] == nil && params["threadId"] == nil)
	var instructions string
	require(json.Unmarshal(params["developerInstructions"], &instructions) == nil)
	require(strings.HasPrefix(instructions, "Review the assigned work."))
	require(strings.Contains(instructions, "identity managed:ux"))
	require(strings.Contains(instructions, "managed:dx (DX)"))
	require(strings.Contains(instructions, "Use only observer_send_message"))
	var tools []map[string]json.RawMessage
	require(json.Unmarshal(params["dynamicTools"], &tools) == nil && len(tools) == 1)
	require(string(tools[0]["type"]) == `"function"` && string(tools[0]["name"]) == `"observer_send_message"`)
	reply(message, map[string]any{"thread": map[string]string{"id": "owned-thread"}})
	if scenario == "blocked-turn-input" {
		for {
			time.Sleep(time.Hour)
		}
	}
	for i := 1; ; i++ {
		message = read()
		require(string(message["method"]) == `"turn/start"`)
		turnID := fmt.Sprintf("turn-%d", i)
		if scenario == "rpc-error" {
			write(map[string]any{"id": message["id"], "error": map[string]any{"code": -32602, "message": "private token and body"}})
			continue
		}
		if scenario == "early-completion" {
			write(completion(turnID, "completed", "early result"))
		}
		reply(message, map[string]any{"turn": map[string]any{"id": turnID, "status": "inProgress", "items": []any{}}})
		switch scenario {
		case "early-completion":
			continue
		case "disconnect":
			os.Exit(0)
		case "wait":
			_, _ = io.Copy(io.Discard, os.Stdin)
			os.Exit(0)
		case "interrupted":
			write(completion(turnID, "interrupted", ""))
			continue
		case "blocked-tool-response":
			write(map[string]any{"id": "tool", "method": "item/tool/call", "params": map[string]any{
				"threadId": "owned-thread", "turnId": turnID, "tool": "observer_send_message",
				"arguments": map[string]string{"target": "dx", "body": "Review ready.", "message_id": "review-1"},
			}})
			for {
				time.Sleep(time.Hour)
			}
		}
		for _, method := range []string{"item/commandExecution/requestApproval", "item/fileChange/requestApproval"} {
			write(map[string]any{"id": "approval", "method": method, "params": map[string]any{}})
			response := read()
			require(strings.Contains(string(response["result"]), `"decline"`))
		}
		for _, threadID := range []string{"other-thread", "owned-thread"} {
			write(map[string]any{"id": "tool", "method": "item/tool/call", "params": map[string]any{
				"threadId": threadID, "turnId": turnID, "callId": "call", "tool": "observer_send_message",
				"arguments": map[string]string{"target": "dx", "body": "본문 그대로 전달해요", "message_id": fmt.Sprintf("review-%d", i)},
			}})
			response := read()
			var result struct {
				Success bool `json:"success"`
				Items   []struct {
					Type string `json:"type"`
					Text string `json:"text"`
				} `json:"contentItems"`
			}
			require(json.Unmarshal(response["result"], &result) == nil && result.Success == (threadID == "owned-thread"))
			if result.Success {
				require(len(result.Items) == 1 && result.Items[0].Type == "inputText")
				var receipt map[string]string
				require(json.Unmarshal([]byte(result.Items[0].Text), &receipt) == nil)
				require(len(receipt) == 3 && receipt["id"] == fmt.Sprintf("review-%d", i) && receipt["target"] == "dx" && receipt["state"] == "queued")
			}
		}
		write(completion("unrelated-turn", "completed", "wrong result"))
		write(completion(turnID, "completed", "검토 완료"))
	}
}

func completion(turnID, status, text string) map[string]any {
	return map[string]any{"method": "turn/completed", "params": map[string]any{
		"threadId": "owned-thread", "turn": map[string]any{"id": turnID, "status": status, "items": []any{
			map[string]string{"type": "agentMessage", "phase": "commentary", "text": "not final"},
			map[string]string{"type": "agentMessage", "phase": "final_answer", "text": text},
		}},
	}}
}

func TestStartRejectsInvalidConfig(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := Start(ctx, workers.RuntimeConfig{Executable: "relative", CWD: t.TempDir()}, rejectSend); err == nil {
		t.Fatal("accepted relative executable")
	}
}
