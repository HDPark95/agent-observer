package claudeworker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"agent-observer/internal/messaging"
	"agent-observer/internal/workers"
)

func fixtureWorker(t *testing.T, scenario string, send workers.SendFunc) *Worker {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cfg := workers.RuntimeConfig{Executable: executable, CWD: t.TempDir(), WorkerID: "managed:ux", Instructions: "Review assigned work.", Peers: []workers.Peer{{ID: "managed:dx", Name: "DX"}}}
	w, err := start(context.Background(), cfg, send, func(ctx context.Context, args []string) *exec.Cmd {
		return exec.CommandContext(ctx, executable, append([]string{"-test.run=^TestProtocolHelper$", "--", scenario}, args...)...)
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

func rejectSend(context.Context, workers.Outbound) (workers.SendReceipt, error) {
	return workers.SendReceipt{}, errors.New("unexpected outbound message")
}

func TestOwnedConversationRunsSequentialInputsAndSendsBodies(t *testing.T) {
	var accepted atomic.Bool
	var mu sync.Mutex
	var sent []workers.Outbound
	w := fixtureWorker(t, "send", func(_ context.Context, msg workers.Outbound) (workers.SendReceipt, error) {
		if !accepted.Load() {
			t.Error("send preceded durable input acceptance")
		}
		mu.Lock()
		sent = append(sent, msg)
		mu.Unlock()
		return workers.SendReceipt{ID: msg.ID, Target: msg.Target, State: "queued"}, nil
	})
	var previous workers.Receipt
	for i := 1; i <= 2; i++ {
		accepted.Store(false)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		receipt, err := w.Run(ctx, workers.Input{ID: fmt.Sprintf("input-%d", i), Source: "agent", Body: "메시지 본문을 전달해요.", OnAccepted: func(receipt workers.Receipt) error {
			if receipt.ConversationID != "owned-session" || receipt.ExecutionID == "" || receipt.Output != "" {
				t.Errorf("acceptance = %#v", receipt)
			}
			accepted.Store(true)
			return nil
		}})
		cancel() // A completed Run must detach its cancellation callback.
		if err != nil || receipt.Output != "검토 완료" {
			t.Fatalf("Run = %#v, %v", receipt, err)
		}
		if i == 2 && (receipt.ConversationID != previous.ConversationID || receipt.ExecutionID == previous.ExecutionID) {
			t.Fatalf("conversation/input identity mismatch: %#v then %#v", previous, receipt)
		}
		previous = receipt
	}
	mu.Lock()
	defer mu.Unlock()
	if len(sent) != 2 || sent[0].Body != "본문 그대로 전달해요" || sent[0].Target != "managed:dx" || sent[0].ID != "reply-1" || sent[1].ID != "reply-2" {
		t.Fatalf("sent = %#v", sent)
	}
}

func TestProtocolFailuresNeverComplete(t *testing.T) {
	for _, scenario := range []string{"wrong-ack-id", "wrong-ack-body", "wrong-ack-session", "nested-ack", "duplicate-ack", "no-init", "missing-mcp", "unexpected-tool", "result-before-ack", "wrong-result-session", "wrong-result-correlation", "multiple-result-inputs", "nested-result", "failed-result", "disconnect", "invalid-json", "oversize-frame"} {
		t.Run(scenario, func(t *testing.T) {
			w := fixtureWorker(t, scenario, rejectSend)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			receipt, err := w.Run(ctx, workers.Input{ID: "input", Source: "agent", Body: "Review."})
			if err == nil || receipt.Output != "" {
				t.Fatalf("Run = %#v, %v; want failed execution", receipt, err)
			}
			if strings.Contains(err.Error(), "private-provider-detail") {
				t.Fatalf("unsafe error: %v", err)
			}
			select {
			case <-w.wait:
			default:
				t.Fatal("process not reaped after failure")
			}
		})
	}
}

func TestAcceptedIdentitySurvivesFailure(t *testing.T) {
	for _, scenario := range []string{"disconnect", "failed-result", "wrong-result-correlation"} {
		t.Run(scenario, func(t *testing.T) {
			w := fixtureWorker(t, scenario, rejectSend)
			var accepted workers.Receipt
			receipt, err := w.Run(context.Background(), workers.Input{ID: "input", Source: "agent", Body: "Review.", OnAccepted: func(r workers.Receipt) error { accepted = r; return nil }})
			if err == nil || accepted.ExecutionID == "" || receipt != accepted {
				t.Fatalf("Run = %#v, %v; accepted %#v", receipt, err, accepted)
			}
		})
	}
}

func TestReceiptFailureStopsBeforeAnySend(t *testing.T) {
	var sends atomic.Int32
	w := fixtureWorker(t, "send", func(context.Context, workers.Outbound) (workers.SendReceipt, error) {
		sends.Add(1)
		return workers.SendReceipt{}, nil
	})
	receipt, err := w.Run(context.Background(), workers.Input{ID: "input", Source: "agent", Body: "Review.", OnAccepted: func(workers.Receipt) error { return errors.New("private-storage-detail") }})
	if err == nil || strings.Contains(err.Error(), "private-storage-detail") || receipt.ExecutionID == "" || sends.Load() != 0 {
		t.Fatalf("Run = %#v, %v; sends %d", receipt, err, sends.Load())
	}
}

func TestCancelAfterAcceptancePreservesReceipt(t *testing.T) {
	w := fixtureWorker(t, "wait", rejectSend)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	receipt, err := w.Run(ctx, workers.Input{ID: "input", Source: "agent", Body: "Review.", OnAccepted: func(workers.Receipt) error { cancel(); return nil }})
	if !errors.Is(err, context.Canceled) || receipt.ExecutionID == "" {
		t.Fatalf("Run = %#v, %v", receipt, err)
	}
	if _, err := w.Run(context.Background(), workers.Input{ID: "later", Source: "agent", Body: "Must not run"}); err == nil {
		t.Fatal("canceled process accepted another input")
	}
}

func TestDeadlineInterruptsBlockedInput(t *testing.T) {
	w := fixtureWorker(t, "unread-input", rejectSend)
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	started := time.Now()
	receipt, err := w.Run(ctx, workers.Input{ID: "input", Source: "agent", Body: strings.Repeat("<", messaging.MaxBodyRunes)})
	if !errors.Is(err, context.DeadlineExceeded) || receipt.ExecutionID != "" || time.Since(started) > 3*time.Second {
		t.Fatalf("Run = %#v, %v after %v", receipt, err, time.Since(started))
	}
	select {
	case <-w.wait:
	default:
		t.Fatal("blocked process not reaped")
	}
}

func TestPrivateConfigurationIsRemovedOnClose(t *testing.T) {
	w := fixtureWorker(t, "normal", rejectSend)
	dir := w.dir
	for _, name := range []string{"mcp.json", "instructions.txt"} {
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0600 {
			t.Fatalf("%s permissions = %o", name, info.Mode().Perm())
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("private configuration remains: %v", err)
	}
}

func TestStartFailureRemovesPrivateConfiguration(t *testing.T) {
	cfg := workers.RuntimeConfig{Executable: "/missing-observer-claude", CWD: t.TempDir()}
	var configPath string
	w, err := start(context.Background(), cfg, rejectSend, func(ctx context.Context, args []string) *exec.Cmd {
		for i, arg := range args {
			if arg == "--mcp-config" {
				configPath = args[i+1]
			}
		}
		return exec.CommandContext(ctx, cfg.Executable, args...)
	})
	if err == nil || w != nil || configPath == "" {
		t.Fatalf("start = %v, %v; configuration path missing: %v", w, err, configPath == "")
	}
	if _, err := os.Stat(filepath.Dir(configPath)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed start left configuration: %v", err)
	}
}

func TestStreamCorrelationHelpersRejectUnrelatedInputs(t *testing.T) {
	receipt := workers.Receipt{ConversationID: "session", ExecutionID: "input"}
	valid := streamFrame{UUID: "result", SessionID: "session", UserMessageUUIDs: []string{"input"}}
	if !matchesResult(valid, receipt) {
		t.Fatal("matching plural input correlation was rejected")
	}
	valid.UserMessageUUIDs = nil
	if !matchesResult(valid, receipt) {
		t.Fatal("serialized legacy result without correlation was rejected")
	}
	for _, raw := range []string{`null`, `[]`, `{}`} {
		if hasServerErrors(json.RawMessage(raw)) {
			t.Errorf("empty server errors %s rejected", raw)
		}
	}
	if !hasServerErrors(json.RawMessage(`[{"name":"observer","error":"private"}]`)) {
		t.Fatal("server errors were ignored")
	}
	frame := streamFrame{UUID: "input", SessionID: "session"}
	frame.Message.Role = "user"
	frame.Message.Content = json.RawMessage(`[{"type":"text","text":"body"}]`)
	if !matchesReplay(frame, "input", "session", "body") {
		t.Fatal("text block replay rejected")
	}
	frame.Message.Content = json.RawMessage(`[{"type":"text","text":"body"},{"type":"text","text":"other"}]`)
	if matchesReplay(frame, "input", "session", "body") {
		t.Fatal("extra replay content accepted")
	}
}

// TestProtocolHelper is a child-process fixture for the documented stream-json
// protocol. It never invokes a model or changes host authentication/settings.
func TestProtocolHelper(t *testing.T) {
	args := os.Args
	marker := -1
	for i, arg := range args {
		if arg == "--" {
			marker = i
			break
		}
	}
	if marker < 0 || marker+1 >= len(args) {
		return
	}
	scenario := args[marker+1]
	flags := args[marker+2:]
	configPath := ""
	for i, arg := range flags {
		if arg == "--mcp-config" && i+1 < len(flags) {
			configPath = flags[i+1]
		}
	}
	if scenario == "unread-input" {
		for {
			time.Sleep(time.Hour)
		}
	}
	decoder, encoder := json.NewDecoder(os.Stdin), json.NewEncoder(os.Stdout)
	write := func(value any) {
		if encoder.Encode(value) != nil {
			os.Exit(1)
		}
	}
	for i := 1; ; i++ {
		var input struct {
			UUID      string         `json:"uuid"`
			SessionID string         `json:"session_id"`
			Message   map[string]any `json:"message"`
		}
		if decoder.Decode(&input) != nil {
			os.Exit(0)
		}
		if i > 1 && input.SessionID != "owned-session" {
			os.Exit(2)
		}
		if scenario == "invalid-json" {
			fmt.Fprintln(os.Stdout, "not json private-provider-detail")
			os.Exit(0)
		}
		if scenario == "oversize-frame" {
			fmt.Fprintln(os.Stdout, strings.Repeat("x", maxFrameBytes+1))
			os.Exit(0)
		}
		if i == 1 && scenario != "no-init" {
			servers := []any{map[string]string{"name": "observer", "status": "connected"}}
			tools := []string{"Read", "Glob", "Grep", "EndConversation", qualifiedSendTool}
			if scenario == "missing-mcp" {
				servers = nil
			}
			if scenario == "unexpected-tool" {
				tools = append(tools, "Bash")
			}
			write(map[string]any{"type": "system", "subtype": "init", "session_id": "owned-session", "tools": tools, "mcp_servers": servers})
		}
		ack := map[string]any{"type": "user", "uuid": input.UUID, "session_id": "owned-session", "isReplay": true, "parent_tool_use_id": nil, "message": input.Message}
		result := map[string]any{"type": "result", "subtype": "success", "uuid": fmt.Sprintf("result-%d", i), "session_id": "owned-session", "user_message_uuid": input.UUID, "result": "검토 완료"}
		switch scenario {
		case "wrong-ack-id":
			ack["uuid"] = "unexpected"
		case "wrong-ack-session":
			ack["session_id"] = "unrelated"
		case "wrong-ack-body":
			ack["message"] = map[string]string{"role": "user", "content": "unrelated"}
		case "nested-ack":
			ack["parent_tool_use_id"] = "nested-tool"
		case "result-before-ack":
			write(result)
		}
		write(ack)
		if scenario == "duplicate-ack" {
			write(ack)
		}
		if scenario == "wait" {
			for {
				time.Sleep(time.Hour)
			}
		}
		if scenario == "disconnect" {
			os.Exit(0)
		}
		if scenario == "send" {
			fixtureSend(configPath, i)
		}
		switch scenario {
		case "wrong-result-session":
			result["session_id"] = "unrelated"
		case "wrong-result-correlation":
			result["user_message_uuid"] = "different-input"
		case "multiple-result-inputs":
			result["user_message_uuids"] = []string{input.UUID, "other"}
		case "nested-result":
			result["parent_tool_use_id"] = "nested-tool"
		case "failed-result":
			result["is_error"] = true
			result["subtype"] = "error_during_execution"
			result["errors"] = []string{"private-provider-detail"}
		case "missing-result":
			delete(result, "result")
		case "empty-result":
			result["result"] = ""
		case "oversize-result":
			result["result"] = strings.Repeat("x", (48<<10)+1)
		}
		write(result)
	}
}

func fixtureSend(path string, index int) {
	var config struct {
		MCPServers map[string]struct {
			URL     string            `json:"url"`
			Headers map[string]string `json:"headers"`
		} `json:"mcpServers"`
	}
	data, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(data, &config) != nil {
		os.Exit(2)
	}
	server := config.MCPServers["observer"]
	data, _ = json.Marshal(map[string]any{"jsonrpc": "2.0", "id": index, "method": "tools/call", "params": map[string]any{"name": "observer_send_message", "arguments": map[string]string{"target": "managed:dx", "body": "본문 그대로 전달해요", "message_id": fmt.Sprintf("reply-%d", index)}}})
	req, err := http.NewRequest(http.MethodPost, server.URL, bytes.NewReader(data))
	if err != nil {
		os.Exit(2)
	}
	for key, value := range server.Headers {
		req.Header.Set(key, value)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("MCP-Protocol-Version", "2025-06-18")
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Do(req)
	if err != nil {
		os.Exit(2)
	}
	defer response.Body.Close()
	data, err = io.ReadAll(response.Body)
	if err != nil || response.StatusCode != http.StatusOK || bytes.Contains(data, []byte(`"isError":true`)) {
		os.Exit(2)
	}
}

func TestFinalResultPresenceAndLargeOutputPreserveCompletion(t *testing.T) {
	for _, scenario := range []string{"missing-result", "empty-result", "oversize-result"} {
		t.Run(scenario, func(t *testing.T) {
			w := fixtureWorker(t, scenario, rejectSend)
			receipt, err := w.Run(context.Background(), workers.Input{ID: "input", Source: "agent", Body: "Review."})
			if err != nil || receipt.ConversationID == "" || receipt.ExecutionID == "" {
				t.Fatalf("completion lost: %v", err)
			}
			want := messaging.OutputComplete
			if scenario == "missing-result" {
				want = messaging.OutputOmitted
			}
			if receipt.OutputState != want {
				t.Fatalf("state=%q", receipt.OutputState)
			}
			if scenario == "oversize-result" && len(receipt.Output) != 48<<10+1 {
				t.Fatalf("provider discarded output: %d", len(receipt.Output))
			}
		})
	}
}
