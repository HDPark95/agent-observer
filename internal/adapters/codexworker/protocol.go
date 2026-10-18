package codexworker

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"agent-observer/internal/messaging"
	"agent-observer/internal/workers"
)

const maxFrameBytes = 4 << 20

type wireMessage struct {
	ID     json.RawMessage     `json:"id"`
	Method string              `json:"method"`
	Params json.RawMessage     `json:"params"`
	Result json.RawMessage     `json:"result"`
	Error  *struct{ Code int } `json:"error"`
}

type wireResult struct {
	message wireMessage
	err     error
}

type publicItem struct {
	Type  string `json:"type"`
	Phase string `json:"phase"`
	Text  string `json:"text"`
}

type turn struct {
	ID     string       `json:"id"`
	Status string       `json:"status"`
	Items  []publicItem `json:"items"`
	Error  *struct {
		Message string `json:"message"`
	} `json:"error"`
}

func readFrames(ctx context.Context, input io.Reader, output chan<- wireResult) {
	defer close(output)
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 64<<10), maxFrameBytes)
	for scanner.Scan() {
		var message wireMessage
		if err := json.Unmarshal(scanner.Bytes(), &message); err != nil {
			select {
			case output <- wireResult{err: errors.New("invalid codex worker protocol frame")}:
			case <-ctx.Done():
			}
			return
		}
		select {
		case output <- wireResult{message: message}:
		case <-ctx.Done():
			return
		}
	}
	if scanner.Err() != nil {
		select {
		case output <- wireResult{err: errors.New("read codex worker protocol")}:
		case <-ctx.Done():
		}
	}
}

func (w *Worker) write(ctx context.Context, message any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if w.ctx.Err() != nil {
		return errors.New("codex worker is closed")
	}
	// A child can stop reading stdin while keeping the pipe open. Closing our
	// pipe on cancellation unblocks Encode; waiting for its return first would
	// prevent Run and initialization from ever reaching Close.
	stopped := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(stopped)
		w.shutdown()
	})
	err := json.NewEncoder(w.input).Encode(message)
	if !stop() {
		<-stopped // Do not leave a callback able to close a subsequent turn.
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err != nil {
		return errors.New("write codex worker protocol")
	}
	return nil
}

func (w *Worker) read(ctx context.Context) (wireMessage, error) {
	select {
	case <-ctx.Done():
		return wireMessage{}, ctx.Err()
	case <-w.ctx.Done():
		return wireMessage{}, errors.New("codex worker is closed")
	case result, ok := <-w.frames:
		if !ok {
			return wireMessage{}, errors.New("codex worker disconnected")
		}
		return result.message, result.err
	}
}

func (w *Worker) request(ctx context.Context, method string, params any) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if w.ctx.Err() != nil {
		return nil, errors.New("codex worker is closed")
	}
	w.nextID++
	id := w.nextID
	if err := w.write(ctx, map[string]any{"id": id, "method": method, "params": params}); err != nil {
		return nil, err
	}
	for {
		message, err := w.read(ctx)
		if err != nil {
			return nil, err
		}
		var responseID int
		if message.Method == "" && json.Unmarshal(message.ID, &responseID) == nil && responseID == id {
			if message.Error != nil {
				// Provider error messages can echo prompts or credentials. Keep only the code.
				return nil, fmt.Errorf("codex worker %s rejected (code %d)", method, message.Error.Code)
			}
			if len(message.Result) == 0 {
				return nil, errors.New("codex worker response has no result")
			}
			return message.Result, nil
		}
		if message.Method != "" && message.Method != "item/tool/call" && len(message.ID) > 0 {
			if err := w.handleRequest(ctx, workers.Receipt{}, message); err != nil {
				return nil, err
			}
			continue
		}
		if len(w.pending) >= 256 {
			return nil, errors.New("codex worker response queue exceeded its limit")
		}
		// A completion or dynamic call can precede the turn/start response. Keep
		// it until the provider-issued turn identity has been recorded.
		w.pending = append(w.pending, message)
	}
}

func (w *Worker) next(ctx context.Context) (wireMessage, error) {
	if err := ctx.Err(); err != nil {
		return wireMessage{}, err
	}
	if len(w.pending) == 0 {
		return w.read(ctx)
	}
	message := w.pending[0]
	w.pending[0] = wireMessage{}
	w.pending = w.pending[1:]
	return message, nil
}

func (w *Worker) awaitTurn(ctx context.Context, receipt workers.Receipt) (workers.Receipt, error) {
	for {
		message, err := w.next(ctx)
		if err != nil {
			return receipt, err
		}
		if message.Method != "" && len(message.ID) > 0 {
			if err := w.handleRequest(ctx, receipt, message); err != nil {
				return receipt, err
			}
			continue
		}
		switch message.Method {
		case "item/completed":
			var event struct {
				ThreadID string     `json:"threadId"`
				TurnID   string     `json:"turnId"`
				Item     publicItem `json:"item"`
			}
			if json.Unmarshal(message.Params, &event) != nil {
				return receipt, errors.New("invalid codex worker item completion")
			}
			if event.ThreadID == receipt.ConversationID && event.TurnID == receipt.ExecutionID {
				receipt = captureFinal(receipt, event.Item)
			}
		case "turn/completed":
			var event struct {
				ThreadID string `json:"threadId"`
				Turn     turn   `json:"turn"`
			}
			if json.Unmarshal(message.Params, &event) != nil {
				return receipt, errors.New("invalid codex worker turn completion")
			}
			if event.ThreadID == receipt.ConversationID && event.Turn.ID == receipt.ExecutionID {
				return completedReceipt(receipt, event.Turn)
			}
		}
	}
}

func completedReceipt(receipt workers.Receipt, result turn) (workers.Receipt, error) {
	for _, item := range result.Items {
		receipt = captureFinal(receipt, item)
	}
	if receipt.OutputState == "" {
		receipt.OutputState, receipt.OutputReason = messaging.OutputOmitted, "no-final-text"
	}
	if result.Status != "completed" {
		if result.Error != nil && strings.Contains(strings.ToLower(result.Error.Message), "requires a newer version of codex") {
			return receipt, workers.ErrRuntimeUpgradeRequired
		}
		return receipt, errors.New("codex worker turn did not complete")
	}
	return receipt, nil
}

func captureFinal(receipt workers.Receipt, item publicItem) workers.Receipt {
	if item.Type == "agentMessage" && (item.Phase == "" || item.Phase == "final_answer") {
		receipt.Output = item.Text
		receipt.OutputState, receipt.OutputReason = messaging.OutputComplete, ""
	}
	return receipt
}

func (w *Worker) handleRequest(ctx context.Context, receipt workers.Receipt, message wireMessage) error {
	var result any
	switch message.Method {
	case "item/tool/call":
		result = w.callSend(ctx, receipt, message.Params)
	case "item/commandExecution/requestApproval", "item/fileChange/requestApproval":
		result = map[string]string{"decision": "decline"}
	case "execCommandApproval", "applyPatchApproval":
		result = map[string]any{"decision": map[string]any{"denied": map[string]string{"rejection": "Observer worker does not grant additional permissions"}}}
	case "item/permissions/requestApproval":
		result = map[string]any{"permissions": map[string]any{}, "scope": "turn"}
	case "mcpServer/elicitation/request":
		result = map[string]string{"action": "decline"}
	case "item/tool/requestUserInput":
		result = map[string]any{"answers": map[string]any{}}
	case "currentTime/read":
		result = map[string]int64{"currentTimeAt": time.Now().Unix()}
	default:
		return w.write(ctx, map[string]any{"id": message.ID, "error": map[string]any{"code": -32601, "message": "unsupported Observer worker request"}})
	}
	return w.write(ctx, map[string]any{"id": message.ID, "result": result})
}

func (w *Worker) callSend(ctx context.Context, receipt workers.Receipt, raw json.RawMessage) any {
	var call struct {
		ThreadID  string          `json:"threadId"`
		TurnID    string          `json:"turnId"`
		Tool      string          `json:"tool"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if json.Unmarshal(raw, &call) != nil || call.ThreadID != receipt.ConversationID || call.TurnID != receipt.ExecutionID || call.Tool != "observer_send_message" {
		return failedSend("invalid_scope", false, "rejected", "Use observer_send_message only from this worker's active turn.")
	}
	args, err := workers.DecodeSendArguments(call.Arguments)
	if err != nil {
		return sendFailure(err)
	}
	result, err := w.send(ctx, args)
	if err != nil {
		return sendFailure(err)
	}
	return sentMessage(result)
}

func toolResult(success bool, text string) any {
	return map[string]any{"success": success, "contentItems": []any{map[string]string{"type": "inputText", "text": text}}}
}

// sentMessage encodes only the durable send receipt into Codex's tool format.
// Message bodies and source identities are intentionally absent from the reply.
func sentMessage(receipt workers.SendReceipt) any {
	encoded, _ := json.Marshal(receipt)
	return toolResult(true, string(encoded))
}
