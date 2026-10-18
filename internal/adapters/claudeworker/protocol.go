package claudeworker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	"agent-observer/internal/messaging"
	"agent-observer/internal/workers"
)

const maxFrameBytes = 4 << 20

type streamFrame struct {
	Type            string          `json:"type"`
	Subtype         string          `json:"subtype"`
	UUID            string          `json:"uuid"`
	SessionID       string          `json:"session_id"`
	IsReplay        bool            `json:"isReplay"`
	ParentToolUseID json.RawMessage `json:"parent_tool_use_id"`
	Message         struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
	} `json:"message"`
	Tools      []string `json:"tools"`
	MCPServers []struct {
		Name   string `json:"name"`
		Status string `json:"status"`
	} `json:"mcp_servers"`
	MCPServerErrors  json.RawMessage `json:"mcp_server_errors"`
	IsError          bool            `json:"is_error"`
	Result           *string         `json:"result"`
	UserMessageUUID  string          `json:"user_message_uuid"`
	UserMessageUUIDs []string        `json:"user_message_uuids"`
}

type frameResult struct {
	frame streamFrame
	err   error
}

func userInput(uuid, session, text string) any {
	return map[string]any{
		"type": "user", "uuid": uuid, "session_id": session, "parent_tool_use_id": nil,
		"message":         map[string]string{"role": "user", "content": text},
		"client_composed": true,
	}
}

func readFrames(ctx context.Context, input io.Reader, output chan<- frameResult) {
	defer close(output)
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 64<<10), maxFrameBytes)
	for scanner.Scan() {
		var frame streamFrame
		err := json.Unmarshal(scanner.Bytes(), &frame)
		if err != nil || frame.Type == "" {
			sendFrame(ctx, output, frameResult{err: errors.New("invalid claude worker protocol frame")})
			return
		}
		if !sendFrame(ctx, output, frameResult{frame: frame}) {
			return
		}
	}
	if scanner.Err() != nil {
		sendFrame(ctx, output, frameResult{err: errors.New("read claude worker protocol")})
	}
}

func sendFrame(ctx context.Context, output chan<- frameResult, value frameResult) bool {
	select {
	case output <- value:
		return true
	case <-ctx.Done():
		return false
	}
}

func (w *Worker) write(ctx context.Context, input any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := json.NewEncoder(w.input).Encode(input); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errors.New("write claude worker protocol")
	}
	return ctx.Err()
}

func (w *Worker) read(ctx context.Context) (streamFrame, error) {
	if err := ctx.Err(); err != nil {
		return streamFrame{}, err
	}
	select {
	case <-ctx.Done():
		return streamFrame{}, ctx.Err()
	case <-w.ctx.Done():
		return streamFrame{}, errors.New("claude worker is closed")
	case value, ok := <-w.frames:
		if !ok {
			return streamFrame{}, errors.New("claude worker disconnected")
		}
		return value.frame, value.err
	}
}

func (w *Worker) awaitResult(ctx context.Context, msg workers.Input, uuid, text string, active *activeInput) (workers.Receipt, error) {
	receipt := workers.Receipt{ConversationID: w.sessionID}
	accepted := false
	for {
		frame, err := w.read(ctx)
		if err != nil {
			return receipt, err
		}
		switch frame.Type {
		case "system":
			if frame.Subtype == "init" {
				if err := w.initialize(frame); err != nil {
					return receipt, err
				}
				receipt.ConversationID = w.sessionID
			}
		case "user":
			if !frame.IsReplay {
				continue
			} // Tool results are also user events.
			if accepted || !matchesReplay(frame, uuid, w.sessionID, text) {
				return receipt, errors.New("claude worker input acknowledgment does not match")
			}
			// This is our submitted UUID acknowledged by Claude, not a native turn ID.
			receipt.ExecutionID = frame.UUID
			if msg.OnAccepted != nil {
				if err := msg.OnAccepted(receipt); err != nil {
					return receipt, errors.New("persist claude worker delivery receipt")
				}
			}
			if err := ctx.Err(); err != nil {
				return receipt, err
			}
			accepted = true
			close(active.ready) // Outbound sends cannot precede durable acceptance.
		case "result":
			if err := ctx.Err(); err != nil {
				return receipt, err
			}
			if !accepted || !matchesResult(frame, receipt) {
				return receipt, errors.New("claude worker completion does not match accepted input")
			}
			if frame.IsError || frame.Subtype != "success" {
				return receipt, errors.New("claude worker execution did not complete")
			}
			receipt.OutputState, receipt.OutputReason = messaging.OutputOmitted, "no-final-text"
			if frame.Result != nil {
				receipt.Output = *frame.Result
				receipt.OutputState, receipt.OutputReason = messaging.OutputComplete, ""
			}
			return receipt, nil
		}
	}
}

func (w *Worker) initialize(frame streamFrame) error {
	if frame.SessionID == "" || (w.sessionID != "" && frame.SessionID != w.sessionID) {
		return errors.New("claude worker returned an unexpected session identity")
	}
	connected := false
	for _, server := range frame.MCPServers {
		if server.Name != "observer" || server.Status != "connected" {
			return errors.New("claude worker messaging server is unavailable or unexpected")
		}
		connected = true
	}
	if !connected || hasServerErrors(frame.MCPServerErrors) {
		return errors.New("claude worker messaging server is unavailable")
	}
	found := false
	for _, tool := range frame.Tools {
		switch tool {
		case qualifiedSendTool:
			found = true
		case "Read", "Glob", "Grep", "EndConversation":
		default:
			return errors.New("claude worker exposed an unexpected tool")
		}
	}
	if !found {
		return errors.New("claude worker messaging tool is unavailable")
	}
	w.sessionID = frame.SessionID
	return nil
}

func hasServerErrors(raw json.RawMessage) bool {
	return len(raw) > 0 && !bytes.Equal(raw, []byte("null")) && !bytes.Equal(raw, []byte("[]")) && !bytes.Equal(raw, []byte("{}"))
}

func matchesReplay(frame streamFrame, uuid, session, text string) bool {
	if session == "" || frame.UUID != uuid || frame.SessionID != session || frame.Message.Role != "user" {
		return false
	}
	if len(frame.ParentToolUseID) > 0 && !bytes.Equal(frame.ParentToolUseID, []byte("null")) {
		return false
	}
	var content string
	if json.Unmarshal(frame.Message.Content, &content) == nil {
		return content == text
	}
	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	return json.Unmarshal(frame.Message.Content, &blocks) == nil && len(blocks) == 1 && blocks[0].Type == "text" && blocks[0].Text == text
}

func matchesResult(frame streamFrame, receipt workers.Receipt) bool {
	if frame.UUID == "" || frame.SessionID != receipt.ConversationID {
		return false
	}
	if len(frame.ParentToolUseID) > 0 && !bytes.Equal(frame.ParentToolUseID, []byte("null")) {
		return false
	}
	if frame.UserMessageUUID != "" && frame.UserMessageUUID != receipt.ExecutionID {
		return false
	}
	if len(frame.UserMessageUUIDs) > 0 && (len(frame.UserMessageUUIDs) != 1 || frame.UserMessageUUIDs[0] != receipt.ExecutionID) {
		return false
	}
	// Older CLIs omit correlation fields. Only one accepted input is in flight
	// in this owned process, so its first result belongs to that serialized Run.
	return true
}
