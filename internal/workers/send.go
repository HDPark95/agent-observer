package workers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"unicode/utf8"

	"agent-observer/internal/messaging"
)

// DecodeSendArguments accepts one tool object under the domain's send contract.
func DecodeSendArguments(raw []byte) (Outbound, error) {
	raw = bytes.TrimSpace(raw)
	if !utf8.Valid(raw) || len(raw) == 0 || raw[0] != '{' {
		return Outbound{}, messaging.ErrInvalid
	}
	var args struct {
		ID     string `json:"message_id"`
		Target string `json:"target"`
		Body   string `json:"body"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&args) != nil || decoder.Decode(new(any)) != io.EOF {
		return Outbound{}, messaging.ErrInvalid
	}
	if err := messaging.ValidateRequest(messaging.Request{ID: args.ID, Target: args.Target, Body: args.Body}); err != nil {
		return Outbound{}, err
	}
	return Outbound{ID: args.ID, Target: args.Target, Body: args.Body}, nil
}

// SendToolSpec returns a fresh common schema. Adapters add only wire metadata.
func SendToolSpec() map[string]any {
	return map[string]any{"name": "observer_send_message",
		"description": fmt.Sprintf("Send one message to a configured Observer teammate. Stores its body and automatically runs the recipient. Reuse message_id only for an identical retry. Body must be nonblank UTF-8, at most %d characters and %d bytes; controls other than newline, carriage return and tab are prohibited.", messaging.MaxBodyRunes, messaging.MaxBodyBytes),
		"inputSchema": map[string]any{"type": "object", "additionalProperties": false,
			"required": []string{"target", "body", "message_id"}, "properties": map[string]any{
				"target":     map[string]any{"type": "string", "minLength": 1, "maxLength": messaging.MaxAgentRunes},
				"message_id": map[string]any{"type": "string", "pattern": messaging.MessageIDPattern},
				"body":       map[string]any{"type": "string", "minLength": 1, "maxLength": messaging.MaxBodyRunes},
			}},
	}
}
