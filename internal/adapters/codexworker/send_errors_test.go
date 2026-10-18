package codexworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"agent-observer/internal/messaging"
	"agent-observer/internal/workers"
)

func TestSendErrorsGiveSafeCorrectiveActions(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		code      string
		retryable bool
		outcome   string
		action    string
	}{
		{"conflict", messaging.ErrConflict, "id_conflict", false, "rejected", "different content"},
		{"invalid", messaging.ErrInvalid, "invalid_request", false, "rejected", "Correct the target"},
		{"recipient", messaging.ErrNotFound, "recipient_not_found", false, "rejected", "configured team"},
		{"stopped", messaging.ErrRecipientUnavailable, "recipient_unavailable", false, "rejected", "recipient is stopped"},
		{"unauthorized", messaging.ErrUnauthorized, "not_authorized", false, "rejected", "messaging scope"},
		{"forbidden", messaging.ErrForbidden, "not_authorized", false, "rejected", "messaging scope"},
		{"transport", messaging.ErrUnsupported, "unsupported_transport", false, "rejected", "configure"},
		{"capacity", messaging.ErrCapacity, "capacity_reached", false, "rejected", "free messaging capacity"},
		{"closed", messaging.ErrClosed, "store_unavailable", false, "rejected", "restore the message store"},
		{"locked", messaging.ErrLocked, "store_unavailable", false, "rejected", "restore the message store"},
		{"uncertain", messaging.ErrOutcomeUnknown, "outcome_unknown", true, "unknown", "Retry only the identical message_id"},
		{"canceled", context.Canceled, "send_interrupted", false, "unknown", "Check Observer's delivery status"},
		{"deadline", context.DeadlineExceeded, "send_interrupted", false, "unknown", "Check Observer's delivery status"},
		{"unknown", errors.New("private provider failure"), "send_failed", false, "unknown", "acceptance is not confirmed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			called := false
			w := &Worker{send: func(context.Context, workers.Outbound) (workers.SendReceipt, error) {
				called = true
				return workers.SendReceipt{ID: "private partial result"}, fmt.Errorf("private token and body: %w", test.err)
			}}
			result := w.callSend(context.Background(), workers.Receipt{ConversationID: "owned-thread", ExecutionID: "turn-1"}, sendCall(t, "owned-thread", "turn-1", map[string]string{
				"target": "dx", "body": "Review ready.", "message_id": "review-1",
			}))
			failure := decodeSendFailure(t, result)
			if !called || failure.Code != test.code || failure.Retryable != test.retryable || failure.Outcome != test.outcome || !strings.Contains(failure.Action, test.action) {
				t.Errorf("send error = %#v, called = %v; want code=%s retryable=%v outcome=%s action containing %q", failure, called, test.code, test.retryable, test.outcome, test.action)
			}
			if strings.Contains(failure.Action, "did not accept") {
				t.Error("send failure claims a definitive transport outcome")
			}
		})
	}
}

func TestInvalidToolCallsHaveStructuredErrorsAndNeverSend(t *testing.T) {
	w := &Worker{send: func(context.Context, workers.Outbound) (workers.SendReceipt, error) {
		t.Fatal("invalid tool call reached the sender")
		return workers.SendReceipt{}, nil
	}}
	for _, test := range []struct {
		name string
		raw  json.RawMessage
		code string
	}{
		{"scope", sendCall(t, "other-thread", "turn-1", map[string]string{"target": "dx", "body": "Review.", "message_id": "review-1"}), "invalid_scope"},
		{"arguments", sendCall(t, "owned-thread", "turn-1", map[string]string{"target": "dx", "body": "Review."}), "invalid_request"},
		{"extra field", sendCall(t, "owned-thread", "turn-1", map[string]string{"target": "dx", "body": "Review.", "message_id": "review-1", "secret": "private"}), "invalid_request"},
	} {
		t.Run(test.name, func(t *testing.T) {
			failure := decodeSendFailure(t, w.callSend(context.Background(), workers.Receipt{ConversationID: "owned-thread", ExecutionID: "turn-1"}, test.raw))
			if failure.Code != test.code || failure.Retryable || failure.Outcome != "rejected" {
				t.Errorf("invalid call error = %#v", failure)
			}
		})
	}
}

type sendErrorResult struct {
	Code      string `json:"code"`
	Retryable bool   `json:"retryable"`
	Outcome   string `json:"outcome"`
	Action    string `json:"action"`
}

func decodeSendFailure(t *testing.T, result any) sendErrorResult {
	t.Helper()
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "private") {
		t.Fatalf("tool error exposed private diagnostics: %s", encoded)
	}
	var tool struct {
		Success bool `json:"success"`
		Items   []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"contentItems"`
	}
	if err := json.Unmarshal(encoded, &tool); err != nil {
		t.Fatal(err)
	}
	if tool.Success || len(tool.Items) != 1 || tool.Items[0].Type != "inputText" {
		t.Fatalf("unexpected failed tool response: %s", encoded)
	}
	var failure sendErrorResult
	if err := json.Unmarshal([]byte(tool.Items[0].Text), &failure); err != nil {
		t.Fatalf("tool failure was not structured JSON: %v", err)
	}
	return failure
}

func sendCall(t *testing.T, threadID, turnID string, args any) json.RawMessage {
	t.Helper()
	encoded, err := json.Marshal(map[string]any{
		"threadId": threadID, "turnId": turnID, "tool": "observer_send_message", "arguments": args,
	})
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
