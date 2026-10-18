package workers

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"agent-observer/internal/messaging"
)

func TestSendArgumentsUseDomainValidation(t *testing.T) {
	for _, tc := range []struct {
		name, raw string
		valid     bool
	}{
		{"unicode", `{"message_id":"one","target":"managed:ux","body":"한🙂"}`, true},
		{"blank", `{"message_id":"one","target":"managed:ux","body":" "}`, false},
		{"control", `{"message_id":"one","target":"managed:ux","body":"a\u0000"}`, false},
		{"path", `{"message_id":"one","target":"../ux","body":"a"}`, false},
		{"extra", `{"message_id":"one","target":"managed:ux","body":"a","sender":"agent"}`, false},
		{"trailing", `{"message_id":"one","target":"managed:ux","body":"a"} {}`, false},
		{"invalid-utf8", `{"message_id":"one","target":"managed:ux","body":"` + string([]byte{0xff}) + `"}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeSendArguments([]byte(tc.raw))
			if (err == nil) != tc.valid {
				t.Fatalf("validation=%v", err)
			}
		})
	}
	raw, _ := json.Marshal(map[string]string{"message_id": "one", "target": "managed:ux", "body": strings.Repeat("a", messaging.MaxBodyRunes+1)})
	if _, err := DecodeSendArguments(raw); !errors.Is(err, messaging.ErrInvalid) {
		t.Fatal("oversize tool body accepted")
	}
}
func TestSendFailureUncertaintyWinsAndNeverLeaksDetails(t *testing.T) {
	for _, tc := range []struct {
		err           error
		code, outcome string
	}{
		{errors.Join(messaging.ErrCapacity, messaging.ErrOutcomeUnknown), "outcome_unknown", "unknown"},
		{errors.Join(messaging.ErrInvalid, context.Canceled), "send_interrupted", "unknown"},
		{messaging.ErrConflict, "id_conflict", "rejected"},
		{errors.New("PRIVATE unexpected detail"), "send_failed", "unknown"},
	} {
		got := DescribeSendFailure(tc.err)
		if got.Code != tc.code || got.Outcome != tc.outcome || strings.Contains(got.Action, "PRIVATE") {
			t.Fatalf("failure=%+v", got)
		}
	}
}
