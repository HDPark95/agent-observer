package codexworker

import (
	"strings"
	"testing"

	"agent-observer/internal/messaging"
	"agent-observer/internal/workers"
)

func TestFinalResultPresenceAndLargeOutputPreserveCompletion(t *testing.T) {
	for _, tc := range []struct {
		name, text string
		seen       bool
	}{{"missing", "", false}, {"empty", "", true}, {"large", strings.Repeat("한", 50000), true}} {
		t.Run(tc.name, func(t *testing.T) {
			prior := workers.Receipt{ConversationID: "session", ExecutionID: "turn"}
			if tc.seen {
				prior = captureFinal(prior, publicItem{Type: "agentMessage", Phase: "final_answer", Text: tc.text})
			}
			// Completion frames sometimes omit already-emitted items.
			got, err := completedReceipt(prior, turn{Status: "completed"})
			if err != nil || got.Output != tc.text || got.ConversationID != "session" || got.ExecutionID != "turn" {
				t.Fatal("output or acceptance lost")
			}
			want := messaging.OutputOmitted
			if tc.seen {
				want = messaging.OutputComplete
			}
			if got.OutputState != want {
				t.Fatalf("state=%q", got.OutputState)
			}
			if got2 := captureFinal(got, publicItem{Type: "agentMessage", Phase: "commentary", Text: "not final"}); got2 != got {
				t.Fatal("commentary replaced final")
			}
		})
	}
}
