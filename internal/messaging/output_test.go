package messaging

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestOutputEvidenceSurvivesArchiveAndRestart(t *testing.T) {
	for _, tc := range []struct {
		name, text string
		state      OutputState
		reason     string
	}{
		{"legacy", "legacy text", "", ""},
		{"complete", "full result", OutputComplete, ""},
		{"truncated", "partial result", OutputTruncated, "display-limit"},
		{"omitted", "", OutputOmitted, "no-final-text"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := managedFixture(t)
			ctx := context.Background()
			req := managedRequest()
			if _, err := f.service.Send(ctx, f.sender, req); err != nil {
				t.Fatal(err)
			}
			if _, ok, err := f.store.ClaimManaged(ctx, recipientPrincipal(f)); err != nil || !ok {
				t.Fatal("claim", err)
			}
			delivery := Delivery{Provider: "codex", State: "completed", ThreadID: "thread", TurnID: "turn", Output: tc.text, OutputState: tc.state, OutputReason: tc.reason}
			if err := f.store.UpdateDelivery(ctx, recipientPrincipal(f), req.ID, delivery); err != nil {
				t.Fatal(err)
			}
			before, _, err := f.store.Snapshot(f.principal.SessionID, f.principal.Turn)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.store.Prune(ctx); err != nil {
				t.Fatal(err)
			}
			reopened := reopenSequenceStore(t, f.store)
			after, _, err := reopened.Snapshot(f.principal.SessionID, f.principal.Turn)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("output evidence changed on restart: %v", err)
			}
		})
	}
}

func TestDeliveryRejectsContradictoryOutputEvidence(t *testing.T) {
	base := Delivery{State: "completed", ThreadID: "thread", TurnID: "turn", UpdatedAt: time.Now()}
	for _, d := range []Delivery{
		func() Delivery { d := base; d.OutputState = "invented"; return d }(),
		func() Delivery { d := base; d.OutputState = OutputComplete; d.OutputReason = "display-limit"; return d }(),
		func() Delivery {
			d := base
			d.OutputState = OutputOmitted
			d.OutputReason = "no-final-text"
			d.Output = "not omitted"
			return d
		}(),
		func() Delivery { d := base; d.OutputState = OutputTruncated; return d }(),
		func() Delivery { d := base; d.OutputReason = "display-limit"; return d }(),
		func() Delivery { d := base; d.State = "running"; d.OutputState = OutputComplete; return d }(),
	} {
		if validDelivery(d) {
			t.Fatalf("contradiction accepted: %+v", d)
		}
	}
}
