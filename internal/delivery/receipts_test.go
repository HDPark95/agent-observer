package delivery

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"agent-observer/internal/domain"
)

type dxPartialResult struct{}

func (dxPartialResult) Error() string             { return "partial acceptance" }
func (dxPartialResult) Retryable() bool           { return false }
func (dxPartialResult) RetryDelay() time.Duration { return 0 }
func (dxPartialResult) PartialAcceptance() (int64, bool) {
	return 1, true
}

func dxReopen(t *testing.T, box *Outbox, d Destination) *Outbox {
	t.Helper()
	if err := box.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(box.directory, []Destination{d})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	return reopened
}

func dxStatus(t *testing.T, box *Outbox, pending, accepted, rejected, partial int) {
	t.Helper()
	status := box.Status()
	if len(status) != 1 {
		t.Fatalf("destinations=%d, want 1", len(status))
	}
	got := status[0]
	if got.Pending != pending || got.Accepted != accepted ||
		got.Rejected != rejected || got.PartialUnknown != partial {
		t.Fatalf("status=%+v; want pending=%d accepted=%d rejected=%d partial=%d",
			got, pending, accepted, rejected, partial)
	}
	if len(box.sinks[0].records) != pending {
		t.Fatalf("payload records=%d, want pending count %d",
			len(box.sinks[0].records), pending)
	}
	if len(box.sinks[0].receipts) != accepted+rejected+partial {
		t.Fatalf("receipt count=%d, want %d",
			len(box.sinks[0].receipts), accepted+rejected+partial)
	}
}

func dxEntryPath(box *Outbox, o domain.Observation) string {
	return filepath.Join(
		box.sinkDirectory(box.sinks[0]),
		digest([]byte(o.Key()))+".json",
	)
}

func dxFingerprint(t *testing.T, o domain.Observation) string {
	t.Helper()
	data, err := json.Marshal(o)
	if err != nil {
		t.Fatal(err)
	}
	return digest(data)
}

func dxWriteJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func dxReadReceipt(t *testing.T, box *Outbox, o domain.Observation) receipt {
	t.Helper()
	data, err := os.ReadFile(dxEntryPath(box, o))
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	if _, exists := fields["observation"]; exists {
		t.Fatal("terminal receipt retained an observation payload")
	}
	if len(data) > 512 {
		t.Fatalf("terminal receipt is not compact: %d bytes", len(data))
	}
	var got receipt
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.Version != 1 || got.Key != o.Key() ||
		got.Fingerprint != dxFingerprint(t, o) {
		t.Fatalf("receipt identity mismatch: %+v", got)
	}
	return got
}

func TestDXPartialAcceptanceRestartDedupAndChangedContent(t *testing.T) {
	calls := 0
	var sent [][]domain.Observation
	d := Destination{
		ID: "partial-regression",
		Exporter: exportFunc(func(_ context.Context, batch []domain.Observation) error {
			calls++
			sent = append(sent, append([]domain.Observation(nil), batch...))
			if calls == 1 {
				return dxPartialResult{}
			}
			return nil
		}),
	}
	box := newTestOutbox(t, d)
	a, b := testObservation("a"), testObservation("b")
	submit(t, box, a, b)
	box.flush(context.Background(), box.sinks[0])

	dxStatus(t, box, 0, 0, 0, 2)
	ra := dxReadReceipt(t, box, a)
	rb := dxReadReceipt(t, box, b)
	for _, r := range []receipt{ra, rb} {
		if r.Accepted || r.Rejected || r.Partial == nil {
			t.Fatalf("partial response invented individual outcome: %+v", r)
		}
		if r.Partial.RejectedSpans != 1 || !validDigest(r.Partial.ID) {
			t.Fatalf("invalid partial-batch metadata: %+v", r.Partial)
		}
	}
	if ra.Partial.ID != rb.Partial.ID {
		t.Fatal("observations from one partial batch have different batch IDs")
	}

	// No replay within the same process, including after duplicate collection.
	submit(t, box, a, b)
	box.flush(context.Background(), box.sinks[0])
	if calls != 1 {
		t.Fatalf("partial batch replayed: calls=%d", calls)
	}

	box = dxReopen(t, box, d)
	dxStatus(t, box, 0, 0, 0, 2)
	restored := dxReadReceipt(t, box, a)
	if restored.Partial == nil || *restored.Partial != *ra.Partial {
		t.Fatal("restart lost partial-batch metadata")
	}
	submit(t, box, a, b)
	box.flush(context.Background(), box.sinks[0])
	if calls != 1 {
		t.Fatalf("restart replayed partial batch: calls=%d", calls)
	}

	// A changed observation is a new delivery; the other remains unknown.
	a.Outcome = "error"
	submit(t, box, a)
	dxStatus(t, box, 1, 0, 0, 1)
	box.flush(context.Background(), box.sinks[0])
	dxStatus(t, box, 0, 1, 0, 1)
	if calls != 2 || len(sent[1]) != 1 ||
		sent[1][0].ID != a.ID || sent[1][0].Outcome != "error" {
		t.Fatalf("changed delivery: calls=%d sent=%+v", calls, sent)
	}
	changed := dxReadReceipt(t, box, a)
	if !changed.Accepted || changed.Rejected || changed.Partial != nil {
		t.Fatalf("changed observation retained stale partial outcome: %+v", changed)
	}

	box = dxReopen(t, box, d)
	submit(t, box, a, b)
	box.flush(context.Background(), box.sinks[0])
	dxStatus(t, box, 0, 1, 0, 1)
	if calls != 2 {
		t.Fatalf("updated receipt failed restart dedup: calls=%d", calls)
	}
}

func TestDXTerminalReceiptsBeyondFormerLimitDoNotConsumeCapacity(t *testing.T) {
	const terminalCount = 100001
	delivered := 0
	d := Destination{
		ID: "terminal-capacity",
		Exporter: exportFunc(func(_ context.Context, batch []domain.Observation) error {
			delivered += len(batch)
			return nil
		}),
	}
	box := newTestOutbox(t, d)
	if err := box.Close(); err != nil {
		t.Fatal(err)
	}

	// Actual disk fixtures exercise startup capacity, not just an in-memory map.
	// Fixture writes intentionally avoid per-file fsync; production save is
	// exercised by the new and changed submissions below.
	for i := 0; i < terminalCount; i++ {
		o := testObservation("terminal-" + strconv.Itoa(i))
		r := record{
			Observation: o,
			Fingerprint: dxFingerprint(t, o),
			Accepted:    true,
		}
		dxWriteJSON(t, dxEntryPath(box, o), compactReceipt(o.Key(), r))
	}
	box = dxReopen(t, box, d)
	dxStatus(t, box, 0, terminalCount, 0, 0)

	old := testObservation("terminal-0")
	fresh := testObservation("fresh-after-limit")
	submit(t, box, old, fresh)
	dxStatus(t, box, 1, terminalCount, 0, 0)
	box.flush(context.Background(), box.sinks[0])
	dxStatus(t, box, 0, terminalCount+1, 0, 0)
	if delivered != 1 {
		t.Fatalf("delivered=%d, want only the new observation", delivered)
	}
	dxReadReceipt(t, box, fresh)

	// An existing terminal identity also needs pending capacity when changed.
	old.Outcome = "error"
	submit(t, box, old)
	dxStatus(t, box, 1, terminalCount, 0, 0)
	box.flush(context.Background(), box.sinks[0])
	dxStatus(t, box, 0, terminalCount+1, 0, 0)
	if delivered != 2 {
		t.Fatalf("delivered=%d, want new and changed observations", delivered)
	}
	dxReadReceipt(t, box, old)

	box = dxReopen(t, box, d)
	submit(t, box, old, fresh)
	box.flush(context.Background(), box.sinks[0])
	dxStatus(t, box, 0, terminalCount+1, 0, 0)
	if delivered != 2 {
		t.Fatalf("restart lost terminal dedup: delivered=%d", delivered)
	}
}

func TestDXLegacyTerminalMigrationCompactsAndRetainsPending(t *testing.T) {
	delivered := 0
	d := Destination{
		ID: "legacy-migration",
		Exporter: exportFunc(func(_ context.Context, batch []domain.Observation) error {
			delivered += len(batch)
			return nil
		}),
	}
	box := newTestOutbox(t, d)
	if err := box.Close(); err != nil {
		t.Fatal(err)
	}
	accepted := testObservation("legacy-accepted")
	rejected := testObservation("legacy-rejected")
	rejected.ObservedAt = time.Time{}
	pending := testObservation("legacy-pending")

	for _, r := range []record{
		{Observation: accepted, Accepted: true, Attempts: 3},
		{Observation: rejected, Rejected: true},
		{Observation: pending, Attempts: 2},
	} {
		r.Fingerprint = dxFingerprint(t, r.Observation)
		// Marshal record directly: these must be legacy full-payload files.
		dxWriteJSON(t, dxEntryPath(box, r.Observation), r)
	}

	box = dxReopen(t, box, d)
	dxStatus(t, box, 1, 1, 1, 0)
	ar := dxReadReceipt(t, box, accepted)
	rr := dxReadReceipt(t, box, rejected)
	if !ar.Accepted || ar.Rejected || ar.Partial != nil ||
		!rr.Rejected || rr.Accepted || rr.Partial != nil {
		t.Fatalf("migration changed outcomes: accepted=%+v rejected=%+v", ar, rr)
	}
	pr, exists := box.sinks[0].records[pending.Key()]
	if !exists || pr.Observation != pending || pr.Attempts != 2 {
		t.Fatalf("migration lost pending payload or attempts: %+v", pr)
	}
	data, err := os.ReadFile(dxEntryPath(box, pending))
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	if _, exists := fields["observation"]; !exists {
		t.Fatal("migration compacted an undelivered observation")
	}

	// Reopen the migrated files before delivery to prove format compatibility.
	box = dxReopen(t, box, d)
	submit(t, box, accepted, rejected, pending)
	box.flush(context.Background(), box.sinks[0])
	dxStatus(t, box, 0, 2, 1, 0)
	if delivered != 1 {
		t.Fatalf("migration replayed terminal observations: delivered=%d", delivered)
	}
	dxReadReceipt(t, box, pending)

	rejected.ObservedAt = accepted.ObservedAt
	submit(t, box, rejected)
	dxStatus(t, box, 1, 2, 0, 0)
	box.flush(context.Background(), box.sinks[0])
	dxStatus(t, box, 0, 3, 0, 0)
	if delivered != 2 {
		t.Fatalf("corrected rejected observation not delivered: delivered=%d", delivered)
	}

	box = dxReopen(t, box, d)
	submit(t, box, accepted, rejected, pending)
	box.flush(context.Background(), box.sinks[0])
	dxStatus(t, box, 0, 3, 0, 0)
	if delivered != 2 {
		t.Fatalf("migrated receipts failed restart dedup: delivered=%d", delivered)
	}
}

// Start from the post-rename state, then fail and recover actual directory
// confirmation. This checks that local uncertainty never re-exports the receipt.
func TestReceiptDirectoryConfirmationDoesNotReplay(t *testing.T) {
	calls := 0
	destination := Destination{ID: "receipt-sync", Exporter: exportFunc(func(context.Context, []domain.Observation) error { calls++; return nil })}
	box := newTestOutbox(t, destination)
	observation := testObservation("accepted")
	submit(t, box, observation)
	box.flush(context.Background(), box.sinks[0])
	sink := box.sinks[0]
	sink.syncPending = true
	dir := box.sinkDirectory(sink)
	moved := dir + "-unavailable"
	if err := os.Rename(dir, moved); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := os.Stat(moved); err == nil {
			_ = os.Rename(moved, dir)
		}
	})
	if err := box.Submit([]domain.Observation{observation}); err == nil {
		t.Fatal("unconfirmed receipt reported durable success")
	}
	box.flush(context.Background(), sink)
	if calls != 1 || !sink.syncPending || len(sink.records) != 0 {
		t.Fatal("unconfirmed receipt reverted or was re-exported")
	}
	if err := os.Rename(moved, dir); err != nil {
		t.Fatal(err)
	}
	submit(t, box, observation)
	box.flush(context.Background(), sink)
	if calls != 1 || sink.syncPending {
		t.Fatal("confirmation retried remote delivery")
	}
	reopened := dxReopen(t, box, destination)
	submit(t, reopened, observation)
	reopened.flush(context.Background(), reopened.sinks[0])
	if calls != 1 {
		t.Fatal("receipt replayed after restart")
	}
}
