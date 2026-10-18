package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"agent-observer/internal/domain"
)

func TestAdmittedBoundaryRecordCanRetryAndReopen(t *testing.T) {
	destination := Destination{ID: "bounded", Exporter: exportFunc(func(context.Context, []domain.Observation) error { return errors.New("temporary") })}
	box := newTestOutbox(t, destination)
	// Search the actual serialized boundary, including JSON escaping and identity.
	low, high := 0, maxEntryBytes
	for low < high {
		mid := (low + high + 1) / 2
		o := testObservation(strings.Repeat("x", mid))
		raw, _ := json.Marshal(o)
		_, err := encodeEntry(o.Key(), record{Observation: o, Fingerprint: digest(raw)})
		if err == nil {
			low = mid
		} else {
			high = mid - 1
		}
	}
	o := testObservation(strings.Repeat("x", low))
	submit(t, box, o)
	if err := box.Submit([]domain.Observation{testObservation(strings.Repeat("x", low+1))}); err == nil {
		t.Fatal("oversize admission succeeded")
	}
	if len(box.sinks[0].records) != 1 {
		t.Fatal("rejection changed pending records")
	}
	box.flush(context.Background(), box.sinks[0])
	r := box.sinks[0].records[o.Key()]
	if r.Attempts != 1 || r.NextAttempt.IsZero() {
		t.Fatal("retry not saved")
	}
	r.Attempts = maxAttempts
	r.NextAttempt = time.Time{}
	if err := box.save(box.sinks[0], o.Key(), r); err != nil {
		t.Fatal(err)
	}
	box.flush(context.Background(), box.sinks[0])
	if box.sinks[0].records[o.Key()].Attempts != maxAttempts {
		t.Fatal("attempt counter overflowed")
	}
	if err := box.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(box.directory, []Destination{destination})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.Status()[0].Pending != 1 {
		t.Fatal("accepted pending record lost on restart")
	}
	if err := reopened.Submit([]domain.Observation{o}); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(reopened.sinkDirectory(reopened.sinks[0]))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() > maxEntryBytes {
			t.Fatal("unreadable entry written")
		}
	}
}

func TestOutboxRejectsEscapedOversizeWithoutPoisoningRestart(t *testing.T) {
	d := Destination{ID: "escape", Exporter: exportFunc(func(context.Context, []domain.Observation) error { return nil })}
	b := newTestOutbox(t, d)
	o := testObservation(strings.Repeat("<", maxEntryBytes/2))
	if err := b.Submit([]domain.Observation{o}); err == nil {
		t.Fatal("JSON expansion escaped size guard")
	}
	b.Close()
	reopened, err := New(b.directory, []Destination{d})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.Status()[0].Pending != 0 {
		t.Fatal("rejected record survived")
	}
}

func TestOutboxCommittedReceiptIsNotExportedTwiceAfterSyncFailure(t *testing.T) {
	calls := 0
	d := Destination{ID: "sync", Exporter: exportFunc(func(context.Context, []domain.Observation) error { calls++; return nil })}
	b := newTestOutbox(t, d)
	o := testObservation("one")
	submit(t, b, o)
	syncDir := b.fs.SyncDir
	b.fs.SyncDir = func(string) error { return errors.New("injected") }
	b.flush(context.Background(), b.sinks[0])
	if b.Status()[0].Accepted != 1 || !b.sinks[0].syncPending {
		t.Fatal("committed receipt lost")
	}
	b.flush(context.Background(), b.sinks[0])
	if calls != 1 {
		t.Fatal("uncertain receipt exported again")
	}
	b.fs.SyncDir = syncDir
	if err := b.Submit([]domain.Observation{o}); err != nil {
		t.Fatal(err)
	}
	if b.sinks[0].syncPending {
		t.Fatal("directory sync not repaired")
	}
	b.Close()
	reopened, err := New(b.directory, []Destination{d})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopened.flush(context.Background(), reopened.sinks[0])
	if calls != 1 {
		t.Fatal("receipt exported after restart")
	}
}

func TestLoadEntryReadBound(t *testing.T) {
	d := Destination{ID: "load", Exporter: exportFunc(func(context.Context, []domain.Observation) error { return nil })}
	b := newTestOutbox(t, d)
	dir := b.sinkDirectory(b.sinks[0])
	b.Close()
	if err := os.WriteFile(filepath.Join(dir, "large.json"), []byte(strings.Repeat("x", maxEntryBytes+1)), 0600); err != nil {
		t.Fatal(err)
	}
	if reopened, err := New(b.directory, []Destination{d}); err == nil {
		reopened.Close()
		t.Fatal("oversize disk record loaded")
	}
}
