package messaging

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// These fixtures start after a rename has committed, with durability awaiting
// confirmation. They do not inject or claim to reproduce a kernel failure
// between rename and fsync. Retry failures and recovery use the real filesystem
// and real directory fsync, with no replacement persistence functions.
func TestPendingDurabilitySendRetryDoesNotDuplicate(t *testing.T) {
	f := newFixture(t)
	first, err := f.service.Send(context.Background(), f.sender, testRequest())
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(f.store.directory, "mailbox.json"))
	if err != nil {
		t.Fatal(err)
	}
	f.store.syncPending = true
	changes := f.store.Changes()
	restore := makeDirectoryUnavailable(t, f.store.directory)

	for i := 0; i < 2; i++ {
		if _, err := f.service.Send(context.Background(), f.sender, testRequest()); !errors.Is(err, ErrOutcomeUnknown) {
			t.Fatalf("unconfirmed send retry error = %v; want outcome unknown", err)
		}
		if !f.store.syncPending {
			t.Fatal("failed directory sync cleared the pending state")
		}
	}
	second := testRequest()
	second.ID = "must-not-commit"
	if _, err := f.service.Send(context.Background(), f.sender, second); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("new send while durability is unconfirmed = %v; want outcome unknown", err)
	}
	if _, err := f.service.Ack(context.Background(), f.receiver, first.ID); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("new Ack while durability is unconfirmed = %v; want outcome unknown", err)
	}
	assertSingleUnacknowledgedMessage(t, f, first)
	select {
	case <-changes:
		t.Fatal("failed durability confirmation notified a graph mutation")
	default:
	}

	restore()
	retry, err := f.service.Send(context.Background(), f.sender, testRequest())
	if err != nil || !reflect.DeepEqual(retry, first) {
		t.Fatalf("confirmed retry = %v, %v; want original receipt", retry, err)
	}
	if f.store.syncPending {
		t.Fatal("successful directory fsync did not clear the pending state")
	}
	assertSingleUnacknowledgedMessage(t, f, first)
	after, err := os.ReadFile(filepath.Join(f.store.directory, "mailbox.json"))
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatal("confirmation retry rewrote the committed queue")
	}
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(f.store.directory)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restored := fixture{store: reopened, service: NewService(reopened, map[string]Transport{MailboxTransport: reopened.Mailbox()}), sender: f.sender, receiver: f.receiver, principal: f.principal}
	assertSingleUnacknowledgedMessage(t, restored, first)
}

func TestPendingDurabilityAckRetryPreservesReceipt(t *testing.T) {
	f := newFixture(t)
	if _, err := f.service.Send(context.Background(), f.sender, testRequest()); err != nil {
		t.Fatal(err)
	}
	acked, err := f.service.Ack(context.Background(), f.receiver, testRequest().ID)
	if err != nil {
		t.Fatal(err)
	}
	f.store.syncPending = true
	changes := f.store.Changes()
	restore := makeDirectoryUnavailable(t, f.store.directory)
	if _, err := f.service.Ack(context.Background(), f.receiver, acked.ID); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("unconfirmed Ack retry error = %v; want outcome unknown", err)
	}
	if !f.store.syncPending {
		t.Fatal("failed Ack confirmation cleared the pending state")
	}
	snapshot, revision, _ := f.store.Snapshot("session-a", "turn-a")
	if len(snapshot) != 1 || revision != 2 || !reflect.DeepEqual(snapshot[0], acked) {
		t.Fatalf("failed Ack confirmation changed receipt: %v, revision %d", snapshot, revision)
	}
	restore()
	retry, err := f.service.Ack(context.Background(), f.receiver, acked.ID)
	if err != nil || !reflect.DeepEqual(retry, acked) {
		t.Fatalf("confirmed Ack retry = %v, %v; want original Ack", retry, err)
	}
	if f.store.syncPending {
		t.Fatal("successful Ack confirmation did not clear the pending state")
	}
	if inbox, err := f.service.Receive(context.Background(), f.receiver); err != nil || len(inbox) != 0 {
		t.Fatalf("Ack retry requeued a message: %v, %v", inbox, err)
	}
	select {
	case <-changes:
		t.Fatal("Ack confirmation changed the graph revision")
	default:
	}
}

func TestCloseReportsPendingDurabilityFailureAndReopenRecovers(t *testing.T) {
	f := newFixture(t)
	first, err := f.service.Send(context.Background(), f.sender, testRequest())
	if err != nil {
		t.Fatal(err)
	}
	f.store.syncPending = true
	restore := makeDirectoryUnavailable(t, f.store.directory)
	if err := f.store.Close(); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("Close error = %v; want outcome unknown", err)
	}
	if _, err := f.store.Authenticate(f.sender); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed store authentication error = %v; want closed", err)
	}
	restore()
	reopened, err := Open(f.store.directory)
	if err != nil {
		t.Fatalf("reopen after unconfirmed Close: %v", err)
	}
	defer reopened.Close()
	service := NewService(reopened, map[string]Transport{MailboxTransport: reopened.Mailbox()})
	retry, err := service.Send(context.Background(), f.sender, testRequest())
	if err != nil || !reflect.DeepEqual(retry, first) {
		t.Fatalf("reopened retry = %v, %v; want original receipt", retry, err)
	}
	assertSingleUnacknowledgedMessage(t, fixture{store: reopened, service: service, receiver: f.receiver}, first)
}

func makeDirectoryUnavailable(t *testing.T, directory string) func() {
	t.Helper()
	moved := directory + "-unavailable"
	if err := os.Rename(directory, moved); err != nil {
		t.Fatal(err)
	}
	restored := false
	restore := func() {
		t.Helper()
		if restored {
			return
		}
		if err := os.Rename(moved, directory); err != nil {
			t.Fatalf("restore temporary messaging directory: %v", err)
		}
		restored = true
	}
	t.Cleanup(restore)
	return restore
}

func assertSingleUnacknowledgedMessage(t *testing.T, f fixture, want Message) {
	t.Helper()
	inbox, err := f.service.Receive(context.Background(), f.receiver)
	if err != nil || len(inbox) != 1 || !reflect.DeepEqual(inbox[0], want) {
		t.Fatalf("inbox after durability retry = %v, %v; want original message once", inbox, err)
	}
	snapshot, revision, _ := f.store.Snapshot("session-a", "turn-a")
	if len(snapshot) != 1 || revision != 1 || !reflect.DeepEqual(snapshot[0], want) {
		t.Fatalf("snapshot after durability retry = %v, revision %d; want original revision 1", snapshot, revision)
	}
}

// Unlike the pending-state fixtures above, this test injects an instance-owned
// failure at the actual post-rename directory-sync boundary.
func TestAtomicCommitSyncFailureRetainsQueueAndRetriesIdentically(t *testing.T) {
	f := newFixture(t)
	syncDir := f.store.fs.SyncDir
	f.store.fs.SyncDir = func(string) error { return errors.New("injected directory sync") }
	_, err := f.service.Send(context.Background(), f.sender, testRequest())
	if !errors.Is(err, ErrOutcomeUnknown) || len(f.store.state.Messages) != 1 || !f.store.syncPending {
		t.Fatalf("commit was rolled back: %v", err)
	}
	f.store.fs.SyncDir = syncDir
	retry, err := f.service.Send(context.Background(), f.sender, testRequest())
	if err != nil || retry.ID != testRequest().ID || len(f.store.state.Messages) != 1 || f.store.syncPending {
		t.Fatal("identical retry lost or duplicated commit", err)
	}
	reopened := reopenSequenceStore(t, f.store)
	messages, _, err := reopened.Snapshot(f.principal.SessionID, f.principal.Turn)
	if err != nil || len(messages) != 1 || messages[0].ID != retry.ID {
		t.Fatal("committed queue lost on restart", err)
	}
}
