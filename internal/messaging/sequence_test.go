package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestManagedFIFOIgnoresWallClock(t *testing.T) {
	for _, clockChange := range []time.Duration{0, -time.Hour} {
		t.Run(clockChange.String(), func(t *testing.T) {
			f := managedFixture(t)
			now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
			f.store.now = func() time.Time { return now }
			want := []string{"z-first", "a-second"}
			for _, id := range want {
				request := managedRequest()
				request.ID = id
				if _, err := f.service.Send(context.Background(), f.sender, request); err != nil {
					t.Fatal(err)
				}
				now = now.Add(clockChange)
			}
			reopened := reopenSequenceStore(t, f.store)
			for _, id := range want {
				claimAndComplete(t, reopened, recipientPrincipal(f), id)
			}
			if _, ok, err := reopened.ClaimManaged(context.Background(), recipientPrincipal(f)); err != nil || ok {
				t.Fatalf("terminal queue claim = %v, %v; want empty", ok, err)
			}
		})
	}
}

func TestManagedConcurrentEnqueuesFollowStoreSerialization(t *testing.T) {
	f := managedFixture(t)
	ctx := context.Background()
	base := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	var accepted uint64
	// now runs under the store lock. Its counter records the actual enqueue
	// serialization independently of goroutine start/return order or message ID.
	f.store.now = func() time.Time {
		accepted++
		return base.Add(-time.Duration(accepted) * time.Second)
	}
	const total = 24
	var wg sync.WaitGroup
	start := make(chan struct{})
	errCh := make(chan error, total)
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			request := managedRequest()
			request.ID = fmt.Sprintf("message-%02d", total-i)
			_, err := f.service.Send(ctx, f.sender, request)
			errCh <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	want := make(map[uint64]string, total)
	for _, stored := range f.store.state.Messages {
		order := uint64(base.Sub(stored.Message.AcceptedAt) / time.Second)
		if stored.Sequence != order {
			t.Fatalf("message %s sequence = %d; serialized enqueue = %d", stored.Message.ID, stored.Sequence, order)
		}
		want[order] = stored.Message.ID
	}
	if f.store.state.LastSequence != total || len(want) != total {
		t.Fatalf("concurrent enqueues: high-water mark %d, distinct orders %d; want %d each", f.store.state.LastSequence, len(want), total)
	}
	for sequence := uint64(1); sequence <= total; sequence++ {
		claimAndComplete(t, f.store, recipientPrincipal(f), want[sequence])
	}
}

func TestLegacyMigrationPreservesRecordsAndContinuesSequence(t *testing.T) {
	f := managedFixture(t)
	ctx := context.Background()
	now := time.Now().UTC().Add(-AckRetention)
	f.store.now = func() time.Time { return now }
	mailboxRequest := testRequest()
	if _, err := f.service.Send(ctx, f.sender, mailboxRequest); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.Ack(ctx, f.receiver, mailboxRequest.ID); err != nil {
		t.Fatal(err)
	}
	now = now.Add(AckRetention)
	// Build the old redacted representation directly: v1 had no archive.
	key := messageKey(f.principal.SessionID, f.principal.Turn, mailboxRequest.ID)
	redacted := f.store.state.Messages[key]
	redacted.Message.Body, redacted.Message.BodyExpiredAt = "", &now
	f.store.state.Messages[key] = redacted
	f.store.state.Revisions[scopeKey(f.principal.SessionID, f.principal.Turn)]++
	for _, id := range []string{"completed", "z-queued", "a-queued"} {
		request := managedRequest()
		request.ID = id
		if _, err := f.service.Send(ctx, f.sender, request); err != nil {
			t.Fatal(err)
		}
		if id == "completed" {
			claimAndComplete(t, f.store, recipientPrincipal(f), id)
		}
	}
	legacy := f.store.copyState()
	legacy.Version = 1
	legacy.CapacityBytes = 0
	legacy.LastSequence = 0
	for key, stored := range legacy.Messages {
		stored.Sequence = 0
		legacy.Messages[key] = stored
	}
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	writeSequenceState(t, f.store.directory, legacy)
	reopened, err := Open(f.store.directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	persisted := readSequenceState(t, reopened.directory)
	if persisted.Version != storeVersion || persisted.LastSequence != 4 {
		t.Fatalf("migration did not persist before Open returned: version %d, high-water mark %d", persisted.Version, persisted.LastSequence)
	}
	if !reflect.DeepEqual(persisted.Capabilities, legacy.Capabilities) || !reflect.DeepEqual(persisted.Revisions, legacy.Revisions) {
		t.Fatal("migration changed capability or revision records")
	}
	for key, before := range legacy.Messages {
		after, found, err := reopened.lookupMessage(key)
		if err != nil || !found {
			t.Fatalf("migrated receipt %s missing: %v", key, err)
		}
		after.Sequence = 0
		if !reflect.DeepEqual(after, before) {
			t.Fatalf("migration changed record %s", before.Message.ID)
		}
	}
	// Legacy records retain their former timestamp/ID order. A new record must
	// remain after them even when its clock precedes every old record.
	reopened.now = func() time.Time { return now.Add(-60 * 24 * time.Hour) }
	service := NewService(reopened, map[string]Transport{MailboxTransport: reopened.Mailbox(), ManagedTransport: reopened.Managed()})
	request := managedRequest()
	request.ID = "new-append"
	if _, err := service.Send(ctx, f.sender, request); err != nil {
		t.Fatal(err)
	}
	if reopened.state.LastSequence != 5 {
		t.Fatalf("appended sequence high-water mark = %d; want 5", reopened.state.LastSequence)
	}
	for _, id := range []string{"a-queued", "z-queued", "new-append"} {
		claimAndComplete(t, reopened, recipientPrincipal(f), id)
	}
	request.ID = "completed"
	if message, err := service.Send(ctx, f.sender, request); err != nil || message.Delivery.State != "completed" {
		t.Fatalf("completed retry after migration = %+v, %v", message.Delivery, err)
	}
	if message, err := service.Send(ctx, f.sender, mailboxRequest); err != nil || message.BodyExpiredAt == nil || message.Body != "" {
		t.Fatalf("expired retry after migration = %+v, %v", message, err)
	}
	if reopened.state.LastSequence != 5 {
		t.Fatal("idempotent retry consumed an enqueue sequence")
	}
	again := reopenSequenceStore(t, reopened)
	if again.state.LastSequence != 5 {
		t.Fatal("restart reset enqueue high-water mark")
	}
	if _, ok, err := again.ClaimManaged(ctx, recipientPrincipal(f)); err != nil || ok {
		t.Fatalf("completed record replayed after migration/reopen: %v, %v", ok, err)
	}
	request.ID = "after-restart"
	if _, err := again.Managed().Send(ctx, f.principal, request); err != nil {
		t.Fatal(err)
	}
	if again.state.LastSequence != 6 {
		t.Fatalf("post-restart sequence = %d; want 6", again.state.LastSequence)
	}
}

func TestRejectsInvalidSequencesWithoutReplacingRecords(t *testing.T) {
	tests := map[string]func(*diskState){
		"zero":                 func(state *diskState) { setStoredSequence(state, "message-a", 0) },
		"duplicate":            func(state *diskState) { setStoredSequence(state, "message-b", 1) },
		"above-high-water":     func(state *diskState) { state.LastSequence = 1 },
		"legacy-with-sequence": func(state *diskState) { state.Version, state.LastSequence = 1, 0 },
		"legacy-with-high-water": func(state *diskState) {
			state.Version = 1
			state.CapacityBytes = 0
			setStoredSequence(state, "message-a", 0)
			setStoredSequence(state, "message-b", 0)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			f := managedFixture(t)
			for _, id := range []string{"message-a", "message-b"} {
				request := managedRequest()
				request.ID = id
				if _, err := f.service.Send(context.Background(), f.sender, request); err != nil {
					t.Fatal(err)
				}
			}
			state := f.store.copyState()
			if err := f.store.Close(); err != nil {
				t.Fatal(err)
			}
			mutate(&state)
			before := writeSequenceState(t, f.store.directory, state)
			assertSequenceOpenRejected(t, f.store.directory, before)
		})
	}
	for _, invalid := range []string{"-1", "18446744073709551616"} {
		t.Run(invalid, func(t *testing.T) {
			f := managedFixture(t)
			if _, err := f.service.Send(context.Background(), f.sender, managedRequest()); err != nil {
				t.Fatal(err)
			}
			if err := f.store.Close(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(f.store.directory, "mailbox.json")
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			corrupt := []byte(strings.Replace(string(raw), `"sequence":1`, `"sequence":`+invalid, 1))
			if err := os.WriteFile(path, corrupt, 0600); err != nil {
				t.Fatal(err)
			}
			assertSequenceOpenRejected(t, f.store.directory, corrupt)
		})
	}
}

func TestSequenceExhaustionDoesNotWrapOrBreakRetry(t *testing.T) {
	f := managedFixture(t)
	ctx := context.Background()
	request := managedRequest()
	if _, err := f.service.Send(ctx, f.sender, request); err != nil {
		t.Fatal(err)
	}
	state := f.store.copyState()
	state.LastSequence = math.MaxUint64
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	writeSequenceState(t, f.store.directory, state)
	reopened, err := Open(f.store.directory)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.Managed().Send(ctx, f.principal, request); err != nil {
		t.Fatalf("retry at sequence exhaustion: %v", err)
	}
	request.ID = "overflow"
	if _, err := reopened.Managed().Send(ctx, f.principal, request); !errors.Is(err, ErrCapacity) {
		t.Fatalf("sequence overflow error = %v; want capacity", err)
	}
	if reopened.state.LastSequence != math.MaxUint64 || len(reopened.state.Messages) != 1 {
		t.Fatal("exhausted sequence wrapped or appended a record")
	}
}

func TestFailedEnqueueDoesNotConsumeSequence(t *testing.T) {
	f := managedFixture(t)
	ctx := context.Background()
	request := managedRequest()
	if _, err := f.service.Send(ctx, f.sender, request); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(f.store.directory, "mailbox.json")
	if err := os.Chmod(path, 0400); err != nil {
		t.Fatal(err)
	}
	request.ID = "next-message"
	if _, err := f.service.Send(ctx, f.sender, request); err == nil {
		t.Fatal("enqueue succeeded with invalid store mode")
	}
	if f.store.state.LastSequence != 1 || len(f.store.state.Messages) != 1 {
		t.Fatal("failed enqueue changed sequence or records")
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.Send(ctx, f.sender, request); err != nil {
		t.Fatal(err)
	}
	stored := f.store.state.Messages[messageKey(f.principal.SessionID, f.principal.Turn, request.ID)]
	if stored.Sequence != 2 || f.store.state.LastSequence != 2 {
		t.Fatalf("enqueue after recovery sequence = %d, high-water mark = %d; want 2", stored.Sequence, f.store.state.LastSequence)
	}
}

func claimAndComplete(t *testing.T, store *Store, principal Principal, wantID string) {
	t.Helper()
	ctx := context.Background()
	message, ok, err := store.ClaimManaged(ctx, principal)
	if err != nil || !ok || message.ID != wantID {
		t.Fatalf("claim = %q, %v, %v; want %q", message.ID, ok, err, wantID)
	}
	if err := store.UpdateDelivery(ctx, principal, message.ID, Delivery{State: "completed", ThreadID: "thread-one", TurnID: "turn-one"}); err != nil {
		t.Fatal(err)
	}
}

func reopenSequenceStore(t *testing.T, store *Store) *Store {
	t.Helper()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(store.directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	return reopened
}

func writeSequenceState(t *testing.T, directory string, state diskState) []byte {
	t.Helper()
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "mailbox.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	return raw
}

func readSequenceState(t *testing.T, directory string) diskState {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(directory, "mailbox.json"))
	if err != nil {
		t.Fatal(err)
	}
	var state diskState
	if err := json.Unmarshal(raw, &state); err != nil {
		t.Fatal(err)
	}
	return state
}

func setStoredSequence(state *diskState, id string, sequence uint64) {
	key := messageKey("session-a", "turn-a", id)
	stored := state.Messages[key]
	stored.Sequence = sequence
	state.Messages[key] = stored
}

func assertSequenceOpenRejected(t *testing.T, directory string, before []byte) {
	t.Helper()
	if store, err := Open(directory); err == nil {
		store.Close()
		t.Fatal("invalid sequence state accepted")
	}
	after, err := os.ReadFile(filepath.Join(directory, "mailbox.json"))
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatal("invalid sequence state was overwritten")
	}
}
