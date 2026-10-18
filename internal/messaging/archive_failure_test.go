package messaging

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestArchiveExpirationAdoptsRenameAndNotifiesOnSyncFailure(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	now := time.Now().UTC()
	f.store.now = func() time.Time { return now }
	for _, id := range []string{"first", "second"} {
		req := testRequest()
		req.ID = id
		if _, err := f.service.Send(ctx, f.sender, req); err != nil {
			t.Fatal(err)
		}
		if _, err := f.service.Ack(ctx, f.receiver, id); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.store.Prune(ctx); err != nil {
		t.Fatal(err)
	}
	changes := f.store.Changes()
	now = now.Add(AckRetention + time.Second)
	// Store-local fault injection covers the boundary after a real rename and
	// before successful directory fsync; it does not substitute the test runner.
	confirmations := 0
	f.store.syncArchiveDirectory = func(dir string) error {
		if filepath.Base(dir) == "receipts" {
			confirmations++
			if confirmations >= 2 {
				return errors.New("injected sync failure")
			}
		}
		return syncDirectory(dir)
	}
	if err := f.store.Prune(ctx); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("sync failure: %v", err)
	}
	select {
	case <-changes:
	default:
		t.Fatal("adopted expiration did not notify observers after a later error")
	}
	if len(f.store.archiveSync) != 1 {
		t.Fatal("unconfirmed directory not retained")
	}
	now = now.Add(-AckRetention - time.Second)
	messages, _, err := f.store.Snapshot(f.principal.SessionID, f.principal.Turn)
	if err != nil || len(messages) != 2 {
		t.Fatalf("snapshot: %+v %v", messages, err)
	}
	for _, m := range messages {
		if m.BodyExpiredAt == nil || m.Body != "" {
			t.Fatal("clock rollback restored content after committed expiration")
		}
	}
	if err := f.store.confirmDurability(); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("sync uncertainty forgotten: %v", err)
	}
	f.store.syncArchiveDirectory = syncDirectory
	if err := f.store.Prune(ctx); err != nil {
		t.Fatal(err)
	}
	if len(f.store.archiveSync) != 0 {
		t.Fatal("directory recovery was not confirmed")
	}
	reopened := reopenSequenceStore(t, f.store)
	messages, _, err = reopened.Snapshot(f.principal.SessionID, f.principal.Turn)
	if err != nil || len(messages) != 2 || messages[0].BodyExpiredAt == nil || messages[1].BodyExpiredAt == nil {
		t.Fatalf("expiration lost on restart: %+v %v", messages, err)
	}
}

func TestArchiveDuplicateRecoversBeforeActiveRemoval(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		name := "same-instant"
		if conflict {
			name = "changed-output"
		}
		t.Run(name, func(t *testing.T) {
			f := managedFixture(t)
			ctx := context.Background()
			req := managedRequest()
			if _, err := f.service.Send(ctx, f.sender, req); err != nil {
				t.Fatal(err)
			}
			claimAndComplete(t, f.store, recipientPrincipal(f), req.ID)
			key := messageKey(f.principal.SessionID, f.principal.Turn, req.ID)
			stored := f.store.state.Messages[key]
			// A different location object is semantically the same instant.
			stored.Message.AcceptedAt = stored.Message.AcceptedAt.In(time.FixedZone("zero", 0))
			stored.Message.Delivery.UpdatedAt = stored.Message.Delivery.UpdatedAt.In(time.FixedZone("zero", 0))
			f.store.state.Messages[key] = stored
			path := filepath.Join(f.store.directory, "mailbox.json")
			if err := os.Chmod(path, 0400); err != nil {
				t.Fatal(err)
			}
			if err := f.store.Prune(ctx); err == nil {
				t.Fatal("active removal unexpectedly succeeded")
			}
			if len(f.store.archive.messages) != 1 || len(f.store.state.Messages) != 1 {
				t.Fatal("fixture did not reach active/archive duplicate boundary")
			}
			if err := os.Chmod(path, 0600); err != nil {
				t.Fatal(err)
			}
			if conflict {
				state := f.store.copyState()
				changed := state.Messages[key]
				changed.Message = cloneMessage(changed.Message)
				changed.Message.Delivery.Output = "conflicting output"
				state.Messages[key] = changed
				if err := f.store.commit(ctx, state, false); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(f.store.directory)
			if conflict {
				if err == nil {
					reopened.Close()
					t.Fatal("conflicting archive silently accepted")
				}
				return
			}
			if err != nil {
				t.Fatalf("equal-instant receipt rejected: %v", err)
			}
			defer reopened.Close()
			if len(reopened.state.Messages) != 0 || len(reopened.archive.messages) != 1 {
				t.Fatal("interrupted compaction did not recover")
			}
			if _, ok, err := reopened.ClaimManaged(ctx, recipientPrincipal(f)); err != nil || ok {
				t.Fatalf("terminal work replayed: %v %v", ok, err)
			}
		})
	}
}
