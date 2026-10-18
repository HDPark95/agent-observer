package messaging

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestArchivePreservesReceiptBodyAuthorityAndTeamStart(t *testing.T) {
	f := managedFixture(t)
	ctx := context.Background()
	request := managedRequest()
	if _, err := f.service.Send(ctx, f.sender, request); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := f.store.ClaimManaged(ctx, recipientPrincipal(f)); err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	if err := f.store.UpdateDelivery(ctx, recipientPrincipal(f), request.ID, Delivery{Provider: "codex", State: "completed", ThreadID: "original-thread", TurnID: "original-turn", Output: "보관할 완료 결과"}); err != nil {
		t.Fatal(err)
	}
	before, rev, err := f.store.Snapshot(f.principal.SessionID, f.principal.Turn)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.Prune(ctx); err != nil {
		t.Fatal(err)
	}
	if len(f.store.state.Messages) != 0 || len(f.store.state.Capabilities) != 0 || len(f.store.state.Revisions) != 0 {
		t.Fatal("terminal scope still consumes active capacity")
	}
	if after, gotRev, err := f.store.Snapshot(f.principal.SessionID, f.principal.Turn); err != nil || gotRev != rev || !reflect.DeepEqual(before, after) {
		t.Fatalf("archive changed visible state: %v rev=%d err=%v", after, gotRev, err)
	}
	f.store.now = func() time.Time { return time.Now().Add(365 * 24 * time.Hour) }
	if err := f.store.Prune(ctx); err != nil {
		t.Fatal(err)
	}
	if after, _, err := f.store.Snapshot(f.principal.SessionID, f.principal.Turn); err != nil || !reflect.DeepEqual(after, before) {
		t.Fatal("archival changed managed conversation retention")
	}
	f.store.now = time.Now

	// An unused enrollment must remain valid after subsequent compaction.
	token, err := f.store.Enroll(ctx, Principal{SessionID: "another-session", Turn: "another-turn", AgentID: "agent:unused"})
	if err != nil {
		t.Fatal(err)
	}
	if err = f.store.Prune(ctx); err != nil {
		t.Fatal(err)
	}
	reopened := reopenSequenceStore(t, f.store)
	if _, err := reopened.Authenticate(token); err != nil {
		t.Fatalf("unused capability lost: %v", err)
	}
	if _, err := reopened.Authenticate(f.sender); err != nil {
		t.Fatalf("sender capability lost: %v", err)
	}
	retry, found, err := reopened.FindTeamStart(ctx, f.principal.SessionID, f.principal.AgentID, request)
	if err != nil || !found || !reflect.DeepEqual(retry, before[0]) {
		t.Fatalf("cross-turn receipt retry: %+v %v %v", retry, found, err)
	}
	service := NewService(reopened, map[string]Transport{ManagedTransport: reopened.Managed()})
	if retry, err = service.Send(ctx, f.sender, request); err != nil || !reflect.DeepEqual(retry, before[0]) {
		t.Fatalf("archived retry: %+v %v", retry, err)
	}
	changed := request
	changed.Body += " changed"
	if _, err = service.Send(ctx, f.sender, changed); !errors.Is(err, ErrConflict) {
		t.Fatalf("different body accepted: %v", err)
	}
	request.ID = "next-message"
	if _, err = service.Send(ctx, f.sender, request); err != nil {
		t.Fatal(err)
	}
	claimAndComplete(t, reopened, recipientPrincipal(f), request.ID)
	if reopened.state.LastSequence != 2 {
		t.Fatal("archived message replay consumed an enqueue sequence")
	}
}

func TestArchiveExpirationIsDurableAndAdvancesRevision(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	now := time.Now().UTC()
	f.store.now = func() time.Time { return now }
	req := testRequest()
	if _, err := f.service.Send(ctx, f.sender, req); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.Ack(ctx, f.receiver, req.ID); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Prune(ctx); err != nil {
		t.Fatal(err)
	}
	_, before, err := f.store.Snapshot(f.principal.SessionID, f.principal.Turn)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(AckRetention + time.Second)
	if err := f.store.Prune(ctx); err != nil {
		t.Fatal(err)
	}
	messages, after, err := f.store.Snapshot(f.principal.SessionID, f.principal.Turn)
	if err != nil || after != before+1 || len(messages) != 1 || messages[0].BodyExpiredAt == nil || messages[0].Body != "" {
		t.Fatalf("expiry: %+v %d %v", messages, after, err)
	}
	key := messageKey(f.principal.SessionID, f.principal.Turn, req.ID)
	if _, err := os.Stat(f.store.archivePath("payloads", key)); !os.IsNotExist(err) {
		t.Fatalf("expired payload still present: %v", err)
	}
	// Reopening uses the real clock, before the artificial expiration above.
	reopened := reopenSequenceStore(t, f.store)
	messages, revision, err := reopened.Snapshot(f.principal.SessionID, f.principal.Turn)
	if err != nil || revision != after || messages[0].BodyExpiredAt == nil {
		t.Fatalf("clock rollback restored content: %+v %d %v", messages, revision, err)
	}
	if retry, err := NewService(reopened, map[string]Transport{MailboxTransport: reopened.Mailbox()}).Send(ctx, f.sender, req); err != nil || retry.Body != "" || retry.BodyExpiredAt == nil {
		t.Fatalf("retry rehydrated content: %+v %v", retry, err)
	}
}

func TestArchiveFailurePreservesActiveRecordAndPendingWork(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	req := testRequest()
	if _, err := f.service.Send(ctx, f.sender, req); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.Ack(ctx, f.receiver, req.ID); err != nil {
		t.Fatal(err)
	}
	pending := req
	pending.ID = "pending"
	if _, err := f.service.Send(ctx, f.sender, pending); err != nil {
		t.Fatal(err)
	}
	receipts := filepath.Join(f.store.directory, "archive", "receipts")
	if err := os.Chmod(receipts, 0500); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Prune(ctx); err == nil {
		t.Fatal("archive succeeded with invalid receipt directory")
	}
	if len(f.store.state.Messages) != 2 {
		t.Fatal("failed archive removed active data")
	}
	if err := os.Chmod(receipts, 0700); err != nil {
		t.Fatal(err)
	}
	// Failed receipt write left a payload without a receipt; restart cleans it
	// before retrying compaction from the authoritative active snapshot.
	reopened := reopenSequenceStore(t, f.store)
	if len(reopened.state.Messages) != 1 || len(reopened.archive.messages) != 1 {
		t.Fatal("pending work was archived or terminal work was lost")
	}
	service := NewService(reopened, map[string]Transport{MailboxTransport: reopened.Mailbox()})
	inbox, err := service.Receive(ctx, f.receiver)
	if err != nil || len(inbox) != 1 || inbox[0].ID != pending.ID {
		t.Fatalf("pending inbox: %+v %v", inbox, err)
	}
}

func TestArchivedMissingOrCorruptPayloadFailsClosed(t *testing.T) {
	for _, mode := range []string{"missing", "changed"} {
		t.Run(mode, func(t *testing.T) {
			f := newFixture(t)
			ctx := context.Background()
			req := testRequest()
			if _, err := f.service.Send(ctx, f.sender, req); err != nil {
				t.Fatal(err)
			}
			if _, err := f.service.Ack(ctx, f.receiver, req.ID); err != nil {
				t.Fatal(err)
			}
			if err := f.store.Prune(ctx); err != nil {
				t.Fatal(err)
			}
			path := f.store.archivePath("payloads", messageKey(f.principal.SessionID, f.principal.Turn, req.ID))
			original, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "missing" {
				err = os.Remove(path)
			} else {
				err = os.WriteFile(path, []byte(`{"body":"changed"}`), 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := f.store.Snapshot(f.principal.SessionID, f.principal.Turn); err == nil {
				t.Fatal("corrupt history silently displayed")
			}
			if _, err := f.service.Send(ctx, f.sender, req); err == nil {
				t.Fatal("corrupt retry created or returned a message")
			}
			if len(f.store.state.Messages) != 0 {
				t.Fatal("retry queued a duplicate")
			}
			if err := os.WriteFile(path, original, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := f.service.Send(ctx, f.sender, req); err != nil {
				t.Fatalf("repaired archive not readable: %v", err)
			}
		})
	}
}

// Seed validated state directly to exercise production-size capacity boundaries
// without 10,000 unrelated fsyncs during fixture setup.
func populatedState(t *testing.T, f fixture, count int, managed bool) diskState {
	t.Helper()
	state := f.store.copyState()
	now := time.Now().UTC()
	for i := 1; i <= count; i++ {
		req := testRequest()
		if managed {
			req = managedRequest()
		}
		req.ID = fmt.Sprintf("capacity-%05d", i)
		if req.Transport == "" {
			req.Transport = MailboxTransport
		}
		m := Message{ID: req.ID, SessionID: f.principal.SessionID, Turn: f.principal.Turn, Source: f.principal.AgentID, Target: req.Target, Body: req.Body, Transport: req.Transport, AcceptedAt: now}
		if managed {
			m.Delivery = &Delivery{State: "queued", UpdatedAt: now}
		}
		state.Messages[messageKey(m.SessionID, m.Turn, m.ID)] = storedMessage{Message: m, Fingerprint: requestFingerprint(f.principal, req), Sequence: uint64(i)}
	}
	state.LastSequence = uint64(count)
	state.Revisions[scopeKey(f.principal.SessionID, f.principal.Turn)] = uint64(count)
	if !validState(state) {
		t.Fatal("invalid capacity fixture")
	}
	return state
}

func TestTerminalRecordsDoNotConsumeLifetimeMessageCapacity(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	state := populatedState(t, f, MaxMessages, false)
	key := messageKey(f.principal.SessionID, f.principal.Turn, "capacity-00001")
	stored := state.Messages[key]
	at := stored.Message.AcceptedAt
	stored.Message.AckedAt = &at
	state.Messages[key] = stored
	if err := f.store.commit(ctx, state, false); err != nil {
		t.Fatal(err)
	}
	req := testRequest()
	req.ID = "after-limit"
	if _, err := f.service.Send(ctx, f.sender, req); err != nil {
		t.Fatalf("terminal history blocked admission: %v", err)
	}
	if len(f.store.state.Messages) != MaxMessages || len(f.store.archive.messages) != 1 {
		t.Fatal("compaction removed pending work or retained terminal capacity")
	}
	req.ID = "capacity-00001"
	if retry, err := f.service.Send(ctx, f.sender, req); err != nil || retry.AckedAt == nil {
		t.Fatalf("receipt retry lost: %+v %v", retry, err)
	}
	req.ID = "still-full"
	if _, err := f.service.Send(ctx, f.sender, req); !errors.Is(err, ErrCapacity) {
		t.Fatalf("active capacity bypassed: %v", err)
	}
	reopened := reopenSequenceStore(t, f.store)
	if len(reopened.state.Messages) != MaxMessages || reopened.state.LastSequence != MaxMessages+1 {
		t.Fatal("restart altered active capacity or sequence")
	}
}

func TestLegacyReservationFinishesAcceptedWorkButRejectsNewAdmission(t *testing.T) {
	f := managedFixture(t)
	ctx := context.Background()
	state := populatedState(t, f, 500, true)
	if err := checkAdmission(state); !errors.Is(err, ErrCapacity) {
		t.Fatalf("fixture must exceed reserved budget: %v", err)
	}
	state.Version = 2
	state.CapacityBytes = 0
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	writeSequenceState(t, f.store.directory, state)
	reopened := reopenSequenceStore(t, f.store)
	if reopened.state.CapacityBytes <= maxStoreBytes {
		t.Fatal("accepted legacy work did not receive a migration allowance")
	}
	req := managedRequest()
	req.ID = "new-admission"
	if _, err := reopened.Managed().Send(ctx, f.principal, req); !errors.Is(err, ErrCapacity) {
		t.Fatalf("new admission consumed reserved space: %v", err)
	}
	if _, err := reopened.Enroll(ctx, Principal{SessionID: "new-session", Turn: "new-turn", AgentID: "agent:new"}); !errors.Is(err, ErrCapacity) {
		t.Fatalf("enrollment consumed reserved space: %v", err)
	}
	message, ok, err := reopened.ClaimManaged(ctx, recipientPrincipal(f))
	if err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	completion := Delivery{Provider: "codex", State: "completed", ThreadID: strings.Repeat("t", 256), TurnID: strings.Repeat("u", 256), Output: strings.Repeat("<", MaxBodyRunes)}
	if err := reopened.UpdateDelivery(ctx, recipientPrincipal(f), message.ID, completion); err != nil {
		t.Fatalf("accepted work could not finish with maximally escaped output: %v", err)
	}
	saved := readSequenceState(t, reopened.directory)
	if !validState(saved) || saved.Messages[messageKey(f.principal.SessionID, f.principal.Turn, message.ID)].Message.Delivery.Output != completion.Output {
		t.Fatal("completion was not persisted")
	}
}
