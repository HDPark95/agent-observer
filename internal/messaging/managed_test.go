package messaging

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func managedFixture(t *testing.T) fixture {
	t.Helper()
	f := newFixture(t)
	f.service = NewService(f.store, map[string]Transport{MailboxTransport: f.store.Mailbox(), ManagedTransport: f.store.Managed()})
	return f
}

func managedRequest() Request {
	request := testRequest()
	request.Transport = ManagedTransport
	return request
}

func recipientPrincipal(f fixture) Principal {
	principal := f.principal
	principal.AgentID = testRequest().Target
	return principal
}

func TestManagedQueueClaimAndCompletion(t *testing.T) {
	f := managedFixture(t)
	ctx := context.Background()
	principal := recipientPrincipal(f)
	sent, err := f.service.Send(ctx, f.sender, managedRequest())
	if err != nil {
		t.Fatal(err)
	}
	if sent.Delivery == nil || sent.Delivery.State != "queued" || sent.AckedAt != nil {
		t.Fatalf("new managed message = %+v; want queued without mailbox Ack", sent)
	}
	if inbox, err := f.service.Receive(ctx, f.receiver); err != nil || len(inbox) != 0 {
		t.Fatalf("mailbox received managed message: %v, %v", inbox, err)
	}
	if _, err := f.service.Ack(ctx, f.receiver, sent.ID); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("mailbox Ack of managed message error = %v; want unsupported", err)
	}
	changed := f.store.Changes()
	claimed, ok, err := f.store.ClaimManaged(ctx, principal)
	if err != nil || !ok || claimed.ID != sent.ID || claimed.Delivery.State != "dispatching" {
		t.Fatalf("claim = %+v, %v, %v; want sent message dispatching", claimed, ok, err)
	}
	select {
	case <-changed:
	default:
		t.Fatal("claim did not notify observers")
	}
	running := Delivery{State: "running", ThreadID: "thread-one", TurnID: "turn-one"}
	if err := f.store.UpdateDelivery(ctx, principal, sent.ID, running); err != nil {
		t.Fatal(err)
	}
	completed := running
	completed.State = "completed"
	completed.Output = "SSE 화면 반영을 확인했습니다."
	if err := f.store.UpdateDelivery(ctx, principal, sent.ID, completed); err != nil {
		t.Fatal(err)
	}
	snapshot, revision, _ := f.store.Snapshot(principal.SessionID, principal.Turn)
	if len(snapshot) != 1 || revision != 4 || snapshot[0].Delivery.State != "completed" || snapshot[0].Delivery.Output != completed.Output || snapshot[0].AckedAt != nil {
		t.Fatalf("completed snapshot = %+v, revision %d", snapshot, revision)
	}
	if _, ok, err := f.store.ClaimManaged(ctx, principal); err != nil || ok {
		t.Fatalf("completed message claimed again: %v, %v", ok, err)
	}
	if err := f.store.UpdateDelivery(ctx, principal, sent.ID, running); !errors.Is(err, ErrInvalid) {
		t.Fatalf("completed -> running error = %v; want invalid", err)
	}
	// Snapshots must not expose the live state's delivery pointer.
	snapshot[0].Delivery.State = "queued"
	again, _, _ := f.store.Snapshot(principal.SessionID, principal.Turn)
	if again[0].Delivery.State != "completed" || claimed.Delivery.State != "dispatching" || sent.Delivery.State != "queued" {
		t.Fatal("delivery snapshot aliased mutable state")
	}
}

func TestManagedClaimsSerializeRecipientInQueueOrder(t *testing.T) {
	f := managedFixture(t)
	ctx := context.Background()
	principal := recipientPrincipal(f)
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	f.store.now = func() time.Time { return now }
	for _, id := range []string{"message-first", "message-second"} {
		request := managedRequest()
		request.ID = id
		if _, err := f.service.Send(ctx, f.sender, request); err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Second)
	}
	var wg sync.WaitGroup
	claims := make(chan Message, 8)
	errorsSeen := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			message, ok, err := f.store.ClaimManaged(ctx, principal)
			if err != nil {
				errorsSeen <- err
			}
			if ok {
				claims <- message
			}
		}()
	}
	wg.Wait()
	close(claims)
	close(errorsSeen)
	for err := range errorsSeen {
		t.Error(err)
	}
	var got []Message
	for message := range claims {
		got = append(got, message)
	}
	if len(got) != 1 || got[0].ID != "message-first" {
		t.Fatalf("concurrent claims = %+v; want only oldest message", got)
	}
	if err := f.store.UpdateDelivery(ctx, principal, got[0].ID, Delivery{State: "completed", ThreadID: "thread-one", TurnID: "turn-one"}); err != nil {
		t.Fatal(err)
	}
	message, ok, err := f.store.ClaimManaged(ctx, principal)
	if err != nil || !ok || message.ID != "message-second" {
		t.Fatalf("next claim = %+v, %v, %v; want second message", message, ok, err)
	}
}

func TestManagedRecoveryIsScopedAndDurable(t *testing.T) {
	f := managedFixture(t)
	ctx := context.Background()
	principal := recipientPrincipal(f)
	other := principal
	other.Turn = "turn-other"
	otherToken := enroll(t, f.store, other)
	for _, item := range []struct {
		token     string
		principal Principal
	}{{f.receiver, principal}, {otherToken, other}} {
		request := managedRequest()
		if _, err := f.service.Send(ctx, item.token, request); err != nil {
			t.Fatal(err)
		}
		if _, ok, err := f.store.ClaimManaged(ctx, item.principal); err != nil || !ok {
			t.Fatalf("claim = %v, %v", ok, err)
		}
	}
	if err := f.store.UpdateDelivery(ctx, principal, "message-one", Delivery{State: "running", ThreadID: "thread-one", TurnID: "turn-one"}); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(f.store.directory)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	changed := reopened.Changes()
	if err := reopened.RecoverManaged(ctx, principal.SessionID, principal.Turn); err != nil {
		t.Fatal(err)
	}
	select {
	case <-changed:
	default:
		t.Fatal("recovery did not notify observers")
	}
	snapshot, revision, _ := reopened.Snapshot(principal.SessionID, principal.Turn)
	if len(snapshot) != 1 || revision != 4 || snapshot[0].Delivery.State != "unknown" || snapshot[0].Delivery.ThreadID != "thread-one" || snapshot[0].Delivery.Error != "interrupted-delivery" {
		t.Fatalf("recovery snapshot = %+v, revision %d", snapshot, revision)
	}
	otherSnapshot, otherRevision, _ := reopened.Snapshot(other.SessionID, other.Turn)
	if len(otherSnapshot) != 1 || otherRevision != 2 || otherSnapshot[0].Delivery.State != "dispatching" {
		t.Fatalf("recovery changed other scope = %+v, revision %d", otherSnapshot, otherRevision)
	}
	if _, ok, err := reopened.ClaimManaged(ctx, principal); err != nil || ok {
		t.Fatalf("recovered uncertain message was requeued: %v, %v", ok, err)
	}
	if err := reopened.RecoverManaged(ctx, principal.SessionID, principal.Turn); err != nil {
		t.Fatal(err)
	}
	_, againRevision, _ := reopened.Snapshot(principal.SessionID, principal.Turn)
	if againRevision != revision {
		t.Fatal("repeated recovery mutated an unknown receipt")
	}
	service := NewService(reopened, map[string]Transport{ManagedTransport: reopened.Managed()})
	again, err := service.Send(ctx, f.receiver, managedRequest())
	if err != nil || !reflect.DeepEqual(again, snapshot[0]) {
		t.Fatalf("duplicate after reopen = %+v, %v; want same unknown message", again, err)
	}
}

func TestManagedValidationAndReceiptBinding(t *testing.T) {
	f := managedFixture(t)
	ctx := context.Background()
	principal := recipientPrincipal(f)
	request := managedRequest()
	if _, err := f.service.Send(ctx, f.sender, request); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := f.store.ClaimManaged(ctx, principal); err != nil || !ok {
		t.Fatalf("claim = %v, %v", ok, err)
	}
	if err := f.store.UpdateDelivery(ctx, f.principal, request.ID, Delivery{State: "unknown"}); !errors.Is(err, ErrForbidden) {
		t.Fatalf("sender changed delivery: %v; want forbidden", err)
	}
	running := Delivery{State: "running", ThreadID: "thread-one", TurnID: "turn-one"}
	for _, invalid := range []Delivery{
		{State: "queued"},
		{State: "completed"},
		{State: "running", ThreadID: "../thread", TurnID: "turn-one"},
		{State: "unknown", Error: "Provider failed with secret=abc"},
		{State: "completed", ThreadID: "thread-one", TurnID: "turn-one", Output: strings.Repeat("x", MaxBodyBytes+1)},
		{State: "completed", ThreadID: "thread-one", TurnID: "turn-one", Output: "invalid\x00text"},
	} {
		if err := f.store.UpdateDelivery(ctx, principal, request.ID, invalid); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid delivery accepted: state=%s error=%v", invalid.State, err)
		}
	}
	if err := f.store.UpdateDelivery(ctx, principal, request.ID, running); err != nil {
		t.Fatal(err)
	}
	for _, changed := range []Delivery{
		{State: "completed", ThreadID: "thread-other", TurnID: "turn-one"},
		{State: "unknown"},
		{State: "completed", ThreadID: "thread-one", TurnID: "turn-other"},
	} {
		if err := f.store.UpdateDelivery(ctx, principal, request.ID, changed); !errors.Is(err, ErrInvalid) {
			t.Fatalf("receipt replaced: %v", err)
		}
	}
	unknown := running
	unknown.State = "unknown"
	unknown.Error = "connection-lost"
	if err := f.store.UpdateDelivery(ctx, principal, request.ID, unknown); err != nil {
		t.Fatal(err)
	}
	if err := f.store.UpdateDelivery(ctx, principal, request.ID, running); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unknown -> running error = %v; want invalid", err)
	}
}

func TestManagedPersistenceFailureDoesNotClaim(t *testing.T) {
	f := managedFixture(t)
	ctx := context.Background()
	principal := recipientPrincipal(f)
	if _, err := f.service.Send(ctx, f.sender, managedRequest()); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(f.store.directory, "mailbox.json")
	if err := os.Chmod(path, 0400); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := f.store.ClaimManaged(ctx, principal); err == nil || ok {
		t.Fatalf("claim with invalid store mode = %v, %v; want no claim", ok, err)
	}
	snapshot, revision, _ := f.store.Snapshot(principal.SessionID, principal.Turn)
	if len(snapshot) != 1 || revision != 1 || snapshot[0].Delivery.State != "queued" {
		t.Fatal("failed persistence changed queued state")
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := f.store.ClaimManaged(ctx, principal); err != nil || !ok {
		t.Fatalf("claim after restoring storage = %v, %v", ok, err)
	}
}

func TestManagedAndMailboxReopenTogether(t *testing.T) {
	f := managedFixture(t)
	ctx := context.Background()
	if _, err := f.service.Send(ctx, f.sender, testRequest()); err != nil {
		t.Fatal(err)
	}
	request := managedRequest()
	request.ID = "managed-message"
	if _, err := f.service.Send(ctx, f.sender, request); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(f.store.directory)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	snapshot, revision, _ := reopened.Snapshot(f.principal.SessionID, f.principal.Turn)
	if len(snapshot) != 2 || revision != 2 {
		t.Fatalf("reopened mixed store has %d messages, revision %d", len(snapshot), revision)
	}
	message, ok, err := reopened.ClaimManaged(ctx, recipientPrincipal(f))
	if err != nil || !ok || message.ID != request.ID {
		t.Fatalf("mixed store claim = %+v, %v, %v", message, ok, err)
	}
}

func TestManagedTerminalRetrySurvivesReopen(t *testing.T) {
	for _, terminal := range []string{"completed", "unknown"} {
		t.Run(terminal, func(t *testing.T) {
			f := managedFixture(t)
			ctx := context.Background()
			principal := recipientPrincipal(f)
			if _, err := f.service.Send(ctx, f.sender, managedRequest()); err != nil {
				t.Fatal(err)
			}
			if _, ok, err := f.store.ClaimManaged(ctx, principal); err != nil || !ok {
				t.Fatalf("claim = %v, %v", ok, err)
			}
			if terminal == "unknown" {
				if err := f.store.RecoverManaged(ctx, principal.SessionID, principal.Turn); err != nil {
					t.Fatal(err)
				}
			} else if err := f.store.UpdateDelivery(ctx, principal, "message-one", Delivery{State: terminal, ThreadID: "thread-one", TurnID: "turn-one", Output: "done"}); err != nil {
				t.Fatal(err)
			}
			if err := f.store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(f.store.directory)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			service := NewService(reopened, map[string]Transport{ManagedTransport: reopened.Managed()})
			message, err := service.Send(ctx, f.sender, managedRequest())
			if err != nil || message.Delivery.State != terminal {
				t.Fatalf("retry state = %+v, %v; want %s", message.Delivery, err, terminal)
			}
			if _, ok, err := reopened.ClaimManaged(ctx, principal); err != nil || ok {
				t.Fatalf("terminal retry claimed = %v, %v; want no claim", ok, err)
			}
		})
	}
}

func TestManagedClaimAndRecoveryDoNotCrossSessions(t *testing.T) {
	f := managedFixture(t)
	ctx := context.Background()
	principal := recipientPrincipal(f)
	other := principal
	other.SessionID = "other-session"
	otherToken := enroll(t, f.store, other)
	if _, err := f.service.Send(ctx, otherToken, managedRequest()); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := f.store.ClaimManaged(ctx, principal); err != nil || ok {
		t.Fatalf("cross-session claim = %v, %v; want no claim", ok, err)
	}
	if err := f.store.UpdateDelivery(ctx, principal, "message-one", Delivery{State: "unknown"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-session update error = %v; want not found", err)
	}
	if _, ok, err := f.store.ClaimManaged(ctx, other); err != nil || !ok {
		t.Fatalf("other-session claim = %v, %v", ok, err)
	}
	if err := f.store.RecoverManaged(ctx, principal.SessionID, principal.Turn); err != nil {
		t.Fatal(err)
	}
	snapshot, revision, _ := f.store.Snapshot(other.SessionID, other.Turn)
	if len(snapshot) != 1 || revision != 2 || snapshot[0].Delivery.State != "dispatching" {
		t.Fatal("recovery changed a different session")
	}
}
