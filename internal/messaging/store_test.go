package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type fixture struct {
	store     *Store
	service   *Service
	sender    string
	receiver  string
	principal Principal
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "mailbox"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	principal := Principal{SessionID: "session-a", Turn: "turn-a", AgentID: "subagent:/root/ux"}
	receiver := principal
	receiver.AgentID = "subagent:/root/dx"
	return fixture{store: store, service: NewService(store, map[string]Transport{MailboxTransport: store.Mailbox()}), sender: enroll(t, store, principal), receiver: enroll(t, store, receiver), principal: principal}
}

func enroll(t *testing.T, store *Store, principal Principal) string {
	t.Helper()
	token, err := store.Enroll(context.Background(), principal)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func testRequest() Request {
	return Request{ID: "message-one", Target: "subagent:/root/dx", Body: "화면 동작을 확인했습니다.\n메시지함의 답장을 기다립니다."}
}

func TestSendReceiveAckAndObservationAreIndependent(t *testing.T) {
	f := newFixture(t)
	changed := f.store.Changes()
	sent, err := f.service.Send(context.Background(), f.sender, testRequest())
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-changed:
	default:
		t.Fatal("send did not notify observers")
	}
	snapshot, revision, _ := f.store.Snapshot("session-a", "turn-a")
	if len(snapshot) != 1 || revision != 1 || !reflect.DeepEqual(snapshot[0], sent) {
		t.Fatalf("snapshot = %v, revision = %d; want one matching message and revision 1", snapshot, revision)
	}
	// Reading the graph or queue is not an Ack, even when repeated.
	for i := 0; i < 2; i++ {
		inbox, err := f.service.Receive(context.Background(), f.receiver)
		if err != nil || len(inbox) != 1 || inbox[0].AckedAt != nil {
			t.Fatalf("receive = %v, %v; want one unacknowledged message", inbox, err)
		}
	}
	if _, err := f.service.Ack(context.Background(), f.sender, sent.ID); !errors.Is(err, ErrForbidden) {
		t.Fatalf("sender Ack error = %v; want forbidden", err)
	}
	acked, err := f.service.Ack(context.Background(), f.receiver, sent.ID)
	if err != nil || acked.AckedAt == nil {
		t.Fatalf("recipient Ack = %v, %v", acked, err)
	}
	again, err := f.service.Ack(context.Background(), f.receiver, sent.ID)
	if err != nil || !reflect.DeepEqual(again, acked) {
		t.Fatalf("Ack retry changed the receipt: %v, %v", again, err)
	}
	inbox, err := f.service.Receive(context.Background(), f.receiver)
	if err != nil || len(inbox) != 0 {
		t.Fatalf("acked inbox length = %d, error %v; want empty", len(inbox), err)
	}
	_, revision, _ = f.store.Snapshot("session-a", "turn-a")
	if revision != 2 {
		t.Fatalf("revision after Ack retry = %d; want 2", revision)
	}
}

func TestSendIdempotencyAndConflict(t *testing.T) {
	f := newFixture(t)
	request := testRequest()
	first, err := f.service.Send(context.Background(), f.sender, request)
	if err != nil {
		t.Fatal(err)
	}
	changed := f.store.Changes()
	request.Transport = MailboxTransport
	again, err := f.service.Send(context.Background(), f.sender, request)
	if err != nil || !reflect.DeepEqual(again, first) {
		t.Fatalf("same-content retry = %v, %v; want original receipt", again, err)
	}
	select {
	case <-changed:
		t.Fatal("idempotent retry changed observation")
	default:
	}
	request.Body = "different body"
	if _, err := f.service.Send(context.Background(), f.sender, request); !errors.Is(err, ErrConflict) {
		t.Fatalf("different-content retry error = %v; want conflict", err)
	}
	if _, err := f.service.Send(context.Background(), f.receiver, testRequest()); !errors.Is(err, ErrConflict) {
		t.Fatalf("different-sender ID reuse error = %v; want conflict", err)
	}
	inbox, _ := f.service.Receive(context.Background(), f.receiver)
	if len(inbox) != 1 || inbox[0].Body != first.Body {
		t.Fatalf("retry changed recipient queue: %v", inbox)
	}
}

func TestIsolationAndScopedRevision(t *testing.T) {
	f := newFixture(t)
	otherTurn := f.principal
	otherTurn.Turn = "turn-b"
	otherSession := f.principal
	otherSession.SessionID = "claude-code:session-b"
	for _, scope := range []Principal{otherTurn, otherSession} {
		sender := enroll(t, f.store, scope)
		recipient := scope
		recipient.AgentID = testRequest().Target
		receiver := enroll(t, f.store, recipient)
		if inbox, err := f.service.Receive(context.Background(), receiver); err != nil || len(inbox) != 0 {
			t.Fatalf("other scope inbox = %v, %v", inbox, err)
		}
		if _, err := f.service.Send(context.Background(), sender, testRequest()); err != nil {
			t.Fatal(err)
		}
		if snapshot, revision, _ := f.store.Snapshot(f.principal.SessionID, f.principal.Turn); len(snapshot) != 0 || revision != 0 {
			t.Fatalf("other scope changed original scope: len %d revision %d", len(snapshot), revision)
		}
	}
	if _, err := f.service.Ack(context.Background(), f.receiver, "message-one"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-scope Ack error = %v; want not found", err)
	}
	if _, err := f.service.Send(context.Background(), f.sender, testRequest()); err != nil {
		t.Fatal(err)
	}
	if inbox, err := f.service.Receive(context.Background(), f.receiver); err != nil || len(inbox) != 1 || inbox[0].SessionID != "session-a" {
		t.Fatalf("scoped receive = %v, %v", inbox, err)
	}
}

func TestRecipientEnrollmentAndReplyBinding(t *testing.T) {
	f := newFixture(t)
	request := testRequest()
	request.Target = "subagent:/root/missing"
	if _, err := f.service.Send(context.Background(), f.sender, request); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unenrolled recipient error = %v; want not found", err)
	}
	request = testRequest()
	request.ReplyTo = "nonexistent"
	if _, err := f.service.Send(context.Background(), f.sender, request); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid reply error = %v; want invalid", err)
	}
	if _, err := f.service.Send(context.Background(), f.sender, testRequest()); err != nil {
		t.Fatal(err)
	}
	reply := Request{ID: "reply-one", Target: f.principal.AgentID, Body: "확인했습니다.", ReplyTo: "message-one"}
	if _, err := f.service.Send(context.Background(), f.receiver, reply); err != nil {
		t.Fatal(err)
	}
	reply.ID = "wrong-reply"
	reply.Target = testRequest().Target
	if _, err := f.service.Send(context.Background(), f.sender, reply); !errors.Is(err, ErrInvalid) {
		t.Fatalf("reply from wrong conversation end error = %v; want invalid", err)
	}
}

func TestPersistenceAndProcessLock(t *testing.T) {
	f := newFixture(t)
	if _, err := f.service.Send(context.Background(), f.sender, testRequest()); err != nil {
		t.Fatal(err)
	}
	if second, err := Open(f.store.directory); !errors.Is(err, ErrLocked) {
		if second != nil {
			second.Close()
		}
		t.Fatalf("second Open error = %v; want locked", err)
	}
	admin, err := os.ReadFile(f.store.AdminTokenPath())
	if err != nil {
		t.Fatal(err)
	}
	if !f.store.AuthenticateAdmin(strings.TrimSpace(string(admin))) || f.store.AuthenticateAdmin("invalid") {
		t.Fatal("admin authentication mismatch")
	}
	stored, err := os.ReadFile(filepath.Join(f.store.directory, "mailbox.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(stored), f.sender) || strings.Contains(string(stored), strings.TrimSpace(string(admin))) {
		t.Fatal("state persisted a plaintext capability or administrator credential")
	}
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(f.store.directory)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if !reopened.AuthenticateAdmin(strings.TrimSpace(string(admin))) {
		t.Fatal("admin credential changed after reopen")
	}
	service := NewService(reopened, map[string]Transport{MailboxTransport: reopened.Mailbox()})
	inbox, err := service.Receive(context.Background(), f.receiver)
	if err != nil || len(inbox) != 1 || inbox[0].Body != testRequest().Body {
		t.Fatalf("reopened inbox = %v, %v", inbox, err)
	}
	if _, err := service.Send(context.Background(), f.sender, testRequest()); err != nil {
		t.Fatal(err)
	}
	if inbox, _ := service.Receive(context.Background(), f.receiver); len(inbox) != 1 {
		t.Fatal("reopen retry duplicated message")
	}
	for _, path := range []string{reopened.directory, reopened.AdminTokenPath(), filepath.Join(reopened.directory, "mailbox.json"), filepath.Join(reopened.directory, ".lock")} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		want := os.FileMode(0600)
		if info.IsDir() {
			want = 0700
		}
		if info.Mode().Perm() != want {
			t.Errorf("mode for %s = %o; want %o", filepath.Base(path), info.Mode().Perm(), want)
		}
	}
}

func TestPersistenceFailureRollsBackSendAndAck(t *testing.T) {
	f := newFixture(t)
	path := filepath.Join(f.store.directory, "mailbox.json")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0400); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.Send(context.Background(), f.sender, testRequest()); err == nil {
		t.Fatal("send succeeded with invalid state file permissions")
	}
	if snapshot, revision, _ := f.store.Snapshot("session-a", "turn-a"); len(snapshot) != 0 || revision != 0 {
		t.Fatalf("failed send mutated memory: %v, %d", snapshot, revision)
	}
	after, _ := os.ReadFile(path)
	if string(before) != string(after) {
		t.Fatal("failed send changed durable state")
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.Send(context.Background(), f.sender, testRequest()); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0400); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.Ack(context.Background(), f.receiver, "message-one"); err == nil {
		t.Fatal("Ack succeeded with invalid state file permissions")
	}
	if inbox, err := f.service.Receive(context.Background(), f.receiver); err != nil || len(inbox) != 1 {
		t.Fatalf("failed Ack removed pending message: %v, %v", inbox, err)
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.Ack(context.Background(), f.receiver, "message-one"); err != nil {
		t.Fatal(err)
	}
}

func TestCancellationUnsupportedAndBounds(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := f.service.Send(ctx, f.sender, testRequest()); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Send error = %v", err)
	}
	if _, err := f.service.Receive(ctx, f.receiver); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Receive error = %v", err)
	}
	if _, err := f.service.Ack(ctx, f.receiver, "message-one"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Ack error = %v", err)
	}
	if _, err := f.store.Enroll(ctx, f.principal); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Enroll error = %v", err)
	}
	request := testRequest()
	request.Transport = "codex-native"
	if _, err := f.service.Send(context.Background(), f.sender, request); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("unsupported transport error = %v", err)
	}
	if snapshot, revision, _ := f.store.Snapshot("session-a", "turn-a"); len(snapshot) != 0 || revision != 0 {
		t.Fatal("failed/unsupported request created an observation")
	}
	for _, body := range []string{"", "  ", "hello\x00", string([]byte{0xff}), strings.Repeat("x", MaxBodyRunes+1)} {
		request = testRequest()
		request.Body = body
		if _, err := f.service.Send(context.Background(), f.sender, request); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid body error = %v", err)
		}
	}
	if _, err := f.service.Send(context.Background(), "invalid", testRequest()); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("invalid token error = %v", err)
	}
}

func TestRetentionPreservesRetryIdentityAndPendingBody(t *testing.T) {
	f := newFixture(t)
	now := time.Date(2026, 10, 16, 1, 0, 0, 0, time.UTC)
	f.store.now = func() time.Time { return now }
	if _, err := f.service.Send(context.Background(), f.sender, testRequest()); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.Ack(context.Background(), f.receiver, "message-one"); err != nil {
		t.Fatal(err)
	}
	pending := testRequest()
	pending.ID = "pending"
	if _, err := f.service.Send(context.Background(), f.sender, pending); err != nil {
		t.Fatal(err)
	}
	now = now.Add(AckRetention + time.Second)
	if err := f.store.Prune(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot, revision, _ := f.store.Snapshot("session-a", "turn-a")
	if len(snapshot) != 2 || revision != 4 {
		t.Fatalf("retention snapshot len %d revision %d", len(snapshot), revision)
	}
	if snapshot[0].Body != "" || snapshot[0].BodyExpiredAt == nil || snapshot[1].Body != pending.Body {
		t.Fatalf("retention state = %v", snapshot)
	}
	retry, err := f.service.Send(context.Background(), f.sender, testRequest())
	if err != nil || retry.BodyExpiredAt == nil || retry.Body != "" {
		t.Fatalf("expired replay = %v, %v", retry, err)
	}
	if inbox, _ := f.service.Receive(context.Background(), f.receiver); len(inbox) != 1 || inbox[0].ID != "pending" {
		t.Fatalf("retention/retry changed inbox: %v", inbox)
	}
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(f.store.directory)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if snapshot, _, _ := reopened.Snapshot("session-a", "turn-a"); len(snapshot) != 2 || snapshot[0].BodyExpiredAt == nil {
		t.Fatalf("reopen lost retention: %v", snapshot)
	}
}

func TestSnapshotAndReceiptsAreDetached(t *testing.T) {
	f := newFixture(t)
	if _, err := f.service.Send(context.Background(), f.sender, testRequest()); err != nil {
		t.Fatal(err)
	}
	acked, err := f.service.Ack(context.Background(), f.receiver, "message-one")
	if err != nil {
		t.Fatal(err)
	}
	*acked.AckedAt = time.Time{}
	snapshot, _, _ := f.store.Snapshot("session-a", "turn-a")
	if snapshot[0].AckedAt.IsZero() {
		t.Fatal("caller mutated stored Ack time")
	}
	snapshot[0].Body = "mutated"
	*snapshot[0].AckedAt = time.Time{}
	check, _, _ := f.store.Snapshot("session-a", "turn-a")
	if check[0].Body != testRequest().Body || check[0].AckedAt.IsZero() {
		t.Fatal("snapshot aliases store memory")
	}
}

func TestConcurrentSameIDEnqueuesOnce(t *testing.T) {
	f := newFixture(t)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := f.service.Send(context.Background(), f.sender, testRequest()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if inbox, err := f.service.Receive(context.Background(), f.receiver); err != nil || len(inbox) != 1 {
		t.Fatalf("concurrent inbox = %v, %v", inbox, err)
	}
	if _, revision, _ := f.store.Snapshot("session-a", "turn-a"); revision != 1 {
		t.Fatalf("concurrent send revision %d; want1", revision)
	}
}

func TestRejectsUnsafeAndCorruptStores(t *testing.T) {
	parent := t.TempDir()
	target := filepath.Join(parent, "real")
	if err := os.Mkdir(target, 0700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(parent, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if store, err := Open(link); err == nil {
		store.Close()
		t.Fatal("accepted symlink store")
	}
	if err := os.Chmod(target, 0755); err != nil {
		t.Fatal(err)
	}
	if store, err := Open(target); err == nil {
		store.Close()
		t.Fatal("accepted public store")
	}
	f := newFixture(t)
	if _, err := f.service.Send(context.Background(), f.sender, testRequest()); err != nil {
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
	var state diskState
	if err := json.Unmarshal(raw, &state); err != nil {
		t.Fatal(err)
	}
	key := messageKey("session-a", "turn-a", "message-one")
	stored := state.Messages[key]
	stored.Message.Body = "corrupt content"
	state.Messages[key] = stored
	raw, _ = json.Marshal(state)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if store, err := Open(f.store.directory); err == nil {
		store.Close()
		t.Fatal("accepted corrupt fingerprint")
	}
	after, _ := os.ReadFile(path)
	if string(raw) != string(after) {
		t.Fatal("Open replaced corrupt evidence")
	}
}

func TestReceiveOrderAndEnrollmentDoesNotNotifyGraph(t *testing.T) {
	f := newFixture(t)
	changed := f.store.Changes()
	enroll(t, f.store, Principal{SessionID: "session-a", Turn: "turn-a", AgentID: "agent"})
	select {
	case <-changed:
		t.Fatal("capability enrollment emitted a graph change")
	default:
	}
	now := time.Date(2026, 10, 16, 1, 0, 0, 0, time.UTC)
	f.store.now = func() time.Time { return now }
	for _, id := range []string{"z-last", "a-first"} {
		request := testRequest()
		request.ID = id
		if _, err := f.service.Send(context.Background(), f.sender, request); err != nil {
			t.Fatal(err)
		}
	}
	inbox, err := f.service.Receive(context.Background(), f.receiver)
	if err != nil || len(inbox) != 2 || inbox[0].ID != "a-first" || inbox[1].ID != "z-last" {
		t.Fatalf("same-time order = %v, %v", inbox, err)
	}
}

func TestOpenRemovesOnlyAbandonedPrivateTransactions(t *testing.T) {
	f := newFixture(t)
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	orphan := filepath.Join(f.store.directory, ".mailbox-interrupted")
	other := filepath.Join(f.store.directory, "operator-note")
	for _, path := range []string{orphan, other} {
		if err := os.WriteFile(path, []byte("private content"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	reopened, err := Open(f.store.directory)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("abandoned transaction still present: %v", err)
	}
	if _, err := os.Stat(other); err != nil {
		t.Fatalf("unrelated file was removed: %v", err)
	}
}

func TestCapacityPreservesExistingQueue(t *testing.T) {
	f := newFixture(t)
	if _, err := f.service.Send(context.Background(), f.sender, testRequest()); err != nil {
		t.Fatal(err)
	}
	// Fill only the cardinality fixture; the send must reject before persisting
	// or depending on these unrelated synthetic records.
	for i := len(f.store.state.Messages); i < MaxMessages; i++ {
		f.store.state.Messages["capacity-"+time.Unix(int64(i), 0).Format(time.RFC3339)] = storedMessage{}
	}
	request := testRequest()
	request.ID = "over-capacity"
	if _, err := f.service.Send(context.Background(), f.sender, request); !errors.Is(err, ErrCapacity) {
		t.Fatalf("full store send error = %v", err)
	}
	if _, err := f.service.Send(context.Background(), f.sender, testRequest()); err != nil {
		t.Fatalf("idempotent retry at capacity failed: %v", err)
	}
	if inbox, err := f.service.Receive(context.Background(), f.receiver); err != nil || len(inbox) != 1 || inbox[0].ID != "message-one" {
		t.Fatalf("capacity changed pending queue: %v, %v", inbox, err)
	}
}
