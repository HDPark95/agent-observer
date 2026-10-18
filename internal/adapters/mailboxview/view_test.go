package mailboxview

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"agent-observer/internal/messaging"
)

func TestFeedReadDoesNotAckAndRecoversFromStore(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "mailbox")
	store, err := messaging.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	sender, err := store.Enroll(ctx, messaging.Principal{SessionID: "session", Turn: "turn", AgentID: "agent"})
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := store.Enroll(ctx, messaging.Principal{SessionID: "session", Turn: "turn", AgentID: "subagent:/root/ux"})
	if err != nil {
		t.Fatal(err)
	}
	service := messaging.NewService(store, map[string]messaging.Transport{"mailbox": store.Mailbox()})
	_, err = service.Send(ctx, sender, messaging.Request{ID: "one", Target: "subagent:/root/ux", Body: "확인 요청\n다음 줄"})
	if err != nil {
		t.Fatal(err)
	}
	// A fresh observer attaches after the message has already been delivered.
	feed := New(store)
	nodes, revision := feed.Read("session", "turn")
	if len(nodes) != 1 || nodes[0].String("body") != "확인 요청\n다음 줄" || nodes[0].String("bodySource") != "observer-mailbox" {
		t.Fatal("message not recoverable without separate capture")
	}
	first := nodes[0]
	unchanged, sameRevision := feed.Read("session", "turn")
	if revision != sameRevision || reflect.ValueOf(first).Pointer() != reflect.ValueOf(unchanged[0]).Pointer() {
		t.Fatal("idle read replaced immutable node")
	}
	if got, _ := feed.Read("session", "other-turn"); len(got) != 0 {
		t.Fatal("turn isolation failed")
	}
	if got, _ := service.Receive(ctx, receiver); len(got) != 1 {
		t.Fatal("observation acknowledged the message")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = messaging.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	recovered, _ := New(store).Read("session", "turn")
	if !reflect.DeepEqual(nodes, recovered) {
		t.Fatal("observer restart changed message identity or body")
	}
}

func TestManagedProjectionRetainsProviderAndLegacyUnknown(t *testing.T) {
	for _, transport := range []string{messaging.ManagedTransport, messaging.LegacyManagedTransport} {
		for _, provider := range []string{"", "codex", "claude-code"} {
			t.Run(transport+"/"+provider, func(t *testing.T) {
				message := messaging.Message{ID: "message", SessionID: "session", Turn: "turn", Source: "managed:ux", Target: "managed:dx", Body: "adapter evidence", Transport: transport,
					Delivery: &messaging.Delivery{Provider: provider, State: "completed", ThreadID: "conversation", TurnID: "execution", Output: "done"}}
				node := project(message)
				if node.String("runtimeProvider") != provider || node.String("bodySource") != "observer-managed" || node.String("body") != message.Body || node.String("deliveryState") != "completed" {
					t.Fatalf("managed projection lost persisted provenance or body: %+v", node)
				}
				if strings.Contains(node.String("evidence"), "Codex") {
					t.Fatal("projection guessed provider from an unrelated current configuration")
				}
			})
		}
	}
}

func TestArchiveReadFailureReplacesCachedBodyAndRecovers(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "mailbox")
	store, err := messaging.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	sender, err := store.Enroll(ctx, messaging.Principal{SessionID: "session", Turn: "turn", AgentID: "agent"})
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := store.Enroll(ctx, messaging.Principal{SessionID: "session", Turn: "turn", AgentID: "managed:ux"})
	if err != nil {
		t.Fatal(err)
	}
	service := messaging.NewService(store, map[string]messaging.Transport{messaging.MailboxTransport: store.Mailbox()})
	if _, err := service.Send(ctx, sender, messaging.Request{ID: "one", Target: "managed:ux", Body: "preserved body"}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Ack(ctx, receiver, "one"); err != nil {
		t.Fatal(err)
	}
	if err := store.Prune(ctx); err != nil {
		t.Fatal(err)
	}
	feed := New(store)
	before, revision := feed.Read("session", "turn")
	if len(before) != 1 || before[0].String("body") != "preserved body" {
		t.Fatal("archive did not render")
	}
	entries, err := os.ReadDir(filepath.Join(dir, "archive", "payloads"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("payload fixture: %v", err)
	}
	path := filepath.Join(dir, "archive", "payloads", entries[0].Name())
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	failed, failureRevision := feed.Read("session", "turn")
	if failureRevision == revision || len(failed) != 1 || failed[0].String("status") != "오류" || failed[0].String("body") != "" {
		t.Fatal("cached body hid archive read failure")
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	recovered, restoredRevision := feed.Read("session", "turn")
	if restoredRevision != revision || !reflect.DeepEqual(recovered, before) {
		t.Fatal("repair did not restore original projection")
	}
}
