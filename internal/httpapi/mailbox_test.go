package httpapi

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"agent-observer/internal/adapters/mailboxview"
	"agent-observer/internal/messaging"
	"agent-observer/internal/model"
	"agent-observer/internal/sessions"
)

func mailboxFixture(t *testing.T) (*sessions.Registry, *messaging.Store, http.Handler, string) {
	t.Helper()
	dir := t.TempDir()
	r := sessions.New("")
	for _, id := range []string{"a", "b"} {
		file := filepath.Join(dir, id+".jsonl")
		text := fixture(id, "turn-"+id) + fmt.Sprintf("{\"type\":\"event_msg\",\"payload\":{\"type\":\"item_completed\",\"turn_id\":%q,\"item\":{\"type\":\"SubAgentActivity\",\"agent_path\":\"/root/receiver\",\"agent_thread_id\":\"child\",\"kind\":\"started\"}}}\n", "turn-"+id)
		if err := os.WriteFile(file, []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := r.Register(model.Record{"source": file}, nil); err != nil {
			t.Fatal(err)
		}
	}
	store, err := messaging.Open(filepath.Join(dir, "mailbox"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	r.SetMessageFeed(mailboxview.New(store))
	m := &Mailbox{Store: store, Service: messaging.NewService(store, map[string]messaging.Transport{"mailbox": store.Mailbox()})}
	admin, err := os.ReadFile(store.AdminTokenPath())
	if err != nil {
		t.Fatal(err)
	}
	return r, store, HandlerWithMessaging(r, "8767", nil, m), strings.TrimSpace(string(admin))
}

func mailboxPost(t *testing.T, h http.Handler, path, token string, payload any, status int) model.Record {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("POST", "/api/mailbox/"+path, bytes.NewReader(body))
	r.Host = "127.0.0.1:8767"
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != status {
		t.Fatalf("%s: got %d, want %d (%s)", path, w.Code, status, w.Body.String())
	}
	var result model.Record
	if status == http.StatusOK && json.Unmarshal(w.Body.Bytes(), &result) != nil {
		t.Fatal("invalid JSON response")
	}
	return result
}

func TestMailboxHTTPGraphAndIsolation(t *testing.T) {
	r, _, h, admin := mailboxFixture(t)
	enroll := func(session, agent string) string {
		return mailboxPost(t, h, "enroll", admin, model.Record{"sessionId": session, "turn": "turn-" + session, "agentId": agent}, 200).String("token")
	}
	sender := enroll("a", "agent")
	receiver := enroll("a", "subagent:/root/receiver")
	other := enroll("b", "subagent:/root/receiver")
	mailboxPost(t, h, "enroll", sender, model.Record{}, 403)
	mailboxPost(t, h, "enroll", admin, model.Record{"sessionId": "a", "turn": "turn-a", "agentId": "subagent:/root/unobserved"}, 400)
	before, _ := r.State("b")
	request := model.Record{"id": "test-message", "target": "subagent:/root/receiver", "body": "첫 줄\n둘째 줄", "transport": "mailbox"}
	first := mailboxPost(t, h, "send", sender, request, 200).Object("message")
	again := mailboxPost(t, h, "send", sender, request, 200).Object("message")
	if !reflect.DeepEqual(first, again) {
		t.Fatal("idempotent request changed receipt")
	}
	conflict := request.Clone()
	conflict["body"] = "다른 내용"
	mailboxPost(t, h, "send", sender, conflict, 409)
	spoof := request.Clone()
	spoof["source"] = "other-agent"
	mailboxPost(t, h, "send", sender, spoof, 400)
	unsupported := request.Clone()
	unsupported["id"], unsupported["transport"] = "unsupported", "codex-native"
	mailboxPost(t, h, "send", sender, unsupported, 422)
	mailboxPost(t, h, "receive", "bad-token", model.Record{}, 401)
	got := mailboxPost(t, h, "receive", receiver, model.Record{}, 200).Array("messages")
	if len(got) != 1 || model.As(got[0]).String("body") != request.String("body") {
		t.Fatal("single send did not deliver exact body")
	}
	if len(mailboxPost(t, h, "receive", other, model.Record{}, 200).Array("messages")) != 0 {
		t.Fatal("cross-session inbox exposed")
	}
	mailboxPost(t, h, "ack", sender, model.Record{"id": "test-message"}, 403)
	mailboxPost(t, h, "ack", receiver, model.Record{"id": "test-message"}, 200)
	state, err := r.State("a")
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, n := range state["nodes"].([]model.Record) {
		if n.String("bodySource") == "observer-mailbox" {
			count++
			if n.String("body") != request.String("body") || n.String("status") != "수신 확인" || n.String("source") != "agent" || n.String("target") != request.String("target") {
				t.Fatalf("wrong message projection: %v", n)
			}
		}
	}
	if count != 1 {
		t.Fatalf("got %d message nodes, want one", count)
	}
	after, _ := r.State("b")
	if before.String("revision") != after.String("revision") {
		t.Fatal("unrelated session revision changed")
	}
	if len(mailboxPost(t, h, "receive", receiver, model.Record{}, 200).Array("messages")) != 0 {
		t.Fatal("acknowledged message remained pending")
	}
}

func TestMailboxSSEWithoutTranscriptWrite(t *testing.T) {
	r, store, _, _ := mailboxFixture(t)
	sender, err := store.Enroll(context.Background(), messaging.Principal{SessionID: "a", Turn: "turn-a", AgentID: "agent"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Enroll(context.Background(), messaging.Principal{SessionID: "a", Turn: "turn-a", AgentID: "subagent:/root/receiver"}); err != nil {
		t.Fatal(err)
	}
	service := messaging.NewService(store, map[string]messaging.Transport{"mailbox": store.Mailbox()})
	server := httptest.NewUnstartedServer(nil)
	_, port, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	server.Config.Handler = HandlerWithMessaging(r, port, nil, &Mailbox{Store: store, Service: service})
	server.Start()
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/api/events?session=a&format=delta", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "data: ") && strings.Contains(scanner.Text(), "\"nodes\"") {
			break
		}
	}
	if _, err := service.Send(ctx, sender, messaging.Request{ID: "live", Target: "subagent:/root/receiver", Body: "live mailbox update", Transport: "mailbox"}); err != nil {
		t.Fatal(err)
	}
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), "live mailbox update") {
			return
		}
	}
	t.Fatalf("mailbox delta not pushed while transcript idle: %v", scanner.Err())
}
