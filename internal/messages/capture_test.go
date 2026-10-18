package messages

import (
	"testing"
	"time"

	"agent-observer/internal/model"
)

type fake struct {
	turn  string
	nodes []model.Record
}

func (f *fake) Read() (model.Record, []model.Record, string) {
	ns := []model.Record{}
	for _, n := range f.nodes {
		ns = append(ns, n.Clone())
	}
	return model.Record{"turn": f.turn, "revision": 1}, ns, "root-thread"
}
func TestHookJoinAndIsolation(t *testing.T) {
	f := &fake{turn: "turn"}
	c := New(f)
	p := model.Record{"sessionId": "wrong", "callId": "call", "target": "dx", "body": "검사 완료"}
	if _, e := c.Publish(p, true); e == nil {
		t.Fatal("wrong session")
	}
	p["sessionId"] = "root-thread"
	r, e := c.Publish(p, true)
	if e != nil || r.String("status") != "pending" {
		t.Fatal(r, e)
	}
	f.nodes = []model.Record{{"id": "a2a:/root/ux:call", "kind": "a2a", "sender": "/root/ux", "recipient": "/root/dx"}}
	s := c.Read()
	if s["nodes"].([]model.Record)[0].String("body") != "검사 완료" {
		t.Fatal(s)
	}
	rev := s.String("revision")
	if _, e = c.Publish(p, true); e != nil {
		t.Fatal(e)
	}
	if c.Read().String("revision") != rev {
		t.Fatal("duplicate changed revision")
	}
	p["body"] = "conflict"
	if _, e = c.Publish(p, true); e == nil {
		t.Fatal("conflicting body accepted")
	}
	f.turn = "next"
	if c.Read()["nodes"].([]model.Record)[0].String("body") != "" {
		t.Fatal("body crossed turns")
	}
	c.pending["old"] = pending{"expired", "dx", time.Now().Add(-121 * time.Second)}
	c.Read()
	if len(c.pending) != 0 {
		t.Fatal("expired pending retained")
	}
}

// Match Observer's read-only source contract instead of handing the capture an
// already-cloned fake, so accidental in-place overlays fail this regression.
type sharedSource struct {
	nodes []model.Record
	turn  string
}

func (f *sharedSource) Read() (model.Record, []model.Record, string) {
	return model.Record{"turn": f.turn, "revision": 1}, f.nodes, "root-thread"
}
func TestBodyOverlayDoesNotMutateSourceOrEarlierResponse(t *testing.T) {
	f := &sharedSource{turn: "turn", nodes: []model.Record{{"id": "a2a:/root/ux:call", "kind": "a2a", "sender": "/root/ux", "recipient": "/root/dx"}}}
	c := New(f)
	before := c.Read()
	_, e := c.Publish(model.Record{"sessionId": "root-thread", "callId": "call", "target": "dx", "body": "검사 완료"}, true)
	if e != nil {
		t.Fatal(e)
	}
	attached := c.Read()
	if attached["nodes"].([]model.Record)[0].String("body") != "검사 완료" {
		t.Fatal("body missing")
	}
	if f.nodes[0].String("body") != "" || before["nodes"].([]model.Record)[0].String("body") != "" {
		t.Fatal("shared record mutated")
	}
	f.turn = "next"
	next := c.Read()
	if next["nodes"].([]model.Record)[0].String("body") != "" {
		t.Fatal("body leaked into new turn")
	}
	if attached["nodes"].([]model.Record)[0].String("body") != "검사 완료" {
		t.Fatal("old response changed")
	}
}

func TestObservedBodyRequiresExactEventBinding(t *testing.T) {
	node := model.Record{"id": "a2a:/root/ux:call", "kind": "a2a", "source": "ux", "target": "dx"}
	payload := model.Record{"eventId": node["id"], "turn": "turn", "source": "ux", "target": "dx", "body": "review complete"}
	for _, field := range []string{"eventId", "turn", "source", "target"} {
		t.Run(field, func(t *testing.T) {
			c := New(&fake{turn: "turn", nodes: []model.Record{node}})
			invalid := payload.Clone()
			invalid[field] = "different"
			if _, err := c.Publish(invalid, false); err == nil {
				t.Fatalf("Publish accepted a mismatched %s", field)
			}
			if got := c.Read()["nodes"].([]model.Record)[0].String("body"); got != "" {
				t.Fatalf("rejected body was attached: %q", got)
			}
			if _, err := c.Publish(payload, false); err != nil {
				t.Fatalf("Publish exact match: %v", err)
			}
			if got := c.Read()["nodes"].([]model.Record)[0].String("body"); got != payload.String("body") {
				t.Fatalf("attached body = %q, want %q", got, payload.String("body"))
			}
		})
	}
}

func TestAmbiguousHookCallDoesNotAttachBody(t *testing.T) {
	f := &fake{turn: "turn", nodes: []model.Record{
		{"id": "a2a:/root/ux:call", "kind": "a2a", "sender": "/root/ux", "recipient": "/root/dx"},
		{"id": "a2a:/root/other:call", "kind": "a2a", "sender": "/root/other", "recipient": "/root/dx"},
	}}
	c := New(f)
	result, err := c.Publish(model.Record{"sessionId": "root-thread", "callId": "call", "target": "dx", "body": "review complete"}, true)
	if err != nil || result.String("status") != "pending" {
		t.Fatalf("ambiguous Publish = %v, %v; want pending without error", result, err)
	}
	for _, node := range c.Read()["nodes"].([]model.Record) {
		if got := node.String("body"); got != "" {
			t.Fatalf("ambiguous node %s received body %q", node.String("id"), got)
		}
	}
}

func TestPendingBodiesJoinByCallAndRecipientRatherThanArrivalOrder(t *testing.T) {
	f := &fake{turn: "turn"}
	c := New(f)
	for _, call := range []string{"second", "first", "wrong-recipient"} {
		_, err := c.Publish(model.Record{"sessionId": "root-thread", "callId": call, "target": "dx", "body": "body-" + call}, true)
		if err != nil {
			t.Fatal(err)
		}
	}
	f.nodes = []model.Record{
		{"id": "a2a:/root/ux:first", "kind": "a2a", "sender": "/root/ux", "recipient": "/root/dx"},
		{"id": "a2a:/root/ux:wrong-recipient", "kind": "a2a", "sender": "/root/ux", "recipient": "/root/other"},
		{"id": "a2a:/root/ux:second", "kind": "a2a", "sender": "/root/ux", "recipient": "/root/dx"},
	}
	for i, node := range c.Read()["nodes"].([]model.Record) {
		want := []string{"body-first", "", "body-second"}[i]
		if got := node.String("body"); got != want {
			t.Fatalf("node %s body = %q; want %q", node.String("id"), got, want)
		}
	}
}

func TestObservedBodyRedactionAndIdempotency(t *testing.T) {
	node := model.Record{"id": "a2a:/root/ux:call", "kind": "a2a", "source": "ux", "target": "dx"}
	c := New(&fake{turn: "turn", nodes: []model.Record{node}})
	payload := model.Record{"eventId": node["id"], "turn": "turn", "source": "ux", "target": "dx", "body": "token=synthetic-value\nreview complete"}
	if _, err := c.Publish(payload, false); err != nil {
		t.Fatal(err)
	}
	before := c.Read()
	body := before["nodes"].([]model.Record)[0].String("body")
	if body != "token=[가림]\nreview complete" {
		t.Fatalf("redacted body = %q", body)
	}
	if _, err := c.Publish(payload, false); err != nil {
		t.Fatal(err)
	}
	if c.Read().String("revision") != before.String("revision") {
		t.Fatal("duplicate changed revision")
	}
	payload["body"] = "conflict"
	if _, err := c.Publish(payload, false); err == nil {
		t.Fatal("conflicting body accepted")
	}
	if c.Read()["nodes"].([]model.Record)[0].String("body") != body {
		t.Fatal("conflict overwrote first body")
	}
}
