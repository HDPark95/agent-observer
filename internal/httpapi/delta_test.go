package httpapi

import (
	"encoding/json"
	"reflect"
	"testing"

	"agent-observer/internal/model"
)

func TestDeltaChangesAndReset(t *testing.T) {
	a := model.Record{"id": "a", "label": "original"}
	b := model.Record{"id": "b"}
	c := model.Record{"id": "c"}
	state := func(turn string, nodes ...model.Record) model.Record {
		return model.Record{"sessionId": "s", "turn": turn, "nodes": nodes}
	}
	var d streamDelta
	kind, full := d.next(state("t", a, b))
	if kind != "state" || full["sequence"] != 1 {
		t.Fatal(full)
	}
	changed := a.Clone()
	changed["label"] = "updated"
	kind, patch := d.next(state("t", changed, b, c))
	if kind != "delta" || len(patch["upsert"].([]model.Record)) != 2 || patch["order"] != nil || patch["baseSequence"] != 1 {
		t.Fatal(patch)
	}
	if a["label"] != "original" {
		t.Fatal("mutated snapshot")
	}
	_, patch = d.next(state("t", c, changed))
	if !reflect.DeepEqual(patch["removed"], []string{"b"}) || !reflect.DeepEqual(patch["order"], []string{"c", "a"}) {
		t.Fatal(patch)
	}
	_, patch = d.next(state("t", c.Clone(), changed.Clone()))
	if len(patch["upsert"].([]model.Record)) != 0 || patch["order"] != nil {
		t.Fatal(patch)
	}
	kind, full = d.next(state("new", b))
	if kind != "state" || len(full["nodes"].([]model.Record)) != 1 {
		t.Fatal(full)
	}
	var reconnect streamDelta
	kind, _ = reconnect.next(state("new", b))
	if kind != "state" {
		t.Fatal(kind)
	}
}
func TestDeltaAppendWireSize(t *testing.T) {
	nodes := make([]model.Record, 10000)
	for i := range nodes {
		nodes[i] = model.Record{"id": string(rune(i + 100)), "label": "unchanged node"}
	}
	var d streamDelta
	d.next(model.Record{"turn": "t", "nodes": nodes})
	next := append(append([]model.Record{}, nodes...), model.Record{"id": "new", "label": "new node"})
	_, patch := d.next(model.Record{"turn": "t", "nodes": next})
	raw, err := json.Marshal(patch)
	if err != nil || len(raw) > 500 || len(patch["upsert"].([]model.Record)) != 1 {
		t.Fatalf("append bytes=%d error=%v", len(raw), err)
	}
}
