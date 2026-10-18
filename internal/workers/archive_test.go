package workers

import (
	"encoding/json"
	"reflect"
	"testing"

	"agent-observer/internal/model"
)

func TestArchivedWorkersPreserveHistoricalParticipants(t *testing.T) {
	base := []model.Record{
		{"id": "one", "kind": "a2a", "bodySource": "observer-managed", "source": "managed:old", "target": "managed:gone", "runtimeProvider": "codex", "at": "2026-10-15T00:00:00Z"},
		{"id": "two", "kind": "a2a", "bodySource": "observer-managed", "source": "managed:old", "target": "managed:gone", "runtimeProvider": "claude", "at": "2026-10-15T00:00:01Z"},
		{"id": "three", "kind": "a2a", "bodySource": "observer-managed", "source": "managed:old", "target": "managed:unknown"},
	}
	before, _ := json.Marshal(base)
	teams := &Teams{template: Config{Owner: "new-owner", Workers: []Definition{{ID: "managed:old", Name: "Renamed", Task: "New task"}}}, provider: Provider{ID: "new-provider"}}
	got := teams.archivedView(base)
	byID := map[string]model.Record{}
	for _, n := range got {
		if byID[n.String("id")] != nil {
			t.Fatal("duplicate node")
		}
		byID[n.String("id")] = n
	}
	for _, id := range []string{"managed:old", "managed:gone", "managed:unknown"} {
		n := byID[id]
		if n.String("label") != id || n.String("parent") != "" || n.String("currentTask") != "" {
			t.Fatalf("invented history: %v", n)
		}
	}
	if byID["managed:gone"].String("provider") != "mixed" || byID["managed:old"].String("provider") != "unknown" || byID["managed:unknown"].String("provider") != "unknown" {
		t.Fatal("incorrect provider attribution")
	}
	if _, ok := byID["managed:unknown"]["at"]; ok {
		t.Fatal("invented timestamp")
	}
	for _, msg := range base {
		if !reflect.DeepEqual(byID[msg.String("id")], msg) {
			t.Fatal("message endpoints changed")
		}
		if byID["managed-execution:"+msg.String("id")].String("parent") != msg.String("target") {
			t.Fatal("execution lost group")
		}
	}
	after, _ := json.Marshal(base)
	if string(before) != string(after) {
		t.Fatal("input mutated")
	}
	if !reflect.DeepEqual(got, teams.archivedView(got)) {
		t.Fatal("non-idempotent projection")
	}
}
