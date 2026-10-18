package observer

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"agent-observer/internal/model"
)

const discoveryThread = "11111111-2222-3333-4444-555555555555"

func writeDiscoveryRecords(t *testing.T, path string, rows ...model.Record) {
	t.Helper()
	var data []byte
	for _, row := range rows {
		encoded, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		data = append(data, encoded...)
		data = append(data, '\n')
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func discoveryRoot(t *testing.T) string {
	t.Helper()
	source := filepath.Join(t.TempDir(), "parent.jsonl")
	writeDiscoveryRecords(t, source,
		model.Record{"type": "session_meta", "payload": model.Record{"id": "parent"}},
		model.Record{"type": "event_msg", "timestamp": "2026-10-15T01:00:00Z", "payload": model.Record{"type": "task_started", "turn_id": "turn"}},
		model.Record{"type": "event_msg", "timestamp": "2026-10-15T01:00:01Z", "payload": model.Record{"type": "item_completed", "turn_id": "turn", "item": model.Record{"type": "SubAgentActivity", "agent_path": "/root/ux", "agent_thread_id": discoveryThread, "kind": "started"}}},
	)
	return source
}

func writeDiscoveryChild(t *testing.T, path, thread, target string) {
	t.Helper()
	arguments, err := json.Marshal(model.Record{"target": target, "message": "synthetic review"})
	if err != nil {
		t.Fatal(err)
	}
	writeDiscoveryRecords(t, path,
		model.Record{"type": "session_meta", "ordinal": 0, "payload": model.Record{"id": thread, "agent_path": "/root/ux", "parent_thread_id": "parent", "subagent_history_start_ordinal": 1}},
		model.Record{"type": "response_item", "ordinal": 1, "timestamp": "2026-10-15T01:00:02Z", "payload": model.Record{"type": "function_call", "namespace": "collaboration", "name": "send_message", "call_id": "call", "arguments": string(arguments)}},
	)
}

func TestLateChildConnectsAndNextTurnPrunesIt(t *testing.T) {
	source := discoveryRoot(t)
	o := New(source, nil)
	o.Read()
	if o.pending != 1 || len(o.auto) != 0 {
		t.Fatalf("before child: pending=%d connected=%d", o.pending, len(o.auto))
	}
	child := filepath.Join(filepath.Dir(source), "rollout-test-"+discoveryThread+".jsonl")
	writeDiscoveryChild(t, child, discoveryThread, "/root")
	_, nodes, _ := o.Read()
	if o.pending != 0 || len(o.auto) != 1 {
		t.Fatalf("after child: pending=%d connected=%d", o.pending, len(o.auto))
	}
	found := 0
	for _, n := range nodes {
		if n.String("kind") == "a2a" {
			found++
			if n.String("body") != "synthetic review" || n.String("target") != "agent" {
				t.Fatalf("message = %v", n)
			}
		}
	}
	if found != 1 {
		t.Fatalf("message count = %d, want 1", found)
	}
	f, err := os.OpenFile(source, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteString("{\"type\":\"event_msg\",\"timestamp\":\"2026-10-15T01:01:00Z\",\"payload\":{\"type\":\"task_started\",\"turn_id\":\"next\"}}\n")
	closeErr := f.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	_, nodes, _ = o.Read()
	if len(o.auto) != 0 || o.pending != 0 {
		t.Fatal("child survived new turn")
	}
	for _, n := range nodes {
		if n.String("kind") == "a2a" {
			t.Fatal("message leaked into next turn")
		}
	}
}

func TestChildMessagesRequireKnownRecipientAndMatchingOwner(t *testing.T) {
	for _, tc := range []struct{ name, thread, target string }{
		{"unknown recipient", discoveryThread, "/root/unknown"},
		{"wrong thread", "99999999-2222-3333-4444-555555555555", "/root"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := discoveryRoot(t)
			child := filepath.Join(filepath.Dir(source), "child.jsonl")
			writeDiscoveryChild(t, child, tc.thread, tc.target)
			o := New(source, []string{child})
			_, nodes, _ := o.Read()
			for _, n := range nodes {
				if n.String("kind") == "a2a" {
					t.Fatalf("unbound message accepted: %v", n)
				}
			}
		})
	}
}
