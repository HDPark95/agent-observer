package delivery

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"agent-observer/internal/domain"
	"agent-observer/internal/model"
	"agent-observer/internal/sessions"
)

func writeCodexSource(t *testing.T, directory, session string, rows ...model.Record) string {
	t.Helper()
	path := filepath.Join(directory, session+".jsonl")
	header := model.Record{"type": "session_meta", "payload": model.Record{"id": session}}
	all := append([]model.Record{header}, rows...)
	var data []byte
	for _, row := range all {
		line, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		data = append(data, line...)
		data = append(data, '\n')
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func codexEvent(stamp, kind, turn string) model.Record {
	return model.Record{"type": "event_msg", "timestamp": stamp, "payload": model.Record{"type": kind, "turn_id": turn}}
}

func completedTurn(turn, tool string) []model.Record {
	return []model.Record{
		codexEvent("2026-10-16T01:00:00Z", "task_started", turn),
		{"type": "response_item", "timestamp": "2026-10-16T01:00:01Z", "payload": model.Record{"type": "function_call", "name": "test_tool", "call_id": tool}},
		{"type": "response_item", "timestamp": "2026-10-16T01:00:02Z", "payload": model.Record{"type": "function_call_output", "call_id": tool}},
		codexEvent("2026-10-16T01:00:03Z", "task_complete", turn),
	}
}

func registerCodexSource(t *testing.T, registry *sessions.Registry, path string, metadataOnly bool) string {
	t.Helper()
	result, err := registry.Register(model.Record{"source": path, "metadataOnly": metadataOnly}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return result.String("sessionId")
}

func TestCollectorPreservesCompletedTurnBeforeNextTurn(t *testing.T) {
	for _, readUIFirst := range []bool{false, true} {
		name := "collector-first"
		if readUIFirst {
			name = "ui-first"
		}
		t.Run(name, func(t *testing.T) {
			rows := append(completedTurn("turn-a", "tool-a"), codexEvent("2026-10-16T01:00:04Z", "task_started", "turn-b"))
			path := writeCodexSource(t, t.TempDir(), "session", rows...)
			registry := sessions.New("")
			id := registerCodexSource(t, registry, path, false)
			if readUIFirst {
				state, err := registry.State(id)
				if err != nil || state.String("turn") != "turn-b" {
					t.Fatalf("UI did not advance to next turn: state=%v err=%v", state, err)
				}
			}
			box := newTestOutbox(t, Destination{ID: "project", Exporter: exportFunc(func(context.Context, []domain.Observation) error { return nil })})
			collector := NewCollector(registry, box)
			collector.collect()
			key := (domain.Observation{Run: domain.RunIdentity{Provider: "codex", SessionID: "session", TurnID: "turn-a"}, ID: "tool-a"}).Key()
			if _, ok := box.sinks[0].records[key]; !ok {
				t.Fatal("completed prior-turn tool was lost before entering the outbox")
			}
		})
	}
}

func TestCollectorOnlyCollectsSelectedSourcesAndDoesNotExportSynchronously(t *testing.T) {
	directory := t.TempDir()
	selected := writeCodexSource(t, directory, "selected", completedTurn("turn", "tool")...)
	offered := writeCodexSource(t, directory, "offered", completedTurn("turn", "tool")...)
	registry := sessions.New("")
	registerCodexSource(t, registry, selected, false)
	registerCodexSource(t, registry, offered, true)
	calls := 0
	box := newTestOutbox(t, Destination{ID: "project", Exporter: exportFunc(func(context.Context, []domain.Observation) error {
		calls++
		return nil
	})})
	collector := NewCollector(registry, box)
	collector.collect()
	if calls != 0 {
		t.Fatal("source collection performed synchronous network export")
	}
	count := len(box.sinks[0].records)
	if count == 0 {
		t.Fatal("selected source was not collected")
	}
	for _, record := range box.sinks[0].records {
		if record.Observation.Run.SessionID != "selected" {
			t.Fatalf("metadata-only offered source was consumed: %+v", record.Observation.Run)
		}
	}
	collector.collect()
	if got := len(box.sinks[0].records); got != count {
		t.Fatalf("unchanged source produced duplicate observations: %d -> %d", count, got)
	}
	box.flush(context.Background(), box.sinks[0])
	collector.collect()
	box.flush(context.Background(), box.sinks[0])
	if calls != 1 {
		t.Fatalf("unchanged collected records were sent again: %d calls", calls)
	}
}

func TestCollectorKeepsEqualNodeAndTurnIDsSeparateAcrossSessions(t *testing.T) {
	directory := t.TempDir()
	registry := sessions.New("")
	for _, session := range []string{"session-one", "session-two"} {
		path := writeCodexSource(t, directory, session, completedTurn("same-turn", "same-tool")...)
		registerCodexSource(t, registry, path, false)
	}
	box := newTestOutbox(t, Destination{ID: "project", Exporter: exportFunc(func(context.Context, []domain.Observation) error { return nil })})
	NewCollector(registry, box).collect()
	found := map[string]bool{}
	for _, record := range box.sinks[0].records {
		if record.Observation.ID == "same-tool" {
			found[record.Observation.Run.SessionID] = true
		}
	}
	if len(found) != 2 || !found["session-one"] || !found["session-two"] {
		t.Fatalf("sessions shared or overwrote observations: %v", found)
	}
}

func TestCollectorRetainsUnpersistedBatchBeforeReadingAgain(t *testing.T) {
	directory := t.TempDir()
	path := writeCodexSource(t, directory, "session", completedTurn("turn-a", "tool-a")...)
	registry := sessions.New("")
	registerCodexSource(t, registry, path, false)
	box := newTestOutbox(t, Destination{ID: "project", Exporter: exportFunc(func(context.Context, []domain.Observation) error { return nil })})
	collector := NewCollector(registry, box)
	sinkDirectory := box.sinkDirectory(box.sinks[0])
	offlineDirectory := sinkDirectory + "-offline"
	if err := os.Rename(sinkDirectory, offlineDirectory); err != nil {
		t.Fatal(err)
	}
	collector.collect()
	if len(box.sinks[0].records) != 0 {
		t.Fatal("failed outbox write was treated as persisted")
	}
	if err := os.Rename(offlineDirectory, sinkDirectory); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	encoder := json.NewEncoder(file)
	for _, row := range completedTurn("turn-b", "tool-b") {
		if err := encoder.Encode(row); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	collector.collect()
	for turn, id := range map[string]string{"turn-a": "tool-a", "turn-b": "tool-b"} {
		key := (domain.Observation{Run: domain.RunIdentity{Provider: "codex", SessionID: "session", TurnID: turn}, ID: id}).Key()
		if _, ok := box.sinks[0].records[key]; !ok {
			t.Errorf("turn %s was lost after persistence recovered", turn)
		}
	}
}

func TestCollectorMissingChildDoesNotBlockObservedRoot(t *testing.T) {
	rows := completedTurn("turn-a", "tool-a")
	child := model.Record{
		"type": "event_msg", "timestamp": "2026-10-16T01:00:02Z",
		"payload": model.Record{
			"type": "item_completed", "turn_id": "turn-a",
			"item": model.Record{
				"type": "SubAgentActivity", "kind": "started", "agent_path": "/root/missing",
				"agent_thread_id": "11111111-1111-4111-8111-111111111111",
			},
		},
	}
	rows = append(rows[:3], child, rows[3])
	rows = append(rows, codexEvent("2026-10-16T01:00:04Z", "task_started", "turn-b"))
	path := writeCodexSource(t, t.TempDir(), "session", rows...)
	registry := sessions.New("")
	registerCodexSource(t, registry, path, false)
	box := newTestOutbox(t, Destination{ID: "project", Exporter: exportFunc(func(context.Context, []domain.Observation) error { return nil })})
	collector := NewCollector(registry, box)
	collector.collect()
	key := (domain.Observation{Run: domain.RunIdentity{Provider: "codex", SessionID: "session", TurnID: "turn-a"}, ID: "tool-a"}).Key()
	if _, ok := box.sinks[0].records[key]; !ok {
		t.Fatal("unavailable child suppressed independently observed root activity")
	}
	encoded, err := json.Marshal(collector.Status())
	if err != nil {
		t.Fatal(err)
	}
	var status struct {
		Warning string `json:"collectionWarning"`
	}
	if err := json.Unmarshal(encoded, &status); err != nil {
		t.Fatal(err)
	}
	if status.Warning != "child-source-incomplete" {
		t.Fatalf("missing child coverage was not reported: %s", encoded)
	}
}
