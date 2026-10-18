package sources

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"agent-observer/internal/model"
)

func writeHistoryRows(t *testing.T, file string, rows ...model.Record) {
	t.Helper()
	var data bytes.Buffer
	for _, row := range rows {
		if err := json.NewEncoder(&data).Encode(row); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(file, data.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
}

func codexRow(turn, typ, stamp string) model.Record {
	return model.Record{"type": "event_msg", "timestamp": stamp, "payload": model.Record{"type": typ, "turn_id": turn}}
}

func TestCodexHistoryPreservesCrossedTurnsAndReplaysReadFailure(t *testing.T) {
	file := filepath.Join(t.TempDir(), "codex.jsonl")
	rows := []model.Record{
		{"type": "session_meta", "payload": model.Record{"id": "one"}},
		codexRow("a", "task_started", "2026-10-16T00:00:00Z"),
		codexRow("a", "task_complete", "2026-10-16T00:00:01Z"),
		codexRow("b", "task_started", "2026-10-16T00:00:02Z"),
		codexRow("b", "task_complete", "2026-10-16T00:00:03Z"),
	}
	writeHistoryRows(t, file, rows...)
	history, err := OpenHistory(Selection{ID: "one", Path: file})
	if err != nil {
		t.Fatal(err)
	}
	turns, err := history.Read()
	if err != nil || len(turns) != 2 {
		t.Fatalf("turns = %v, error = %v", turns, err)
	}
	for i, expected := range []string{"a", "b"} {
		turn := turns[i]
		if turn.String("turn") != expected || turn.String("provider") != Codex || turn.String("sourceSessionId") != "one" || turn.String("sessionId") != "one" {
			t.Fatalf("turn metadata = %v", turn)
		}
		if nodesByID(turn["nodes"].([]model.Record))["agent"].String("status") != "턴 종료" {
			t.Fatalf("completion lost: %v", turn)
		}
	}
	ui, err := Open("", file, nil)
	if err != nil {
		t.Fatal(err)
	}
	current, _, _ := ui.Read()
	if current.String("turn") != "b" {
		t.Fatalf("UI no longer returns latest turn: %v", current)
	}
	previous, err := json.Marshal(turns)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(file, file+".saved"); err != nil {
		t.Fatal(err)
	}
	if result, err := history.Read(); err == nil || len(result) != 0 {
		t.Fatalf("missing root accepted: %v, %v", result, err)
	}
	rows = append(rows, codexRow("c", "task_started", "2026-10-16T00:00:04Z"))
	writeHistoryRows(t, file, rows...)
	replayed, err := history.Read()
	if err != nil || len(replayed) != 3 {
		t.Fatalf("failed read skipped history: %v, %v", replayed, err)
	}
	after, err := json.Marshal(turns)
	if err != nil || !bytes.Equal(previous, after) {
		t.Fatal("earlier turn snapshot mutated")
	}
	rows[0].Object("payload")["id"] = "another-session"
	writeHistoryRows(t, file, rows...)
	if result, err := history.Read(); err == nil || len(result) != 0 {
		t.Fatalf("replacement identity accepted: %v, %v", result, err)
	}
}

func TestClaudeHistoryRetainsCompletedTurnBeforeNextPrompt(t *testing.T) {
	file := filepath.Join(t.TempDir(), "claude.jsonl")
	for _, prompt := range []string{"a", "b"} {
		request := hook("UserPromptSubmit")
		request["prompt_id"] = prompt
		captureFixture(t, file, request)
		tool := hook("PostToolUse")
		tool["prompt_id"], tool["tool_use_id"], tool["tool_name"] = prompt, prompt+"-tool", "Read"
		captureFixture(t, file, tool)
		stop := hook("Stop")
		stop["prompt_id"] = prompt
		captureFixture(t, file, stop)
	}
	history, err := OpenHistory(Selection{ID: "claude-code:session-one", Provider: ClaudeCode, Path: file})
	if err != nil {
		t.Fatal(err)
	}
	turns, err := history.Read()
	if err != nil || len(turns) != 2 {
		t.Fatalf("turns = %v, error = %v", turns, err)
	}
	for i, prompt := range []string{"a", "b"} {
		turn := turns[i]
		nodes := nodesByID(turn["nodes"].([]model.Record))
		if turn.String("turn") != prompt || turn.String("sessionId") != "claude-code:session-one" || nodes["claude:tool:"+prompt+"-tool"].String("status") != "완료 관측" || nodes["agent"].String("status") != "턴 종료" {
			t.Fatalf("lost or mixed turn: %v", turn)
		}
		if len(nodes) != 3 {
			t.Fatalf("another turn's tool leaked: %v", nodes)
		}
	}
	// A malformed event fails the batch; repairing the file replays all turns.
	valid, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	invalid := append(append([]byte(nil), valid...), []byte("{\"schemaVersion\":99}\n")...)
	if err := os.WriteFile(file, invalid, 0600); err != nil {
		t.Fatal(err)
	}
	if result, err := history.Read(); err == nil || len(result) != 0 {
		t.Fatalf("invalid event accepted: %v, %v", result, err)
	}
	if err := os.WriteFile(file, valid, 0600); err != nil {
		t.Fatal(err)
	}
	if result, err := history.Read(); err != nil || len(result) != 2 {
		t.Fatalf("repair did not replay history: %v, %v", result, err)
	}
}

func TestCodexHistoryChildWindowAndMissingChildWarning(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "root.jsonl")
	childID := "11111111-2222-3333-4444-555555555555"
	rows := []model.Record{{"type": "session_meta", "payload": model.Record{"id": "root"}}}
	for i, turn := range []string{"a", "b"} {
		start := fmt.Sprintf("2026-10-16T00:00:0%dZ", i*2)
		rows = append(rows, codexRow(turn, "task_started", start))
		agent := codexRow(turn, "item_completed", start)
		agent.Object("payload")["item"] = model.Record{"type": "SubAgentActivity", "kind": "started", "agent_path": "/root/ux", "agent_thread_id": childID}
		rows = append(rows, agent)
	}
	writeHistoryRows(t, file, rows...)
	history, err := OpenHistory(Selection{ID: "root", Path: file})
	if err != nil {
		t.Fatal(err)
	}
	turns, err := history.Read()
	if err != nil || len(turns) != 2 || turns[0].Object("autoAgents")["pending"] != 1 {
		t.Fatalf("missing child stopped root history or hid gap: %v, %v", turns, err)
	}
	child := filepath.Join(dir, "rollout-test-"+childID+".jsonl")
	childRows := []model.Record{{"type": "session_meta", "payload": model.Record{"id": childID, "parent_thread_id": "root", "agent_path": "/root/ux", "subagent_history_start_ordinal": 1}}}
	for i, stamp := range []string{"2026-10-15T23:59:59Z", "2026-10-16T00:00:00.500Z", "2026-10-16T00:00:02Z", "2026-10-16T00:00:02.500Z"} {
		row := codexRow("child", "item_completed", stamp)
		row["ordinal"] = i + 1
		row.Object("payload")["item"] = model.Record{"type": "ImageView", "id": fmt.Sprintf("activity-%d", i)}
		childRows = append(childRows, row)
	}
	writeHistoryRows(t, child, childRows...)
	current, err := history.Read()
	if err != nil || len(current) != 1 || current[0].String("turn") != "b" || current[0].Object("autoAgents")["pending"] != 0 {
		t.Fatalf("current-turn child did not recover: %v, %v", current, err)
	}
	if nodesByID(current[0]["nodes"].([]model.Record))["child:"+childID+":activity-2"] == nil {
		t.Fatal("newly available current-turn child event missing")
	}
	// Restart replay backfills old turns; ordinary reads continue the current one.
	history, err = OpenHistory(Selection{ID: "root", Path: file})
	if err != nil {
		t.Fatal(err)
	}
	turns, err = history.Read()
	if err != nil || len(turns) != 2 {
		t.Fatalf("child history = %v, %v", turns, err)
	}
	for i, turn := range turns {
		nodes := nodesByID(turn["nodes"].([]model.Record))
		prefix := "child:" + childID + ":"
		if nodes[prefix+"activity-0"] != nil {
			t.Fatal("pre-turn child event included")
		}
		if i == 0 && (nodes[prefix+"activity-1"] == nil || nodes[prefix+"activity-2"] != nil || nodes[prefix+"activity-3"] != nil) {
			t.Fatalf("first turn child boundary incorrect: %v", nodes)
		}
		if i == 1 && (nodes[prefix+"activity-1"] != nil || nodes[prefix+"activity-2"] == nil || nodes[prefix+"activity-3"] == nil) {
			t.Fatalf("second turn child boundary incorrect: %v", nodes)
		}
	}
}
