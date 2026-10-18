package sessions

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"agent-observer/internal/model"
	"agent-observer/internal/sources"
)

func TestProviderIDsAndSelectedSourcesRemainIsolated(t *testing.T) {
	dir := t.TempDir()
	codex := filepath.Join(dir, "codex.jsonl")
	claude := filepath.Join(dir, "claude.jsonl")
	const nativeID = "shared-session"
	data := `{"type":"session_meta","payload":{"id":"shared-session","cwd":"/project"}}
{"timestamp":"2026-10-16T01:00:00Z","type":"event_msg","payload":{"type":"task_started","turn_id":"codex-turn"}}
`
	if err := os.WriteFile(codex, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	if err := sources.CaptureClaudeHook(strings.NewReader(`{"hook_event_name":"UserPromptSubmit","session_id":"shared-session","prompt_id":"claude-turn","cwd":"/project","prompt":"must stay private"}`), claude); err != nil {
		t.Fatal(err)
	}
	r := New("")
	for _, p := range []model.Record{{"source": codex}, {"source": claude, "provider": sources.ClaudeCode, "metadataOnly": true}} {
		if _, err := r.Register(p, nil); err != nil {
			t.Fatal(err)
		}
	}
	if selected := r.SelectedSources(); len(selected) != 1 || selected[0].ID != nativeID {
		t.Fatalf("unopened source selected: %+v", selected)
	}
	claudeID := "claude-code:" + nativeID
	if _, err := r.Open(model.Record{"sessionId": claudeID}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ id, provider, turn string }{{nativeID, sources.Codex, "codex-turn"}, {claudeID, sources.ClaudeCode, "claude-turn"}, {nativeID, sources.Codex, "codex-turn"}} {
		state, err := r.State(tc.id)
		if err != nil {
			t.Fatal(err)
		}
		if state.String("provider") != tc.provider || state.String("turn") != tc.turn || state.String("sourceSessionId") != nativeID {
			t.Fatalf("provider data mixed: %v", state)
		}
	}
	selected := r.SelectedSources()
	if len(selected) != 2 || selected[0].ID == selected[1].ID {
		t.Fatalf("provider identities collided: %+v", selected)
	}
	originalPath := selected[0].Path
	selected[0].Path = "changed"
	if r.SelectedSources()[0].Path != originalPath {
		t.Fatal("selection metadata leaked mutable registry state")
	}
}
