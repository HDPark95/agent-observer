package sources

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"agent-observer/internal/model"
)

func identityTranscript(t *testing.T, id, turn string) []byte {
	t.Helper()
	var data bytes.Buffer
	rows := []model.Record{
		{"type": "session_meta", "payload": model.Record{"id": id}},
		{
			"type":      "event_msg",
			"timestamp": "2026-10-16T00:00:00Z",
			"payload": model.Record{
				"type": "task_started", "turn_id": turn,
			},
		},
	}
	for _, row := range rows {
		if err := json.NewEncoder(&data).Encode(row); err != nil {
			t.Fatal(err)
		}
	}
	return data.Bytes()
}

func replaceIdentityFile(t *testing.T, file string, data []byte) {
	t.Helper()
	next := file + ".next"
	if err := os.WriteFile(next, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(next, file); err != nil {
		t.Fatal(err)
	}
}

func TestCodexIdentityPinnedBeforeFirstReadAndRecovers(t *testing.T) {
	file := filepath.Join(t.TempDir(), "source.jsonl")
	replaceIdentityFile(t, file, identityTranscript(t, "one", "original"))
	reader, err := Open(Codex, file, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Selection happened in Open; the first read must not select a new ID.
	replaceIdentityFile(t, file, identityTranscript(t, "two", "foreign"))
	for attempt := 0; attempt < 2; attempt++ {
		state, nodes, root := reader.Read()
		if state["error"] == nil || root != "one" {
			t.Fatalf("selection changed: root=%q, error=%v", root, state["error"])
		}
		if state.String("turn") != "" || len(nodes) != 0 {
			t.Fatalf("foreign data consumed: %v", state)
		}
	}

	replaceIdentityFile(t, file, identityTranscript(t, "one", "recovered"))
	state, _, root := reader.Read()
	if state["error"] != nil || root != "one" ||
		state.String("turn") != "recovered" {
		t.Fatalf("same-session recovery failed: root=%q, state=%v", root, state)
	}
}

func TestCodexPublishedSnapshotSurvivesIdentityRejectionAndRecovery(t *testing.T) {
	for _, mode := range []string{"replacement", "same-inode", "in-place-recovery"} {
		t.Run(mode, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "source.jsonl")
			replaceIdentityFile(t, file, identityTranscript(t, "one", "original"))
			reader, err := Open(Codex, file, nil)
			if err != nil {
				t.Fatal(err)
			}
			old, _, root := reader.Read()
			if old["error"] != nil || root != "one" ||
				old.String("turn") != "original" {
				t.Fatalf("initial read failed: %v", old)
			}
			before, err := json.Marshal(old)
			if err != nil {
				t.Fatal(err)
			}

			// Equal-length IDs/turns keep size unchanged for the inode case.
			foreign := identityTranscript(t, "two", "intruder")
			if mode == "replacement" {
				replaceIdentityFile(t, file, foreign)
			} else if err := os.WriteFile(file, foreign, 0600); err != nil {
				t.Fatal(err)
			}
			for attempt := 0; attempt < 2; attempt++ {
				state, _, root := reader.Read()
				if state["error"] == nil || root != "one" ||
					state.String("turn") != "original" {
					t.Fatalf("foreign replacement accepted: root=%q, state=%v",
						root, state)
				}
			}

			recovered := identityTranscript(t, "one", "recovered")
			if mode == "in-place-recovery" {
				if err := os.WriteFile(file, recovered, 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				replaceIdentityFile(t, file, recovered)
			}
			state, _, root := reader.Read()
			if state["error"] != nil || root != "one" ||
				state.String("turn") != "recovered" {
				t.Fatalf("recovery failed: root=%q, state=%v", root, state)
			}
			after, err := json.Marshal(old)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("previously published snapshot mutated")
			}
		})
	}
}

func TestCodexIncompleteReplacementHeaderDefersReset(t *testing.T) {
	file := filepath.Join(t.TempDir(), "source.jsonl")
	replaceIdentityFile(t, file, identityTranscript(t, "one", "original"))
	reader, err := Open(Codex, file, nil)
	if err != nil {
		t.Fatal(err)
	}
	initial, _, _ := reader.Read()
	if initial["error"] != nil {
		t.Fatal(initial)
	}

	complete := identityTranscript(t, "one", "recovered")
	boundary := bytes.IndexByte(complete, '\n')
	if boundary < 0 {
		t.Fatal("fixture has no header newline")
	}
	// Complete JSON is still an incomplete JSONL header without its newline.
	replaceIdentityFile(t, file, complete[:boundary])
	for attempt := 0; attempt < 2; attempt++ {
		state, _, root := reader.Read()
		if state["error"] != nil || root != "one" ||
			state.String("turn") != "original" {
			t.Fatalf("incomplete header reset or changed selection: %v", state)
		}
	}

	f, err := os.OpenFile(file, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := f.Write(complete[boundary:])
	closeErr := f.Close()
	if writeErr != nil {
		t.Fatal(writeErr)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	state, _, root := reader.Read()
	if state["error"] != nil || root != "one" ||
		state.String("turn") != "recovered" {
		t.Fatalf("completed replacement did not reset/replay: %v", state)
	}
}
