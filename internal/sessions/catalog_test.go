package sessions

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"agent-observer/internal/model"
)

func writeCatalogSource(t *testing.T, root, name string, metadata model.Record) string {
	t.Helper()
	dir := filepath.Join(root, "2026", "10", "15")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	header, err := json.Marshal(model.Record{"type": "session_meta", "payload": metadata})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "rollout-"+name+".jsonl")
	if err := os.WriteFile(path, append(header, []byte("\nPRIVATE BODY NOT JSON\n")...), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestCatalogFiltersInvalidAndAmbiguousSources(t *testing.T) {
	root := t.TempDir()
	original := writeCatalogSource(t, root, "real", model.Record{"id": "real"})
	writeCatalogSource(t, root, "child", model.Record{"id": "child", "agent_path": "/root/ux", "parent_thread_id": "real"})
	for _, name := range []string{"duplicate-a", "duplicate-b"} {
		writeCatalogSource(t, root, name, model.Record{"id": "duplicate"})
	}
	if err := os.Symlink(original, filepath.Join(filepath.Dir(original), "rollout-alias.jsonl")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(filepath.Dir(original), "rollout-bad.jsonl"), []byte("invalid\n"), 0600); err != nil {
		t.Fatal(err)
	}
	c := catalog{root: root}
	got := c.read()
	if len(got) != 1 || got["real"] == nil {
		t.Fatalf("catalog = %v; want only real", got)
	}
	missing := catalog{root: filepath.Join(root, "missing")}
	if len(missing.read()) != 0 {
		t.Fatal("missing directory returned entries")
	}
	r := New(root)
	listed := r.Listing()
	for _, item := range listed["sessions"].([]model.Record) {
		if _, exists := item["source"]; exists {
			t.Fatal("listing exposed source field")
		}
	}
	listing, err := json.Marshal(listed)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(listing), "PRIVATE") || strings.Contains(string(listing), original) || len(r.entries) != 0 {
		t.Fatalf("catalog exposed content or opened a transcript: %s", listing)
	}
}

func TestOpenRevalidatesCatalogAndOfferedSources(t *testing.T) {
	for _, offered := range []bool{false, true} {
		t.Run(map[bool]string{false: "catalog", true: "hook"}[offered], func(t *testing.T) {
			root := t.TempDir()
			source := writeCatalogSource(t, root, "selected", model.Record{"id": "selected"})
			r := New(root)
			if offered {
				r = New("") // The offered source cannot be found through catalog discovery.
				result, err := r.Register(model.Record{"source": source, "sessionId": "selected", "metadataOnly": true}, nil)
				if err != nil || result.Bool("registered") {
					t.Fatalf("Register = %v, %v", result, err)
				}
			}
			listing := r.Listing()["sessions"].([]model.Record)
			if len(listing) != 1 || listing[0].Bool("registered") {
				t.Fatalf("unselected listing = %v", listing)
			}
			if len(r.entries) != 0 {
				t.Fatal("listing opened a transcript")
			}
			if _, err := r.State("selected"); err == nil {
				t.Fatal("unselected session exposed")
			}
			writeCatalogSource(t, root, "selected", model.Record{"id": "replacement"})
			if _, err := r.Open(model.Record{"sessionId": "selected"}); err == nil {
				t.Fatal("changed identity accepted")
			}
			if len(r.entries) != 0 {
				t.Fatal("rejected source registered")
			}
			writeCatalogSource(t, root, "selected", model.Record{"id": "selected"})
			if _, err := r.Open(model.Record{"sessionId": "selected"}); err != nil {
				t.Fatal(err)
			}
			if len(r.entries) != 1 || r.entries["selected"] == nil {
				t.Fatal("selection did not open expected session")
			}
		})
	}
}

func TestTitlesRefreshAndExplicitLabelWins(t *testing.T) {
	root := t.TempDir()
	source := writeCatalogSource(t, root, "session-a", model.Record{"id": "session-a", "cwd": "/work/demo"})
	writeCatalogSource(t, root, "session-b", model.Record{"id": "session-b"})
	r := New(root)
	r.titles.path = filepath.Join(root, "titles.jsonl")
	writeTitle := func(title string) {
		t.Helper()
		row, err := json.Marshal(model.Record{"id": "session-a", "thread_name": title})
		if err != nil {
			t.Fatal(err)
		}
		f, err := os.OpenFile(r.titles.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, writeErr := f.Write(append(row, []byte("\ninvalid\n")...))
		closeErr := f.Close()
		if writeErr != nil {
			t.Fatal(writeErr)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
	}
	writeTitle("화면 동작 검사")
	labels := func() map[string]model.Record {
		result := map[string]model.Record{}
		for _, item := range r.Listing()["sessions"].([]model.Record) {
			result[item.String("sessionId")] = item
		}
		return result
	}
	list := labels()
	if len(list) != 2 || list["session-a"].String("label") != "화면 동작 검사" || list["session-a"].String("project") != "demo" || list["session-b"].String("label") != "제목 없는 대화 · ession-b" || len(r.entries) != 0 {
		t.Fatalf("metadata listing = %v", list)
	}
	if _, err := r.Open(model.Record{"sessionId": "session-a"}); err != nil {
		t.Fatal(err)
	}
	state, err := r.State("session-a")
	if err != nil || state.String("sessionLabel") != "화면 동작 검사" {
		t.Fatalf("initial State = %v, %v", state, err)
	}
	writeTitle("변경된 작업 제목")
	for _, label := range []string{"변경된 작업 제목", "직접 정한 이름"} {
		if label == "직접 정한 이름" {
			if _, err := r.Register(model.Record{"source": source, "label": label}, nil); err != nil {
				t.Fatal(err)
			}
		}
		state, err := r.State("session-a")
		if err != nil || state.String("sessionLabel") != label {
			t.Fatalf("State = %v, %v; want label %s", state, err, label)
		}
		if got := labels()["session-a"].String("label"); got != label {
			t.Fatalf("listing label = %q; want %q", got, label)
		}
	}
}

func TestRegisterRejectsInvalidSelection(t *testing.T) {
	root := t.TempDir()
	source := writeCatalogSource(t, root, "root", model.Record{"id": "root"})
	child := writeCatalogSource(t, root, "child", model.Record{"id": "child", "agent_path": "/root/ux", "parent_thread_id": "root"})
	for name, payload := range map[string]model.Record{
		"relative path":  {"source": "relative.jsonl"},
		"wrong identity": {"source": source, "sessionId": "wrong"},
		"child as root":  {"source": child},
		"invalid label":  {"source": source, "label": 42},
		"long label":     {"source": source, "label": strings.Repeat("가", 101)},
	} {
		t.Run(name, func(t *testing.T) {
			r := New(root)
			if _, err := r.Register(payload, nil); err == nil {
				t.Fatal("invalid selection accepted")
			}
			if len(r.entries) != 0 {
				t.Fatal("invalid selection changed entries")
			}
		})
	}
}
