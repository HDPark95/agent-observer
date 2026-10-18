package sessions

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"agent-observer/internal/model"
)

func TestSelectionIsolation(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "sessions", "2026", "10", "15")
	os.MkdirAll(dir, 0700)
	for _, id := range []string{"one", "two"} {
		data := fmt.Sprintf("{\"type\":\"session_meta\",\"payload\":{\"id\":%q,\"cwd\":\"/project\"}}\n{\"type\":\"event_msg\",\"payload\":{\"type\":\"task_started\",\"turn_id\":%q}}\n", id, id+"-turn")
		if err := os.WriteFile(filepath.Join(dir, "rollout-"+id+".jsonl"), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	os.WriteFile(filepath.Join(root, "session_index.jsonl"), []byte("{\"id\":\"one\",\"thread_name\":\"실제 작업 이름\"}\n"), 0600)
	r := New(filepath.Join(root, "sessions"))
	list := r.Listing()["sessions"].([]model.Record)
	if len(list) != 2 || len(r.entries) != 0 {
		t.Fatal("catalog must not open transcripts")
	}
	if _, err := r.State("one"); err == nil {
		t.Fatal("unselected body exposed")
	}
	for _, id := range []string{"one", "two"} {
		if _, err := r.Open(model.Record{"sessionId": id}); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{"one", "two", "one"} {
		s, err := r.State(id)
		if err != nil || s.String("turn") != id+"-turn" {
			t.Fatalf("mixed session: %v %v", s, err)
		}
	}
	s, _ := r.State("one")
	if s.String("sessionLabel") != "실제 작업 이름" {
		t.Fatal(s)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 10; n++ {
				r.State("one")
				r.Listing()
			}
		}()
	}
	wg.Wait()
	link := filepath.Join(root, "link.jsonl")
	os.Symlink(filepath.Join(dir, "rollout-one.jsonl"), link)
	if _, err := r.Register(model.Record{"source": link}, nil); err == nil {
		t.Fatal("symlink accepted")
	}
}
