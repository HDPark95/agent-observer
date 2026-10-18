package observer

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"agent-observer/internal/model"
)

func clean(v any) any {
	switch x := v.(type) {
	case map[string]any:
		r := map[string]any{}
		for k, v := range x {
			if k != "seen" && k != "checkedAt" && k != "revision" {
				r[k] = clean(v)
			}
		}
		return r
	case []any:
		r := []any{}
		for _, v := range x {
			r = append(r, clean(v))
		}
		return r
	}
	return v
}
func canonical(v any) any {
	b, _ := json.Marshal(v)
	var result any
	json.Unmarshal(b, &result)
	return clean(result)
}
func TestRecordedEventContract(t *testing.T) {
	var steps []struct {
		Name, Target, Content, Mode string
		Expected                    model.Record
	}
	raw, e := os.ReadFile("testdata/event_contract.json")
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(raw, &steps); e != nil {
		t.Fatal(e)
	}
	dir := t.TempDir()
	root, child := filepath.Join(dir, "root.jsonl"), filepath.Join(dir, "child.jsonl")
	for _, path := range []string{root, child} {
		if err := os.WriteFile(path, nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	o := New(root, []string{child})
	for _, step := range steps {
		t.Run(step.Name, func(t *testing.T) {
			p := root
			if step.Target == "child" {
				p = child
			}
			flags := os.O_WRONLY | os.O_APPEND
			if step.Mode == "replace" {
				flags = os.O_WRONLY | os.O_TRUNC
			}
			f, e := os.OpenFile(p, flags, 0600)
			if e != nil {
				t.Fatal(e)
			}
			_, writeErr := f.WriteString(step.Content)
			closeErr := f.Close()
			if writeErr != nil {
				t.Fatal(writeErr)
			}
			if closeErr != nil {
				t.Fatal(closeErr)
			}
			actual, _, _ := o.Read()
			if !reflect.DeepEqual(canonical(actual), canonical(step.Expected)) {
				a, _ := json.MarshalIndent(canonical(actual), "", " ")
				b, _ := json.MarshalIndent(canonical(step.Expected), "", " ")
				t.Fatalf("actual:\n%s\nexpected:\n%s", a, b)
			}
			again, _, _ := o.Read()
			if actual["revision"] != again["revision"] {
				t.Fatal("idle read changed revision")
			}
		})
	}
}

// An HTTP encoder can still be using a previous result when the next poll updates
// a node or resets the transcript. Neither operation may mutate that result.
func TestPublishedSnapshotsSurviveConcurrentUpdatesAndReset(t *testing.T) {
	source := filepath.Join(t.TempDir(), "root.jsonl")
	initial := `{"type":"session_meta","payload":{"id":"root"}}
{"type":"event_msg","timestamp":"2026-10-15T01:00:00Z","payload":{"type":"task_started","turn_id":"turn"}}
{"type":"response_item","timestamp":"2026-10-15T01:00:01Z","payload":{"type":"function_call","call_id":"call","name":"tool"}}
`
	if e := os.WriteFile(source, []byte(initial), 0600); e != nil {
		t.Fatal(e)
	}
	o := New(source, nil)
	old, _, _ := o.Read()
	before, e := json.Marshal(old)
	if e != nil {
		t.Fatal(e)
	}
	done := make(chan bool, 1)
	go func() {
		for i := 0; i < 300; i++ {
			b, e := json.Marshal(old)
			if e != nil || string(b) != string(before) {
				done <- false
				return
			}
		}
		done <- true
	}()
	for i := 0; i < 50; i++ {
		f, e := os.OpenFile(source, os.O_APPEND|os.O_WRONLY, 0600)
		if e != nil {
			t.Fatal(e)
		}
		_, e = f.WriteString("{\"type\":\"response_item\",\"timestamp\":\"2026-10-15T01:00:02Z\",\"payload\":{\"type\":\"function_call_output\",\"call_id\":\"call\"}}\n")
		f.Close()
		if e != nil {
			t.Fatal(e)
		}
		state, nodes, _ := o.Read()
		if nodes[len(nodes)-1].String("status") != "반환 관측" {
			t.Fatal(state)
		}
	}
	if e := os.WriteFile(source, []byte("{\"type\":\"session_meta\",\"payload\":{\"id\":\"root\"}}\n"), 0600); e != nil {
		t.Fatal(e)
	}
	_, nodes, root := o.Read()
	if len(nodes) != 0 || root != "root" {
		t.Fatal("cached snapshot survived replacement")
	}
	if !<-done {
		t.Fatal("previous response mutated while encoding")
	}
	after, _ := json.Marshal(old)
	if string(after) != string(before) {
		t.Fatal("previous snapshot mutated")
	}
}
