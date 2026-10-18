package agents

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestDiscoveryDirectoriesBoundedByUTCDate(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "2026", "10", "01", "parent.jsonl")
	tests := []struct {
		name  string
		stamp string
		days  []string
	}{
		{name: "KST next day", stamp: "2026-10-15T23:22:30.700Z", days: []string{"2026/10/14", "2026/10/15", "2026/10/16"}},
		{name: "offset converted to UTC", stamp: "2026-10-16T08:22:30.700+09:00", days: []string{"2026/10/14", "2026/10/15", "2026/10/16"}},
		{name: "year boundary", stamp: "2027-01-01T00:00:00Z", days: []string{"2026/12/31", "2027/01/01", "2027/01/02"}},
		{name: "invalid date", stamp: "2026-02-30T00:00:00Z"},
		{name: "truncated timestamp", stamp: "2026-10-15"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := map[string]bool{filepath.Dir(source): true}
			for _, day := range tt.days {
				want[filepath.Join(root, filepath.FromSlash(day))] = true
			}
			if got := DiscoveryDirectories(source, tt.stamp); !reflect.DeepEqual(got, want) {
				t.Errorf("DiscoveryDirectories(%q) = %v, want %v", tt.stamp, got, want)
			}
		})
	}
	flat := filepath.Join(root, "parent.jsonl")
	if got := DiscoveryDirectories(flat, "2026-10-15T23:22:30.700Z"); len(got) != 1 || !got[root] {
		t.Errorf("non-date source searched unrelated directories: %v", got)
	}
}

func TestDiscoveryLocalDateBoundaryPreservesIdentity(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "2026", "10", "15", "parent.jsonl")
	id := "11111111-2222-3333-4444-555555555555"
	expected := Expected{id, "/root/ux", "parent"}
	stamp := "2026-10-15T23:22:30.700Z"
	write := func(day, filename, parent string) string {
		t.Helper()
		path := filepath.Join(root, "2026", "10", day, filename)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		data := fmt.Sprintf("{\"type\":\"session_meta\",\"payload\":{\"id\":%q,\"agent_path\":\"/root/ux\",\"parent_thread_id\":%q,\"subagent_history_start_ordinal\":1}}\n", id, parent)
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	name := "rollout-test-" + id + ".jsonl"
	write("17", name, "parent")
	if Discover(source, expected, stamp) != nil {
		t.Fatal("searched outside the UTC date window")
	}
	write("16", "rollout-unrelated-99999999-2222-3333-4444-555555555555.jsonl", "parent")
	if Discover(source, expected, stamp) != nil {
		t.Fatal("accepted a file without the exact child ID in its name")
	}
	write("16", name, "wrong")
	if Discover(source, expected, stamp) != nil {
		t.Fatal("accepted a child of a different parent")
	}
	want := write("16", name, "parent")
	reader := Discover(source, expected, stamp)
	if reader == nil || reader.Cursor.Path != want {
		t.Fatalf("KST next-day child was not found: %v", reader)
	}
	write("14", name, "parent")
	if Discover(source, expected, stamp) != nil {
		t.Fatal("accepted ambiguous matches across adjacent date directories")
	}
}

func TestDiscoveryIdentityAndReplacement(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "2026", "10", "15", "parent.jsonl")
	if err := os.MkdirAll(filepath.Dir(source), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(source, nil, 0600); err != nil {
		t.Fatal(err)
	}
	id := "11111111-2222-3333-4444-555555555555"
	expected := Expected{id, "/root/ux", "parent"}
	dest := filepath.Join(dir, "2026", "10", "16", "rollout-test-"+id+".jsonl")
	if err := os.MkdirAll(filepath.Dir(dest), 0700); err != nil {
		t.Fatal(err)
	}
	write := func(path, parent string) {
		t.Helper()
		data := fmt.Sprintf("{\"type\":\"session_meta\",\"payload\":{\"id\":%q,\"agent_path\":\"/root/ux\",\"parent_thread_id\":%q,\"subagent_history_start_ordinal\":1}}\n", id, parent)
		if e := os.WriteFile(path, []byte(data), 0600); e != nil {
			t.Fatal(e)
		}
	}
	stamp := "2026-10-16T00:00:00Z"
	write(dest, "wrong")
	if Discover(source, expected, stamp) != nil {
		t.Fatal("wrong parent")
	}
	write(dest, "parent")
	r := Discover(source, expected, stamp)
	if r == nil {
		t.Fatal("date-boundary lookup failed")
	}
	if err := r.Read(); err != nil {
		t.Fatal(err)
	}
	if r.Path != "/root/ux" {
		t.Fatal("header invalid")
	}
	replacement := filepath.Join(dir, "replacement")
	write(replacement, "wrong")
	if err := os.Rename(replacement, dest); err != nil {
		t.Fatal(err)
	}
	if err := r.Read(); err != nil {
		t.Fatal(err)
	}
	if r.Path != "" {
		t.Fatal("replacement identity accepted")
	}
	write(dest, "parent")
	other := filepath.Join(filepath.Dir(dest), "rollout-other-"+id+".jsonl")
	write(other, "parent")
	if Discover(source, expected, stamp) != nil {
		t.Fatal("ambiguous match")
	}
	if err := os.Remove(other); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(dest); err != nil {
		t.Fatal(err)
	}
	write(replacement, "parent")
	if err := os.Symlink(replacement, dest); err != nil {
		t.Fatal(err)
	}
	if Discover(source, expected, stamp) != nil {
		t.Fatal("symlink match")
	}
}
