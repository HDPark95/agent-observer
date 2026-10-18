package sources

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"agent-observer/internal/model"
)

func captureFixture(t *testing.T, file string, event model.Record) {
	t.Helper()
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if err := CaptureClaudeHook(bytes.NewReader(data), file); err != nil {
		t.Fatal(err)
	}
}

func hook(kind string) model.Record {
	return model.Record{"hook_event_name": kind, "session_id": "session-one", "prompt_id": "prompt-one", "cwd": "/private/project", "transcript_path": "/do-not-read/private-transcript.jsonl"}
}

func nodesByID(nodes []model.Record) map[string]model.Record {
	result := make(map[string]model.Record, len(nodes))
	for _, n := range nodes {
		result[n.String("id")] = n
	}
	return result
}

func TestClaudeCollectionLifecycleAndPrivacy(t *testing.T) {
	file := filepath.Join(t.TempDir(), "claude.jsonl")
	start := hook("UserPromptSubmit")
	start["prompt"] = "secret prompt body"
	captureFixture(t, file, start)
	meta, err := Header(ClaudeCode, file)
	if err != nil || meta.SessionID != "session-one" || meta.Provider != ClaudeCode || meta.Project != "project" {
		t.Fatalf("metadata = %+v, %v", meta, err)
	}
	reader, err := Open(ClaudeCode, file, nil)
	if err != nil {
		t.Fatal(err)
	}
	before, oldNodes, root := reader.Read()
	oldJSON, err := json.Marshal(oldNodes)
	if err != nil {
		t.Fatal(err)
	}
	if root != "session-one" || before.String("turn") != "prompt-one" || len(oldNodes) != 2 {
		t.Fatalf("initial state = %v", before)
	}
	agent := hook("SubagentStart")
	agent["agent_id"], agent["agent_type"] = "agent-a", "Explore"
	captureFixture(t, file, agent)
	pre := hook("PreToolUse")
	pre["agent_id"], pre["agent_type"] = "agent-a", "Explore"
	pre["tool_use_id"], pre["tool_name"] = "tool-one", "Read"
	pre["tool_input"] = model.Record{"file_path": "/private/skills/google-go-style/SKILL.md", "command": "secret shell argument"}
	captureFixture(t, file, pre)
	state, nodes, _ := reader.Read()
	byID := nodesByID(nodes)
	if byID["claude:tool:tool-one"].String("parent") != "subagent:claude:agent-a" || byID["claude:tool:tool-one"].String("status") != "실행 중" {
		t.Fatalf("tool ownership = %v", byID)
	}
	if byID["claude:tool:tool-one:skill:google-go-style"].String("status") != "읽기 시도" {
		t.Fatalf("skill missing: %v", byID)
	}
	previous, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	post := pre.Clone()
	post["hook_event_name"], post["duration_ms"] = "PostToolUse", float64(12)
	post["tool_response"] = model.Record{"content": "secret tool output"}
	captureFixture(t, file, post)
	agent["hook_event_name"], agent["last_assistant_message"] = "SubagentStop", "secret assistant text"
	captureFixture(t, file, agent)
	captureFixture(t, file, hook("Stop"))
	updated, final, _ := reader.Read()
	byID = nodesByID(final)
	if byID["claude:tool:tool-one"].String("status") != "완료 관측" || byID["claude:tool:tool-one"].Number("durationMs") != 12 {
		t.Fatalf("completion = %v", byID)
	}
	if byID["claude:tool:tool-one:skill:google-go-style"].String("status") != "읽기 성공" || byID["subagent:claude:agent-a"].String("status") != "완료 관측" || byID["agent"].String("status") != "턴 종료" {
		t.Fatalf("terminal states = %v", byID)
	}
	again, _, _ := reader.Read()
	if again["revision"] != updated["revision"] {
		t.Fatal("idle read changed revision")
	}
	oldAfter, err := json.Marshal(oldNodes)
	if err != nil {
		t.Fatal(err)
	}
	previousAfter, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(oldJSON, oldAfter) || !bytes.Equal(previous, previousAfter) {
		t.Fatal("published snapshot mutated")
	}
	stored, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"secret", "transcript_path", "/private", "tool_input", "tool_response", "last_assistant_message"} {
		if strings.Contains(string(stored), forbidden) {
			t.Fatalf("raw field persisted: %q", forbidden)
		}
	}
	if dirs := reader.WatchDirectories(); len(dirs) != 1 || dirs[0] != filepath.Dir(file) {
		t.Fatalf("watch scope = %v", dirs)
	}
}

func TestClaudeCaptureRejectsMixedSources(t *testing.T) {
	file := filepath.Join(t.TempDir(), "claude.jsonl")
	captureFixture(t, file, hook("SessionStart"))
	other := hook("UserPromptSubmit")
	other["session_id"] = "other-session"
	data, err := json.Marshal(other)
	if err != nil {
		t.Fatal(err)
	}
	if err := CaptureClaudeHook(bytes.NewReader(data), file); err == nil {
		t.Fatal("accepted another session")
	}
	if _, err := Open(ClaudeCode, file, []string{"child.jsonl"}); err == nil {
		t.Fatal("accepted Codex child transcript")
	}
	if _, err := Open("unknown", file, nil); err == nil {
		t.Fatal("accepted unknown provider")
	}
	if _, err := Header(Codex, file); err == nil {
		t.Fatal("accepted Claude source as Codex")
	}
	link := filepath.Join(filepath.Dir(file), "link.jsonl")
	if err := os.Symlink(file, link); err != nil {
		t.Fatal(err)
	}
	if err := CaptureClaudeHook(bytes.NewReader(data), link); err == nil {
		t.Fatal("accepted symlink output")
	}
	if _, err := Header(ClaudeCode, link); err == nil {
		t.Fatal("accepted symlink input")
	}
}

func TestClaudeCaptureConcurrentAppends(t *testing.T) {
	file := filepath.Join(t.TempDir(), "claude.jsonl")
	captureFixture(t, file, hook("UserPromptSubmit"))
	var group sync.WaitGroup
	errors := make(chan error, 12)
	for i := 0; i < 12; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			data, err := json.Marshal(hook("Stop"))
			if err == nil {
				err = CaptureClaudeHook(bytes.NewReader(data), file)
			}
			errors <- err
		}()
	}
	group.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(data), []byte("\n"))
	if len(lines) != 13 {
		t.Fatalf("got %d records, want 13", len(lines))
	}
	for _, line := range lines {
		var e claudeEvent
		if err := json.Unmarshal(line, &e); err != nil {
			t.Fatal(err)
		}
		if err := e.validate(); err != nil {
			t.Fatal(err)
		}
	}
}

func appendEvent(t *testing.T, file string, e claudeEvent) {
	t.Helper()
	data, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(file, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(append(data, '\n')); err != nil {
		t.Fatal(err)
	}
}

func TestClaudePromptIsolationFailureAndReplacement(t *testing.T) {
	file := filepath.Join(t.TempDir(), "claude.jsonl")
	captureFixture(t, file, hook("UserPromptSubmit"))
	reader, err := Open(ClaudeCode, file, nil)
	if err != nil {
		t.Fatal(err)
	}
	reader.Read()
	start := hook("UserPromptSubmit")
	start["prompt_id"] = "prompt-two"
	captureFixture(t, file, start)
	late := hook("PostToolUse")
	late["tool_name"], late["tool_use_id"] = "Bash", "old-tool"
	captureFixture(t, file, late)
	failure := hook("PostToolUseFailure")
	failure["prompt_id"], failure["tool_name"], failure["tool_use_id"], failure["error"] = "prompt-two", "Bash", "failed-tool", "sensitive failure text"
	captureFixture(t, file, failure)
	state, nodes, _ := reader.Read()
	byID := nodesByID(nodes)
	if state.String("turn") != "prompt-two" || byID["claude:tool:old-tool"] != nil || byID["claude:tool:failed-tool"].String("status") != "오류" {
		t.Fatalf("mixed turn or missing failure = %v", state)
	}
	// Retried prompt notifications and late starts cannot reset a newer graph.
	captureFixture(t, file, hook("UserPromptSubmit"))
	captureFixture(t, file, start)
	failure["hook_event_name"] = "PreToolUse"
	captureFixture(t, file, failure)
	state, nodes, _ = reader.Read()
	if state.String("turn") != "prompt-two" || nodesByID(nodes)["claude:tool:failed-tool"].String("status") != "오류" {
		t.Fatalf("replayed prompt or tool start regressed state: %v", state)
	}
	e, err := readClaudeHook(strings.NewReader(`{"hook_event_name":"Stop","session_id":"other-session"}`))
	if err != nil {
		t.Fatal(err)
	}
	appendEvent(t, file, *e)
	state, _, root := reader.Read()
	if state["error"] == nil || root != "session-one" {
		t.Fatalf("mixed session not rejected: %v", state)
	}
	if err := os.WriteFile(file, nil, 0600); err != nil {
		t.Fatal(err)
	}
	captureFixture(t, file, hook("SessionStart"))
	state, nodes, _ = reader.Read()
	if state["turn"] != nil || len(nodes) != 0 || state["error"] != nil {
		t.Fatalf("replacement retained old graph: %v", state)
	}
}

func TestClaudeCaptureValidation(t *testing.T) {
	for _, input := range []string{
		`{"hook_event_name":"UserPromptSubmit","session_id":"../wrong"}`,
		`{"hook_event_name":"PreToolUse","session_id":"one","tool_name":"Read"}`,
		`{"hook_event_name":"SubagentStop","session_id":"one"}`,
		`{"hook_event_name":"PostToolUse","session_id":"one","tool_name":"Read","tool_use_id":"tool","duration_ms":-1}`,
		`{"hook_event_name":`,
	} {
		if _, err := readClaudeHook(strings.NewReader(input)); err == nil {
			t.Fatalf("accepted malformed event: %s", input)
		}
	}
	file := filepath.Join(t.TempDir(), "ignored.jsonl")
	if err := CaptureClaudeHook(strings.NewReader(`{"hook_event_name":"Notification","session_id":"one","message":"private"}`), file); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(file); !os.IsNotExist(err) {
		t.Fatal("unsupported event created capture file")
	}
	if _, err := readClaudeHook(strings.NewReader(strings.Repeat(" ", 1000001))); err == nil {
		t.Fatal("oversize hook accepted")
	}
}

func TestClaudeNativeSkillAndUnknownFields(t *testing.T) {
	e, err := readClaudeHook(strings.NewReader(`{"hook_event_name":"PostToolUse","session_id":"one","tool_use_id":"tool","tool_name":"Skill","tool_input":{"skill":"plugin:review","args":"private arguments"}}`))
	if err != nil || e.SkillName != "plugin:review" {
		t.Fatalf("native Skill metadata = %v, %v", e, err)
	}
	data, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	var row model.Record
	if err := json.Unmarshal(data, &row); err != nil {
		t.Fatal(err)
	}
	row["body"] = "private body"
	if _, err := decodeClaude(row); err == nil {
		t.Fatal("foreign raw fields accepted as Observer schema")
	}
}

func TestClaudePartialRecordAndMissingPromptIdentity(t *testing.T) {
	file := filepath.Join(t.TempDir(), "claude.jsonl")
	e, err := readClaudeHook(strings.NewReader(`{"hook_event_name":"UserPromptSubmit","session_id":"one"}`))
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, data, 0600); err != nil {
		t.Fatal(err)
	}
	reader, err := Open(ClaudeCode, file, nil)
	if err != nil {
		t.Fatal(err)
	}
	state, nodes, _ := reader.Read()
	if len(nodes) != 0 || state["turn"] != nil {
		t.Fatal("partial record projected")
	}
	if err := CaptureClaudeHook(strings.NewReader(`{"hook_event_name":"Stop","session_id":"one"}`), file); err == nil {
		t.Fatal("appended after partial record")
	}
	if err := os.WriteFile(file, append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	state, nodes, _ = reader.Read()
	if state.String("turn") != e.EventID || len(nodes) != 2 {
		t.Fatalf("missing prompt fallback = %v", state)
	}
}

func TestCodexDefaultAdapterPreservesContract(t *testing.T) {
	file := filepath.Join(t.TempDir(), "codex.jsonl")
	data := `{"type":"session_meta","payload":{"id":"codex-one","cwd":"/project"}}
{"type":"event_msg","timestamp":"2026-10-16T00:00:00Z","payload":{"type":"task_started","turn_id":"turn-one"}}
`
	if err := os.WriteFile(file, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	meta, err := Header("", file)
	if err != nil || meta.Provider != Codex || meta.SessionID != "codex-one" {
		t.Fatalf("metadata = %+v, %v", meta, err)
	}
	r, err := Open("", file, nil)
	if err != nil {
		t.Fatal(err)
	}
	state, nodes, id := r.Read()
	if state.String("turn") != "turn-one" || id != "codex-one" || len(nodes) != 2 {
		t.Fatalf("default Codex changed: %v", state)
	}
}
