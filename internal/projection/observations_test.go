package projection

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"agent-observer/internal/model"
)

func TestFromGraphPrivacyAndImmutability(t *testing.T) {
	node := model.Record{
		"id": "tool-id", "kind": "tool", "parent": "agent", "status": "완료 관측",
		"at": "2026-10-16T10:00:00+09:00", "toolName": "custom-sensitive-tool",
		"label": "PRIVATE-LABEL", "body": "PRIVATE-BODY", "arguments": "PRIVATE-ARGUMENTS",
		"output": "PRIVATE-OUTPUT", "path": "PRIVATE-PATH", "browserSites": []string{"PRIVATE-SITE"},
		"reasoning": "PRIVATE-REASONING", "arbitrary": map[string]string{"secret": "PRIVATE-SECRET"},
	}
	state := model.Record{
		"turn": "turn-id", "started": "2026-10-16T00:59:00Z", "nodes": []model.Record{node},
		"title": "PRIVATE-TITLE", "sessionLabel": "PRIVATE-SESSION", "source": "PRIVATE-SOURCE",
	}
	before, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	observations := FromGraph("codex", "session-id", state)
	if len(observations) != 1 {
		t.Fatalf("got %d observations, want 1", len(observations))
	}
	got := observations[0]
	if got.ToolName != "tool" || got.Kind != "tool" || got.Outcome != "completed" || got.ParentID != "agent" {
		t.Fatalf("unexpected metadata: %+v", got)
	}
	if !got.StartedAt.IsZero() || !got.EndedAt.IsZero() || got.ObservedAt.Format(time.RFC3339) != "2026-10-16T01:00:00Z" {
		t.Fatalf("invented execution times or lost observed time: %+v", got)
	}
	encoded, err := json.Marshal(observations)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "PRIVATE-") || strings.Contains(string(encoded), "custom-sensitive-tool") {
		t.Fatalf("private graph fields reached export projection: %s", encoded)
	}
	observations[0].ID = "different"
	after, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("projection or returned value mutation changed source graph")
	}
}

func TestFromGraphWaitsForExecutionEvidence(t *testing.T) {
	nodes := []model.Record{
		{"id": "request", "kind": "request", "status": "요청 관측"},
		{"id": "pending-call", "kind": "call", "status": "반환 대기"},
		{"id": "unknown-call", "kind": "call", "status": "unrecognized"},
		{"id": "active-agent", "kind": "agent", "status": "작업 중"},
		{"id": "active-child", "kind": "subagent", "status": "생성 관측"},
		{"id": "active-tool", "kind": "tool", "status": "실행 중"},
		{"id": "returned", "kind": "call", "status": "반환 관측"},
		{"id": "complete-agent", "kind": "agent", "status": "턴 종료"},
		{"id": "complete-child", "kind": "subagent", "status": "완료 관측"},
		{"id": "read", "kind": "skill", "status": "읽기 성공"},
		{"id": "attempt", "kind": "skill", "status": "읽기 시도"},
		{"id": "a2a", "kind": "a2a", "status": "호출 반환 관측"},
		{"id": "future", "kind": "some-new-kind", "status": "완료 관측"},
		{"kind": "tool", "status": "완료 관측"},
	}
	state := model.Record{"turn": "turn", "nodes": nodes}
	got := FromGraph("codex", "session", state)
	ids, outcomes := []string{}, []string{}
	for _, observation := range got {
		ids = append(ids, observation.ID)
		outcomes = append(outcomes, observation.Outcome)
	}
	if want := []string{"request", "returned", "complete-agent", "complete-child", "read", "attempt", "a2a"}; !reflect.DeepEqual(ids, want) {
		t.Fatalf("export IDs = %v, want %v", ids, want)
	}
	if want := []string{"observed", "returned", "completed", "completed", "read", "read-attempt", "returned"}; !reflect.DeepEqual(outcomes, want) {
		t.Fatalf("export outcomes = %v, want %v", outcomes, want)
	}
}

func TestFromGraphRequiresRunIdentity(t *testing.T) {
	state := model.Record{"turn": "turn", "nodes": []model.Record{{"id": "request", "kind": "request"}}}
	for _, input := range []struct{ provider, session string }{{"", "session"}, {"arbitrary", "session"}, {"codex", ""}} {
		if got := FromGraph(input.provider, input.session, state); len(got) != 0 {
			t.Fatalf("incomplete identity %+v produced %v", input, got)
		}
	}
	state["turn"] = ""
	if got := FromGraph("codex", "session", state); len(got) != 0 {
		t.Fatal("unidentified turn produced observations")
	}
}

func TestGraphNodesSupportsDecodedJSON(t *testing.T) {
	var state model.Record
	if err := json.Unmarshal([]byte(`{"turn":"t","nodes":[{"id":"a","kind":"tool","status":"completed","startMs":1000,"endMs":2000}]}`), &state); err != nil {
		t.Fatal(err)
	}
	got := FromGraph("claude-code", "s", state)
	if len(got) != 1 || !got[0].StartedAt.Equal(time.Unix(1, 0)) || !got[0].EndedAt.Equal(time.Unix(2, 0)) {
		t.Fatalf("decoded graph observations = %+v", got)
	}
}

func TestExecutionTimesPreserveUnknowns(t *testing.T) {
	for _, test := range []struct {
		name       string
		start, end any
		knownStart bool
		knownEnd   bool
	}{
		{"missing", nil, nil, false, false},
		{"epoch", float64(0), int64(1000), true, true},
		{"partial", int(1000), nil, true, false},
		{"json-number", json.Number("1000"), json.Number("2000"), true, true},
		{"reversed", float64(2000), float64(1000), false, false},
		{"non-finite", math.NaN(), math.Inf(1), false, false},
		{"fractional", float64(0.25), "1000", false, false},
		{"out-of-range", float64(-1), float64(253402300800000), false, false},
		{"invalid-number", json.Number("no"), nil, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			start, end := executionTimes(model.Record{"startMs": test.start, "endMs": test.end})
			if (!start.IsZero()) != test.knownStart || (!end.IsZero()) != test.knownEnd {
				t.Errorf("known start/end = %v/%v, want %v/%v", !start.IsZero(), !end.IsZero(), test.knownStart, test.knownEnd)
			}
		})
	}
	if got := timestamp("missing"); !got.IsZero() {
		t.Fatalf("invalid timestamp became %s", got)
	}
}
