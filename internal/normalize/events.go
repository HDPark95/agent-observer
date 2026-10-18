// Package normalize interprets metadata records. It has no file or network access.
package normalize

import (
	"path"
	"regexp"
	"strings"
	"time"

	"agent-observer/internal/model"
)

var ValidID = regexp.MustCompile(`^[\pL\pN_-]{1,100}$`)
var agentPath = regexp.MustCompile(`^/root(?:/[A-Za-z0-9_-]+)*$`)

func AgentPath(s string) bool { return agentPath.MatchString(s) }
func AgentID(s string) string {
	if s == "/root" {
		return "agent"
	}
	return "subagent:" + s
}

var callName = regexp.MustCompile(`^[\pL\pN_.:_-]{1,100}$`)
var mcpName = regexp.MustCompile(`^[\pL\pN_ ./:-]{1,160}$`)
var skillName = regexp.MustCompile(`^[\pL\pN_.-]{1,100}$`)

// State preserves insertion order so snapshots match the established graph contract.
type State struct {
	Nodes                     map[string]model.Record
	Order                     []string
	Turn, Started, Root, Last string
	Revision                  uint64
}

func New() *State { return &State{Nodes: map[string]model.Record{}, Order: []string{}} }
func (s *State) Put(id, kind, label, stamp, status string, parent any, extra model.Record) {
	if id == "" {
		return
	}
	n := s.Nodes[id]
	if n == nil {
		n = model.Record{"at": model.Nullable(stamp)}
		s.Order = append(s.Order, id)
	} else {
		n = n.Clone() // Previously published records remain immutable.
	}
	n["id"] = id
	n["kind"] = kind
	n["label"] = label
	n["status"] = status
	n["parent"] = parent
	n["seen"] = float64(time.Now().UnixNano()) / 1e9
	for k, v := range extra {
		n[k] = v
	}
	s.Nodes[id] = n
}

// List returns a new ordered slice of immutable records. Callers must clone a
// record before changing it; Put replaces records instead of mutating them.
func (s *State) List() []model.Record {
	out := make([]model.Record, 0, len(s.Order))
	for _, id := range s.Order {
		if n := s.Nodes[id]; n != nil {
			out = append(out, n)
		}
	}
	return out
}
func (s *State) Agent(item model.Record, stamp string) {
	p := item.String("agent_path")
	status := map[string]string{"started": "생성 관측", "interacted": "추가 작업 관측", "completed": "완료 관측"}[item.String("kind")]
	if !AgentPath(p) || p == "/root" || status == "" {
		return
	}
	s.Put(AgentID(p), "subagent", path.Base(p), stamp, status, AgentID(path.Dir(p)), model.Record{"agentPath": p, "threadId": item["agent_thread_id"], "updatedAt": stamp, "evidence": "SubAgentActivity · 에이전트 상태 기록"})
}
func (s *State) Consume(row model.Record) {
	p := row.Object("payload")
	typ := p.String("type")
	stamp := row.String("timestamp")
	event := row.String("type")
	if event == "session_meta" && s.Root == "" {
		s.Root = p.String("id")
	}
	if event == "event_msg" && typ == "task_started" {
		s.Turn = p.String("turn_id")
		s.Started = stamp
		s.Nodes = map[string]model.Record{}
		s.Order = []string{}
		s.Put("request", "request", "사용자 요청", stamp, "요청 관측", nil, nil)
		s.Put("agent", "agent", "Codex 에이전트", stamp, "작업 중", "request", nil)
	}
	if s.Turn == "" {
		return
	}
	if event == "event_msg" && typ == "task_complete" && p.String("turn_id") == s.Turn {
		s.Put("agent", "agent", "Codex 에이전트", stamp, "턴 종료", "request", nil)
	}
	if event == "response_item" {
		id := p.String("call_id")
		if typ == "custom_tool_call" || typ == "function_call" {
			name := p.String("name")
			if !callName.MatchString(name) {
				name = "tool"
			}
			s.Put(id, "call", name, stamp, "반환 대기", "agent", model.Record{"evidence": "도구 호출 시작 기록", "scope": "호출 묶음 · 내부 도구와 중복 집계 금지"})
		} else if typ == "custom_tool_call_output" || typ == "function_call_output" {
			if n := s.Nodes[id]; n != nil {
				s.Put(id, n.String("kind"), n.String("label"), stamp, "반환 관측", "agent", model.Record{"ended": model.Nullable(stamp)})
			}
		}
	}
	if event == "event_msg" && typ == "item_completed" {
		if p.String("turn_id") != s.Turn {
			return
		}
		s.Item(p, stamp)
	}
	s.Last = stamp
	s.Revision++
}
func (s *State) Item(p model.Record, stamp string) {
	item := p.Object("item")
	kind := item.String("type")
	id := item.String("id")
	extra := model.Record{"evidence": "item_completed · 완료 후 관측", "startMs": p["started_at_ms"], "endMs": p["completed_at_ms"]}
	switch kind {
	case "SubAgentActivity":
		s.Agent(item, stamp)
	case "CommandExecution":
		label, tool := Command(item)
		status := "완료 관측"
		if item.Number("exit_code") != 0 {
			status = "오류"
		}
		extra["toolName"] = tool
		extra["exitCode"] = item["exit_code"]
		s.Put(id, "tool", label, stamp, status, "agent", extra)
		for _, v := range item.Array("parsed_cmd") {
			cmd := model.As(v)
			p := cmd.String("path")
			if cmd.String("type") == "read" && strings.HasSuffix(p, "/SKILL.md") {
				name := path.Base(path.Dir(p))
				if skillName.MatchString(name) {
					s.Put(id+":skill:"+name, "skill", name, stamp, "읽기 시도", id, model.Record{"evidence": "parsed_cmd.type=read · 개별 읽기 결과 미수집 · 실제 적용 여부 미판정"})
				}
			}
		}
	case "McpToolCall":
		if b := Browser(item); b != nil {
			label := b.String("label")
			delete(b, "label")
			for k, v := range b {
				extra[k] = v
			}
			status := "호출 완료 관측"
			if item.String("status") == "failed" || item.Object("result").Bool("isError") {
				status = "오류"
			}
			s.Put(id, "tool", label, stamp, status, "agent", extra)
		} else {
			label := item.String("server") + " / " + item.String("tool")
			if !mcpName.MatchString(label) {
				label = "MCP 도구"
			}
			status := item.String("status")
			if status == "" {
				status = "완료 관측"
			}
			s.Put(id, "tool", label, stamp, status, "agent", extra)
		}
	case "ImageView", "FileChange", "WebSearch":
		s.Put(id, "tool", kind, stamp, "완료 관측", "agent", extra)
	case "AgentMessage":
		label := "진행 메시지"
		if item.String("phase") == "final" {
			label = "최종 답변"
		}
		s.Put(id, "message", label, stamp, "완료 관측", "agent", model.Record{"evidence": "메시지 메타데이터 · 원문 미전송"})
	}
}
