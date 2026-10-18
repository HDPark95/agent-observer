package sources

import (
	"path/filepath"
	"sync"
	"time"

	"agent-observer/internal/model"
	"agent-observer/internal/transcript"
)

type claudeReader struct {
	mu                           sync.Mutex
	cursor                       transcript.Cursor
	session, turn, started, last string
	nodes                        map[string]model.Record
	order                        []string
	seen                         map[string]bool
	prompts                      map[string]bool
	revision                     uint64
	snapshot                     []model.Record
	rejected                     bool
	history                      bool
	turns                        []model.Record
}

func newClaude(file, session string) *claudeReader {
	r := &claudeReader{cursor: transcript.Cursor{Path: file}, session: session}
	r.reset()
	return r
}

func (r *claudeReader) reset() {
	r.turn, r.started, r.last = "", "", ""
	r.nodes, r.seen = map[string]model.Record{}, map[string]bool{}
	r.prompts = map[string]bool{}
	r.order, r.snapshot = []string{}, nil
	r.rejected = false
	r.turns = nil
	r.revision++
}

func (r *claudeReader) Read() (model.Record, []model.Record, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var problem any
	if err := r.cursor.Read(r.reset, r.consume); err != nil {
		problem = "Claude Code 관측 기록을 읽을 수 없습니다."
	}
	if r.rejected {
		problem = "다른 세션 또는 잘못된 Claude Code 관측 기록을 제외했습니다."
	}
	state := r.snapshotState(problem)
	return state, r.snapshot, r.session
}

func (r *claudeReader) snapshotState(problem any) model.Record {
	if r.snapshot == nil {
		r.snapshot = make([]model.Record, 0, len(r.order))
		for _, id := range r.order {
			r.snapshot = append(r.snapshot, r.nodes[id])
		}
	}
	return model.Record{"turn": model.Nullable(r.turn), "started": model.Nullable(r.started), "revision": r.revision, "nodes": r.snapshot, "lastEvent": model.Nullable(r.last), "checkedAt": time.Now().UTC().Format(time.RFC3339Nano), "error": problem, "mode": "Claude Code 훅 · 메타데이터 증분 읽기", "autoAgents": model.Record{"connected": 0, "pending": 0}, "agentSourceErrors": []string{}}
}

func (r *claudeReader) WatchDirectories() []string { return []string{filepath.Dir(r.cursor.Path)} }

func (r *claudeReader) consume(row model.Record) {
	e, err := decodeClaude(row)
	if err != nil || e.SessionID != r.session {
		if !r.rejected {
			r.rejected = true
			r.revision++
		}
		return
	}
	if r.seen[e.EventID] {
		return
	}
	r.seen[e.EventID] = true
	if e.Kind == "SessionStart" {
		return
	}
	if e.Kind == "UserPromptSubmit" && e.AgentID == "" {
		if e.PromptID != "" && r.prompts[e.PromptID] {
			if e.PromptID != r.turn || r.nodes["request"].String("status") == "요청 관측" {
				return
			}
			r.put("request", "request", "사용자 요청", "요청 관측", "", e, nil)
		} else {
			r.startTurn(e, true)
		}
	} else {
		if r.turn == "" {
			r.startTurn(e, false)
		}
		if e.PromptID != "" && e.PromptID != r.turn {
			return
		}
		r.apply(e)
	}
	r.last = e.ObservedAt
	r.revision++
	r.snapshot = nil
}

func (r *claudeReader) startTurn(e claudeEvent, observed bool) {
	if r.history && r.turn != "" {
		r.turns = append(r.turns, r.snapshotState(nil))
	}
	r.turn = e.PromptID
	if r.turn == "" {
		r.turn = e.EventID
	}
	r.started = e.ObservedAt
	if e.PromptID != "" {
		r.prompts[e.PromptID] = true
	}
	r.nodes, r.order = map[string]model.Record{}, []string{}
	status := "요청 경계 미관측"
	if observed {
		status = "요청 관측"
	}
	r.put("request", "request", "사용자 요청", status, "", e, nil)
	r.put("agent", "agent", "Claude Code 에이전트", "작업 중", "request", e, nil)
}

// put replaces a changed record; previously returned snapshots remain immutable.
func (r *claudeReader) put(id, kind, label, status, parent string, e claudeEvent, extra model.Record) {
	n := r.nodes[id]
	if n == nil {
		n = model.Record{"id": id, "at": e.ObservedAt}
		r.order = append(r.order, id)
	} else {
		n = n.Clone()
	}
	n["kind"], n["label"], n["status"], n["parent"] = kind, label, status, model.Nullable(parent)
	n["updatedAt"], n["evidence"] = e.ObservedAt, "Claude Code "+e.Kind+" · 훅 수신 시각"
	n["scope"] = "본문·명령·결과 미수집 · 수신 시각은 실제 실행 시작/종료 시각과 다를 수 있습니다."
	for key, value := range extra {
		n[key] = value
	}
	r.nodes[id] = n
}

func (r *claudeReader) apply(e claudeEvent) {
	switch e.Kind {
	case "SubagentStart", "SubagentStop":
		status := "생성 관측"
		if e.Kind == "SubagentStop" {
			status = "완료 관측"
		}
		r.agent(e, status)
	case "PreToolUse", "PostToolUse", "PostToolUseFailure":
		r.tool(e)
	case "Stop", "StopFailure", "SessionEnd":
		status := "턴 종료"
		if e.Kind == "StopFailure" {
			status = "오류"
		}
		if e.AgentID == "" {
			r.put("agent", "agent", "Claude Code 에이전트", status, "request", e, model.Record{"ended": e.ObservedAt})
		} else {
			r.agent(e, status)
		}
	}
}

func (r *claudeReader) agent(e claudeEvent, status string) string {
	if e.AgentID == "" {
		return "agent"
	}
	id := "subagent:claude:" + e.AgentID
	label := e.AgentType
	if label == "" {
		label = r.nodes[id].String("label")
		if label == "" {
			label = e.AgentID
		}
	}
	r.put(id, "subagent", label, status, "agent", e, model.Record{"threadId": e.AgentID, "scope": "같은 Claude Code 세션 소속 · 중첩 생성자 관계 미수집"})
	return id
}

func (r *claudeReader) tool(e claudeEvent) {
	owner := "agent"
	if e.AgentID != "" {
		owner = "subagent:claude:" + e.AgentID
		if r.nodes[owner] == nil {
			r.agent(e, "활동 관측")
		}
	}
	id := "claude:tool:" + e.ToolID
	if previous := r.nodes[id]; previous != nil && previous.String("ended") != "" && e.Kind == "PreToolUse" {
		return
	}
	status := "실행 중"
	extra := model.Record{"toolName": e.ToolName, "ownerAgent": owner}
	if e.Kind != "PreToolUse" {
		status = "완료 관측"
		extra["ended"] = e.ObservedAt
		if e.Kind == "PostToolUseFailure" {
			status = "오류"
		}
	}
	if e.DurationMS != nil {
		extra["durationMs"] = *e.DurationMS
	}
	label := map[string]string{"Bash": "셸 명령 실행", "Read": "파일 읽기", "Write": "파일 작성", "Edit": "파일 수정", "Glob": "파일 목록 검색", "Grep": "내용 검색", "WebFetch": "웹 문서 읽기", "WebSearch": "웹 검색", "Agent": "서브에이전트 실행", "Skill": "스킬 호출"}[e.ToolName]
	if label == "" {
		label = e.ToolName
	}
	r.put(id, "tool", label, status, owner, e, extra)
	r.skill(e, id, owner)
}

func (r *claudeReader) skill(e claudeEvent, toolID, owner string) {
	if e.SkillName == "" {
		return
	}
	status := "읽기 시도"
	if e.ToolName == "Skill" {
		status = "호출 관측"
	}
	if e.Kind == "PostToolUse" {
		status = "읽기 성공"
		if e.ToolName == "Skill" {
			status = "호출 완료 관측"
		}
	}
	if e.Kind == "PostToolUseFailure" {
		status = "오류"
	}
	r.put(toolID+":skill:"+e.SkillName, "skill", e.SkillName, status, toolID, e, model.Record{"ownerAgent": owner, "evidence": "Claude Code " + e.ToolName + " 훅 · 실제 적용 여부 미판정"})
}
