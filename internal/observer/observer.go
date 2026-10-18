// Package observer combines one root transcript and its verified child activities.
package observer

import (
	"errors"
	"path"
	"reflect"
	"sort"
	"sync"
	"time"

	"agent-observer/internal/agents"
	"agent-observer/internal/model"
	"agent-observer/internal/normalize"
	"agent-observer/internal/transcript"
)

type Observer struct {
	mu               sync.Mutex
	cursor           transcript.Cursor
	state            *normalize.State
	explicit         []*agents.Reader
	auto             map[string]*agents.Reader
	pending          int
	errors           []string
	snapshot         []model.Record
	snapshotRevision uint64
	history          bool
	selectedSession  string
	turns            []model.Record
}

func New(source string, children []string) *Observer {
	return NewPinned(source, children, "")
}

// NewPinned binds collection to metadata already validated by sources.
// An empty ID preserves direct construction, pinning the first valid header.
func NewPinned(source string, children []string, sessionID string) *Observer {
	o := &Observer{
		cursor:          transcript.Cursor{Path: source},
		state:           normalize.New(),
		auto:            map[string]*agents.Reader{},
		selectedSession: sessionID,
	}
	o.cursor.ValidateHeader = o.validateSession
	for _, p := range children {
		o.explicit = append(o.explicit, agents.New(p, nil))
	}
	return o
}

func (o *Observer) validateSession(row model.Record) error {
	p := row.Object("payload")
	id := p.String("id")
	if row.String("type") != "session_meta" ||
		!normalize.ValidID.MatchString(id) ||
		p.String("parent_thread_id") != "" ||
		(p.String("agent_path") != "" && p.String("agent_path") != "/root") {
		return errors.New("root Codex session metadata required")
	}
	if o.selectedSession != "" && id != o.selectedSession {
		return errors.New("selected Codex session identity changed")
	}
	o.selectedSession = id
	return nil
}

func (o *Observer) consumeChecked(row model.Record) error {
	if row.String("type") == "session_meta" {
		if err := o.validateSession(row); err != nil {
			return err
		}
	}
	o.stateConsume(row)
	return nil
}

func (o *Observer) reset() {
	revision := o.state.Revision
	if o.cursor.Offset > 0 {
		revision++
	}
	o.state = normalize.New()
	o.state.Revision = revision
	o.auto = map[string]*agents.Reader{}
	o.snapshot = nil
	o.turns = nil
}
func (o *Observer) connect() {
	active := map[string]model.Record{}
	for _, n := range o.state.Nodes {
		if n.String("kind") == "subagent" && n.String("threadId") != "" {
			active[n.String("threadId")] = n
		}
	}
	for id, r := range o.auto {
		if active[id] == nil || !agents.Exists(r.Cursor.Path) {
			delete(o.auto, id)
		}
	}
	o.pending = 0
	for id, n := range active {
		parentPath := path.Dir(n.String("agentPath"))
		parent := o.state.Root
		if parentPath != "/root" {
			parent = o.state.Nodes[normalize.AgentID(parentPath)].String("threadId")
		}
		e := agents.Expected{Thread: id, Path: n.String("agentPath"), Parent: parent}
		r := o.auto[id]
		if r != nil && *r.Expected != e {
			delete(o.auto, id)
			r = nil
		}
		if r == nil && parent != "" {
			r = agents.Discover(o.cursor.Path, e, n.String("at"))
			if r != nil {
				o.auto[id] = r
			}
		}
		if r == nil {
			o.pending++
		}
	}
}
func (o *Observer) merge() {
	o.mergeBefore("")
}

func (o *Observer) mergeBefore(end string) {
	o.errors = []string{}
	o.connect()
	expected := o.collectChildNodes(end)
	o.replaceChildNodes(expected)
}

// childReaders deduplicates explicit and discovered transcripts in stable order.
func (o *Observer) childReaders() []*agents.Reader {
	readers := map[string]*agents.Reader{}
	for _, r := range o.explicit {
		readers[r.Cursor.Path] = r
	}
	for _, r := range o.auto {
		readers[r.Cursor.Path] = r
	}
	paths := []string{}
	for p := range readers {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	result := make([]*agents.Reader, 0, len(paths))
	for _, p := range paths {
		result = append(result, readers[p])
	}
	return result
}

// collectChildNodes admits only current-turn records from verified owners.
func (o *Observer) collectChildNodes(end string) map[string]model.Record {
	expected := map[string]model.Record{}
	for _, r := range o.childReaders() {
		if r.Read() != nil {
			o.errors = append(o.errors, "선택한 자식 기록을 읽을 수 없습니다.")
			continue
		}
		if r.Expected != nil && r.Path == "" {
			delete(o.auto, r.Expected.Thread)
			o.pending++
			continue
		}
		owner := o.state.Nodes[normalize.AgentID(r.Path)]
		if r.Path == "" || owner == nil || owner.String("threadId") != r.Thread {
			continue
		}
		for id, n := range r.Activities {
			if o.withinTurn(n.String("at"), end) {
				expected[id] = n
			}
		}
		for _, message := range r.Calls {
			if !o.withinTurn(message.String("at"), end) {
				continue
			}
			if n := o.messageNode(message); n != nil {
				expected[n.String("id")] = n
			}
		}
	}
	return expected
}

func (o *Observer) withinTurn(stamp, end string) bool {
	observed, err := time.Parse(time.RFC3339Nano, stamp)
	if err != nil {
		return false
	}
	begin, err := time.Parse(time.RFC3339Nano, o.state.Started)
	if err != nil || observed.Before(begin) {
		return false
	}
	if end == "" {
		return true
	}
	finish, err := time.Parse(time.RFC3339Nano, end)
	return err == nil && observed.Before(finish)
}

// replaceChildNodes retains the root order and advances the revision only when
// the projected child records change. Existing published records stay immutable.
func (o *Observer) replaceChildNodes(expected map[string]model.Record) {
	old := map[string]model.Record{}
	for id, n := range o.state.Nodes {
		if n.String("kind") == "a2a" || n.Bool("childActivity") {
			old[id] = n
		}
	}
	if reflect.DeepEqual(old, expected) {
		return
	}
	order := []string{}
	for _, id := range o.state.Order {
		if old[id] == nil {
			order = append(order, id)
		}
	}
	for id := range old {
		delete(o.state.Nodes, id)
	}
	ids := []string{}
	for id := range expected {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		a, _ := time.Parse(time.RFC3339Nano, expected[ids[i]].String("at"))
		b, _ := time.Parse(time.RFC3339Nano, expected[ids[j]].String("at"))
		if a.Equal(b) {
			return ids[i] < ids[j]
		}
		return a.Before(b)
	})
	for _, id := range ids {
		o.state.Nodes[id] = expected[id]
		order = append(order, id)
	}
	o.state.Order = order
	o.state.Revision++
}

// messageNode projects an observed message only when both agents are known.
func (o *Observer) messageNode(message model.Record) model.Record {
	sender, target := normalize.AgentID(message.String("sender")), normalize.AgentID(message.String("recipient"))
	if o.state.Nodes[sender] == nil || o.state.Nodes[target] == nil {
		return nil
	}
	n := message.Clone()
	n["kind"] = "a2a"
	n["label"] = o.state.Nodes[sender].String("label") + " → " + o.state.Nodes[target].String("label")
	n["parent"] = sender
	n["source"] = sender
	n["target"] = target
	n["status"] = "송신 호출 관측"
	if message.Bool("returned") {
		n["status"] = "호출 반환 관측"
	}
	n["evidence"] = "선택한 자식 기록의 collaboration.send_message · 송수신 경로"
	n["scope"] = "호출 반환은 상대의 수신·이해 확인과 다릅니다. 본문은 선택한 기록에서 읽어 로컬에 표시합니다."
	return n
}

// Read publishes immutable node records. Callers may read concurrently with
// later calls, but must clone the slice and any records they need to modify.
func (o *Observer) Read() (model.Record, []model.Record, string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	var errorMessage any
	if o.cursor.ReadChecked(o.reset, o.consumeChecked) != nil {
		errorMessage = "현재 대화 기록을 읽을 수 없습니다."
	}
	o.merge()
	state := o.snapshotState(errorMessage)
	return state, o.snapshot, o.selectedSession
}

func (o *Observer) snapshotState(problem any) model.Record {
	if o.snapshot == nil || o.snapshotRevision != o.state.Revision {
		o.snapshot = o.state.List()
		o.snapshotRevision = o.state.Revision
	}
	nodes := o.snapshot
	return model.Record{"autoAgents": model.Record{"connected": len(o.auto), "pending": o.pending}, "agentSourceErrors": o.errors, "turn": model.Nullable(o.state.Turn), "started": model.Nullable(o.state.Started), "revision": o.state.Revision, "nodes": nodes, "lastEvent": model.Nullable(o.state.Last), "checkedAt": time.Now().Format(time.RFC3339Nano), "error": problem, "mode": "실제 transcript 증분 읽기 · 0.5초 폴링"}
}

func (o *Observer) stateConsume(r model.Record) {
	payload := r.Object("payload")
	if o.history && r.String("type") == "event_msg" && payload.String("type") == "task_started" && o.state.Turn != "" {
		if payload.String("turn_id") == o.state.Turn {
			return
		}
		o.preserveTurn(r.String("timestamp"))
	}
	o.state.Consume(r)
}

// NewHistory creates an independent export cursor. Ordinary UI readers never
// retain completed turns or perform the per-boundary child merge.
func NewHistory(source string, children []string) *Observer {
	return NewHistoryPinned(source, children, "")
}

func NewHistoryPinned(
	source string,
	children []string,
	sessionID string,
) *Observer {
	o := NewPinned(source, children, sessionID)
	o.history = true
	return o
}

func (o *Observer) preserveTurn(end string) {
	o.mergeBefore(end)
	o.turns = append(o.turns, o.snapshotState(nil))
}

// ReadTurns returns every crossed turn boundary plus the current turn. An error
// returns no partial batch; callers must reopen the independent history cursor
// before retrying, then deduplicate the replay at their delivery boundary.
func (o *Observer) ReadTurns() ([]model.Record, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.history {
		return nil, errors.New("turn history is not enabled")
	}
	if err := o.cursor.ReadChecked(o.reset, o.consumeChecked); err != nil {
		return nil, err
	}
	if o.state.Turn != "" {
		o.preserveTurn("")
	}
	turns := o.turns
	o.turns = nil
	return turns, nil
}

// WatchDirectories follows the same bounded child-discovery scope as connect.
func (o *Observer) WatchDirectories() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	dirs := agents.DiscoveryDirectories(o.cursor.Path, "")
	for _, n := range o.state.Nodes {
		if n.String("kind") == "subagent" {
			for dir := range agents.DiscoveryDirectories(o.cursor.Path, n.String("at")) {
				dirs[dir] = true
			}
		}
	}
	for _, r := range o.explicit {
		for dir := range agents.DiscoveryDirectories(r.Cursor.Path, "") {
			dirs[dir] = true
		}
	}
	for _, r := range o.auto {
		for dir := range agents.DiscoveryDirectories(r.Cursor.Path, "") {
			dirs[dir] = true
		}
	}
	result := []string{}
	for dir := range dirs {
		result = append(result, dir)
	}
	return result
}
