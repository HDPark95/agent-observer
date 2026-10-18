// Package messages joins explicit message bodies to observed calls, with optional local storage.
package messages

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"agent-observer/internal/agents"
	"agent-observer/internal/model"
	"agent-observer/internal/normalize"
)

// Source publishes immutable node slices and records.
type Source interface {
	Read() (model.Record, []model.Record, string)
}
type pending struct {
	body, target string
	at           time.Time
}
type Capture struct {
	mu              sync.Mutex
	source          Source
	turn, root      string
	revision        uint64
	bodies, origins map[string]string
	pending         map[string]pending
	bindings        map[string]binding
	store           *Store
	identity        Identity
	restored        *storedState
	storeErr        error
}

func New(source Source) *Capture {
	return &Capture{source: source, bodies: map[string]string{}, origins: map[string]string{}, pending: map[string]pending{}, bindings: map[string]binding{}}
}
func (c *Capture) read() (model.Record, []model.Record) {
	state, nodes, root := c.source.Read()
	c.root = root
	previousRevision := c.revision
	if state.String("turn") != c.turn {
		c.turn = state.String("turn")
		c.bodies = map[string]string{}
		c.origins = map[string]string{}
		c.pending = map[string]pending{}
		c.bindings = map[string]binding{}
		if c.restored != nil && c.restored.Turn == c.turn {
			c.restore(*c.restored)
		}
		c.restored = nil
		c.revision++
	}
	c.attachPending(nodes)
	if c.revision != previousRevision || c.storeErr != nil {
		_, c.storeErr = c.persist()
	}
	nodes = c.overlayBodies(nodes)
	state["nodes"] = nodes
	state["revision"] = fmt.Sprintf("%v:%d", state["revision"], c.revision)
	state["messageStoreEnabled"] = c.store != nil
	if c.storeErr != nil {
		state["messageStoreError"] = "메시지 본문을 로컬 저장소에 보관하지 못했습니다."
		if errors.Is(c.storeErr, ErrDurabilityUnknown) {
			state["messageStoreError"] = "메시지 본문은 저장되었지만 디스크 반영 여부를 확인하지 못했습니다."
		}
	}
	return state, nodes
}

// attachPending joins only an unambiguous observed call to its matching target.
// The caller holds c.mu.
func (c *Capture) attachPending(nodes []model.Record) {
	for id, p := range c.pending {
		if time.Since(p.at) > 120*time.Second {
			delete(c.pending, id)
			c.revision++
			continue
		}
		matches := []model.Record{}
		for _, n := range nodes {
			if n.String("kind") == "a2a" && strings.HasSuffix(n.String("id"), ":"+id) {
				matches = append(matches, n)
			}
		}
		if len(matches) != 1 {
			continue
		}
		n := matches[0]
		target := agents.Relative(n.String("sender"), p.target)
		_, exists := c.bodies[n.String("id")]
		if target == n.String("recipient") && !exists {
			c.bodies[n.String("id")] = p.body
			c.origins[n.String("id")] = "native-hook"
			c.bindings[n.String("id")] = bind(n)
			c.revision++
		}
		delete(c.pending, id)
		c.revision++
	}
}

// overlayBodies preserves the immutable source snapshot, copying only when a
// body is present and cloning only the records that receive an overlay.
func (c *Capture) overlayBodies(nodes []model.Record) []model.Record {
	copied := false
	for i, n := range nodes {
		if body, ok := c.bodies[n.String("id")]; ok && n.String("kind") == "a2a" && c.bindings[n.String("id")] == bind(n) {
			if !copied {
				nodes = append([]model.Record(nil), nodes...)
				copied = true
			}
			n = n.Clone()
			nodes[i] = n
			n["body"] = body
			n["bodyUnavailable"] = nil
			origin := c.origins[n.String("id")]
			if origin == "" {
				origin = "sender-local"
			}
			n["bodySource"] = origin
		}
	}
	return nodes
}

func (c *Capture) Read() model.Record {
	c.mu.Lock()
	defer c.mu.Unlock()
	state, _ := c.read()
	return state
}

var targetPattern = regexp.MustCompile(`^(?:/root/)?[\pL\pN_/-]{1,200}$`)

func (c *Capture) Publish(payload model.Record, hook bool) (model.Record, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	state, nodes := c.read()
	if c.storeErr != nil {
		if errors.Is(c.storeErr, ErrDurabilityUnknown) {
			return nil, ErrDurabilityUnknown
		}
		return nil, errors.New("cannot save message body locally")
	}
	raw, ok := payload["body"].(string)
	if !ok || utf8.RuneCountInString(raw) > 12000 || agents.Body(raw) == nil {
		return nil, errors.New("본문 형식 오류")
	}
	body := agents.Body(raw).(string)
	beforeRevision := c.revision
	var before storedState
	if c.store != nil {
		before = c.snapshot()
	}
	var result model.Record
	var err error
	if hook {
		result, err = c.publishHook(payload, body, nodes)
	} else {
		result, err = c.publishObserved(payload, body, state, nodes)
	}
	if err != nil {
		return nil, err
	}
	if committed, err := c.persist(); err != nil {
		if !committed {
			c.rollback(before)
			c.revision = beforeRevision
		}
		c.storeErr = err
		if committed {
			return nil, ErrDurabilityUnknown
		}
		return nil, errors.New("cannot save message body locally")
	}
	return result, nil
}

// publishHook may queue a body before its transcript call becomes visible.
// The caller holds c.mu and has refreshed c.root through read.
func (c *Capture) publishHook(payload model.Record, body string, nodes []model.Record) (model.Record, error) {
	if payload.String("sessionId") != c.root {
		return nil, errors.New("관찰 중인 세션이 아닙니다")
	}
	id, target := payload.String("callId"), payload.String("target")
	if !normalize.ValidID.MatchString(id) || !targetPattern.MatchString(target) {
		return nil, errors.New("메시지 식별자 오류")
	}
	for key, existing := range c.bodies {
		if strings.HasSuffix(key, ":"+id) {
			binding := c.bindings[key]
			if existing != body || agents.Relative(binding.Sender, target) != binding.Recipient {
				return nil, errors.New("본문 충돌")
			}
			for _, node := range nodes {
				if node.String("id") == key {
					if binding != bind(node) {
						return nil, errors.New("메시지 식별자 충돌")
					}
					return model.Record{"status": "attached"}, nil
				}
			}
			return model.Record{"status": "pending"}, nil
		}
	}
	previous, exists := c.pending[id]
	if exists && (previous.body != body || previous.target != target) {
		return nil, errors.New("본문 충돌")
	}
	if len(c.pending) >= 256 && !exists {
		return nil, errors.New("대기열 한도")
	}
	if !exists {
		c.pending[id] = pending{body, target, time.Now()}
	}
	c.attachPending(nodes)
	status := "attached"
	if _, ok := c.pending[id]; ok {
		status = "pending"
	}
	return model.Record{"status": status}, nil
}

// publishObserved requires an exact event, turn, and endpoint match.
// The caller holds c.mu.
func (c *Capture) publishObserved(payload model.Record, body string, state model.Record, nodes []model.Record) (model.Record, error) {
	if payload.String("turn") != state.String("turn") {
		return nil, errors.New("현재 턴과 일치하지 않습니다")
	}
	for _, n := range nodes {
		if n.String("id") == payload.String("eventId") && n.String("kind") == "a2a" && n.String("source") == payload.String("source") && n.String("target") == payload.String("target") {
			id := n.String("id")
			previous, exists := c.bodies[id]
			if exists && (previous != body || c.bindings[id] != bind(n)) {
				return nil, errors.New("본문 충돌")
			}
			if !exists {
				c.bodies[id] = body
				c.bindings[id] = bind(n)
				c.revision++
			}
			return model.Record{"eventId": id, "status": "attached"}, nil
		}
	}
	return nil, errors.New("일치하는 메시지 송신 기록이 없습니다")
}

func (c *Capture) WatchDirectories() []string {
	if source, ok := c.source.(interface{ WatchDirectories() []string }); ok {
		return source.WatchDirectories()
	}
	return nil
}
