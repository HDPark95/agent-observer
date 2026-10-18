package messages

import (
	"errors"
	"path/filepath"
	"time"
	"unicode/utf8"

	"agent-observer/internal/agents"
	"agent-observer/internal/model"
	"agent-observer/internal/normalize"
)

// NewWithStore restores only bodies bound to the same selected source and turn.
// It does not inspect any source other than the caller's explicit selection.
func NewWithStore(source Source, store *Store, identity Identity) (*Capture, error) {
	c := New(source)
	if store == nil {
		return c, nil
	}
	if identity.Provider == "" || identity.Session == "" || !filepath.IsAbs(identity.Source) {
		return nil, errors.New("message store requires a complete source identity")
	}
	state, err := store.load(identity)
	if err != nil {
		return nil, err
	}
	c.store, c.identity, c.restored = store, identity, state
	return c, nil
}

func bind(node model.Record) binding {
	return binding{
		ID: node.String("id"), Sender: node.String("sender"), Recipient: node.String("recipient"),
		Source: node.String("source"), Target: node.String("target"), At: node.String("at"),
	}
}

func (c *Capture) snapshot() storedState {
	state := storedState{Version: 1, Identity: c.identity, Turn: c.turn, Bodies: map[string]storedBody{}, Pending: map[string]storedPending{}}
	for id, body := range c.bodies {
		state.Bodies[id] = storedBody{Body: body, Origin: c.origins[id], Binding: c.bindings[id]}
	}
	for id, p := range c.pending {
		state.Pending[id] = storedPending{Body: p.body, Target: p.target, At: p.at}
	}
	return state
}

func (c *Capture) persist() (bool, error) {
	if c.store == nil || c.turn == "" {
		return false, nil
	}
	return c.store.save(c.snapshot())
}

func (c *Capture) restore(state storedState) {
	c.bodies, c.origins, c.bindings = map[string]string{}, map[string]string{}, map[string]binding{}
	c.pending = map[string]pending{}
	for id, saved := range state.Bodies {
		body := storedText(saved.Body)
		if body == "" || saved.Binding.ID != id || (saved.Origin != "" && saved.Origin != "native-hook" && saved.Origin != "sender-local") {
			continue
		}
		c.bodies[id], c.origins[id], c.bindings[id] = body, saved.Origin, saved.Binding
	}
	for id, p := range state.Pending {
		body := storedText(p.Body)
		age := time.Since(p.At)
		if len(c.pending) >= 256 || body == "" || !normalize.ValidID.MatchString(id) || !targetPattern.MatchString(p.Target) || age < 0 || age > 120*time.Second {
			continue
		}
		c.pending[id] = pending{body: body, target: p.Target, at: p.At}
	}
}

func storedText(raw string) string {
	if utf8.RuneCountInString(raw) > 12000 {
		return ""
	}
	body, _ := agents.Body(raw).(string)
	return body
}

// rollback restores an in-memory snapshot exactly. Unlike disk restore, it must
// not expire or revalidate entries that predate the failed transaction.
func (c *Capture) rollback(state storedState) {
	c.bodies, c.origins, c.bindings = map[string]string{}, map[string]string{}, map[string]binding{}
	c.pending = map[string]pending{}
	for id, v := range state.Bodies {
		c.bodies[id], c.origins[id], c.bindings[id] = v.Body, v.Origin, v.Binding
	}
	for id, v := range state.Pending {
		c.pending[id] = pending{body: v.Body, target: v.Target, at: v.At}
	}
}
