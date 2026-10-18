package sessions

import (
	"errors"
	"fmt"

	"agent-observer/internal/messaging"
	"agent-observer/internal/model"
)

// ResolveTeam opens only the named session, then binds the configured owner to
// its observed turn. A catalog read alone never grants a task execution scope.
func (r *Registry) ResolveTeam(session, owner string) (messaging.Principal, error) {
	if session == "" || owner == "" {
		return messaging.Principal{}, errors.New("missing selected session")
	}
	if _, err := r.Open(model.Record{"sessionId": session}); err != nil {
		return messaging.Principal{}, err
	}
	state, err := r.State(session)
	if err != nil || state.String("turn") == "" {
		return messaging.Principal{}, errors.New("session has no observed turn")
	}
	nodes, _ := state["nodes"].([]model.Record)
	for _, node := range nodes {
		if node.String("id") == owner && node.String("kind") == "agent" {
			return messaging.Principal{SessionID: session, Turn: state.String("turn"), AgentID: owner}, nil
		}
	}
	return messaging.Principal{}, errors.New("team owner was not observed")
}

// MessageFeed publishes immutable local message observations. It does not send
// messages and is separate from the metadata export pipeline.
type MessageFeed interface {
	Read(session, turn string) ([]model.Record, string)
	Changes() <-chan struct{}
}

type messageView struct {
	revision string
	nodes    []model.Record
}

// SetMessageFeed is configured before serving HTTP requests.
func (r *Registry) SetMessageFeed(feed MessageFeed) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.messageFeed = feed
	r.messageViews = make(map[string]messageView)
	r.signalLocked()
}

func (r *Registry) MessageChanges() <-chan struct{} {
	r.mu.Lock()
	feed := r.messageFeed
	r.mu.Unlock()
	if feed == nil {
		return nil
	}
	return feed.Changes()
}

// addMessages caches the merged immutable slice so idle reads don't
// copy a large transcript graph again for the same message revision.
func (r *Registry) addMessages(id string, state model.Record) {
	r.mu.Lock()
	feed := r.messageFeed
	r.mu.Unlock()
	if feed == nil {
		return
	}
	nodes, revision := feed.Read(id, state.String("turn"))
	r.mu.Lock()
	defer r.mu.Unlock()
	state["mailboxEnabled"] = true
	key := fmt.Sprintf("%s:%v:mailbox:%s", state.String("turn"), state["revision"], revision)
	if len(nodes) > 0 {
		view := r.messageViews[id]
		if view.revision != key {
			base, _ := state["nodes"].([]model.Record)
			merged := make([]model.Record, 0, len(base)+len(nodes))
			merged = append(merged, base...)
			merged = append(merged, nodes...)
			view = messageView{key, merged}
			r.messageViews[id] = view
		}
		state["nodes"] = view.nodes
	} else {
		delete(r.messageViews, id)
	}
	state["revision"] = key
}

// ValidateParticipant binds enrollment to an explicitly selected current turn
// and an agent observed in its source. A display name is not an identity.
func (r *Registry) ValidateParticipant(session, turn, agent string) error {
	if session == "" || turn == "" || agent == "" {
		return errors.New("missing participant scope")
	}
	state, err := r.State(session)
	if err != nil || state.String("turn") != turn {
		return errors.New("participant scope is not selected")
	}
	nodes, _ := state["nodes"].([]model.Record)
	for _, n := range nodes {
		if n.String("id") == agent && (n.String("kind") == "agent" || n.String("kind") == "subagent") {
			return nil
		}
	}
	return errors.New("participant was not observed")
}
