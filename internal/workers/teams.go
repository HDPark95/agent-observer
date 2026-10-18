package workers

import (
	"context"
	"sync"

	"agent-observer/internal/adapters/mailboxview"
	"agent-observer/internal/messaging"
	"agent-observer/internal/model"
)

// ScopeResolver opens only the explicitly selected session and resolves its
// currently observed owner and turn. It must not select a latest session.
type ScopeResolver interface {
	ResolveTeam(session, owner string) (messaging.Principal, error)
}

type StartRequest struct {
	SessionID string `json:"sessionId"`
	ID        string `json:"id"`
	Target    string `json:"target"`
	Body      string `json:"body"`
}

type teamScope struct{ session, turn string }

// Teams binds an operator-owned template to immutable session/turn scopes.
// Starting a new scope never moves a running worker or its send callback.
type Teams struct {
	ctx      context.Context
	cancel   context.CancelFunc
	store    *messaging.Store
	template Config
	provider Provider
	resolver ScopeResolver
	feed     *mailboxview.Feed
	startMu  sync.Mutex // admission, scope creation and shutdown
	mu       sync.Mutex // short projection lookups; never held during graph reads
	managers map[teamScope]*Manager
	closed   bool
	changeMu sync.Mutex
	changed  chan struct{}
	done     sync.WaitGroup
}

func NewTeams(ctx context.Context, store *messaging.Store, cfg Config, provider Provider, resolver ScopeResolver) (*Teams, error) {
	if err := cfg.validateTemplate(); err != nil {
		return nil, err
	}
	if !provider.valid() || cfg.Provider != "" && cfg.Provider != provider.ID || resolver == nil {
		return nil, messaging.ErrInvalid
	}
	ctx, cancel := context.WithCancel(ctx)
	cfg.Workers = append([]Definition(nil), cfg.Workers...)
	t := &Teams{ctx: ctx, cancel: cancel, store: store, template: cfg, provider: provider, resolver: resolver,
		feed: mailboxview.New(store), managers: make(map[teamScope]*Manager), changed: make(chan struct{})}
	t.watch(store.Changes, ctx)
	if err := t.restorePending(); err != nil {
		t.Close()
		return nil, err
	}
	return t, nil
}

func (t *Teams) restorePending() error {
	pending := t.store.PendingTeams(t.template.Owner)
	for _, scope := range pending {
		messages, _, err := t.store.Snapshot(scope.SessionID, scope.Turn)
		if err != nil {
			return err
		}
		for _, msg := range messages {
			if msg.Delivery != nil && (msg.Delivery.State == "queued" || msg.Delivery.State == "dispatching" || msg.Delivery.State == "running") && !t.hasTarget(msg.Target) {
				return messaging.ErrRecipientUnavailable
			}
		}
	}
	for _, scope := range pending {
		if _, err := t.ensure(scope.SessionID, scope.Turn); err != nil {
			return err
		}
	}
	return nil
}

// Start is an authenticated host operation. The server owns executable, working
// directory, roles and sender; callers supply only a session, ID and task.
func (t *Teams) Start(ctx context.Context, input StartRequest) (messaging.Message, error) {
	t.startMu.Lock()
	defer t.startMu.Unlock()
	if t.closed || t.ctx.Err() != nil {
		return messaging.Message{}, messaging.ErrClosed
	}
	req := messaging.Request{ID: input.ID, Target: input.Target, Body: input.Body, Transport: messaging.ManagedTransport}
	existing, found, err := t.store.FindTeamStart(ctx, input.SessionID, t.template.Owner, req)
	if err != nil {
		return messaging.Message{}, err
	}
	if found {
		// Do not resolve the current turn for a retry. Its durable receipt owns
		// the original scope even after the transcript moves to another turn.
		if existing.Delivery.State == "queued" || existing.Delivery.State == "dispatching" || existing.Delivery.State == "running" {
			if _, err := t.ensure(existing.SessionID, existing.Turn); err != nil {
				return messaging.Message{}, err
			}
			existing, _, err = t.store.FindTeamStart(ctx, input.SessionID, t.template.Owner, req)
		}
		return existing, err
	}
	if !t.hasTarget(req.Target) {
		return messaging.Message{}, messaging.ErrNotFound
	}
	principal, err := t.resolver.ResolveTeam(input.SessionID, t.template.Owner)
	if err != nil || principal.SessionID != input.SessionID || principal.AgentID != t.template.Owner || principal.Turn == "" {
		return messaging.Message{}, messaging.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return messaging.Message{}, err
	}
	manager, err := t.ensure(principal.SessionID, principal.Turn)
	if err != nil {
		return messaging.Message{}, err
	}
	if err := t.store.EnsureParticipant(ctx, principal); err != nil {
		return messaging.Message{}, err
	}
	return manager.Send(ctx, principal, req)
}

func (t *Teams) hasTarget(id string) bool {
	for _, d := range t.template.Workers {
		if d.ID == id {
			return true
		}
	}
	return false
}

// ensure holds startMu, but not mu: New can publish updates immediately.
func (t *Teams) ensure(session, turn string) (*Manager, error) {
	key := teamScope{session, turn}
	t.mu.Lock()
	m := t.managers[key]
	t.mu.Unlock()
	if m != nil {
		return m, nil
	}
	t.retirePreviousTurns(session, turn)
	if err := t.makeRoom(); err != nil {
		return nil, err
	}
	cfg := t.template
	cfg.SessionID, cfg.Turn, cfg.Automatic = session, turn, false
	m, err := New(t.ctx, t.store, cfg, t.provider)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	t.managers[key] = m
	t.mu.Unlock()
	t.watch(m.Changes, m.ctx)
	t.signal()
	return m, nil
}

func (t *Teams) retirePreviousTurns(session, turn string) {
	t.mu.Lock()
	var retired []*Manager
	for key, m := range t.managers {
		if key.session == session && key.turn != turn && m.stopIfIdle() {
			delete(t.managers, key)
			retired = append(retired, m)
		}
	}
	t.mu.Unlock()
	for _, m := range retired {
		m.Close()
	}
}

// Bound concurrent teams. Completed scopes can be reopened from durable bodies;
// active or storage-failed queues are never evicted to admit another task.
func (t *Teams) makeRoom() error {
	t.mu.Lock()
	if len(t.managers) < 16 {
		t.mu.Unlock()
		return nil
	}
	var evicted *Manager
	for key, m := range t.managers {
		if m.stopIfIdle() {
			delete(t.managers, key)
			evicted = m
			break
		}
	}
	t.mu.Unlock()
	if evicted == nil {
		return messaging.ErrCapacity
	}
	evicted.Close()
	return nil
}

func (t *Teams) Send(ctx context.Context, source messaging.Principal, req messaging.Request) (messaging.Message, error) {
	t.startMu.Lock()
	defer t.startMu.Unlock()
	if t.closed {
		return messaging.Message{}, messaging.ErrClosed
	}
	t.mu.Lock()
	m := t.managers[teamScope{source.SessionID, source.Turn}]
	t.mu.Unlock()
	if m == nil {
		return messaging.Message{}, messaging.ErrForbidden
	}
	return m.Send(ctx, source, req)
}

func (t *Teams) Read(session, turn string) ([]model.Record, string) {
	t.mu.Lock()
	m := t.managers[teamScope{session, turn}]
	t.mu.Unlock()
	if m != nil {
		return m.Read(session, turn)
	}
	base, revision := t.feed.Read(session, turn)
	return t.archivedView(base), revision + ":archived"
}

func (t *Teams) Changes() <-chan struct{} {
	t.changeMu.Lock()
	defer t.changeMu.Unlock()
	return t.changed
}

func (t *Teams) signal() {
	t.changeMu.Lock()
	defer t.changeMu.Unlock()
	close(t.changed)
	t.changed = make(chan struct{})
}

func (t *Teams) watch(changes func() <-chan struct{}, ctx context.Context) {
	t.done.Add(1)
	go func() {
		defer t.done.Done()
		for {
			changed := changes()
			t.signal()
			select {
			case <-ctx.Done():
				return
			case <-changed:
			}
		}
	}()
}

func (t *Teams) Close() {
	t.startMu.Lock()
	defer t.startMu.Unlock()
	t.closed = true
	t.cancel()
	t.mu.Lock()
	managers := make([]*Manager, 0, len(t.managers))
	for _, m := range t.managers {
		managers = append(managers, m)
	}
	t.mu.Unlock()
	for _, m := range managers {
		m.Close()
	}
	t.done.Wait()
}
