package workers

import (
	"context"
	"sync"
	"time"

	"agent-observer/internal/adapters/mailboxview"
	"agent-observer/internal/messaging"
	"agent-observer/internal/model"
)

const maxTeamMessages = 32

type workerState struct {
	status, conversationID string
	updated                time.Time
}

// recipientQueue is the durable part of dispatching, separate from the
// manager's admission and display responsibilities.
type recipientQueue interface {
	ClaimManaged(context.Context, messaging.Principal) (messaging.Message, bool, error)
	UpdateDelivery(context.Context, messaging.Principal, string, messaging.Delivery) error
}

type Manager struct {
	store       *messaging.Store
	queue       recipientQueue
	config      Config
	provider    Provider
	feed        *mailboxview.Feed
	ctx         context.Context
	cancel      context.CancelFunc
	done        sync.WaitGroup
	sendMu      sync.Mutex
	closed      bool            // guarded by sendMu
	unavailable map[string]bool // guarded by sendMu, like send/claim admission
	mu          sync.Mutex
	states      map[string]workerState
	changed     chan struct{}
	revision    uint64
	viewKey     string
	view        []model.Record
}

// New starts only queue listeners. A runtime is created lazily when an
// enrolled recipient receives its first message. The caller validates the owner
// against the selected graph before construction.
func New(ctx context.Context, store *messaging.Store, cfg Config, provider Provider) (*Manager, error) {
	return newManager(ctx, store, store, cfg, provider)
}

func newManager(ctx context.Context, store *messaging.Store, queue recipientQueue, cfg Config, provider Provider) (*Manager, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if !provider.valid() || (cfg.Provider != "" && cfg.Provider != provider.ID) {
		return nil, messaging.ErrInvalid
	}
	cfg.Workers = append([]Definition(nil), cfg.Workers...)
	if err := store.RecoverManaged(ctx, cfg.SessionID, cfg.Turn); err != nil {
		return nil, err
	}
	for _, d := range cfg.Workers {
		if err := store.EnsureParticipant(ctx, messaging.Principal{SessionID: cfg.SessionID, Turn: cfg.Turn, AgentID: d.ID}); err != nil {
			return nil, err
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	m := &Manager{store: store, queue: queue, config: cfg, provider: provider, feed: mailboxview.New(store), ctx: ctx, cancel: cancel,
		states: make(map[string]workerState), unavailable: make(map[string]bool), changed: make(chan struct{})}
	for _, d := range cfg.Workers {
		m.states[d.ID] = workerState{status: "유휴 · 메시지 대기", updated: time.Now()}
	}
	m.done.Add(1)
	go func() { defer m.done.Done(); m.watchStore(ctx) }()
	for _, d := range cfg.Workers {
		m.done.Add(1)
		go func(def Definition) { defer m.done.Done(); m.runRecipient(ctx, def) }(d)
	}
	return m, nil
}

func (m *Manager) Close() {
	m.cancel()
	m.sendMu.Lock()
	m.closed = true
	m.sendMu.Unlock()
	m.done.Wait()
}

// stopIfIdle closes admission atomically with checking the durable queue. A
// completed delivery can still be returning from Run, so also check live state.
func (m *Manager) stopIfIdle() bool {
	m.sendMu.Lock()
	defer m.sendMu.Unlock()
	if len(m.unavailable) != 0 {
		return false
	}
	messages, _, err := m.store.Snapshot(m.config.SessionID, m.config.Turn)
	if err != nil {
		return false
	}
	for _, msg := range messages {
		if msg.Delivery != nil && msg.Delivery.State != "completed" && msg.Delivery.State != "unknown" {
			return false
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, state := range m.states {
		if state.status == "작업 중" {
			return false
		}
	}
	m.closed = true
	m.cancel()
	return true
}

func (m *Manager) principal(id string) messaging.Principal {
	return messaging.Principal{SessionID: m.config.SessionID, Turn: m.config.Turn, AgentID: id}
}

// Send is the sole body persistence path used by both the CLI and each worker's
// dynamic tool. Sender identity is bound by the worker callback, never its input.
func (m *Manager) Send(ctx context.Context, source messaging.Principal, req messaging.Request) (messaging.Message, error) {
	if source.SessionID != m.config.SessionID || source.Turn != m.config.Turn || !m.hasWorker(req.Target) || (source.AgentID != m.config.Owner && !m.hasWorker(source.AgentID)) {
		return messaging.Message{}, messaging.ErrForbidden
	}
	if !messaging.IsManagedTransport(req.Transport) {
		return messaging.Message{}, messaging.ErrUnsupported
	}
	m.sendMu.Lock()
	defer m.sendMu.Unlock()
	if m.closed {
		return messaging.Message{}, messaging.ErrClosed
	}
	messages, _, err := m.store.Snapshot(source.SessionID, source.Turn)
	if err != nil {
		return messaging.Message{}, err
	}
	count, retry := 0, false
	for _, message := range messages {
		if messaging.IsManagedTransport(message.Transport) {
			count++
		}
		if message.ID == req.ID {
			retry = true
		}
	}
	// Existing IDs may only return the original receipt or an ID conflict. This
	// remains useful after a dispatcher stops and cannot create another delivery.
	if m.unavailable[req.Target] && !retry {
		return messaging.Message{}, messaging.ErrRecipientUnavailable
	}
	if count >= maxTeamMessages && !retry {
		return messaging.Message{}, messaging.ErrCapacity
	}
	return m.store.Managed().Send(ctx, source, req)
}

func (m *Manager) hasWorker(id string) bool {
	for _, d := range m.config.Workers {
		if d.ID == id {
			return true
		}
	}
	return false
}

func (m *Manager) sendAs(source string) SendFunc {
	return func(ctx context.Context, input Outbound) (SendReceipt, error) {
		if err := messaging.ValidateRequest(messaging.Request{ID: input.ID, Target: input.Target, Body: input.Body}); err != nil {
			return SendReceipt{}, err
		}
		message, err := m.Send(ctx, m.principal(source), messaging.Request{ID: input.ID, Target: input.Target, Body: input.Body, Transport: messaging.ManagedTransport})
		if err != nil {
			return SendReceipt{}, err
		}
		return SendReceipt{ID: message.ID, Target: message.Target, State: message.Delivery.State}, nil
	}
}

func (m *Manager) setState(id, status, conversation string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	previous := m.states[id]
	if conversation == "" {
		conversation = previous.conversationID
	}
	m.states[id] = workerState{status, conversation, time.Now()}
	m.signalLocked()
}

func (m *Manager) signalLocked() { m.revision++; close(m.changed); m.changed = make(chan struct{}) }

func (m *Manager) Changes() <-chan struct{} { m.mu.Lock(); defer m.mu.Unlock(); return m.changed }

func (m *Manager) watchStore(ctx context.Context) {
	for {
		changes := m.store.Changes()
		m.mu.Lock()
		m.signalLocked()
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-changes:
		}
	}
}
