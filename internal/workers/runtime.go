package workers

import (
	"context"
	"errors"
	"strings"

	"agent-observer/internal/messaging"
)

// ErrRuntimeUpgradeRequired identifies a provider-declared client compatibility
// failure without retaining its potentially sensitive raw error message.
var ErrRuntimeUpgradeRequired = errors.New("worker runtime upgrade required")

// Runtime executes messages in one owned provider conversation under a read-only
// policy: no file or external-service mutations. Implementations must honor
// cancellation, including blocked writes, and release owned resources on Close.
// No preexisting conversation identity is accepted from a sender. An uncertain
// result must retain observed IDs and must never trigger an adapter-owned retry.
type Runtime interface {
	Run(context.Context, Input) (Receipt, error)
	Close() error
}

// Input carries a durable message. Run must call OnAccepted once the provider
// accepts execution, before reporting completion. A callback error must stop the
// execution and be returned with any receipt already observed.
type Input struct {
	ID, Source, Body string
	OnAccepted       func(Receipt) error
}

// Receipt IDs are opaque to the manager. For stored-schema compatibility they
// map to Delivery.ThreadID and Delivery.TurnID, without implying a particular
// provider's conversation or execution protocol.
type Receipt struct {
	ConversationID, ExecutionID, Output string
	OutputState                         messaging.OutputState
	OutputReason                        string
}

// RuntimeConfig is trusted operator configuration. Instructions are the worker
// role; protocol and messaging-tool instructions belong to the provider adapter.
type RuntimeConfig struct {
	Executable, CWD, Instructions, WorkerID string
	Peers                                   []Peer
}

type Peer struct {
	ID, Name string
}

// Outbound cannot choose the sender or execution scope: the manager binds both
// before supplying SendFunc to a runtime.
type Outbound struct {
	ID, Target, Body string
}

type SendReceipt struct {
	ID     string `json:"id"`
	Target string `json:"target"`
	State  string `json:"state"`
}

type SendFunc func(context.Context, Outbound) (SendReceipt, error)

// Factory creates a runtime using the manager's lifetime context. On error it
// must release resources acquired during setup; on success the manager owns
// Close. Startup and provider-specific instructions belong to the adapter.
type Factory func(context.Context, RuntimeConfig, SendFunc) (Runtime, error)

// Provider supplies one team's runtime and presentation identity. It is copied
// at manager construction and does not change while messages are being handled.
type Provider struct {
	ID, Name, ExecutionLabel string
	Factory                  Factory
}

func (p Provider) valid() bool {
	return messaging.ValidProviderID(p.ID) && strings.TrimSpace(p.Name) != "" && strings.TrimSpace(p.ExecutionLabel) != "" && p.Factory != nil
}
