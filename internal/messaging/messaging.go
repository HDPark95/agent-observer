// Package messaging delivers explicitly addressed local messages independently
// of Observer projections and external telemetry exporters.
package messaging

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	MailboxTransport = "mailbox"
	ManagedTransport = "managed"
	// LegacyManagedTransport remains accepted for existing clients and records.
	LegacyManagedTransport = "codex-managed"
	MaxBodyBytes           = 48 << 10
	MaxBodyRunes           = 12000
	MaxAgentRunes          = 256
	MessageIDPattern       = `^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`
	MaxMessages            = 10000
	MaxCapabilities        = 10000
	AckRetention           = 7 * 24 * time.Hour
)

var (
	ErrInvalid              = errors.New("invalid message request")
	ErrUnauthorized         = errors.New("invalid messaging capability")
	ErrForbidden            = errors.New("message does not belong to this recipient")
	ErrNotFound             = errors.New("message or recipient not found")
	ErrConflict             = errors.New("message ID already has different content")
	ErrUnsupported          = errors.New("message transport is unsupported")
	ErrCapacity             = errors.New("message store capacity reached")
	ErrClosed               = errors.New("message store is closed")
	ErrLocked               = errors.New("message store is already in use")
	ErrOutcomeUnknown       = errors.New("message storage outcome is not confirmed; retry with the same ID")
	ErrRecipientUnavailable = errors.New("message recipient is unavailable")
)

// Principal is a capability's immutable scope. AgentID is the observed graph
// identity, not a caller-controlled display name.
type Principal struct {
	SessionID string `json:"sessionId"`
	Turn      string `json:"turn"`
	AgentID   string `json:"agentId"`
}

type Request struct {
	ID        string `json:"id"`
	Target    string `json:"target"`
	Body      string `json:"body"`
	ReplyTo   string `json:"replyTo,omitempty"`
	Transport string `json:"transport,omitempty"`
}

// Message is a value snapshot. AckedAt is evidence of an explicit recipient Ack,
// never an inference that a model understood or completed work.
type Message struct {
	ID            string     `json:"id"`
	SessionID     string     `json:"sessionId"`
	Turn          string     `json:"turn"`
	Source        string     `json:"source"`
	Target        string     `json:"target"`
	Body          string     `json:"body"`
	ReplyTo       string     `json:"replyTo,omitempty"`
	Transport     string     `json:"transport"`
	AcceptedAt    time.Time  `json:"acceptedAt"`
	AckedAt       *time.Time `json:"ackedAt,omitempty"`
	BodyExpiredAt *time.Time `json:"bodyExpiredAt,omitempty"`
	Delivery      *Delivery  `json:"delivery,omitempty"`
}

// OutputState records how much observed final text was retained.
type OutputState string

const (
	OutputComplete  OutputState = "complete"
	OutputTruncated OutputState = "truncated"
	OutputOmitted   OutputState = "omitted"
)

// Delivery records provider execution separately from an explicit mailbox Ack.
// Error is a safe diagnostic code, never an unfiltered provider error.
// Empty output metadata remains unknown for legacy records.
type Delivery struct {
	// Provider identifies the adapter selected for this execution attempt.
	// Provider acceptance is established by receipt IDs and state, not this field.
	// Empty remains unknown for older records; never infer it from current config.
	Provider     string      `json:"provider,omitempty"`
	State        string      `json:"state"`
	ThreadID     string      `json:"threadId,omitempty"`
	TurnID       string      `json:"turnId,omitempty"`
	Output       string      `json:"output,omitempty"`
	OutputState  OutputState `json:"outputState,omitempty"`
	OutputReason string      `json:"outputReason,omitempty"`
	Error        string      `json:"error,omitempty"`
	UpdatedAt    time.Time   `json:"updatedAt"`
}

// Transport is the delivery behavior the sender consumes. Receive and Ack are
// deliberately mailbox operations, not capabilities every provider must fake.
type Transport interface {
	Send(context.Context, Principal, Request) (Message, error)
}

// Service authenticates senders and selects only explicitly configured adapters.
// Observer availability is not a dependency of Send, Receive or Ack.
type Service struct {
	store      *Store
	transports map[string]Transport
}

func NewService(store *Store, transports map[string]Transport) *Service {
	configured := make(map[string]Transport, len(transports))
	for name, transport := range transports {
		if transport != nil && !IsManagedTransport(name) {
			configured[name] = transport
		}
	}
	managed := transports[ManagedTransport]
	if managed == nil {
		managed = transports[LegacyManagedTransport]
	}
	if managed != nil {
		configured[ManagedTransport] = managed
		configured[LegacyManagedTransport] = managed
	}
	return &Service{store: store, transports: configured}
}

func (s *Service) Send(ctx context.Context, token string, request Request) (Message, error) {
	if err := ctx.Err(); err != nil {
		return Message{}, err
	}
	principal, err := s.store.Authenticate(token)
	if err != nil {
		return Message{}, err
	}
	if request.Transport == "" {
		request.Transport = MailboxTransport
	}
	if err := ValidateRequest(request); err != nil {
		return Message{}, err
	}
	transport, ok := s.transports[request.Transport]
	if !ok {
		return Message{}, ErrUnsupported
	}
	return transport.Send(ctx, principal, request)
}

func (s *Service) Receive(ctx context.Context, token string) ([]Message, error) {
	return s.store.receive(ctx, token)
}

func (s *Service) Ack(ctx context.Context, token, id string) (Message, error) {
	return s.store.ack(ctx, token, id)
}

// IsManagedTransport recognizes the provider-neutral name and its legacy alias.
func IsManagedTransport(name string) bool {
	return name == ManagedTransport || name == LegacyManagedTransport
}

var idPattern = regexp.MustCompile(MessageIDPattern)
var sessionPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_:-]{0,255}$`)
var agentPattern = regexp.MustCompile(fmt.Sprintf(`^[\pL\pN_:/.-]{1,%d}$`, MaxAgentRunes))

func validPrincipal(principal Principal) bool {
	return sessionPattern.MatchString(principal.SessionID) && idPattern.MatchString(principal.Turn) && validAgent(principal.AgentID)
}

func validAgent(agent string) bool {
	return agentPattern.MatchString(agent) && !strings.Contains(agent, "..") && !strings.Contains(agent, "//")
}

// ValidateRequest is the shared message admission contract for transports and tools.
func ValidateRequest(request Request) error {
	if !idPattern.MatchString(request.ID) || !validAgent(request.Target) || (request.ReplyTo != "" && !idPattern.MatchString(request.ReplyTo)) {
		return ErrInvalid
	}
	if !utf8.ValidString(request.Body) || len(request.Body) > MaxBodyBytes || utf8.RuneCountInString(request.Body) > MaxBodyRunes || strings.TrimSpace(request.Body) == "" {
		return ErrInvalid
	}
	for _, r := range request.Body {
		if unicode.IsControl(r) && r != '\n' && r != '\r' && r != '\t' {
			return ErrInvalid
		}
	}
	return nil
}

func cloneMessage(message Message) Message {
	if message.Delivery != nil {
		delivery := *message.Delivery
		message.Delivery = &delivery
	}
	if message.AckedAt != nil {
		at := *message.AckedAt
		message.AckedAt = &at
	}
	if message.BodyExpiredAt != nil {
		at := *message.BodyExpiredAt
		message.BodyExpiredAt = &at
	}
	return message
}
