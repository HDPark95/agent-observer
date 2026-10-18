package messaging

import (
	"context"
	"regexp"
	"time"
	"unicode"
	"unicode/utf8"
)

type managed struct{ store *Store }

// Managed durably queues a message for an Observer-owned worker. The dispatcher
// claims it separately; storing a body does not claim provider acceptance.
func (s *Store) Managed() Transport { return managed{store: s} }

func (m managed) Send(ctx context.Context, principal Principal, request Request) (Message, error) {
	if !IsManagedTransport(request.Transport) {
		return Message{}, ErrUnsupported
	}
	return m.store.send(ctx, principal, request)
}

// ClaimManaged takes the oldest queued message for this enrolled recipient.
// The dispatching state is durable before return, so a process crash cannot
// silently turn an uncertain provider submission into a new queued attempt.
// A recipient may have only one active claim at a time.
func (s *Store) ClaimManaged(ctx context.Context, principal Principal) (Message, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkManagedPrincipal(ctx, principal); err != nil {
		return Message{}, false, err
	}
	var selected Message
	var selectedSequence uint64
	for _, stored := range s.state.Messages {
		message := stored.Message
		if !managedRecipient(message, principal) {
			continue
		}
		if message.Delivery.State == "dispatching" || message.Delivery.State == "running" {
			return Message{}, false, nil
		}
		if message.Delivery.State != "queued" {
			continue
		}
		if selected.ID == "" || stored.Sequence < selectedSequence {
			selected = message
			selectedSequence = stored.Sequence
		}
	}
	if selected.ID == "" {
		return Message{}, false, nil
	}
	delivery := Delivery{State: "dispatching", UpdatedAt: s.deliveryTime(selected)}
	if err := s.commitDelivery(ctx, selected, delivery); err != nil {
		return Message{}, false, err
	}
	selected.Delivery = &delivery
	return cloneMessage(selected), true, nil
}

// UpdateDelivery advances a claimed message using the recipient's bound
// identity. Provider receipt IDs cannot be replaced once recorded. UpdatedAt is
// assigned by the store; callers need not supply it.
func (s *Store) UpdateDelivery(ctx context.Context, principal Principal, id string, delivery Delivery) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkManagedPrincipal(ctx, principal); err != nil {
		return err
	}
	if !idPattern.MatchString(id) {
		return ErrInvalid
	}
	stored, ok := s.state.Messages[messageKey(principal.SessionID, principal.Turn, id)]
	if !ok {
		return ErrNotFound
	}
	message := stored.Message
	if message.Target != principal.AgentID {
		return ErrForbidden
	}
	if !IsManagedTransport(message.Transport) {
		return ErrUnsupported
	}
	previous := *message.Delivery
	if previous.Provider != "" && delivery.Provider != previous.Provider {
		return ErrInvalid
	}
	if !validDeliveryTransition(previous.State, delivery.State) {
		return ErrInvalid
	}
	if previous.ThreadID != "" && delivery.ThreadID != previous.ThreadID || previous.TurnID != "" && delivery.TurnID != previous.TurnID {
		return ErrInvalid
	}
	delivery.UpdatedAt = s.deliveryTime(message)
	if !validDelivery(delivery) {
		return ErrInvalid
	}
	return s.commitDelivery(ctx, message, delivery)
}

// RecoverManaged marks in-flight deliveries in one explicitly selected scope as
// unknown. It does not resend them or change other sessions, queued messages or
// terminal receipts. Call before starting dispatchers for that scope.
func (s *Store) RecoverManaged(ctx context.Context, sessionID, turn string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkContext(ctx); err != nil {
		return err
	}
	if !sessionPattern.MatchString(sessionID) || !idPattern.MatchString(turn) {
		return ErrInvalid
	}
	next := s.copyState()
	changed := false
	for key, stored := range next.Messages {
		message := stored.Message
		if message.SessionID != sessionID || message.Turn != turn || !IsManagedTransport(message.Transport) {
			continue
		}
		previous := *message.Delivery
		if previous.State != "dispatching" && previous.State != "running" {
			continue
		}
		previous.State = "unknown"
		previous.Error = "interrupted-delivery"
		previous.UpdatedAt = s.deliveryTime(message)
		message.Delivery = &previous
		stored.Message = message
		next.Messages[key] = stored
		next.Revisions[scopeKey(sessionID, turn)]++
		changed = true
	}
	if !changed {
		return s.confirmDurability()
	}
	return s.commit(ctx, next, true)
}

func (s *Store) checkManagedPrincipal(ctx context.Context, principal Principal) error {
	if err := s.checkContext(ctx); err != nil {
		return err
	}
	if !validPrincipal(principal) {
		return ErrInvalid
	}
	if !s.enrolled(principal) {
		return ErrUnauthorized
	}
	return nil
}

func managedRecipient(message Message, principal Principal) bool {
	return IsManagedTransport(message.Transport) && message.SessionID == principal.SessionID && message.Turn == principal.Turn && message.Target == principal.AgentID
}

func (s *Store) deliveryTime(message Message) time.Time {
	at := s.now().UTC()
	if at.Before(message.Delivery.UpdatedAt) {
		return message.Delivery.UpdatedAt
	}
	return at
}

func (s *Store) commitDelivery(ctx context.Context, message Message, delivery Delivery) error {
	key := messageKey(message.SessionID, message.Turn, message.ID)
	stored := s.state.Messages[key]
	message.Delivery = &delivery
	stored.Message = message
	next := s.copyState()
	next.Messages[key] = stored
	next.Revisions[scopeKey(message.SessionID, message.Turn)]++
	return s.commit(ctx, next, true)
}

func validDeliveryTransition(from, to string) bool {
	switch from {
	case "dispatching":
		return to == "running" || to == "completed" || to == "unknown"
	case "running":
		return to == "completed" || to == "unknown"
	default:
		return false
	}
}

func validMessageDelivery(message Message) bool {
	switch message.Transport {
	case MailboxTransport:
		return message.Delivery == nil
	case ManagedTransport, LegacyManagedTransport:
		return message.Delivery != nil && message.AckedAt == nil && message.BodyExpiredAt == nil && !message.Delivery.UpdatedAt.Before(message.AcceptedAt) && validDelivery(*message.Delivery)
	default:
		return false
	}
}

var providerIDPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)

// ValidProviderID accepts a nonempty stable adapter identifier used in receipts.
func ValidProviderID(id string) bool { return providerIDPattern.MatchString(id) }

var deliveryErrorPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)

func validDelivery(delivery Delivery) bool {
	if !validOutputEvidence(delivery) {
		return false
	}
	if delivery.Provider != "" && !ValidProviderID(delivery.Provider) {
		return false
	}
	if delivery.UpdatedAt.IsZero() || !validDeliveryText(delivery.Output) || delivery.Error != "" && !deliveryErrorPattern.MatchString(delivery.Error) {
		return false
	}
	if delivery.ThreadID != "" && !sessionPattern.MatchString(delivery.ThreadID) || delivery.TurnID != "" && !sessionPattern.MatchString(delivery.TurnID) {
		return false
	}
	switch delivery.State {
	case "queued", "dispatching":
		return delivery.ThreadID == "" && delivery.TurnID == "" && delivery.Output == "" && delivery.Error == ""
	case "running":
		return delivery.ThreadID != "" && delivery.TurnID != "" && delivery.Output == "" && delivery.Error == ""
	case "completed":
		return delivery.ThreadID != "" && delivery.TurnID != "" && delivery.Error == ""
	case "unknown":
		return delivery.Output == ""
	default:
		return false
	}
}

func validDeliveryText(text string) bool {
	if !utf8.ValidString(text) || len(text) > MaxBodyBytes || utf8.RuneCountInString(text) > MaxBodyRunes {
		return false
	}
	for _, r := range text {
		if unicode.IsControl(r) && r != '\n' && r != '\r' && r != '\t' {
			return false
		}
	}
	return true
}

// Execution status and output completeness are separate facts. The current
// dispatcher retains final output only for completed executions.
func validOutputEvidence(d Delivery) bool {
	if d.OutputState == "" {
		return d.OutputReason == ""
	}
	if d.State != "completed" {
		return false
	}
	switch d.OutputState {
	case OutputComplete:
		return d.OutputReason == ""
	case OutputTruncated:
		return d.OutputReason == "display-limit" || d.OutputReason == "sanitized"
	case OutputOmitted:
		return d.Output == "" && (d.OutputReason == "sanitized" || d.OutputReason == "no-final-text")
	default:
		return false
	}
}
