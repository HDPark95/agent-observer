package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"agent-observer/internal/messaging"
	"agent-observer/internal/model"
	"agent-observer/internal/sessions"
	"agent-observer/internal/workers"
)

// Mailbox is HTTP composition of a messaging service and its enrollment store.
// No graph read or external exporter participates in a message send.
type Mailbox struct {
	Store   *messaging.Store
	Service *messaging.Service
	Teams   *workers.Teams
}

func (m *Mailbox) serve(w http.ResponseWriter, r *http.Request, registry *sessions.Registry, port string) {
	if !allowedOrigin(r, port) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || token == "" {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if r.URL.Path == "/api/mailbox/enroll" {
		m.enroll(w, r, registry, token)
		return
	}
	if r.URL.Path == "/api/mailbox/team/start" {
		m.startTeam(w, r, token)
		return
	}
	if _, err := m.Store.Authenticate(token); err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	switch r.URL.Path {
	case "/api/mailbox/send":
		var request messaging.Request
		if !readMailboxPayload(w, r, &request) {
			return
		}
		message, err := m.Service.Send(r.Context(), token, request)
		if err != nil {
			mailboxError(w, err)
			return
		}
		writeJSON(w, model.Record{"message": message})
	case "/api/mailbox/receive":
		if !readMailboxPayload(w, r, &struct{}{}) {
			return
		}
		messages, err := m.Service.Receive(r.Context(), token)
		if err != nil {
			mailboxError(w, err)
			return
		}
		if len(messages) > 16 {
			messages = messages[:16]
		}
		if messages == nil {
			messages = []messaging.Message{}
		}
		writeJSON(w, model.Record{"messages": messages})
	case "/api/mailbox/ack":
		var request struct {
			ID string `json:"id"`
		}
		if !readMailboxPayload(w, r, &request) {
			return
		}
		message, err := m.Service.Ack(r.Context(), token, request.ID)
		if err != nil {
			mailboxError(w, err)
			return
		}
		writeJSON(w, model.Record{"message": message})
	default:
		http.NotFound(w, r)
	}
}

func (m *Mailbox) startTeam(w http.ResponseWriter, r *http.Request, token string) {
	if !m.Store.AuthenticateAdmin(token) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	if m.Teams == nil {
		http.Error(w, "Automatic teams disabled", http.StatusServiceUnavailable)
		return
	}
	var input workers.StartRequest
	if !readMailboxPayload(w, r, &input) {
		return
	}
	message, err := m.Teams.Start(r.Context(), input)
	if err != nil {
		mailboxError(w, err)
		return
	}
	writeJSON(w, model.Record{"message": message})
}

func (m *Mailbox) enroll(w http.ResponseWriter, r *http.Request, registry *sessions.Registry, token string) {
	if !m.Store.AuthenticateAdmin(token) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	var principal messaging.Principal
	if !readMailboxPayload(w, r, &principal) {
		return
	}
	if registry.ValidateParticipant(principal.SessionID, principal.Turn, principal.AgentID) != nil {
		http.Error(w, "Unobserved participant", http.StatusBadRequest)
		return
	}
	capability, err := m.Store.Enroll(r.Context(), principal)
	if err != nil {
		mailboxError(w, err)
		return
	}
	writeJSON(w, model.Record{"token": capability})
}

func readMailboxPayload(w http.ResponseWriter, r *http.Request, target any) bool {
	if r.Header.Get("Content-Type") != "application/json" {
		http.Error(w, "Expected JSON", http.StatusBadRequest)
		return false
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 80000))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return false
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return false
	}
	return true
}

func mailboxError(w http.ResponseWriter, err error) {
	status, code := http.StatusInternalServerError, "mailbox-storage-error"
	switch {
	case errors.Is(err, messaging.ErrUnauthorized):
		status, code = http.StatusUnauthorized, "unauthorized"
	case errors.Is(err, messaging.ErrForbidden):
		status, code = http.StatusForbidden, "forbidden"
	case errors.Is(err, messaging.ErrNotFound):
		status, code = http.StatusNotFound, "not-found"
	case errors.Is(err, messaging.ErrConflict):
		status, code = http.StatusConflict, "message-id-conflict"
	case errors.Is(err, messaging.ErrUnsupported):
		status, code = http.StatusUnprocessableEntity, "unsupported-transport"
	case errors.Is(err, messaging.ErrInvalid):
		status, code = http.StatusBadRequest, "invalid-request"
	case errors.Is(err, messaging.ErrCapacity):
		status, code = http.StatusInsufficientStorage, "mailbox-full"
	case errors.Is(err, messaging.ErrClosed):
		status, code = http.StatusServiceUnavailable, "mailbox-unavailable"
	case errors.Is(err, messaging.ErrRecipientUnavailable):
		status, code = http.StatusServiceUnavailable, "recipient-unavailable"
	case errors.Is(err, messaging.ErrOutcomeUnknown):
		status, code = http.StatusServiceUnavailable, "outcome-unknown-retry-same-id"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		status, code = http.StatusRequestTimeout, "request-canceled"
	}
	// Never reflect user content, capabilities, or storage paths into errors.
	http.Error(w, code, status)
}
