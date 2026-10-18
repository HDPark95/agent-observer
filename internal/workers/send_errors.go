package workers

import (
	"context"
	"errors"

	"agent-observer/internal/messaging"
)

// SendFailure contains fixed recovery guidance, never raw provider diagnostics.
type SendFailure struct {
	Code      string `json:"code"`
	Retryable bool   `json:"retryable"`
	Outcome   string `json:"outcome"`
	Action    string `json:"action"`
}

func failure(code string, retryable bool, outcome, action string) SendFailure {
	return SendFailure{code, retryable, outcome, action}
}

// DescribeSendFailure gives uncertainty precedence over rejection, including
// joined errors. Only an identical retry may resolve an uncertain commit.
func DescribeSendFailure(err error) SendFailure {
	switch {
	case errors.Is(err, messaging.ErrOutcomeUnknown):
		return failure("outcome_unknown", true, "unknown", "Acceptance is not confirmed. Retry only the identical message_id, target, and body to resolve it without duplicating delivery.")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return failure("send_interrupted", false, "unknown", "Check Observer's delivery status before trying again. Preserve the identical message_id, target, and body if a retry is needed.")
	case errors.Is(err, messaging.ErrConflict):
		return failure("id_conflict", false, "rejected", "This message_id already identifies different content. Check the original message; use a new ID only for a distinct message.")
	case errors.Is(err, messaging.ErrInvalid):
		return failure("invalid_request", false, "rejected", "Correct the target, message_id, or body to match the tool constraints. Do not repeat the invalid request.")
	case errors.Is(err, messaging.ErrNotFound):
		return failure("recipient_not_found", false, "rejected", "Check the target against the configured team and have the recipient enrolled before sending.")
	case errors.Is(err, messaging.ErrRecipientUnavailable):
		return failure("recipient_unavailable", false, "rejected", "The recipient is stopped. Ask the coordinator to restore it before retrying the identical message_id and body.")
	case errors.Is(err, messaging.ErrUnauthorized), errors.Is(err, messaging.ErrForbidden):
		return failure("not_authorized", false, "rejected", "Ask the coordinator to repair this worker's messaging scope. Do not retry with another identity.")
	case errors.Is(err, messaging.ErrUnsupported):
		return failure("unsupported_transport", false, "rejected", "Ask the coordinator to configure the managed messaging transport.")
	case errors.Is(err, messaging.ErrCapacity):
		return failure("capacity_reached", false, "rejected", "Ask the coordinator to free messaging capacity before retrying the identical message_id and body.")
	case errors.Is(err, messaging.ErrClosed), errors.Is(err, messaging.ErrLocked):
		return failure("store_unavailable", false, "rejected", "Ask the coordinator to restore the message store before retrying the identical message_id and body.")
	default:
		return failure("send_failed", false, "unknown", "Check Observer's delivery status and resolve the failure before retrying. Preserve the identical message_id, target, and body; acceptance is not confirmed.")
	}
}
