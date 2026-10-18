package httpapi

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"agent-observer/internal/messaging"
)

func TestMailboxUnavailableRecipientUsesStableRedactedError(t *testing.T) {
	response := httptest.NewRecorder()
	mailboxError(response, fmt.Errorf("private storage path: %w", messaging.ErrRecipientUnavailable))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d; want %d", response.Code, http.StatusServiceUnavailable)
	}
	if body := strings.TrimSpace(response.Body.String()); body != "recipient-unavailable" {
		t.Fatalf("body = %q; want stable code without private context", body)
	}
}
