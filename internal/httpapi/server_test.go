package httpapi

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"agent-observer/internal/messages"
	"agent-observer/internal/sessions"
)

func TestHTTPContract(t *testing.T) {
	h := Handler(sessions.New(""), "8767")
	for _, tc := range []struct {
		method, path, host, origin, body string
		status                           int
	}{
		{"GET", "/", "127.0.0.1:8767", "", "", 200},
		{"GET", "/api/sessions", "127.0.0.1:8767", "", "", 200},
		{"GET", "/api/state?session=missing", "127.0.0.1:8767", "", "", 404},
		{"GET", "/", "example.com", "", "", 403},
		{"POST", "/api/messages", "127.0.0.1:8767", "https://example.com", "{}", 403},
		{"POST", "/api/messages", "127.0.0.1:8767", "", "[]", 400},
		{"POST", "/api/messages", "127.0.0.1:8767", "", strings.Repeat("x", 80001), 400},
		{"GET", "/missing", "localhost:8767", "", "", http.StatusNotFound},
		{"GET", "/api/events", "127.0.0.1:8767", "https://example.com", "", http.StatusForbidden},
		{"POST", "/missing", "localhost:8767", "http://localhost:8767", "{}", http.StatusNotFound},
		{"POST", "/api/messages", "127.0.0.1:8767", "", "null", http.StatusBadRequest},
		{"POST", "/api/messages", "127.0.0.1:8767", "", "{} {}", http.StatusBadRequest},
		{"DELETE", "/api/sessions", "127.0.0.1:8767", "", "", http.StatusMethodNotAllowed},
	} {
		r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		r.Host = tc.host
		r.Header.Set("Origin", tc.origin)
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Errorf("%s %s (host %q, origin %q): status = %d, want %d", tc.method, tc.path, tc.host, tc.origin, w.Code, tc.status)
		}
		if tc.status == http.StatusMethodNotAllowed && w.Header().Get("Allow") != "GET, POST" {
			t.Error("method rejection lost Allow header")
		}
	}
}

func TestPostRequiresJSONContentType(t *testing.T) {
	h := Handler(sessions.New(""), "8767")
	r := httptest.NewRequest(http.MethodPost, "/api/sessions/open", strings.NewReader(`{}`))
	r.Host = "127.0.0.1:8767"
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("missing content type: status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestAttachmentDurabilityFailureIsRetryableWithoutLeakingDetails(t *testing.T) {
	w := httptest.NewRecorder()
	writeAttachmentError(w, fmt.Errorf("private-path: %w", messages.ErrDurabilityUnknown))
	if w.Code != http.StatusServiceUnavailable || w.Header().Get("Retry-After") != "1" || !strings.Contains(w.Body.String(), "identical attachment") || strings.Contains(w.Body.String(), "private-path") {
		t.Fatalf("unexpected uncertainty response: %v", w)
	}
	w = httptest.NewRecorder()
	writeAttachmentError(w, fmt.Errorf("private invalid request"))
	if w.Code != http.StatusBadRequest || strings.Contains(w.Body.String(), "private") {
		t.Fatal("invalid request response changed")
	}
}
