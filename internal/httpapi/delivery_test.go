package httpapi

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"agent-observer/internal/model"
	"agent-observer/internal/sessions"
)

func TestDeliveryStatusIsOptInAndReadOnly(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		calls := 0
		var status func() any
		if enabled {
			status = func() any { calls++; return model.Record{"enabled": true, "destinations": []any{}} }
		}
		h := HandlerWithDelivery(sessions.New(""), "8767", status)
		r := httptest.NewRequest("GET", "/api/delivery", nil)
		r.Host = "127.0.0.1:8767"
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		var body model.Record
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if w.Code != 200 || body.Bool("enabled") != enabled {
			t.Fatalf("delivery status = %d %v", w.Code, body)
		}
		wantCalls := 0
		if enabled {
			wantCalls = 1
		}
		if calls != wantCalls {
			t.Fatalf("status calls = %d, want %d", calls, wantCalls)
		}
		r.Host = "example.com"
		w = httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 403 || calls != wantCalls {
			t.Fatal("delivery status bypassed host boundary")
		}
	}
}
