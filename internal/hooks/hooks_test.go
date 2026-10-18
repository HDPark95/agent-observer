package hooks

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"agent-observer/internal/model"
)

func TestDeliveryFailuresRemainAdvisory(t *testing.T) {
	var redirected atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { redirected.Add(1) }))
	defer target.Close()
	for _, mode := range []string{"success", "unavailable", "redirect", "timeout", "offline"} {
		t.Run(mode, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				if r.URL.Path != "/api/sessions" || len(body) != 3 || body["metadataOnly"] != true || body["sessionId"] != "selected" {
					t.Errorf("unexpected registration payload: %v", body)
				}
				switch mode {
				case "unavailable":
					w.WriteHeader(http.StatusServiceUnavailable)
				case "redirect":
					http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
				case "timeout":
					<-r.Context().Done()
				}
			}))
			defer server.Close()
			_, portString, err := net.SplitHostPort(server.Listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			port, err := strconv.Atoi(portString)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "offline" {
				server.Close()
			}
			var out bytes.Buffer
			start := time.Now()
			Run(strings.NewReader(`{"hook_event_name":"UserPromptSubmit","session_id":"selected","transcript_path":"/selected.jsonl","prompt":"never forward"}`), &out, port)
			if elapsed := time.Since(start); elapsed > 2*time.Second {
				t.Fatalf("hook exceeded host timeout: %v", elapsed)
			}
			if mode == "success" {
				if out.Len() != 0 {
					t.Fatalf("successful hook output: %s", &out)
				}
				return
			}
			var result map[string]any
			if err := json.Unmarshal(out.Bytes(), &result); err != nil || len(result) != 1 || result["systemMessage"] == nil {
				t.Fatalf("expected advisory warning only: %s, %v", &out, err)
			}
		})
	}
	if redirected.Load() != 0 {
		t.Fatal("hook followed a redirect with session metadata")
	}
}

func TestOnlyApprovedFields(t *testing.T) {
	route, p := Payload(model.Record{"hook_event_name": "UserPromptSubmit", "session_id": "s", "transcript_path": "/selected.jsonl", "prompt": "private"})
	if route != "/api/sessions" || !p.Bool("metadataOnly") || len(p) != 3 {
		t.Fatal(route, p)
	}
	_, p = Payload(model.Record{"hook_event_name": "SessionStart", "source": "fork"})
	if p != nil {
		t.Fatal("fork registered")
	}
	e := model.Record{"hook_event_name": "PostToolUse", "tool_name": "collaboration.send_message", "session_id": "s", "tool_use_id": "call", "tool_input": model.Record{"target": "dx", "message": "검사 완료", "other": "private"}, "tool_response": "private"}
	route, p = Payload(e)
	if route != "/api/message-hooks" || p.String("body") != "검사 완료" || len(p) != 4 {
		t.Fatal(route, p)
	}
	e["tool_name"] = "other"
	_, p = Payload(e)
	if p != nil {
		t.Fatal("unrelated tool captured")
	}
}

func TestEncryptedHookBodyIsNotForwardedAsPlaintext(t *testing.T) {
	route, payload := Payload(model.Record{"hook_event_name": "PostToolUse", "tool_name": "collaborationsend_message", "tool_input": model.Record{"target": "/root", "message": "gAAAAA" + strings.Repeat("x", 80)}})
	if route != "" || payload != nil {
		t.Fatal("encrypted hook input was forwarded as a message body")
	}
}

func TestNamespacedMessageToolNames(t *testing.T) {
	for _, name := range []string{"send_message", "collaboration.send_message", "collaboration/send_message", "collaborationsend_message"} {
		t.Run(name, func(t *testing.T) {
			route, p := Payload(model.Record{"hook_event_name": "PostToolUse", "tool_name": name, "session_id": "selected", "tool_use_id": "call", "tool_input": model.Record{"target": "/root", "message": "automatic body"}})
			if route != "/api/message-hooks" || p.String("body") != "automatic body" {
				t.Fatalf("Payload(%q) = %q, %v; want message capture", name, route, p)
			}
		})
	}
	for _, name := range []string{"mcp__slack__send_message", "othersend_message", "collaborationsend_message_extra", "collaboration.send_email"} {
		_, p := Payload(model.Record{"hook_event_name": "PostToolUse", "tool_name": name, "tool_input": model.Record{"target": "/root", "message": "unrelated"}})
		if p != nil {
			t.Fatalf("unrelated tool %q was captured", name)
		}
	}
}
