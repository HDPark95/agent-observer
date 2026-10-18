package hooks

import (
	"strings"
	"testing"

	"agent-observer/internal/model"
)

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
