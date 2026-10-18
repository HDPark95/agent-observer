// Package hooks forwards only approved hook fields to the local observer.
package hooks

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
	"unicode/utf8"

	"agent-observer/internal/agents"
	"agent-observer/internal/model"
)

func Payload(event model.Record) (string, model.Record) {
	kind := event.String("hook_event_name")
	if kind == "SessionStart" || kind == "UserPromptSubmit" {
		if kind == "SessionStart" && event.String("source") == "fork" {
			return "", nil
		}
		if event.String("transcript_path") == "" || event.String("session_id") == "" {
			return "", nil
		}
		return "/api/sessions", model.Record{"source": event["transcript_path"], "sessionId": event["session_id"], "metadataOnly": true}
	}
	if kind != "PostToolUse" {
		return "", nil
	}
	tool := event.String("tool_name")
	if tool != "send_message" && tool != "collaboration.send_message" && tool != "collaboration/send_message" && tool != "collaborationsend_message" {
		return "", nil
	}
	args := event.Object("tool_input")
	message, ok := args["message"].(string)
	if !ok || utf8.RuneCountInString(message) > 12000 || agents.Encrypted(message) {
		return "", nil
	}
	return "/api/message-hooks", model.Record{"sessionId": event["session_id"], "callId": event["tool_use_id"], "target": args["target"], "body": message}
}
func Run(in io.Reader, out io.Writer, port int) {
	raw, err := io.ReadAll(io.LimitReader(in, 100001))
	if err != nil || len(raw) > 100000 {
		return
	}
	var event model.Record
	if json.Unmarshal(raw, &event) != nil {
		return
	}
	route, p := Payload(event)
	if p == nil {
		return
	}
	body, _ := json.Marshal(p)
	client := http.Client{Timeout: 800 * time.Millisecond, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Post(fmt.Sprintf("http://127.0.0.1:%d%s", port, route), "application/json", bytes.NewReader(body))
	if err == nil {
		defer response.Body.Close()
		if response.StatusCode == 200 {
			return
		}
	}
	json.NewEncoder(out).Encode(model.Record{"systemMessage": "Observer 자동 수집을 완료하지 못했습니다. 원래 작업은 재실행하지 않습니다."})
}
