package httpapi

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"agent-observer/internal/model"
	"agent-observer/internal/sessions"
)

func fixture(id, turn string) string {
	return fmt.Sprintf("{\"type\":\"session_meta\",\"payload\":{\"id\":%q}}\n{\"type\":\"event_msg\",\"timestamp\":\"2026-10-15T00:00:00Z\",\"payload\":{\"type\":\"task_started\",\"turn_id\":%q}}\n", id, turn)
}
func appendTool(t *testing.T, path, id string) {
	t.Helper()
	f, e := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		if err := f.Close(); err != nil {
			t.Error(err)
		}
	}()
	if _, err := fmt.Fprintf(f, "{\"type\":\"event_msg\",\"timestamp\":\"2026-10-15T00:00:00Z\",\"payload\":{\"type\":\"item_completed\",\"turn_id\":\"turn-a\",\"item\":{\"type\":\"CommandExecution\",\"id\":%q,\"command\":[\"echo\",\"ok\"],\"exit_code\":0}}}\n", id); err != nil {
		t.Fatal(err)
	}
}
func TestStreamFilesystemReconnectAndIsolation(t *testing.T) {
	for _, format := range []string{"snapshot", "delta"} {
		t.Run(format, func(t *testing.T) { testStreamFilesystemReconnectAndIsolation(t, format) })
	}
}
func testStreamFilesystemReconnectAndIsolation(t *testing.T, format string) {
	root := t.TempDir()
	dir := filepath.Join(root, "sessions", "2026", "10", "15")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	a, b := filepath.Join(dir, "a.jsonl"), filepath.Join(dir, "b.jsonl")
	if err := os.WriteFile(a, []byte(fixture("a", "turn-a")), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(b, []byte(fixture("b", "turn-b")), 0600); err != nil {
		t.Fatal(err)
	}
	registry := sessions.New("")
	if _, err := registry.Register(model.Record{"source": a}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Register(model.Record{"source": b}, nil); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(nil)
	_, port, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	server.Config.Handler = Handler(registry, port)
	server.Start()
	defer server.Close()
	open := func(id string) (<-chan model.Record, context.CancelFunc) {
		ctx, cancel := context.WithCancel(context.Background())
		req, err := http.NewRequestWithContext(ctx, "GET", server.URL+"/api/events?format="+format+"&session="+id, nil)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		if resp.StatusCode != 200 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
			resp.Body.Close()
			cancel()
			t.Fatal(resp.Status)
		}
		out := make(chan model.Record, 20)
		go func() {
			defer resp.Body.Close()
			defer close(out)
			scanner := bufio.NewScanner(resp.Body)
			scanner.Buffer(make([]byte, 4096), 1000000)
			kind := ""
			var baseline model.Record
			for scanner.Scan() {
				line := scanner.Text()
				if strings.HasPrefix(line, "event: ") {
					kind = strings.TrimPrefix(line, "event: ")
				}
				if (kind == "state" || kind == "delta") && strings.HasPrefix(line, "data: ") {
					var state model.Record
					if err := json.Unmarshal([]byte(line[6:]), &state); err != nil {
						t.Errorf("decode stream state: %v", err)
						return
					}
					if kind == "delta" {
						state = reconstructDelta(t, baseline, state)
					}
					baseline = state
					select {
					case out <- state:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
		return out, cancel
	}
	next := func(ch <-chan model.Record) model.Record {
		t.Helper()
		select {
		case s := <-ch:
			if s == nil {
				t.Fatal("stream closed")
			}
			if s.Object("transport")["fileWatch"] != true {
				t.Fatalf("file notification coverage unhealthy: %v", s.Object("transport"))
			}
			return s
		case <-time.After(2 * time.Second):
			t.Fatal("no filesystem-driven snapshot within 2s (recovery is 10s)")
			return nil
		}
	}
	ca, closeA := open("a")
	defer closeA()
	cb, closeB := open("b")
	defer closeB()
	if next(ca).String("sessionId") != "a" || next(cb).String("sessionId") != "b" {
		t.Fatal("initial scope")
	}
	appendTool(t, a, "only-a")
	state := next(ca)
	raw, _ := json.Marshal(state)
	if !strings.Contains(string(raw), "only-a") {
		t.Fatal(string(raw))
	}
	select {
	case s := <-cb:
		t.Fatalf("unrelated session event: %v", s)
	case <-time.After(100 * time.Millisecond):
	}

	// A child created after subscription must push both its tool and its A2A call.
	childID := "11111111-2222-3333-4444-555555555555"
	appendRow := func(path string, row model.Record) {
		row["timestamp"] = "2026-10-15T00:00:01Z"
		t.Helper()
		f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := f.Close(); err != nil {
				t.Error(err)
			}
		}()
		if err := json.NewEncoder(f).Encode(row); err != nil {
			t.Fatal(err)
		}
	}
	appendRow(a, model.Record{"type": "event_msg", "payload": model.Record{"type": "item_completed", "turn_id": "turn-a", "item": model.Record{"type": "SubAgentActivity", "id": "spawn", "agent_path": "/root/ux", "agent_thread_id": childID, "kind": "started"}}})
	next(ca)
	// Tomorrow does not exist when the stream starts. Creating the directory
	// and transcript together must wake discovery without the 10s recovery tick.
	childDir := filepath.Join(filepath.Dir(dir), "16")
	if err := os.MkdirAll(childDir, 0700); err != nil {
		t.Fatal(err)
	}
	child := filepath.Join(childDir, "rollout-test-"+childID+".jsonl")
	appendRow(child, model.Record{"type": "session_meta", "payload": model.Record{"id": childID, "agent_path": "/root/ux", "parent_thread_id": "a", "subagent_history_start_ordinal": 1}})
	appendRow(child, model.Record{"type": "event_msg", "ordinal": 2, "payload": model.Record{"type": "item_completed", "item": model.Record{"type": "CommandExecution", "id": "child-tool", "command": []string{"echo", "ok"}, "exit_code": 0}}})
	awaitText := func(text string) model.Record {
		t.Helper()
		for {
			s := next(ca)
			raw, _ := json.Marshal(s)
			if strings.Contains(string(raw), text) {
				return s
			}
		}
	}
	awaitText("child-tool")
	args, _ := json.Marshal(model.Record{"target": "/root", "message": ""})
	appendRow(child, model.Record{"type": "response_item", "ordinal": 3, "payload": model.Record{"type": "function_call", "namespace": "collaboration", "name": "send_message", "call_id": "message", "arguments": string(args)}})
	awaitText("a2a:/root/ux:message")
	// Losing and restoring a date directory must not strand the child watcher.
	childBytes, err := os.ReadFile(child)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(childDir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(childDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(child, childBytes, 0600); err != nil {
		t.Fatal(err)
	}
	appendRow(child, model.Record{"type": "event_msg", "ordinal": 4, "payload": model.Record{"type": "item_completed", "item": model.Record{"type": "CommandExecution", "id": "child-after-directory-recreation", "command": []string{"echo", "ok"}, "exit_code": 0}}})
	awaitText("child-after-directory-recreation")
	select {
	case s := <-cb:
		t.Fatalf("child discovery leaked into unrelated session: %v", s)
	default:
	}
	if _, err := registry.Publish(model.Record{"sessionId": "a", "callId": "message", "target": "/root", "body": "hook body arrives without transcript write"}, true); err != nil {
		t.Fatal(err)
	}
	awaitText("hook body arrives without transcript write")
	closeA()
	reconnected, cancel := open("a")
	defer cancel()
	raw, _ = json.Marshal(next(reconnected))
	if !strings.Contains(string(raw), "only-a") {
		t.Fatal("reconnect lost latest state")
	}
	// Atomic replacement must be observed through the directory watch.
	temp := filepath.Join(dir, "new")
	if err := os.WriteFile(temp, []byte(fixture("a", "replacement")), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(temp, a); err != nil {
		t.Fatal(err)
	}
	for {
		if next(reconnected).String("turn") == "replacement" {
			break
		}
	}
	if _, err := registry.Register(model.Record{"source": a, "label": "renamed"}, nil); err != nil {
		t.Fatal(err)
	}
	if next(reconnected).String("sessionLabel") != "renamed" {
		t.Fatal("registration notification lost")
	}
}
func TestStreamRejectsUnknownAndCrossOrigin(t *testing.T) {
	h := Handler(sessions.New(""), "8767")
	for _, tc := range []struct {
		path, origin string
		status       int
	}{{"/api/events?session=missing", "", 404}, {"/api/events", "https://example.com", 403}} {
		req := httptest.NewRequest("GET", tc.path, nil)
		req.Host = "127.0.0.1:8767"
		req.Header.Set("Origin", tc.origin)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != tc.status {
			t.Fatal(w.Code)
		}
	}
}

// Consumer contract tested against real file updates, child discovery, message
// bodies, source replacement and reconnect in both stream formats.
func reconstructDelta(t *testing.T, previous, patch model.Record) model.Record {
	t.Helper()
	if previous == nil || patch["baseSequence"] != previous["sequence"] || patch["sessionId"] != previous["sessionId"] {
		t.Error("invalid delta baseline")
	}
	nodes := map[string]model.Record{}
	order := []string{}
	removed := map[string]bool{}
	for _, id := range patch.Array("removed") {
		removed[id.(string)] = true
	}
	for _, value := range previous.Array("nodes") {
		n := model.As(value)
		id := n.String("id")
		if !removed[id] {
			nodes[id] = n
			order = append(order, id)
		}
	}
	for _, value := range patch.Array("upsert") {
		n := model.As(value)
		id := n.String("id")
		if _, ok := nodes[id]; !ok {
			order = append(order, id)
		}
		nodes[id] = n
	}
	if explicit, ok := patch["order"].([]any); ok {
		order = nil
		for _, id := range explicit {
			order = append(order, id.(string))
		}
	}
	result := patch.Object("metadata").Clone()
	list := []any{}
	for _, id := range order {
		list = append(list, nodes[id])
	}
	result["nodes"] = list
	result["sequence"] = patch["sequence"]
	return result
}
