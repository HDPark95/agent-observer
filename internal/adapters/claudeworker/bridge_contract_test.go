package claudeworker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"agent-observer/internal/messaging"
	"agent-observer/internal/workers"
)

const validToolCall = `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"observer_send_message","arguments":{"target":"managed:dx","message_id":"test-message","body":"test body"}}}`

func reviewBridge(t *testing.T, send workers.SendFunc) *messageBridge {
	t.Helper()
	b, err := newMessageBridge(context.Background(), send)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.close)
	return b
}

func reviewRequest(b *messageBridge, body string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, b.url(), strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+b.token)
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Accept", "application/json, text/event-stream")
	return r
}

func TestMessageBridgeHTTPContract(t *testing.T) {
	var calls atomic.Int32
	b := reviewBridge(t, func(context.Context, workers.Outbound) (workers.SendReceipt, error) {
		calls.Add(1)
		return workers.SendReceipt{}, nil
	})
	tests := []struct {
		name, body string
		change     func(*http.Request)
		status     int
		contains   string
	}{
		{name: "initialize", body: `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}`, status: 200, contains: `"protocolVersion":"2025-06-18"`},
		{name: "list", body: `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`, status: 200, contains: `"name":"observer_send_message"`},
		{name: "notification", body: `{"jsonrpc":"2.0","method":"notifications/initialized"}`, status: 202},
		{name: "ping", body: `{"jsonrpc":"2.0","id":"ping","method":"ping"}`, status: 200, contains: `"result":{}`},
		{name: "GET", change: func(r *http.Request) { r.Method = http.MethodGet }, status: 405},
		{name: "bad host", change: func(r *http.Request) { r.Host = "unexpected.invalid" }, status: 403},
		{name: "origin", change: func(r *http.Request) { r.Header.Set("Origin", b.url()) }, status: 403},
		{name: "no token", change: func(r *http.Request) { r.Header.Del("Authorization") }, status: 401},
		{name: "bad token", change: func(r *http.Request) { r.Header.Set("Authorization", "Bearer wrong") }, status: 401},
		{name: "query", change: func(r *http.Request) { r.URL.RawQuery = "x=1" }, status: 404},
		{name: "bad version", change: func(r *http.Request) { r.Header.Set("MCP-Protocol-Version", "unknown") }, status: 400},
		{name: "unknown method", body: `{"jsonrpc":"2.0","id":1,"method":"not-supported"}`, status: 200, contains: `"code":-32601`},
		{name: "null id", body: `{"jsonrpc":"2.0","id":null,"method":"ping"}`, status: 200, contains: `"code":-32600`},
		{name: "batch", body: `[]`, status: 200, contains: `"code":-32600`},
		{name: "two objects", body: `{} {}`, status: 200, contains: `"code":-32600`},
		{name: "malformed", body: `{`, status: 200, contains: `"code":-32600`},
		{name: "oversized", body: strings.Repeat("x", maxRequestBytes+1), status: 413},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := reviewRequest(b, tt.body)
			if tt.change != nil {
				tt.change(r)
			}
			w := httptest.NewRecorder()
			b.serveHTTP(w, r)
			if w.Code != tt.status || !strings.Contains(w.Body.String(), tt.contains) {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			if tt.status == 202 && w.Body.Len() != 0 {
				t.Fatal("notification response must be empty")
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatal("non-tool request invoked sender")
	}
}

func TestMessageBridgeRejectsInvalidToolInput(t *testing.T) {
	var calls atomic.Int32
	b := reviewBridge(t, func(context.Context, workers.Outbound) (workers.SendReceipt, error) {
		calls.Add(1)
		return workers.SendReceipt{}, nil
	})
	a := b.begin(context.Background())
	close(a.ready)
	defer b.end(a)
	tests := []struct{ name, raw string }{
		{"unknown tool", strings.Replace(validToolCall, "observer_send_message", "other_tool", 1)},
		{"unknown sender", strings.Replace(validToolCall, `"body":"test body"`, `"body":"test body","source":"forged"`, 1)},
		{"empty body", strings.Replace(validToolCall, "test body", " ", 1)},
		{"large body", strings.Replace(validToolCall, "test body", strings.Repeat("x", messaging.MaxBodyRunes+1), 1)},
		{"control character", strings.Replace(validToolCall, "test body", `test\u0000body`, 1)},
		{"bad id", strings.Replace(validToolCall, "test-message", "bad id", 1)},
		{"bad target", strings.Replace(validToolCall, "managed:dx", "../dx", 1)},
		{"null arguments", `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"observer_send_message","arguments":null}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			b.serveHTTP(w, reviewRequest(b, tt.raw))
			if !strings.Contains(w.Body.String(), `"isError":true`) {
				t.Fatalf("expected tool rejection: %s", w.Body.String())
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatalf("invalid input reached sender %d times", calls.Load())
	}
}

func TestMessageBridgeWaitsForDurableAcceptance(t *testing.T) {
	sent := make(chan workers.Outbound, 1)
	b := reviewBridge(t, func(_ context.Context, input workers.Outbound) (workers.SendReceipt, error) {
		sent <- input
		return workers.SendReceipt{ID: input.ID, Target: input.Target, State: "queued"}, nil
	})
	a := b.begin(context.Background())
	defer b.end(a)
	response := make(chan *httptest.ResponseRecorder, 1)
	go func() { w := httptest.NewRecorder(); b.serveHTTP(w, reviewRequest(b, validToolCall)); response <- w }()
	select {
	case <-sent:
		t.Fatal("send ran before receipt persistence")
	case <-time.After(25 * time.Millisecond):
	}
	close(a.ready)
	select {
	case input := <-sent:
		if input.Body != "test body" || input.Target != "managed:dx" {
			t.Fatalf("unexpected input: %+v", input)
		}
	case <-time.After(time.Second):
		t.Fatal("accepted send blocked")
	}
	select {
	case w := <-response:
		if !strings.Contains(w.Body.String(), `"isError":false`) || strings.Contains(w.Body.String(), "test body") {
			t.Fatalf("unexpected reply: %s", w.Body.String())
		}
	case <-time.After(time.Second):
		t.Fatal("response blocked")
	}
}

func TestMessageBridgeRejectedAcceptanceAndEndedInputCannotSend(t *testing.T) {
	for _, stage := range []string{"idle", "acceptance-failed", "ended", "request-cancelled"} {
		t.Run(stage, func(t *testing.T) {
			var calls atomic.Int32
			b := reviewBridge(t, func(context.Context, workers.Outbound) (workers.SendReceipt, error) {
				calls.Add(1)
				return workers.SendReceipt{}, nil
			})
			r := reviewRequest(b, validToolCall)
			if stage != "idle" {
				a := b.begin(context.Background())
				defer b.end(a)
				switch stage {
				case "acceptance-failed":
					a.cancel()
				case "ended":
					close(a.ready)
					b.end(a)
				case "request-cancelled":
					ctx, cancel := context.WithCancel(r.Context())
					cancel()
					r = r.WithContext(ctx)
					close(a.ready)
				}
			}
			w := httptest.NewRecorder()
			b.serveHTTP(w, r)
			if calls.Load() != 0 || !strings.Contains(w.Body.String(), `"isError":true`) {
				t.Fatalf("rejected input reached sender: calls=%d reply=%s", calls.Load(), w.Body.String())
			}
		})
	}
}

func TestMessageBridgeEndCancelsAndJoinsAdmittedSender(t *testing.T) {
	entered, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	b := reviewBridge(t, func(ctx context.Context, _ workers.Outbound) (workers.SendReceipt, error) {
		calls.Add(1)
		close(entered)
		<-ctx.Done()
		close(cancelled)
		<-release
		return workers.SendReceipt{}, ctx.Err()
	})
	a := b.begin(context.Background())
	close(a.ready)
	done := make(chan struct{})
	go func() { defer close(done); b.serveHTTP(httptest.NewRecorder(), reviewRequest(b, validToolCall)) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("sender did not start")
	}
	ended := make(chan struct{})
	go func() { b.end(a); close(ended) }()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("end did not cancel sender")
	}
	select {
	case <-ended:
		t.Fatal("end returned with sender still in flight")
	default:
	}
	close(release)
	select {
	case <-ended:
	case <-time.After(time.Second):
		t.Fatal("end did not join sender")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("request did not finish")
	}
	b.serveHTTP(httptest.NewRecorder(), reviewRequest(b, validToolCall))
	if calls.Load() != 1 {
		t.Fatal("ended input admitted another send")
	}
}

func TestMessageBridgeLoopbackAndClose(t *testing.T) {
	b := reviewBridge(t, func(context.Context, workers.Outbound) (workers.SendReceipt, error) {
		return workers.SendReceipt{}, errors.New("SECRET diagnostic")
	})
	a := b.begin(context.Background())
	close(a.ready)
	r := reviewRequest(b, validToolCall)
	r.RequestURI = ""
	client := &http.Client{Timeout: time.Second}
	response, err := client.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "SECRET") || !strings.Contains(string(data), "send_failed") {
		t.Fatalf("unsafe failure reply: %s", data)
	}
	b.close()
	if _, err := client.Get(b.url()); err == nil {
		t.Fatal("closed endpoint still accepts connections")
	}
}

func TestMessageBridgeSafeFailureSemantics(t *testing.T) {
	for _, tt := range []struct {
		err           error
		code, outcome string
		retry         bool
	}{
		{messaging.ErrOutcomeUnknown, "outcome_unknown", "unknown", true},
		{messaging.ErrConflict, "id_conflict", "rejected", false},
		{context.DeadlineExceeded, "send_interrupted", "unknown", false},
		{messaging.ErrRecipientUnavailable, "recipient_unavailable", "rejected", false},
	} {
		t.Run(tt.code, func(t *testing.T) {
			data, err := json.Marshal(sendFailure(tt.err))
			if err != nil {
				t.Fatal(err)
			}
			var envelope struct {
				IsError bool `json:"isError"`
				Content []struct {
					Text string `json:"text"`
				} `json:"content"`
			}
			if json.Unmarshal(data, &envelope) != nil || !envelope.IsError || len(envelope.Content) != 1 {
				t.Fatalf("invalid tool reply: %s", data)
			}
			var detail struct {
				Code, Outcome string
				Retryable     bool
			}
			if json.Unmarshal([]byte(envelope.Content[0].Text), &detail) != nil || detail.Code != tt.code || detail.Outcome != tt.outcome || detail.Retryable != tt.retry {
				t.Fatalf("wrong send semantics: %s", data)
			}
		})
	}
}
