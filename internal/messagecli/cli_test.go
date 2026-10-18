package messagecli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"agent-observer/internal/messaging"
)

const testToken = "private-scoped-token"

func fixtureMessage() messaging.Message {
	return messaging.Message{
		ID: "stable-message-id", SessionID: "session-a", Turn: "turn-a",
		Source: "agent:sender", Target: "agent:recipient", Body: "first line\n두 번째 줄\n",
		Transport: "mailbox", AcceptedAt: time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC),
	}
}

func identityFile(t *testing.T, endpoint, agent string) string {
	t.Helper()
	data, err := json.Marshal(identity{endpoint, testToken, "session-a", "turn-a", agent})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "identity.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func invoke(t *testing.T, args []string, body string) (int, string, string) {
	t.Helper()
	var out, diagnostics bytes.Buffer
	code := Run(args, strings.NewReader(body), &out, &diagnostics)
	if strings.Contains(out.String()+diagnostics.String(), testToken) {
		t.Fatal("CLI disclosed a scoped credential")
	}
	return code, out.String(), diagnostics.String()
}

func writeJSON(t *testing.T, out io.Writer, value any) {
	t.Helper()
	if err := json.NewEncoder(out).Encode(value); err != nil {
		t.Errorf("encode fixture: %v", err)
	}
}

func TestSendSingleRequestPreservesIDAndBody(t *testing.T) {
	want := fixtureMessage()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/api/mailbox/send" || r.Method != http.MethodPost {
			t.Errorf("request = %s %s; want one send POST", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer "+testToken {
			t.Error("missing scoped authorization")
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
		}
		if payload["id"] != want.ID || payload["body"] != want.Body || payload["target"] != want.Target || payload["transport"] != "mailbox" {
			t.Error("send did not preserve message payload")
		}
		for _, field := range []string{"source", "sessionId", "turn", "token", "agentId"} {
			if _, ok := payload[field]; ok {
				t.Errorf("caller-controlled identity field: %s", field)
			}
		}
		writeJSON(t, w, map[string]any{"message": want, "token": testToken})
	}))
	defer server.Close()
	path := identityFile(t, server.URL, want.Source)
	args := []string{"send", "--identity", path, "--id", want.ID, "--to", want.Target, "--body-stdin"}
	for attempt := 1; attempt <= 2; attempt++ {
		code, out, diagnostics := invoke(t, args, want.Body)
		if code != 0 {
			t.Fatalf("send exit %d: %s", code, diagnostics)
		}
		var response struct{ Message messaging.Message }
		if err := json.Unmarshal([]byte(out), &response); err != nil || response.Message.Body != want.Body || response.Message.ID != want.ID {
			t.Fatalf("wrong send response: %s", out)
		}
		if got := calls.Load(); got != int32(attempt) {
			t.Fatalf("send made %d HTTP calls after %d invocations; must not publish separately", got, attempt)
		}
	}
}

func TestSendRejectsMismatchedResponse(t *testing.T) {
	cases := map[string]func(*messaging.Message){
		"session":   func(m *messaging.Message) { m.SessionID = "session-b" },
		"turn":      func(m *messaging.Message) { m.Turn = "turn-b" },
		"sender":    func(m *messaging.Message) { m.Source = "agent:other" },
		"recipient": func(m *messaging.Message) { m.Target = "agent:other" },
		"body":      func(m *messaging.Message) { m.Body = "changed" },
		"ID":        func(m *messaging.Message) { m.ID = "other" },
		"reply":     func(m *messaging.Message) { m.ReplyTo = "other" },
		"transport": func(m *messaging.Message) { m.Transport = "native" },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			want := fixtureMessage()
			got := want
			change(&got)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writeJSON(t, w, map[string]any{"message": got})
			}))
			defer server.Close()
			path := identityFile(t, server.URL, want.Source)
			code, out, _ := invoke(t, []string{"send", "--identity", path, "--id", want.ID, "--to", want.Target, "--body-stdin"}, want.Body)
			if code == 0 || out != "" {
				t.Fatal("mismatched response was emitted as success")
			}
		})
	}
}

func TestSendRetryAcceptsAcknowledgedTombstone(t *testing.T) {
	want := fixtureMessage()
	ack := want.AcceptedAt.Add(time.Second)
	expiry := ack.Add(messaging.AckRetention)
	tombstone := want
	tombstone.Body = ""
	tombstone.AckedAt = &ack
	tombstone.BodyExpiredAt = &expiry
	cases := []struct {
		name   string
		change func(*messaging.Message)
		ok     bool
	}{
		{"valid", func(*messaging.Message) {}, true},
		{"unacknowledged", func(m *messaging.Message) { m.AckedAt = nil }, false},
		{"expired before acknowledgement", func(m *messaging.Message) { at := ack.Add(-time.Second); m.BodyExpiredAt = &at }, false},
		{"acknowledged before acceptance", func(m *messaging.Message) { at := want.AcceptedAt.Add(-time.Second); m.AckedAt = &at }, false},
		{"nonempty expired body", func(m *messaging.Message) { m.Body = "other content" }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			response := tombstone
			tc.change(&response)
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				var request messaging.Request
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.ID != want.ID || request.Body != want.Body {
					t.Error("retry changed original ID or content")
				}
				writeJSON(t, w, map[string]any{"message": response})
			}))
			defer server.Close()
			path := identityFile(t, server.URL, want.Source)
			code, out, diagnostics := invoke(t, []string{"send", "--identity", path, "--id", want.ID, "--to", want.Target, "--body-stdin"}, want.Body)
			if (code == 0) != tc.ok || calls.Load() != 1 {
				t.Fatalf("tombstone retry exit=%d calls=%d: %s", code, calls.Load(), diagnostics)
			}
			if tc.ok && (!strings.Contains(out, want.ID) || !strings.Contains(out, "bodyExpiredAt")) {
				t.Fatal("durable receipt was not returned")
			}
		})
	}
}

func TestEnrollSavesPrivateIdentityWithoutPrintingToken(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/api/mailbox/enroll" || r.Header.Get("Authorization") != "Bearer private-admin-token" {
			t.Error("wrong enrollment endpoint or authorization")
		}
		var payload map[string]string
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || payload["agentId"] != "agent:sender" || payload["sessionId"] != "session-a" || payload["turn"] != "turn-a" {
			t.Error("enrollment scope was not preserved")
		}
		writeJSON(t, w, map[string]string{"token": testToken})
	}))
	defer server.Close()
	dir := t.TempDir()
	admin := filepath.Join(dir, "admin")
	if err := os.WriteFile(admin, []byte("private-admin-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(dir, "identity.json")
	args := []string{"enroll", "--endpoint", server.URL, "--admin-file", admin, "--session", "session-a", "--turn", "turn-a", "--agent", "agent:sender", "--out", output}
	code, out, diagnostics := invoke(t, args, "")
	if code != 0 || out != "{\"enrolled\":true}\n" {
		t.Fatalf("enroll exit %d, out=%q, diagnostics=%s", code, out, diagnostics)
	}
	info, err := os.Stat(output)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("identity was not written with 0600 permissions")
	}
	id, err := readIdentity(output)
	if err != nil || id.Token != testToken || id.AgentID != "agent:sender" || id.Endpoint != server.URL {
		t.Fatal("saved identity does not match enrollment")
	}
	code, _, _ = invoke(t, args, "")
	if code == 0 || calls.Load() != 1 {
		t.Fatal("existing identity was overwritten or enrollment repeated")
	}
}

func TestReceiveWaitAndSeparateAck(t *testing.T) {
	message := fixtureMessage()
	var receives, acks atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/mailbox/receive":
			if receives.Add(1) == 1 {
				writeJSON(t, w, map[string]any{"messages": []messaging.Message{}})
				return
			}
			writeJSON(t, w, map[string]any{"messages": []messaging.Message{message}})
		case "/api/mailbox/ack":
			acks.Add(1)
			var payload map[string]string
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || payload["id"] != message.ID {
				t.Error("ack did not preserve message ID")
			}
			at := message.AcceptedAt.Add(time.Second)
			message.AckedAt = &at
			writeJSON(t, w, map[string]any{"message": message})
		default:
			t.Errorf("unexpected request: %s", r.URL.Path)
		}
	}))
	defer server.Close()
	path := identityFile(t, server.URL, message.Target)
	code, out, diagnostics := invoke(t, []string{"receive", "--identity", path, "--wait", "2s"}, "")
	if code != 0 || !strings.Contains(out, message.ID) || receives.Load() != 2 || acks.Load() != 0 {
		t.Fatalf("receive exit %d, calls=%d, acks=%d: %s", code, receives.Load(), acks.Load(), diagnostics)
	}
	code, out, diagnostics = invoke(t, []string{"ack", "--identity", path, "--id", message.ID}, "")
	if code != 0 || !strings.Contains(out, "ackedAt") || acks.Load() != 1 {
		t.Fatalf("ack exit %d: %s", code, diagnostics)
	}
}

func TestReceiveWaitExpiresWithoutAck(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeJSON(t, w, map[string]any{"messages": []messaging.Message{}})
	}))
	defer server.Close()
	path := identityFile(t, server.URL, "agent:recipient")
	started := time.Now()
	code, out, diagnostics := invoke(t, []string{"receive", "--identity", path, "--wait", "20ms"}, "")
	if code != 0 || out != "{\"messages\":[]}\n" || calls.Load() != 1 || time.Since(started) > time.Second {
		t.Fatalf("bounded receive failed: exit=%d out=%s diagnostics=%s", code, out, diagnostics)
	}
}

func TestReceiveWaitCannotInventEmptyInboxOnTimeout(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	path := identityFile(t, server.URL, "agent:recipient")
	code, out, _ := invoke(t, []string{"receive", "--identity", path, "--wait", "20ms"}, "")
	if code == 0 || out != "" {
		t.Fatal("timed out before observing any inbox state, but reported empty inbox")
	}
}

func TestCredentialsRejectUnsafeFiles(t *testing.T) {
	path := identityFile(t, "http://127.0.0.1:8767", "agent:sender")
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := readIdentity(path); err == nil {
		t.Fatal("world-readable credential accepted")
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := readIdentity(link); err == nil {
		t.Fatal("symlink credential accepted")
	}
	if _, err := readIdentity("relative.json"); err == nil {
		t.Fatal("relative credential path accepted")
	}
}

func TestRejectsInvalidFlagsBeforeAnySend(t *testing.T) {
	cases := [][]string{
		{"send", "--identity", "/unused", "--id", "same-id", "--to", "target", "--body-stdin", "--source", testToken},
		{"send", "--identity", "/unused", "--id", "same-id", "--to", "target", "--body-stdin", "--endpoint", "http://127.0.0.1:1"},
		{"send", "--identity", "/unused", "--to", "target", "--body-stdin"},
		{"send", "--identity", "/unused", "--id", "same-id", "--to", "target"},
		{"send", "--identity", "/unused", "--id", "same-id", "--to", "target", "--body-stdin", "--transport", "native"},
		{"receive", "--identity", "/unused", "--wait", "31s"},
		{"receive", "--identity", "/unused", "--wait", "-1s"},
	}
	for _, args := range cases {
		code, out, _ := invoke(t, args, "body")
		if code != 2 || out != "" {
			t.Errorf("invalid flags exit=%d, want2", code)
		}
	}
}

func TestBodyLimitAndUTF8(t *testing.T) {
	for _, body := range []string{"", " \n", strings.Repeat("가", 12001), "\xff"} {
		if _, err := readBody(strings.NewReader(body)); err == nil {
			t.Fatal("invalid body accepted")
		}
	}
	body := strings.Repeat("😀", 12000)
	if got, err := readBody(strings.NewReader(body)); err != nil || got != body {
		t.Fatal("valid maximum rune body rejected")
	}
}

func TestOnlyExplicitNumericLoopbackEndpoint(t *testing.T) {
	for _, endpoint := range []string{
		"https://127.0.0.1:8767", "http://localhost:8767", "http://example.com:8767", "http://127.0.0.1",
		"http://127.0.0.1:0", "http://127.0.0.1:65536", "http://127.0.0.1:08767", "http://127.0.0.1:8767/api",
		"http://user:secret@127.0.0.1:8767", "http://127.0.0.1:8767?x=1", "http://127.0.0.1:8767#x", "http://[::1]:8767",
	} {
		if _, err := localEndpoint(endpoint); err == nil {
			t.Errorf("unsafe endpoint accepted: %s", endpoint)
		}
	}
	if got, err := localEndpoint("http://127.0.0.1:8767/"); err != nil || got != "http://127.0.0.1:8767" {
		t.Fatal("valid loopback endpoint rejected")
	}
}

func TestRedirectDoesNotForwardCredential(t *testing.T) {
	var redirected atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected.Add(1) }))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	path := identityFile(t, server.URL, "agent:recipient")
	code, out, diagnostics := invoke(t, []string{"receive", "--identity", path}, "")
	if code == 0 || out != "" || !strings.Contains(diagnostics, "HTTP 307") || redirected.Load() != 0 {
		t.Fatal("redirect followed or reported as success")
	}
}

func TestHTTPFailureDoesNotEchoResponseBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, testToken+" internal-sensitive-error")
	}))
	defer server.Close()
	path := identityFile(t, server.URL, "agent:sender")
	code, out, diagnostics := invoke(t, []string{"send", "--identity", path, "--id", "same-id", "--to", "recipient", "--body-stdin"}, "body")
	if code == 0 || out != "" || strings.Contains(diagnostics, "internal-sensitive-error") || !strings.Contains(diagnostics, "HTTP 403") {
		t.Fatal("HTTP failure was not sanitized")
	}
}

func TestRequestRespectsCancellation(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	client, err := newClient(server.URL, testToken)
	if err != nil {
		t.Fatal(err)
	}
	defer client.close()
	if client.http.Timeout != 5*time.Second {
		t.Fatal("mailbox client lacks a bounded HTTP timeout")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err = client.post(ctx, "/api/mailbox/send", struct{}{})
	if err == nil || strings.Contains(err.Error(), testToken) || time.Since(started) > time.Second {
		t.Fatal("request cancellation was not bounded or sanitized")
	}
}

func TestSendManagedAliasReceipts(t *testing.T) {
	for _, requested := range []string{messaging.ManagedTransport, messaging.LegacyManagedTransport} {
		for _, stored := range []string{messaging.ManagedTransport, messaging.LegacyManagedTransport, messaging.MailboxTransport} {
			t.Run(requested+"/"+stored, func(t *testing.T) {
				message := fixtureMessage()
				message.Transport = stored
				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					var request messaging.Request
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.Transport != requested {
						t.Errorf("request transport = %q, %v; want %q", request.Transport, err, requested)
					}
					writeJSON(t, w, map[string]any{"message": message})
				}))
				defer server.Close()
				path := identityFile(t, server.URL, message.Source)
				code, out, diagnostics := invoke(t, []string{"send", "--identity", path, "--transport", requested, "--id", message.ID, "--to", message.Target, "--body-stdin"}, message.Body)
				wantSuccess := messaging.IsManagedTransport(stored)
				if (code == 0) != wantSuccess || calls.Load() != 1 {
					t.Fatalf("alias send exit = %d, calls = %d: %s", code, calls.Load(), diagnostics)
				}
				if !wantSuccess && out != "" {
					t.Fatal("unrelated mailbox transport accepted as managed receipt")
				}
			})
		}
	}
}
