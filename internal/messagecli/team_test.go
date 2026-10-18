package messagecli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"agent-observer/internal/messaging"
	"agent-observer/internal/workers"
)

func TestTeamStartUsesSessionEnvironmentAndPersistentClientConfig(t *testing.T) {
	t.Setenv("CODEX_THREAD_ID", "selected-session")
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/api/mailbox/team/start" || r.Header.Get("Authorization") != "Bearer "+testToken {
			t.Error("wrong start endpoint or credential")
		}
		var req workers.StartRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		if req.SessionID != "selected-session" || req.ID != "task" || req.Target != "managed:ux" {
			t.Errorf("wrong scope: %+v", req)
		}
		writeJSON(t, w, map[string]any{"message": messaging.Message{ID: req.ID, SessionID: req.SessionID, Turn: "server-turn", Source: "agent", Target: req.Target, Body: req.Body,
			Transport: messaging.ManagedTransport, AcceptedAt: time.Now(), Delivery: &messaging.Delivery{State: "queued"}}})
	}))
	defer server.Close()
	dir := t.TempDir()
	admin := filepath.Join(dir, "admin.token")
	if err := os.WriteFile(admin, []byte(testToken), 0600); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(dir, "client.json")
	invokeTeam := func(args []string, body string) string {
		t.Helper()
		var out, errOut bytes.Buffer
		if code := RunTeam(args, strings.NewReader(body), &out, &errOut); code != 0 {
			t.Fatalf("code=%d error=%s", code, errOut.String())
		}
		if strings.Contains(out.String()+errOut.String(), testToken) || body != "" && strings.Contains(out.String(), body) {
			t.Fatal("team client echoed credential or task body")
		}
		return out.String()
	}
	invokeTeam([]string{"configure", "--config", config, "--endpoint", server.URL, "--admin-file", admin}, "")
	for i := 0; i < 2; i++ {
		out := invokeTeam([]string{"start", "--config", config, "--id", "task", "--body-stdin"}, "private task body")
		if !strings.Contains(out, "server-turn") {
			t.Fatal("resolved turn missing from receipt")
		}
	}
	if calls != 2 {
		t.Fatalf("expected one POST per command, got %d", calls)
	}
	t.Setenv("CODEX_THREAD_ID", "")
	if _, err := parseTeamOptions("start", []string{"--config", config, "--id", "task", "--body-stdin"}); err == nil {
		t.Fatal("missing session selected an implicit latest session")
	}
	_, err := parseTeamOptions("start", []string{"--config", config, "--session", "selected", "--id", "task", "--body-stdin", "--to", ""})
	if err == nil || !strings.Contains(err.Error(), "--to") {
		t.Fatalf("invalid recipient needs a target-specific error: %v", err)
	}
}
