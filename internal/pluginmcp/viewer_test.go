package pluginmcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func testViewer(t *testing.T) *Viewer {
	t.Helper()
	v := NewViewer()
	t.Cleanup(func() { _ = v.Close() })
	return v
}

func testPort(t *testing.T, endpoint string) int {
	t.Helper()
	_, port, err := net.SplitHostPort(strings.TrimPrefix(endpoint, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	n, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func testSource(t *testing.T, id string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "selected.jsonl")
	data := fmt.Sprintf("{\"type\":\"session_meta\",\"payload\":{\"id\":%q}}\n{\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"PRIVATE_FIXTURE_BODY\"}}\n", id)
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestViewerStartReuseAndOwnership(t *testing.T) {
	ctx := context.Background()
	owner := testViewer(t)
	if got := owner.Status(ctx); got["running"] != false || owner.endpoint != "" {
		t.Fatalf("status started a viewer: %v", got)
	}
	started, err := owner.Start(ctx, 0)
	if err != nil || started["running"] != true || started["ownedByPlugin"] != true {
		t.Fatalf("start: %v %v", started, err)
	}
	again, err := owner.Start(ctx, 0)
	if err != nil || again["url"] != started["url"] {
		t.Fatalf("repeat: %v %v", again, err)
	}
	guest := testViewer(t)
	reused, err := guest.Start(ctx, testPort(t, owner.endpoint))
	if err != nil || reused["running"] != true || reused["ownedByPlugin"] != false {
		t.Fatalf("reuse: %v %v", reused, err)
	}
	if err := guest.Close(); err != nil {
		t.Fatal(err)
	}
	if !owner.healthy(ctx, owner.endpoint) {
		t.Fatal("closing guest stopped existing service")
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if guest.healthy(ctx, owner.endpoint) {
		t.Fatal("owned service survived close")
	}
}

func TestViewerRejectsUnrelatedOrInvalidHealth(t *testing.T) {
	for _, kind := range []string{"unrelated", "redirect", "oversized", "invalid-json"} {
		t.Run(kind, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch kind {
				case "redirect":
					http.Redirect(w, r, "/healthz", http.StatusFound)
				case "oversized":
					fmt.Fprint(w, `{"product":"agent-observer","padding":"`+strings.Repeat("x", 9000)+`"}`)
				case "invalid-json":
					fmt.Fprint(w, "not JSON")
				default:
					fmt.Fprint(w, `{"product":"another-app"}`)
				}
			}))
			defer server.Close()
			v := testViewer(t)
			if _, err := v.Start(context.Background(), testPort(t, server.URL)); err == nil {
				t.Fatal("invalid health response was accepted")
			}
			if v.endpoint != "" {
				t.Fatal("failed start retained an endpoint")
			}
		})
	}
}

func TestViewerRecoversAfterReusedServerStops(t *testing.T) {
	ctx := context.Background()
	owner, guest := testViewer(t), testViewer(t)
	if _, err := owner.Start(ctx, 0); err != nil {
		t.Fatal(err)
	}
	port := testPort(t, owner.endpoint)
	if _, err := guest.Start(ctx, port); err != nil {
		t.Fatal(err)
	}
	if _, err := guest.Connect(ctx, testSource(t, "old-session"), "", ""); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if guest.Status(ctx)["running"] != false {
		t.Fatal("stopped server reported healthy")
	}
	got, err := guest.Start(ctx, port)
	if err != nil || got["running"] != true || got["ownedByPlugin"] != true || len(guest.connected) != 0 {
		t.Fatalf("restart retained stale state: %v %v", got, err)
	}
}

func TestViewerConnectSelectionAndPrivacy(t *testing.T) {
	ctx := context.Background()
	v := testViewer(t)
	source := testSource(t, "selected-session")
	if _, err := v.Connect(ctx, source, "", ""); err == nil {
		t.Fatal("connected before start")
	}
	if _, err := v.Start(ctx, 0); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []struct{ source, id, provider string }{
		{"relative.jsonl", "", ""}, {source, "different-session", ""}, {source, "", "unsupported"},
		{filepath.Join(t.TempDir(), "absent.jsonl"), "", ""},
	} {
		if _, err := v.Connect(ctx, bad.source, bad.id, bad.provider); err == nil {
			t.Fatal("invalid selection accepted")
		}
	}
	for i := 0; i < 2; i++ {
		got, err := v.Connect(ctx, source, "selected-session", "codex")
		if err != nil || got["sessionId"] != "selected-session" {
			t.Fatalf("connect: %v %v", got, err)
		}
		encoded, err := json.Marshal(got)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(encoded), source) || strings.Contains(string(encoded), "PRIVATE_FIXTURE_BODY") {
			t.Fatal("connection result disclosed transcript data")
		}
	}
	if len(v.connected) != 1 {
		t.Fatal("repeated connection duplicated the selection")
	}
	if _, err := v.Connect(ctx, testSource(t, "selected-session"), "", ""); err == nil {
		t.Fatal("same ID from another path accepted")
	}
	if _, err := v.View(ctx, "unselected-session"); err == nil {
		t.Fatal("unselected session view allowed")
	}
	got, err := v.View(ctx, "selected-session")
	if err != nil || got["browserOpened"] != false || !strings.HasSuffix(got["url"].(string), "/?session=selected-session") {
		t.Fatalf("view: %v %v", got, err)
	}
}
