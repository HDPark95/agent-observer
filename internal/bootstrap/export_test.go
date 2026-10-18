package bootstrap_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"agent-observer/internal/bootstrap"
	"agent-observer/internal/delivery"
	"agent-observer/internal/domain"
	"agent-observer/internal/model"
	"agent-observer/internal/sessions"
)

func writeConfig(t *testing.T, config bootstrap.ExportConfig) string {
	t.Helper()
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "exports.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func closeBox(t *testing.T, box *delivery.Outbox) {
	t.Helper()
	if err := box.Close(); err != nil {
		t.Error(err)
	}
}

func TestLoadExportsRequiresValidExplicitConfiguration(t *testing.T) {
	t.Setenv("OBSERVER_TEST_BAD_HEADERS", "SYNTHETIC_PRIVATE_HEADERS")
	t.Setenv("OBSERVER_TEST_EMPTY_KEY", "")
	base := bootstrap.ExportConfig{Outbox: t.TempDir(), Destinations: []bootstrap.DestinationConfig{{ID: "local-tempo", Type: "otlp", Endpoint: "http://127.0.0.1:4318/v1/traces"}}}
	for _, tc := range []struct {
		name   string
		change func(*bootstrap.ExportConfig)
	}{
		{"missing-id", func(c *bootstrap.ExportConfig) { c.Destinations[0].ID = "" }},
		{"blank-id", func(c *bootstrap.ExportConfig) { c.Destinations[0].ID = " " }},
		{"relative-outbox", func(c *bootstrap.ExportConfig) { c.Outbox = "relative" }},
		{"missing-destinations", func(c *bootstrap.ExportConfig) { c.Destinations = nil }},
		{"unknown-provider", func(c *bootstrap.ExportConfig) { c.Destinations[0].Type = "private-type" }},
		{"remote-plaintext", func(c *bootstrap.ExportConfig) { c.Destinations[0].Endpoint = "http://private-host.test/v1/traces" }},
		{"malformed-headers", func(c *bootstrap.ExportConfig) { c.Destinations[0].HeadersEnv = "OBSERVER_TEST_BAD_HEADERS" }},
		{"missing-env-name", func(c *bootstrap.ExportConfig) { c.Destinations[0].Type = "langfuse" }},
		{"empty-env-value", func(c *bootstrap.ExportConfig) {
			c.Destinations[0].Type = "langfuse"
			c.Destinations[0].PublicKeyEnv = "OBSERVER_TEST_EMPTY_KEY"
		}},
		{"duplicate-destination", func(c *bootstrap.ExportConfig) { c.Destinations = append(c.Destinations, c.Destinations[0]) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := base
			config.Destinations = append([]bootstrap.DestinationConfig(nil), base.Destinations...)
			tc.change(&config)
			box, err := bootstrap.LoadExports(writeConfig(t, config))
			if err == nil {
				closeBox(t, box)
				t.Fatal("invalid configuration accepted")
			}
			for _, forbidden := range []string{"SYNTHETIC_PRIVATE_HEADERS", "private-host", "private-type", "OBSERVER_TEST"} {
				if strings.Contains(err.Error(), forbidden) {
					t.Errorf("configuration detail leaked: %v", err)
				}
			}
		})
	}
	for _, text := range []string{`{"unknown":"SYNTHETIC_PRIVATE_CONFIG"}`, `{"outbox":`, `{}` + `{}`} {
		path := filepath.Join(t.TempDir(), "private-config.json")
		if err := os.WriteFile(path, []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
		box, err := bootstrap.LoadExports(path)
		if err == nil {
			closeBox(t, box)
			t.Fatal("malformed configuration accepted")
		}
		if strings.Contains(err.Error(), "PRIVATE") || strings.Contains(err.Error(), path) {
			t.Fatalf("configuration content leaked: %v", err)
		}
	}
}

func TestDestinationIdentitySeparatesProjectAndTenantButNotKeyRotation(t *testing.T) {
	t.Setenv("OBSERVER_TEST_API_KEY", "SYNTHETIC_KEY_ONE")
	t.Setenv("OBSERVER_TEST_WORKSPACE", "workspace-one")
	config := bootstrap.ExportConfig{Outbox: t.TempDir(), Destinations: []bootstrap.DestinationConfig{{ID: "workspace-one-project-a", Type: "langsmith", Endpoint: "https://example.test/api/v1/runs/batch", APIKeyEnv: "OBSERVER_TEST_API_KEY", WorkspaceIDEnv: "OBSERVER_TEST_WORKSPACE", Project: "project-a"}}}
	identity := func() string {
		box, err := bootstrap.LoadExports(writeConfig(t, config))
		if err != nil {
			t.Fatal(err)
		}
		defer closeBox(t, box)
		return box.Status()[0].Destination
	}
	initial := identity()
	t.Setenv("OBSERVER_TEST_API_KEY", "SYNTHETIC_KEY_TWO")
	if rotated := identity(); rotated != initial {
		t.Fatal("credential rotation changed destination checkpoint identity")
	}
	config.Destinations[0].Project = "project-b"
	project := identity()
	if project == initial {
		t.Fatal("different project reused destination checkpoint identity")
	}
	t.Setenv("OBSERVER_TEST_WORKSPACE", "workspace-two")
	config.Destinations[0].ID = "workspace-two-project-b"
	tenant := identity()
	if tenant == project {
		t.Fatal("explicit tenant change reused destination checkpoint identity")
	}
	if strings.Contains(initial, "workspace") || strings.Contains(initial, "example.test") || strings.Contains(initial, "project") {
		t.Fatalf("status exposes configuration: %s", initial)
	}
}

type received struct {
	provider string
	body     []byte
}

func destinationServer(t *testing.T, provider string, sink chan<- received) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("%s method=%s", provider, r.Method)
		}
		switch provider {
		case "otlp":
			if r.URL.Path != "/v1/traces" || r.Header.Get("X-Scope-OrgID") != "synthetic-team" {
				t.Error("OTLP endpoint or environment header missing")
			}
		case "langfuse":
			public, secret, ok := r.BasicAuth()
			if !ok || public != "SYNTHETIC_PUBLIC" || secret != "SYNTHETIC_SECRET" || r.URL.Path != "/api/public/otel/v1/traces" {
				t.Error("Langfuse endpoint or environment credentials missing")
			}
		case "langsmith":
			if r.URL.Path != "/api/v1/runs/batch" || r.Header.Get("x-api-key") != "SYNTHETIC_API_KEY" || r.Header.Get("x-tenant-id") != "synthetic-workspace" {
				t.Error("LangSmith endpoint or environment credentials missing")
			}
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		sink <- received{provider: provider, body: body}
		if provider == "langsmith" {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{}`)
	}))
}

func syntheticCodex(t *testing.T, session string) string {
	t.Helper()
	rows := []model.Record{
		{"type": "session_meta", "payload": model.Record{"id": session}},
		{"type": "event_msg", "timestamp": "2026-10-16T01:00:00Z", "payload": model.Record{"type": "task_started", "turn_id": "turn-test"}},
		{"type": "event_msg", "timestamp": "2026-10-16T01:00:00Z", "payload": model.Record{"type": "user_message", "message": "PRIVATE_REQUEST_BODY"}},
		{"type": "response_item", "timestamp": "2026-10-16T01:00:01Z", "payload": model.Record{"type": "function_call", "name": "exec_command", "call_id": "call-test", "arguments": `{"cmd":"echo PRIVATE_COMMAND"}`}},
		{"type": "response_item", "timestamp": "2026-10-16T01:00:02Z", "payload": model.Record{"type": "function_call_output", "call_id": "call-test", "output": "PRIVATE_TOOL_OUTPUT"}},
		{"type": "event_msg", "timestamp": "2026-10-16T01:00:03Z", "payload": model.Record{"type": "task_complete", "turn_id": "turn-test"}},
	}
	path := filepath.Join(t.TempDir(), "selected.jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	encoder := json.NewEncoder(f)
	for _, row := range rows {
		if err := encoder.Encode(row); err != nil {
			f.Close()
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestSelectedSourceCollectorExportsToThreeConfiguredDestinations(t *testing.T) {
	t.Setenv("OBSERVER_TEST_HEADERS", `{"X-Scope-OrgID":"synthetic-team"}`)
	t.Setenv("OBSERVER_TEST_PUBLIC", "SYNTHETIC_PUBLIC")
	t.Setenv("OBSERVER_TEST_SECRET", "SYNTHETIC_SECRET")
	t.Setenv("OBSERVER_TEST_KEY", "SYNTHETIC_API_KEY")
	t.Setenv("OBSERVER_TEST_TENANT", "synthetic-workspace")
	receivedRequests := make(chan received, 32)
	otlp := destinationServer(t, "otlp", receivedRequests)
	defer otlp.Close()
	langfuse := destinationServer(t, "langfuse", receivedRequests)
	defer langfuse.Close()
	langsmith := destinationServer(t, "langsmith", receivedRequests)
	defer langsmith.Close()
	config := bootstrap.ExportConfig{Outbox: t.TempDir(), Destinations: []bootstrap.DestinationConfig{
		{ID: "test-tempo-team", Type: "otlp", Endpoint: otlp.URL + "/v1/traces", HeadersEnv: "OBSERVER_TEST_HEADERS"},
		{ID: "test-langfuse-project", Type: "langfuse", Endpoint: langfuse.URL + "/api/public/otel/v1/traces", PublicKeyEnv: "OBSERVER_TEST_PUBLIC", SecretKeyEnv: "OBSERVER_TEST_SECRET"},
		{ID: "test-langsmith-workspace", Type: "langsmith", Endpoint: langsmith.URL + "/api/v1/runs/batch", APIKeyEnv: "OBSERVER_TEST_KEY", WorkspaceIDEnv: "OBSERVER_TEST_TENANT", Project: "synthetic-project"},
	}}
	box, err := bootstrap.LoadExports(writeConfig(t, config))
	if err != nil {
		t.Fatal(err)
	}
	defer closeBox(t, box)
	invalid := domain.Observation{ID: "invalid-timestamp", Run: domain.RunIdentity{Provider: "codex", SessionID: "selected-session", TurnID: "turn-test"}}
	if err := box.Submit([]domain.Observation{invalid}); err != nil {
		t.Fatal(err)
	}
	registry := sessions.New("")
	for _, selected := range []struct {
		session      string
		metadataOnly bool
	}{{"selected-session", false}, {"offered-session", true}} {
		path := syntheticCodex(t, selected.session)
		if _, err := registry.Register(model.Record{"source": path, "metadataOnly": selected.metadataOnly}, nil); err != nil {
			t.Fatal(err)
		}
	}
	collector := delivery.NewCollector(registry, box)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { collector.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	awaitAccepted(t, box)
	cancel()
	<-done
	close(receivedRequests)
	seen := make(map[string]int)
	selectedRun := domain.RunIdentity{Provider: "codex", SessionID: "selected-session", TurnID: "turn-test"}
	offeredRun := domain.RunIdentity{Provider: "codex", SessionID: "offered-session", TurnID: "turn-test"}
	for request := range receivedRequests {
		seen[request.provider]++
		for _, forbidden := range []string{"PRIVATE_", "SYNTHETIC_SECRET", "SYNTHETIC_API_KEY", "selected-session", "offered-session", "invalid-timestamp", offeredRun.Key(), invalid.Key()} {
			if strings.Contains(string(request.body), forbidden) {
				t.Errorf("%s body contains forbidden value %q", request.provider, forbidden)
			}
		}
		if !strings.Contains(string(request.body), "observer.observation_id") {
			t.Errorf("%s did not receive observations", request.provider)
		}
		if !strings.Contains(string(request.body), selectedRun.Key()) {
			t.Errorf("%s did not receive the selected run", request.provider)
		}
	}
	if seen["otlp"] == 0 || seen["langfuse"] == 0 || seen["langsmith"] == 0 {
		t.Fatalf("missing destination requests: %v", seen)
	}
	assertOutboxNoCredentials(t, config.Outbox)
	accepted := box.Status()
	closeBox(t, box)
	reloaded, err := bootstrap.LoadExports(writeConfig(t, config))
	if err != nil {
		t.Fatal(err)
	}
	if restored := reloaded.Status(); len(restored) != len(accepted) {
		t.Fatalf("destination checkpoints disappeared: %v", restored)
	} else {
		for i := range restored {
			if restored[i].Accepted != accepted[i].Accepted || restored[i].Rejected != 1 || restored[i].Pending != 0 {
				t.Errorf("destination receipt not restored: %+v -> %+v", accepted[i], restored[i])
			}
		}
	}
	closeBox(t, reloaded)
	config.Destinations = config.Destinations[2:]
	config.Destinations[0].ID = "different-langsmith-workspace"
	newTenant, err := bootstrap.LoadExports(writeConfig(t, config))
	if err != nil {
		t.Fatal(err)
	}
	defer closeBox(t, newTenant)
	if status := newTenant.Status()[0]; status.Accepted != 0 || status.Pending != 0 || status.Rejected != 0 {
		t.Fatalf("new logical tenant inherited another tenant's checkpoint: %+v", status)
	}
}

func awaitAccepted(t *testing.T, box *delivery.Outbox) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		all := true
		for _, status := range box.Status() {
			if status.Accepted < 2 || status.Pending != 0 || status.Rejected != 1 {
				all = false
			}
		}
		if all {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("delivery did not converge: %+v", box.Status())
		case <-ticker.C:
		}
	}
}

func assertOutboxNoCredentials(t *testing.T, directory string) {
	t.Helper()
	err := filepath.WalkDir(directory, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(data), "SYNTHETIC_") || strings.Contains(string(data), "PRIVATE_") {
			t.Errorf("outbox contains secret or transcript content: %s", entry.Name())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
