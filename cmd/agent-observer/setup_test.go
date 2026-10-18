package main

import (
	"bytes"
	"flag"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"agent-observer/internal/httpapi"
	"agent-observer/internal/sessions"
)

func TestInitRequiresCatalogConsent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "config.json")
	var out, stderr bytes.Buffer
	args := []string{"--user-config", path, "--catalog-root", filepath.Join(t.TempDir(), "not-read")}
	if setupCommand("init", args, &out, &stderr) == 0 {
		t.Fatal("catalog consent not required")
	}
	if code := setupCommand("init", append(args, "--allow-catalog"), &out, &stderr); code != 0 {
		t.Fatalf("init: %s", &stderr)
	}
	if setupCommand("init", append(args, "--allow-catalog"), &out, &stderr) == 0 {
		t.Fatal("replaced existing configuration")
	}
}

func TestDoctorIdentifiesProductWithoutReadingSessions(t *testing.T) {
	registry := sessions.New("")
	var requested string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requested = r.URL.Path
		_, port, _ := strings.Cut(r.Host, ":")
		httpapi.Handler(registry, port).ServeHTTP(w, r)
	}))
	defer server.Close()
	var out bytes.Buffer
	if err := identifyServer(server.URL, &out); err != nil {
		t.Fatal(err)
	}
	if requested != "/healthz" {
		t.Fatal(requested)
	}
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"product":"other"}`)) }))
	defer other.Close()
	if err := identifyServer(other.URL, &out); err == nil {
		t.Fatal("accepted a different service")
	}
}

func TestInitExplainsExplicitConfiguration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "config.json")
	var out, stderr bytes.Buffer
	if code := setupCommand("init", []string{"--user-config", path}, &out, &stderr); code != 0 {
		t.Fatalf("init: %s", &stderr)
	}
	if !strings.Contains(out.String(), "same --user-config") || !strings.Contains(out.String(), path) {
		t.Fatalf("missing instructions for starting with the created configuration: %s", &out)
	}
}

func TestUsageListsFirstRunCommands(t *testing.T) {
	var out bytes.Buffer
	flags := flag.NewFlagSet("agent-observer", flag.ContinueOnError)
	flags.SetOutput(&out)
	flags.Int("port", 8767, "loopback port")
	printUsage(flags, &out)
	for _, text := range []string{"./agent-observer demo", "init", "doctor", "version", "Ctrl-C", "-port"} {
		if !strings.Contains(out.String(), text) {
			t.Errorf("help is missing %q", text)
		}
	}
}
