package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"agent-observer/internal/workers"
)

func TestWorkerProviderSelection(t *testing.T) {
	for _, tt := range []struct{ id, wantID, wantName string }{
		{"", "codex", "Codex"},
		{"codex", "codex", "Codex"},
		{"claude-code", "claude-code", "Claude Code"},
	} {
		provider, err := selectWorkerProvider(tt.id)
		if err != nil || provider.ID != tt.wantID || provider.Name != tt.wantName || provider.ExecutionLabel == "" || provider.Factory == nil {
			t.Fatalf("provider %q = %+v, %v; want %s adapter", tt.id, provider, err, tt.wantName)
		}
	}
	for _, id := range []string{"unknown", "Codex", "Claude-Code", "claude"} {
		if _, err := selectWorkerProvider(id); err == nil {
			t.Fatalf("unsupported provider %q silently selected a fallback", id)
		}
	}
}

func workerConfigFile(t *testing.T, provider string) string {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cfg := workers.Config{Provider: provider, SessionID: "session", Turn: "turn", Owner: "agent", Executable: executable, CWD: t.TempDir(), Workers: []workers.Definition{{ID: "managed:ux", Name: "UX"}}}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "team.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestWorkerConfigurationDefaultsOnlyAtCompositionRoot(t *testing.T) {
	path := workerConfigFile(t, "")
	config, err := workers.LoadConfig(path)
	if err != nil || config.Provider != "" {
		t.Fatalf("config loader defaulted provider: %q, %v", config.Provider, err)
	}
	team, err := loadWorkerTeam(path)
	if err != nil || team.config.Provider != "codex" || team.provider.ID != "codex" {
		t.Fatalf("composition did not resolve legacy configuration: %+v, %v", team, err)
	}
}

func TestWorkerConfigurationPreservesExplicitProviderAndExecutable(t *testing.T) {
	for _, id := range []string{"codex", "claude-code"} {
		t.Run(id, func(t *testing.T) {
			path := workerConfigFile(t, id)
			config, err := workers.LoadConfig(path)
			if err != nil {
				t.Fatal(err)
			}
			team, err := loadWorkerTeam(path)
			if err != nil || team.config.Provider != id || team.provider.ID != id {
				t.Fatalf("configuration selected a different provider: %+v, %v", team, err)
			}
			if team.config.Executable != config.Executable || team.config.CWD != config.CWD {
				t.Fatal("provider selection changed the operator's executable or directory")
			}
		})
	}
}

func TestUnsupportedWorkerProviderDoesNotOpenMailbox(t *testing.T) {
	for _, existing := range []bool{false, true} {
		name := "new store"
		if existing {
			name = "existing v1 store"
		}
		t.Run(name, func(t *testing.T) {
			mailbox := filepath.Join(t.TempDir(), "mailbox")
			const state = `{"version":1,"capabilities":{},"messages":{},"revisions":{}}`
			if existing {
				if err := os.Mkdir(mailbox, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(mailbox, "mailbox.json"), []byte(state), 0600); err != nil {
					t.Fatal(err)
				}
			}
			original := os.Args
			t.Cleanup(func() { os.Args = original })
			os.Args = []string{"agent-observer", "--workers-config", workerConfigFile(t, "unregistered-runtime"), "--mailbox-store", mailbox}
			if code := run(); code != 2 {
				t.Fatalf("unsupported provider exit = %d; want 2", code)
			}
			if !existing {
				if _, err := os.Stat(mailbox); !os.IsNotExist(err) {
					t.Fatalf("unsupported provider touched mailbox: %v", err)
				}
				return
			}
			data, err := os.ReadFile(filepath.Join(mailbox, "mailbox.json"))
			if err != nil || string(data) != state {
				t.Fatal("unsupported provider migrated or changed existing state")
			}
			entries, err := os.ReadDir(mailbox)
			if err != nil || len(entries) != 1 || entries[0].Name() != "mailbox.json" {
				t.Fatal("unsupported provider opened storage before rejecting configuration")
			}
		})
	}
}
