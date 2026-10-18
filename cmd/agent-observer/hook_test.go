package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHookSetupFailuresDoNotBlock(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	malformed := filepath.Join(dir, "malformed.json")
	insecure := filepath.Join(dir, "insecure.json")
	for path, data := range map[string]string{malformed: "{private-content", insecure: `{"version":1,"port":8767}`} {
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chmod(insecure, 0644); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(dir, "missing.json")
	cases := map[string][]string{
		"missing config":   {"--user-config", missing},
		"malformed config": {"--user-config", malformed},
		"insecure config":  {"--user-config", insecure},
		"unknown option":   {"--private-option=secret", "--user-config", missing},
		"invalid number":   {"--port", "private-value"},
		"positional":       {"unexpected-private-argument"},
		"provider":         {"--source-provider", "unknown"},
		"claude output":    {"--source-provider", "claude-code", "--hook-output", "relative.jsonl"},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			var out, stderr bytes.Buffer
			code := hookCommand(args, strings.NewReader(`{"hook_event_name":"SessionStart","session_id":"test"}`), &out, &stderr)
			if code != 0 {
				t.Fatalf("hook returned %d; collection errors must not reject a prompt", code)
			}
			assertHookWarning(t, out.Bytes())
			if stderr.Len() != 0 {
				t.Fatalf("setup error exposed diagnostics: %q", &stderr)
			}
		})
	}
}

func TestHookPortValidationAndHelp(t *testing.T) {
	config := filepath.Join(t.TempDir(), "private", "config.json")
	var out, stderr bytes.Buffer
	if setupCommand("init", []string{"--user-config", config}, &out, &stderr) != 0 {
		t.Fatalf("init: %s", &stderr)
	}
	for _, port := range []string{"0", "1023", "65536"} {
		out.Reset()
		stderr.Reset()
		if hookCommand([]string{"--user-config", config, "--port", port}, strings.NewReader("{}"), &out, &stderr) != 0 {
			t.Fatal("invalid port rejected prompt")
		}
		assertHookWarning(t, out.Bytes())
	}
	out.Reset()
	stderr.Reset()
	if hookCommand([]string{"--help"}, strings.NewReader(""), &out, &stderr) != 0 || out.Len() != 0 || !strings.Contains(stderr.String(), "-user-config") {
		t.Fatalf("hook help: stdout=%q stderr=%q", &out, &stderr)
	}
}

func assertHookWarning(t *testing.T, raw []byte) {
	t.Helper()
	var warning map[string]any
	if err := json.Unmarshal(raw, &warning); err != nil {
		t.Fatalf("expected one JSON warning: %q (%v)", raw, err)
	}
	if message, ok := warning["systemMessage"].(string); !ok || message == "" || len(warning) != 1 {
		t.Fatalf("expected advisory systemMessage only: %v", warning)
	}
	if bytes.Contains(raw, []byte("private")) || bytes.Contains(raw, []byte("secret")) {
		t.Fatal("hook exposed arguments or configuration contents")
	}
}
