package claudeworker

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"agent-observer/internal/workers"
)

func TestLaunchConfigurationHasOnlyReadToolsAndBoundMessaging(t *testing.T) {
	bridge, err := newMessageBridge(context.Background(), rejectSend)
	if err != nil {
		t.Fatal(err)
	}
	defer bridge.close()
	cfg := workers.RuntimeConfig{WorkerID: "managed:ux", Instructions: "Review message clarity.", Peers: []workers.Peer{{ID: "managed:dx", Name: "DX"}}}
	dir, args, err := prepareConfig(cfg, bridge)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	flags := make(map[string]string)
	for i, arg := range args {
		if strings.HasPrefix(arg, "--") {
			flags[arg] = ""
			if i+1 < len(args) && !strings.HasPrefix(args[i+1], "--") {
				flags[arg] = args[i+1]
			}
		}
	}
	for key, want := range map[string]string{
		"--input-format": "stream-json", "--output-format": "stream-json",
		"--tools": "Read,Glob,Grep", "--allowedTools": "Read,Glob,Grep," + qualifiedSendTool,
		"--permission-mode": "dontAsk", "--permission-prompts": "none",
		"--restricted": "", "--strict-mcp-config": "", "--no-session-persistence": "", "--disable-slash-commands": "", "--no-chrome": "",
	} {
		if got, ok := flags[key]; !ok || got != want {
			t.Errorf("%s = %q (present %v), want %q", key, got, ok, want)
		}
	}
	for _, key := range []string{"--dangerously-skip-permissions", "--allow-dangerously-skip-permissions", "--resume", "--continue", "--bare"} {
		if _, ok := flags[key]; ok {
			t.Errorf("unexpected launch option %s", key)
		}
	}
	if strings.Contains(strings.Join(args, " "), bridge.token) || strings.Contains(strings.Join(args, " "), cfg.Instructions) {
		t.Fatal("private configuration leaked into process arguments")
	}
	text, err := os.ReadFile(filepath.Join(dir, "instructions.txt"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{cfg.Instructions, cfg.WorkerID, "managed:dx", qualifiedSendTool} {
		if !strings.Contains(string(text), want) {
			t.Errorf("worker instructions missing %q", want)
		}
	}
}
