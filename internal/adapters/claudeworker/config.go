package claudeworker

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"agent-observer/internal/workers"
)

const qualifiedSendTool = "mcp__observer__observer_send_message"

func prepareConfig(cfg workers.RuntimeConfig, bridge *messageBridge) (string, []string, error) {
	dir, err := os.MkdirTemp("", "observer-claude-*")
	if err != nil {
		return "", nil, errors.New("create claude worker configuration")
	}
	fail := func() (string, []string, error) {
		_ = os.RemoveAll(dir)
		return "", nil, errors.New("write claude worker configuration")
	}
	mcpPath, promptPath := filepath.Join(dir, "mcp.json"), filepath.Join(dir, "instructions.txt")
	config := map[string]any{"mcpServers": map[string]any{"observer": map[string]any{
		"type": "http", "url": bridge.url(), "headers": map[string]string{"Authorization": "Bearer " + bridge.token},
	}}}
	data, err := json.Marshal(config)
	if err != nil {
		return fail()
	}
	if err := os.WriteFile(mcpPath, data, 0600); err != nil {
		return fail()
	}
	if err := os.WriteFile(promptPath, []byte(workerInstructions(cfg)), 0600); err != nil {
		return fail()
	}
	args := []string{
		"--print", "--input-format", "stream-json", "--output-format", "stream-json", "--verbose",
		"--replay-user-messages", "--no-session-persistence", "--restricted",
		"--permission-mode", "dontAsk", "--permission-prompts", "none",
		"--tools", "Read,Glob,Grep", "--allowedTools", "Read,Glob,Grep," + qualifiedSendTool,
		"--disable-slash-commands", "--strict-mcp-config", "--mcp-config", mcpPath,
		"--settings", `{"disableAllHooks":true,"autoMemoryEnabled":false}`,
		"--append-system-prompt-file", promptPath, "--no-chrome",
	}
	return dir, args, nil
}

func workerInstructions(cfg workers.RuntimeConfig) string {
	var text strings.Builder
	text.WriteString(cfg.Instructions)
	fmt.Fprintf(&text, "\n\nYou are the Observer-managed worker %s.\nConfigured teammates:\n", cfg.WorkerID)
	for _, peer := range cfg.Peers {
		fmt.Fprintf(&text, "- %s (%s)\n", peer.ID, peer.Name)
	}
	text.WriteString("Use " + qualifiedSendTool + " to send messages to these teammates. One call durably records the body and automatically runs the recipient; do not poll mailboxes or invoke a separate recording tool. Supply a stable message_id for each distinct message and reuse it only for an identical retry. Do not send replies unless the task requires them. Work read-only within the configured working directory: do not modify files, execute shell commands, access external services, or spawn other agents.\n")
	return text.String()
}
