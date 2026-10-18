package pluginmcp

func toolDefinitions() []map[string]any {
	text := map[string]any{"type": "string"}
	return append([]map[string]any{
		tool("observer_start", "Start a local Observer viewer on demand, or reuse an existing Observer. No sessions or user settings are read. Plugin-owned servers stop when this MCP connection closes.", false,
			map[string]any{"port": map[string]any{"type": "integer", "description": "Default 8767; 0 selects an available loopback port; otherwise 1024–65535.", "minimum": 0, "maximum": 65535}}, nil),
		tool("observer_status", "Check only the selected viewer's health. Does not discover or read sessions.", true, map[string]any{}, nil),
		tool("observer_connect_session", "Connect only an explicitly selected absolute JSONL transcript. Returns its ID and viewer URL, never transcript contents. Do not guess a latest session.", false,
			map[string]any{"source": text, "session_id": map[string]any{"type": "string", "description": "Optional expected source session ID, checked against the selected file."}, "provider": map[string]any{"type": "string", "enum": []string{"codex", "claude-code"}}}, []string{"source"}),
		tool("observer_open_view", "Get the verified loopback URL for the host's browser-opening tool. This tool returns a URL; it does not itself open a browser. Only sessions connected in this MCP connection can be selected.", true,
			map[string]any{"session_id": text}, nil),
	}, teamToolDefinitions()...)
}

func tool(name, description string, readOnly bool, properties map[string]any, required []string) map[string]any {
	schema := map[string]any{"type": "object", "properties": properties, "additionalProperties": false}
	if len(required) > 0 {
		schema["required"] = required
	}
	return map[string]any{
		"name": name, "description": description, "inputSchema": schema,
		"annotations": map[string]any{"readOnlyHint": readOnly, "destructiveHint": false, "idempotentHint": true, "openWorldHint": false},
	}
}
