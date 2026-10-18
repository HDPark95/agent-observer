package domain

// SafeProvider rejects arbitrary strings before they reach external metadata.
func SafeProvider(provider string) string {
	switch provider {
	case "codex", "claude-code":
		return provider
	default:
		return "unknown"
	}
}

// SafeKind preserves only the observation categories understood by Observer.
func SafeKind(kind string) string {
	switch kind {
	case "request", "agent", "subagent", "call", "tool", "skill", "message", "a2a":
		return kind
	default:
		return "unknown"
	}
}

// SafeOutcome accepts normalized evidence states, not arbitrary provider text.
// A returned tool call and a completed turn do not imply task success.
func SafeOutcome(outcome string) string {
	switch outcome {
	case "observed", "running", "returned", "completed", "error", "read", "read-attempt", "unknown":
		return outcome
	default:
		return "unknown"
	}
}

// SafeToolName limits export names to known tool identifiers. Custom commands,
// MCP names, file names, and browser sites stay in the local graph projection.
func SafeToolName(name string) string {
	switch name {
	case "", "shell", "cat", "sed", "head", "tail", "rg", "grep", "ls", "find",
		"python", "python3", "node", "uv", "npm", "pnpm", "yarn", "pytest", "git",
		"curl", "mkdir", "wc", "codex", "claude", "go", "make", "gofmt",
		"cua_repl.js", "tools.skill_observation", "tools.observation_workflow",
		"Bash", "Read", "Write", "Edit", "MultiEdit", "Glob", "Grep", "LS",
		"WebFetch", "WebSearch", "Agent", "Task", "TaskOutput", "TaskStop",
		"TodoWrite", "NotebookEdit", "Skill", "ToolSearch", "SendMessage":
		return name
	default:
		return "tool"
	}
}
