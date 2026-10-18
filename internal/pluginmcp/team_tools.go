package pluginmcp

import (
	"context"
	"encoding/json"
	"errors"
)

func (p *protocol) invokeTeam(ctx context.Context, name string, raw json.RawMessage) (map[string]any, error) {
	invalid := errors.New("invalid team arguments; use the documented input schema")
	switch name {
	case "observer_team_create":
		var args createTeamArgs
		if decodeArguments(raw, &args) != nil {
			return nil, invalid
		}
		return p.teams.Create(ctx, p.viewer, args)
	case "observer_team_send":
		var args struct {
			TeamID string `json:"team_id"`
			ID     string `json:"message_id"`
			Target string `json:"target"`
			Body   string `json:"body"`
		}
		if decodeArguments(raw, &args) != nil {
			return nil, invalid
		}
		return p.teams.Send(ctx, args.TeamID, args.ID, args.Target, args.Body)
	default:
		var args struct {
			TeamID string `json:"team_id"`
		}
		if decodeArguments(raw, &args) != nil || args.TeamID == "" {
			return nil, invalid
		}
		if name == "observer_team_stop" {
			return p.teams.Stop(args.TeamID), nil
		}
		return p.teams.Status(args.TeamID)
	}
}

func teamToolDefinitions() []map[string]any {
	text := map[string]any{"type": "string"}
	member := map[string]any{"type": "object", "additionalProperties": false,
		"properties": map[string]any{"id": map[string]any{"type": "string", "pattern": "^managed:[a-z][a-z0-9_-]{0,63}$"}, "name": text, "task": text, "instructions": text},
		"required":   []string{"id", "name", "task", "instructions"}}
	create := tool("observer_team_create", "Create or reconnect an Observer-managed Codex team for a connected session and its observed turn. Requires an explicitly selected absolute workspace. Defaults to UX and DX. Opens a dedicated loopback viewer, persists messages privately, and never substitutes native subagents. Workers are read-only; executing them uses the user's Codex account. Keep the returned teamId for messages and status.", false,
		map[string]any{"session_id": text, "workspace": text, "codex_executable": map[string]any{"type": "string", "description": "Optional absolute path to a compatible installed Codex executable, selected from trusted host context. Defaults to codex on PATH. Does not change the configured model."}, "members": map[string]any{"type": "array", "items": member, "minItems": 1, "maxItems": 8}}, []string{"session_id", "workspace"})
	send := tool("observer_team_send", "Send initial work or a follow-up to a member of a created team. One call stores the body and queues automatic execution; workers receive messages without polling and reply with their bound observer_send_message tool. Sender and scope are bound to this team. Reuse message_id only for an identical retry. bodyPersisted, executionAccepted and completed are separate evidence.", false,
		map[string]any{"team_id": text, "message_id": map[string]any{"type": "string", "pattern": "^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$"}, "target": text, "body": map[string]any{"type": "string", "maxLength": 12000}}, []string{"team_id", "message_id", "target", "body"})
	// Team creation can resume queued work, and sending invokes a model provider.
	create["annotations"].(map[string]any)["openWorldHint"] = true
	// The source may advance to a different turn between calls.
	create["annotations"].(map[string]any)["idempotentHint"] = false
	send["annotations"].(map[string]any)["openWorldHint"] = true
	return []map[string]any{create, send,
		tool("observer_team_status", "Read scoped delivery receipts, accepted execution IDs and completion states. No message bodies or credentials are returned. Open the returned team URL to inspect actual conversation bodies.", true, map[string]any{"team_id": text}, []string{"team_id"}),
		tool("observer_team_stop", "Stop this team's owned workers and viewer. Stored bodies are preserved; interrupted execution is not retried automatically. Other Observer servers and teams are unaffected.", false, map[string]any{"team_id": text}, []string{"team_id"}),
	}
}
