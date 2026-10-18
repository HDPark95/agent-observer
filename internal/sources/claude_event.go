package sources

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"path"
	"regexp"
	"strings"
	"time"

	"agent-observer/internal/model"
)

var safeID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,160}$`)
var safeName = regexp.MustCompile(`^[\pL\pN_.:-]{1,160}$`)

// claudeEvent is the persisted allowlist. Raw hook input never crosses it.
type claudeEvent struct {
	SchemaVersion int      `json:"schemaVersion"`
	Provider      string   `json:"provider"`
	SessionID     string   `json:"sessionId"`
	EventID       string   `json:"eventId"`
	Kind          string   `json:"kind"`
	ObservedAt    string   `json:"observedAt"`
	Project       string   `json:"project"`
	PromptID      string   `json:"promptId,omitempty"`
	AgentID       string   `json:"agentId,omitempty"`
	AgentType     string   `json:"agentType,omitempty"`
	ToolID        string   `json:"toolId,omitempty"`
	ToolName      string   `json:"toolName,omitempty"`
	SkillName     string   `json:"skillName,omitempty"`
	DurationMS    *float64 `json:"durationMs,omitempty"`
}

func supportedClaudeEvent(kind string) bool {
	switch kind {
	case "SessionStart", "UserPromptSubmit", "PreToolUse", "PostToolUse", "PostToolUseFailure", "SubagentStart", "SubagentStop", "Stop", "StopFailure", "SessionEnd":
		return true
	}
	return false
}

func (e claudeEvent) validate() error {
	if e.SchemaVersion != 1 || e.Provider != ClaudeCode || !safeID.MatchString(e.SessionID) || !safeID.MatchString(e.EventID) || !supportedClaudeEvent(e.Kind) {
		return errors.New("invalid Claude Code observer metadata")
	}
	if _, err := time.Parse(time.RFC3339Nano, e.ObservedAt); err != nil {
		return errors.New("invalid observer timestamp")
	}
	for _, id := range []string{e.PromptID, e.AgentID, e.ToolID} {
		if id != "" && !safeID.MatchString(id) {
			return errors.New("invalid Claude Code identifier")
		}
	}
	for _, name := range []string{e.Project, e.AgentType, e.ToolName, e.SkillName} {
		if name != "" && !safeName.MatchString(name) {
			return errors.New("invalid Claude Code metadata name")
		}
	}
	if strings.Contains(e.Kind, "ToolUse") && (e.ToolID == "" || e.ToolName == "") {
		return errors.New("tool metadata requires identity")
	}
	if strings.HasPrefix(e.Kind, "Subagent") && e.AgentID == "" {
		return errors.New("subagent metadata requires identity")
	}
	if e.DurationMS != nil && (math.IsNaN(*e.DurationMS) || math.IsInf(*e.DurationMS, 0) || *e.DurationMS < 0) {
		return errors.New("invalid tool duration")
	}
	return nil
}

func decodeClaude(row model.Record) (claudeEvent, error) {
	data, err := json.Marshal(row)
	if err != nil {
		return claudeEvent{}, err
	}
	var event claudeEvent
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&event); err != nil {
		return event, err
	}
	return event, event.validate()
}

func readClaudeHook(in io.Reader) (*claudeEvent, error) {
	data, err := io.ReadAll(io.LimitReader(in, 1000001))
	if err != nil {
		return nil, fmt.Errorf("read Claude hook: %w", err)
	}
	if len(data) > 1000000 {
		return nil, errors.New("input for Claude hook exceeds size limit")
	}
	var raw model.Record
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, errors.New("invalid Claude hook JSON")
	}
	kind := raw.String("hook_event_name")
	if !supportedClaudeEvent(kind) {
		return nil, nil
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, fmt.Errorf("create observer event ID: %w", err)
	}
	e := &claudeEvent{SchemaVersion: 1, Provider: ClaudeCode, SessionID: raw.String("session_id"), EventID: hex.EncodeToString(id[:]), Kind: kind, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), Project: projectName(raw.String("cwd"), "ClaudeCode"), PromptID: raw.String("prompt_id"), AgentID: raw.String("agent_id"), AgentType: metadataName(raw.String("agent_type"))}
	if strings.Contains(kind, "ToolUse") {
		e.ToolID = raw.String("tool_use_id")
		e.ToolName = metadataName(raw.String("tool_name"))
		e.SkillName = claudeSkill(e.ToolName, raw.Object("tool_input"))
		if duration, ok := raw["duration_ms"].(float64); ok {
			e.DurationMS = &duration
		}
	}
	return e, e.validate()
}

func metadataName(value string) string {
	if safeName.MatchString(value) {
		return value
	}
	return ""
}

func claudeSkill(tool string, input model.Record) string {
	if tool == "Skill" {
		return metadataName(input.String("skill"))
	}
	file := strings.ReplaceAll(input.String("file_path"), `\`, "/")
	if tool == "Read" && path.Base(file) == "SKILL.md" {
		return metadataName(path.Base(path.Dir(file)))
	}
	return ""
}
