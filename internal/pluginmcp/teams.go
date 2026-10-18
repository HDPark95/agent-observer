package pluginmcp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"

	"agent-observer/internal/adapters/codexworker"
	"agent-observer/internal/messaging"
	"agent-observer/internal/model"
	"agent-observer/internal/sessions"
	"agent-observer/internal/workers"
)

type createTeamArgs struct {
	SessionID  string               `json:"session_id"`
	Workspace  string               `json:"workspace"`
	Executable string               `json:"codex_executable"`
	Members    []workers.Definition `json:"members"`
}

// teamHost is confined to the MCP request loop. Each team owns an immutable
// execution scope, private durable store and viewer; no host credentials escape.
type teamHost struct {
	ctx        context.Context
	teams      map[string]*pluginTeam
	provider   workers.Provider
	executable func() (string, error)
	storage    func() (string, error)
}

type pluginTeam struct {
	id        string
	principal messaging.Principal
	members   []workers.Definition
	store     *messaging.Store
	manager   *workers.Manager
	viewer    *Viewer
}

func newTeamHost(ctx context.Context) *teamHost {
	return &teamHost{ctx: ctx, teams: make(map[string]*pluginTeam), provider: codexworker.Provider(),
		executable: func() (string, error) { return exec.LookPath("codex") }, storage: pluginTeamDirectory}
}

func pluginTeamDirectory() (string, error) {
	base, err := os.UserConfigDir()
	if err != nil {
		return "", errors.New("cannot locate private team storage")
	}
	path := filepath.Join(base, "agent-observer", "plugin-teams")
	if err := os.MkdirAll(path, 0700); err != nil {
		return "", errors.New("cannot create private team storage")
	}
	return path, nil
}

func defaultMembers() []workers.Definition {
	return []workers.Definition{
		{ID: "managed:ux", Name: "UX", Task: "메시지 UX 검토", Instructions: "Review the requested user experience. Communicate necessary findings to managed:dx with observer_send_message, then finish. Do not send acknowledgements that invite another reply."},
		{ID: "managed:dx", Name: "DX", Task: "실행·전달 동작 검토", Instructions: "Review the requested developer experience. Reply to managed:ux with observer_send_message when useful, then finish. Do not send acknowledgements that invite another reply."},
	}
}

func (h *teamHost) Create(ctx context.Context, viewer *Viewer, input createTeamArgs) (map[string]any, error) {
	selected, ok := viewer.selected[input.SessionID]
	if !ok || input.SessionID == "" {
		return nil, errors.New("connect the explicitly selected session before creating a team")
	}
	registry := sessions.New("")
	if _, err := registry.Register(model.Record{"source": selected.source, "provider": selected.provider, "sessionId": input.SessionID}, nil); err != nil {
		return nil, errors.New("cannot reopen the selected session")
	}
	principal, err := registry.ResolveTeam(input.SessionID, "agent")
	if err != nil {
		return nil, errors.New("selected session has no observed owner and turn yet")
	}
	cfg, err := h.configuration(input, principal)
	if err != nil {
		return nil, err
	}
	id := teamIdentity(cfg)
	if team := h.teams[id]; team != nil {
		return team.description(), nil
	}
	if len(h.teams) >= 4 {
		return nil, errors.New("four teams are already open; stop a team before creating another")
	}
	team, err := h.open(ctx, id, cfg, principal, registry)
	if err != nil {
		return nil, err
	}
	h.teams[id] = team
	return team.description(), nil
}

func (h *teamHost) configuration(input createTeamArgs, principal messaging.Principal) (workers.Config, error) {
	if !filepath.IsAbs(input.Workspace) {
		return workers.Config{}, errors.New("select an absolute workspace directory for read-only workers")
	}
	executable := input.Executable
	if executable == "" {
		var err error
		executable, err = h.executable()
		if err != nil {
			return workers.Config{}, errors.New("install and sign in to Codex CLI before starting managed workers")
		}
	}
	if !filepath.IsAbs(executable) {
		return workers.Config{}, errors.New("select an absolute Codex executable path")
	}
	members := input.Members
	if len(members) == 0 {
		members = defaultMembers()
	}
	return workers.Config{Provider: h.provider.ID, SessionID: principal.SessionID, Turn: principal.Turn, Owner: principal.AgentID,
		Executable: executable, CWD: filepath.Clean(input.Workspace), Workers: append([]workers.Definition(nil), members...)}, nil
}

func teamIdentity(cfg workers.Config) string {
	encoded, _ := json.Marshal(cfg)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func (h *teamHost) open(ctx context.Context, id string, cfg workers.Config, principal messaging.Principal, registry *sessions.Registry) (_ *pluginTeam, err error) {
	base, err := h.storage()
	if err != nil {
		return nil, err
	}
	store, err := messaging.Open(filepath.Join(base, id))
	if err != nil {
		return nil, errors.New("cannot open private team storage; another MCP connection may own this team")
	}
	team := &pluginTeam{id: id, principal: principal, members: cfg.Workers, store: store, viewer: NewViewer()}
	defer func() {
		if err != nil {
			team.close()
		}
	}()
	if err = store.EnsureParticipant(ctx, principal); err != nil {
		return nil, errors.New("cannot persist team owner")
	}
	team.manager, err = workers.New(h.ctx, store, cfg, h.provider)
	if err != nil {
		return nil, errors.New("cannot create team; check workspace and 1 to 8 unique managed worker definitions")
	}
	registry.SetMessageFeed(team.manager)
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, errors.New("cannot open the team viewer")
	}
	if _, err = team.viewer.serveRegistry(ctx, listener, registry); err != nil {
		return nil, err
	}
	return team, nil
}

func (t *pluginTeam) description() map[string]any {
	members := make([]map[string]string, 0, len(t.members))
	for _, member := range t.members {
		members = append(members, map[string]string{"id": member.ID, "name": member.Name, "task": member.Task})
	}
	return map[string]any{"teamId": t.id, "sessionId": t.principal.SessionID, "turn": t.principal.Turn,
		"members": members, "url": t.viewer.viewURL(t.principal.SessionID), "transport": "observer-managed", "readOnlyWorkers": true}
}

func (h *teamHost) Send(ctx context.Context, teamID, id, target, body string) (map[string]any, error) {
	team := h.teams[teamID]
	if team == nil {
		return nil, errors.New("unknown team; create or reconnect the selected team first")
	}
	message, err := team.manager.Send(ctx, team.principal, messaging.Request{ID: id, Target: target, Body: body, Transport: messaging.ManagedTransport})
	if err != nil {
		return nil, teamSendError(err)
	}
	result := messageReceipt(message)
	result["teamId"], result["url"] = teamID, team.viewer.viewURL(team.principal.SessionID)
	return result, nil
}

func teamSendError(err error) error {
	switch {
	case errors.Is(err, messaging.ErrConflict):
		return errors.New("message ID conflicts with stored content; use a new ID only for a new task")
	case errors.Is(err, messaging.ErrOutcomeUnknown):
		return errors.New("storage outcome is unconfirmed; retry only the identical team, ID, target and body")
	case errors.Is(err, messaging.ErrInvalid), errors.Is(err, messaging.ErrForbidden):
		return errors.New("invalid message or recipient outside the selected team")
	case errors.Is(err, messaging.ErrRecipientUnavailable), errors.Is(err, messaging.ErrClosed):
		return errors.New("recipient unavailable; inspect team status before sending more work")
	case errors.Is(err, messaging.ErrCapacity):
		return errors.New("team message capacity reached")
	default:
		return errors.New("message storage failed; inspect status and retain the identical request for recovery")
	}
}

func messageReceipt(message messaging.Message) map[string]any {
	delivery := message.Delivery
	return map[string]any{"messageId": message.ID, "source": message.Source, "target": message.Target,
		"bodyPersisted": true, "bodySource": "observer-managed", "state": delivery.State,
		"executionAccepted": delivery.ThreadID != "" && delivery.TurnID != "", "completed": delivery.State == "completed",
		"provider": delivery.Provider, "conversationId": delivery.ThreadID, "executionId": delivery.TurnID, "error": delivery.Error,
		"errorAdvice": deliveryAdvice(delivery.Error)}
}

func deliveryAdvice(code string) string {
	if code == "runtime-upgrade-required" {
		return "The configured model requires a newer Codex runtime. Update Codex CLI or select a compatible installed codex_executable when creating a team. The message was retained and was not replayed."
	}
	return ""
}

func (h *teamHost) Status(teamID string) (map[string]any, error) {
	team := h.teams[teamID]
	if team == nil {
		return nil, errors.New("unknown team; create or reconnect the selected team first")
	}
	messages, _, err := team.store.Snapshot(team.principal.SessionID, team.principal.Turn)
	if err != nil {
		return nil, errors.New("cannot read team delivery status")
	}
	receipts := make([]map[string]any, 0, len(messages))
	for _, message := range messages {
		if messaging.IsManagedTransport(message.Transport) && message.Delivery != nil {
			receipts = append(receipts, messageReceipt(message))
		}
	}
	result := team.description()
	result["messages"] = receipts
	return result, nil
}

func (h *teamHost) Stop(teamID string) map[string]any {
	if team := h.teams[teamID]; team != nil {
		team.close()
		delete(h.teams, teamID)
	}
	return map[string]any{"teamId": teamID, "stopped": true, "storedMessagesPreserved": true}
}

func (t *pluginTeam) close() {
	if t.manager != nil {
		t.manager.Close()
	}
	_ = t.viewer.Close()
	_ = t.store.Close()
}

func (h *teamHost) Close() {
	for id := range h.teams {
		h.Stop(id)
	}
}
