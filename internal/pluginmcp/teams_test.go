package pluginmcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"agent-observer/internal/messaging"
	"agent-observer/internal/workers"
)

type teamRuntime struct {
	id   string
	send workers.SendFunc
	runs *atomic.Int64
}

func (r *teamRuntime) Run(ctx context.Context, input workers.Input) (workers.Receipt, error) {
	r.runs.Add(1)
	receipt := workers.Receipt{ConversationID: "thread-" + r.id, ExecutionID: "turn-" + input.ID}
	if err := input.OnAccepted(receipt); err != nil {
		return receipt, err
	}
	var outbound workers.Outbound
	switch {
	case r.id == "managed:ux" && input.Source == "agent":
		outbound = workers.Outbound{ID: "ux-dx-" + input.ID, Target: "managed:dx", Body: "UX finding: " + input.Body}
	case r.id == "managed:dx":
		outbound = workers.Outbound{ID: "dx-ux-" + input.ID, Target: "managed:ux", Body: "DX confirmed: " + input.Body}
	default:
		return receipt, nil
	}
	_, err := r.send(ctx, outbound)
	return receipt, err
}
func (*teamRuntime) Close() error { return nil }

func teamFixture(t *testing.T) (*teamHost, *Viewer, *atomic.Int64) {
	t.Helper()
	h := newTeamHost(context.Background())
	base := t.TempDir()
	h.storage = func() (string, error) { return base, nil }
	h.executable = os.Executable
	runs := &atomic.Int64{}
	h.provider = workers.Provider{ID: "fixture", Name: "Fixture", ExecutionLabel: "Fixture", Factory: func(_ context.Context, cfg workers.RuntimeConfig, send workers.SendFunc) (workers.Runtime, error) {
		return &teamRuntime{id: cfg.WorkerID, send: send, runs: runs}, nil
	}}
	t.Cleanup(h.Close)
	v := testViewer(t)
	if _, err := v.Start(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	return h, v, runs
}

func connectTeamSource(t *testing.T, v *Viewer, session, turn string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "selected.jsonl")
	data := fmt.Sprintf("{\"type\":\"session_meta\",\"payload\":{\"id\":%q}}\n{\"type\":\"event_msg\",\"payload\":{\"type\":\"task_started\",\"turn_id\":%q}}\n{\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"selected task\"}}\n", session, turn)
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Connect(context.Background(), path, session, "codex"); err != nil {
		t.Fatal(err)
	}
}

func awaitTeam(t *testing.T, team *pluginTeam, count int) []messaging.Message {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		changed := team.store.Changes()
		messages, _, err := team.store.Snapshot(team.principal.SessionID, team.principal.Turn)
		if err != nil {
			t.Fatal(err)
		}
		complete := len(messages) == count
		for _, message := range messages {
			complete = complete && message.Delivery != nil && message.Delivery.State == "completed"
		}
		if complete {
			return messages
		}
		select {
		case <-changed:
		case <-deadline:
			t.Fatalf("deliveries did not complete: %+v", messages)
		}
	}
}

func TestPluginTeamDurableAutomaticConversationAndReconnect(t *testing.T) {
	h, v, runs := teamFixture(t)
	ctx := context.Background()
	connectTeamSource(t, v, "selected", "turn-one")
	input := createTeamArgs{SessionID: "selected", Workspace: t.TempDir()}
	created, err := h.Create(ctx, v, input)
	if err != nil {
		t.Fatal(err)
	}
	id := created["teamId"].(string)
	team := h.teams[id]
	if runs.Load() != 0 || created["url"] == v.viewURL("selected") {
		t.Fatal("creation ran a worker or reused an unrelated viewer")
	}
	if repeated, err := h.Create(ctx, v, input); err != nil || repeated["teamId"] != id || repeated["url"] != created["url"] {
		t.Fatalf("create not idempotent: %v %v", repeated, err)
	}
	receipt, err := h.Send(ctx, id, "request", "managed:ux", "PRIVATE_TEAM_TASK")
	if err != nil || receipt["bodyPersisted"] != true {
		t.Fatalf("send: %v %v", receipt, err)
	}
	messages := awaitTeam(t, team, 3)
	if messages[1].Source != "managed:ux" || messages[2].Source != "managed:dx" || !strings.Contains(messages[2].Body, "PRIVATE_TEAM_TASK") {
		t.Fatalf("scope-bound automatic reply missing: %+v", messages)
	}
	response, err := http.Get(team.viewer.endpoint + "/api/state?session=selected")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var state struct {
		Nodes []map[string]any `json:"nodes"`
	}
	if err := json.NewDecoder(response.Body).Decode(&state); err != nil {
		t.Fatal(err)
	}
	bodies := 0
	for _, node := range state.Nodes {
		if node["bodySource"] == "observer-managed" && node["body"] != nil {
			bodies++
		}
	}
	if bodies != 3 {
		t.Fatalf("viewer has %d managed bodies, want 3", bodies)
	}
	status, err := h.Status(id)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(status)
	if strings.Contains(string(encoded), "PRIVATE_TEAM_TASK") || strings.Contains(string(encoded), "admin.token") {
		t.Fatal("status exposed message body or credentials")
	}
	if _, err := h.Send(ctx, id, "request", "managed:ux", "different"); err == nil {
		t.Fatal("message ID conflict accepted")
	}
	if _, err := h.Send(ctx, id, "bad-target", "managed:other", "body"); err == nil {
		t.Fatal("unknown recipient accepted")
	}
	h.Stop(id)
	if v.Status(ctx)["running"] != true || v.healthy(ctx, team.viewer.endpoint) {
		t.Fatal("stop affected external viewer or left owned viewer running")
	}
	restored, err := h.Create(ctx, v, input)
	if err != nil || restored["teamId"] != id {
		t.Fatalf("reconnect: %v %v", restored, err)
	}
	if _, err := h.Send(ctx, id, "request", "managed:ux", "PRIVATE_TEAM_TASK"); err != nil {
		t.Fatal(err)
	}
	awaitTeam(t, h.teams[id], 3)
	if runs.Load() != 3 {
		t.Fatalf("completed retry executed again: %d runs", runs.Load())
	}
}

func TestPluginTeamUsesExplicitInstalledRuntime(t *testing.T) {
	h, v, _ := teamFixture(t)
	connectTeamSource(t, v, "selected", "turn")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	h.executable = func() (string, error) { t.Fatal("PATH lookup used despite explicit runtime"); return "", nil }
	created, err := h.Create(context.Background(), v, createTeamArgs{SessionID: "selected", Workspace: t.TempDir(), Executable: executable})
	if err != nil {
		t.Fatal(err)
	}
	if created["transport"] != "observer-managed" {
		t.Fatal("managed runtime not created")
	}
}

func TestPluginTeamSelectionAndScopeIsolation(t *testing.T) {
	h, v, _ := teamFixture(t)
	ctx := context.Background()
	workspace := t.TempDir()
	if _, err := h.Create(ctx, v, createTeamArgs{SessionID: "unselected", Workspace: workspace}); err == nil {
		t.Fatal("unselected session admitted")
	}
	connectTeamSource(t, v, "one", "turn-one")
	connectTeamSource(t, v, "two", "turn-two")
	for _, input := range []createTeamArgs{
		{SessionID: "one", Workspace: "relative"},
		{SessionID: "one", Workspace: workspace, Executable: "relative/codex"},
		{SessionID: "one", Workspace: workspace, Members: []workers.Definition{{ID: "agent", Name: "forged owner"}}},
		{SessionID: "one", Workspace: workspace, Members: []workers.Definition{{ID: "managed:ux", Name: "UX"}, {ID: "managed:ux", Name: "Duplicate"}}},
	} {
		if _, err := h.Create(ctx, v, input); err == nil {
			t.Fatal("invalid creation accepted")
		}
	}
	first, err := h.Create(ctx, v, createTeamArgs{SessionID: "one", Workspace: workspace})
	if err != nil {
		t.Fatal(err)
	}
	second, err := h.Create(ctx, v, createTeamArgs{SessionID: "two", Workspace: workspace})
	if err != nil {
		t.Fatal(err)
	}
	a, b := first["teamId"].(string), second["teamId"].(string)
	if a == b {
		t.Fatal("session scopes reused team identity")
	}
	if _, err := h.Send(ctx, a, "task", "managed:ux", "first scope"); err != nil {
		t.Fatal(err)
	}
	awaitTeam(t, h.teams[a], 3)
	status, err := h.Status(b)
	if err != nil || len(status["messages"].([]map[string]any)) != 0 {
		t.Fatal("messages crossed sessions")
	}
	p := protocol{viewer: v, teams: h}
	if _, err := p.invoke(ctx, "observer_team_send", json.RawMessage(`{"team_id":"x","source":"managed:dx","message_id":"x","target":"managed:ux","body":"forged"}`)); err == nil {
		t.Fatal("caller-controlled sender accepted")
	}
}
