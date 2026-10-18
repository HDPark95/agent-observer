package httpapi

import (
	"context"
	"os"
	"testing"

	"agent-observer/internal/messaging"
	"agent-observer/internal/model"
	"agent-observer/internal/workers"
)

type teamHTTPRuntime struct{}

func (teamHTTPRuntime) Run(_ context.Context, input workers.Input) (workers.Receipt, error) {
	r := workers.Receipt{ConversationID: "thread-http", ExecutionID: "turn-http"}
	return r, input.OnAccepted(r)
}
func (teamHTTPRuntime) Close() error { return nil }

func TestTeamStartHTTPRequiresHostCredentialAndObservedSession(t *testing.T) {
	registry, store, _, admin := mailboxFixture(t)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cfg := workers.Config{Automatic: true, Owner: "agent", Executable: executable, CWD: t.TempDir(), Workers: []workers.Definition{{ID: "managed:ux", Name: "UX"}}}
	provider := workers.Provider{ID: "fixture", Name: "Fixture", ExecutionLabel: "Fixture", Factory: func(context.Context, workers.RuntimeConfig, workers.SendFunc) (workers.Runtime, error) {
		return teamHTTPRuntime{}, nil
	}}
	teams, err := workers.NewTeams(context.Background(), store, cfg, provider, registry)
	if err != nil {
		t.Fatal(err)
	}
	defer teams.Close()
	registry.SetMessageFeed(teams)
	service := messaging.NewService(store, map[string]messaging.Transport{messaging.ManagedTransport: teams})
	h := HandlerWithMessaging(registry, "8767", nil, &Mailbox{Store: store, Service: service, Teams: teams})
	req := model.Record{"sessionId": "a", "id": "start", "target": "managed:ux", "body": "actual local task"}
	scoped, err := store.Enroll(context.Background(), messaging.Principal{SessionID: "a", Turn: "turn-a", AgentID: "agent"})
	if err != nil {
		t.Fatal(err)
	}
	mailboxPost(t, h, "team/start", scoped, req, 403)
	req["turn"] = "caller-forged-turn"
	mailboxPost(t, h, "team/start", admin, req, 400)
	delete(req, "turn")
	req["sessionId"] = "unselected"
	mailboxPost(t, h, "team/start", admin, req, 400)
	req["sessionId"] = "a"
	receipt := mailboxPost(t, h, "team/start", admin, req, 200)["message"].(map[string]any)
	if receipt["turn"] != "turn-a" || receipt["source"] != "agent" {
		t.Fatalf("caller scope did not come from observed graph: %v", receipt)
	}
	state, err := registry.State("b")
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range state["nodes"].([]model.Record) {
		if node.String("bodySource") == "observer-managed" {
			t.Fatal("team body leaked to a different session")
		}
	}
}
