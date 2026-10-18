package workers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"agent-observer/internal/messaging"
)

type scopeFixture struct {
	mu    sync.Mutex
	turns map[string]string
}

func (s *scopeFixture) ResolveTeam(session, owner string) (messaging.Principal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	turn := s.turns[session]
	if turn == "" {
		return messaging.Principal{}, messaging.ErrInvalid
	}
	return messaging.Principal{SessionID: session, Turn: turn, AgentID: owner}, nil
}

func (s *scopeFixture) set(session, turn string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.turns[session] = turn
}

type automaticRuntime struct {
	serial int64
	send   SendFunc
}

func (r *automaticRuntime) Run(ctx context.Context, input Input) (Receipt, error) {
	receipt := Receipt{ConversationID: fmt.Sprintf("thread-%d", r.serial), ExecutionID: "turn-" + input.ID}
	if err := input.OnAccepted(receipt); err != nil {
		return receipt, err
	}
	// A real send callback is exercised inside the normal runtime fixture. It
	// remains bound to its creating scope when the resolver advances a turn.
	if input.Source == "agent" {
		_, err := r.send(ctx, Outbound{ID: "reply-" + input.ID, Target: "managed:dx", Body: "reply: " + input.Body})
		return receipt, err
	}
	return receipt, nil
}

func (*automaticRuntime) Close() error { return nil }

func automaticFixture(t *testing.T) (*Teams, *messaging.Store, *scopeFixture, Config, Provider, *atomic.Int64) {
	t.Helper()
	store, err := messaging.Open(filepath.Join(t.TempDir(), "mailbox"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Automatic: true, Owner: "agent", Executable: executable, CWD: t.TempDir(), Workers: []Definition{{ID: "managed:ux", Name: "UX"}, {ID: "managed:dx", Name: "DX"}}}
	resolver := &scopeFixture{turns: map[string]string{"one": "turn-one", "two": "turn-two"}}
	starts := &atomic.Int64{}
	provider := testProvider(func(_ context.Context, _ RuntimeConfig, send SendFunc) (Runtime, error) {
		return &automaticRuntime{serial: starts.Add(1), send: send}, nil
	})
	teams, err := NewTeams(context.Background(), store, cfg, provider, resolver)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(teams.Close)
	return teams, store, resolver, cfg, provider, starts
}

func completedTeam(t *testing.T, store *messaging.Store, session, turn string, count int) []messaging.Message {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		changed := store.Changes()
		messages, _, _ := store.Snapshot(session, turn)
		complete := len(messages) == count
		for _, m := range messages {
			complete = complete && m.Delivery != nil && m.Delivery.State == "completed"
		}
		if complete {
			return messages
		}
		select {
		case <-changed:
		case <-deadline:
			t.Fatalf("team did not complete in %s/%s: %+v", session, turn, messages)
		}
	}
}

func TestAutomaticTeamsResolveTurnsAndKeepCallbacksIsolated(t *testing.T) {
	teams, store, resolver, _, _, starts := automaticFixture(t)
	ctx := context.Background()
	first := StartRequest{SessionID: "one", ID: "first", Target: "managed:ux", Body: "first task"}
	if _, err := teams.Start(ctx, first); err != nil {
		t.Fatal(err)
	}
	completedTeam(t, store, "one", "turn-one", 2)
	resolver.set("one", "next-turn")
	for _, req := range []StartRequest{{SessionID: "one", ID: "second", Target: "managed:ux", Body: "next task"}, {SessionID: "two", ID: "first", Target: "managed:ux", Body: "other session"}} {
		if _, err := teams.Start(ctx, req); err != nil {
			t.Fatal(err)
		}
	}
	completedTeam(t, store, "one", "next-turn", 2)
	completedTeam(t, store, "two", "turn-two", 2)
	retry, err := teams.Start(ctx, first)
	if err != nil || retry.Turn != "turn-one" {
		t.Fatalf("retry moved into a new turn: %+v %v", retry, err)
	}
	if starts.Load() != 6 {
		t.Fatalf("expected six isolated worker conversations, got %d", starts.Load())
	}
	for _, scope := range []teamScope{{"one", "turn-one"}, {"one", "next-turn"}, {"two", "turn-two"}} {
		nodes, _ := teams.Read(scope.session, scope.turn)
		bodies := 0
		for _, node := range nodes {
			if node.String("bodySource") == "observer-managed" {
				bodies++
			}
		}
		if bodies != 2 {
			t.Fatalf("scope %v has %d message bodies", scope, bodies)
		}
	}
}

func TestAutomaticStartConcurrentRetryConflictAndRestart(t *testing.T) {
	teams, store, resolver, cfg, provider, starts := automaticFixture(t)
	req := StartRequest{SessionID: "one", ID: "same", Target: "managed:ux", Body: "one task"}
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := teams.Start(context.Background(), req); err != nil {
				t.Errorf("start: %v", err)
			}
		}()
	}
	wg.Wait()
	completedTeam(t, store, "one", "turn-one", 2)
	teams.Close()
	resolver.set("one", "next-turn")
	reopened, err := NewTeams(context.Background(), store, cfg, provider, resolver)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	receipt, err := reopened.Start(context.Background(), req)
	if err != nil || receipt.Turn != "turn-one" || starts.Load() != 2 {
		t.Fatalf("restart repeated or moved task: %+v %v starts=%d", receipt, err, starts.Load())
	}
	nodes, _ := reopened.Read("one", "turn-one")
	groups := 0
	for _, node := range nodes {
		if node.String("kind") == "subagent" {
			groups++
			if node.String("status") != "저장된 기록 · 실행기 미연결" {
				t.Fatal("archived group claims a live runtime")
			}
		}
	}
	if groups != 2 || starts.Load() != 2 {
		t.Fatalf("restart lost groups or launched runtimes: groups=%d starts=%d", groups, starts.Load())
	}
	req.Body = "changed task"
	if _, err := reopened.Start(context.Background(), req); !errors.Is(err, messaging.ErrConflict) {
		t.Fatalf("changed retry: %v", err)
	}
}

func TestAutomaticStartRejectsInvalidScopeAndCancellation(t *testing.T) {
	teams, _, _, _, _, starts := automaticFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	valid := StartRequest{SessionID: "one", ID: "task", Target: "managed:ux", Body: "task"}
	if _, err := teams.Start(ctx, valid); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled request: %v", err)
	}
	for _, req := range []StartRequest{
		{SessionID: "absent", ID: "task", Target: "managed:ux", Body: "task"},
		{SessionID: "one", ID: "task", Target: "managed:absent", Body: "task"},
		{SessionID: "one", ID: "task", Target: "managed:ux", Body: ""},
	} {
		if _, err := teams.Start(context.Background(), req); err == nil {
			t.Fatal("invalid request accepted")
		}
	}
	if len(teams.managers) != 0 || starts.Load() != 0 {
		t.Fatal("invalid start created a team")
	}
}

func TestAutomaticTeamsRecoverPendingScopesWithoutReadingTranscripts(t *testing.T) {
	teams, store, resolver, cfg, provider, _ := automaticFixture(t)
	ctx := context.Background()
	if _, err := teams.Start(ctx, StartRequest{SessionID: "one", ID: "seed", Target: "managed:ux", Body: "seed"}); err != nil {
		t.Fatal(err)
	}
	completedTeam(t, store, "one", "turn-one", 2)
	teams.Close()
	owner := messaging.Principal{SessionID: "one", Turn: "turn-one", AgentID: "agent"}
	for _, id := range []string{"crashed", "queued"} {
		if _, err := store.Managed().Send(ctx, owner, messaging.Request{ID: id, Target: "managed:ux", Body: id, Transport: messaging.ManagedTransport}); err != nil {
			t.Fatal(err)
		}
	}
	recipient := owner
	recipient.AgentID = "managed:ux"
	if _, found, err := store.ClaimManaged(ctx, recipient); !found || err != nil {
		t.Fatalf("claim: found=%v err=%v", found, err)
	}
	// Recovery is of an already authorized durable queue; no selected transcript
	// exists in the resolver after restart.
	resolver.set("one", "")
	reopened, err := NewTeams(ctx, store, cfg, provider, resolver)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	deadline := time.After(5 * time.Second)
	for {
		changed := store.Changes()
		messages, _, _ := store.Snapshot("one", "turn-one")
		complete := 0
		for _, m := range messages {
			if m.ID == "crashed" && m.Delivery.State != "unknown" {
				t.Fatalf("uncertain delivery was retried: %+v", m.Delivery)
			}
			if m.Delivery.State == "completed" {
				complete++
			}
		}
		if len(messages) == 5 && complete == 4 {
			return
		}
		select {
		case <-changed:
		case <-deadline:
			t.Fatalf("queued work did not recover: %+v", messages)
		}
	}
}

func TestAutomaticTemplateRejectsPinnedScope(t *testing.T) {
	_, _, _, cfg, _, _ := automaticFixture(t)
	file := filepath.Join(t.TempDir(), "team.json")
	write := func() {
		t.Helper()
		data, err := json.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write()
	if _, err := LoadConfig(file); err != nil {
		t.Fatal(err)
	}
	cfg.SessionID, cfg.Turn = "old-session", "old-turn"
	write()
	if _, err := LoadConfig(file); err == nil {
		t.Fatal("automatic template silently accepted a stale pinned scope")
	}
}

func TestAutomaticRecoveryRejectsRemovedPendingRecipient(t *testing.T) {
	teams, store, resolver, cfg, provider, starts := automaticFixture(t)
	teams.Close()
	owner := messaging.Principal{SessionID: "one", Turn: "turn-one", AgentID: "agent"}
	recipient := owner
	recipient.AgentID = "managed:removed"
	for _, p := range []messaging.Principal{owner, recipient} {
		if err := store.EnsureParticipant(context.Background(), p); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Managed().Send(context.Background(), owner, messaging.Request{ID: "pending", Target: recipient.AgentID, Body: "pending task", Transport: messaging.ManagedTransport}); err != nil {
		t.Fatal(err)
	}
	if recovered, err := NewTeams(context.Background(), store, cfg, provider, resolver); !errors.Is(err, messaging.ErrRecipientUnavailable) {
		if recovered != nil {
			recovered.Close()
		}
		t.Fatalf("removed queued recipient was silently ignored: %v", err)
	}
	messages, _, _ := store.Snapshot(owner.SessionID, owner.Turn)
	if len(messages) != 1 || messages[0].Delivery.State != "queued" || starts.Load() != 0 {
		t.Fatal("failed recovery changed queue or launched a worker")
	}
}
