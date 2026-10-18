package workers

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"agent-observer/internal/messaging"
	"agent-observer/internal/model"
)

func TestRuntimeCompatibilityFailureRetainsReceiptWithoutReplay(t *testing.T) {
	f := newManagerFixture(t)
	sendRequest(t, f, "incompatible", "managed:ux", "review")
	run := awaitRun(t, f.runtimes["managed:ux"])
	run.complete <- ErrRuntimeUpgradeRequired
	message := awaitDelivery(t, f, "incompatible", "unknown")
	if message.Delivery.Error != "runtime-upgrade-required" || message.Delivery.ThreadID == "" {
		t.Fatalf("compatibility evidence lost: %+v", message.Delivery)
	}
	sendRequest(t, f, "incompatible", "managed:ux", "review")
	f.manager.Close()
	if f.runtimes["managed:ux"].starts.Load() != 1 {
		t.Fatal("failed request was replayed")
	}
}

func TestProviderContractWakesPeersWithTypedReceipts(t *testing.T) {
	for _, identity := range []Provider{
		{ID: "first-runtime", Name: "First Runtime", ExecutionLabel: "첫 제공자 실행"},
		{ID: "second-runtime", Name: "Second Runtime", ExecutionLabel: "다른 제공자 실행"},
	} {
		t.Run(identity.ID, func(t *testing.T) {
			f := newManagerFixtureWithProvider(t, nil, func(factory Factory) Provider {
				identity.Factory = factory
				return identity
			})
			if len(f.created) != 0 {
				t.Fatal("provider started without a message")
			}
			sendRequest(t, f, "request", "managed:ux", "review")
			uxRun := awaitRun(t, f.runtimes["managed:ux"])
			outbound := Outbound{ID: "peer-request", Target: "managed:dx", Body: "peer input"}
			receipt, err := f.runtimes["managed:ux"].send(context.Background(), outbound)
			if err != nil || receipt.ID != outbound.ID || receipt.Target != outbound.Target || receipt.State != "queued" {
				t.Fatalf("typed receipt = %+v, %v", receipt, err)
			}
			dxRun := awaitRun(t, f.runtimes["managed:dx"])
			if dxRun.message.Source != "managed:ux" || dxRun.message.Body != outbound.Body {
				t.Fatalf("bound peer input = %+v", dxRun.message)
			}
			dxRun.complete <- nil
			completed := awaitDelivery(t, f, outbound.ID, "completed")
			if completed.Delivery.Provider != identity.ID || completed.Delivery.ThreadID != "conversation-dx" || completed.Delivery.TurnID != "turn-peer-request" {
				t.Fatalf("neutral receipt lost provider provenance or opaque IDs: %+v", completed.Delivery)
			}
			retried, err := f.runtimes["managed:ux"].send(context.Background(), outbound)
			if err != nil || retried.State != "completed" || f.runtimes["managed:dx"].starts.Load() != 1 {
				t.Fatalf("retry = %+v, %v; starts=%d", retried, err, f.runtimes["managed:dx"].starts.Load())
			}
			outbound.Body = "changed"
			if _, err := f.runtimes["managed:ux"].send(context.Background(), outbound); !errors.Is(err, messaging.ErrConflict) {
				t.Fatalf("same-ID changed body = %v; want conflict", err)
			}
			outbound.ID, outbound.Target = "outside-scope", "managed:outside"
			if _, err := f.runtimes["managed:ux"].send(context.Background(), outbound); !errors.Is(err, messaging.ErrForbidden) {
				t.Fatalf("unregistered recipient = %v; want forbidden", err)
			}
			uxRun.complete <- errors.New("provider outcome unknown")
			failed := awaitDelivery(t, f, "request", "unknown")
			if failed.Delivery.Provider != identity.ID || failed.Delivery.ThreadID != "conversation-ux" || failed.Delivery.TurnID != "turn-request" {
				t.Fatalf("uncertain receipt lost provenance or observed IDs: %+v", failed.Delivery)
			}
			sendRequest(t, f, "request", "managed:ux", "review")
			f.manager.Close()
			if f.runtimes["managed:ux"].starts.Load() != 1 {
				t.Fatal("uncertain execution was silently replayed")
			}
		})
	}
}

func TestRuntimeConfigAndProviderRemainDetachedFromCaller(t *testing.T) {
	configs := make(chan RuntimeConfig, 2)
	provider := Provider{ID: "second-runtime", Name: "Second Runtime", ExecutionLabel: "다른 제공자 실행"}
	f := newManagerFixtureWithProvider(t, nil, func(factory Factory) Provider {
		provider.Factory = func(ctx context.Context, cfg RuntimeConfig, send SendFunc) (Runtime, error) {
			configs <- cfg
			return factory(ctx, cfg, send)
		}
		return provider
	})
	provider.ID, provider.Name, provider.ExecutionLabel, provider.Factory = "changed", "Changed", "changed", nil
	f.cfg.Workers[0].Name = "Changed"
	f.cfg.Workers[0].Instructions = "Changed"
	sendRequest(t, f, "first", "managed:ux", "review")
	uxRun := awaitRun(t, f.runtimes["managed:ux"])
	first := <-configs
	if first.WorkerID != "managed:ux" || first.Instructions != "" || first.Executable != f.cfg.Executable || first.CWD != f.cfg.CWD {
		t.Fatalf("runtime configuration was changed or core added provider instructions: %+v", first)
	}
	wantPeers := []Peer{{ID: "managed:ux", Name: "UX"}, {ID: "managed:dx", Name: "DX"}}
	if !reflect.DeepEqual(first.Peers, wantPeers) {
		t.Fatalf("peers = %+v; want %+v", first.Peers, wantPeers)
	}
	first.Peers[0].Name = "Runtime changed its peers"
	sendRequest(t, f, "second", "managed:dx", "review")
	dxRun := awaitRun(t, f.runtimes["managed:dx"])
	second := <-configs
	if !reflect.DeepEqual(second.Peers, wantPeers) {
		t.Fatalf("one runtime mutated another runtime's peers: %+v", second.Peers)
	}
	nodes, _ := f.manager.Read(f.cfg.SessionID, f.cfg.Turn)
	for _, node := range nodes {
		if node.String("id") == "managed:ux" && (node.String("label") != "UX" || node.String("provider") != "second-runtime") {
			t.Fatalf("caller mutation changed provider or worker projection: %+v", node)
		}
	}
	uxRun.complete <- nil
	dxRun.complete <- nil
	awaitDelivery(t, f, "first", "completed")
	awaitDelivery(t, f, "second", "completed")
}

func TestManagerRejectsMissingOrMismatchedProvider(t *testing.T) {
	f := newManagerFixture(t)
	f.manager.Close()
	_, _, factory := newTestFactory()
	for _, provider := range []Provider{
		{},
		{ID: "runtime", Name: "Runtime", ExecutionLabel: "실행"},
		{ID: "runtime", Name: "Runtime", Factory: factory},
		{ID: "runtime", Name: " ", ExecutionLabel: "실행", Factory: factory},
		{ID: "runtime", Name: "Runtime", ExecutionLabel: " ", Factory: factory},
		{ID: "Invalid Runtime", Name: "Runtime", ExecutionLabel: "실행", Factory: factory},
		{ID: "runtime", ExecutionLabel: "실행", Factory: factory},
		{Name: "Runtime", ExecutionLabel: "실행", Factory: factory},
	} {
		if _, err := New(context.Background(), f.store, f.cfg, provider); !errors.Is(err, messaging.ErrInvalid) {
			t.Fatalf("invalid provider %+v error = %v", provider, err)
		}
	}
	f.cfg.Provider = "other-runtime"
	if _, err := New(context.Background(), f.store, f.cfg, testProvider(factory)); !errors.Is(err, messaging.ErrInvalid) {
		t.Fatalf("mismatched configured provider error = %v", err)
	}
	f.cfg.Provider = "test-runtime"
	manager, err := New(context.Background(), f.store, f.cfg, testProvider(factory))
	if err != nil {
		t.Fatal(err)
	}
	manager.Close()
}

func TestExecutionProjectionDoesNotRelabelHistoricalProvider(t *testing.T) {
	provider := Provider{ID: "second-runtime", Name: "Second Runtime", ExecutionLabel: "다른 제공자 실행"}
	for _, tt := range []struct {
		storedProvider, wantLabel string
	}{
		{"", "작업자 실행"},
		{"first-runtime", "작업자 실행"},
		{"second-runtime", "다른 제공자 실행"},
	} {
		message := model.Record{"id": "message", "runtimeProvider": tt.storedProvider}
		node := executionNode(provider, Definition{ID: "managed:ux", Task: "UX 검토"}, message)
		if node.String("toolName") != tt.wantLabel || node.String("runtimeProvider") != tt.storedProvider {
			t.Fatalf("stored provider %q projected as %v", tt.storedProvider, node)
		}
	}
}
