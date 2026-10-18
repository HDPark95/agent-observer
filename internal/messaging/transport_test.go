package messaging

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"testing"
)

func TestManagedAliasesShareRegistrationAndReceipt(t *testing.T) {
	for _, registration := range []string{ManagedTransport, LegacyManagedTransport} {
		for _, firstName := range []string{ManagedTransport, LegacyManagedTransport} {
			t.Run(registration+"/"+firstName, func(t *testing.T) {
				f := managedFixture(t)
				service := NewService(f.store, map[string]Transport{registration: f.store.Managed()})
				request := managedRequest()
				request.Transport = firstName
				first, err := service.Send(context.Background(), f.sender, request)
				if err != nil || first.Transport != firstName {
					t.Fatalf("first send = %+v, %v; want original wire label %q", first, err, firstName)
				}
				_, revision, _ := f.store.Snapshot(f.principal.SessionID, f.principal.Turn)
				for _, retryName := range []string{ManagedTransport, LegacyManagedTransport} {
					request.Transport = retryName
					again, err := service.Send(context.Background(), f.sender, request)
					if err != nil || !reflect.DeepEqual(first, again) {
						t.Fatalf("retry with %q = %+v, %v; want original receipt", retryName, again, err)
					}
				}
				messages, after, _ := f.store.Snapshot(f.principal.SessionID, f.principal.Turn)
				if len(messages) != 1 || revision != after {
					t.Fatal("alias retry enqueued another message or changed revision")
				}
				request.Body += " changed"
				if _, err := service.Send(context.Background(), f.sender, request); !errors.Is(err, ErrConflict) {
					t.Fatalf("changed body error = %v; want conflict", err)
				}
			})
		}
	}
}

func TestLegacyManagedRecordsLoadRetryAndDispatch(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(strconv.Itoa(version), func(t *testing.T) {
			f := managedFixture(t)
			ctx := context.Background()
			request := managedRequest()
			request.Transport = LegacyManagedTransport
			first, err := f.service.Send(ctx, f.sender, request)
			if err != nil {
				t.Fatal(err)
			}
			state := f.store.copyState()
			state.Version = version
			state.CapacityBytes = 0
			if version == 1 {
				state.LastSequence = 0
			}
			key := messageKey(f.principal.SessionID, f.principal.Turn, first.ID)
			stored := state.Messages[key]
			// This is the pre-alias hash algorithm, independent of the production
			// canonicalizer, so old data compatibility cannot pass by construction.
			encoded, err := json.Marshal(struct {
				Principal Principal `json:"principal"`
				Request   Request   `json:"request"`
			}{f.principal, request})
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(encoded)
			stored.Fingerprint = hex.EncodeToString(digest[:])
			if version == 1 {
				stored.Sequence = 0
			}
			state.Messages[key] = stored
			if err := f.store.Close(); err != nil {
				t.Fatal(err)
			}
			writeSequenceState(t, f.store.directory, state)
			reopened, err := Open(f.store.directory)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			got := reopened.state.Messages[key]
			if !reflect.DeepEqual(got.Message, first) || got.Fingerprint != stored.Fingerprint || !reflect.DeepEqual(reopened.state.Capabilities, state.Capabilities) || !reflect.DeepEqual(reopened.state.Revisions, state.Revisions) {
				t.Fatal("load changed message, fingerprint, capabilities or revision")
			}
			service := NewService(reopened, map[string]Transport{ManagedTransport: reopened.Managed()})
			request.Transport = ManagedTransport
			if retry, err := service.Send(ctx, f.sender, request); err != nil || !reflect.DeepEqual(retry, first) {
				t.Fatalf("new-name retry of old record = %+v, %v", retry, err)
			}
			claimAndComplete(t, reopened, recipientPrincipal(f), first.ID)
			request.ID = "new-message"
			if added, err := service.Send(ctx, f.sender, request); err != nil || added.Transport != ManagedTransport {
				t.Fatalf("new managed message = %+v, %v", added, err)
			}
			claimAndComplete(t, reopened, recipientPrincipal(f), request.ID)
			if _, ok, err := reopened.ClaimManaged(ctx, recipientPrincipal(f)); err != nil || ok {
				t.Fatalf("terminal aliases replayed: %v, %v", ok, err)
			}
		})
	}
}

func TestLegacyManagedRecoveryPreservesReceiptAndAlias(t *testing.T) {
	f := managedFixture(t)
	ctx := context.Background()
	request := managedRequest()
	request.Transport = LegacyManagedTransport
	if _, err := f.service.Send(ctx, f.sender, request); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := f.store.ClaimManaged(ctx, recipientPrincipal(f)); err != nil || !ok {
		t.Fatalf("claim = %v, %v", ok, err)
	}
	if err := f.store.UpdateDelivery(ctx, recipientPrincipal(f), request.ID, Delivery{State: "running", ThreadID: "conversation", TurnID: "execution"}); err != nil {
		t.Fatal(err)
	}
	if err := f.store.RecoverManaged(ctx, f.principal.SessionID, f.principal.Turn); err != nil {
		t.Fatal(err)
	}
	messages, _, _ := f.store.Snapshot(f.principal.SessionID, f.principal.Turn)
	if len(messages) != 1 || messages[0].Transport != LegacyManagedTransport || messages[0].Delivery.State != "unknown" || messages[0].Delivery.ThreadID != "conversation" || messages[0].Delivery.TurnID != "execution" {
		t.Fatalf("recovery lost legacy state: %+v", messages)
	}
}

func TestManagedProviderReceiptCannotChange(t *testing.T) {
	f := managedFixture(t)
	ctx := context.Background()
	request := managedRequest()
	if _, err := f.service.Send(ctx, f.sender, request); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := f.store.ClaimManaged(ctx, recipientPrincipal(f)); err != nil || !ok {
		t.Fatalf("claim = %v, %v", ok, err)
	}
	running := Delivery{Provider: "codex", State: "running", ThreadID: "conversation", TurnID: "execution"}
	invalid := running
	invalid.Provider = "not a provider"
	if err := f.store.UpdateDelivery(ctx, recipientPrincipal(f), request.ID, invalid); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid provider error = %v; want invalid", err)
	}
	if err := f.store.UpdateDelivery(ctx, recipientPrincipal(f), request.ID, running); err != nil {
		t.Fatal(err)
	}
	for _, replacement := range []string{"", "claude-code"} {
		completed := running
		completed.State, completed.Provider = "completed", replacement
		if err := f.store.UpdateDelivery(ctx, recipientPrincipal(f), request.ID, completed); !errors.Is(err, ErrInvalid) {
			t.Fatalf("provider replacement %q error = %v; want invalid", replacement, err)
		}
	}
	reopened := reopenSequenceStore(t, f.store)
	if err := reopened.RecoverManaged(ctx, f.principal.SessionID, f.principal.Turn); err != nil {
		t.Fatal(err)
	}
	messages, _, _ := reopened.Snapshot(f.principal.SessionID, f.principal.Turn)
	if len(messages) != 1 || messages[0].Delivery.Provider != "codex" || messages[0].Delivery.State != "unknown" {
		t.Fatal("reopen or recovery discarded original provider provenance")
	}
}
