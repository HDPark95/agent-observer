package messages

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"agent-observer/internal/model"
)

func TestCapturedBodySurvivesRestart(t *testing.T) {
	store, identity := testStore(t)
	source := messageSource()
	first := persistentCapture(t, source, store, identity)
	if _, err := first.Publish(hookBody("restart retained"), true); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(store.directory)
	if err != nil {
		t.Fatal(err)
	}
	restarted := persistentCapture(t, source, reopened, identity)
	if got := restarted.Read()["nodes"].([]model.Record)[0].String("body"); got != "restart retained" {
		t.Fatalf("body after restart = %q, want captured body", got)
	}
	before := restarted.Read().String("revision")
	if _, err := restarted.Publish(hookBody("restart retained"), true); err != nil {
		t.Fatal(err)
	}
	if restarted.Read().String("revision") != before {
		t.Fatal("duplicate publish changed body revision")
	}
	if _, err := restarted.Publish(hookBody("conflicting replacement"), true); err == nil {
		t.Fatal("conflicting body accepted")
	}
	if got := New(source).Read()["nodes"].([]model.Record)[0].String("body"); got != "" {
		t.Fatal("default memory-only capture restored local data")
	}
}

func TestPendingBodySurvivesRestartWithoutExtendingExpiry(t *testing.T) {
	store, identity := testStore(t)
	source := &fake{turn: "turn"}
	first := persistentCapture(t, source, store, identity)
	if result, err := first.Publish(hookBody("pending body"), true); err != nil || result.String("status") != "pending" {
		t.Fatalf("pending publish = %v, %v", result, err)
	}
	restarted := persistentCapture(t, source, store, identity)
	restarted.Read()
	if !first.pending["call"].at.Equal(restarted.pending["call"].at) {
		t.Fatal("restart extended pending lifetime")
	}
	source.nodes = messageSource().nodes
	if got := restarted.Read()["nodes"].([]model.Record)[0].String("body"); got != "pending body" {
		t.Fatalf("late observed call body = %q", got)
	}
	if got := persistentCapture(t, source, store, identity).Read()["nodes"].([]model.Record)[0].String("body"); got != "pending body" {
		t.Fatalf("attached pending body after second restart = %q", got)
	}
}

func TestStoredBodiesRequireExactSourceTurnAndEvent(t *testing.T) {
	for _, field := range []string{"provider", "source", "session", "turn", "id", "sender", "recipient", "at"} {
		t.Run(field, func(t *testing.T) {
			store, identity := testStore(t)
			source := messageSource()
			first := persistentCapture(t, source, store, identity)
			if _, err := first.Publish(hookBody("private to one event"), true); err != nil {
				t.Fatal(err)
			}
			switch field {
			case "provider":
				identity.Provider = "claude-code"
			case "source":
				identity.Source = "/different/selected.jsonl"
			case "session":
				identity.Session = "other-thread"
			case "turn":
				source.turn = "next-turn"
			default:
				source.nodes[0][field] = "different"
			}
			if got := persistentCapture(t, source, store, identity).Read()["nodes"].([]model.Record)[0].String("body"); got != "" {
				t.Fatalf("body crossed changed %s: %q", field, got)
			}
		})
	}
}

func TestStorePersistsRedactedTextWithPrivatePermissions(t *testing.T) {
	store, identity := testStore(t)
	capture := persistentCapture(t, messageSource(), store, identity)
	if _, err := capture.Publish(hookBody("token=private-test-value"), true); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(store.filename(identity))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "private-test-value") {
		t.Fatal("unredacted value was saved")
	}
	for path, mode := range map[string]os.FileMode{store.directory: 0700, store.filename(identity): 0600} {
		info, err := os.Lstat(path)
		if err != nil || info.Mode().Perm() != mode {
			t.Fatalf("permission for %s = %v, %v; want %v", path, info, err, mode)
		}
	}
}

func TestRestoredBodyWaitsForTheSameObservedEvent(t *testing.T) {
	store, identity := testStore(t)
	source := messageSource()
	capture := persistentCapture(t, source, store, identity)
	if _, err := capture.Publish(hookBody("known event"), true); err != nil {
		t.Fatal(err)
	}
	nodes := source.nodes
	source.nodes = nil
	restarted := persistentCapture(t, source, store, identity)
	if result, err := restarted.Publish(hookBody("known event"), true); err != nil || result.String("status") != "pending" {
		t.Fatalf("unobserved restored event acknowledged as attached: %v, %v", result, err)
	}
	source.nodes = nodes
	if result, err := restarted.Publish(hookBody("known event"), true); err != nil || result.String("status") != "attached" {
		t.Fatalf("exact restored event did not attach: %v, %v", result, err)
	}
	source.nodes[0]["at"] = "2026-10-16T00:00:01Z"
	if _, err := restarted.Publish(hookBody("known event"), true); err == nil {
		t.Fatal("same call ID with changed event identity acknowledged")
	}
}

func TestStoreRejectsTamperedIdentityAndDoesNotLoadUntilSelected(t *testing.T) {
	store, identity := testStore(t)
	if err := os.WriteFile(store.filename(identity), []byte(`{"version":1,"identity":{"provider":"other"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenStore(store.directory); err != nil {
		t.Fatalf("opening store read unselected bodies: %v", err)
	}
	if _, err := NewWithStore(messageSource(), store, identity); err == nil {
		t.Fatal("mismatched file identity accepted")
	}
}

func TestStoreRejectsSymlinksAndUnsafePermissions(t *testing.T) {
	store, identity := testStore(t)
	link := filepath.Join(t.TempDir(), "linked")
	if err := os.Symlink(store.directory, link); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenStore(link); err == nil {
		t.Fatal("symlink directory accepted")
	}
	outside := filepath.Join(t.TempDir(), "outside.json")
	if err := os.WriteFile(outside, []byte("untouched"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, store.filename(identity)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.load(identity); err == nil {
		t.Fatal("symlink file read")
	}
	if _, err := store.save(storedState{Version: 1, Identity: identity}); err == nil {
		t.Fatal("symlink destination replaced")
	}
	if raw, err := os.ReadFile(outside); err != nil || string(raw) != "untouched" {
		t.Fatalf("symlink target changed: %q, %v", raw, err)
	}
	if err := os.Chmod(store.directory, 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenStore(store.directory); err == nil {
		t.Fatal("non-private directory accepted")
	}
	if _, err := OpenStore("relative/path"); err == nil {
		t.Fatal("relative path accepted")
	}
}

func TestStoreWriteFailureDoesNotAcknowledgeOrAttachBody(t *testing.T) {
	store, identity := testStore(t)
	capture := persistentCapture(t, messageSource(), store, identity)
	capture.Read()
	if err := os.Rename(store.directory, store.directory+"-moved"); err != nil {
		t.Fatal(err)
	}
	if _, err := capture.Publish(hookBody("must not acknowledge"), true); err == nil {
		t.Fatal("failed durable write was acknowledged")
	}
	state := capture.Read()
	if state.String("messageStoreError") == "" || state["nodes"].([]model.Record)[0].String("body") != "" {
		t.Fatal("failed durable write attached a body or hid the failure")
	}
}

func TestExpiredPendingAndPreviousTurnAreNotRestored(t *testing.T) {
	store, identity := testStore(t)
	for _, turn := range []string{"turn", "previous-turn"} {
		state := storedState{Version: 1, Identity: identity, Turn: turn,
			Pending: map[string]storedPending{"call": {Body: "expired", Target: "dx", At: time.Now().Add(-121 * time.Second)}}}
		if _, err := store.save(state); err != nil {
			t.Fatal(err)
		}
		capture := persistentCapture(t, messageSource(), store, identity)
		if got := capture.Read()["nodes"].([]model.Record)[0].String("body"); got != "" {
			t.Fatal("expired or old-turn pending body restored")
		}
	}
}

func testStore(t *testing.T) (*Store, Identity) {
	t.Helper()
	parent, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store, err := OpenStore(filepath.Join(parent, "bodies"))
	if err != nil {
		t.Fatal(err)
	}
	return store, Identity{Provider: "codex", Source: "/selected/root.jsonl", Session: "root-thread"}
}

func persistentCapture(t *testing.T, source Source, store *Store, identity Identity) *Capture {
	t.Helper()
	capture, err := NewWithStore(source, store, identity)
	if err != nil {
		t.Fatal(err)
	}
	return capture
}

func messageSource() *fake {
	return &fake{turn: "turn", nodes: []model.Record{{"id": "a2a:/root/ux:call", "kind": "a2a", "sender": "/root/ux", "recipient": "/root/dx", "at": "2026-10-16T00:00:00Z"}}}
}

func hookBody(body string) model.Record {
	return model.Record{"sessionId": "root-thread", "callId": "call", "target": "dx", "body": body}
}
