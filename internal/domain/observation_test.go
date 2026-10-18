package domain

import (
	"regexp"
	"testing"
	"time"
)

func TestIdentityNamespaces(t *testing.T) {
	runs := []RunIdentity{
		{Provider: "codex", SessionID: "session", TurnID: "turn"},
		{Provider: "claude-code", SessionID: "session", TurnID: "turn"},
		{Provider: "codex", SessionID: "other", TurnID: "turn"},
		{Provider: "codex", SessionID: "session", TurnID: "other"},
		{Provider: "codex", SessionID: "a:b", TurnID: "c"},
		{Provider: "codex", SessionID: "a", TurnID: "b:c"},
	}
	keys, traces, spans, ids := map[string]bool{}, map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, run := range runs {
		for _, identity := range []struct {
			name  string
			value string
			seen  map[string]bool
		}{
			{"key", run.Key(), keys}, {"trace", run.TraceID(), traces},
			{"span", run.SpanID(), spans}, {"run", run.RunID(), ids},
		} {
			if identity.seen[identity.value] {
				t.Fatalf("different run %+v has duplicate %s %q", run, identity.name, identity.value)
			}
			identity.seen[identity.value] = true
		}
	}
}

func TestObservationIdentityIsStableAcrossUpdates(t *testing.T) {
	run := RunIdentity{Provider: "codex", SessionID: "session", TurnID: "turn"}
	first := Observation{Run: run, ID: "tool-1", Outcome: "running"}
	updated := first
	updated.Outcome = "returned"
	updated.EndedAt = time.Date(2026, 10, 16, 1, 2, 3, 0, time.UTC)
	if first.Key() != updated.Key() || first.SpanID() != updated.SpanID() || first.RunID() != updated.RunID() {
		t.Fatal("status updates changed the observation identity")
	}
	other := first
	other.ID = "tool-2"
	if first.Key() == other.Key() || first.SpanID() == other.SpanID() || first.RunID() == other.RunID() {
		t.Fatal("distinct observations share identity")
	}
	if first.TraceID() != run.TraceID() || first.TraceID() != other.TraceID() {
		t.Fatal("observations do not share their turn trace")
	}
	if first.Key() == run.Key() || first.SpanID() == run.SpanID() || first.RunID() == run.RunID() {
		t.Fatal("observation identity collides with its turn root")
	}
	if got := first.Key(); got != (Observation{Run: run, ID: "tool-1"}).Key() {
		t.Fatal("equivalent reconstructed observation changed identity")
	}
}

func TestDestinationIdentifierFormats(t *testing.T) {
	run := RunIdentity{Provider: "codex", SessionID: "session", TurnID: "turn"}
	observation := Observation{Run: run, ID: "tool-1"}
	for _, test := range []struct{ value, pattern string }{
		{run.Key(), `^[0-9a-f]{64}$`},
		{run.TraceID(), `^[0-9a-f]{32}$`},
		{run.SpanID(), `^[0-9a-f]{16}$`},
		{observation.SpanID(), `^[0-9a-f]{16}$`},
		{run.RunID(), `^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`},
		{observation.RunID(), `^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`},
	} {
		if !regexp.MustCompile(test.pattern).MatchString(test.value) {
			t.Errorf("identifier %q does not match %s", test.value, test.pattern)
		}
	}
}

// These identities survive process upgrades and are reused by destination
// receipts. Changing the derivation requires an explicit migration decision.
func TestPersistedIdentityCompatibility(t *testing.T) {
	run := RunIdentity{Provider: "codex", SessionID: "session", TurnID: "turn"}
	observation := Observation{Run: run, ID: "tool-1"}
	for _, test := range []struct{ name, got, want string }{
		{"run key", run.Key(), "812f00a85deebcf32ba90dc99574bbfb9f1b470cbf3c35aaa86af2e362b847b6"},
		{"root span", run.SpanID(), "3ed43ed2273112c7"},
		{"root UUID", run.RunID(), "ad455e09-95ca-521f-917e-2abbfc551f72"},
		{"observation key", observation.Key(), "5f6637a9bc2f974e6b63ec1469b69c29438ad354e8c956d0d841b1f0dfdb1494"},
		{"observation UUID", observation.RunID(), "362d8981-2c77-55f3-839c-734f14d7bbf9"},
	} {
		if test.got != test.want {
			t.Errorf("%s = %s, want %s", test.name, test.got, test.want)
		}
	}
}

func TestMetadataRejectsArbitraryProviderText(t *testing.T) {
	secret := "custom-secret-value"
	if got := SafeProvider(secret); got != "unknown" {
		t.Errorf("provider leaked %q", got)
	}
	if got := SafeKind(secret); got != "unknown" {
		t.Errorf("kind leaked %q", got)
	}
	if got := SafeOutcome(secret); got != "unknown" {
		t.Errorf("outcome leaked %q", got)
	}
	if got := SafeToolName(secret); got != "tool" {
		t.Errorf("tool leaked %q", got)
	}
	if SafeProvider("claude-code") != "claude-code" || SafeToolName("Bash") != "Bash" || SafeOutcome("returned") != "returned" {
		t.Fatal("known metadata identifiers were lost")
	}
}
