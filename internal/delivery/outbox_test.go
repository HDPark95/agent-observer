package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"agent-observer/internal/domain"
)

type exportFunc func(context.Context, []domain.Observation) error

func (f exportFunc) Export(ctx context.Context, observations []domain.Observation) error {
	return f(ctx, observations)
}

func (f exportFunc) Validate(observation domain.Observation) error {
	if observation.ObservedAt.IsZero() || observation.TurnStartedAt.IsZero() {
		return errors.New("timestamp unavailable")
	}
	return nil
}

func testObservation(id string) domain.Observation {
	at := time.Date(2026, 10, 16, 1, 2, 3, 0, time.UTC)
	return domain.Observation{
		ID: id, Run: domain.RunIdentity{Provider: "codex", SessionID: "session", TurnID: "turn"},
		Kind: "tool", ToolName: "go", Outcome: "completed", ObservedAt: at, TurnStartedAt: at.Add(-time.Minute),
	}
}

func newTestOutbox(t *testing.T, destinations ...Destination) *Outbox {
	t.Helper()
	box, err := New(filepath.Join(t.TempDir(), "outbox"), destinations)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = box.Close() })
	return box
}

func submit(t *testing.T, box *Outbox, observations ...domain.Observation) {
	t.Helper()
	if err := box.Submit(observations); err != nil {
		t.Fatal(err)
	}
}

func TestOutboxRestartDeduplicatesAcceptedAndRetainsPending(t *testing.T) {
	calls := 0
	exporter := exportFunc(func(_ context.Context, observations []domain.Observation) error {
		calls += len(observations)
		return nil
	})
	destination := Destination{ID: "project-a", Exporter: exporter}
	box := newTestOutbox(t, destination)
	accepted, pending := testObservation("accepted"), testObservation("pending")
	submit(t, box, accepted)
	box.flush(context.Background(), box.sinks[0])
	submit(t, box, accepted, pending)
	if err := box.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := New(box.directory, []Destination{destination})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	if status := restarted.Status()[0]; status.Accepted != 1 || status.Pending != 1 {
		t.Fatalf("restart status = %+v, want 1 accepted and 1 pending", status)
	}
	submit(t, restarted, accepted, pending)
	restarted.flush(context.Background(), restarted.sinks[0])
	if calls != 2 {
		t.Fatalf("exported %d observations, want accepted and pending exactly once each", calls)
	}
	if status := restarted.Status()[0]; status.Accepted != 2 || status.Pending != 0 {
		t.Fatalf("final status = %+v", status)
	}
}

func TestOutboxAcceptedObservationChangeIsDeliveredAgain(t *testing.T) {
	var received []string
	box := newTestOutbox(t, Destination{ID: "project", Exporter: exportFunc(func(_ context.Context, observations []domain.Observation) error {
		for _, observation := range observations {
			received = append(received, observation.Outcome)
		}
		return nil
	})})
	observation := testObservation("tool")
	submit(t, box, observation)
	box.flush(context.Background(), box.sinks[0])
	observation.Outcome = "error"
	submit(t, box, observation)
	if status := box.Status()[0]; status.Pending != 1 || status.Accepted != 0 {
		t.Fatalf("changed observation retained an old acceptance: %+v", status)
	}
	box.flush(context.Background(), box.sinks[0])
	if len(received) != 2 || received[0] != "completed" || received[1] != "error" {
		t.Fatalf("received outcomes = %v", received)
	}
}

func TestOutboxOldAcknowledgementCannotAcceptNewObservation(t *testing.T) {
	started := make(chan []domain.Observation, 1)
	release := make(chan struct{})
	box := newTestOutbox(t, Destination{ID: "project", Exporter: exportFunc(func(ctx context.Context, observations []domain.Observation) error {
		started <- observations
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})})
	first := testObservation("tool")
	submit(t, box, first)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { box.flush(ctx, box.sinks[0]); close(done) }()
	select {
	case batch := <-started:
		if len(batch) != 1 || batch[0].Outcome != "completed" {
			t.Fatalf("in-flight batch = %+v", batch)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("export did not start")
	}
	newer := first
	newer.Outcome = "error"
	submitted := make(chan error, 1)
	go func() { submitted <- box.Submit([]domain.Observation{newer}) }()
	select {
	case err := <-submitted:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight export blocked a new observation")
	}
	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("export did not finish")
	}
	if status := box.Status()[0]; status.Accepted != 0 || status.Pending != 1 {
		t.Fatalf("old acknowledgement accepted the new observation: %+v", status)
	}
	box.flush(context.Background(), box.sinks[0])
	if batch := <-started; len(batch) != 1 || batch[0].Outcome != "error" {
		t.Fatalf("new batch = %+v", batch)
	}
	if status := box.Status()[0]; status.Accepted != 1 || status.Pending != 0 {
		t.Fatalf("new observation was not accepted: %+v", status)
	}
}

func TestOutboxWorkersIsolateSlowDestinationAndCancel(t *testing.T) {
	slowStarted, fastSent := make(chan struct{}), make(chan struct{})
	box := newTestOutbox(t,
		Destination{ID: "slow", Exporter: exportFunc(func(ctx context.Context, _ []domain.Observation) error {
			close(slowStarted)
			<-ctx.Done()
			return ctx.Err()
		})},
		Destination{ID: "fast", Exporter: exportFunc(func(context.Context, []domain.Observation) error {
			close(fastSent)
			return nil
		})},
	)
	submit(t, box, testObservation("tool"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { box.Run(ctx); close(done) }()
	for _, event := range []<-chan struct{}{slowStarted, fastSent} {
		select {
		case <-event:
		case <-time.After(2 * time.Second):
			t.Fatal("a destination blocked another destination")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("outbox worker did not stop after cancellation")
	}
	if status := box.Status()[0]; status.Pending != 1 {
		t.Fatalf("cancelled in-flight delivery was lost: %+v", status)
	}
}

type deliveryFailure struct {
	retry bool
	delay time.Duration
}

func (e deliveryFailure) Error() string             { return "test delivery failure" }
func (e deliveryFailure) Retryable() bool           { return e.retry }
func (e deliveryFailure) RetryDelay() time.Duration { return e.delay }

func TestOutboxRetryPolicyAndRetryAfter(t *testing.T) {
	for _, retry := range []bool{false, true} {
		name := "rejected"
		if retry {
			name = "retryable"
		}
		t.Run(name, func(t *testing.T) {
			calls := 0
			box := newTestOutbox(t, Destination{ID: "project", Exporter: exportFunc(func(context.Context, []domain.Observation) error {
				calls++
				if calls == 1 {
					return deliveryFailure{retry: retry, delay: time.Minute}
				}
				return nil
			})})
			observation := testObservation("tool")
			submit(t, box, observation)
			before := time.Now()
			box.flush(context.Background(), box.sinks[0])
			if !retry {
				if box.Status()[0].Rejected != 1 || len(box.sinks[0].records) != 0 {
					t.Fatal("permanent failure was not retained as a terminal receipt")
				}
				box.flush(context.Background(), box.sinks[0])
				if calls != 1 {
					t.Fatal("permanently rejected observation retried")
				}
				return
			}
			r := box.sinks[0].records[observation.Key()]
			if r.Rejected == retry || r.Accepted || r.Attempts != 1 {
				t.Fatalf("retry=%v record=%+v", retry, r)
			}
			if r.NextAttempt.Before(before.Add(time.Minute)) {
				t.Fatalf("Retry-After was ignored: next=%s", r.NextAttempt)
			}
			box.flush(context.Background(), box.sinks[0])
			if calls != 1 {
				t.Fatal("retried before the configured delay")
			}
			r.NextAttempt = time.Time{}
			if err := box.save(box.sinks[0], observation.Key(), r); err != nil {
				t.Fatal(err)
			}
			box.flush(context.Background(), box.sinks[0])
			if retry && calls != 2 {
				t.Fatal("retryable record was not attempted again")
			}
			if !retry && calls != 1 {
				t.Fatal("permanently rejected record was retried")
			}
		})
	}
}

func TestOutboxDestinationIdentityKeepsReceiptsSeparate(t *testing.T) {
	var calls atomic.Int32
	exporter := exportFunc(func(context.Context, []domain.Observation) error { calls.Add(1); return nil })
	a := Destination{ID: FingerprintDestination("langfuse", "https://example.invalid/otel", "project-a"), Exporter: exporter}
	b := Destination{ID: FingerprintDestination("langfuse", "https://example.invalid/otel", "project-b"), Exporter: exporter}
	box := newTestOutbox(t, a)
	observation := testObservation("tool")
	submit(t, box, observation)
	box.flush(context.Background(), box.sinks[0])
	if err := box.Close(); err != nil {
		t.Fatal(err)
	}
	other, err := New(box.directory, []Destination{b})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Close() })
	submit(t, other, observation)
	other.flush(context.Background(), other.sinks[0])
	if got := calls.Load(); got != 2 {
		t.Fatalf("destination change reused an old receipt: calls=%d", got)
	}
}

func TestOutboxInvalidTimestampDoesNotPoisonValidBatch(t *testing.T) {
	var received []domain.Observation
	box := newTestOutbox(t, Destination{ID: "project", Exporter: exportFunc(func(_ context.Context, observations []domain.Observation) error {
		for _, observation := range observations {
			if observation.ObservedAt.IsZero() {
				return errors.New("timestamp unavailable")
			}
		}
		received = append(received, observations...)
		return nil
	})})
	valid, invalid := testObservation("valid"), testObservation("invalid")
	invalid.ObservedAt = time.Time{}
	submit(t, box, valid, invalid)
	box.flush(context.Background(), box.sinks[0])
	if len(received) != 1 || received[0].ID != "valid" {
		t.Fatalf("invalid observation blocked valid delivery: received=%+v", received)
	}
	if status := box.Status()[0]; status.Accepted != 1 || status.Rejected != 1 || status.Pending != 0 {
		t.Fatalf("invalid observation was not isolated: %+v", status)
	}
	invalid.ObservedAt = valid.ObservedAt
	submit(t, box, invalid)
	box.flush(context.Background(), box.sinks[0])
	if status := box.Status()[0]; status.Accepted != 2 || status.Rejected != 0 || status.Pending != 0 {
		t.Fatalf("corrected observation remained permanently rejected: %+v", status)
	}
}

func TestOutboxRejectsConcurrentWriterAndReleasesLock(t *testing.T) {
	destination := Destination{ID: "project", Exporter: exportFunc(func(context.Context, []domain.Observation) error { return nil })}
	box := newTestOutbox(t, destination)
	if duplicate, err := New(box.directory, []Destination{destination}); err == nil {
		_ = duplicate.Close()
		t.Fatal("same outbox accepted concurrent writers")
	}
	if err := box.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := New(box.directory, []Destination{destination})
	if err != nil {
		t.Fatalf("closed outbox did not release writer lock: %v", err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
}

func TestOutboxRejectsCorruptPersistedFingerprint(t *testing.T) {
	destination := Destination{ID: "project", Exporter: exportFunc(func(context.Context, []domain.Observation) error { return nil })}
	box := newTestOutbox(t, destination)
	observation := testObservation("tool")
	submit(t, box, observation)
	path := filepath.Join(box.sinkDirectory(box.sinks[0]), digest([]byte(observation.Key()))+".json")
	if err := box.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var stored record
	if err := json.Unmarshal(data, &stored); err != nil {
		t.Fatal(err)
	}
	stored.Observation.Outcome = "error"
	changed, err := json.Marshal(stored)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, changed, 0600); err != nil {
		t.Fatal(err)
	}
	if restarted, err := New(box.directory, []Destination{destination}); err == nil {
		_ = restarted.Close()
		t.Fatal("corrupt observation retained an unrelated delivery fingerprint")
	}
}
