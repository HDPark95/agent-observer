package sessions

import (
	"testing"
	"time"

	"agent-observer/internal/model"
)

type blockedCapture struct {
	operation string
	entered   chan struct{}
	release   chan struct{}
}

func (c *blockedCapture) block(operation string) {
	if c.operation != operation {
		return
	}
	select {
	case c.entered <- struct{}{}:
	default:
	}
	<-c.release
}
func (c *blockedCapture) Read() model.Record {
	c.block("read")
	return model.Record{"turn": "turn", "revision": "1", "nodes": []model.Record{}}
}
func (c *blockedCapture) Publish(model.Record, bool) (model.Record, error) {
	c.block("publish")
	return model.Record{"status": "attached"}, nil
}
func (c *blockedCapture) WatchDirectories() []string { c.block("watch"); return nil }

func TestSlowCaptureDoesNotBlockOtherSessions(t *testing.T) {
	cases := []struct {
		name, operation string
		run             func(*Registry)
	}{
		{"state", "read", func(r *Registry) { _, _ = r.State("slow") }},
		{"listing", "read", func(r *Registry) { r.Listing() }},
		{"publish", "publish", func(r *Registry) { _, _ = r.Publish(model.Record{"sessionId": "slow"}, false) }},
		{"watch", "watch", func(r *Registry) { r.WatchDirectories("slow") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			slow := &blockedCapture{operation: tc.operation, entered: make(chan struct{}, 1), release: make(chan struct{})}
			defer close(slow.release)
			r := New("")
			r.entries["slow"] = &entry{capture: slow}
			r.entries["fast"] = &entry{capture: &blockedCapture{}}
			done := make(chan struct{})
			go func() { tc.run(r); close(done) }()
			select {
			case <-slow.entered:
			case <-time.After(time.Second):
				t.Fatal("slow capture was not entered")
			}
			progressed := make(chan error, 1)
			go func() {
				_, err := r.State("fast")
				if err == nil {
					_, err = r.Publish(model.Record{"sessionId": "fast"}, false)
				}
				r.MetadataListing()
				r.Changes()
				progressed <- err
			}()
			select {
			case err := <-progressed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("unrelated session waited for slow capture")
			}
			// Cleanup releases the source; the completion wait catches leaked operations.
			t.Cleanup(func() {
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("source operation did not finish")
				}
			})
		})
	}
}
