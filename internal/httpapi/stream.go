package httpapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"agent-observer/internal/model"
	"agent-observer/internal/sessions"
	"github.com/fsnotify/fsnotify"
)

// One bounded stream per browser. No snapshot queue: a slow client gets the
// latest state after its write completes, and a stalled write expires in 5s.
// Reconnection always starts with a full snapshot; Last-Event-ID replay is unnecessary.
func streamEvents(w http.ResponseWriter, r *http.Request, registry *sessions.Registry) {
	id := r.URL.Query().Get("session")
	if id != "" {
		if _, err := registry.State(id); err != nil {
			http.Error(w, "Unknown session", 404)
			return
		}
	}
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		http.Error(w, "File notifications unavailable", http.StatusServiceUnavailable)
		return
	}
	defer watcher.Close()

	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("X-Accel-Buffering", "no")
	controller := http.NewResponseController(w)
	send := func(event string, payload []byte) error {
		_ = controller.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, payload); err != nil {
			return err
		}
		return controller.Flush()
	}
	// The normal HTTP WriteTimeout does not terminate healthy long-lived streams.
	defer controller.SetWriteDeadline(time.Time{})
	var lastListing, lastState []byte
	var delta streamDelta
	useDelta := r.URL.Query().Get("format") == "delta"
	watchError := false
	recovery := time.NewTicker(10 * time.Second)
	defer recovery.Stop()
	for {
		changed := registry.Changes() // subscribe before reads so concurrent hook writes cannot be missed
		messageChanged := registry.MessageChanges()
		// Install known watches before reading. Newly discovered child directories
		// are reconciled below and read once more after their watches are active.
		syncDirectoryWatches(watcher, registry.WatchDirectories(id))
		listing, _ := json.Marshal(registry.MetadataListing())
		if !bytes.Equal(listing, lastListing) {
			if send("sessions", listing) != nil {
				return
			}
			lastListing = listing
		}
		if id != "" {
			state, err := registry.State(id)
			if err != nil {
				return
			}
			healthy, watchesChanged := syncDirectoryWatches(watcher, registry.WatchDirectories(id))
			if watchesChanged {
				state, err = registry.State(id)
				if err != nil {
					return
				}
			}
			state["mode"] = "실제 transcript 증분 읽기 · SSE"
			state["transport"] = model.Record{"kind": "sse", "fileWatch": healthy && !watchError, "recoverySeconds": 10}
			// checkedAt is volatile. The signature includes error/discovery/body revisions,
			// but never serializes the full node graph on unrelated file notifications.
			signature := model.Record{}
			for k, v := range state {
				if k != "nodes" && k != "checkedAt" {
					signature[k] = v
				}
			}
			key, _ := json.Marshal(signature)
			if !bytes.Equal(key, lastState) {
				event, message := "state", state
				if useDelta {
					event, message = delta.next(state)
				}
				payload, err := json.Marshal(message)
				if err != nil || send(event, payload) != nil {
					return
				}
				lastState = key
			}
		}
		// A watcher error gets one explicit recovery frame. The full read above
		// repairs missed events; a later successful pass may report healthy again.
		watchError = false
		_ = controller.SetWriteDeadline(time.Time{})
		select {
		case <-r.Context().Done():
			return
		case <-changed:
		case <-messageChanged:
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			if event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Remove|fsnotify.Rename) == 0 {
				continue
			}
		case _, ok := <-watcher.Errors:
			if !ok {
				return
			}
			watchError = true
		case <-recovery.C:
			// Repair missed filesystem notifications/new date directories, refresh only
			// catalog metadata, and keep the SSE connection alive during idle sessions.
			if send("heartbeat", []byte(`{}`)) != nil {
				return
			}
		}
	}
}
