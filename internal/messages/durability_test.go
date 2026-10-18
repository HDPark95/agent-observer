package messages

import (
	"errors"
	"os"
	"testing"

	"agent-observer/internal/model"
)

func TestPublishDistinguishesCommitFromDurability(t *testing.T) {
	for _, stage := range []string{"rename", "directory"} {
		t.Run(stage, func(t *testing.T) {
			store, identity := testStore(t)
			source := messageSource()
			capture := persistentCapture(t, source, store, identity)
			capture.Read()
			before, err := os.ReadFile(store.filename(identity))
			if err != nil {
				t.Fatal(err)
			}
			rename, syncDir := store.fs.Rename, store.fs.SyncDir
			fail := errors.New("injected")
			if stage == "rename" {
				store.fs.Rename = func(string, string) error { return fail }
			} else {
				store.fs.SyncDir = func(string) error { return fail }
			}
			_, err = capture.Publish(hookBody("retained after commit"), true)
			if err == nil || errors.Is(err, ErrDurabilityUnknown) != (stage == "directory") {
				t.Fatalf("publish error=%v", err)
			}
			data, err := os.ReadFile(store.filename(identity))
			if err != nil {
				t.Fatal(err)
			}
			body := capture.bodies["a2a:/root/ux:call"]
			if stage == "rename" {
				if string(data) != string(before) || body != "" {
					t.Fatal("precommit failure changed state")
				}
			} else {
				if string(data) == string(before) || body != "retained after commit" {
					t.Fatal("committed state rolled back")
				}
				if capture.Read().String("messageStoreError") == "" {
					t.Fatal("uncertain durability hidden")
				}
				reopened, err := OpenStore(store.directory)
				if err != nil {
					t.Fatal(err)
				}
				restarted := persistentCapture(t, source, reopened, identity)
				if restarted.Read()["nodes"].([]model.Record)[0].String("body") != body {
					t.Fatal("disk and memory disagree")
				}
			}
			store.fs.Rename, store.fs.SyncDir = rename, syncDir
			if _, err := capture.Publish(hookBody("retained after commit"), true); err != nil {
				t.Fatal(err)
			}
			revision := capture.Read().String("revision")
			if _, err := capture.Publish(hookBody("retained after commit"), true); err != nil {
				t.Fatal(err)
			}
			if capture.Read().String("revision") != revision {
				t.Fatal("identical retry duplicated body")
			}
		})
	}
}
