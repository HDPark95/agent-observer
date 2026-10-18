package httpapi

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/fsnotify/fsnotify"
)

func TestMissingDiscoveryDirectoryKeepsNotificationCoverage(t *testing.T) {
	root := t.TempDir()
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		t.Fatal(err)
	}
	defer watcher.Close()
	future := filepath.Join(root, "2027", "01", "01")
	if healthy, _ := syncDirectoryWatches(watcher, []string{future}); !healthy {
		t.Fatal("absent future date incorrectly marked file watcher unhealthy")
	}
	if got := watcher.WatchList(); len(got) != 1 || got[0] != root {
		t.Fatalf("watch list = %v, want existing ancestor %q", got, root)
	}
	if err := os.MkdirAll(future, 0700); err != nil {
		t.Fatal(err)
	}
	if healthy, _ := syncDirectoryWatches(watcher, []string{future}); !healthy {
		t.Fatal("created directory not watched")
	}
	if got := watcher.WatchList(); len(got) != 1 || got[0] != future {
		t.Fatalf("watch did not advance to created directory: %v", got)
	}
}

func TestWatchFailuresRemainUnhealthy(t *testing.T) {
	root := t.TempDir()
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		t.Fatal(err)
	}
	defer watcher.Close()
	if healthy, _ := syncDirectoryWatches(watcher, []string{root}); !healthy {
		t.Fatal("initial watch failed")
	}
	file := filepath.Join(root, "not-a-directory")
	if err := os.WriteFile(file, nil, 0600); err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{file, filepath.Join(file, "child")} {
		if healthy, _ := syncDirectoryWatches(watcher, []string{target}); healthy {
			t.Fatalf("invalid directory %q reported healthy", target)
		}
		if got := watcher.WatchList(); len(got) != 1 || got[0] != root {
			t.Fatalf("failed replacement discarded existing coverage: %v", got)
		}
	}
	if err := watcher.Close(); err != nil {
		t.Fatal(err)
	}
	if healthy, _ := syncDirectoryWatches(watcher, []string{root}); healthy {
		t.Fatal("closed file watcher reported healthy")
	}
}
