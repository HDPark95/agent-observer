package httpapi

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/fsnotify/fsnotify"
)

// syncDirectoryWatches covers future discovery directories through their
// nearest existing ancestor. Creation moves coverage to the target; it never
// expands which transcripts the registry is authorized to read.
func syncDirectoryWatches(watcher *fsnotify.Watcher, directories []string) (healthy, changed bool) {
	present := map[string]bool{}
	for _, dir := range watcher.WatchList() {
		present[dir] = true
	}
	wanted := map[string]bool{}
	healthy = true
	for _, target := range directories {
		dir, err := existingWatchDirectory(target)
		if err != nil {
			healthy = false
			continue
		}
		if wanted[dir] {
			continue
		}
		wanted[dir] = true
		if !present[dir] {
			if watcher.Add(dir) != nil {
				healthy = false
			} else {
				changed = true
			}
		}
	}
	// Install replacement watches before removing ancestors, closing the gap
	// during date-directory creation and atomic transcript replacement.
	// On a failed replacement, retain existing coverage until the next retry.
	if !healthy {
		return healthy, changed
	}
	for dir := range present {
		if !wanted[dir] {
			if err := watcher.Remove(dir); err != nil && !errors.Is(err, fsnotify.ErrNonExistentWatch) {
				healthy = false
			} else {
				changed = true
			}
		}
	}
	return healthy, changed
}

func existingWatchDirectory(target string) (string, error) {
	dir := filepath.Clean(target)
	for {
		info, err := os.Stat(dir)
		if err == nil {
			if !info.IsDir() {
				return "", errors.New("watch target is not a directory")
			}
			return dir, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", err
		}
		dir = parent
	}
}
