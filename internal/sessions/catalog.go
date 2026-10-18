package sessions

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"agent-observer/internal/model"
	"agent-observer/internal/sources"
)

type entry struct {
	source, project, label, updated string
	provider, sessionID             string
	children                        []string
	capture                         captureReader
}
type captureReader interface {
	Read() model.Record
	Publish(model.Record, bool) (model.Record, error)
}

func header(source string) (string, *entry, error) { return providerHeader("codex", source) }

func providerHeader(provider, source string) (string, *entry, error) {
	metadata, err := sources.Header(provider, source)
	if err != nil {
		return "", nil, err
	}
	st, err := os.Stat(source)
	if err != nil {
		return "", nil, err
	}
	id := metadata.SessionID
	if metadata.Provider != "codex" {
		id = metadata.Provider + ":" + id
	}
	return id, &entry{source: source, provider: metadata.Provider, sessionID: metadata.SessionID, project: metadata.Project, updated: st.ModTime().UTC().Format(time.RFC3339Nano)}, nil
}

type catalog struct {
	root      string
	refreshed time.Time
	entries   map[string]*entry
}

func (c *catalog) read() map[string]*entry {
	if c.root == "" || (!c.refreshed.IsZero() && time.Since(c.refreshed) < 10*time.Second) {
		return c.entries
	}
	found := map[string]*entry{}
	duplicates := map[string]bool{}
	st, e := os.Lstat(c.root)
	if e == nil && st.IsDir() {
		days, _ := filepath.Glob(filepath.Join(c.root, "[0-9][0-9][0-9][0-9]", "[0-9][0-9]", "[0-9][0-9]"))
		sort.Sort(sort.Reverse(sort.StringSlice(days)))
		examined := 0
		for _, day := range days {
			safe := true
			for _, p := range []string{day, filepath.Dir(day), filepath.Dir(filepath.Dir(day))} {
				s, e := os.Lstat(p)
				if e != nil || !s.IsDir() {
					safe = false
				}
			}
			if !safe {
				continue
			}
			files, _ := filepath.Glob(filepath.Join(day, "rollout-*.jsonl"))
			sort.Sort(sort.Reverse(sort.StringSlice(files)))
			for _, p := range files {
				examined++
				id, item, e := header(p)
				if e == nil {
					if found[id] != nil {
						duplicates[id] = true
					}
					found[id] = item
				}
				if examined >= 1000 || len(found) >= 100 {
					break
				}
			}
			if examined >= 1000 || len(found) >= 100 {
				break
			}
		}
	}
	for id := range duplicates {
		delete(found, id)
	}
	c.entries = found
	c.refreshed = time.Now()
	return found
}

type titles struct {
	path  string
	info  os.FileInfo
	names map[string]string
}

func (t *titles) read() map[string]string {
	empty := map[string]string{}
	if t.path == "" {
		return empty
	}
	st, e := os.Lstat(t.path)
	if e != nil || !st.Mode().IsRegular() {
		return empty
	}
	if t.info != nil && os.SameFile(st, t.info) && st.Size() == t.info.Size() && st.ModTime() == t.info.ModTime() {
		return t.names
	}
	f, e := os.Open(t.path)
	if e != nil {
		return empty
	}
	defer f.Close()
	offset := st.Size() - 4*1024*1024
	if offset < 0 {
		offset = 0
	}
	if _, e = f.Seek(offset, io.SeekStart); e != nil {
		return empty
	}
	reader := bufio.NewReader(io.LimitReader(f, 4*1024*1024))
	if offset > 0 {
		reader.ReadBytes('\n')
	}
	names := map[string]string{}
	for {
		line, err := reader.ReadBytes('\n')
		var row model.Record
		if json.Unmarshal(line, &row) == nil {
			title := strings.TrimSpace(row.String("thread_name"))
			id := row.String("id")
			if id != "" && title != "" {
				r := []rune(title)
				if len(r) > 500 {
					title = string(r[:500])
				}
				names[id] = title
			}
		}
		if err != nil {
			break
		}
	}
	t.info = st
	t.names = names
	return names
}
