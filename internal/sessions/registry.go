// Package sessions owns explicit sessions and a metadata-only catalog.
package sessions

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"agent-observer/internal/messages"
	"agent-observer/internal/model"
	"agent-observer/internal/sources"
)

type Registry struct {
	mu               sync.Mutex
	changed          chan struct{}
	entries, offered map[string]*entry
	order            []string
	catalog          catalog
	titles           titles
	messageStore     *messages.Store
	messageFeed      MessageFeed
	messageViews     map[string]messageView
}

func New(root string) *Registry {
	r := &Registry{changed: make(chan struct{}), entries: map[string]*entry{}, offered: map[string]*entry{}, catalog: catalog{root: root, entries: map[string]*entry{}}}
	if root != "" {
		r.titles.path = filepath.Join(filepath.Dir(root), "session_index.jsonl")
	}
	return r
}

// NewWithStore enables local message persistence only when directory is explicit.
func NewWithStore(root, directory string) (*Registry, error) {
	r := New(root)
	if directory == "" {
		return r, nil
	}
	store, err := messages.OpenStore(directory)
	if err != nil {
		return nil, err
	}
	r.messageStore = store
	return r, nil
}
func (r *Registry) label(id string, e *entry, names map[string]string) string {
	if e.label != "" {
		return e.label
	}
	if title := names[id]; title != "" {
		return title
	}
	suffix := id
	if len(id) > 8 {
		suffix = id[len(id)-8:]
	}
	return "제목 없는 대화 · " + suffix
}
func (r *Registry) Register(p model.Record, children []string) (model.Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer r.signalLocked()
	return r.register(p, children)
}
func (r *Registry) register(p model.Record, children []string) (model.Record, error) {
	source := p.String("source")
	if strings.HasPrefix(source, "~/") {
		home, _ := os.UserHomeDir()
		source = filepath.Join(home, source[2:])
	}
	if !filepath.IsAbs(source) || filepath.Ext(source) != ".jsonl" {
		return nil, errors.New("절대 경로의 JSONL 파일이 필요합니다")
	}
	provider := p.String("provider")
	if provider == "" {
		provider = "codex"
	}
	id, item, err := providerHeader(provider, source)
	if err != nil {
		return nil, err
	}
	resolved, err := filepath.EvalSymlinks(source)
	if err != nil {
		return nil, err
	}
	item.source = resolved
	if wanted := p.String("sessionId"); wanted != "" && wanted != id && wanted != item.sessionID {
		return nil, errors.New("세션 ID와 기록 파일이 일치하지 않습니다")
	}
	if p["label"] != nil {
		if _, ok := p["label"].(string); !ok {
			return nil, errors.New("세션 이름 형식 오류")
		}
	}
	item.label = p.String("label")
	if utf8.RuneCountInString(item.label) > 100 {
		return nil, errors.New("세션 이름은 100자 이하여야 합니다")
	}
	if p.Bool("metadataOnly") && r.entries[id] == nil {
		if len(r.offered) >= 100 && r.offered[id] == nil {
			return nil, errors.New("세션 목록 한도")
		}
		r.offered[id] = item
		return model.Record{"sessionId": id, "label": r.label(id, item, r.titles.read()), "registered": false}, nil
	}
	if previous := r.entries[id]; previous != nil {
		if previous.source != resolved {
			return nil, errors.New("이미 다른 경로로 등록된 세션입니다")
		}
		if item.label != "" {
			previous.label = item.label
		}
	} else {
		if len(r.entries) >= 32 {
			return nil, errors.New("최대 32개 세션을 연결할 수 있습니다")
		}
		reader, err := sources.Open(provider, resolved, children)
		if err != nil {
			return nil, err
		}
		item.capture, err = messages.NewWithStore(reader, r.messageStore, messages.Identity{Provider: item.provider, Source: resolved, Session: item.sessionID})
		if err != nil {
			return nil, err
		}
		item.children = append([]string(nil), children...)
		r.entries[id] = item
		r.order = append(r.order, id)
	}
	return model.Record{"sessionId": id, "label": r.label(id, r.entries[id], r.titles.read())}, nil
}
func (r *Registry) Open(p model.Record) (model.Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer r.signalLocked()
	id := p.String("sessionId")
	if e := r.entries[id]; e != nil {
		return model.Record{"sessionId": id, "label": r.label(id, e, r.titles.read())}, nil
	}
	item := r.offered[id]
	if item == nil {
		item = r.catalog.read()[id]
	}
	if item == nil {
		return nil, errors.New("목록에 없는 세션입니다")
	}
	return r.register(model.Record{"source": item.source, "sessionId": id, "label": item.label, "provider": item.provider}, nil)
}
func (r *Registry) State(id string) (model.Record, error) {
	id, e, label, err := r.selected(id)
	if err != nil {
		return nil, err
	}
	if e.capture == nil {
		return model.Record{"sessionId": nil, "turn": nil, "revision": "empty", "nodes": []model.Record{}, "error": nil}, nil
	}
	state := e.capture.Read()
	state["sessionId"] = id
	state["provider"] = e.provider
	state["sourceSessionId"] = e.sessionID
	state["sessionLabel"] = label
	r.addMessages(id, state)
	return state, nil
}

// selected copies registry-owned metadata. Capture serializes its own I/O;
// keeping that work outside r.mu allows unrelated sessions to make progress.
func (r *Registry) selected(id string) (string, entry, string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if id == "" && len(r.order) > 0 {
		id = r.order[0]
	}
	if id == "" {
		return "", entry{}, "", nil
	}
	e := r.entries[id]
	if e == nil {
		return "", entry{}, "", errors.New("등록되지 않은 세션입니다")
	}
	return id, *e, r.label(id, e, r.titles.read()), nil
}
func (r *Registry) Listing() model.Record { return r.listing(true) }

// MetadataListing never reads transcript bodies, including previously selected sessions.
func (r *Registry) MetadataListing() model.Record { return r.listing(false) }
func (r *Registry) listing(includeState bool) model.Record {
	all, labels := r.listingEntries()
	items := []model.Record{}
	for id, e := range all {
		updated := e.updated
		if st, err := os.Stat(e.source); err == nil {
			updated = st.ModTime().UTC().Format(time.RFC3339Nano)
		}
		item := model.Record{"sessionId": id, "provider": e.provider, "sourceSessionId": e.sessionID, "label": labels[id], "project": e.project, "updatedAt": updated, "registered": e.capture != nil}
		if includeState && e.capture != nil {
			s := e.capture.Read()
			item["error"] = s["error"]
			item["lastEvent"] = s["lastEvent"]
			nodes, _ := s["nodes"].([]model.Record)
			item["nodes"] = len(nodes)
		}
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		a, b := items[i].String("updatedAt"), items[j].String("updatedAt")
		if a == b {
			return items[i].String("sessionId") < items[j].String("sessionId")
		}
		return a > b
	})
	return model.Record{"sessions": items}
}

func (r *Registry) listingEntries() (map[string]entry, map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	all := map[string]entry{}
	for id, e := range r.catalog.read() {
		all[id] = *e
	}
	for id, e := range r.offered {
		all[id] = *e
	}
	for id, e := range r.entries {
		all[id] = *e
	}
	names := r.titles.read()
	labels := make(map[string]string, len(all))
	for id, e := range all {
		labels[id] = r.label(id, &e, names)
	}
	return all, labels
}
func (r *Registry) Publish(p model.Record, hook bool) (model.Record, error) {
	r.mu.Lock()
	id := p.String("sessionId")
	if id == "" && !hook && len(r.order) == 1 {
		id = r.order[0]
	}
	e := r.entries[id]
	r.mu.Unlock()
	if e == nil {
		return nil, errors.New("등록되지 않은 세션입니다")
	}
	result, err := e.capture.Publish(p, hook)
	r.mu.Lock()
	r.signalLocked()
	r.mu.Unlock()
	return result, err
}

// Changes is a broadcast generation, not a queue of transcript content.
func (r *Registry) Changes() <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.changed
}
func (r *Registry) signalLocked() { close(r.changed); r.changed = make(chan struct{}) }

// WatchDirectories exposes only directories needed by this selected session and
// the metadata index. Directory notifications grant no permission to read bodies.
func (r *Registry) WatchDirectories(id string) []string {
	r.mu.Lock()
	dirs := []string{}
	if r.titles.path != "" {
		dirs = append(dirs, filepath.Dir(r.titles.path))
	}
	e := r.entries[id]
	r.mu.Unlock()
	if e != nil {
		dirs = append(dirs, filepath.Dir(e.source))
		if source, ok := e.capture.(interface{ WatchDirectories() []string }); ok {
			dirs = append(dirs, source.WatchDirectories()...)
		}
	}
	return dirs
}

// SelectedSources returns only explicit selections. Export uses its own readers
// so UI reads cannot consume a turn before the delivery worker sees it.
func (r *Registry) SelectedSources() []sources.Selection {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]sources.Selection, 0, len(r.order))
	for _, id := range r.order {
		e := r.entries[id]
		result = append(result, sources.Selection{ID: id, Provider: e.provider, Path: e.source, Children: append([]string(nil), e.children...)})
	}
	return result
}
