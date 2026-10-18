package sources

import (
	"errors"
	"sync"

	"agent-observer/internal/model"
	"agent-observer/internal/observer"
)

// Selection is an explicit source identity, never a directory discovery request.
type Selection struct {
	ID, Provider, Path string
	Children           []string
}

// History reads completed and current turns independently of the UI cursor.
type History interface {
	Read() ([]model.Record, error)
}

type turnReader interface {
	ReadTurns() ([]model.Record, error)
}

type history struct {
	mu        sync.Mutex
	selection Selection
	metadata  Metadata
	reader    turnReader
}

func OpenHistory(selection Selection) (History, error) {
	meta, err := Header(selection.Provider, selection.Path)
	if err != nil {
		return nil, err
	}
	id := meta.SessionID
	if meta.Provider != Codex {
		id = meta.Provider + ":" + id
	}
	if selection.ID != "" && selection.ID != id {
		return nil, errors.New("selected history session identity does not match source")
	}
	if meta.Provider == ClaudeCode && len(selection.Children) != 0 {
		return nil, errors.New("cannot attach Codex child transcripts to a Claude Code history")
	}
	selection.ID, selection.Provider = id, meta.Provider
	selection.Children = append([]string(nil), selection.Children...)
	return &history{selection: selection, metadata: meta}, nil
}

func (h *history) openReader() turnReader {
	if h.metadata.Provider == Codex {
		return observer.NewHistoryPinned(h.selection.Path, h.selection.Children, h.metadata.SessionID)
	}
	r := newClaude(h.selection.Path, h.metadata.SessionID)
	r.history = true
	return r
}

// Read restarts a failed cursor so a transient source failure cannot silently
// skip an already-consumed turn. Delivery deduplicates that intentional replay.
func (h *history) Read() ([]model.Record, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	meta, err := Header(h.selection.Provider, h.selection.Path)
	if err != nil {
		h.reader = nil
		return nil, err
	}
	if meta.SessionID != h.metadata.SessionID {
		h.reader = nil
		return nil, errors.New("history source session identity changed")
	}
	if h.reader == nil {
		h.reader = h.openReader()
	}
	turns, err := h.reader.ReadTurns()
	if err != nil {
		h.reader = nil
		return nil, err
	}
	for _, turn := range turns {
		turn["sessionId"], turn["provider"], turn["sourceSessionId"] = h.selection.ID, meta.Provider, meta.SessionID
	}
	return turns, nil
}

func (r *claudeReader) ReadTurns() ([]model.Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.history {
		return nil, errors.New("turn history is not enabled")
	}
	if err := r.cursor.Read(r.reset, r.consume); err != nil {
		return nil, err
	}
	if r.rejected {
		return nil, errors.New("invalid or mismatched Claude source event in history")
	}
	if r.turn != "" {
		r.turns = append(r.turns, r.snapshotState(nil))
	}
	turns := r.turns
	r.turns = nil
	return turns, nil
}
