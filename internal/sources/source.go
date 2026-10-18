// Package sources isolates provider-specific collection from session orchestration.
package sources

import (
	"errors"
	"path/filepath"

	"agent-observer/internal/model"
	"agent-observer/internal/normalize"
	"agent-observer/internal/observer"
	"agent-observer/internal/transcript"
)

const (
	Codex      = "codex"
	ClaudeCode = "claude-code"
)

// Reader publishes immutable graph records for one explicitly selected source.
// The session and message consumers need the same snapshot and watch semantics.
type Reader interface {
	Read() (model.Record, []model.Record, string)
	WatchDirectories() []string
}

// Metadata contains no prompt, transcript body, command, or tool result.
type Metadata struct {
	Provider, SessionID, Project string
}

// Header reads only the bounded first metadata record of the selected file.
func Header(provider, path string) (Metadata, error) {
	if provider == "" {
		provider = Codex
	}
	row, err := transcript.Header(path, 65536)
	if err != nil {
		return Metadata{}, err
	}
	switch provider {
	case Codex:
		p := row.Object("payload")
		id := p.String("id")
		if row.String("type") != "session_meta" || !normalize.ValidID.MatchString(id) || p.String("parent_thread_id") != "" || (p.String("agent_path") != "" && p.String("agent_path") != "/root") {
			return Metadata{}, errors.New("root Codex session metadata required")
		}
		project := filepath.Base(p.String("cwd"))
		if project == "." || project == "/" {
			project = "Codex"
		}
		return Metadata{Provider: provider, SessionID: id, Project: project}, nil
	case ClaudeCode:
		event, err := decodeClaude(row)
		if err != nil {
			return Metadata{}, err
		}
		return Metadata{Provider: provider, SessionID: event.SessionID, Project: event.Project}, nil
	default:
		return Metadata{}, errors.New("unsupported source provider")
	}
}

// Open retains the Codex reader and selects Claude's metadata-only hook reader.
func Open(provider, path string, children []string) (Reader, error) {
	meta, err := Header(provider, path)
	if err != nil {
		return nil, err
	}
	if meta.Provider == Codex {
		return observer.NewPinned(path, children, meta.SessionID), nil
	}
	if len(children) != 0 {
		return nil, errors.New("cannot attach Codex child transcripts to a Claude Code source")
	}
	return newClaude(path, meta.SessionID), nil
}

func projectName(path, fallback string) string {
	name := filepath.Base(path)
	if !safeName.MatchString(name) || name == "." || name == ".." {
		return fallback
	}
	return name
}
