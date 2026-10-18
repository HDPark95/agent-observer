// Package agents reads only explicitly selected or identity-matched child transcripts.
package agents

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"time"

	"agent-observer/internal/model"
	"agent-observer/internal/normalize"
	"agent-observer/internal/transcript"
)

type Expected struct{ Thread, Path, Parent string }
type Reader struct {
	Cursor            transcript.Cursor
	Expected          *Expected
	Path, Thread      string
	floor             float64
	floorValid, first bool
	Calls, Activities map[string]model.Record
}

func New(source string, expected *Expected) *Reader {
	r := &Reader{Cursor: transcript.Cursor{Path: source}, Expected: expected}
	r.reset()
	return r
}
func (r *Reader) reset() {
	r.Path = ""
	r.Thread = ""
	r.floorValid = false
	r.first = true
	r.Calls = map[string]model.Record{}
	r.Activities = map[string]model.Record{}
}
func (r *Reader) Read() error { return r.Cursor.Read(r.reset, r.consume) }
func (r *Reader) consume(row model.Record) {
	p := row.Object("payload")
	if r.first {
		r.first = false
		if row.String("type") == "session_meta" {
			r.Path = p.String("agent_path")
			if !normalize.AgentPath(r.Path) {
				r.Path = ""
			}
			r.Thread = p.String("id")
			r.floor, r.floorValid = p["subagent_history_start_ordinal"].(float64)
			if r.Expected != nil && (r.Thread != r.Expected.Thread || r.Path != r.Expected.Path || p.String("parent_thread_id") != r.Expected.Parent) {
				r.Path = ""
			}
		}
		return
	}
	ordinal, ok := row["ordinal"].(float64)
	if r.Path == "" || !r.floorValid || !ok || ordinal < r.floor {
		return
	}
	if row.String("type") == "event_msg" && p.String("type") == "item_completed" {
		r.activity(row)
		return
	}
	if row.String("type") != "response_item" {
		return
	}
	id := p.String("call_id")
	if !normalize.ValidID.MatchString(id) {
		return
	}
	if p.String("type") == "function_call" && p.String("namespace") == "collaboration" && p.String("name") == "send_message" {
		var args model.Record
		if json.Unmarshal([]byte(p.String("arguments")), &args) != nil {
			return
		}
		target := Relative(r.Path, args.String("target"))
		if !normalize.AgentPath(target) {
			return
		}
		var unavailable any
		if Encrypted(args.String("message")) {
			unavailable = "encrypted"
		}
		r.Calls[id] = model.Record{"id": "a2a:" + r.Path + ":" + id, "sender": r.Path, "recipient": target, "at": row["timestamp"], "returned": false, "body": Body(args["message"]), "bodyUnavailable": unavailable}
	} else if p.String("type") == "function_call_output" {
		if n := r.Calls[id]; n != nil {
			n["returned"] = true
		}
	}
}
func (r *Reader) activity(row model.Record) {
	p := row.Object("payload")
	item := p.Object("item")
	switch item.String("type") {
	case "CommandExecution", "McpToolCall", "FileChange", "ImageView", "WebSearch":
	default:
		return
	}
	if item.String("id") == "" || r.Thread == "" {
		return
	}
	s := normalize.New()
	s.Turn = p.String("turn_id")
	if s.Turn == "" {
		s.Turn = "child-event"
	}
	copy := row.Clone()
	payload := p.Clone()
	payload["turn_id"] = s.Turn
	copy["payload"] = payload
	s.Consume(copy)
	prefix := "child:" + r.Thread + ":"
	for _, record := range s.List() {
		node := record.Clone()
		id := prefix + node.String("id")
		node["id"] = id
		if node.String("parent") == "agent" {
			node["parent"] = normalize.AgentID(r.Path)
		} else {
			node["parent"] = prefix + node.String("parent")
		}
		node["ownerAgent"] = normalize.AgentID(r.Path)
		node["ownerLabel"] = filepath.Base(r.Path)
		node["childActivity"] = true
		r.Activities[id] = node
	}
}

var uuid = regexp.MustCompile(`^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$`)
var dayDir = regexp.MustCompile(`^\d{2}$`)
var yearDir = regexp.MustCompile(`^\d{4}$`)

func Discover(source string, e Expected, stamp string) *Reader {
	if !uuid.MatchString(e.Thread) {
		return nil
	}
	dirs := DiscoveryDirectories(source, stamp)
	matches := []string{}
	for dir := range dirs {
		paths, _ := filepath.Glob(filepath.Join(dir, "rollout-*-"+e.Thread+".jsonl"))
		for _, file := range paths {
			row, err := transcript.Header(file, 1024*1024)
			if err != nil {
				continue
			}
			p := row.Object("payload")
			_, floor := p["subagent_history_start_ordinal"].(float64)
			if row.String("type") == "session_meta" && p.String("id") == e.Thread && p.String("agent_path") == e.Path && p.String("parent_thread_id") == e.Parent && floor {
				matches = append(matches, file)
			}
		}
	}
	sort.Strings(matches)
	if len(matches) == 1 {
		return New(matches[0], &e)
	}
	return nil
}
func Exists(p string) bool { s, e := os.Stat(p); return e == nil && s.Mode().IsRegular() }

func DiscoveryDirectories(source, stamp string) map[string]bool {
	dir := filepath.Dir(source)
	dirs := map[string]bool{dir: true}
	if !dayDir.MatchString(filepath.Base(dir)) || !dayDir.MatchString(filepath.Base(filepath.Dir(dir))) || !yearDir.MatchString(filepath.Base(filepath.Dir(filepath.Dir(dir)))) {
		return dirs
	}
	at, err := time.Parse(time.RFC3339Nano, stamp)
	if err != nil {
		return dirs
	}
	root := filepath.Dir(filepath.Dir(filepath.Dir(dir)))
	// Transcript directories use local dates, which can differ from UTC by a
	// day. Keep the candidate window bounded; Discover still matches exact IDs
	// and validates the child header before opening any transcript body.
	for offset := -1; offset <= 1; offset++ {
		day := at.UTC().AddDate(0, 0, offset).Format("2006/01/02")
		dirs[filepath.Join(root, filepath.FromSlash(day))] = true
	}
	return dirs
}
