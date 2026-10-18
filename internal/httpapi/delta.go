package httpapi

import (
	"reflect"

	"agent-observer/internal/model"
)

// streamDelta owns only references to immutable published records. The baseline
// is private to one connection; reconnects and turn changes send a snapshot.
type streamDelta struct {
	previous model.Record
	sequence int
}

func (d *streamDelta) next(state model.Record) (string, model.Record) {
	d.sequence++
	if d.previous == nil || !reflect.DeepEqual(d.previous["turn"], state["turn"]) {
		d.previous = state
		full := state.Clone()
		full["sequence"] = d.sequence
		return "state", full
	}
	old := d.previous["nodes"].([]model.Record)
	nodes := state["nodes"].([]model.Record)
	before := make(map[string]model.Record, len(old))
	for _, n := range old {
		before[n.String("id")] = n
	}
	present := make(map[string]bool, len(nodes))
	upsert := []model.Record{}
	added := []string{}
	order := make([]string, 0, len(nodes))
	for _, n := range nodes {
		id := n.String("id")
		present[id] = true
		order = append(order, id)
		prev, exists := before[id]
		if !exists {
			added = append(added, id)
		}
		// DeepEqual short-circuits identical map references, and handles body overlay
		// copies without repeatedly sending their unchanged contents.
		if !exists || !reflect.DeepEqual(prev, n) {
			upsert = append(upsert, n)
		}
	}
	removed := []string{}
	expected := make([]string, 0, len(nodes))
	for _, n := range old {
		id := n.String("id")
		if present[id] {
			expected = append(expected, id)
		} else {
			removed = append(removed, id)
		}
	}
	expected = append(expected, added...)
	metadata := state.Clone()
	delete(metadata, "nodes")
	patch := model.Record{"sessionId": state["sessionId"], "baseSequence": d.sequence - 1, "sequence": d.sequence, "metadata": metadata, "upsert": upsert, "removed": removed}
	// Ordinary append/update/delete never sends all node IDs. Reordering is rare
	// (e.g. a late child transcript) and needs an explicit order to remain exact.
	if !reflect.DeepEqual(expected, order) {
		patch["order"] = order
	}
	d.previous = state
	return "delta", patch
}
