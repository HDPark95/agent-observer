package export

import (
	"time"

	"agent-observer/internal/domain"
)

func validate(observations []domain.Observation) error {
	starts := make(map[domain.RunIdentity]time.Time)
	for _, o := range observations {
		if o.ID == "" || o.Run.SessionID == "" || o.Run.TurnID == "" || o.ObservedAt.IsZero() || o.TurnStartedAt.IsZero() {
			return &Error{Code: "invalid-observation"}
		}
		for _, at := range []time.Time{o.ObservedAt, o.TurnStartedAt, o.StartedAt, o.EndedAt} {
			if !at.IsZero() && (at.UnixNano() < 0 || !time.Unix(0, at.UnixNano()).Equal(at)) {
				return &Error{Code: "invalid-observation"}
			}
		}
		if !o.StartedAt.IsZero() && !o.EndedAt.IsZero() && o.EndedAt.Before(o.StartedAt) {
			return &Error{Code: "invalid-observation"}
		}
		if previous, ok := starts[o.Run]; ok && !previous.Equal(o.TurnStartedAt) {
			return &Error{Code: "invalid-observation"}
		}
		starts[o.Run] = o.TurnStartedAt
	}
	return nil
}

// spanTimes preserves unknown execution duration as a point observation. The
// timing source travels with the payload so zero does not claim a fast call.
func spanTimes(o domain.Observation) (start, end time.Time, source string) {
	if !o.StartedAt.IsZero() && !o.EndedAt.IsZero() {
		return o.StartedAt, o.EndedAt, "execution"
	}
	return o.ObservedAt, o.ObservedAt, "observation"
}

func metadata(o domain.Observation) map[string]string {
	_, _, timing := spanTimes(o)
	values := map[string]string{
		"observer.run_id":         o.Run.Key(),
		"observer.observation_id": o.Key(),
		"observer.provider":       domain.SafeProvider(o.Run.Provider),
		"observer.kind":           domain.SafeKind(o.Kind),
		"observer.outcome":        domain.SafeOutcome(o.Outcome),
		"observer.timing.source":  timing,
	}
	if tool := domain.SafeToolName(o.ToolName); tool != "" {
		values["observer.tool"] = tool
	}
	if o.ParentID != "" {
		parent := domain.Observation{Run: o.Run, ID: o.ParentID}
		values["observer.graph_parent_id"] = parent.Key()
	}
	return values
}

func observationName(o domain.Observation) string {
	name := domain.SafeKind(o.Kind)
	if tool := domain.SafeToolName(o.ToolName); tool != "" {
		name += ": " + tool
	}
	return name
}

func roots(observations []domain.Observation) []domain.Observation {
	seen := make(map[domain.RunIdentity]bool)
	var result []domain.Observation
	for _, o := range observations {
		if !seen[o.Run] {
			seen[o.Run] = true
			result = append(result, o)
		}
	}
	return result
}
