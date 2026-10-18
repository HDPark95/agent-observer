// Package projection selects exportable observation metadata from the local graph.
package projection

import (
	"encoding/json"
	"math"
	"time"

	"agent-observer/internal/domain"
	"agent-observer/internal/model"
)

// FromGraph selects a strict field allowlist. It never exports graph labels,
// bodies, arguments, outputs, paths, titles, or arbitrary additional attributes.
// The returned values do not share mutable state with the graph.
func FromGraph(provider, sourceSessionID string, state model.Record) []domain.Observation {
	provider = domain.SafeProvider(provider)
	turn := state.String("turn")
	if provider == "unknown" || sourceSessionID == "" || turn == "" {
		return nil
	}
	run := domain.RunIdentity{Provider: provider, SessionID: sourceSessionID, TurnID: turn}
	started := timestamp(state.String("started"))
	result := []domain.Observation{}
	for _, node := range graphNodes(state["nodes"]) {
		kind := domain.SafeKind(node.String("kind"))
		outcome := nodeOutcome(node)
		if node.String("id") == "" || kind == "unknown" || !settled(kind, outcome) {
			continue
		}
		start, end := executionTimes(node)
		result = append(result, domain.Observation{
			ID: node.String("id"), Run: run, ParentID: node.String("parent"),
			Kind: kind, ToolName: domain.SafeToolName(node.String("toolName")), Outcome: outcome,
			ObservedAt: timestamp(node.String("at")), StartedAt: start, EndedAt: end, TurnStartedAt: started,
		})
	}
	return result
}

func graphNodes(value any) []model.Record {
	if nodes, ok := value.([]model.Record); ok {
		return nodes
	}
	values, _ := value.([]any)
	nodes := make([]model.Record, 0, len(values))
	for _, value := range values {
		if node := model.As(value); len(node) > 0 {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

// settled avoids publishing a partially observed execution as a completed span.
// Point observations such as requests and skill reads have no running interval.
func settled(kind, outcome string) bool {
	switch kind {
	case "call":
		return outcome == "returned" || outcome == "completed" || outcome == "error"
	case "agent", "subagent":
		return outcome == "completed" || outcome == "error"
	default:
		return outcome != "running"
	}
}

func nodeOutcome(node model.Record) string {
	switch node.String("status") {
	case "오류", "failed", "error":
		return "error"
	case "반환 대기", "작업 중", "실행 중", "생성 관측", "추가 작업 관측", "running", "pending", "in_progress":
		return "running"
	case "반환 관측", "호출 반환 관측", "returned":
		return "returned"
	case "완료 관측", "호출 완료 관측", "턴 종료", "completed", "success":
		return "completed"
	case "읽기 성공":
		return "read"
	case "읽기 시도":
		return "read-attempt"
	case "요청 관측", "송신 호출 관측", "observed":
		return "observed"
	default:
		return "unknown"
	}
}

func timestamp(value string) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}
	}
	return parsed.UTC()
}

func executionTimes(node model.Record) (time.Time, time.Time) {
	start, end := milliseconds(node["startMs"]), milliseconds(node["endMs"])
	if !start.IsZero() && !end.IsZero() && end.Before(start) {
		// Contradictory execution timestamps cannot establish an interval.
		return time.Time{}, time.Time{}
	}
	return start, end
}

func milliseconds(value any) time.Time {
	var millis float64
	switch value := value.(type) {
	case float64:
		millis = value
	case int64:
		millis = float64(value)
	case int:
		millis = float64(value)
	case json.Number:
		var err error
		millis, err = value.Float64()
		if err != nil {
			return time.Time{}
		}
	default:
		return time.Time{}
	}
	// Millisecond timestamps must be finite, integral, and fit RFC 3339's year
	// range. Explicit Unix epoch zero is known; a missing value is unknown.
	if math.IsNaN(millis) || math.IsInf(millis, 0) || math.Trunc(millis) != millis || millis < 0 || millis > 253402300799999 {
		return time.Time{}
	}
	return time.UnixMilli(int64(millis)).UTC()
}
