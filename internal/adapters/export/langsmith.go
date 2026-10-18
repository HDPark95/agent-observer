package export

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"agent-observer/internal/domain"
)

// LangSmithConfig requires the full batch endpoint. Project is a configured
// LangSmith project name, not a conversation title or source path.
type LangSmithConfig struct {
	Endpoint    string
	APIKey      string
	WorkspaceID string
	Project     string
	Timeout     time.Duration
}

// LangSmith sends metadata-only run batches with stable IDs for retry/upsert.
// The delivery consumer owns retry policy and whether any export is enabled.
type LangSmith struct {
	transport *transport
	project   string
}

func (e *LangSmith) Validate(o domain.Observation) error { return validate([]domain.Observation{o}) }

func NewLangSmith(config LangSmithConfig) (*LangSmith, error) {
	if config.APIKey == "" || config.Project == "" {
		return nil, errors.New("missing LangSmith API key or project")
	}
	headers := map[string]string{"x-api-key": config.APIKey}
	if config.WorkspaceID != "" {
		headers["x-tenant-id"] = config.WorkspaceID
	}
	t, err := newTransport(config.Endpoint, headers, config.Timeout)
	if err != nil {
		return nil, err
	}
	return &LangSmith{transport: t, project: config.Project}, nil
}

func (e *LangSmith) Export(ctx context.Context, observations []domain.Observation) error {
	if len(observations) == 0 {
		return nil
	}
	if err := validate(observations); err != nil {
		return err
	}
	_, err := e.transport.post(ctx, e.payload(observations))
	var deliveryError *Error
	if errors.As(err, &deliveryError) && (deliveryError.StatusCode == http.StatusRequestTimeout || deliveryError.StatusCode == http.StatusInternalServerError) {
		deliveryError.Retry = true
	}
	return err
}

type run struct {
	ID          string            `json:"id"`
	TraceID     string            `json:"trace_id"`
	ParentID    string            `json:"parent_run_id,omitempty"`
	DottedOrder string            `json:"dotted_order"`
	Name        string            `json:"name"`
	RunType     string            `json:"run_type"`
	Project     string            `json:"session_name"`
	Start       time.Time         `json:"start_time"`
	End         time.Time         `json:"end_time"`
	Inputs      map[string]string `json:"inputs"`
	Extra       map[string]any    `json:"extra"`
	Error       string            `json:"error,omitempty"`
}

func (e *LangSmith) payload(observations []domain.Observation) any {
	runs := make([]run, 0, len(observations)+len(roots(observations)))
	for _, o := range roots(observations) {
		runs = append(runs, e.root(o))
	}
	for _, o := range observations {
		runs = append(runs, e.observationRun(o))
	}
	return map[string]any{"post": runs}
}

func (e *LangSmith) root(o domain.Observation) run {
	id := o.Run.RunID()
	return run{ID: id, TraceID: id, DottedOrder: order(o.TurnStartedAt, id), Name: "agent-observer turn", RunType: "chain", Project: e.project,
		Start: o.TurnStartedAt, End: o.TurnStartedAt, Inputs: map[string]string{},
		Extra: map[string]any{"metadata": map[string]string{"observer.run_id": o.Run.Key(), "observer.provider": domain.SafeProvider(o.Run.Provider), "observer.timing.source": "observation"}}}
}

func (e *LangSmith) observationRun(o domain.Observation) run {
	start, end, _ := spanTimes(o)
	rootID, id := o.Run.RunID(), o.RunID()
	r := run{ID: id, TraceID: rootID, ParentID: rootID, DottedOrder: order(o.TurnStartedAt, rootID) + "." + order(start, id),
		Name: observationName(o), RunType: langsmithType(o.Kind), Project: e.project, Start: start, End: end,
		Inputs: map[string]string{}, Extra: map[string]any{"metadata": metadata(o)}}
	if domain.SafeOutcome(o.Outcome) == "error" {
		r.Error = "observed error"
	}
	return r
}

func langsmithType(kind string) string {
	switch domain.SafeKind(kind) {
	case "tool", "call":
		return "tool"
	default:
		return "chain"
	}
}

func order(at time.Time, id string) string {
	return fmt.Sprintf("%s%06dZ%s", at.UTC().Format("20060102T150405"), at.Nanosecond()/1000, id)
}
