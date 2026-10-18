package export

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"

	"agent-observer/internal/domain"
)

// OTLPConfig requires the full traces endpoint, including /v1/traces. Tempo
// tenants or collector authentication can be supplied through Headers.
type OTLPConfig struct {
	Endpoint string
	Headers  map[string]string
	Timeout  time.Duration
}

// LangfuseConfig requires the full /api/public/otel/v1/traces endpoint. Values
// come from explicit application configuration, never discovery by the adapter.
type LangfuseConfig struct {
	Endpoint  string
	PublicKey string
	SecretKey string
	Timeout   time.Duration
}

// OTLP sends JSON-encoded OTLP spans without owning retries or background work.
type OTLP struct {
	transport *transport
	langfuse  bool
}

// Validate lets delivery reject one unexportable observation before batching it
// with valid observations. Missing source timestamps stay unknown locally.
func (e *OTLP) Validate(o domain.Observation) error { return validate([]domain.Observation{o}) }

func NewOTLP(config OTLPConfig) (*OTLP, error) {
	t, err := newTransport(config.Endpoint, config.Headers, config.Timeout)
	if err != nil {
		return nil, err
	}
	return &OTLP{transport: t}, nil
}

func NewLangfuse(config LangfuseConfig) (*OTLP, error) {
	if config.PublicKey == "" || config.SecretKey == "" {
		return nil, errors.New("missing Langfuse public or secret key")
	}
	auth := base64.StdEncoding.EncodeToString([]byte(config.PublicKey + ":" + config.SecretKey))
	exporter, err := NewOTLP(OTLPConfig{Endpoint: config.Endpoint, Timeout: config.Timeout, Headers: map[string]string{
		"Authorization":                "Basic " + auth,
		"x-langfuse-ingestion-version": "4",
	}})
	if err != nil {
		return nil, err
	}
	exporter.langfuse = true
	return exporter, nil
}

func (e *OTLP) Export(ctx context.Context, observations []domain.Observation) error {
	if len(observations) == 0 {
		return nil
	}
	if err := validate(observations); err != nil {
		return err
	}
	body, err := e.transport.post(ctx, e.payload(observations))
	if err != nil {
		return err
	}
	return otlpResponse(body)
}

type attribute struct {
	Key   string      `json:"key"`
	Value stringValue `json:"value"`
}

type stringValue struct {
	Value string `json:"stringValue"`
}

type span struct {
	TraceID      string      `json:"traceId"`
	SpanID       string      `json:"spanId"`
	ParentSpanID string      `json:"parentSpanId,omitempty"`
	Name         string      `json:"name"`
	Kind         int         `json:"kind"`
	Start        string      `json:"startTimeUnixNano"`
	End          string      `json:"endTimeUnixNano"`
	Attributes   []attribute `json:"attributes"`
	Status       spanStatus  `json:"status"`
}

type spanStatus struct {
	Code int `json:"code"`
}

func (e *OTLP) payload(observations []domain.Observation) any {
	spans := make([]span, 0, len(observations)+len(roots(observations)))
	for _, o := range roots(observations) {
		values := map[string]string{"observer.run_id": o.Run.Key(), "observer.provider": domain.SafeProvider(o.Run.Provider), "observer.timing.source": "observation"}
		e.langfuseAttributes(values, o, "chain")
		stamp := strconv.FormatInt(o.TurnStartedAt.UnixNano(), 10)
		spans = append(spans, span{TraceID: o.Run.TraceID(), SpanID: o.Run.SpanID(), Name: "agent-observer turn", Kind: 1, Start: stamp, End: stamp, Attributes: attributes(values)})
	}
	for _, o := range observations {
		spans = append(spans, e.observationSpan(o))
	}
	return map[string]any{"resourceSpans": []any{map[string]any{
		"resource":   map[string]any{"attributes": attributes(map[string]string{"service.name": "agent-observer"})},
		"scopeSpans": []any{map[string]any{"scope": map[string]string{"name": "agent-observer"}, "spans": spans}},
	}}}
}

func (e *OTLP) observationSpan(o domain.Observation) span {
	start, end, _ := spanTimes(o)
	values := metadata(o)
	e.langfuseAttributes(values, o, langfuseType(o.Kind))
	code := 0 // Completion or return alone is not proof of successful execution.
	if domain.SafeOutcome(o.Outcome) == "error" {
		code = 2
	}
	return span{TraceID: o.TraceID(), SpanID: o.SpanID(), ParentSpanID: o.Run.SpanID(), Name: observationName(o), Kind: 1,
		Start: strconv.FormatInt(start.UnixNano(), 10), End: strconv.FormatInt(end.UnixNano(), 10), Attributes: attributes(values), Status: spanStatus{Code: code}}
}

func (e *OTLP) langfuseAttributes(values map[string]string, o domain.Observation, kind string) {
	if !e.langfuse {
		return
	}
	values["langfuse.observation.type"] = kind
	values["langfuse.trace.name"] = "agent-observer turn"
	// An empty turn yields one stable session grouping without sending raw IDs.
	session := domain.RunIdentity{Provider: o.Run.Provider, SessionID: o.Run.SessionID}
	values["langfuse.session.id"] = session.Key()
}

func langfuseType(kind string) string {
	switch domain.SafeKind(kind) {
	case "agent", "subagent":
		return "agent"
	case "call", "tool":
		return "tool"
	case "request", "skill", "message", "a2a":
		return "event"
	default:
		return "span"
	}
}

func attributes(values map[string]string) []attribute {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]attribute, 0, len(keys))
	for _, key := range keys {
		result = append(result, attribute{Key: key, Value: stringValue{Value: values[key]}})
	}
	return result
}

func otlpResponse(body []byte) error {
	var response struct {
		Partial *struct {
			Rejected json.Number `json:"rejectedSpans"`
		} `json:"partialSuccess"`
	}
	if !strings.HasPrefix(strings.TrimSpace(string(body)), "{") || json.Unmarshal(body, &response) != nil {
		return &Error{Code: "invalid-response"}
	}
	if response.Partial == nil {
		return nil
	}
	rejected := int64(0)
	if response.Partial.Rejected != "" {
		var err error
		rejected, err = response.Partial.Rejected.Int64()
		if err != nil || rejected < 0 {
			return &Error{Code: "invalid-response"}
		}
	}
	if rejected > 0 {
		// OTLP forbids retrying the whole request after partial acceptance.
		return &Error{Code: "partial-success", StatusCode: 200, Rejected: rejected}
	}
	return nil
}
