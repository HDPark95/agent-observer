package export

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"agent-observer/internal/domain"
)

func example() domain.Observation {
	started := time.Date(2026, 10, 16, 1, 2, 3, 123456000, time.UTC)
	return domain.Observation{ID: "call-1", Run: domain.RunIdentity{Provider: "codex", SessionID: "session-1", TurnID: "turn-1"},
		ParentID: "agent-1", Kind: "call", ToolName: "go", Outcome: "returned", ObservedAt: started.Add(time.Second), TurnStartedAt: started}
}

func TestOTLPWireContract(t *testing.T) {
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/traces" || r.Header.Get("Content-Type") != "application/json" || r.Header.Get("X-Scope-OrgID") != "team-test" {
			t.Errorf("unexpected request: %s %s headers=%v", r.Method, r.URL.Path, r.Header)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		requests = append(requests, string(body))
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{}`)
	}))
	defer server.Close()
	e, err := NewOTLP(OTLPConfig{Endpoint: server.URL + "/v1/traces", Headers: map[string]string{"X-Scope-OrgID": "team-test"}})
	if err != nil {
		t.Fatal(err)
	}
	o := example()
	for i := 0; i < 2; i++ {
		if err := e.Export(context.Background(), []domain.Observation{o}); err != nil {
			t.Fatal(err)
		}
	}
	if len(requests) != 2 || requests[0] != requests[1] {
		t.Fatalf("retry payload changed: %v", requests)
	}
	var payload struct {
		ResourceSpans []struct {
			ScopeSpans []struct {
				Spans []span `json:"spans"`
			} `json:"scopeSpans"`
		} `json:"resourceSpans"`
	}
	if err := json.Unmarshal([]byte(requests[0]), &payload); err != nil {
		t.Fatal(err)
	}
	spans := payload.ResourceSpans[0].ScopeSpans[0].Spans
	if len(spans) != 2 {
		t.Fatalf("spans=%d, want root and observation", len(spans))
	}
	root, child := spans[0], spans[1]
	if root.TraceID != o.TraceID() || root.SpanID != o.Run.SpanID() || child.ParentSpanID != root.SpanID || child.SpanID != o.SpanID() {
		t.Fatal("stable trace hierarchy not preserved")
	}
	if root.Start != root.End || root.Start != strconv.FormatInt(o.TurnStartedAt.UnixNano(), 10) {
		t.Fatalf("root must be immutable point: %+v", root)
	}
	if child.Start != child.End || child.Start != strconv.FormatInt(o.ObservedAt.UnixNano(), 10) {
		t.Fatalf("unknown execution time became duration: %+v", child)
	}
	attrs := attributeMap(child.Attributes)
	if attrs["observer.timing.source"] != "observation" || attrs["observer.graph_parent_id"] == o.ParentID {
		t.Fatalf("incorrect metadata: %v", attrs)
	}
	if child.Status.Code != 0 {
		t.Fatalf("tool return falsely marked success: %d", child.Status.Code)
	}
	for _, raw := range []string{"session-1", "turn-1", "call-1", "agent-1"} {
		if strings.Contains(requests[0], raw) {
			t.Errorf("raw identity leaked: %s", raw)
		}
	}
}

func TestLangfuseMappingAndAuth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || user != "public-test" || password != "secret-test" || r.Header.Get("x-langfuse-ingestion-version") != "4" || r.URL.Path != "/api/public/otel/v1/traces" {
			t.Error("Langfuse auth or endpoint mismatch")
		}
		var payload struct {
			ResourceSpans []struct{ ScopeSpans []struct{ Spans []span } }
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
		}
		for _, s := range payload.ResourceSpans[0].ScopeSpans[0].Spans {
			attrs := attributeMap(s.Attributes)
			if attrs["langfuse.session.id"] == "" || attrs["langfuse.observation.type"] == "" {
				t.Errorf("missing Langfuse mapping: %v", attrs)
			}
		}
		_, _ = io.WriteString(w, `{}`)
	}))
	defer server.Close()
	e, err := NewLangfuse(LangfuseConfig{Endpoint: server.URL + "/api/public/otel/v1/traces", PublicKey: "public-test", SecretKey: "secret-test"})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Export(context.Background(), []domain.Observation{example()}); err != nil {
		t.Fatal(err)
	}
}

func TestLangSmithWireContract(t *testing.T) {
	var payloads []struct {
		Post []run `json:"post"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/runs/batch" || r.Header.Get("x-api-key") != "key-test" || r.Header.Get("x-tenant-id") != "workspace-test" {
			t.Error("LangSmith request mismatch")
		}
		var p struct {
			Post []run `json:"post"`
		}
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			t.Error(err)
		}
		payloads = append(payloads, p)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	e, err := NewLangSmith(LangSmithConfig{Endpoint: server.URL + "/api/v1/runs/batch", APIKey: "key-test", WorkspaceID: "workspace-test", Project: "observer-test"})
	if err != nil {
		t.Fatal(err)
	}
	o := example()
	o.StartedAt, o.EndedAt = o.ObservedAt, o.ObservedAt.Add(2*time.Second)
	o.Outcome = "error"
	for i := 0; i < 2; i++ {
		if err := e.Export(context.Background(), []domain.Observation{o}); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(payloads[0], payloads[1]) {
		t.Fatal("retry IDs or payload changed")
	}
	runs := payloads[0].Post
	if len(runs) != 2 {
		t.Fatalf("runs=%d", len(runs))
	}
	root, child := runs[0], runs[1]
	if child.ID != o.RunID() || child.TraceID != root.ID || child.ParentID != root.ID || child.RunType != "tool" || child.Project != "observer-test" {
		t.Fatalf("incorrect run hierarchy: %+v", child)
	}
	if root.Start != root.End || child.Start != o.StartedAt || child.End != o.EndedAt || child.Error != "observed error" {
		t.Fatal("known timing or error lost")
	}
	if !strings.HasPrefix(child.DottedOrder, root.DottedOrder+".") || !strings.HasPrefix(root.DottedOrder, "20261016T010203123456Z") {
		t.Fatalf("incorrect dotted order: %s", child.DottedOrder)
	}
	if len(child.Inputs) != 0 {
		t.Fatal("inputs must remain empty")
	}
	values := child.Extra["metadata"].(map[string]any)
	if values["observer.timing.source"] != "execution" {
		t.Fatalf("timing source=%v", values)
	}
}

func TestExportMetadataIsAllowlisted(t *testing.T) {
	o := example()
	o.Kind, o.ToolName, o.Outcome, o.Run.Provider = "private-title", "secret-command", "raw-tool-output", "private-provider"
	o.ParentID, o.ID = "private-parent", "private-node"
	e := &OTLP{}
	payload, err := json.Marshal(e.payload([]domain.Observation{o}))
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{o.Kind, o.ToolName, o.Outcome, o.Run.Provider, o.ParentID, o.ID} {
		if strings.Contains(string(payload), forbidden) {
			t.Errorf("untrusted metadata leaked: %s", forbidden)
		}
	}
}

func TestOTLPResponseDecisions(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		rejected   int64
		wantError  bool
	}{
		{"accepted", `{}`, 0, false},
		{"warning", `{"partialSuccess":{"rejectedSpans":"0","errorMessage":"private remote detail"}}`, 0, false},
		{"rejected-string", `{"partialSuccess":{"rejectedSpans":"2","errorMessage":"private remote detail"}}`, 2, true},
		{"rejected-number", `{"partialSuccess":{"rejectedSpans":2}}`, 2, true},
		{"invalid", `not-json`, 0, true},
		{"null", `null`, 0, true},
		{"empty", ``, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := otlpResponse([]byte(tc.body))
			if (err != nil) != tc.wantError {
				t.Fatalf("error=%v", err)
			}
			if err == nil {
				return
			}
			var deliveryError *Error
			if !errors.As(err, &deliveryError) || deliveryError.Retryable() || deliveryError.Rejected != tc.rejected {
				t.Fatalf("decision=%v", err)
			}
			if strings.Contains(err.Error(), "private") {
				t.Fatal("remote body leaked")
			}
		})
	}
}

func TestHTTPFailureDecisionsAndRedirect(t *testing.T) {
	var targetCalls int
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { targetCalls++ }))
	defer target.Close()
	for _, status := range []int{301, 307, 400, 401, 429, 500, 502, 503, 504} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Retry-After", "7")
				w.Header().Set("Location", target.URL)
				w.WriteHeader(status)
				_, _ = io.WriteString(w, "private response")
			}))
			defer server.Close()
			e, err := NewOTLP(OTLPConfig{Endpoint: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			err = e.Export(context.Background(), []domain.Observation{example()})
			var deliveryError *Error
			if !errors.As(err, &deliveryError) || deliveryError.StatusCode != status || deliveryError.Retryable() != retryableStatus(status) || deliveryError.RetryDelay() != 7*time.Second {
				t.Fatalf("decision=%v", err)
			}
			if strings.Contains(err.Error(), "private") || strings.Contains(err.Error(), server.URL) {
				t.Fatal("error exposed remote content")
			}
		})
	}
	if targetCalls != 0 {
		t.Fatalf("followed %d redirects", targetCalls)
	}
}

func TestCancellationAndValidation(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests++; _, _ = io.WriteString(w, `{}`) }))
	defer server.Close()
	e, err := NewOTLP(OTLPConfig{Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := e.Export(ctx, []domain.Observation{example()}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error=%v", err)
	}
	if err := e.Export(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	for _, at := range []time.Time{{}, time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)} {
		o := example()
		o.ObservedAt = at
		if err := e.Export(context.Background(), []domain.Observation{o}); err == nil {
			t.Fatalf("invalid observed timestamp %s accepted", at)
		}
	}
	if requests != 0 {
		t.Fatalf("invalid/canceled/empty input sent %d requests", requests)
	}
	for _, endpoint := range []string{"file:///tmp/test", "https://user:password@example.test/traces", "https://example.test/traces?secret=value"} {
		if _, err := NewOTLP(OTLPConfig{Endpoint: endpoint}); err == nil {
			t.Fatalf("invalid endpoint accepted: %s", endpoint)
		}
	}
}

func TestCancellationStopsInFlightRequest(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()
	e, err := NewOTLP(OTLPConfig{Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() { finished <- e.Export(ctx, []domain.Observation{example()}) }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not start")
	}
	cancel()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("in-flight cancellation error=%v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("export ignored cancellation")
	}
}

func TestEndpointRequiresTLSOutsideLoopback(t *testing.T) {
	for _, tc := range []struct {
		endpoint string
		allowed  bool
	}{
		{"https://example.test/v1/traces", true},
		{"http://127.0.0.1:4318/v1/traces", true},
		{"http://localhost:4318/v1/traces", true},
		{"http://[::1]:4318/v1/traces", true},
		{"http://example.test/v1/traces", false},
		{"http://127.0.0.1.example.test/v1/traces", false},
		{"http://192.168.1.1:4318/v1/traces", false},
	} {
		t.Run(tc.endpoint, func(t *testing.T) {
			_, err := NewOTLP(OTLPConfig{Endpoint: tc.endpoint})
			if (err == nil) != tc.allowed {
				t.Fatalf("allowed=%v, error=%v", tc.allowed, err)
			}
		})
	}
}

func attributeMap(attrs []attribute) map[string]string {
	values := make(map[string]string)
	for _, attr := range attrs {
		values[attr.Key] = attr.Value.Value
	}
	return values
}
