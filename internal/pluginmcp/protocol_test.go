package pluginmcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"
)

const initialize = `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}`

func TestDiscoveryDoesNotStartOrReadSessions(t *testing.T) {
	var output bytes.Buffer
	v := testViewer(t)
	p := protocol{viewer: v, output: json.NewEncoder(&output)}
	for _, frame := range []string{initialize, `{"jsonrpc":"2.0","method":"notifications/initialized"}`, `{"jsonrpc":"2.0","id":2,"method":"tools/list"}`} {
		if err := p.handle(context.Background(), []byte(frame)); err != nil {
			t.Fatal(err)
		}
	}
	if v.endpoint != "" || v.server != nil || len(v.connected) != 0 {
		t.Fatal("discovery caused viewer side effects")
	}
	if strings.Count(output.String(), `"jsonrpc"`) != 2 || !strings.Contains(output.String(), "observer_connect_session") {
		t.Fatalf("unexpected MCP discovery: %s", &output)
	}
}

func TestProtocolErrorsAndArgumentBoundaries(t *testing.T) {
	for _, frame := range []string{
		`{`, `[]`, `{"jsonrpc":"2.0","id":null,"method":"ping"}`,
		`{"jsonrpc":"2.0","id":3,"method":"unknown"}`,
		`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"observer_start","arguments":{"port":-1}}}`,
		`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"observer_start","arguments":{"extra":"PRIVATE_FIXTURE_BODY"}}}`,
		`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"observer_status","arguments":null}}`,
		`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"observer_status","arguments":[]}}`,
	} {
		var output bytes.Buffer
		if err := Serve(context.Background(), strings.NewReader(initialize+"\n"+frame+"\n"), &output); err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(strings.TrimSpace(output.String()), "\n")
		last := lines[len(lines)-1]
		if !strings.Contains(last, `"error"`) && !strings.Contains(last, `"isError":true`) {
			t.Fatalf("error missing for %s: %s", frame, last)
		}
		if strings.Contains(last, "PRIVATE_FIXTURE_BODY") {
			t.Fatal("invalid arguments echoed")
		}
	}
	var output bytes.Buffer
	if err := Serve(context.Background(), strings.NewReader(strings.Repeat("x", 70*1024)), &output); err == nil {
		t.Fatal("oversized frame accepted")
	}
}

func TestOptionalArgumentsCanBeOmitted(t *testing.T) {
	var output bytes.Buffer
	frame := `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"observer_status"}}`
	if err := Serve(context.Background(), strings.NewReader(initialize+"\n"+frame+"\n"), &output); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), `"isError":true`) || !strings.Contains(output.String(), `"running":false`) {
		t.Fatalf("omitted optional arguments rejected: %s", &output)
	}
}

func TestEOFClosesOnlyOwnedViewer(t *testing.T) {
	input, inputWriter := io.Pipe()
	output, outputWriter := io.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- Serve(context.Background(), input, outputWriter)
		_ = outputWriter.Close()
	}()
	t.Cleanup(func() { _ = inputWriter.Close(); _ = output.Close() })
	scanner := bufio.NewScanner(output)
	exchange := func(frame string) map[string]any {
		t.Helper()
		if _, err := io.WriteString(inputWriter, frame+"\n"); err != nil {
			t.Fatal(err)
		}
		if !scanner.Scan() {
			t.Fatal("MCP response missing")
		}
		var value map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	exchange(initialize)
	reply := exchange(`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"observer_start","arguments":{"port":0}}}`)
	state := reply["result"].(map[string]any)["structuredContent"].(map[string]any)
	endpoint := state["url"].(string)
	probe := testViewer(t)
	if !probe.healthy(context.Background(), endpoint) {
		t.Fatal("viewer did not start")
	}
	if err := inputWriter.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("MCP did not stop at EOF")
	}
	if probe.healthy(context.Background(), endpoint) {
		t.Fatal("owned viewer survived EOF")
	}
}
