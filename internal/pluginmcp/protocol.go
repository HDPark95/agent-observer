package pluginmcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	"agent-observer/internal/buildinfo"
)

type request struct {
	Version string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

type protocol struct {
	viewer      *Viewer
	teams       *teamHost
	output      *json.Encoder
	initialized bool
}

// Serve reads newline-delimited JSON-RPC. Closing stdin ends plugin-owned servers.
func Serve(ctx context.Context, input io.Reader, output io.Writer) error {
	p := protocol{viewer: NewViewer(), teams: newTeamHost(ctx), output: json.NewEncoder(output)}
	defer p.viewer.Close()
	defer p.teams.Close()
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 4096), 64*1024)
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := p.handle(ctx, scanner.Bytes()); err != nil {
			return err
		}
	}
	if scanner.Err() != nil {
		return errors.New("cannot read MCP input; messages must fit within 64 KiB")
	}
	return nil
}

func (p *protocol) handle(ctx context.Context, data []byte) error {
	var req request
	if !json.Valid(data) {
		return p.failure(nil, -32700, "Invalid JSON")
	}
	if json.Unmarshal(data, &req) != nil || req.Version != "2.0" || req.Method == "" || !validID(req.ID) {
		return p.failure(nil, -32600, "Invalid request")
	}
	if len(req.ID) == 0 { // Notifications never receive responses.
		return nil
	}
	if req.Method == "initialize" {
		return p.initialize(req)
	}
	if !p.initialized {
		return p.failure(req.ID, -32000, "Initialize the MCP connection first")
	}
	switch req.Method {
	case "ping":
		return p.result(req.ID, map[string]any{})
	case "tools/list":
		return p.result(req.ID, map[string]any{"tools": toolDefinitions()})
	case "tools/call":
		return p.call(ctx, req)
	default:
		return p.failure(req.ID, -32601, "Method not found")
	}
}

func validID(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return true
	}
	var value any
	if json.Unmarshal(raw, &value) != nil {
		return false
	}
	switch value.(type) {
	case string, float64:
		return true
	default:
		return false
	}
}

func (p *protocol) initialize(req request) error {
	var params struct {
		ProtocolVersion string `json:"protocolVersion"`
	}
	if json.Unmarshal(req.Params, &params) != nil || params.ProtocolVersion == "" {
		return p.failure(req.ID, -32602, "Invalid initialization parameters")
	}
	version := params.ProtocolVersion
	if version != "2025-03-26" && version != "2025-06-18" && version != "2025-11-25" {
		version = "2025-06-18"
	}
	p.initialized = true
	return p.result(req.ID, map[string]any{
		"protocolVersion": version, "capabilities": map[string]any{"tools": map[string]any{}},
		"serverInfo": map[string]string{"name": "agent-observer", "version": buildinfo.Version},
	})
}

func decodeArguments(raw json.RawMessage, target any) error {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		raw = []byte("{}")
	}
	if raw[0] != '{' {
		return errors.New("arguments must be an object")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func (p *protocol) call(ctx context.Context, req request) error {
	var params struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if json.Unmarshal(req.Params, &params) != nil {
		return p.failure(req.ID, -32602, "Invalid tool parameters")
	}
	result, err := p.invoke(ctx, params.Name, params.Arguments)
	if err != nil {
		return p.result(req.ID, map[string]any{"isError": true, "content": []any{map[string]string{"type": "text", "text": err.Error()}}})
	}
	text, err := json.Marshal(result)
	if err != nil {
		return p.failure(req.ID, -32603, "Cannot encode tool result")
	}
	return p.result(req.ID, map[string]any{"structuredContent": result, "content": []any{map[string]string{"type": "text", "text": string(text)}}})
}

func (p *protocol) invoke(ctx context.Context, name string, raw json.RawMessage) (map[string]any, error) {
	invalid := errors.New("invalid tool arguments; use the documented input schema")
	switch name {
	case "observer_team_create", "observer_team_send", "observer_team_status", "observer_team_stop":
		return p.invokeTeam(ctx, name, raw)
	case "observer_start":
		var args struct {
			Port *int `json:"port"`
		}
		if decodeArguments(raw, &args) != nil {
			return nil, invalid
		}
		port := 8767
		if p.viewer.endpoint != "" {
			port = 0
		}
		if args.Port != nil {
			port = *args.Port
		}
		return p.viewer.Start(ctx, port)
	case "observer_connect_session":
		var args struct {
			Source    string `json:"source"`
			SessionID string `json:"session_id"`
			Provider  string `json:"provider"`
		}
		if decodeArguments(raw, &args) != nil {
			return nil, invalid
		}
		return p.viewer.Connect(ctx, args.Source, args.SessionID, args.Provider)
	case "observer_open_view":
		var args struct {
			SessionID string `json:"session_id"`
		}
		if decodeArguments(raw, &args) != nil {
			return nil, invalid
		}
		return p.viewer.View(ctx, args.SessionID)
	case "observer_status":
		if decodeArguments(raw, &struct{}{}) != nil {
			return nil, invalid
		}
		return p.viewer.Status(ctx), nil
	default:
		return nil, errors.New("unknown Observer tool")
	}
}

func (p *protocol) result(id json.RawMessage, result any) error {
	return p.output.Encode(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}

func (p *protocol) failure(id json.RawMessage, code int, message string) error {
	return p.output.Encode(map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": code, "message": message}})
}
