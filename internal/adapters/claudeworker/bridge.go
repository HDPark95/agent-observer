package claudeworker

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"time"
	"unicode/utf8"

	"agent-observer/internal/messaging"
	"agent-observer/internal/workers"
)

const maxRequestBytes = 64 << 10

type activeInput struct {
	ctx    context.Context
	cancel context.CancelFunc
	ready  chan struct{}
	gate   sync.Mutex
	ended  bool
}

// messageBridge exposes only the sender already bound to this worker by its
// manager. Neither the caller nor the MCP arguments can select a sender/scope.
type messageBridge struct {
	token, host string
	server      *http.Server
	cancel      context.CancelFunc
	send        workers.SendFunc
	mu          sync.Mutex
	active      *activeInput
	closed      bool
	once        sync.Once
}

func newMessageBridge(ctx context.Context, send workers.SendFunc) (*messageBridge, error) {
	if send == nil {
		return nil, errors.New("claude messaging sender is unavailable")
	}
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return nil, errors.New("create claude messaging capability")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, errors.New("listen for claude worker messages")
	}
	ctx, cancel := context.WithCancel(ctx)
	b := &messageBridge{token: hex.EncodeToString(secret[:]), host: listener.Addr().String(), cancel: cancel, send: send}
	b.server = &http.Server{Handler: http.HandlerFunc(b.serveHTTP), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8 << 10,
		BaseContext: func(net.Listener) context.Context { return ctx }}
	go func() { _ = b.server.Serve(listener) }()
	go func() { <-ctx.Done(); b.close() }()
	return b, nil
}

func (b *messageBridge) url() string { return "http://" + b.host + "/mcp" }

func (b *messageBridge) close() {
	b.once.Do(func() {
		b.cancel()
		b.mu.Lock()
		b.closed = true
		active := b.active
		b.active = nil
		b.mu.Unlock()
		if active != nil {
			active.finish()
		}
		_ = b.server.Close()
	})
}

func (b *messageBridge) begin(ctx context.Context) *activeInput {
	ctx, cancel := context.WithCancel(ctx)
	a := &activeInput{ctx: ctx, cancel: cancel, ready: make(chan struct{})}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active != nil {
		b.active.cancel()
	}
	if b.closed {
		cancel()
		return a
	}
	b.active = a
	return a
}

func (b *messageBridge) end(a *activeInput) {
	a.finish()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active == a {
		b.active = nil
	}
}

// Cancel first to interrupt an in-flight sender, then join its admission gate.
// Once finish returns no old call can begin or outlive this input's ownership.
func (a *activeInput) finish() {
	a.cancel()
	a.gate.Lock()
	a.ended = true
	a.gate.Unlock()
}

type rpcRequest struct {
	Version string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

func (b *messageBridge) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Host != b.host || r.Header.Get("Origin") != "" {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+b.token)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if r.URL.Path != "/mcp" || r.URL.RawQuery != "" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if !supportedProtocol(r.Header.Get("MCP-Protocol-Version")) {
		http.Error(w, "unsupported protocol version", http.StatusBadRequest)
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBytes))
	if err != nil {
		http.Error(w, "request too large or unreadable", http.StatusRequestEntityTooLarge)
		return
	}
	var req rpcRequest
	if !utf8.Valid(raw) || decodeObject(raw, &req) != nil || req.Version != "2.0" || req.Method == "" {
		rpcError(w, nil, -32600, "invalid request")
		return
	}
	if len(req.ID) == 0 {
		if req.Method == "notifications/initialized" || req.Method == "notifications/cancelled" {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		rpcError(w, nil, -32600, "request ID is required")
		return
	}
	if !validRPCID(req.ID) {
		rpcError(w, nil, -32600, "invalid request ID")
		return
	}
	b.dispatch(w, r, req)
}

func supportedProtocol(version string) bool {
	switch version {
	case "", "2025-03-26", "2025-06-18", "2025-11-25":
		return true
	default:
		return false
	}
}

func validRPCID(raw json.RawMessage) bool {
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

func (b *messageBridge) dispatch(w http.ResponseWriter, r *http.Request, req rpcRequest) {
	var result any
	switch req.Method {
	case "initialize":
		result = map[string]any{"protocolVersion": "2025-06-18", "capabilities": map[string]any{"tools": map[string]any{}},
			"serverInfo": map[string]string{"name": "observer", "version": "1.0.0"}}
	case "ping":
		result = map[string]any{}
	case "tools/list":
		result = map[string]any{"tools": []any{sendToolDefinition()}}
	case "tools/call":
		result = b.call(r.Context(), req.Params)
	default:
		rpcError(w, req.ID, -32601, "method not found")
		return
	}
	rpcJSON(w, map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result})
}

func sendToolDefinition() any { return workers.SendToolSpec() }

func (b *messageBridge) call(ctx context.Context, raw json.RawMessage) any {
	var call struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
		Meta      json.RawMessage `json:"_meta,omitempty"`
	}
	if decodeObject(raw, &call) != nil || call.Name != "observer_send_message" {
		return sendFailure(messaging.ErrInvalid)
	}
	args, err := workers.DecodeSendArguments(call.Arguments)
	if err != nil {
		return sendFailure(err)
	}
	b.mu.Lock()
	active := b.active
	b.mu.Unlock()
	if active == nil {
		return sendFailure(messaging.ErrRecipientUnavailable)
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(active.ctx, cancel)
	defer stop()
	defer cancel()
	select {
	case <-ctx.Done():
		return sendFailure(ctx.Err())
	case <-active.ready:
	}
	active.gate.Lock()
	defer active.gate.Unlock()
	if active.ended {
		return sendFailure(messaging.ErrRecipientUnavailable)
	}
	if err := active.ctx.Err(); err != nil {
		return sendFailure(err)
	}
	if err := ctx.Err(); err != nil {
		return sendFailure(err)
	}
	receipt, err := b.send(ctx, args)
	if err != nil {
		return sendFailure(err)
	}
	return sentMessage(receipt)
}

func decodeObject(raw []byte, target any) error {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] != '{' {
		return errors.New("expected object")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return errors.New("expected one object")
	}
	return nil
}

func rpcError(w http.ResponseWriter, id json.RawMessage, code int, message string) {
	rpcJSON(w, map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": code, "message": message}})
}

func rpcJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}
