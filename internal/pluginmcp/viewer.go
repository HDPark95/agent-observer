// Package pluginmcp exposes the local viewer through a small stdio MCP adapter.
package pluginmcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"time"

	"agent-observer/internal/httpapi"
	"agent-observer/internal/sessions"
)

// Viewer is confined to the serial MCP request loop. It owns only servers it starts.
type Viewer struct {
	endpoint  string
	server    *http.Server
	connected map[string]bool
	selected  map[string]selection
	client    *http.Client
}

type selection struct{ source, provider string }

func NewViewer() *Viewer {
	return &Viewer{
		connected: make(map[string]bool),
		selected:  make(map[string]selection),
		client: &http.Client{
			Timeout:       3 * time.Second,
			Transport:     &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: time.Second}).DialContext},
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
}

func (v *Viewer) Start(ctx context.Context, port int) (map[string]any, error) {
	if ctx.Err() != nil {
		return nil, errors.New("observer start was cancelled")
	}
	if port != 0 && (port < 1024 || port > 65535) {
		return nil, errors.New("choose port 0 for an available port, or a port between 1024 and 65535")
	}
	if v.endpoint != "" {
		healthy := v.healthy(ctx, v.endpoint)
		if ctx.Err() != nil {
			return nil, errors.New("observer start was cancelled")
		}
		if healthy {
			if port != 0 && v.endpoint != fmt.Sprintf("http://127.0.0.1:%d", port) {
				return nil, errors.New("this MCP connection already selected a different viewer port")
			}
			return v.state(true), nil
		}
		v.reset()
	}
	listener, err := net.Listen("tcp4", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
		if !v.healthy(ctx, endpoint) {
			return nil, errors.New("port is unavailable or is not an Observer service; choose another port")
		}
		v.endpoint = endpoint
		return v.state(true), nil
	}
	return v.serveRegistry(ctx, listener, sessions.New(""))
}

func (v *Viewer) serveRegistry(ctx context.Context, listener net.Listener, registry *sessions.Registry) (map[string]any, error) {
	_, actualPort, _ := net.SplitHostPort(listener.Addr().String())
	v.server = &http.Server{
		Handler:           httpapi.Handler(registry, actualPort),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second,
	}
	v.endpoint = "http://" + listener.Addr().String()
	server := v.server
	go func() { _ = server.Serve(listener) }()
	if !v.healthy(ctx, v.endpoint) {
		v.reset()
		return nil, errors.New("observer did not become ready; retry observer_start")
	}
	return v.state(true), nil
}

func (v *Viewer) Status(ctx context.Context) map[string]any {
	return v.state(v.endpoint != "" && v.healthy(ctx, v.endpoint))
}

func (v *Viewer) state(running bool) map[string]any {
	return map[string]any{
		"running": running,
		"url":     v.endpoint, "ownedByPlugin": v.server != nil,
		"connectedSessions": len(v.connected),
	}
}

func (v *Viewer) reset() {
	_ = v.Close()
	v.server = nil
	v.endpoint = ""
	v.connected = make(map[string]bool)
	v.selected = make(map[string]selection)
}

func (v *Viewer) healthy(ctx context.Context, endpoint string) bool {
	var result struct {
		Product string `json:"product"`
	}
	return v.request(ctx, http.MethodGet, endpoint+"/healthz", nil, &result) == nil && result.Product == "agent-observer"
}

func (v *Viewer) requireRunning(ctx context.Context) error {
	if v.endpoint == "" || !v.healthy(ctx, v.endpoint) {
		return errors.New("observer is not running; call observer_start first")
	}
	return nil
}

func (v *Viewer) Connect(ctx context.Context, source, expectedID, provider string) (map[string]any, error) {
	if !filepath.IsAbs(source) || filepath.Ext(source) != ".jsonl" || len(source) > 8192 || len(expectedID) > 256 {
		return nil, errors.New("provide the absolute JSONL path explicitly selected by the user")
	}
	if provider == "" {
		provider = "codex"
	}
	if provider != "codex" && provider != "claude-code" {
		return nil, errors.New("unsupported source provider")
	}
	if err := v.requireRunning(ctx); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(map[string]string{"source": source, "sessionId": expectedID, "provider": provider})
	if err != nil {
		return nil, errors.New("cannot encode session selection")
	}
	var result struct {
		SessionID string `json:"sessionId"`
	}
	if v.request(ctx, http.MethodPost, v.endpoint+"/api/sessions", payload, &result) != nil || result.SessionID == "" || len(result.SessionID) > 256 {
		return nil, errors.New("cannot connect the selected transcript; check its path, provider, and expected session ID")
	}
	v.connected[result.SessionID] = true
	v.selected[result.SessionID] = selection{source, provider}
	return map[string]any{"sessionId": result.SessionID, "url": v.viewURL(result.SessionID), "connected": true}, nil
}

// View returns a URL for the host's browser tool; it never claims to open a tab.
func (v *Viewer) View(ctx context.Context, id string) (map[string]any, error) {
	if err := v.requireRunning(ctx); err != nil {
		return nil, err
	}
	if id != "" && !v.connected[id] {
		return nil, errors.New("connect this session explicitly before opening its viewer")
	}
	return map[string]any{"url": v.viewURL(id), "browserOpened": false}, nil
}

func (v *Viewer) viewURL(id string) string {
	if id == "" {
		return v.endpoint + "/"
	}
	return v.endpoint + "/?session=" + url.QueryEscape(id)
}

func (v *Viewer) request(ctx context.Context, method, target string, body []byte, result any) error {
	req, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(body))
	if err != nil {
		return errors.New("cannot create local request")
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	response, err := v.client.Do(req)
	if err != nil {
		return errors.New("local Observer request failed")
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 8193))
	if err != nil || response.StatusCode != http.StatusOK || len(data) > 8192 || json.Unmarshal(data, result) != nil {
		return errors.New("invalid local Observer response")
	}
	return nil
}

func (v *Viewer) Close() error {
	v.client.CloseIdleConnections()
	if v.server != nil {
		return v.server.Close()
	}
	return nil
}
