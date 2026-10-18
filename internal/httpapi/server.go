// Package httpapi exposes the existing loopback-only HTTP contract.
package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"agent-observer/agent_observer"
	"agent-observer/internal/buildinfo"
	"agent-observer/internal/messages"
	"agent-observer/internal/model"
	"agent-observer/internal/sessions"
)

func Handler(registry *sessions.Registry, port string) http.Handler {
	return HandlerWithDelivery(registry, port, nil)
}

// HandlerWithDelivery adds metadata-only delivery status without changing graph reads.
func HandlerWithDelivery(registry *sessions.Registry, port string, deliveryStatus func() any) http.Handler {
	return HandlerWithMessaging(registry, port, deliveryStatus, nil)
}

// HandlerWithMessaging adds an explicitly enabled local transport. Existing
// capture endpoints keep their observational semantics.
func HandlerWithMessaging(registry *sessions.Registry, port string, deliveryStatus func() any, mailbox *Mailbox) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "127.0.0.1:"+port && r.Host != "localhost:"+port {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'")
		if strings.HasPrefix(r.URL.Path, "/api/mailbox/") {
			if mailbox == nil {
				http.Error(w, "Mailbox disabled", http.StatusServiceUnavailable)
				return
			}
			mailbox.serve(w, r, registry, port)
			return
		}
		switch r.Method {
		case http.MethodGet:
			serveGet(w, r, registry, port, deliveryStatus)
		case http.MethodPost:
			servePost(w, r, registry, port)
		default:
			w.Header().Set("Allow", "GET, POST")
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

func allowedOrigin(r *http.Request, port string) bool {
	origin := r.Header.Get("Origin")
	return origin == "" || origin == "http://127.0.0.1:"+port || origin == "http://localhost:"+port
}

func serveGet(w http.ResponseWriter, r *http.Request, registry *sessions.Registry, port string, deliveryStatus func() any) {
	var result model.Record
	var err error
	switch r.URL.Path {
	case "/healthz":
		result = model.Record{"product": "agent-observer", "version": buildinfo.Version}
	case "/api/delivery":
		var status any = model.Record{"enabled": false}
		if deliveryStatus != nil {
			status = deliveryStatus()
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(status)
		return
	case "/":
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(assets.Index)
		return
	case "/api/sessions":
		result = registry.Listing()
	case "/api/events":
		if !allowedOrigin(r, port) {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		streamEvents(w, r, registry)
		return
	case "/api/state":
		result, err = registry.State(r.URL.Query().Get("session"))
	default:
		http.NotFound(w, r)
		return
	}
	if err != nil {
		http.Error(w, "Unknown session", http.StatusNotFound)
		return
	}
	writeJSON(w, result)
}

func readPayload(w http.ResponseWriter, r *http.Request) (model.Record, bool) {
	if r.Header.Get("Content-Type") != "application/json" {
		return nil, false
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 80000))
	if err != nil {
		return nil, false
	}
	var payload model.Record
	if json.Unmarshal(body, &payload) != nil || payload == nil {
		return nil, false
	}
	return payload, true
}

func servePost(w http.ResponseWriter, r *http.Request, registry *sessions.Registry, port string) {
	if !allowedOrigin(r, port) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	payload, ok := readPayload(w, r)
	if !ok {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	var result model.Record
	var err error
	switch r.URL.Path {
	case "/api/sessions":
		result, err = registry.Register(payload, nil)
	case "/api/sessions/open":
		result, err = registry.Open(payload)
	case "/api/messages", "/api/message-hooks":
		result, err = registry.Publish(payload, strings.HasSuffix(r.URL.Path, "message-hooks"))
	default:
		http.NotFound(w, r)
		return
	}
	if err != nil {
		writeAttachmentError(w, err)
		return
	}
	writeJSON(w, result)
}

// A committed-but-unconfirmed attachment is not an invalid request. Preserve
// the identical attachment for retries; never disclose filesystem diagnostics.
func writeAttachmentError(w http.ResponseWriter, err error) {
	if errors.Is(err, messages.ErrDurabilityUnknown) {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "Message body committed; durability unconfirmed. Retry only the identical attachment.", http.StatusServiceUnavailable)
		return
	}
	http.Error(w, "Invalid message attachment", http.StatusBadRequest)
}

func writeJSON(w http.ResponseWriter, result model.Record) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	// A write failure usually means the client disconnected. The response may
	// already be committed, so do not append a second HTTP error response.
	_ = json.NewEncoder(w).Encode(result)
}

func Serve(listener net.Listener, registry *sessions.Registry) error {
	return ServeWithDelivery(listener, registry, nil)
}

func ServeWithDelivery(listener net.Listener, registry *sessions.Registry, deliveryStatus func() any) error {
	return ServeWithMessaging(listener, registry, deliveryStatus, nil)
}

func ServeWithMessaging(listener net.Listener, registry *sessions.Registry, deliveryStatus func() any, mailbox *Mailbox) error {
	_, port, _ := net.SplitHostPort(listener.Addr().String())
	server := http.Server{Handler: HandlerWithMessaging(registry, port, deliveryStatus, mailbox), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second}
	return server.Serve(listener)
}
