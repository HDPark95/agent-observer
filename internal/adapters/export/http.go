// Package export translates metadata-only observations to telemetry APIs.
package export

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
	"strconv"
	"strings"
	"time"
)

const maxResponseBytes = 64 << 10

// Error reports a delivery decision without exposing endpoints, credentials,
// response bodies, or the original transport error's URL.
type Error struct {
	Code       string
	StatusCode int
	Retry      bool
	RetryAfter time.Duration
	Rejected   int64
}

func (e *Error) Error() string {
	return fmt.Sprintf("telemetry export %s (HTTP %d, rejected %d)", e.Code, e.StatusCode, e.Rejected)
}

func (e *Error) Retryable() bool { return e.Retry }

func (e *Error) RetryDelay() time.Duration { return e.RetryAfter }

// Rejected is a span count, including any synthetic turn spans.
// It cannot identify rejected observations.
func (e *Error) PartialAcceptance() (int64, bool) {
	return e.Rejected, e.Code == "partial-success"
}

type transport struct {
	endpoint string
	headers  http.Header
	client   *http.Client
}

func newTransport(endpoint string, headers map[string]string, timeout time.Duration) (*transport, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.Fragment != "" || u.RawQuery != "" {
		return nil, errors.New("export endpoint must be an HTTP(S) URL without credentials, query, or fragment")
	}
	if u.Scheme == "http" && !loopbackHost(u.Hostname()) {
		return nil, errors.New("export endpoint requires HTTPS except for loopback HTTP")
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	h := make(http.Header)
	for key, value := range headers {
		if strings.ContainsAny(key+value, "\r\n") {
			return nil, errors.New("export headers must not contain line breaks")
		}
		h.Set(key, value)
	}
	client := &http.Client{
		Timeout: timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return &transport{endpoint: endpoint, headers: h, client: client}, nil
}

func loopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	address := net.ParseIP(host)
	return address != nil && address.IsLoopback()
}

func (t *transport) post(ctx context.Context, payload any) ([]byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, &Error{Code: "encode"}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, &Error{Code: "request"}
	}
	req.Header = t.headers.Clone()
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, &Error{Code: "transport", Retry: true}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &Error{Code: "http", StatusCode: resp.StatusCode,
			Retry: retryableStatus(resp.StatusCode), RetryAfter: retryDelay(resp.Header.Get("Retry-After"), time.Now())}
	}
	body, err = io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil || len(body) > maxResponseBytes {
		return nil, &Error{Code: "response", StatusCode: resp.StatusCode}
	}
	return body, nil
}

func retryableStatus(status int) bool {
	return status == http.StatusTooManyRequests || status == http.StatusBadGateway ||
		status == http.StatusServiceUnavailable || status == http.StatusGatewayTimeout
}

func retryDelay(value string, now time.Time) time.Duration {
	if seconds, err := strconv.ParseInt(value, 10, 32); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if deadline, err := http.ParseTime(value); err == nil && deadline.After(now) {
		return deadline.Sub(now)
	}
	return 0
}
