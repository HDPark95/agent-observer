package messagecli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type client struct {
	endpoint string
	token    string
	http     *http.Client
}

func newClient(endpoint, token string) (*client, error) {
	base, err := localEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	if !validToken(token) {
		return nil, errors.New("invalid mailbox credential")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// Loopback credentials never go through an environment-configured HTTP proxy.
	transport.Proxy = nil
	return &client{
		endpoint: base,
		token:    token,
		http: &http.Client{
			Transport: transport,
			Timeout:   5 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

func localEndpoint(endpoint string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "http" || u.User != nil || u.Hostname() != "127.0.0.1" || u.Opaque != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawFragment != "" || (u.Path != "" && u.Path != "/") || u.RawPath != "" {
		return "", errors.New("endpoint must be http://127.0.0.1:<port> with no credentials, path, query, or fragment")
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil || port < 1 || port > 65535 || u.Host != "127.0.0.1:"+strconv.Itoa(port) {
		return "", errors.New("endpoint requires a numeric loopback port between 1 and 65535")
	}
	return "http://" + u.Host, nil
}

func validToken(token string) bool {
	if len(token) == 0 || len(token) > 4096 {
		return false
	}
	for _, b := range []byte(token) {
		if b < 33 || b > 126 {
			return false
		}
	}
	return true
}

func (c *client) close() {
	c.http.CloseIdleConnections()
}

func (c *client) post(ctx context.Context, path string, payload any) (json.RawMessage, error) {
	body, err := json.Marshal(payload)
	if err != nil || len(body) > maxJSONBytes {
		return nil, errors.New("mailbox request exceeds the JSON size limit")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+path, bytes.NewReader(body))
	if err != nil {
		return nil, errors.New("cannot create mailbox request")
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+c.token)
	response, err := c.http.Do(request)
	if err != nil {
		return nil, errors.New("mailbox request failed or timed out")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("mailbox request rejected (HTTP %d)", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil || len(data) > maxResponseBytes || !json.Valid(data) || !strings.HasPrefix(strings.TrimSpace(string(data)), "{") {
		return nil, errors.New("invalid mailbox response")
	}
	return json.RawMessage(data), nil
}
