// Package messagecli provides a local mailbox client for explicit agent identities.
package messagecli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"agent-observer/internal/messaging"
	"agent-observer/internal/userconfig"
)

const (
	maxJSONBytes     = 80000
	maxResponseBytes = 2 * 1024 * 1024
	maxWait          = 30 * time.Second
	pollInterval     = 250 * time.Millisecond
)

type identity struct {
	Endpoint  string `json:"endpoint"`
	Token     string `json:"token"`
	SessionID string `json:"sessionId"`
	Turn      string `json:"turn"`
	AgentID   string `json:"agentId"`
}

type options struct {
	identityFile string
	endpoint     string
	adminFile    string
	sessionID    string
	turn         string
	agentID      string
	output       string
	id           string
	target       string
	replyTo      string
	transport    string
	bodyStdin    bool
	wait         time.Duration
}

// Run executes enroll, send, receive, or ack. The send operation makes one HTTP
// request; its caller retains the message ID for retries with the same contents.
func Run(args []string, in io.Reader, out, errOut io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(errOut, "usage: agent-observer message <enroll|send|receive|ack> [flags]")
		return 2
	}
	opts, err := parseOptions(args[0], args[1:])
	if err != nil {
		fmt.Fprintln(errOut, err)
		return 2
	}
	if args[0] == "enroll" {
		err = enroll(opts, out)
	} else {
		err = execute(args[0], opts, in, out)
	}
	if err != nil {
		fmt.Fprintln(errOut, err)
		return 1
	}
	return 0
}

func parseOptions(command string, args []string) (options, error) {
	var opts options
	var userConfig string
	flags := flag.NewFlagSet("message", flag.ContinueOnError)
	// Flag errors can contain supplied values. Credentials must never be echoed.
	flags.SetOutput(io.Discard)
	if command == "enroll" {
		flags.StringVar(&userConfig, "user-config", "", "absolute local configuration path")
		flags.StringVar(&opts.endpoint, "endpoint", "", "local Observer endpoint")
		flags.StringVar(&opts.adminFile, "admin-file", "", "absolute admin credential file")
		flags.StringVar(&opts.sessionID, "session", "", "selected session ID")
		flags.StringVar(&opts.turn, "turn", "", "selected turn ID")
		flags.StringVar(&opts.agentID, "agent", "", "graph agent node ID")
		flags.StringVar(&opts.output, "out", "", "new absolute identity file")
	} else {
		flags.StringVar(&opts.identityFile, "identity", "", "absolute identity file")
		switch command {
		case "send":
			flags.StringVar(&opts.id, "id", "", "stable message ID, retained for retry")
			flags.StringVar(&opts.target, "to", "", "recipient graph agent node ID")
			flags.StringVar(&opts.replyTo, "reply-to", "", "original message ID")
			flags.StringVar(&opts.transport, "transport", "mailbox", "explicit transport")
			flags.BoolVar(&opts.bodyStdin, "body-stdin", false, "read message body from stdin")
		case "receive":
			flags.DurationVar(&opts.wait, "wait", 0, "bounded receive wait, at most 30s")
		case "ack":
			flags.StringVar(&opts.id, "id", "", "message ID to acknowledge")
		default:
			return opts, errors.New("unknown message command; use enroll, send, receive, or ack")
		}
	}
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return opts, errors.New("invalid message command flags")
	}
	if command == "enroll" && opts.endpoint == "" {
		cfg, err := userconfig.Load(userConfig)
		if err != nil {
			return opts, err
		}
		opts.endpoint = cfg.Endpoint()
	}
	return opts, validateOptions(command, opts)
}

func validateOptions(command string, opts options) error {
	if command == "enroll" {
		if opts.adminFile == "" || opts.output == "" || !validID(opts.sessionID) || !validID(opts.turn) || !validID(opts.agentID) {
			return errors.New("enroll requires --admin-file, --session, --turn, --agent, and --out")
		}
		return nil
	}
	if opts.identityFile == "" {
		return errors.New("--identity is required")
	}
	switch command {
	case "send":
		if !validID(opts.id) || !validID(opts.target) || !opts.bodyStdin {
			return errors.New("send requires --id, --to, and --body-stdin")
		}
		if opts.replyTo != "" && !validID(opts.replyTo) {
			return errors.New("invalid --reply-to identifier")
		}
		if opts.transport != messaging.MailboxTransport && !messaging.IsManagedTransport(opts.transport) {
			return errors.New("unsupported message transport; use mailbox or managed")
		}
	case "receive":
		if opts.wait < 0 || opts.wait > maxWait {
			return errors.New("--wait must be between 0s and 30s")
		}
	case "ack":
		if !validID(opts.id) {
			return errors.New("ack requires --id")
		}
	}
	return nil
}

func validID(value string) bool {
	return value != "" && len(value) <= 512 && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\r\n\x00")
}

func execute(command string, opts options, in io.Reader, out io.Writer) error {
	id, err := readIdentity(opts.identityFile)
	if err != nil {
		return err
	}
	client, err := newClient(id.Endpoint, id.Token)
	if err != nil {
		return err
	}
	defer client.close()
	var response json.RawMessage
	var request messaging.Request
	switch command {
	case "send":
		body, err := readBody(in)
		if err != nil {
			return err
		}
		request = messaging.Request{ID: opts.id, Target: opts.target, Body: body, ReplyTo: opts.replyTo, Transport: opts.transport}
		response, err = client.post(context.Background(), "/api/mailbox/send", request)
		if err != nil {
			return fmt.Errorf("send: %w; if delivery is uncertain, retry the same ID and contents", err)
		}
	case "receive":
		response, err = client.receive(opts.wait)
	case "ack":
		response, err = client.post(context.Background(), "/api/mailbox/ack", struct {
			ID string `json:"id"`
		}{opts.id})
	}
	if err != nil {
		return err
	}
	return writeResponse(out, response, command, id, request, opts.id)
}

func readBody(in io.Reader) (string, error) {
	body, err := io.ReadAll(io.LimitReader(in, maxJSONBytes+1))
	if err != nil {
		return "", errors.New("cannot read message body from stdin")
	}
	if len(body) > messaging.MaxBodyBytes || !utf8.Valid(body) || utf8.RuneCount(body) > messaging.MaxBodyRunes || strings.TrimSpace(string(body)) == "" {
		return "", errors.New("message body must be valid UTF-8 and contain 1 to 12000 characters")
	}
	return string(body), nil
}

// writeResponse only emits message fields from the protocol, after checking the
// capability scope. Successful but malformed responses must not leak credentials.
func writeResponse(out io.Writer, data json.RawMessage, command string, id identity, request messaging.Request, messageID string) error {
	if command == "receive" {
		var response struct {
			Messages []messaging.Message `json:"messages"`
		}
		if json.Unmarshal(data, &response) != nil || response.Messages == nil {
			return errors.New("invalid mailbox receive response")
		}
		for _, message := range response.Messages {
			if !matchingScope(message, id) || message.Target != id.AgentID {
				return errors.New("mailbox response does not match the enrolled recipient")
			}
		}
		return encodeResponse(out, response)
	}
	var response struct {
		Message messaging.Message `json:"message"`
	}
	if json.Unmarshal(data, &response) != nil || !matchingScope(response.Message, id) || response.Message.ID != messageID {
		return errors.New("mailbox response does not match the requested message scope")
	}
	message := response.Message
	if command == "send" && (message.Source != id.AgentID || message.Target != request.Target || !matchingBody(message, request.Body) || message.ReplyTo != request.ReplyTo || !sameTransport(message.Transport, request.Transport)) {
		return errors.New("mailbox response does not match the submitted message")
	}
	if command == "ack" && (message.Target != id.AgentID || message.AckedAt == nil) {
		return errors.New("mailbox response does not confirm the recipient acknowledgement")
	}
	return encodeResponse(out, response)
}

func matchingScope(message messaging.Message, id identity) bool {
	if message.SessionID != id.SessionID || message.Turn != id.Turn || !validID(message.ID) || !validID(message.Source) || !validID(message.Target) || (message.Transport != messaging.MailboxTransport && !messaging.IsManagedTransport(message.Transport)) || message.AcceptedAt.IsZero() {
		return false
	}
	if message.AckedAt != nil && message.AckedAt.Before(message.AcceptedAt) {
		return false
	}
	if message.BodyExpiredAt != nil {
		return message.Body == "" && message.AckedAt != nil && !message.BodyExpiredAt.Before(*message.AckedAt)
	}
	return true
}

func matchingBody(message messaging.Message, body string) bool {
	// A retry after acknowledged-body retention returns the durable ID receipt.
	// matchingScope already checked the tombstone's acknowledgement and dates.
	return message.Body == body || message.BodyExpiredAt != nil
}

func encodeResponse(out io.Writer, value any) error {
	if json.NewEncoder(out).Encode(value) != nil {
		return errors.New("mailbox operation returned but output could not be written")
	}
	return nil
}

func (c *client) receive(wait time.Duration) (json.RawMessage, error) {
	ctx := context.Background()
	if wait > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, wait)
		defer cancel()
	}
	last := json.RawMessage(`{"messages":[]}`)
	observed := false
	for {
		response, err := c.post(ctx, "/api/mailbox/receive", struct{}{})
		if err != nil {
			if ctx.Err() != nil && observed {
				return last, nil
			}
			return nil, err
		}
		var result struct {
			Messages []json.RawMessage `json:"messages"`
		}
		if json.Unmarshal(response, &result) != nil || result.Messages == nil {
			return nil, errors.New("invalid mailbox receive response")
		}
		if len(result.Messages) > 0 || wait == 0 {
			return response, nil
		}
		last = response
		observed = true
		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return last, nil
		case <-timer.C:
		}
	}
}

// A retry may recover a receipt written with the legacy managed transport name.
func sameTransport(a, b string) bool {
	return a == b || messaging.IsManagedTransport(a) && messaging.IsManagedTransport(b)
}
