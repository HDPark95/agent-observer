package messagecli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"agent-observer/internal/messaging"
	"agent-observer/internal/userconfig"
	"agent-observer/internal/workers"
)

type teamClientConfig struct {
	Endpoint  string `json:"endpoint"`
	AdminFile string `json:"adminFile"`
}

type teamOptions struct {
	config, endpoint, admin, session, id, target string
	bodyStdin                                    bool
}

// RunTeam provides a host-authenticated entry point without per-turn identity
// files. Only the one-time endpoint and credential path are saved, not a token.
func RunTeam(args []string, in io.Reader, out, errOut io.Writer) int {
	if len(args) == 0 || args[0] != "configure" && args[0] != "start" {
		fmt.Fprintln(errOut, "usage: agent-observer team <configure|start> [flags]")
		return 2
	}
	opts, err := parseTeamOptions(args[0], args[1:])
	if err == nil {
		if args[0] == "configure" {
			err = configureTeam(opts, out)
		} else {
			err = startTeam(opts, in, out)
		}
	}
	if err != nil {
		fmt.Fprintln(errOut, err)
		return 1
	}
	return 0
}

func parseTeamOptions(command string, args []string) (teamOptions, error) {
	var opts teamOptions
	var userConfig string
	flags := flag.NewFlagSet("team", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&opts.config, "config", "", "absolute client configuration path")
	if command == "configure" {
		flags.StringVar(&userConfig, "user-config", "", "absolute local configuration path")
		flags.StringVar(&opts.endpoint, "endpoint", "", "local Observer endpoint")
		flags.StringVar(&opts.admin, "admin-file", "", "absolute host-owned admin credential file")
	} else {
		flags.StringVar(&opts.session, "session", os.Getenv("CODEX_THREAD_ID"), "selected session; defaults to CODEX_THREAD_ID")
		flags.StringVar(&opts.id, "id", "", "start ID unique within this session; retain for retries")
		flags.StringVar(&opts.target, "to", "managed:ux", "configured worker")
		flags.BoolVar(&opts.bodyStdin, "body-stdin", false, "read the initial task from stdin")
	}
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return opts, errors.New("invalid team command flags")
	}
	if command == "configure" && opts.endpoint == "" {
		cfg, err := userconfig.Load(userConfig)
		if err != nil {
			return opts, err
		}
		opts.endpoint = cfg.Endpoint()
	}
	if opts.config == "" {
		base, err := os.UserConfigDir()
		if err != nil {
			return opts, errors.New("cannot locate client configuration; supply --config")
		}
		opts.config = filepath.Join(base, "agent-observer", "team-client.json")
	}
	if !filepath.IsAbs(opts.config) {
		return opts, errors.New("team client configuration path must be absolute")
	}
	if command == "start" && !validID(opts.target) {
		return opts, errors.New("team start requires a valid --to worker ID")
	}
	if command == "start" && (!validID(opts.session) || !validID(opts.id) || !opts.bodyStdin) {
		return opts, errors.New("team start requires --id and --body-stdin; use --session outside a Codex session")
	}
	return opts, nil
}

func configureTeam(opts teamOptions, out io.Writer) error {
	cfg := teamClientConfig{Endpoint: opts.endpoint, AdminFile: opts.admin}
	client, err := teamClient(cfg)
	if err != nil {
		return err
	}
	client.close()
	if existing, err := loadTeamConfig(opts.config); err == nil {
		if existing != cfg {
			return errors.New("client configuration exists; use a separate --config path")
		}
		return encodeResponse(out, map[string]bool{"configured": true})
	}
	if err := os.MkdirAll(filepath.Dir(opts.config), 0700); err != nil {
		return errors.New("cannot create client configuration directory")
	}
	file, err := reserveIdentityFile(opts.config)
	if err != nil {
		return err
	}
	defer file.Close()
	if json.NewEncoder(file).Encode(cfg) != nil || file.Sync() != nil {
		return errors.New("cannot save team client configuration")
	}
	return encodeResponse(out, map[string]bool{"configured": true})
}

func loadTeamConfig(path string) (teamClientConfig, error) {
	var cfg teamClientConfig
	data, err := readPrivateFile(path, 8192)
	if err != nil {
		return cfg, errors.New("team client is not configured; run team configure once")
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&cfg) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return cfg, errors.New("invalid team client configuration")
	}
	return cfg, nil
}

func teamClient(cfg teamClientConfig) (*client, error) {
	credential, err := readPrivateFile(cfg.AdminFile, 4097)
	if err != nil {
		return nil, err
	}
	return newClient(cfg.Endpoint, strings.TrimSpace(string(credential)))
}

func startTeam(opts teamOptions, in io.Reader, out io.Writer) error {
	cfg, err := loadTeamConfig(opts.config)
	if err != nil {
		return err
	}
	body, err := readBody(in)
	if err != nil {
		return err
	}
	client, err := teamClient(cfg)
	if err != nil {
		return err
	}
	defer client.close()
	request := workers.StartRequest{SessionID: opts.session, ID: opts.id, Target: opts.target, Body: body}
	data, err := client.post(context.Background(), "/api/mailbox/team/start", request)
	if err != nil {
		return fmt.Errorf("team start: %w; retain the same session, ID, target and body for retry", err)
	}
	var result struct {
		Message messaging.Message `json:"message"`
	}
	if json.Unmarshal(data, &result) != nil {
		return errors.New("invalid team start response")
	}
	m := result.Message
	id := identity{SessionID: opts.session, Turn: m.Turn, AgentID: m.Source}
	if !validID(m.Turn) || !matchingScope(m, id) || m.ID != opts.id || m.Target != opts.target || !matchingBody(m, body) || !messaging.IsManagedTransport(m.Transport) || m.Delivery == nil {
		return errors.New("team response does not match the submitted task")
	}
	// Initial task bodies and credentials do not need to be echoed into a parent
	// tool trace. The local graph can display the durable body on demand.
	return encodeResponse(out, struct {
		ID        string `json:"id"`
		SessionID string `json:"sessionId"`
		Turn      string `json:"turn"`
		Target    string `json:"target"`
		State     string `json:"state"`
		URL       string `json:"url"`
	}{m.ID, m.SessionID, m.Turn, m.Target, m.Delivery.State, client.endpoint + "/?" + url.Values{"session": {m.SessionID}}.Encode()})
}
