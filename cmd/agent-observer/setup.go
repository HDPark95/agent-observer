package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"time"

	"agent-observer/internal/buildinfo"
	"agent-observer/internal/userconfig"
)

func setupCommand(command string, args []string, out, errOut io.Writer) int {
	if command == "version" || command == "--version" {
		fmt.Fprintf(out, "agent-observer %s (%s) %s %s/%s\n", buildinfo.Version, buildinfo.Revision, runtime.Version(), runtime.GOOS, runtime.GOARCH)
		return 0
	}
	f := flag.NewFlagSet(command, flag.ContinueOnError)
	f.SetOutput(errOut)
	path := f.String("user-config", "", "absolute local configuration path")
	port := f.Int("port", userconfig.DefaultPort, "loopback port")
	root := f.String("catalog-root", "", "optional absolute metadata catalog path")
	consent := f.Bool("allow-catalog", false, "allow metadata discovery under --catalog-root")
	connect := f.Bool("connect", false, "doctor: identify the local HTTP service")
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if f.NArg() != 0 {
		fmt.Fprintln(errOut, "unexpected positional arguments")
		return 2
	}
	var err error
	if command == "init" {
		if *connect || (*root != "") != *consent {
			err = errors.New("init requires --catalog-root and --allow-catalog together; --connect is doctor-only")
		} else {
			err = initialize(*path, userconfig.Config{Version: 1, Port: *port, CatalogRoot: *root}, out)
		}
	} else if flagSet(f, "port") || flagSet(f, "catalog-root") || *consent {
		err = errors.New("doctor reads saved settings; use init to create configuration")
	} else {
		err = diagnose(*path, *connect, out)
	}
	if err != nil {
		fmt.Fprintln(errOut, err)
		return 1
	}
	return 0
}

func initialize(path string, cfg userconfig.Config, out io.Writer) error {
	explicit := path != ""
	if path == "" {
		var err error
		path, err = userconfig.DefaultPath()
		if err != nil {
			return err
		}
	}
	if err := userconfig.SaveNew(path, cfg); err != nil {
		return err
	}
	fmt.Fprintf(out, "Configuration created: %q\nEndpoint: %s\nMetadata catalog enabled: %t\n", path, cfg.Endpoint(), cfg.CatalogRoot != "")
	if explicit {
		fmt.Fprintln(out, "Use the same --user-config option when starting the server and running doctor.")
	}
	fmt.Fprintln(out, "Run ./agent-observer from the extracted folder (or agent-observer if installed on PATH). No sessions were read.")
	return nil
}

func printUsage(flags *flag.FlagSet, out io.Writer) {
	fmt.Fprintln(out, `Usage: agent-observer [command] [options]

Commands:
  (no command)  Start the local viewer; no sessions are selected automatically
  demo          Open the synthetic sample; no account or transcript needed
  init          Create optional saved settings without overwriting existing ones
  doctor        Check settings; add --connect to check the running server
  mcp           Serve plugin tools over stdio; starts the viewer only on request
  version       Show the executable version and platform
  hook          Read a configured provider hook from standard input
  team          Configure or start Observer-managed workers
  message       Send or inspect scoped local messages

From the extracted folder: ./agent-observer demo
Open the printed URL. Press Ctrl-C to stop the server.
Use init --help, doctor --help, team --help, or message --help for command options.

Server options:`)
	flags.PrintDefaults()
}

func diagnose(path string, connect bool, out io.Writer) error {
	cfg, err := userconfig.Load(path)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "PASS configuration\nEndpoint: %s\nMetadata catalog enabled: %t\n", cfg.Endpoint(), cfg.CatalogRoot != "")
	if cfg.CatalogRoot != "" {
		st, err := os.Stat(cfg.CatalogRoot)
		if err != nil || !st.IsDir() {
			return errors.New("FAIL configured catalog directory is unavailable")
		}
		fmt.Fprintln(out, "PASS catalog directory exists (contents not read)")
	}
	fmt.Fprintln(out, "Native message bodies: provider-dependent; encrypted bodies cannot be recovered\nManaged workers / exporters / provider authentication: not checked")
	if !connect {
		fmt.Fprintln(out, "HTTP service: not checked (use --connect)")
		return nil
	}
	return identifyServer(cfg.Endpoint(), out)
}

func identifyServer(endpoint string, out io.Writer) error {
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	resp, err := client.Get(endpoint + "/healthz")
	if err != nil {
		return errors.New("FAIL Observer HTTP service is unavailable")
	}
	defer resp.Body.Close()
	var health struct {
		Product string `json:"product"`
		Version string `json:"version"`
	}
	if resp.StatusCode != http.StatusOK || json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&health) != nil || health.Product != "agent-observer" {
		return errors.New("FAIL endpoint did not identify itself as Agent Observer")
	}
	if len(health.Version) > 128 {
		return errors.New("FAIL invalid service version")
	}
	fmt.Fprintf(out, "PASS service self-identifies as Observer (%q); no session bodies requested\n", health.Version)
	return nil
}

func flagSet(f *flag.FlagSet, name string) bool {
	found := false
	f.Visit(func(v *flag.Flag) {
		if v.Name == name {
			found = true
		}
	})
	return found
}
