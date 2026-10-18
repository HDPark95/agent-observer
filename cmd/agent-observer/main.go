package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"agent-observer/internal/adapters/mailboxview"
	"agent-observer/internal/bootstrap"
	"agent-observer/internal/delivery"
	"agent-observer/internal/hooks"
	"agent-observer/internal/httpapi"
	"agent-observer/internal/messagecli"
	"agent-observer/internal/messaging"
	"agent-observer/internal/model"
	"agent-observer/internal/sessions"
	sourceadapter "agent-observer/internal/sources"
	"agent-observer/internal/userconfig"
	"agent-observer/internal/workers"
)

type paths []string

func (p *paths) String() string     { return fmt.Sprint([]string(*p)) }
func (p *paths) Set(v string) error { *p = append(*p, v); return nil }
func main()                         { os.Exit(run()) }

func run() int {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "mcp":
			return mcpCommand(os.Args[2:])
		case "init", "doctor", "version", "--version":
			return setupCommand(os.Args[1], os.Args[2:], os.Stdout, os.Stderr)
		}
	}

	if len(os.Args) > 1 && os.Args[1] == "message" {
		return messagecli.Run(os.Args[2:], os.Stdin, os.Stdout, os.Stderr)
	}
	if len(os.Args) > 1 && os.Args[1] == "team" {
		return messagecli.RunTeam(os.Args[2:], os.Stdin, os.Stdout, os.Stderr)
	}
	var sources, children paths
	flags := flag.NewFlagSet("agent-observer", flag.ContinueOnError)
	flags.Usage = func() { printUsage(flags, os.Stderr) }
	configPath := flags.String("user-config", "", "absolute local configuration path")
	port := flags.Int("port", userconfig.DefaultPort, "loopback port (1024–65535)")
	root := flags.String("catalog-root", "", "optional metadata-only recent-session catalog")
	provider := flags.String("source-provider", "codex", "codex or claude-code")
	hookOutput := flags.String("hook-output", "", "absolute Claude hook metadata JSONL path")
	exportConfig := flags.String("export-config", "", "explicit metadata export configuration JSON")
	messageStore := flags.String("message-store", "", "opt-in absolute private directory for local message bodies")
	mailboxStore := flags.String("mailbox-store", "", "opt-in absolute private directory for agent message delivery")
	workersConfig := flags.String("workers-config", "", "explicit Observer-managed team configuration")
	flags.Var(&sources, "source", "explicit root transcript; repeatable")
	flags.Var(&children, "agent-source", "explicit child transcript; repeatable")
	hook := len(os.Args) > 1 && os.Args[1] == "hook"
	args := os.Args[1:]
	demo := len(args) > 0 && args[0] == "demo"
	if hook || demo {
		args = args[1:]
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "unexpected positional arguments")
		return 2
	}
	if demo {
		invalid := false
		flags.Visit(func(f *flag.Flag) {
			if f.Name != "port" {
				invalid = true
			}
		})
		if invalid {
			fmt.Fprintln(os.Stderr, "demo accepts only --port; real sources and integrations remain disabled")
			return 2
		}
	} else if !(hook && *provider == "claude-code") {
		cfg, err := userconfig.Load(*configPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		if !flagSet(flags, "port") {
			*port = cfg.Port
		}
		if !flagSet(flags, "catalog-root") {
			*root = cfg.CatalogRoot
		}
	}
	if *port < 1024 || *port > 65535 {
		fmt.Fprintln(os.Stderr, "port must be between 1024 and 65535")
		return 2
	}
	if hook && *provider == "claude-code" {
		if err := sourceadapter.CaptureClaudeHook(os.Stdin, *hookOutput); err != nil {
			fmt.Fprintln(os.Stderr, "cannot collect Claude hook metadata")
			return 1
		}
		return 0
	}
	if *provider != "codex" && *provider != "claude-code" {
		fmt.Fprintln(os.Stderr, "unknown source provider")
		return 2
	}
	if hook {
		hooks.Run(os.Stdin, os.Stdout, *port)
		return 0
	}
	registry, err := sessions.NewWithStore(*root, *messageStore)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	for _, p := range children {
		st, e := os.Stat(p)
		if e != nil || !st.Mode().IsRegular() {
			fmt.Fprintln(os.Stderr, "invalid child transcript")
			return 2
		}
	}
	for _, p := range sources {
		p, err := filepath.Abs(p)
		if err != nil {
			fmt.Fprintln(os.Stderr, "invalid selected transcript")
			return 2
		}
		if _, err := registry.Register(model.Record{"source": p, "provider": *provider}, children); err != nil {
			fmt.Fprintln(os.Stderr, "invalid selected transcript")
			return 2
		}
	}
	var team workerTeam
	if *workersConfig != "" {
		if *mailboxStore == "" {
			fmt.Fprintln(os.Stderr, "managed workers require --mailbox-store")
			return 2
		}
		team, err = loadWorkerTeam(*workersConfig)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		if !team.config.Automatic && registry.ValidateParticipant(team.config.SessionID, team.config.Turn, team.config.Owner) != nil {
			fmt.Fprintln(os.Stderr, "worker configuration must bind an explicitly selected execution and owner")
			return 2
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var collector *delivery.Collector
	if *exportConfig != "" {
		box, err := bootstrap.LoadExports(*exportConfig)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		defer box.Close()
		collector = delivery.NewCollector(registry, box)
	}
	listener, err := net.Listen("tcp4", fmt.Sprintf("127.0.0.1:%d", *port))
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot bind loopback port %d; stop your existing Observer with Ctrl-C or choose another port, for example --port 18767\n", *port)
		return 1
	}
	defer listener.Close()

	var mailbox *httpapi.Mailbox
	if *mailboxStore != "" {
		store, err := messaging.Open(*mailboxStore)
		if err != nil {
			fmt.Fprintln(os.Stderr, "cannot open local mailbox")
			return 2
		}
		defer store.Close()
		transports := map[string]messaging.Transport{messaging.MailboxTransport: store.Mailbox()}
		var feed sessions.MessageFeed = mailboxview.New(store)
		var teams *workers.Teams
		if *workersConfig != "" && team.config.Automatic {
			teams, err = workers.NewTeams(ctx, store, team.config, team.provider, registry)
			if err != nil {
				fmt.Fprintln(os.Stderr, "cannot initialize automatic teams")
				return 2
			}
			defer teams.Close()
			transports[messaging.ManagedTransport], feed = teams, teams
		} else if *workersConfig != "" {
			manager, err := workers.New(ctx, store, team.config, team.provider)
			if err != nil {
				fmt.Fprintln(os.Stderr, "cannot initialize managed workers")
				return 2
			}
			defer manager.Close()
			transports[messaging.ManagedTransport], feed = manager, manager
		}
		service := messaging.NewService(store, transports)
		mailbox = &httpapi.Mailbox{Store: store, Service: service, Teams: teams}
		registry.SetMessageFeed(feed)
	}
	suffix := ""
	if demo {
		suffix = "/?demo=1"
	}
	fmt.Printf("Agent Observer: http://%s%s\n", listener.Addr(), suffix)
	mailboxDone := make(chan struct{})
	if mailbox != nil {
		go func() {
			defer close(mailboxDone)
			maintainMailbox(ctx, mailbox.Store)
		}()
	} else {
		close(mailboxDone)
	}
	var status func() any
	done := make(chan struct{})
	if collector != nil {
		status = collector.Status
		go func() { collector.Run(ctx); close(done) }()
	} else {
		close(done)
	}
	go func() { <-ctx.Done(); listener.Close() }()
	err = httpapi.ServeWithMessaging(listener, registry, status, mailbox)
	stop()
	<-done
	<-mailboxDone
	if err != nil && !errors.Is(err, net.ErrClosed) {
		return 1
	}
	return 0
}

func maintainMailbox(ctx context.Context, store *messaging.Store) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := store.Prune(ctx); err != nil && ctx.Err() == nil {
				fmt.Fprintln(os.Stderr, "mailbox body retention maintenance failed")
			}
		}
	}
}
