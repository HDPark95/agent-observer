package main

import (
	"errors"
	"flag"
	"io"

	"agent-observer/internal/hooks"
	"agent-observer/internal/sources"
	"agent-observer/internal/userconfig"
)

// hookCommand is advisory. In particular, exit 2 would block a Codex prompt.
// Keep hook setup failures outside the server's fail-closed CLI error handling.
func hookCommand(args []string, in io.Reader, out, stderr io.Writer) int {
	flags := flag.NewFlagSet("agent-observer hook", flag.ContinueOnError)
	// Flag errors may contain private arguments. Only explicit help goes to stderr.
	flags.SetOutput(io.Discard)
	flags.Usage = func() {}
	configPath := flags.String("user-config", "", "absolute local configuration path")
	port := flags.Int("port", userconfig.DefaultPort, "loopback port (1024–65535)")
	provider := flags.String("source-provider", "codex", "codex or claude-code")
	output := flags.String("hook-output", "", "absolute Claude hook metadata JSONL path")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			flags.SetOutput(stderr)
			flags.PrintDefaults()
		} else {
			hooks.Warn(out)
		}
		return 0
	}
	if flags.NArg() != 0 || (*provider != "codex" && *provider != "claude-code") {
		hooks.Warn(out)
		return 0
	}
	if *provider == "claude-code" {
		if err := sources.CaptureClaudeHook(in, *output); err != nil {
			hooks.Warn(out)
		}
		return 0
	}
	cfg, err := userconfig.Load(*configPath)
	if err != nil {
		hooks.Warn(out)
		return 0
	}
	if !flagSet(flags, "port") {
		*port = cfg.Port
	}
	if *port < 1024 || *port > 65535 {
		hooks.Warn(out)
		return 0
	}
	hooks.Run(in, out, *port)
	return 0
}
