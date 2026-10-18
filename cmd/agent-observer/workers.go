package main

import (
	"errors"

	"agent-observer/internal/adapters/claudeworker"
	"agent-observer/internal/adapters/codexworker"
	"agent-observer/internal/workers"
)

type workerTeam struct {
	config   workers.Config
	provider workers.Provider
}

// Resolve providers before opening the message store or starting queue listeners.
// Defaulting is confined to the composition root for existing configuration files.
func loadWorkerTeam(path string) (workerTeam, error) {
	cfg, err := workers.LoadConfig(path)
	if err != nil {
		return workerTeam{}, err
	}
	provider, err := selectWorkerProvider(cfg.Provider)
	if err != nil {
		return workerTeam{}, err
	}
	cfg.Provider = provider.ID
	return workerTeam{config: cfg, provider: provider}, nil
}

func selectWorkerProvider(id string) (workers.Provider, error) {
	if id == "" {
		id = "codex"
	}
	providers := map[string]workers.Provider{
		"codex":       codexworker.Provider(),
		"claude-code": claudeworker.Provider(),
	}
	provider, ok := providers[id]
	if !ok {
		return workers.Provider{}, errors.New("unsupported worker provider")
	}
	return provider, nil
}
