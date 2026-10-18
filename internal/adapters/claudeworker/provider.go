// Package claudeworker runs an Observer-owned Claude Code streaming session.
// It never resumes a conversation belonging to another host.
package claudeworker

import (
	"context"

	"agent-observer/internal/workers"
)

// Provider binds the common worker contract to the Claude Code CLI.
func Provider() workers.Provider {
	return workers.Provider{
		ID: "claude-code", Name: "Claude Code", ExecutionLabel: "Claude Code · 메시지 실행",
		Factory: func(ctx context.Context, cfg workers.RuntimeConfig, send workers.SendFunc) (workers.Runtime, error) {
			return Start(ctx, cfg, send)
		},
	}
}
