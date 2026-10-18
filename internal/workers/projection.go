package workers

import (
	"fmt"
	"time"

	"agent-observer/internal/model"
)

// Read is a local-only projection. It never sends input to a worker and never
// enters the external telemetry collector's SelectedSources path.
func (m *Manager) Read(session, turn string) ([]model.Record, string) {
	base, baseRev := m.feed.Read(session, turn)
	if session != m.config.SessionID || turn != m.config.Turn {
		return base, baseRev
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := fmt.Sprintf("%s:workers:%d", baseRev, m.revision)
	if key == m.viewKey {
		return m.view, key
	}
	nodes := projectWorkers(m.config, m.provider, m.states, base)
	m.view, m.viewKey = nodes, key
	return nodes, key
}

// projectWorkers is a pure local projection; worker state and message input
// remain unchanged. Grouping messages once avoids rescanning them per worker.
func projectWorkers(cfg Config, provider Provider, states map[string]workerState, base []model.Record) []model.Record {
	messages := make(map[string][]model.Record)
	for _, message := range base {
		if message.String("bodySource") == "observer-managed" {
			target := message.String("target")
			messages[target] = append(messages[target], message)
		}
	}
	nodes := make([]model.Record, 0, len(base)+len(cfg.Workers))
	nodes = append(nodes, base...)
	for _, d := range cfg.Workers {
		nodes = append(nodes, workerNode(cfg.Owner, provider, d, states[d.ID]))
		for _, message := range messages[d.ID] {
			nodes = append(nodes, executionNode(provider, d, message))
		}
	}
	return nodes
}

func workerNode(owner string, provider Provider, d Definition, state workerState) model.Record {
	at := state.updated.UTC().Format(time.RFC3339Nano)
	return model.Record{"id": d.ID, "kind": "subagent", "label": d.Name, "parent": owner,
		"managedWorker": true, "status": state.status, "at": at, "updatedAt": at,
		"providerThreadId": state.conversationID, "provider": provider.ID, "evidence": "Observer가 생성한 " + provider.Name + " 작업자", "scope": "등록된 실행 범위 안에서 메시지를 받으면 자동 실행합니다.", "currentTask": d.Task}
}

func executionNode(provider Provider, d Definition, message model.Record) model.Record {
	executionLabel := "작업자 실행"
	if provider.ID != "" && message.String("runtimeProvider") == provider.ID {
		executionLabel = provider.ExecutionLabel
	}
	label := d.Task
	if label == "" {
		label = "수신 메시지 검토"
	}
	return model.Record{"id": "managed-execution:" + message.String("id"), "kind": "tool", "label": label,
		"ownerAgent": d.ID, "parent": d.ID, "runtimeProvider": message.String("runtimeProvider"), "toolName": executionLabel, "status": message["status"], "at": message["at"],
		"updatedAt": message["updatedAt"], "evidence": "수신 메시지에 대한 자동 턴 실행 상태", "scope": "작업 이름은 등록된 역할이며 실행 상태는 실제 제공자 응답입니다."}
}
