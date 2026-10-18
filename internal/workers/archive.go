package workers

import (
	"sort"
	"time"

	"agent-observer/internal/model"
)

type workerHistory struct {
	at        time.Time
	providers map[string]bool
}

// archivedView uses only recorded identities and receipt providers. Today's
// worker template cannot supply historical names, roles, owners, or providers.
func (t *Teams) archivedView(base []model.Record) []model.Record {
	histories := collectWorkerHistory(base)
	ids := make([]string, 0, len(histories))
	for id := range histories {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	nodes := append([]model.Record(nil), base...)
	existing := make(map[string]bool, len(base))
	for _, n := range base {
		existing[n.String("id")] = true
	}
	for _, id := range ids {
		if !existing[id] {
			nodes = append(nodes, historicalWorker(id, histories[id]))
			existing[id] = true
		}
	}
	for _, msg := range base {
		if msg.String("bodySource") != "observer-managed" || !workerID.MatchString(msg.String("target")) {
			continue
		}
		id := "managed-execution:" + msg.String("id")
		if existing[id] {
			continue
		}
		n := executionNode(Provider{}, Definition{ID: msg.String("target")}, msg)
		n["scope"] = "저장된 수신 실행 상태입니다. 과거 작업 이름은 미수집입니다."
		nodes = append(nodes, n)
		existing[id] = true
	}
	return nodes
}

func collectWorkerHistory(base []model.Record) map[string]*workerHistory {
	histories := map[string]*workerHistory{}
	for _, msg := range base {
		if msg.String("bodySource") != "observer-managed" {
			continue
		}
		at, err := time.Parse(time.RFC3339Nano, msg.String("updatedAt"))
		if err != nil {
			at, _ = time.Parse(time.RFC3339Nano, msg.String("at"))
		}
		for _, id := range []string{msg.String("source"), msg.String("target")} {
			if !workerID.MatchString(id) {
				continue
			}
			if histories[id] == nil {
				histories[id] = &workerHistory{providers: map[string]bool{}}
			}
			if histories[id].at.Before(at) {
				histories[id].at = at
			}
		}
		// The receipt describes the recipient, not the sender's runtime.
		if h := histories[msg.String("target")]; h != nil {
			provider := msg.String("runtimeProvider")
			if provider == "" {
				provider = "unknown"
			}
			h.providers[provider] = true
		}
	}
	return histories
}

func historicalWorker(id string, h *workerHistory) model.Record {
	providers := make([]string, 0, len(h.providers))
	for p := range h.providers {
		providers = append(providers, p)
	}
	sort.Strings(providers)
	provider := "unknown"
	if len(providers) == 1 {
		provider = providers[0]
	} else if len(providers) > 1 {
		provider = "mixed"
	}
	n := model.Record{"id": id, "kind": "subagent", "label": id, "managedWorker": true, "archivedWorker": true,
		"status": "저장된 기록 · 실행기 미연결", "provider": provider, "historicalProviders": providers,
		"evidence": "저장된 관리 메시지의 참여자 ID", "scope": "과거 이름·역할·상위 작업자는 미수집입니다. 제공자는 수신 실행 기록에서만 확인합니다."}
	if !h.at.IsZero() {
		n["at"] = h.at.UTC().Format(time.RFC3339Nano)
		n["updatedAt"] = n["at"]
	}
	return n
}
