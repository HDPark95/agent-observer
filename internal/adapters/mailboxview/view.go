// Package mailboxview projects durable mailbox facts onto the local graph.
package mailboxview

import (
	"crypto/sha256"
	"fmt"
	"strconv"
	"sync"
	"time"

	"agent-observer/internal/agents"
	"agent-observer/internal/messaging"
	"agent-observer/internal/model"
)

type snapshot struct {
	turn     string
	revision uint64
	nodes    []model.Record
}

type Feed struct {
	store *messaging.Store
	mu    sync.Mutex
	views map[string]snapshot
}

func New(store *messaging.Store) *Feed {
	return &Feed{store: store, views: make(map[string]snapshot)}
}

func (f *Feed) Changes() <-chan struct{} { return f.store.Changes() }

// Read preserves immutable node identity until a scoped mailbox update occurs.
func (f *Feed) Read(session, turn string) ([]model.Record, string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	messages, revision, err := f.store.Snapshot(session, turn)
	if err != nil {
		return []model.Record{{"id": "mailbox-storage-error", "kind": "tool", "parent": "agent", "ownerAgent": "agent", "label": "메시지 보관 기록 읽기 실패", "status": "오류", "evidence": "로컬 메시지 저장소를 확인해 주세요."}}, "storage-error"
	}
	view, found := f.views[session]
	if !found || view.turn != turn || view.revision != revision {
		nodes := make([]model.Record, 0, len(messages))
		for _, message := range messages {
			nodes = append(nodes, project(message))
		}
		view = snapshot{turn, revision, nodes}
		f.views[session] = view
	}
	return view.nodes, strconv.FormatUint(revision, 10)
}

func project(message messaging.Message) model.Record {
	// Keep a separate origin namespace: this is not a native call ID.
	id := fmt.Sprintf("mailbox:%x", sha256.Sum256([]byte(message.SessionID+"\x00"+message.Turn+"\x00"+message.ID)))
	status := "전달 수락 · 수신 확인 대기"
	var ack any
	if message.AckedAt != nil {
		status = "수신 확인"
		ack = message.AckedAt.UTC().Format(time.RFC3339Nano)
	}
	body := agents.Body(message.Body)
	var unavailable any
	if message.BodyExpiredAt != nil {
		body = nil
		unavailable = "expired"
	}
	node := model.Record{
		"id": id, "kind": "a2a", "label": "메시지함 대화", "parent": message.Source,
		"source": message.Source, "target": message.Target, "ownerAgent": message.Source,
		"at": message.AcceptedAt.UTC().Format(time.RFC3339Nano), "ackedAt": ack,
		"status": status, "body": body, "bodyUnavailable": unavailable,
		"bodySource": "observer-mailbox", "messageId": message.ID, "replyTo": message.ReplyTo,
		"evidence": "로컬 메시지함의 저장·수신 확인 기록",
		"scope":    "전달 수락은 메시지함 저장입니다. 수신 확인은 상대의 명시적 Ack이며 작업 완료와 다릅니다.",
	}
	if messaging.IsManagedTransport(message.Transport) && message.Delivery != nil {
		d := message.Delivery
		statuses := map[string]string{"queued": "저장됨 · 자동 실행 대기", "dispatching": "실행 요청 중", "running": "실행기 수락 · 작업 중", "completed": "작업자 턴 완료", "unknown": "결과 미확인 · 자동 재전송 중단"}
		node["label"], node["bodySource"] = "작업자 대화", "observer-managed"
		node["status"], node["deliveryState"] = statuses[d.State], d.State
		node["providerThreadId"], node["providerTurnId"] = d.ThreadID, d.TurnID
		node["runtimeProvider"] = d.Provider
		node["outputState"], node["outputReason"] = string(d.OutputState), d.OutputReason
		node["result"], node["deliveryError"] = agents.Body(d.Output), d.Error
		node["updatedAt"] = d.UpdatedAt.UTC().Format(time.RFC3339Nano)
		node["evidence"] = "Observer 단일 송신 도구와 등록된 작업자 실행기의 응답"
		node["scope"] = "본문 저장과 자동 실행을 연결했습니다. 턴 완료는 실행 종료이며 작업의 정확성 판정은 아닙니다."
	}
	return node
}
