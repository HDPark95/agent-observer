package workers

import (
	"context"
	"errors"
	"time"

	"agent-observer/internal/messaging"
)

func runtimeFailureCode(err error) string {
	if errors.Is(err, ErrRuntimeUpgradeRequired) {
		return "runtime-upgrade-required"
	}
	return "runtime-outcome-unknown"
}

func (m *Manager) runRecipient(ctx context.Context, d Definition) {
	var runtime Runtime
	defer func() { closeRuntime(runtime) }()
	for ctx.Err() == nil {
		changes := m.store.Changes()
		message, found, err := m.claim(ctx, d.ID)
		if err != nil {
			return
		}
		if !found {
			select {
			case <-ctx.Done():
				return
			case <-changes:
				continue
			}
		}
		var healthy bool
		runtime, healthy = m.deliverClaimed(ctx, d, runtime, message)
		if !healthy {
			return
		}
	}
}

// claim shares the admission lock with Send, so a failed claim closes that
// recipient to new input before another message can be persisted.
func (m *Manager) claim(ctx context.Context, id string) (messaging.Message, bool, error) {
	m.sendMu.Lock()
	defer m.sendMu.Unlock()
	if m.closed {
		return messaging.Message{}, false, messaging.ErrClosed
	}
	if m.unavailable[id] {
		return messaging.Message{}, false, messaging.ErrRecipientUnavailable
	}
	message, found, err := m.queue.ClaimManaged(ctx, m.principal(id))
	if err != nil && ctx.Err() == nil {
		m.haltRecipientLocked(id, "")
	}
	return message, found, err
}

func (m *Manager) deliverClaimed(ctx context.Context, d Definition, runtime Runtime, message messaging.Message) (Runtime, bool) {
	m.setState(d.ID, "작업 중", "")
	if runtime == nil {
		var err error
		runtime, err = m.startRuntime(ctx, d)
		if err != nil {
			if m.fail(message, Receipt{}, "runtime-start-failed") != nil {
				return nil, false
			}
			m.setState(d.ID, "시작 실패 · 결과 미확인", "")
			return nil, true
		}
	}
	receipt, runErr := m.runMessage(ctx, runtime, message)
	if runErr != nil {
		err := m.fail(message, receipt, runtimeFailureCode(runErr))
		closeRuntime(runtime)
		if err != nil || !m.recipientAvailable(d.ID) {
			return nil, false
		}
		m.setState(d.ID, "결과 미확인 · 다음 입력 대기", receipt.ConversationID)
		return nil, true
	}
	if err := m.complete(ctx, message, receipt); err != nil {
		return runtime, false
	}
	m.setState(d.ID, "완료 · 다음 메시지 대기", receipt.ConversationID)
	return runtime, true
}

func (m *Manager) startRuntime(ctx context.Context, d Definition) (Runtime, error) {
	peers := make([]Peer, 0, len(m.config.Workers))
	for _, worker := range m.config.Workers {
		peers = append(peers, Peer{ID: worker.ID, Name: worker.Name})
	}
	cfg := RuntimeConfig{Executable: m.config.Executable, CWD: m.config.CWD,
		Instructions: d.Instructions, WorkerID: d.ID, Peers: peers}
	return m.provider.Factory(ctx, cfg, m.sendAs(d.ID))
}

func (m *Manager) runMessage(ctx context.Context, runtime Runtime, message messaging.Message) (Receipt, error) {
	turnCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	return runtime.Run(turnCtx, Input{ID: message.ID, Source: message.Source, Body: message.Body,
		OnAccepted: func(receipt Receipt) error {
			delivery := messaging.Delivery{Provider: m.provider.ID, State: "running", ThreadID: receipt.ConversationID, TurnID: receipt.ExecutionID}
			if err := m.persistDelivery(turnCtx, message, delivery); err != nil {
				return err
			}
			m.setState(message.Target, "작업 중", receipt.ConversationID)
			return nil
		}})
}

func (m *Manager) complete(ctx context.Context, message messaging.Message, receipt Receipt) error {
	receipt = boundedReceipt(receipt)
	delivery := messaging.Delivery{Provider: m.provider.ID, State: "completed", ThreadID: receipt.ConversationID, TurnID: receipt.ExecutionID, Output: receipt.Output, OutputState: receipt.OutputState, OutputReason: receipt.OutputReason}
	if err := m.persistDelivery(ctx, message, delivery); err != nil {
		// Retain uncertainty if possible, but do not resume the recipient even
		// when this fallback write succeeds. The completion was not durable.
		if failErr := m.fail(message, receipt, "completion-storage-error"); failErr != nil {
			return failErr
		}
		return err
	}
	return nil
}

func (m *Manager) persistDelivery(ctx context.Context, message messaging.Message, delivery messaging.Delivery) error {
	m.sendMu.Lock()
	defer m.sendMu.Unlock()
	err := m.queue.UpdateDelivery(ctx, m.principal(message.Target), message.ID, delivery)
	if err != nil {
		m.haltRecipientLocked(message.Target, delivery.ThreadID)
	}
	return err
}

func (m *Manager) haltRecipientLocked(id, thread string) {
	m.unavailable[id] = true
	m.setState(id, "전달 중단 · 저장소 오류 · 새 메시지 수락 안 함", thread)
}

func (m *Manager) recipientAvailable(id string) bool {
	m.sendMu.Lock()
	defer m.sendMu.Unlock()
	return !m.unavailable[id]
}

func closeRuntime(runtime Runtime) {
	if runtime != nil {
		_ = runtime.Close()
	}
}

func (m *Manager) fail(message messaging.Message, receipt Receipt, code string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return m.persistDelivery(ctx, message, messaging.Delivery{Provider: m.provider.ID, State: "unknown", ThreadID: receipt.ConversationID, TurnID: receipt.ExecutionID, Error: code})
}
