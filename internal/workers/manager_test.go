package workers

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"agent-observer/internal/messaging"
	"agent-observer/internal/model"
)

type runInvocation struct {
	message  Input
	complete chan error
}

type testRuntime struct {
	id     string
	send   SendFunc
	runs   chan runInvocation
	starts atomic.Int32
	closes atomic.Int32
}

func (r *testRuntime) conversationID() string { return "conversation-" + r.id }

func (r *testRuntime) Run(ctx context.Context, message Input) (Receipt, error) {
	r.starts.Add(1)
	receipt := Receipt{ConversationID: r.conversationID(), ExecutionID: "turn-" + message.ID}
	if err := message.OnAccepted(receipt); err != nil {
		return receipt, err
	}
	invocation := runInvocation{message: message, complete: make(chan error, 1)}
	select {
	case r.runs <- invocation:
	case <-ctx.Done():
		return receipt, ctx.Err()
	}
	select {
	case err := <-invocation.complete:
		receipt.Output = "작업을 확인했습니다."
		return receipt, err
	case <-ctx.Done():
		return receipt, ctx.Err()
	}
}

func (r *testRuntime) Close() error {
	r.closes.Add(1)
	return nil
}

type managerFixture struct {
	manager  *Manager
	store    *messaging.Store
	cfg      Config
	owner    messaging.Principal
	runtimes map[string]*testRuntime
	created  chan string
}

func newManagerFixture(t *testing.T) managerFixture {
	t.Helper()
	return newManagerFixtureWithQueue(t, nil)
}

func newManagerFixtureWithQueue(t *testing.T, wrap func(*messaging.Store) recipientQueue) managerFixture {
	t.Helper()
	return newManagerFixtureWithProvider(t, wrap, testProvider)
}

func newManagerFixtureWithProvider(t *testing.T, wrap func(*messaging.Store) recipientQueue, providerFor func(Factory) Provider) managerFixture {
	t.Helper()
	store, err := messaging.Open(filepath.Join(t.TempDir(), "mailbox"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{SessionID: "session-one", Turn: "turn-one", Owner: "agent:main", Executable: executable, CWD: t.TempDir(),
		Workers: []Definition{{ID: "managed:ux", Name: "UX", Task: "메시지 UX 검토"}, {ID: "managed:dx", Name: "DX", Task: "SSE 테스트 실행"}}}
	owner := messaging.Principal{SessionID: cfg.SessionID, Turn: cfg.Turn, AgentID: cfg.Owner}
	if _, err := store.Enroll(context.Background(), owner); err != nil {
		t.Fatal(err)
	}
	runtimes, created, factory := newTestFactory()
	queue := recipientQueue(store)
	if wrap != nil {
		queue = wrap(store)
	}
	manager, err := newManager(context.Background(), store, queue, cfg, providerFor(factory))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(manager.Close)
	return managerFixture{manager: manager, store: store, cfg: cfg, owner: owner, runtimes: runtimes, created: created}
}

func newTestFactory() (map[string]*testRuntime, chan string, Factory) {
	runtimes := map[string]*testRuntime{
		"managed:ux": {id: "ux", runs: make(chan runInvocation, 8)},
		"managed:dx": {id: "dx", runs: make(chan runInvocation, 8)},
	}
	created := make(chan string, 8)
	factory := func(ctx context.Context, cfg RuntimeConfig, send SendFunc) (Runtime, error) {
		runtime := runtimes[cfg.WorkerID]
		if runtime == nil {
			return nil, errors.New("unknown configured worker")
		}
		runtime.send = send
		created <- cfg.WorkerID
		return runtime, nil
	}
	return runtimes, created, factory
}

func testProvider(factory Factory) Provider {
	return Provider{ID: "test-runtime", Name: "Test Runtime", ExecutionLabel: "테스트 실행", Factory: factory}
}

func sendRequest(t *testing.T, f managerFixture, id, target, body string) messaging.Message {
	t.Helper()
	message, err := f.manager.Send(context.Background(), f.owner, messaging.Request{ID: id, Target: target, Body: body, Transport: messaging.ManagedTransport})
	if err != nil {
		t.Fatal(err)
	}
	return message
}

func awaitRun(t *testing.T, runtime *testRuntime) runInvocation {
	t.Helper()
	select {
	case run := <-runtime.runs:
		return run
	case <-time.After(3 * time.Second):
		t.Fatal("managed worker was not automatically started")
		return runInvocation{}
	}
}

func awaitDelivery(t *testing.T, f managerFixture, id, state string) messaging.Message {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for {
		changes := f.store.Changes()
		messages, _, _ := f.store.Snapshot(f.cfg.SessionID, f.cfg.Turn)
		for _, message := range messages {
			if message.ID == id && message.Delivery.State == state {
				return message
			}
		}
		select {
		case <-changes:
		case <-ctx.Done():
			t.Fatalf("message %s did not reach %s", id, state)
		}
	}
}

func TestManagerAutomaticallyRunsIdleRecipientAndSerializesQueuedInput(t *testing.T) {
	f := newManagerFixture(t)
	ux := f.runtimes["managed:ux"]
	if len(f.created) != 0 {
		t.Fatal("idle manager created a runtime before input")
	}
	sendRequest(t, f, "first", "managed:ux", "첫 번째 작업입니다.")
	first := awaitRun(t, ux)
	if first.message.ID != "first" || first.message.Source != f.cfg.Owner || first.message.Body != "첫 번째 작업입니다." {
		t.Fatalf("delivered input differs from stored message: %+v", first.message)
	}
	sendRequest(t, f, "second", "managed:ux", "두 번째 작업입니다.")
	awaitDelivery(t, f, "second", "queued")
	if got := ux.starts.Load(); got != 1 {
		t.Fatalf("active worker started %d turns; want only first turn", got)
	}
	first.complete <- nil
	second := awaitRun(t, ux)
	if second.message.ID != "second" || second.message.Body != "두 번째 작업입니다." {
		t.Fatalf("second queued input = %+v", second.message)
	}
	awaitDelivery(t, f, "first", "completed")
	second.complete <- nil
	awaitDelivery(t, f, "second", "completed")
	if len(f.created) != 1 || ux.starts.Load() != 2 {
		t.Fatalf("created %d runtimes and started %d turns; want one runtime/two turns", len(f.created), ux.starts.Load())
	}
	f.manager.Close()
	if ux.closes.Load() != 1 {
		t.Fatalf("closed runtime %d times; want once", ux.closes.Load())
	}
}

func TestManagerCallbackStoresBodyAndWakesPeerWithoutReceive(t *testing.T) {
	f := newManagerFixture(t)
	ux, dx := f.runtimes["managed:ux"], f.runtimes["managed:dx"]
	sendRequest(t, f, "owner-to-ux", "managed:ux", "메시지 화면을 검토해주세요.")
	uxRun := awaitRun(t, ux)
	const body = "UX 확인: 연결선을 누르면 본문이 보여야 합니다.\nDX에서 SSE 반영을 확인해주세요."
	if _, err := ux.send(context.Background(), Outbound{Target: "managed:dx", Body: body, ID: "ux-to-dx"}); err != nil {
		t.Fatal(err)
	}
	dxRun := awaitRun(t, dx)
	if dxRun.message.Source != "managed:ux" || dxRun.message.Body != body {
		t.Fatalf("peer input = %+v; sender or exact body was lost", dxRun.message)
	}
	message := awaitDelivery(t, f, "ux-to-dx", "running")
	if message.Source != "managed:ux" || message.Target != "managed:dx" || message.Body != body || message.AckedAt != nil {
		t.Fatalf("stored peer message differs: %+v", message)
	}
	if _, err := dx.send(context.Background(), Outbound{Target: "managed:ux", Body: "SSE 반영 확인했습니다.", ID: "dx-to-ux"}); err != nil {
		t.Fatal(err)
	}
	awaitDelivery(t, f, "dx-to-ux", "queued")
	if ux.starts.Load() != 1 {
		t.Fatal("reply interrupted active UX turn instead of waiting")
	}
	uxRun.complete <- nil
	uxReply := awaitRun(t, ux)
	if uxReply.message.Source != "managed:dx" || uxReply.message.Body != "SSE 반영 확인했습니다." {
		t.Fatalf("automatic reply = %+v", uxReply.message)
	}
	dxRun.complete <- nil
	uxReply.complete <- nil
	awaitDelivery(t, f, "ux-to-dx", "completed")
	awaitDelivery(t, f, "dx-to-ux", "completed")
}

func TestManagerCompletedDuplicateAndRestartDoNotRunAgain(t *testing.T) {
	f := newManagerFixture(t)
	ux := f.runtimes["managed:ux"]
	sendRequest(t, f, "once", "managed:ux", "한 번만 수행해주세요.")
	run := awaitRun(t, ux)
	run.complete <- nil
	awaitDelivery(t, f, "once", "completed")
	message := sendRequest(t, f, "once", "managed:ux", "한 번만 수행해주세요.")
	if message.Delivery.State != "completed" {
		t.Fatalf("retry delivery = %+v; want existing completed receipt", message.Delivery)
	}
	f.manager.Close()
	if ux.starts.Load() != 1 {
		t.Fatalf("duplicate started %d turns", ux.starts.Load())
	}
	runtimes, created, factory := newTestFactory()
	restarted, err := New(context.Background(), f.store, f.cfg, testProvider(factory))
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if _, err := restarted.Send(context.Background(), f.owner, messaging.Request{ID: "once", Target: "managed:ux", Body: "한 번만 수행해주세요.", Transport: messaging.ManagedTransport}); err != nil {
		t.Fatal(err)
	}
	restarted.Close()
	if len(created) != 0 || runtimes["managed:ux"].starts.Load() != 0 {
		t.Fatal("completed retry launched a runtime after manager restart")
	}
}

func TestManagerRejectsOtherScopeAndHidesWorkerProjection(t *testing.T) {
	f := newManagerFixture(t)
	request := messaging.Request{ID: "wrong-scope", Target: "managed:ux", Body: "isolated", Transport: messaging.ManagedTransport}
	for _, source := range []messaging.Principal{
		{SessionID: "other-session", Turn: f.cfg.Turn, AgentID: f.cfg.Owner},
		{SessionID: f.cfg.SessionID, Turn: "other-turn", AgentID: f.cfg.Owner},
		{SessionID: f.cfg.SessionID, Turn: f.cfg.Turn, AgentID: "unknown:agent"},
	} {
		if _, err := f.manager.Send(context.Background(), source, request); !errors.Is(err, messaging.ErrForbidden) {
			t.Fatalf("wrong scope send error = %v; want forbidden", err)
		}
	}
	request.Target = "managed:missing"
	if _, err := f.manager.Send(context.Background(), f.owner, request); !errors.Is(err, messaging.ErrForbidden) {
		t.Fatalf("unconfigured recipient error = %v; want forbidden", err)
	}
	for _, scope := range [][2]string{{"other-session", f.cfg.Turn}, {f.cfg.SessionID, "other-turn"}} {
		nodes, _ := f.manager.Read(scope[0], scope[1])
		if len(nodes) != 0 {
			t.Fatalf("managed nodes leaked to %v: %v", scope, nodes)
		}
	}
	if len(f.created) != 0 {
		t.Fatal("rejected messages launched a worker")
	}
}

func TestManagerCloseCancelsActiveRunAndPreservesUnknownReceipt(t *testing.T) {
	f := newManagerFixture(t)
	ux := f.runtimes["managed:ux"]
	sendRequest(t, f, "interrupted", "managed:ux", "작업을 시작해주세요.")
	run := awaitRun(t, ux)
	closed := make(chan struct{})
	go func() { f.manager.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("manager.Close did not stop active runtime")
	}
	message := awaitDelivery(t, f, run.message.ID, "unknown")
	if message.Delivery.ThreadID != ux.conversationID() || message.Delivery.TurnID != "turn-interrupted" || ux.closes.Load() != 1 {
		t.Fatalf("shutdown receipt = %+v, closes = %d", message.Delivery, ux.closes.Load())
	}
	f.manager.Close()
	if ux.closes.Load() != 1 {
		t.Fatal("repeated manager.Close closed runtime again")
	}
	if _, err := f.manager.Send(context.Background(), f.owner, messaging.Request{ID: "after-close", Target: "managed:ux", Body: "must not queue", Transport: messaging.ManagedTransport}); !errors.Is(err, messaging.ErrClosed) {
		t.Fatalf("send after Close error = %v; want closed", err)
	}
	messages, _, _ := f.store.Snapshot(f.cfg.SessionID, f.cfg.Turn)
	if len(messages) != 1 {
		t.Fatal("closed manager accepted another message")
	}
}

func TestManagerRuntimeStartFailureIsTerminalAndNotReplayed(t *testing.T) {
	f := newManagerFixture(t)
	f.manager.Close()
	var starts atomic.Int32
	factory := func(context.Context, RuntimeConfig, SendFunc) (Runtime, error) {
		starts.Add(1)
		return nil, errors.New("runtime unavailable")
	}
	manager, err := New(context.Background(), f.store, f.cfg, testProvider(factory))
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	f.manager = manager
	sendRequest(t, f, "startup-failure", "managed:ux", "실행해주세요.")
	message := awaitDelivery(t, f, "startup-failure", "unknown")
	if message.Delivery.Error != "runtime-start-failed" {
		t.Fatalf("startup error = %q", message.Delivery.Error)
	}
	sendRequest(t, f, "startup-failure", "managed:ux", "실행해주세요.")
	manager.Close()
	if starts.Load() != 1 {
		t.Fatalf("failed startup was retried %d times", starts.Load())
	}
}

// faultQueue injects failures at the manager's durable boundary while keeping
// real message admission, receipt storage, and change notifications intact.
type faultQueue struct {
	*messaging.Store
	mu          sync.Mutex
	claimTarget string
	failStates  map[string]bool
	updates     []string
}

var errQueueFault = errors.New("injected persistence failure")

func (q *faultQueue) ClaimManaged(ctx context.Context, principal messaging.Principal) (messaging.Message, bool, error) {
	q.mu.Lock()
	fail := q.claimTarget == principal.AgentID
	q.mu.Unlock()
	if fail {
		messages, _, _ := q.Store.Snapshot(principal.SessionID, principal.Turn)
		for _, message := range messages {
			if message.Target == principal.AgentID && message.Delivery != nil && message.Delivery.State == "queued" {
				return messaging.Message{}, false, errQueueFault
			}
		}
	}
	return q.Store.ClaimManaged(ctx, principal)
}

func (q *faultQueue) UpdateDelivery(ctx context.Context, principal messaging.Principal, id string, delivery messaging.Delivery) error {
	q.mu.Lock()
	q.updates = append(q.updates, delivery.State)
	fail := q.failStates[delivery.State]
	q.mu.Unlock()
	if fail {
		return errQueueFault
	}
	return q.Store.UpdateDelivery(ctx, principal, id, delivery)
}

func (q *faultQueue) failClaim(target string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.claimTarget = target
}

func (q *faultQueue) failUpdate(states ...string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.failStates = make(map[string]bool)
	for _, state := range states {
		q.failStates[state] = true
	}
}

func awaitUnavailable(t *testing.T, f managerFixture, id string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for {
		changes := f.manager.Changes()
		if !f.manager.recipientAvailable(id) {
			nodes, _ := f.manager.Read(f.cfg.SessionID, f.cfg.Turn)
			for _, node := range nodes {
				if node.String("id") == id && strings.Contains(node.String("status"), "새 메시지 수락 안 함") {
					return
				}
			}
			t.Fatal("unavailable recipient not reflected in graph status")
		}
		select {
		case <-changes:
		case <-ctx.Done():
			t.Fatalf("recipient %s did not stop after persistence error", id)
		}
	}
}

func assertNewMessageRejected(t *testing.T, f managerFixture, target string) {
	t.Helper()
	before, _, _ := f.store.Snapshot(f.cfg.SessionID, f.cfg.Turn)
	_, err := f.manager.Send(context.Background(), f.owner, messaging.Request{ID: "after-failure", Target: target, Body: "new input", Transport: messaging.ManagedTransport})
	if !errors.Is(err, messaging.ErrRecipientUnavailable) {
		t.Fatalf("new send error = %v; want recipient unavailable", err)
	}
	after, _, _ := f.store.Snapshot(f.cfg.SessionID, f.cfg.Turn)
	if len(after) != len(before) {
		t.Fatalf("unavailable recipient accepted new message: before %d, after %d", len(before), len(after))
	}
}

func TestManagerClaimFailureClosesAdmissionUntilExplicitRestart(t *testing.T) {
	var queue *faultQueue
	f := newManagerFixtureWithQueue(t, func(store *messaging.Store) recipientQueue {
		queue = &faultQueue{Store: store}
		return queue
	})
	queue.failClaim("managed:ux")
	sendRequest(t, f, "pending", "managed:ux", "preserve queued input")
	awaitUnavailable(t, f, "managed:ux")
	// Storage recovery alone must not make a stopped dispatcher look healthy.
	queue.failClaim("")
	assertNewMessageRejected(t, f, "managed:ux")
	message := sendRequest(t, f, "pending", "managed:ux", "preserve queued input")
	if message.Delivery.State != "queued" || f.runtimes["managed:ux"].starts.Load() != 0 {
		t.Fatalf("failed claim executed or lost original receipt: %+v", message.Delivery)
	}
	_, err := f.manager.Send(context.Background(), f.owner, messaging.Request{ID: "pending", Target: "managed:ux", Body: "changed body", Transport: messaging.ManagedTransport})
	if !errors.Is(err, messaging.ErrConflict) {
		t.Fatalf("changed same-ID retry = %v; want conflict", err)
	}
	// One failed recipient must not prevent an independent healthy worker.
	sendRequest(t, f, "healthy-peer", "managed:dx", "continue")
	dxRun := awaitRun(t, f.runtimes["managed:dx"])
	dxRun.complete <- nil
	awaitDelivery(t, f, "healthy-peer", "completed")
	f.manager.Close()

	runtimes, _, factory := newTestFactory()
	restarted, err := New(context.Background(), f.store, f.cfg, testProvider(factory))
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	f.manager = restarted
	run := awaitRun(t, runtimes["managed:ux"])
	if run.message.ID != "pending" {
		t.Fatalf("restart ran %q; want original queued input", run.message.ID)
	}
	run.complete <- nil
	awaitDelivery(t, f, "pending", "completed")
	if runtimes["managed:ux"].starts.Load() != 1 {
		t.Fatal("restart duplicated queued input")
	}
}

func TestManagerDeliveryWriteFailuresHaltRecipient(t *testing.T) {
	for _, tt := range []struct {
		name       string
		failStates []string
		runErr     error
		wantState  string
	}{
		{"acceptance", []string{"running"}, nil, "unknown"},
		{"completion", []string{"completed"}, nil, "unknown"},
		{"completion and fallback", []string{"completed", "unknown"}, nil, "running"},
		{"runtime failure fallback", []string{"unknown"}, errors.New("provider disconnected"), "running"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var queue *faultQueue
			f := newManagerFixtureWithQueue(t, func(store *messaging.Store) recipientQueue {
				queue = &faultQueue{Store: store}
				return queue
			})
			queue.failUpdate(tt.failStates...)
			sendRequest(t, f, "uncertain", "managed:ux", "do not replay")
			if tt.name != "acceptance" {
				run := awaitRun(t, f.runtimes["managed:ux"])
				sendRequest(t, f, "already-queued", "managed:ux", "wait for explicit recovery")
				run.complete <- tt.runErr
			}
			awaitUnavailable(t, f, "managed:ux")
			assertNewMessageRejected(t, f, "managed:ux")
			// Closing joins the recipient so even a failed fallback has finished.
			f.manager.Close()
			message := awaitDelivery(t, f, "uncertain", tt.wantState)
			if message.Delivery.ThreadID == "" && tt.wantState == "running" {
				t.Fatal("lost observed provider receipt after fallback write failure")
			}
			if tt.name != "acceptance" {
				awaitDelivery(t, f, "already-queued", "queued")
			}
			if got := f.runtimes["managed:ux"].starts.Load(); got != 1 {
				t.Fatalf("uncertain execution was replayed: %d starts", got)
			}
			queue.mu.Lock()
			attempts := strings.Join(queue.updates, ",")
			queue.mu.Unlock()
			if !strings.Contains(attempts, "unknown") {
				t.Fatalf("failure receipt was not attempted: %s", attempts)
			}
		})
	}
}

func TestManagerFailedUnknownWriteRejectsNewInputAfterStorageRecovery(t *testing.T) {
	var queue *faultQueue
	f := newManagerFixtureWithQueue(t, func(store *messaging.Store) recipientQueue {
		queue = &faultQueue{Store: store}
		return queue
	})
	sendRequest(t, f, "uncertain", "managed:ux", "do not replay")
	run := awaitRun(t, f.runtimes["managed:ux"])
	queue.failUpdate("unknown")
	run.complete <- errors.New("provider disconnected")
	awaitUnavailable(t, f, "managed:ux")
	queue.failUpdate()
	assertNewMessageRejected(t, f, "managed:ux")
	message := sendRequest(t, f, "uncertain", "managed:ux", "do not replay")
	if message.Delivery.State != "running" {
		t.Fatalf("write failure changed durable provider state: %+v", message.Delivery)
	}
	f.manager.Close()
	if f.runtimes["managed:ux"].starts.Load() != 1 {
		t.Fatal("failed unknown persistence caused a replay")
	}
}

func TestWorkerProjectionKeepsOwnedActivitiesAndInvalidatesStateCache(t *testing.T) {
	f := newManagerFixture(t)
	// Wait for the store watcher's initial revision before testing cache reuse.
	changes := f.manager.Changes()
	f.manager.mu.Lock()
	initialRevision := f.manager.revision
	f.manager.mu.Unlock()
	if initialRevision == 0 {
		select {
		case <-changes:
		case <-time.After(3 * time.Second):
			t.Fatal("store watcher did not publish initial revision")
		}
	}
	first, firstRevision := f.manager.Read(f.cfg.SessionID, f.cfg.Turn)
	cached, cachedRevision := f.manager.Read(f.cfg.SessionID, f.cfg.Turn)
	if firstRevision != cachedRevision || &first[0] != &cached[0] {
		t.Fatal("unchanged read did not reuse cached projection")
	}
	f.manager.setState("managed:ux", "작업 중", "owned-thread")
	updated, updatedRevision := f.manager.Read(f.cfg.SessionID, f.cfg.Turn)
	if updatedRevision == firstRevision || updated[0].String("providerThreadId") != "owned-thread" {
		t.Fatal("worker state change did not invalidate projection")
	}
	if first[0].String("providerThreadId") != "" {
		t.Fatal("new projection mutated previous snapshot")
	}
	base := []model.Record{
		{"id": "managed-message", "bodySource": "observer-managed", "target": "managed:ux", "status": "실행 완료", "at": "start", "updatedAt": "end"},
		{"id": "native-message", "bodySource": "native", "target": "managed:dx"},
		{"id": "other-managed-message", "bodySource": "observer-managed", "target": "managed:other"},
	}
	states := map[string]workerState{"managed:ux": {status: "완료", conversationID: "owned-thread", updated: time.Unix(1, 0)}}
	nodes := projectWorkers(f.cfg, f.manager.provider, states, base)
	activities := 0
	for _, node := range nodes {
		if node.String("kind") != "tool" {
			continue
		}
		activities++
		if node.String("id") != "managed-execution:managed-message" || node.String("ownerAgent") != "managed:ux" || node.String("label") != "메시지 UX 검토" || node.String("status") != "실행 완료" || node.String("updatedAt") != "end" {
			t.Fatalf("projected execution lost ownership, identity or state: %v", node)
		}
	}
	if activities != 1 || len(nodes) != len(base)+len(f.cfg.Workers)+1 {
		t.Fatalf("projection included unrelated executions: %v", nodes)
	}
	if base[0].String("kind") != "" || states["managed:ux"].status != "완료" {
		t.Fatal("projection modified its input")
	}
}

func TestManagerStartupFailureCannotIgnoreFailedUnknownPersistence(t *testing.T) {
	f := newManagerFixture(t)
	f.manager.Close()
	queue := &faultQueue{Store: f.store, failStates: map[string]bool{"unknown": true}}
	var starts atomic.Int32
	factory := func(context.Context, RuntimeConfig, SendFunc) (Runtime, error) {
		starts.Add(1)
		return nil, errors.New("runtime unavailable")
	}
	manager, err := newManager(context.Background(), f.store, queue, f.cfg, testProvider(factory))
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	f.manager = manager
	sendRequest(t, f, "startup-failure", "managed:ux", "start once")
	awaitUnavailable(t, f, "managed:ux")
	assertNewMessageRejected(t, f, "managed:ux")
	awaitDelivery(t, f, "startup-failure", "dispatching")
	if starts.Load() != 1 {
		t.Fatalf("failed startup was retried %d times", starts.Load())
	}
}
