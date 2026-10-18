package messaging

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"agent-observer/internal/atomicfile"
)

type storedMessage struct {
	Message     Message `json:"message"`
	Fingerprint string  `json:"fingerprint"`
	Sequence    uint64  `json:"sequence,omitempty"`
}

type diskState struct {
	CapacityBytes int64                    `json:"capacityBytes,omitempty"`
	Version       int                      `json:"version"`
	LastSequence  uint64                   `json:"lastSequence,omitempty"`
	Capabilities  map[string]Principal     `json:"capabilities"`
	Messages      map[string]storedMessage `json:"messages"`
	Revisions     map[string]uint64        `json:"revisions"`
}

// Store is an explicitly enabled local mailbox. One atomic state replacement
// commits the recipient queue and Observer's source record together. Observation
// readers never remove or acknowledge queued messages.
type Store struct {
	fs                   atomicfile.Ops
	mu                   sync.Mutex
	directory            string
	lock                 *os.File
	adminHash            [32]byte
	state                diskState
	archive              archiveIndex
	changed              chan struct{}
	closed               bool
	syncPending          bool
	archiveSync          map[string]bool
	syncArchiveDirectory func(string) error
	now                  func() time.Time
}

func Open(directory string) (*Store, error) {
	if !filepath.IsAbs(directory) {
		return nil, errors.New("messaging store requires an absolute directory")
	}
	directory = filepath.Clean(directory)
	// Resolve existing parent aliases (notably macOS /var -> /private/var),
	// while still rejecting a symlink at the store's own path.
	parent, err := filepath.EvalSymlinks(filepath.Dir(directory))
	if err != nil {
		return nil, errors.New("messaging store parent must exist")
	}
	directory = filepath.Join(parent, filepath.Base(directory))
	if err := checkParents(directory); err != nil {
		return nil, err
	}
	if err := os.Mkdir(directory, 0700); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("create messaging directory: %w", err)
	}
	if err := checkDirectory(directory); err != nil {
		return nil, err
	}
	lock, err := lockDirectory(directory)
	if err != nil {
		return nil, err
	}
	store := &Store{fs: atomicfile.Default(), directory: directory, lock: lock, changed: make(chan struct{}), now: time.Now, archiveSync: map[string]bool{}, syncArchiveDirectory: syncDirectory}
	if err := store.removeAbandonedTransactions(); err != nil {
		lock.Close()
		return nil, err
	}
	if err := store.load(); err != nil {
		lock.Close()
		return nil, err
	}
	if err := store.loadArchive(); err != nil {
		lock.Close()
		return nil, err
	}
	if err := store.migrateCapacity(); err != nil {
		lock.Close()
		return nil, err
	}
	if err := syncDirectory(directory); err != nil {
		lock.Close()
		return nil, fmt.Errorf("confirm messaging store directory: %w", err)
	}
	if err := store.loadAdmin(); err != nil {
		lock.Close()
		return nil, err
	}
	if err := store.Prune(context.Background()); err != nil {
		lock.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	err := s.confirmDurability()
	s.closed = true
	close(s.changed)
	return errors.Join(err, s.lock.Close())
}

// AdminTokenPath identifies the private bootstrap credential. Never put its
// contents in a URL, graph node, subprocess argument or observation record.
func (s *Store) AdminTokenPath() string { return filepath.Join(s.directory, "admin.token") }

func (s *Store) AuthenticateAdmin(token string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	actual := sha256.Sum256([]byte(token))
	return !s.closed && subtle.ConstantTimeCompare(actual[:], s.adminHash[:]) == 1
}

// Enroll issues a scoped capability. The caller must verify the principal using
// its trusted enrollment boundary first. Only the hash is persisted here.
func (s *Store) Enroll(ctx context.Context, principal Principal) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkContext(ctx); err != nil {
		return "", err
	}
	if !validPrincipal(principal) {
		return "", ErrInvalid
	}
	return s.enrollLocked(ctx, principal)
}

// EnsureParticipant registers a trusted internal participant without issuing a
// new capability on every restart. It is not an HTTP enrollment boundary.
func (s *Store) EnsureParticipant(ctx context.Context, principal Principal) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkContext(ctx); err != nil {
		return err
	}
	if !validPrincipal(principal) {
		return ErrInvalid
	}
	if s.enrolled(principal) {
		return s.confirmDurability()
	}
	_, err := s.enrollLocked(ctx, principal)
	return err
}

func (s *Store) enrollLocked(ctx context.Context, principal Principal) (string, error) {
	token, err := newToken()
	if err != nil {
		return "", err
	}
	next, err := s.admit(ctx, func() diskState {
		next := s.copyState()
		next.Capabilities[tokenHash(token)] = principal
		return next
	})
	if err != nil {
		return "", err
	}
	if err := s.commit(ctx, next, false); err != nil {
		return "", err
	}
	return token, nil
}

func (s *Store) Authenticate(token string) (Principal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.authenticate(token)
}

func (s *Store) authenticate(token string) (Principal, error) {
	if s.closed {
		return Principal{}, ErrClosed
	}
	if len(token) != 43 {
		return Principal{}, ErrUnauthorized
	}
	principal, ok := s.state.Capabilities[tokenHash(token)]
	if !ok {
		principal, ok = s.archive.capabilities[tokenHash(token)]
	}
	if !ok {
		return Principal{}, ErrUnauthorized
	}
	return principal, nil
}

type mailbox struct{ store *Store }

func (s *Store) Mailbox() Transport { return mailbox{store: s} }

func (m mailbox) Send(ctx context.Context, principal Principal, request Request) (Message, error) {
	if request.Transport != MailboxTransport {
		return Message{}, ErrUnsupported
	}
	return m.store.send(ctx, principal, request)
}

func (s *Store) send(ctx context.Context, principal Principal, request Request) (Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkContext(ctx); err != nil {
		return Message{}, err
	}
	if !validPrincipal(principal) || ValidateRequest(request) != nil {
		return Message{}, ErrInvalid
	}
	if request.Transport != MailboxTransport && !IsManagedTransport(request.Transport) {
		return Message{}, ErrUnsupported
	}
	key := messageKey(principal.SessionID, principal.Turn, request.ID)
	fingerprint := requestFingerprint(principal, request)
	existing, found, err := s.lookupMessage(key)
	if err != nil {
		return Message{}, err
	}
	if found {
		if existing.Fingerprint != fingerprint {
			return Message{}, ErrConflict
		}
		if err := s.confirmDurability(); err != nil {
			return Message{}, err
		}
		return cloneMessage(existing.Message), nil
	}
	if !s.enrolled(principal) {
		return Message{}, ErrUnauthorized
	}
	if !s.enrolled(Principal{SessionID: principal.SessionID, Turn: principal.Turn, AgentID: request.Target}) {
		return Message{}, ErrNotFound
	}
	if request.ReplyTo != "" && !s.validReply(principal, request) {
		return Message{}, ErrInvalid
	}
	if s.state.LastSequence == math.MaxUint64 {
		return Message{}, ErrCapacity
	}
	message := Message{
		ID: request.ID, SessionID: principal.SessionID, Turn: principal.Turn,
		Source: principal.AgentID, Target: request.Target, Body: request.Body,
		ReplyTo: request.ReplyTo, Transport: request.Transport, AcceptedAt: s.now().UTC(),
	}
	if IsManagedTransport(request.Transport) {
		message.Delivery = &Delivery{State: "queued", UpdatedAt: message.AcceptedAt}
	}
	next, err := s.admit(ctx, func() diskState {
		next := s.copyState()
		s.carryParticipants(&next, principal, Principal{SessionID: principal.SessionID, Turn: principal.Turn, AgentID: request.Target})
		next.LastSequence++
		next.Messages[key] = storedMessage{Message: message, Fingerprint: fingerprint, Sequence: next.LastSequence}
		scope := scopeKey(principal.SessionID, principal.Turn)
		next.Revisions[scope] = s.baseRevision(scope) + 1
		return next
	})
	if err != nil {
		return Message{}, err
	}
	if err := s.commit(ctx, next, true); err != nil {
		return Message{}, err
	}
	return cloneMessage(message), nil
}

func (s *Store) enrolled(principal Principal) bool {
	for _, p := range s.archive.capabilities {
		if p == principal {
			return true
		}
	}
	for _, enrolled := range s.state.Capabilities {
		if enrolled == principal {
			return true
		}
	}
	return false
}

func (s *Store) validReply(principal Principal, request Request) bool {
	key := messageKey(principal.SessionID, principal.Turn, request.ReplyTo)
	original, ok := s.state.Messages[key]
	if !ok {
		r, found := s.archive.messages[key]
		original, ok = r.Stored, found
	}
	return ok && original.Message.Source == request.Target && original.Message.Target == principal.AgentID
}

func (s *Store) receive(ctx context.Context, token string) ([]Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkContext(ctx); err != nil {
		return nil, err
	}
	principal, err := s.authenticate(token)
	if err != nil {
		return nil, err
	}
	messages := make([]Message, 0)
	for _, stored := range s.state.Messages {
		message := stored.Message
		if message.Transport == MailboxTransport && message.SessionID == principal.SessionID && message.Turn == principal.Turn && message.Target == principal.AgentID && message.AckedAt == nil {
			messages = append(messages, cloneMessage(message))
		}
	}
	sortMessages(messages)
	return messages, nil
}

func (s *Store) ack(ctx context.Context, token, id string) (Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkContext(ctx); err != nil {
		return Message{}, err
	}
	principal, err := s.authenticate(token)
	if err != nil {
		return Message{}, err
	}
	if !idPattern.MatchString(id) {
		return Message{}, ErrInvalid
	}
	key := messageKey(principal.SessionID, principal.Turn, id)
	stored, ok, lookupErr := s.lookupMessage(key)
	if lookupErr != nil {
		return Message{}, lookupErr
	}
	if !ok {
		return Message{}, ErrNotFound
	}
	if stored.Message.Target != principal.AgentID {
		return Message{}, ErrForbidden
	}
	if stored.Message.Transport != MailboxTransport {
		return Message{}, ErrUnsupported
	}
	if stored.Message.AckedAt != nil {
		if err := s.confirmDurability(); err != nil {
			return Message{}, err
		}
		return cloneMessage(stored.Message), nil
	}
	message := cloneMessage(stored.Message)
	at := s.now().UTC()
	if at.Before(message.AcceptedAt) {
		at = message.AcceptedAt
	}
	message.AckedAt = &at
	stored.Message = message
	next := s.copyState()
	next.Messages[key] = stored
	next.Revisions[scopeKey(principal.SessionID, principal.Turn)]++
	if err := s.commit(ctx, next, true); err != nil {
		return Message{}, err
	}
	return cloneMessage(message), nil
}

// Snapshot is a detached, deterministic view for one explicitly selected
// session and turn. Reading it does not acknowledge delivery or affect the queue.
func (s *Store) Snapshot(sessionID, turn string) ([]Message, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	messages := make([]Message, 0)
	if s.closed {
		return messages, 0, ErrClosed
	}
	revision := s.baseRevision(scopeKey(sessionID, turn))
	for key, r := range s.archive.messages {
		m := r.Stored.Message
		if m.SessionID != sessionID || m.Turn != turn {
			continue
		}
		if _, active := s.state.Messages[key]; active {
			continue
		}
		stored, _, err := s.archivedRecord(key)
		if err != nil {
			return nil, revision, err
		}
		messages = append(messages, stored.Message)
		if r.Stored.Message.BodyExpiredAt == nil && stored.Message.BodyExpiredAt != nil {
			revision++
		}
	}
	for _, stored := range s.state.Messages {
		m := stored.Message
		if m.SessionID == sessionID && m.Turn == turn {
			messages = append(messages, cloneMessage(m))
		}
	}
	sortMessages(messages)
	return messages, revision, nil
}

// Changes returns a broadcast channel closed after the next visible mutation.
// Obtain it before reading a snapshot to avoid losing a concurrent update.
func (s *Store) Changes() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.changed
}

// Prune moves terminal records out of the active budget. Mailbox bodies retain
// their seven-day Ack retention; managed conversations and retry receipts remain.
func (s *Store) Prune(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkContext(ctx); err != nil {
		return err
	}
	return s.compactLocked(ctx)
}

func (s *Store) checkContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return ErrClosed
	}
	return nil
}

func (s *Store) copyState() diskState {
	next := diskState{Version: s.state.Version, CapacityBytes: s.state.CapacityBytes, LastSequence: s.state.LastSequence, Capabilities: make(map[string]Principal, len(s.state.Capabilities)), Messages: make(map[string]storedMessage, len(s.state.Messages)), Revisions: make(map[string]uint64, len(s.state.Revisions))}
	for key, principal := range s.state.Capabilities {
		next.Capabilities[key] = principal
	}
	for key, stored := range s.state.Messages {
		next.Messages[key] = stored
	}
	for key, revision := range s.state.Revisions {
		next.Revisions[key] = revision
	}
	return next
}

func (s *Store) commit(ctx context.Context, next diskState, visible bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.confirmDurability(); err != nil {
		return err
	}
	if next.Version == storeVersion {
		size, err := reservedBytes(next)
		if err != nil {
			return err
		}
		if size <= maxStoreBytes {
			next.CapacityBytes = maxStoreBytes
		}
		if size > next.CapacityBytes {
			return ErrCapacity
		}
	}
	committed, err := s.save(ctx, next)
	if !committed {
		return err
	}
	// Rename is the commit point. A later directory-sync error must never roll
	// memory back to an older state and invite duplicate delivery on retry.
	s.state = next
	s.syncPending = errors.Is(err, ErrOutcomeUnknown)
	if visible {
		close(s.changed)
		s.changed = make(chan struct{})
	}
	return err
}

func (s *Store) confirmDurability() error {
	if err := s.confirmArchiveDurability(); err != nil {
		return err
	}
	if !s.syncPending {
		return nil
	}
	if err := s.fs.SyncDir(s.directory); err != nil {
		return fmt.Errorf("%w: %v", ErrOutcomeUnknown, err)
	}
	s.syncPending = false
	return nil
}

func sortMessages(messages []Message) {
	sort.Slice(messages, func(i, j int) bool {
		if messages[i].AcceptedAt.Equal(messages[j].AcceptedAt) {
			return messages[i].ID < messages[j].ID
		}
		return messages[i].AcceptedAt.Before(messages[j].AcceptedAt)
	})
}

func scopeKey(sessionID, turn string) string       { return sessionID + "\x00" + turn }
func messageKey(sessionID, turn, id string) string { return scopeKey(sessionID, turn) + "\x00" + id }

func requestFingerprint(principal Principal, request Request) string {
	// Keep the original hash domain so both aliases retry existing records without
	// rewriting bodies, fingerprints or delivery state during load.
	if IsManagedTransport(request.Transport) {
		request.Transport = LegacyManagedTransport
	}
	encoded, _ := json.Marshal(struct {
		Principal Principal `json:"principal"`
		Request   Request   `json:"request"`
	}{principal, request})
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func tokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func newToken() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate messaging capability: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}

func validHash(hash string) bool {
	raw, err := hex.DecodeString(hash)
	return err == nil && len(raw) == sha256.Size && strings.ToLower(hash) == hash
}
