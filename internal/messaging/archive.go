package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"
)

// The archive is a compact metadata index. Payloads are read only for the
// selected scope and expire independently. Receipt/auth indexes grow with
// lifetime identities; they do not consume the bounded active snapshot budget.
type archiveIndex struct {
	messages     map[string]archivedMessage
	capabilities map[string]Principal
	revisions    map[string]uint64
}
type archivedMessage struct {
	Version        int           `json:"version"`
	Stored         storedMessage `json:"record"`
	ExpiresAt      time.Time     `json:"expiresAt"`
	PayloadHash    string        `json:"payloadHash"`
	PayloadExpired bool          `json:"payloadExpired,omitempty"`
}
type archivedPayload struct {
	Body   string `json:"body"`
	Output string `json:"output,omitempty"`
}
type archivedScope struct {
	Session  string `json:"session"`
	Turn     string `json:"turn"`
	Revision uint64 `json:"revision"`
}

func terminalMessage(m Message) bool {
	if m.Transport == MailboxTransport {
		return m.AckedAt != nil
	}
	return m.Delivery != nil && (m.Delivery.State == "completed" || m.Delivery.State == "unknown")
}
func terminalTime(m Message) time.Time {
	if m.AckedAt != nil {
		return *m.AckedAt
	}
	return m.Delivery.UpdatedAt
}

func payloadDeadline(m Message) time.Time {
	if m.Transport == MailboxTransport {
		return terminalTime(m).Add(AckRetention)
	}
	// Managed conversations retain their existing indefinite retention policy.
	// Moving data out of the active snapshot does not authorize deleting it.
	return time.Time{}
}

func payloadExpired(r archivedMessage, now time.Time) bool {
	return r.PayloadExpired || !r.ExpiresAt.IsZero() && !now.Before(r.ExpiresAt)
}
func (s *Store) archivePath(kind, key string) string {
	return filepath.Join(s.directory, "archive", kind, tokenHash(key)+".json")
}

func (s *Store) loadArchive() error {
	s.archive = archiveIndex{messages: map[string]archivedMessage{}, capabilities: map[string]Principal{}, revisions: map[string]uint64{}}
	for _, kind := range []string{"payloads", "receipts", "auth", "scopes"} {
		dir := filepath.Join(s.directory, "archive", kind)
		if err := os.MkdirAll(dir, 0700); err != nil {
			return err
		}
		if err := checkDirectory(dir); err != nil {
			return err
		}
		if err := syncDirectory(filepath.Dir(dir)); err != nil {
			return err
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), ".archive-") {
				if _, err := readPrivate(filepath.Join(dir, entry.Name()), maxStoreBytes); err != nil {
					return err
				}
				if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil {
					return err
				}
				continue
			}
			if filepath.Ext(entry.Name()) != ".json" {
				return errors.New("unexpected archive entry")
			}
			if kind == "payloads" {
				continue
			}
			raw, err := readPrivate(filepath.Join(dir, entry.Name()), maxStoreBytes)
			if err != nil {
				return err
			}
			if err := s.loadArchiveEntry(kind, entry.Name(), raw); err != nil {
				return err
			}
		}
		if err := syncDirectory(dir); err != nil {
			return err
		}
	}
	sequences := make(map[uint64]string, len(s.state.Messages))
	for key, stored := range s.state.Messages {
		sequences[stored.Sequence] = key
	}
	for key, r := range s.archive.messages {
		if r.Stored.Sequence > s.state.LastSequence || s.baseRevision(scopeKey(r.Stored.Message.SessionID, r.Stored.Message.Turn)) == 0 {
			return errors.New("archive receipt has no sequence or scope watermark")
		}
		if other, used := sequences[r.Stored.Sequence]; used && other != key {
			return errors.New("conflicting archive enqueue sequence")
		}
		sequences[r.Stored.Sequence] = key
		m := r.Stored.Message
		if !s.enrolled(Principal{m.SessionID, m.Turn, m.Source}) || !s.enrolled(Principal{m.SessionID, m.Turn, m.Target}) {
			return errors.New("archive participant registration missing")
		}
		if active, ok := s.state.Messages[key]; ok && !matchesArchive(active, r) {
			return errors.New("active/archive receipt conflict")
		}
	}
	for hash, p := range s.archive.capabilities {
		if active, ok := s.state.Capabilities[hash]; ok && active != p {
			return errors.New("active/archive capability conflict")
		}
	}
	if err := s.removeOrphanedPayloads(); err != nil {
		return err
	}
	return syncDirectory(s.directory)
}

// An interrupted archive can leave a payload before its receipt is committed.
// Such a payload is not authoritative; the active snapshot still owns the body.
func (s *Store) removeOrphanedPayloads() error {
	known := make(map[string]bool, len(s.archive.messages))
	for key := range s.archive.messages {
		known[filepath.Base(s.archivePath("payloads", key))] = true
	}
	dir := filepath.Join(s.directory, "archive", "payloads")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if known[entry.Name()] {
			continue
		}
		if !validHash(strings.TrimSuffix(entry.Name(), ".json")) {
			return errors.New("unexpected archive payload")
		}
		if _, err := readPrivate(filepath.Join(dir, entry.Name()), 12*MaxBodyRunes+4096); err != nil {
			return err
		}
		if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil {
			return err
		}
	}
	return syncDirectory(dir)
}

func (s *Store) loadArchiveEntry(kind, name string, raw []byte) error {
	invalid := errors.New("invalid messaging archive")
	switch kind {
	case "receipts":
		var r archivedMessage
		if json.Unmarshal(raw, &r) != nil || !validArchiveReceipt(r) {
			return invalid
		}
		m := r.Stored.Message
		key := messageKey(m.SessionID, m.Turn, m.ID)
		if filepath.Base(s.archivePath(kind, key)) != name {
			return invalid
		}
		s.archive.messages[key] = r
	case "auth":
		var item struct {
			Hash      string    `json:"hash"`
			Principal Principal `json:"principal"`
		}
		if json.Unmarshal(raw, &item) != nil || !validHash(item.Hash) || !validPrincipal(item.Principal) || filepath.Base(s.archivePath(kind, item.Hash)) != name {
			return invalid
		}
		s.archive.capabilities[item.Hash] = item.Principal
	case "scopes":
		var scope archivedScope
		if json.Unmarshal(raw, &scope) != nil || !sessionPattern.MatchString(scope.Session) || !idPattern.MatchString(scope.Turn) || scope.Revision == 0 {
			return invalid
		}
		key := scopeKey(scope.Session, scope.Turn)
		if filepath.Base(s.archivePath(kind, key)) != name {
			return invalid
		}
		s.archive.revisions[key] = scope.Revision
	}
	return nil
}

func validArchiveReceipt(r archivedMessage) bool {
	m := r.Stored.Message
	if r.Version != 1 || !terminalMessage(m) || !validHash(r.Stored.Fingerprint) || !validHash(r.PayloadHash) || r.Stored.Sequence == 0 || m.Body != "" || m.AcceptedAt.IsZero() || !idPattern.MatchString(m.ID) {
		return false
	}
	if !validPrincipal(Principal{SessionID: m.SessionID, Turn: m.Turn, AgentID: m.Source}) || !validAgent(m.Target) {
		return false
	}
	if m.ReplyTo != "" && !idPattern.MatchString(m.ReplyTo) || m.BodyExpiredAt != nil && m.BodyExpiredAt.Before(r.ExpiresAt) {
		return false
	}
	if !r.ExpiresAt.Equal(payloadDeadline(m)) {
		return false
	}
	if m.Transport == MailboxTransport {
		return m.Delivery == nil && !m.AckedAt.Before(m.AcceptedAt)
	}
	return IsManagedTransport(m.Transport) && !r.PayloadExpired && m.BodyExpiredAt == nil && m.AckedAt == nil && m.Delivery.Output == "" && !m.Delivery.UpdatedAt.Before(m.AcceptedAt) && validDelivery(*m.Delivery)
}

func messagePayload(m Message) archivedPayload {
	payload := archivedPayload{Body: m.Body}
	if m.Delivery != nil {
		payload.Output = m.Delivery.Output
	}
	return payload
}

func matchesArchive(active storedMessage, archived archivedMessage) bool {
	if !sameArchivedRecord(active, archived.Stored) {
		return false
	}
	if archived.PayloadExpired || active.Message.BodyExpiredAt != nil {
		return true
	}
	raw, err := json.Marshal(messagePayload(active.Message))
	return err == nil && tokenHash(string(raw)) == archived.PayloadHash
}

func sameArchivedRecord(a, b storedMessage) bool {
	if !terminalMessage(a.Message) {
		return false
	}
	a.Message = cloneMessage(a.Message)
	b.Message = cloneMessage(b.Message)
	a.Message.Body = ""
	b.Message.Body = ""
	a.Message.BodyExpiredAt = nil
	b.Message.BodyExpiredAt = nil
	a.Message.AcceptedAt = a.Message.AcceptedAt.UTC()
	b.Message.AcceptedAt = b.Message.AcceptedAt.UTC()
	if a.Message.AckedAt != nil {
		at := a.Message.AckedAt.UTC()
		a.Message.AckedAt = &at
	}
	if b.Message.AckedAt != nil {
		at := b.Message.AckedAt.UTC()
		b.Message.AckedAt = &at
	}
	if a.Message.Delivery != nil {
		a.Message.Delivery.Output = ""
		a.Message.Delivery.UpdatedAt = a.Message.Delivery.UpdatedAt.UTC()
	}
	if b.Message.Delivery != nil {
		b.Message.Delivery.Output = ""
		b.Message.Delivery.UpdatedAt = b.Message.Delivery.UpdatedAt.UTC()
	}
	return reflect.DeepEqual(a, b)
}

// writeArchive confirms the directory rename before active data can be removed.
func (s *Store) writeArchive(ctx context.Context, kind, key string, value any) (bool, error) {
	if err := s.confirmArchiveDurability(); err != nil {
		return false, err
	}
	path := s.archivePath(kind, key)
	dir := filepath.Dir(path)
	if err := checkDirectory(dir); err != nil {
		return false, err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return false, err
	}
	f, err := os.CreateTemp(dir, ".archive-")
	if err != nil {
		return false, err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err != nil {
		f.Close()
		return false, err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return false, err
	}
	if err = f.Close(); err != nil {
		return false, err
	}
	if err = ctx.Err(); err != nil {
		return false, err
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return false, err
	}
	s.archiveSync[dir] = true
	return true, s.confirmArchiveDurability()
}

func (s *Store) confirmArchiveDurability() error {
	for dir := range s.archiveSync {
		if err := s.syncArchiveDirectory(dir); err != nil {
			return fmt.Errorf("%w: archive directory sync", ErrOutcomeUnknown)
		}
		delete(s.archiveSync, dir)
	}
	return nil
}

func (s *Store) archiveMessage(ctx context.Context, key string, stored storedMessage) error {
	if existing, ok := s.archive.messages[key]; ok {
		if !matchesArchive(stored, existing) {
			return errors.New("archive receipt conflict")
		}
		return nil
	}
	m := cloneMessage(stored.Message)
	payload := archivedPayload{Body: m.Body}
	if m.Delivery != nil {
		payload.Output = m.Delivery.Output
		m.Delivery.Output = ""
	}
	m.Body = ""
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	r := archivedMessage{Version: 1, Stored: storedMessage{Message: m, Fingerprint: stored.Fingerprint, Sequence: stored.Sequence}, ExpiresAt: payloadDeadline(m), PayloadHash: tokenHash(string(raw))}
	// An expired active legacy record has already redacted its payload.
	if _, err := s.writeArchive(ctx, "payloads", key, payload); err != nil {
		return err
	}
	committed, err := s.writeArchive(ctx, "receipts", key, r)
	if committed {
		s.archive.messages[key] = r
	}
	return err
}

// compactLocked commits archival in this order: payloads/receipts, authority,
// scope watermarks, then active removal. A failure leaves accepted work intact.
func (s *Store) compactLocked(ctx context.Context) error {
	if err := s.confirmDurability(); err != nil {
		return err
	}
	next := s.copyState()
	if err := s.archiveTerminalMessages(ctx, &next); err != nil {
		return err
	}
	active := activeScopes(next)
	if err := s.archiveIdleParticipants(ctx, &next, active); err != nil {
		return err
	}
	if err := s.archiveScopeRevisions(ctx, &next, active); err != nil {
		return err
	}
	changed := len(next.Messages) != len(s.state.Messages) || len(next.Capabilities) != len(s.state.Capabilities) || len(next.Revisions) != len(s.state.Revisions)
	if changed {
		if err := s.commit(ctx, next, false); err != nil {
			return err
		}
	}
	return s.pruneArchivedPayloads(ctx)
}

func (s *Store) archiveTerminalMessages(ctx context.Context, next *diskState) error {
	for key, stored := range next.Messages {
		if !terminalMessage(stored.Message) {
			continue
		}
		if err := s.archiveMessage(ctx, key, stored); err != nil {
			return err
		}
		delete(next.Messages, key)
	}
	return nil
}

func activeScopes(state diskState) map[string]bool {
	active := make(map[string]bool)
	for _, stored := range state.Messages {
		m := stored.Message
		active[scopeKey(m.SessionID, m.Turn)] = true
	}
	return active
}

func (s *Store) archiveIdleParticipants(ctx context.Context, next *diskState, active map[string]bool) error {
	for hash, p := range next.Capabilities {
		if active[scopeKey(p.SessionID, p.Turn)] {
			continue
		}
		item := struct {
			Hash      string    `json:"hash"`
			Principal Principal `json:"principal"`
		}{hash, p}
		committed, err := s.writeArchive(ctx, "auth", hash, item)
		if committed {
			s.archive.capabilities[hash] = p
		}
		if err != nil {
			return err
		}
		delete(next.Capabilities, hash)
	}
	return nil
}

func (s *Store) archiveScopeRevisions(ctx context.Context, next *diskState, active map[string]bool) error {
	for scope, revision := range next.Revisions {
		if revision > s.archive.revisions[scope] {
			parts := strings.Split(scope, "\x00")
			committed, err := s.writeArchive(ctx, "scopes", scope, archivedScope{parts[0], parts[1], revision})
			if committed {
				s.archive.revisions[scope] = revision
			}
			if err != nil {
				return err
			}
		}
		if !active[scope] {
			delete(next.Revisions, scope)
		}
	}
	return nil
}

func (s *Store) pruneArchivedPayloads(ctx context.Context) error {
	changed := false
	defer func() {
		if changed {
			close(s.changed)
			s.changed = make(chan struct{})
		}
	}()
	for key, r := range s.archive.messages {
		if !payloadExpired(r, s.now()) {
			continue
		}
		// Persist expiration before deleting payload, including across clock
		// rollback or a crash between the receipt rename and payload removal.
		if !r.PayloadExpired {
			r.PayloadExpired = true
			committed, err := s.writeArchive(ctx, "receipts", key, r)
			if committed {
				s.archive.messages[key] = r
				changed = true
			}
			if err != nil {
				return err
			}
		}
		err := os.Remove(s.archivePath("payloads", key))
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		changed = changed || err == nil
	}
	if changed {
		if err := syncDirectory(filepath.Join(s.directory, "archive", "payloads")); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) archivedRecord(key string) (storedMessage, bool, error) {
	r, ok := s.archive.messages[key]
	if !ok {
		return storedMessage{}, false, nil
	}
	stored := r.Stored
	stored.Message = cloneMessage(stored.Message)
	if payloadExpired(r, s.now()) || stored.Message.BodyExpiredAt != nil {
		stored.Message.BodyExpiredAt = &r.ExpiresAt
		return stored, true, nil
	}
	raw, err := readPrivate(s.archivePath("payloads", key), 12*MaxBodyRunes+4096)
	if err != nil {
		return storedMessage{}, true, fmt.Errorf("read archived message payload: %w", err)
	}
	if tokenHash(string(raw)) != r.PayloadHash {
		return storedMessage{}, true, errors.New("archived payload identity mismatch")
	}
	var payload archivedPayload
	if json.Unmarshal(raw, &payload) != nil {
		return storedMessage{}, true, errors.New("invalid archived payload")
	}
	stored.Message.Body = payload.Body
	if stored.Message.Delivery != nil {
		stored.Message.Delivery.Output = payload.Output
	}
	principal := Principal{SessionID: stored.Message.SessionID, Turn: stored.Message.Turn, AgentID: stored.Message.Source}
	req := Request{ID: stored.Message.ID, Target: stored.Message.Target, Body: stored.Message.Body, ReplyTo: stored.Message.ReplyTo, Transport: stored.Message.Transport}
	if ValidateRequest(req) != nil || requestFingerprint(principal, req) != stored.Fingerprint || stored.Message.Delivery != nil && !validDelivery(*stored.Message.Delivery) {
		return storedMessage{}, true, errors.New("invalid archived message content")
	}
	return stored, true, nil
}

func (s *Store) lookupMessage(key string) (storedMessage, bool, error) {
	if stored, ok := s.state.Messages[key]; ok {
		return stored, true, nil
	}
	return s.archivedRecord(key)
}
func (s *Store) baseRevision(scope string) uint64 {
	return max(s.state.Revisions[scope], s.archive.revisions[scope])
}
func (s *Store) carryParticipants(next *diskState, participants ...Principal) {
	for _, p := range participants {
		found := false
		for _, active := range next.Capabilities {
			if active == p {
				found = true
				break
			}
		}
		if found {
			continue
		}
		for hash, archived := range s.archive.capabilities {
			if archived == p {
				next.Capabilities[hash] = p
				break
			}
		}
	}
}
