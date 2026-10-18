package messaging

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const maxStoreBytes = 32 << 20

const storeVersion = 3

// A process can stop before rename, leaving a private temporary snapshot. It is
// never a committed queue; remove it under the exclusive lock so old bodies do
// not outlive the acknowledged-body retention policy in abandoned files.
func (s *Store) removeAbandonedTransactions() error {
	entries, err := os.ReadDir(s.directory)
	if err != nil {
		return fmt.Errorf("read messaging transaction directory: %w", err)
	}
	removed := false
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), ".mailbox-") {
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
			return errors.New("abandoned messaging transaction must be a private regular file")
		}
		if err := os.Remove(filepath.Join(s.directory, entry.Name())); err != nil {
			return fmt.Errorf("remove abandoned messaging transaction: %w", err)
		}
		removed = true
	}
	if removed {
		return syncDirectory(s.directory)
	}
	return nil
}

func checkParents(directory string) error {
	for parent := filepath.Dir(directory); ; parent = filepath.Dir(parent) {
		info, err := os.Lstat(parent)
		if err != nil || !info.IsDir() {
			return errors.New("messaging store parents must be existing real directories")
		}
		if filepath.Dir(parent) == parent {
			return nil
		}
	}
}

func checkDirectory(directory string) error {
	if err := checkParents(directory); err != nil {
		return err
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		return errors.New("messaging store must be a real directory with mode 0700")
	}
	return nil
}

func readPrivate(path string, limit int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Size() > limit {
		return nil, errors.New("messaging file must be regular, private and within its size limit")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return nil, errors.New("messaging file changed while opening")
	}
	raw, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || int64(len(raw)) > limit {
		return nil, errors.New("cannot read bounded messaging file")
	}
	return raw, nil
}

func (s *Store) load() error {
	raw, err := readPrivate(filepath.Join(s.directory, "mailbox.json"), maxMigrationBytes)
	if os.IsNotExist(err) {
		s.state = diskState{Version: storeVersion, CapacityBytes: maxStoreBytes, Capabilities: map[string]Principal{}, Messages: map[string]storedMessage{}, Revisions: map[string]uint64{}}
		return nil
	}
	if err != nil {
		return fmt.Errorf("read messaging store: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var state diskState
	if decoder.Decode(&state) != nil || decoder.Decode(new(any)) != io.EOF || !validState(state) {
		return errors.New("invalid messaging store; existing records were not replaced")
	}
	s.state = state
	if state.Version == 1 {
		return s.migrateSequences()
	}
	return nil
}

func validState(state diskState) bool {
	if state.Version == storeVersion && (state.CapacityBytes < maxStoreBytes || state.CapacityBytes > maxMigrationBytes) {
		return false
	}
	if state.Version < storeVersion && state.CapacityBytes != 0 {
		return false
	}
	if (state.Version != 1 && state.Version != 2 && state.Version != storeVersion) || state.Messages == nil || state.Capabilities == nil || state.Revisions == nil || len(state.Messages) > MaxMessages || len(state.Capabilities) > MaxCapabilities || len(state.Revisions) > MaxMessages || !validSequences(state) {
		return false
	}
	principals := make(map[Principal]bool, len(state.Capabilities))
	for hash, principal := range state.Capabilities {
		if !validHash(hash) || !validPrincipal(principal) {
			return false
		}
		principals[principal] = true
	}
	for scope, revision := range state.Revisions {
		parts := strings.Split(scope, "\x00")
		if len(parts) != 2 || !sessionPattern.MatchString(parts[0]) || !idPattern.MatchString(parts[1]) || revision == 0 {
			return false
		}
	}
	for key, stored := range state.Messages {
		message := stored.Message
		principal := Principal{SessionID: message.SessionID, Turn: message.Turn, AgentID: message.Source}
		if key != messageKey(message.SessionID, message.Turn, message.ID) || !validPrincipal(principal) || !validMessageDelivery(message) || message.AcceptedAt.IsZero() || !validHash(stored.Fingerprint) || state.Revisions[scopeKey(message.SessionID, message.Turn)] == 0 {
			return false
		}
		recipient := Principal{SessionID: message.SessionID, Turn: message.Turn, AgentID: message.Target}
		if !principals[principal] || !principals[recipient] {
			return false
		}
		if message.AckedAt != nil && message.AckedAt.Before(message.AcceptedAt) {
			return false
		}
		if message.BodyExpiredAt != nil {
			if message.AckedAt == nil || message.BodyExpiredAt.Sub(*message.AckedAt) < AckRetention || message.Body != "" || !idPattern.MatchString(message.ID) || !validAgent(message.Target) {
				return false
			}
			continue
		}
		request := Request{ID: message.ID, Target: message.Target, Body: message.Body, ReplyTo: message.ReplyTo, Transport: message.Transport}
		if ValidateRequest(request) != nil || requestFingerprint(principal, request) != stored.Fingerprint {
			return false
		}
	}
	return true
}

func validSequences(state diskState) bool {
	if state.Version == 1 && state.LastSequence != 0 {
		return false
	}
	seen := make(map[uint64]bool, len(state.Messages))
	for _, stored := range state.Messages {
		if state.Version == 1 {
			if stored.Sequence != 0 {
				return false
			}
			continue
		}
		if stored.Sequence == 0 || stored.Sequence > state.LastSequence || seen[stored.Sequence] {
			return false
		}
		seen[stored.Sequence] = true
	}
	return true
}

// Old records have no recoverable enqueue order. Preserve their prior ordering
// by time and ID, breaking cross-scope ties by key for deterministic migration.
// Persist before Open returns so dispatch never uses a temporary sequence.
func (s *Store) migrateSequences() error {
	keys := make([]string, 0, len(s.state.Messages))
	for key := range s.state.Messages {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := s.state.Messages[keys[i]].Message, s.state.Messages[keys[j]].Message
		if !a.AcceptedAt.Equal(b.AcceptedAt) {
			return a.AcceptedAt.Before(b.AcceptedAt)
		}
		if a.ID != b.ID {
			return a.ID < b.ID
		}
		return keys[i] < keys[j]
	})
	next := s.copyState()
	next.Version = 2
	for _, key := range keys {
		next.LastSequence++
		stored := next.Messages[key]
		stored.Sequence = next.LastSequence
		next.Messages[key] = stored
	}
	if err := s.commit(context.Background(), next, false); err != nil {
		return fmt.Errorf("migrate messaging enqueue order: %w", err)
	}
	return nil
}

func (s *Store) loadAdmin() error {
	raw, err := readPrivate(s.AdminTokenPath(), 128)
	if os.IsNotExist(err) {
		token, generateErr := newToken()
		if generateErr != nil {
			return generateErr
		}
		file, createErr := os.OpenFile(s.AdminTokenPath(), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if createErr != nil {
			return fmt.Errorf("create messaging administrator credential: %w", createErr)
		}
		_, writeErr := io.WriteString(file, token+"\n")
		syncErr := file.Sync()
		closeErr := file.Close()
		if writeErr != nil || syncErr != nil || closeErr != nil {
			return errors.New("persist messaging administrator credential")
		}
		if err := syncDirectory(s.directory); err != nil {
			return fmt.Errorf("persist messaging credential directory: %w", err)
		}
		raw = []byte(token)
	} else if err != nil {
		return fmt.Errorf("read messaging administrator credential: %w", err)
	}
	token := strings.TrimSpace(string(raw))
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(decoded) != 32 || len(token) != 43 {
		return errors.New("invalid messaging administrator credential")
	}
	s.adminHash = sha256.Sum256([]byte(token))
	return nil
}

// save returns committed=true once rename makes the new state authoritative.
// Cancellation is honored before that point, never misreported as a rollback
// after a successful commit.
func (s *Store) save(ctx context.Context, state diskState) (committed bool, err error) {
	if err := checkDirectory(s.directory); err != nil {
		return false, err
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return false, fmt.Errorf("encode messaging store: %w", err)
	}
	limit := int64(maxStoreBytes)
	if state.Version == storeVersion {
		limit = state.CapacityBytes
	}
	if int64(len(raw)) > limit {
		return false, ErrCapacity
	}
	path := filepath.Join(s.directory, "mailbox.json")
	committed, err = s.fs.Replace(ctx, path, ".mailbox-*", raw, func() error { return s.checkSaveDestination(path) })
	if committed && err != nil {
		return true, fmt.Errorf("%w: %v", ErrOutcomeUnknown, err)
	}
	return committed, err
}

func (s *Store) checkSaveDestination(path string) error {
	if err := checkDirectory(s.directory); err != nil {
		return err
	}
	if info, err := os.Lstat(path); err == nil {
		if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
			return errors.New("messaging state path must be a private regular file")
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("check messaging state path: %w", err)
	}
	return nil
}
