package messaging

import (
	"context"
	"sort"
)

// FindTeamStart checks a start ID across turns of one selected session. The
// original durable message is also the binding receipt: a retry after a turn
// change must not launch the same task in the new turn. No transcript is read.
func (s *Store) FindTeamStart(ctx context.Context, session, owner string, req Request) (Message, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkContext(ctx); err != nil {
		return Message{}, false, err
	}
	if !sessionPattern.MatchString(session) || !validAgent(owner) || ValidateRequest(req) != nil || !IsManagedTransport(req.Transport) {
		return Message{}, false, ErrInvalid
	}
	var found Message
	for _, stored := range s.teamStartCandidates(session, owner, req.ID) {
		m := stored.Message
		if m.SessionID != session || m.Source != owner || m.ID != req.ID {
			continue
		}
		principal := Principal{SessionID: session, Turn: m.Turn, AgentID: owner}
		if found.ID != "" || stored.Fingerprint != requestFingerprint(principal, req) {
			return Message{}, false, ErrConflict
		}
		full, _, err := s.lookupMessage(messageKey(m.SessionID, m.Turn, m.ID))
		if err != nil {
			return Message{}, false, err
		}
		found = cloneMessage(full.Message)
	}
	if err := s.confirmDurability(); err != nil {
		return Message{}, false, err
	}
	return found, found.ID != "", nil
}

// PendingTeams returns only delivery metadata already authorized and persisted
// by this store. Restart recovery does not discover or read session transcripts.
func (s *Store) PendingTeams(owner string) []Principal {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := make(map[Principal]bool)
	for _, stored := range s.state.Messages {
		m := stored.Message
		if !IsManagedTransport(m.Transport) || m.Delivery.State == "completed" || m.Delivery.State == "unknown" {
			continue
		}
		p := Principal{SessionID: m.SessionID, Turn: m.Turn, AgentID: owner}
		if s.enrolled(p) {
			seen[p] = true
		}
	}
	result := make([]Principal, 0, len(seen))
	for p := range seen {
		result = append(result, p)
	}
	sort.Slice(result, func(i, j int) bool {
		return scopeKey(result[i].SessionID, result[i].Turn) < scopeKey(result[j].SessionID, result[j].Turn)
	})
	return result
}

func (s *Store) teamStartCandidates(session, owner, id string) []storedMessage {
	result := []storedMessage{}
	for key, r := range s.archive.messages {
		if _, active := s.state.Messages[key]; active {
			continue
		}
		m := r.Stored.Message
		if m.SessionID == session && m.Source == owner && m.ID == id {
			result = append(result, r.Stored)
		}
	}
	for _, stored := range s.state.Messages {
		m := stored.Message
		if m.SessionID == session && m.Source == owner && m.ID == id {
			result = append(result, stored)
		}
	}
	return result
}
