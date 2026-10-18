package delivery

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
)

const maxPendingRecords = 100000

type partialBatch struct {
	ID            string `json:"id"`
	RejectedSpans int64  `json:"rejectedSpans"`
}

type receipt struct {
	Version     int           `json:"version"`
	Key         string        `json:"key"`
	Fingerprint string        `json:"fingerprint"`
	Accepted    bool          `json:"accepted,omitempty"`
	Rejected    bool          `json:"rejected,omitempty"`
	Partial     *partialBatch `json:"partial,omitempty"`
}

func terminal(r record) bool {
	return r.Accepted || r.Rejected || r.Partial != nil
}

func compactReceipt(key string, r record) receipt {
	return receipt{
		Version: 1, Key: key, Fingerprint: r.Fingerprint,
		Accepted: r.Accepted, Rejected: r.Rejected, Partial: r.Partial,
	}
}

func (s *sink) install(key string, r record) {
	if terminal(r) {
		delete(s.records, key)
		s.receipts[key] = compactReceipt(key, r)
		return
	}
	delete(s.receipts, key)
	s.records[key] = r
}

func (s *sink) fingerprint(key string) (string, bool) {
	if r, ok := s.records[key]; ok {
		return r.Fingerprint, true
	}
	r, ok := s.receipts[key]
	return r.Fingerprint, ok
}

func validDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size &&
		hex.EncodeToString(decoded) == value
}

func decodeEntry(name string, data []byte) (string, record, bool, error) {
	invalid := errors.New("invalid outbox record")
	var header struct {
		Version int `json:"version"`
	}
	if json.Unmarshal(data, &header) != nil {
		return "", record{}, false, invalid
	}
	if header.Version != 0 {
		var c receipt
		if json.Unmarshal(data, &c) != nil || c.Version != 1 ||
			!validDigest(c.Key) || !validDigest(c.Fingerprint) {
			return "", record{}, false, invalid
		}
		states := 0
		if c.Accepted {
			states++
		}
		if c.Rejected {
			states++
		}
		if c.Partial != nil {
			states++
			if !validDigest(c.Partial.ID) || c.Partial.RejectedSpans <= 0 {
				return "", record{}, false, invalid
			}
		}
		if states != 1 || name != digest([]byte(c.Key))+".json" {
			return "", record{}, false, invalid
		}
		return c.Key, record{
			Fingerprint: c.Fingerprint, Accepted: c.Accepted,
			Rejected: c.Rejected, Partial: c.Partial,
		}, false, nil
	}

	var r record
	if json.Unmarshal(data, &r) != nil || r.Observation.ID == "" {
		return "", record{}, false, invalid
	}
	key := r.Observation.Key()
	encoded, err := json.Marshal(r.Observation)
	if err != nil || r.Fingerprint != digest(encoded) ||
		name != digest([]byte(key))+".json" {
		return "", record{}, false, errors.New("outbox identity mismatch")
	}
	if r.Accepted && r.Rejected || r.Partial != nil {
		return "", record{}, false, invalid
	}
	return key, r, terminal(r), nil
}
func batchPartial(err error, keys []string, records []record) *partialBatch {
	var result interface {
		PartialAcceptance() (int64, bool)
	}
	if !errors.As(err, &result) {
		return nil
	}
	rejected, partial := result.PartialAcceptance()
	if !partial || rejected <= 0 {
		return nil
	}
	identities := make([]string, len(keys))
	for i, key := range keys {
		identities[i] = key + ":" + records[i].Fingerprint
	}
	data, _ := json.Marshal(identities)
	return &partialBatch{ID: digest(data), RejectedSpans: rejected}
}

// confirmSink repairs an uncertain local directory sync without re-exporting.
// The caller holds b.mu; uncertainty in one destination does not stop others.
func (b *Outbox) confirmSink(s *sink) error {
	if !s.syncPending {
		return nil
	}
	if err := b.fs.SyncDir(b.sinkDirectory(s)); err != nil {
		s.lastError = "receipt-sync"
		return errors.New("cannot confirm outbox directory")
	}
	s.syncPending = false
	if s.lastError == "receipt-sync" {
		s.lastError = ""
	}
	return nil
}
