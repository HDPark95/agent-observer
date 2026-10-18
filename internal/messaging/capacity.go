package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
)

// The validator permits 12,000 runes. JSON may encode each as six bytes.
// The remainder covers receipt IDs, provider, state, error and timestamps.
const deliveryEnvelopeBytes = 6*MaxBodyRunes + 2048
const maxMigrationBytes = maxStoreBytes + MaxMessages*(deliveryEnvelopeBytes+512)

func reservedBytes(state diskState) (int64, error) {
	raw, err := json.Marshal(state)
	if err != nil {
		return 0, err
	}
	size := int64(len(raw) + 60) // capacity and sequence counters can grow to 20 digits.
	for _, revision := range state.Revisions {
		size += int64(20 - len(strconv.FormatUint(revision, 10)))
	}
	for _, stored := range state.Messages {
		m := stored.Message
		if IsManagedTransport(m.Transport) && !terminalMessage(m) {
			current, err := json.Marshal(m.Delivery)
			if err != nil {
				return 0, err
			}
			size += max(0, int64(deliveryEnvelopeBytes-len(current)))
		}
		size += 256 // Ack/expiration metadata, including maximum timestamp widths.
	}
	return size, nil
}

func checkAdmission(state diskState) error {
	if len(state.Messages) > MaxMessages || len(state.Capabilities) > MaxCapabilities || len(state.Revisions) > MaxMessages {
		return ErrCapacity
	}
	size, err := reservedBytes(state)
	if err != nil {
		return err
	}
	if size > maxStoreBytes {
		return ErrCapacity
	}
	return nil
}

// Existing accepted work may predate reservations. Persist a bounded migration
// allowance before dispatch, while all NEW admission still uses 32 MiB.
func (s *Store) migrateCapacity() error {
	if s.state.Version == storeVersion {
		return nil
	}
	next := s.copyState()
	next.Version = storeVersion
	next.CapacityBytes = maxStoreBytes
	size, err := reservedBytes(next)
	if err != nil {
		return err
	}
	next.CapacityBytes = max(int64(maxStoreBytes), size)
	if next.CapacityBytes > maxMigrationBytes {
		return ErrCapacity
	}
	return s.commit(context.Background(), next, false)
}

// New participants cannot consume space already promised to active deliveries.
func (s *Store) admit(ctx context.Context, build func() diskState) (diskState, error) {
	next := build()
	if err := checkAdmission(next); !errors.Is(err, ErrCapacity) {
		return next, err
	}
	if err := s.compactLocked(ctx); err != nil {
		return diskState{}, err
	}
	next = build()
	if err := checkAdmission(next); err != nil {
		return diskState{}, err
	}
	return next, nil
}
