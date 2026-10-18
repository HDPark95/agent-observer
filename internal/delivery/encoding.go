package delivery

import (
	"encoding/json"
	"errors"
	"time"
)

const maxEntryBytes = (128 << 10) + 64
const maxAttempts = int(^uint(0) >> 1)

func encodeEntry(key string, r record) ([]byte, error) {
	var value any = r
	if terminal(r) {
		value = compactReceipt(key, r)
	} else {
		if r.Attempts < 0 {
			return nil, errors.New("invalid outbox attempts")
		}
		envelope := r
		envelope.Attempts = maxAttempts
		envelope.NextAttempt = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.FixedZone("", 23*3600+59*60))
		raw, err := json.Marshal(envelope)
		if err != nil || len(raw) > maxEntryBytes {
			return nil, errors.New("outbox retry capacity exceeded")
		}
	}
	data, err := json.Marshal(value)
	if err != nil || len(data) > maxEntryBytes {
		return nil, errors.New("outbox entry size exceeded")
	}
	return data, nil
}
