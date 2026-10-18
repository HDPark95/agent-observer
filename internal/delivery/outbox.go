// Package delivery owns durable metadata delivery independently of graph reads.
package delivery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"agent-observer/internal/atomicfile"
	"agent-observer/internal/domain"
)

// Exporter sends one batch. A nil error means accepted, not verified storage.
type Exporter interface {
	Validate(domain.Observation) error
	Export(context.Context, []domain.Observation) error
}

// Destination ID must change when the endpoint or project changes, not when a credential rotates.
type Destination struct {
	ID       string
	Exporter Exporter
}

type record struct {
	Observation domain.Observation `json:"observation"`
	Fingerprint string             `json:"fingerprint"`
	Partial     *partialBatch      `json:"partial,omitempty"`
	Accepted    bool               `json:"accepted"`
	Rejected    bool               `json:"rejected"`
	Attempts    int                `json:"attempts"`
	NextAttempt time.Time          `json:"nextAttempt"`
}
type sink struct {
	destination Destination
	records     map[string]record
	lastError   string
	receipts    map[string]receipt
	syncPending bool
}

type Status struct {
	Destination    string `json:"destination"`
	Pending        int    `json:"pending"`
	PartialUnknown int    `json:"partialUnknown"`
	Accepted       int    `json:"accepted"`
	Rejected       int    `json:"rejected"`
	LastError      string `json:"lastError,omitempty"`
}

type Outbox struct {
	fs        atomicfile.Ops
	mu        sync.Mutex
	directory string
	sinks     []*sink
	lock      *os.File
}

// New loads only this explicitly configured outbox. Files contain typed metadata, never credentials.
func New(directory string, destinations []Destination) (*Outbox, error) {
	if !filepath.IsAbs(directory) {
		return nil, errors.New("outbox directory must be absolute")
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, errors.New("cannot create outbox directory")
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("invalid outbox directory")
	}
	lock, err := lockDirectory(directory)
	if err != nil {
		return nil, err
	}
	box := &Outbox{directory: directory, lock: lock, fs: atomicfile.Default()}
	loaded := false
	defer func() {
		if !loaded {
			box.Close()
		}
	}()
	seen := map[string]bool{}
	for _, d := range destinations {
		if d.ID == "" || d.Exporter == nil || seen[d.ID] {
			return nil, errors.New("invalid or duplicate destination")
		}
		seen[d.ID] = true
		s := &sink{
			destination: d,
			records:     map[string]record{},
			receipts:    map[string]receipt{},
		}
		if err := box.load(s); err != nil {
			return nil, err
		}
		box.sinks = append(box.sinks, s)
	}
	loaded = true
	return box, nil
}

// Close releases the process lock after Run has stopped.
func (b *Outbox) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.lock == nil {
		return nil
	}
	err := unlockDirectory(b.lock)
	b.lock = nil
	return err
}
func digest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func (b *Outbox) sinkDirectory(s *sink) string {
	return filepath.Join(b.directory, digest([]byte(s.destination.ID)))
}
func (b *Outbox) load(s *sink) error {
	dir := b.sinkDirectory(s)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return errors.New("cannot create destination outbox")
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("invalid destination outbox")
	}
	f, err := os.Open(dir)
	if err != nil {
		return errors.New("cannot read outbox")
	}
	defer f.Close()
	for {
		entries, readErr := f.ReadDir(256)
		for _, entry := range entries {
			if filepath.Ext(entry.Name()) != ".json" {
				continue
			}
			if err := b.loadEntry(s, entry); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			return b.fs.SyncDir(dir)
		}
		if readErr != nil {
			return errors.New("cannot read outbox")
		}
	}
}

func (b *Outbox) loadEntry(s *sink, entry os.DirEntry) error {
	info, err := entry.Info()
	if err != nil || !info.Mode().IsRegular() || info.Size() > maxEntryBytes {
		return errors.New("invalid outbox entry")
	}
	f, err := os.Open(filepath.Join(b.sinkDirectory(s), entry.Name()))
	if err != nil {
		return errors.New("cannot open outbox entry")
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return errors.New("outbox entry changed while opening")
	}
	data, err := io.ReadAll(io.LimitReader(f, int64(maxEntryBytes)+1))
	if err != nil || len(data) > maxEntryBytes {
		return errors.New("cannot read bounded outbox entry")
	}
	key, r, migrate, err := decodeEntry(entry.Name(), data)
	if err != nil {
		return err
	}
	if _, err := encodeEntry(key, r); err != nil {
		return err
	}
	if !terminal(r) && len(s.records) >= maxPendingRecords {
		return errors.New("outbox pending limit exceeded")
	}
	if migrate {
		return b.save(s, key, r)
	}
	s.install(key, r)
	return nil
}
func (b *Outbox) save(s *sink, key string, r record) error {
	if err := b.confirmSink(s); err != nil {
		return err
	}
	data, err := encodeEntry(key, r)
	if err != nil {
		return err
	}
	path := filepath.Join(b.sinkDirectory(s), digest([]byte(key))+".json")
	committed, err := b.fs.Replace(context.Background(), path, ".pending-", data, nil)
	if committed {
		s.install(key, r)
		s.syncPending = err != nil
	}
	if err != nil {
		return errors.New("cannot persist outbox entry")
	}
	return nil
}

// Submit stores only changed observations. It is called by the background collector, never SSE.
func (b *Outbox) Submit(observations []domain.Observation) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, s := range b.sinks {
		if err := b.confirmSink(s); err != nil {
			return err
		}
	}
	for _, o := range observations {
		if o.ID == "" {
			return errors.New("invalid observation")
		}
		data, err := json.Marshal(o)
		if err != nil {
			return errors.New("cannot encode observation")
		}
		key, fingerprint := o.Key(), digest(data)
		for _, s := range b.sinks {
			if old, ok := s.fingerprint(key); ok && old == fingerprint {
				continue
			}
			r := record{Observation: o, Fingerprint: fingerprint}
			if s.destination.Exporter.Validate(o) != nil {
				r.Rejected = true
			}
			if _, pending := s.records[key]; !pending &&
				!terminal(r) && len(s.records) >= maxPendingRecords {
				return errors.New("outbox pending limit reached")
			}
			if err := b.save(s, key, r); err != nil {
				s.lastError = "outbox-write"
				return err
			}
		}
	}
	return nil
}
func (b *Outbox) Status() []Status {
	b.mu.Lock()
	defer b.mu.Unlock()
	result := make([]Status, 0, len(b.sinks))
	for _, s := range b.sinks {
		v := Status{Destination: s.destination.ID, LastError: s.lastError, Pending: len(s.records)}
		for _, r := range s.receipts {
			switch {
			case r.Accepted:
				v.Accepted++
			case r.Rejected:
				v.Rejected++
			case r.Partial != nil:
				v.PartialUnknown++
			}
		}
		result = append(result, v)
	}
	return result
}

// Run gives each destination a worker, so a failed backend cannot hold up others.
// Exactly one Run may be active for an Outbox instance.
func (b *Outbox) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, s := range b.sinks {
		wg.Add(1)
		go func(s *sink) {
			defer wg.Done()
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				b.flush(ctx, s)
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
			}
		}(s)
	}
	wg.Wait()
}
func (b *Outbox) batch(s *sink) ([]string, []record) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.confirmSink(s); err != nil {
		return nil, nil
	}
	keys := []string{}
	for key, r := range s.records {
		if !r.Accepted && !r.Rejected && !time.Now().Before(r.NextAttempt) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	if len(keys) > 64 {
		keys = keys[:64]
	}
	values := make([]record, 0, len(keys))
	for _, key := range keys {
		values = append(values, s.records[key])
	}
	return keys, values
}
func (b *Outbox) flush(ctx context.Context, s *sink) {
	keys, records := b.batch(s)
	if len(keys) == 0 || ctx.Err() != nil {
		return
	}
	observations := make([]domain.Observation, len(records))
	for i, r := range records {
		observations[i] = r.Observation
	}
	sendCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err := s.destination.Exporter.Export(sendCtx, observations)
	cancel()
	if ctx.Err() != nil {
		return
	} // Keep in-flight records pending for the next process.
	partial := batchPartial(err, keys, records)
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, key := range keys {
		r := records[i]
		current, exists := s.records[key]
		if !exists || current.Fingerprint != r.Fingerprint {
			continue
		} // A newer state arrived during HTTP.
		if r.Attempts < maxAttempts {
			r.Attempts++
		}
		if err == nil {
			r.Accepted = true
			s.lastError = ""
		} else if partial != nil {
			r.Partial = partial
			r.NextAttempt = time.Time{}
			s.lastError = "partial-acceptance"
		} else {
			s.lastError = "export-failed"
			var classified interface {
				Retryable() bool
				RetryDelay() time.Duration
			}
			delay := time.Duration(1<<min(r.Attempts, 6)) * time.Second
			if errors.As(err, &classified) {
				r.Rejected = !classified.Retryable()
				if classified.RetryDelay() > delay {
					delay = classified.RetryDelay()
				}
			}
			r.NextAttempt = time.Now().Add(delay)
		}
		if saveErr := b.save(s, key, r); saveErr != nil {
			s.lastError = "receipt-write"
		}
	}
}

// FingerprintDestination avoids placing endpoints or project names in status or filenames.
func FingerprintDestination(kind, endpoint, project string) string {
	data, _ := json.Marshal([]string{kind, endpoint, project})
	return fmt.Sprintf("%s-%s", kind, digest(data)[:16])
}
