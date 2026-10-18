package messages

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
	"time"

	"agent-observer/internal/atomicfile"
)

const maxStoreBytes = 64 << 20

// Identity binds stored messages to one explicitly selected canonical source.
type Identity struct {
	Provider string `json:"provider"`
	Source   string `json:"source"`
	Session  string `json:"session"`
}

type binding struct {
	ID        string `json:"id"`
	Sender    string `json:"sender"`
	Recipient string `json:"recipient"`
	Source    string `json:"source"`
	Target    string `json:"target"`
	At        string `json:"at"`
}

type storedBody struct {
	Body    string  `json:"body"`
	Origin  string  `json:"origin"`
	Binding binding `json:"binding"`
}

type storedPending struct {
	Body   string    `json:"body"`
	Target string    `json:"target"`
	At     time.Time `json:"receivedAt"`
}

type storedState struct {
	Version  int                      `json:"version"`
	Identity Identity                 `json:"identity"`
	Turn     string                   `json:"turn"`
	Bodies   map[string]storedBody    `json:"bodies"`
	Pending  map[string]storedPending `json:"pending"`
}

// Store is an opt-in local body store, separate from metadata export readers.
// A capture writes only its current turn; historical turns are not replayed.
type Store struct {
	directory string
	fs        atomicfile.Ops
}

var ErrDurabilityUnknown = errors.New("message body committed; local durability unconfirmed")

func OpenStore(directory string) (*Store, error) {
	if !filepath.IsAbs(directory) {
		return nil, errors.New("message store requires an absolute directory")
	}
	directory = filepath.Clean(directory)
	if err := checkParents(directory); err != nil {
		return nil, err
	}
	if err := os.Mkdir(directory, 0700); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("create message store: %w", err)
	}
	s := &Store{directory: directory, fs: atomicfile.Default()}
	if err := s.checkDirectory(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) checkDirectory() error {
	if err := checkParents(s.directory); err != nil {
		return err
	}
	info, err := os.Lstat(s.directory)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		return errors.New("message store must be a real directory with mode 0700")
	}
	return nil
}

func checkParents(directory string) error {
	for parent := filepath.Dir(directory); ; parent = filepath.Dir(parent) {
		info, err := os.Lstat(parent)
		if err != nil || !info.IsDir() {
			return errors.New("message store parents must be existing real directories")
		}
		if filepath.Dir(parent) == parent {
			return nil
		}
	}
}

func (s *Store) filename(identity Identity) string {
	raw, _ := json.Marshal(identity)
	sum := sha256.Sum256(raw)
	return filepath.Join(s.directory, hex.EncodeToString(sum[:])+".json")
}

func (s *Store) load(identity Identity) (*storedState, error) {
	if err := s.checkDirectory(); err != nil {
		return nil, err
	}
	file := s.filename(identity)
	info, err := os.Lstat(file)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return nil, errors.New("message store file must be regular with mode 0600")
	}
	f, err := os.Open(file)
	if err != nil {
		return nil, fmt.Errorf("open message store: %w", err)
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return nil, errors.New("message store file changed while opening")
	}
	raw, err := io.ReadAll(io.LimitReader(f, maxStoreBytes+1))
	if err != nil || len(raw) > maxStoreBytes {
		return nil, errors.New("cannot read bounded message store")
	}
	var state storedState
	if json.Unmarshal(raw, &state) != nil || state.Version != 1 || state.Identity != identity {
		return nil, errors.New("invalid message store identity or format")
	}
	return &state, nil
}

func (s *Store) save(state storedState) (bool, error) {
	raw, err := json.Marshal(state)
	if err != nil || len(raw) > maxStoreBytes {
		return false, errors.New("message store size limit exceeded")
	}
	file := s.filename(state.Identity)
	committed, err := s.fs.Replace(context.Background(), file, ".message-*.tmp", raw, func() error {
		if err := s.checkDirectory(); err != nil {
			return err
		}
		return checkDestination(file)
	})
	if committed && err != nil {
		return true, fmt.Errorf("%w: %v", ErrDurabilityUnknown, err)
	}
	return committed, err
}

func checkDestination(file string) error {
	info, err := os.Lstat(file)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return errors.New("message store destination must be regular with mode 0600")
	}
	return nil
}
