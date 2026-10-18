// Package atomicfile replaces a local file while exposing the rename commit
// point. Callers own locking, permissions, capacity, and recovery policy.
package atomicfile

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
)

type File interface {
	io.Writer
	Name() string
	Sync() error
	Close() error
}

// Ops is instance-owned so fault tests cannot affect concurrent stores.
type Ops struct {
	CreateTemp func(string, string) (File, error)
	Rename     func(string, string) error
	Remove     func(string) error
	SyncDir    func(string) error
}

func Default() Ops {
	return Ops{CreateTemp: func(dir, pattern string) (File, error) { return os.CreateTemp(dir, pattern) }, Rename: os.Rename, Remove: os.Remove, SyncDir: SyncDir}
}

func SyncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

// Replace returns committed=true after rename, even if directory sync fails.
// A caller must not roll back memory after that point. validate is repeated
// immediately before commit; cancellation is honored only before rename.
func (fs Ops) Replace(ctx context.Context, path, pattern string, data []byte, validate func() error) (committed bool, err error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if validate != nil {
		if err := validate(); err != nil {
			return false, err
		}
	}
	f, err := fs.CreateTemp(filepath.Dir(path), pattern)
	if err != nil {
		return false, err
	}
	defer fs.Remove(f.Name())
	if err := writeAndClose(f, data); err != nil {
		return false, err
	}
	if validate != nil {
		if err := validate(); err != nil {
			return false, err
		}
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := fs.Rename(f.Name(), path); err != nil {
		return false, err
	}
	return true, fs.SyncDir(filepath.Dir(path))
}

func writeAndClose(f File, data []byte) error {
	n, err := f.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = f.Sync()
	}
	return errors.Join(err, f.Close())
}
