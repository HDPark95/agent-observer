//go:build darwin || linux

package messaging

import (
	"errors"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"

	"agent-observer/internal/atomicfile"
)

func lockDirectory(directory string) (*os.File, error) {
	fd, err := unix.Open(filepath.Join(directory, ".lock"), unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return nil, errors.New("cannot open messaging lock")
	}
	file := os.NewFile(uintptr(fd), "messaging lock")
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		file.Close()
		return nil, errors.New("messaging lock must be a private regular file")
	}
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		file.Close()
		return nil, ErrLocked
	}
	return file, nil
}

func syncDirectory(path string) error { return atomicfile.SyncDir(path) }
