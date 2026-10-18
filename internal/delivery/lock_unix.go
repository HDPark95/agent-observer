//go:build darwin || linux

package delivery

import (
	"errors"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
)

func lockDirectory(directory string) (*os.File, error) {
	fd, err := unix.Open(filepath.Join(directory, ".lock"), unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return nil, errors.New("cannot open outbox lock")
	}
	f := os.NewFile(uintptr(fd), "outbox lock")
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		f.Close()
		return nil, errors.New("outbox already in use")
	}
	return f, nil
}

func unlockDirectory(f *os.File) error { return f.Close() }
