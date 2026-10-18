//go:build unix

package sources

import (
	"errors"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

// Bound contention so observation never holds up the agent indefinitely.
func lockCapture(file *os.File) error {
	deadline := time.Now().Add(200 * time.Millisecond)
	for {
		err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) || time.Now().After(deadline) {
			return errors.New("observer metadata file is busy or cannot be locked")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func unlockCapture(file *os.File) { _ = unix.Flock(int(file.Fd()), unix.LOCK_UN) }
