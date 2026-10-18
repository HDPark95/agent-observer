//go:build !darwin && !linux

package delivery

import (
	"errors"
	"os"
)

func lockDirectory(string) (*os.File, error) {
	return nil, errors.New("persistent export is supported on macOS and Linux")
}
func unlockDirectory(f *os.File) error { return f.Close() }
