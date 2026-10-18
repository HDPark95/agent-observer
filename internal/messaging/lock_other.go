//go:build !darwin && !linux

package messaging

import (
	"errors"
	"os"
)

func lockDirectory(string) (*os.File, error) {
	return nil, errors.New("persistent messaging is supported on macOS and Linux")
}

func syncDirectory(string) error {
	return errors.New("messaging directory sync is unsupported")
}
