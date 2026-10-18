//go:build !unix

package sources

import (
	"errors"
	"os"
)

func lockCapture(*os.File) error {
	return errors.New("hook file capture currently requires macOS or Linux")
}
func unlockCapture(*os.File) {}
