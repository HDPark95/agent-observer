//go:build !darwin && !linux

package userconfig

import "os"

// Private persistent configuration is not supported on other platforms yet.
func owned(os.FileInfo) bool { return false }
