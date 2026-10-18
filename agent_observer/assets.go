// Package assets embeds the existing UI in the Go executable.
package assets

import _ "embed"

//go:embed index.html
var Index []byte
