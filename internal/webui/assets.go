// Package webui embeds the browser UI in the Go executable.
package webui

import _ "embed"

//go:embed index.html
var Index []byte
