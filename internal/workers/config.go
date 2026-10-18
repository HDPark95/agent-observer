// Package workers owns explicitly configured Observer workers and their queues.
package workers

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

type Definition struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Task         string `json:"task"`
	Instructions string `json:"instructions"`
}

// Config binds a team to one explicitly selected execution. Executable and CWD
// are operator configuration, never fields accepted from a message sender.
type Config struct {
	Automatic  bool         `json:"automatic,omitempty"`
	Provider   string       `json:"provider,omitempty"`
	SessionID  string       `json:"sessionId"`
	Turn       string       `json:"turn"`
	Owner      string       `json:"owner"`
	Executable string       `json:"executable"`
	CWD        string       `json:"cwd"`
	Workers    []Definition `json:"workers"`
}

func LoadConfig(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, errors.New("cannot open worker configuration")
	}
	defer f.Close()
	var cfg Config
	decoder := json.NewDecoder(io.LimitReader(f, 128<<10))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&cfg) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return Config{}, errors.New("invalid worker configuration JSON")
	}
	validate := cfg.validate
	if cfg.Automatic {
		if cfg.SessionID != "" || cfg.Turn != "" {
			return Config{}, errors.New("automatic teams must not pin a session or turn")
		}
		validate = cfg.validateTemplate
	}
	if err := validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

var workerID = regexp.MustCompile(`^managed:[a-z][a-z0-9_-]{0,63}$`)

func (c Config) validate() error {
	if c.SessionID == "" || c.Turn == "" {
		return errors.New("workers require an execution scope")
	}
	return c.validateTemplate()
}

func (c Config) validateTemplate() error {
	if c.Owner == "" || len(c.Workers) < 1 || len(c.Workers) > 8 {
		return errors.New("workers require an execution scope, owner and 1 to 8 definitions")
	}
	if !filepath.IsAbs(c.Executable) || !filepath.IsAbs(c.CWD) {
		return errors.New("worker executable and directory must be absolute")
	}
	if st, err := os.Stat(c.Executable); err != nil || !st.Mode().IsRegular() || st.Mode()&0111 == 0 {
		return errors.New("worker executable is unavailable")
	}
	if st, err := os.Stat(c.CWD); err != nil || !st.IsDir() {
		return errors.New("worker directory is unavailable")
	}
	seen := make(map[string]bool)
	for _, d := range c.Workers {
		if !workerID.MatchString(d.ID) || d.ID == c.Owner || seen[d.ID] || strings.TrimSpace(d.Name) == "" || len(d.Name) > 120 || len(d.Task) > 240 || len(d.Instructions) > 16000 {
			return errors.New("invalid or duplicate managed worker definition")
		}
		seen[d.ID] = true
	}
	return nil
}
