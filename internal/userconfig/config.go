// Package userconfig stores opt-in local discovery settings, never credentials
// or transcript contents. Loading configuration does not enumerate sessions.
package userconfig

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const DefaultPort = 8767

type Config struct {
	Version     int    `json:"version"`
	Port        int    `json:"port"`
	CatalogRoot string `json:"catalog_root,omitempty"`
}

func Default() Config { return Config{Version: 1, Port: DefaultPort} }

func DefaultPath() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", errors.New("cannot locate user configuration; supply --user-config")
	}
	return filepath.Join(dir, "agent-observer", "config.json"), nil
}

func (c Config) Endpoint() string { return fmt.Sprintf("http://127.0.0.1:%d", c.Port) }

func Validate(c Config) error {
	if c.Version != 1 {
		return errors.New("unsupported user configuration version")
	}
	if c.Port < 1024 || c.Port > 65535 {
		return errors.New("port must be between 1024 and 65535")
	}
	if c.CatalogRoot != "" && !filepath.IsAbs(c.CatalogRoot) {
		return errors.New("catalog root must be absolute")
	}
	return nil
}

// Load treats only a missing implicit config as a fresh installation.
func Load(path string) (Config, error) {
	explicit := path != ""
	if !explicit {
		var err error
		path, err = DefaultPath()
		if err != nil {
			return Config{}, err
		}
	}
	if !filepath.IsAbs(path) {
		return Config{}, errors.New("user configuration path must be absolute")
	}
	st, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) && !explicit {
		return Default(), nil
	}
	if err != nil {
		return Config{}, errors.New("cannot read user configuration")
	}
	if !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 || st.Size() > 8192 {
		return Config{}, errors.New("user configuration must be a private regular file of at most 8192 bytes")
	}
	if !owned(st) || !privateDirectory(filepath.Dir(path)) {
		return Config{}, errors.New("user configuration and its directory must be private and owned by the current user")
	}
	f, err := os.Open(path)
	if err != nil {
		return Config{}, errors.New("cannot open user configuration")
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(st, opened) || opened.Mode().Perm()&0077 != 0 || !owned(opened) {
		return Config{}, errors.New("user configuration changed while opening")
	}
	data, err := io.ReadAll(io.LimitReader(f, 8193))
	if err != nil || len(data) > 8192 {
		return Config{}, errors.New("cannot read bounded user configuration")
	}
	return decode(data)
}

func decode(data []byte) (Config, error) {
	// This flat object must not contain duplicate settings that different
	// clients might interpret differently.
	d := json.NewDecoder(bytes.NewReader(data))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return Config{}, errors.New("user configuration must be an object")
	}
	seen := map[string]bool{}
	for d.More() {
		key, err := d.Token()
		name, ok := key.(string)
		if err != nil || !ok || seen[name] || name != "version" && name != "port" && name != "catalog_root" {
			return Config{}, errors.New("invalid or duplicate user configuration key")
		}
		seen[name] = true
		var value json.RawMessage
		if d.Decode(&value) != nil || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return Config{}, errors.New("invalid user configuration value")
		}
	}
	d = json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	var c Config
	if d.Decode(&c) != nil || d.Decode(&struct{}{}) != io.EOF {
		return Config{}, errors.New("invalid user configuration JSON")
	}
	return c, Validate(c)
}

// SaveNew never replaces an existing configuration or starts a collector.
func SaveNew(path string, c Config) error {
	if err := Validate(c); err != nil {
		return err
	}
	if !filepath.IsAbs(path) {
		return errors.New("user configuration path must be absolute")
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return errors.New("cannot create user configuration directory")
	}
	if !privateDirectory(dir) {
		return errors.New("user configuration directory must be private and not a symlink")
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return errors.New("cannot create configuration; existing files are never overwritten")
	}
	err = json.NewEncoder(f).Encode(c)
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		_ = os.Remove(path)
		return errors.New("cannot save user configuration")
	}
	return nil
}

func privateDirectory(path string) bool {
	st, err := os.Lstat(path)
	return err == nil && st.IsDir() && st.Mode().Perm()&0077 == 0 && owned(st)
}
