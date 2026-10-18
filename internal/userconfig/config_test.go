package userconfig

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConfigRoundTripAndNoOverwrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "config.json")
	cfg := Config{Version: 1, Port: 18767, CatalogRoot: filepath.Join(t.TempDir(), "not-created")}
	if err := SaveNew(path, cfg); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil || got != cfg {
		t.Fatalf("got %+v, %v", got, err)
	}
	if err := SaveNew(path, Default()); err == nil {
		t.Fatal("overwrote existing configuration")
	}
	if _, err := os.Stat(cfg.CatalogRoot); !os.IsNotExist(err) {
		t.Fatal("configuration must not create catalog")
	}
	if got.Endpoint() != "http://127.0.0.1:18767" {
		t.Fatal(got.Endpoint())
	}
}

func TestConfigRejectsAmbiguousOrInvalidInputs(t *testing.T) {
	for _, input := range []string{
		`null`, `[]`, `{}`, `{"version":2,"port":8767}`,
		`{"version":1,"port":80}`, `{"version":1,"port":65536}`,
		`{"version":1,"port":8767,"port":1234}`,
		`{"version":1,"port":8767,"Port":1234}`,
		`{"version":1,"port":8767,"extra":true}`,
		`{"version":1,"port":8767,"catalog_root":null}`,
		`{"version":1,"port":8767,"catalog_root":"relative"}`,
		`{"version":1,"port":8767} {}`,
	} {
		t.Run(input, func(t *testing.T) {
			if _, err := decode([]byte(input)); err == nil {
				t.Fatal("accepted invalid input")
			}
		})
	}
}

func TestConfigFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if _, err := Load(path); err == nil {
		t.Fatal("missing explicit config accepted")
	}
	if _, err := Load("relative"); err == nil {
		t.Fatal("relative config accepted")
	}
	if err := os.WriteFile(path, []byte(`{"version":1,"port":8767}`), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("public config accepted")
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(link); err == nil {
		t.Fatal("symlink accepted")
	}
	if err := os.WriteFile(path, []byte(strings.Repeat(" ", 8193)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("oversized config accepted")
	}
}
