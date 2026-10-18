package atomicfile

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

var errInjected = errors.New("errInjected storage failure")

type faultFile struct {
	File
	stage string
}

func (f faultFile) Write(p []byte) (int, error) {
	if f.stage == "write" {
		return 0, errInjected
	}
	if f.stage == "short" {
		return len(p) - 1, nil
	}
	return f.File.Write(p)
}
func (f faultFile) Sync() error {
	if f.stage == "sync" {
		return errInjected
	}
	return f.File.Sync()
}
func (f faultFile) Close() error {
	err := f.File.Close()
	if f.stage == "close" {
		return errInjected
	}
	return err
}

func TestReplaceCommitBoundary(t *testing.T) {
	for _, stage := range []string{"create", "write", "short", "sync", "close", "validate", "cancel", "rename", "directory", "success"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "state.json")
			if err := os.WriteFile(path, []byte("old"), 0600); err != nil {
				t.Fatal(err)
			}
			fs := Default()
			create := fs.CreateTemp
			fs.CreateTemp = func(dir, pattern string) (File, error) {
				if stage == "create" {
					return nil, errInjected
				}
				f, err := create(dir, pattern)
				if err != nil {
					return nil, err
				}
				return faultFile{f, stage}, nil
			}
			if stage == "rename" {
				fs.Rename = func(string, string) error { return errInjected }
			}
			if stage == "directory" {
				fs.SyncDir = func(string) error { return errInjected }
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			validations := 0
			committed, err := fs.Replace(ctx, path, ".test-*", []byte("new"), func() error {
				validations++
				if validations == 2 {
					if stage == "validate" {
						return errInjected
					}
					if stage == "cancel" {
						cancel()
					}
				}
				return nil
			})
			wantCommit := stage == "directory" || stage == "success"
			if committed != wantCommit || (err == nil) != (stage == "success") {
				t.Fatalf("commit=%v error=%v", committed, err)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			want := "old"
			if wantCommit {
				want = "new"
			}
			if string(data) != want {
				t.Fatalf("file=%q", data)
			}
			entries, err := os.ReadDir(dir)
			if err != nil || len(entries) != 1 {
				t.Fatalf("temporary file leaked: %d %v", len(entries), err)
			}
			info, err := os.Stat(path)
			if err != nil || info.Mode().Perm() != 0600 {
				t.Fatal("private file mode lost")
			}
		})
	}
}
