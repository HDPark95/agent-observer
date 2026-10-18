package transcript

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"agent-observer/internal/model"
)

func TestMalformedCompleteRecordStopsAtBadLineAndRetriesRepair(t *testing.T) {
	for name, bad := range map[string]string{
		"syntax": `{"broken":}` + "\n",
		"null":   "null\n",
		"array":  "[]\n",
		"blank":  "\n",
	} {
		t.Run(name, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "source.jsonl")
			first := "{\"id\":\"first\"}\n"
			last := "{\"id\":\"last\"}\n"
			write := func(data string) {
				t.Helper()
				if err := os.WriteFile(file, []byte(data), 0600); err != nil {
					t.Fatal(err)
				}
			}
			write(first + bad + last)

			cursor := Cursor{Path: file}
			var got []string
			resets := 0
			read := func() error {
				return cursor.Read(func() { resets++ }, func(row model.Record) {
					got = append(got, row.String("id"))
				})
			}
			for attempt := 0; attempt < 2; attempt++ {
				if err := read(); err == nil {
					t.Fatal("complete malformed record accepted")
				}
				if cursor.Offset != int64(len(first)) {
					t.Fatalf("offset advanced past bad line: %d", cursor.Offset)
				}
				if strings.Join(got, ",") != "first" {
					t.Fatalf("records duplicated or suffix consumed: %v", got)
				}
			}

			// Repair the same inode, preserving the already-consumed prefix.
			write(first + "{\"id\":\"fixed\"}\n" + last)
			if err := read(); err != nil {
				t.Fatal(err)
			}
			if strings.Join(got, ",") != "first,fixed,last" {
				t.Fatalf("repair skipped or duplicated records: %v", got)
			}
			if resets != 1 {
				t.Fatalf("repair unexpectedly reset the cursor: %d", resets)
			}
			if err := read(); err != nil {
				t.Fatal(err)
			}
			if len(got) != 3 {
				t.Fatalf("idle read duplicated records: %v", got)
			}
		})
	}
}

func TestIncompleteEOFRemainsRetryable(t *testing.T) {
	for _, tc := range []struct {
		name, partial, finish string
	}{
		{"partial-json", `{"id":`, "\"next\"}\n"},
		{"missing-newline", `{"id":"next"}`, "\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "source.jsonl")
			first := "{\"id\":\"first\"}\n"
			if err := os.WriteFile(file, []byte(first+tc.partial), 0600); err != nil {
				t.Fatal(err)
			}
			cursor := Cursor{Path: file}
			var got []string
			read := func() error {
				return cursor.Read(func() {}, func(row model.Record) {
					got = append(got, row.String("id"))
				})
			}
			for attempt := 0; attempt < 2; attempt++ {
				if err := read(); err != nil {
					t.Fatalf("incomplete EOF should be retryable: %v", err)
				}
				if cursor.Offset != int64(len(first)) || len(got) != 1 {
					t.Fatalf("incomplete record consumed: offset=%d, got=%v",
						cursor.Offset, got)
				}
			}
			f, err := os.OpenFile(file, os.O_WRONLY|os.O_APPEND, 0600)
			if err != nil {
				t.Fatal(err)
			}
			_, writeErr := f.WriteString(tc.finish)
			closeErr := f.Close()
			if writeErr != nil {
				t.Fatal(writeErr)
			}
			if closeErr != nil {
				t.Fatal(closeErr)
			}
			if err := read(); err != nil {
				t.Fatal(err)
			}
			if strings.Join(got, ",") != "first,next" {
				t.Fatalf("completed record missing or duplicated: %v", got)
			}
			wantOffset := int64(len(first + tc.partial + tc.finish))
			if cursor.Offset != wantOffset {
				t.Fatalf("offset=%d, want=%d", cursor.Offset, wantOffset)
			}
		})
	}
}
