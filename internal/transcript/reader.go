// Package transcript reads complete JSONL records incrementally without retaining raw lines.
package transcript

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"agent-observer/internal/model"
)

// Cursor belongs to one selected file. Callers serialize access.
type Cursor struct {
	Path     string
	Offset   int64
	identity os.FileInfo
	// ValidateHeader checks identity on the descriptor used to consume records.
	ValidateHeader func(model.Record) error
}

func (c *Cursor) Read(reset func(), consume func(model.Record)) error {
	return c.ReadChecked(reset, func(row model.Record) error {
		consume(row)
		return nil
	})
}

// ReadChecked commits a complete record's offset only after successful
// decoding and consumption.
func (c *Cursor) ReadChecked(
	reset func(),
	consume func(model.Record) error,
) error {
	f, err := os.Open(c.Path)
	if err != nil {
		return err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return err
	}
	if !st.Mode().IsRegular() {
		return errors.New("not a regular transcript")
	}

	if c.ValidateHeader != nil {
		ready, err := c.checkHeader(f)
		if err != nil || !ready {
			return err
		}
	}

	if c.identity == nil || !os.SameFile(st, c.identity) ||
		st.Size() < c.Offset {
		reset()
		c.Offset = 0
		c.identity = st
	}
	if _, err := f.Seek(c.Offset, io.SeekStart); err != nil {
		return err
	}

	reader := bufio.NewReader(f)
	for {
		line, err := reader.ReadBytes('\n')
		if err == io.EOF {
			// No newline: leave the offset at the start of this record.
			return nil
		}
		if err != nil {
			return err
		}
		row, err := decodeRecord(line, c.Offset)
		if err != nil {
			return err
		}
		if err := consume(row); err != nil {
			return err
		}
		c.Offset += int64(len(line))
	}
}

func decodeRecord(line []byte, offset int64) (model.Record, error) {
	var row model.Record
	if err := json.Unmarshal(line, &row); err != nil || row == nil {
		// Do not expose transcript contents through parser error text.
		return nil, fmt.Errorf("invalid JSONL object at byte %d", offset)
	}
	return row, nil
}

func (c *Cursor) checkHeader(f *os.File) (bool, error) {
	const limit = 65536
	reader := bufio.NewReader(io.LimitReader(f, limit+1))
	line, err := reader.ReadBytes('\n')
	if len(line) > limit {
		return false, errors.New("header too large")
	}
	if err == io.EOF {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	row, err := decodeRecord(line, 0)
	if err != nil {
		return false, err
	}
	if err := c.ValidateHeader(row); err != nil {
		return false, err
	}
	return true, nil
}

// Header reads a bounded metadata header and rejects symlinks and nonregular files.
func Header(path string, max int) (model.Record, error) {
	st, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() {
		return nil, errors.New("invalid transcript")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	line, err := bufio.NewReader(io.LimitReader(f, int64(max+1))).ReadBytes('\n')
	if len(line) > max {
		return nil, errors.New("header too large")
	}
	if err != nil && err != io.EOF {
		return nil, err
	}
	var r model.Record
	err = json.Unmarshal(line, &r)
	return r, err
}
