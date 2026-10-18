package sources

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"agent-observer/internal/model"
)

// CaptureClaudeHook stores an allowlist of hook metadata in one explicit file.
// It never opens transcript_path or changes Claude's hook/trust configuration.
func CaptureClaudeHook(in io.Reader, output string) error {
	event, err := readClaudeHook(in)
	if err != nil || event == nil {
		return err
	}
	file, err := openCapture(output)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := lockCapture(file); err != nil {
		return err
	}
	defer unlockCapture(file)
	if err := checkCapture(file, event.SessionID); err != nil {
		return err
	}
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode observer metadata: %w", err)
	}
	data = append(data, '\n')
	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("append observer metadata: %w", err)
	}
	return nil
}

func openCapture(output string) (*os.File, error) {
	if !filepath.IsAbs(output) || filepath.Ext(output) != ".jsonl" {
		return nil, errors.New("absolute observer JSONL output path required")
	}
	file, err := os.OpenFile(output, os.O_CREATE|os.O_EXCL|os.O_APPEND|os.O_RDWR, 0600)
	if err == nil {
		return file, nil
	}
	if !errors.Is(err, os.ErrExist) {
		return nil, fmt.Errorf("create observer metadata: %w", err)
	}
	before, err := os.Lstat(output)
	if err != nil || !before.Mode().IsRegular() {
		return nil, errors.New("observer output must be a regular file")
	}
	file, err = os.OpenFile(output, os.O_APPEND|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open observer metadata: %w", err)
	}
	after, err := file.Stat()
	if err != nil || !os.SameFile(before, after) {
		file.Close()
		return nil, errors.New("observer output changed while opening")
	}
	return file, nil
}

// checkCapture keeps providers and native sessions separate without scanning bodies.
func checkCapture(file *os.File, session string) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		return nil
	}
	var last [1]byte
	if _, err := file.ReadAt(last[:], info.Size()-1); err != nil {
		return err
	}
	if last[0] != '\n' {
		return errors.New("observer output ends with an incomplete record")
	}
	line, err := bufio.NewReader(io.NewSectionReader(file, 0, 65537)).ReadBytes('\n')
	if err != nil || len(line) > 65536 {
		return errors.New("invalid observer metadata header")
	}
	var row model.Record
	if json.Unmarshal(line, &row) != nil {
		return errors.New("invalid observer metadata header")
	}
	header, err := decodeClaude(row)
	if err != nil || header.SessionID != session {
		return errors.New("observer output belongs to another session or provider")
	}
	return nil
}
