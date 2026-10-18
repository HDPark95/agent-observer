package normalize

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"unicode"

	"agent-observer/internal/model"
)

var wordName = regexp.MustCompile(`^[\pL\pN_.-]{1,64}$`)

func displayName(s string) string {
	n := filepath.Base(s)
	if !wordName.MatchString(n) || strings.HasPrefix(n, ".") {
		return "파일"
	}
	return n
}

// shellWords lexes a command for labels only. It never evaluates or executes shell text.
func shellWords(s string) []string {
	var words []string
	var b strings.Builder
	var quote rune
	escaped := false
	active := false
	for _, c := range s {
		if escaped {
			b.WriteRune(c)
			escaped = false
			active = true
			continue
		}
		if c == '\\' && quote != '\'' {
			escaped = true
			continue
		}
		if quote != 0 {
			if c == quote {
				quote = 0
			} else {
				b.WriteRune(c)
			}
			active = true
			continue
		}
		if c == '\'' || c == '"' {
			quote = c
			active = true
			continue
		}
		if unicode.IsSpace(c) {
			if active {
				words = append(words, b.String())
				b.Reset()
				active = false
			}
		} else {
			b.WriteRune(c)
			active = true
		}
	}
	if quote != 0 || escaped {
		return nil
	}
	if active {
		words = append(words, b.String())
	}
	return words
}
func has(words []string, s string) bool {
	for _, w := range words {
		if w == s {
			return true
		}
	}
	return false
}

var scriptName = regexp.MustCompile(`^[\pL\pN_./-]+\.(py|js|mjs|cjs)$`)

func Command(item model.Record) (string, string) {
	cmd := item.Array("command")
	source := ""
	if len(cmd) > 0 {
		source, _ = cmd[len(cmd)-1].(string)
	}
	simple := simpleCommandWords(source)
	if len(simple) == 0 {
		simple = mailboxStdinCommand(source)
	}
	w := simple
	if len(w) == 0 {
		w = shellWords(strings.Split(source, "\n")[0])
	}
	exe := ""
	if len(w) > 0 {
		exe = filepath.Base(w[0])
	}
	reader := "파일 읽기"
	if has([]string{"cat", "sed", "head", "tail", "rg"}, exe) {
		reader = exe
	}
	parsed := item.Array("parsed_cmd")
	for _, v := range parsed {
		p := model.As(v)
		if p.String("type") == "read" && strings.HasSuffix(p.String("path"), "/SKILL.md") {
			return "스킬 읽기 · " + displayName(filepath.Dir(p.String("path"))), reader
		}
	}
	if label := taskLabel(simple); label != "" {
		return label, exe
	}
	if label, module := observationLabel(simple); label != "" {
		return label, module
	}
	if label := readLabel(parsed, len(simple) > 0); label != "" {
		return label, reader
	}
	return executableLabel(w, exe)
}

// readLabel summarizes parsed file reads without exposing paths or arguments.
func readLabel(parsed []any, simple bool) string {
	reads := []model.Record{}
	for _, v := range parsed {
		if p := model.As(v); p.String("type") == "read" {
			reads = append(reads, p)
		}
	}
	if len(reads) > 0 {
		if simple && len(reads) == len(parsed) {
			if label := documentLabel(reads); label != "" {
				return label
			}
		}
		p := reads[0]
		name := p.String("path")
		if name == "" {
			name = p.String("name")
		}
		if name == "" {
			name = "파일"
		}
		suffix := ""
		if len(reads) > 1 {
			suffix = fmt.Sprintf(" 외 %d개", len(reads)-1)
		}
		return "파일 읽기 · " + displayName(name) + suffix
	}
	return ""
}

// executableLabel supplies the fallback after higher-priority semantic labels.
func executableLabel(w []string, exe string) (string, string) {
	if exe == "rg" || exe == "grep" {
		label := "내용 검색"
		if has(w, "--files") {
			label = "파일 목록 검색"
		}
		return label + " · " + exe, exe
	}
	if exe == "ls" || exe == "find" {
		return "파일 목록 · " + exe, exe
	}
	if has([]string{"python", "python3", "node", "uv"}, exe) {
		if has(w, "--version") {
			return "버전 확인 · " + exe, exe
		}
		if len(w) > 1 && scriptName.MatchString(w[1]) {
			return "스크립트 실행 · " + displayName(w[1]), exe
		}
		return "코드 실행 · " + exe, exe
	}
	if has([]string{"git", "npm", "pnpm", "pytest", "cat", "sed", "curl", "mkdir", "wc", "codex", "claude"}, exe) {
		suffix := ""
		if len(w) > 1 && has([]string{"status", "diff", "test", "build", "--version", "--help"}, w[1]) {
			suffix = " " + w[1]
		}
		return "명령 실행 · " + exe + suffix, exe
	}
	return "셸 명령 실행", "shell"
}
