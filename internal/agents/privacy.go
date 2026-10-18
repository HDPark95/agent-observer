package agents

import (
	"regexp"
	"strings"
)

var encrypted = regexp.MustCompile(`^gAAAAA[A-Za-z0-9_=-]{80,}$`)
var assignments = regexp.MustCompile(`(?i)\b(authorization\s*[:=]\s*bearer\s+|(?:api[_-]?key|token|password|secret)\s*[:=]\s*)[^\s,;]+`)
var credentials = regexp.MustCompile(`\b(?:sk-[A-Za-z0-9_-]{16,}|gh[pousr]_[A-Za-z0-9_]{16,}|xox[baprs]-[A-Za-z0-9-]+)\b`)

func Encrypted(v string) bool { return encrypted.MatchString(v) }
func Body(value any) any {
	s, ok := value.(string)
	if !ok || Encrypted(s) {
		return nil
	}
	s = assignments.ReplaceAllString(s, "${1}[가림]")
	s = credentials.ReplaceAllString(s, "[가림]")
	r := []rune(s)
	if len(r) > 12000 {
		s = string(r[:12000]) + "\n[긴 메시지 생략]"
	}
	return s
}
func Relative(sender, target string) string {
	if strings.HasPrefix(target, "/") {
		return target
	}
	i := strings.LastIndex(sender, "/")
	if i < 0 {
		return target
	}
	return sender[:i+1] + target
}
