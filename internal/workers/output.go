package workers

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"agent-observer/internal/messaging"
)

const maxOutputRunes = 10000

// boundOutput retains a valid prefix. Filtering is reported as loss too; the
// caller must not describe altered provider text as a complete result.
func boundOutput(value string) (string, messaging.OutputState, string) {
	var out strings.Builder
	count, altered := 0, false
	for len(value) > 0 {
		r, size := utf8.DecodeRuneInString(value)
		value = value[size:]
		if r == utf8.RuneError && size == 1 {
			altered = true
		}
		if unicode.IsControl(r) && r != '\n' && r != '\r' && r != '\t' {
			altered = true
			continue
		}
		if count == maxOutputRunes || out.Len()+utf8.RuneLen(r) > messaging.MaxBodyBytes {
			return out.String(), messaging.OutputTruncated, "display-limit"
		}
		out.WriteRune(r)
		count++
	}
	if altered {
		if out.Len() == 0 {
			return "", messaging.OutputOmitted, "sanitized"
		}
		return out.String(), messaging.OutputTruncated, "sanitized"
	}
	return out.String(), messaging.OutputComplete, ""
}

// Only a provider's explicit final-result evidence can establish completeness.
// Bounds are applied once, here at the storage boundary, for every Runtime.
func boundedReceipt(r Receipt) Receipt {
	if r.OutputState == messaging.OutputOmitted {
		r.Output = ""
		return r
	}
	text, state, reason := boundOutput(r.Output)
	r.Output = text
	if state != messaging.OutputComplete {
		r.OutputState, r.OutputReason = state, reason
	}
	return r
}
