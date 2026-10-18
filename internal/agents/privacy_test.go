package agents

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestMessageBodyPrivacyAndUnicodeLimit(t *testing.T) {
	for _, value := range []any{nil, 42, map[string]string{"message": "invalid"}, "gAAAAA" + strings.Repeat("a", 120)} {
		if Body(value) != nil {
			t.Fatalf("non-plaintext body accepted: %T", value)
		}
	}
	plain := "첫 줄\n둘째 줄 <script>"
	if Body(plain) != plain {
		t.Fatal("body text changed before presentation escaping")
	}
	if got := Body("token=synthetic-value").(string); strings.Contains(got, "synthetic-value") || !strings.Contains(got, "[가림]") {
		t.Fatalf("credential assignment was not redacted: %q", got)
	}
	long := Body(strings.Repeat("한", 13000)).(string)
	want := strings.Repeat("한", 12000) + "\n[긴 메시지 생략]"
	if long != want || !utf8.ValidString(long) {
		t.Fatal("body limit split Unicode text or omitted truncation notice")
	}
}
