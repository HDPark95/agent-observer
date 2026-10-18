package workers

import (
	"context"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"

	"agent-observer/internal/messaging"
)

func assertBoundedOutput(t *testing.T, output string) {
	t.Helper()
	if !utf8.ValidString(output) {
		t.Fatal("output contains malformed UTF-8")
	}
	if count := utf8.RuneCountInString(output); count > 10000 {
		t.Fatalf("output has %d runes; maximum is 10000", count)
	}
	if len(output) > messaging.MaxBodyBytes {
		t.Fatalf("output has %d bytes; maximum is %d",
			len(output), messaging.MaxBodyBytes)
	}
	for _, r := range output {
		if unicode.IsControl(r) && r != '\n' && r != '\r' && r != '\t' {
			t.Fatalf("output retained control character U+%04X", r)
		}
	}
}

func TestBoundedOutputPreservesUnicodeAndSanitizesInvalidText(t *testing.T) {
	input := "한글🙂e\u0301\x00\x1b\n\r\t" +
		string([]byte{0xff, 0xfe}) + "\u0085끝"
	want := "한글🙂e\u0301\n\r\t\uFFFD\uFFFD끝"

	got, _, _ := boundOutput(input)
	if got != want {
		t.Fatalf("boundedOutput = %q; want %q", got, want)
	}
	assertBoundedOutput(t, got)
	if again, _, _ := boundOutput(got); again != got {
		t.Fatal("sanitization is not idempotent")
	}
}

func TestBoundedOutputHonorsRuneAndByteLimits(t *testing.T) {
	for _, tc := range []struct {
		name string
		unit string
	}{
		{"ascii", "a"},
		{"three-byte-unicode", "한"},
		{"four-byte-unicode", "🙂"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wantCount := 10000
			if byteCount := messaging.MaxBodyBytes / len(tc.unit); byteCount < wantCount {
				wantCount = byteCount
			}

			// Removed controls must consume neither the rune nor byte budget.
			input := strings.Repeat("\x00\x1b"+tc.unit, 10001)
			got, _, _ := boundOutput(input)
			want := strings.Repeat(tc.unit, wantCount)
			if got != want {
				t.Fatalf("got %d runes/%d bytes; want %d runes/%d bytes",
					utf8.RuneCountInString(got), len(got), wantCount, len(want))
			}
			assertBoundedOutput(t, got)
		})
	}
}

// Delegate execution so acceptance callbacks and fixture synchronization remain
// unchanged, then substitute the runtime's completion output.
type outputOverrideRuntime struct {
	Runtime
	output string
}

func (r *outputOverrideRuntime) Run(ctx context.Context, input Input) (Receipt, error) {
	receipt, err := r.Runtime.Run(ctx, input)
	receipt.Output = r.output
	return receipt, err
}

func TestManagerSanitizesOutputAndContinuesQueuedDelivery(t *testing.T) {
	prefix := "한글🙂\n\r\t"
	raw := prefix + "\x00\x1b" + string([]byte{0xff}) +
		strings.Repeat("🙂", 10001)

	f := newManagerFixtureWithProvider(t, nil, func(factory Factory) Provider {
		return testProvider(func(
			ctx context.Context,
			cfg RuntimeConfig,
			send SendFunc,
		) (Runtime, error) {
			runtime, err := factory(ctx, cfg, send)
			if err != nil {
				return nil, err
			}
			return &outputOverrideRuntime{Runtime: runtime, output: raw}, nil
		})
	})
	ux := f.runtimes["managed:ux"]

	sendRequest(t, f, "output-first", "managed:ux", "첫 작업")
	first := awaitRun(t, ux)
	if first.message.ID != "output-first" {
		t.Fatalf("unexpected first input: %q", first.message.ID)
	}

	sendRequest(t, f, "output-second", "managed:ux", "다음 작업")
	awaitDelivery(t, f, "output-second", "queued")
	first.complete <- nil

	completed := awaitDelivery(t, f, "output-first", "completed")
	delivery := completed.Delivery
	if delivery.Provider != "test-runtime" ||
		delivery.ThreadID != ux.conversationID() ||
		delivery.TurnID != "turn-output-first" ||
		delivery.Error != "" {
		t.Fatalf("completion receipt was not preserved: %+v", delivery)
	}
	assertBoundedOutput(t, delivery.Output)
	if !strings.HasPrefix(delivery.Output, prefix+"\uFFFD") {
		t.Fatal("completion lost Unicode, whitespace, or replacement character")
	}
	want, _, _ := boundOutput(raw)
	if delivery.Output == raw || delivery.Output != want {
		t.Fatal("persisted completion output was not sanitized and bounded")
	}

	second := awaitRun(t, ux)
	if second.message.ID != "output-second" || second.message.Body != "다음 작업" {
		t.Fatalf("unexpected queued input: %+v", second.message)
	}
	second.complete <- nil
	next := awaitDelivery(t, f, "output-second", "completed")
	if next.Delivery.ThreadID != ux.conversationID() ||
		next.Delivery.TurnID != "turn-output-second" ||
		next.Delivery.Error != "" {
		t.Fatalf("next completion receipt was not preserved: %+v", next.Delivery)
	}
	assertBoundedOutput(t, next.Delivery.Output)

	if len(f.created) != 1 || ux.starts.Load() != 2 {
		t.Fatalf("created %d runtimes and ran %d turns; want one runtime/two turns",
			len(f.created), ux.starts.Load())
	}
}

func TestOutputCompletenessIsExplicit(t *testing.T) {
	for _, tc := range []struct {
		name, input, text string
		state             messaging.OutputState
		reason            string
	}{
		{"empty-final", "", "", messaging.OutputComplete, ""},
		{"exact", strings.Repeat("한", 10000), strings.Repeat("한", 10000), messaging.OutputComplete, ""},
		{"over-48KiB", strings.Repeat("한", 20000), strings.Repeat("한", 10000), messaging.OutputTruncated, "display-limit"},
		{"controls", "a\x00b", "ab", messaging.OutputTruncated, "sanitized"},
		{"only-controls", "\x00", "", messaging.OutputOmitted, "sanitized"},
		{"invalid-utf8", string([]byte{0xff}), "\uFFFD", messaging.OutputTruncated, "sanitized"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := boundedReceipt(Receipt{ConversationID: "s", ExecutionID: "t", Output: tc.input, OutputState: messaging.OutputComplete})
			if got.Output != tc.text || got.OutputState != tc.state || got.OutputReason != tc.reason {
				t.Fatalf("output state=%s reason=%s size=%d", got.OutputState, got.OutputReason, len(got.Output))
			}
			if got.ConversationID != "s" || got.ExecutionID != "t" {
				t.Fatal("output policy changed acceptance")
			}
			if again := boundedReceipt(got); again != got {
				t.Fatal("second boundary upgraded incomplete evidence")
			}
		})
	}
	for _, state := range []messaging.OutputState{"", messaging.OutputTruncated, messaging.OutputOmitted} {
		got := boundedReceipt(Receipt{OutputState: state, OutputReason: "no-final-text"})
		if got.OutputState != state {
			t.Fatal("unknown/incomplete output upgraded")
		}
	}
}

func TestExplicitOmissionNeverRetainsOutput(t *testing.T) {
	for _, text := range []string{"raw text", "\x00secret", string([]byte{0xff}), strings.Repeat("🙂", 20000)} {
		got := boundedReceipt(Receipt{Output: text, OutputState: messaging.OutputOmitted, OutputReason: "no-final-text"})
		if got.Output != "" || got.OutputState != messaging.OutputOmitted || got.OutputReason != "no-final-text" {
			t.Fatal("explicit omission converted into retained output")
		}
	}
}
