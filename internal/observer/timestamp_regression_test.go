package observer

import "testing"

func TestWithinTurnUsesInstantsForUIAndHistory(t *testing.T) {
	const start = "2026-10-16T00:00:00Z"
	const end = "2026-10-16T09:00:02+09:00"
	cases := []struct {
		name, stamp, end string
		want             bool
	}{
		{"fraction", "2026-10-16T00:00:00.500Z", "", true},
		{"inclusive-start", start, "", true},
		{"equivalent-start", "2026-10-16T09:00:00+09:00", "", true},
		{"previous-date-later-instant", "2026-10-15T20:00:01-04:00", "", true},
		{"later-text-earlier-instant", "2026-10-16T01:00:00+02:00", "", false},
		{"invalid-stamp", "invalid", "", false},
		{"empty-stamp", "", "", false},
		{"exclusive-end", "2026-10-16T00:00:02Z", end, false},
		{"before-end", "2026-10-16T00:00:01.999999999Z", end, true},
		{"after-end", "2026-10-16T00:00:03Z", end, false},
		{"invalid-end", "2026-10-16T00:00:01Z", "invalid", false},
	}
	for _, history := range []bool{false, true} {
		mode := "ui"
		if history {
			mode = "history"
		}
		t.Run(mode, func(t *testing.T) {
			o := New("", nil)
			o.history = history
			o.state.Started = start
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					if got := o.withinTurn(tc.stamp, tc.end); got != tc.want {
						t.Fatalf("withinTurn(%q, %q)=%v, want=%v",
							tc.stamp, tc.end, got, tc.want)
					}
				})
			}
			for _, invalidStart := range []string{"", "invalid"} {
				o.state.Started = invalidStart
				if o.withinTurn(start, "") {
					t.Fatalf("invalid start %q accepted", invalidStart)
				}
			}
		})
	}
}
