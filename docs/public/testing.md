# Test ownership

`make check` runs Go formatting, vet, Staticcheck, race-enabled Go tests, the release/diagnostic Python tests, and the Node UI contract checks. `make build` embeds `internal/webui/index.html`; the server does not read HTML from a source checkout at runtime.

The old Python server and its runtime tests have been retired. Their behavioral assertions are covered by Go tests:

| Former Python tests | Go coverage |
| --- | --- |
| Event parsing and command labels | `internal/normalize/*_test.go`, `internal/observer/observer_test.go` |
| Browser host/action classification and privacy | `internal/normalize/browser_test.go` |
| Agent lifecycle, skill/tool ownership, message bounds | `internal/observer/observer_test.go`, `internal/agents/privacy_test.go` |
| Child discovery, identity, late arrival and turn reset | `internal/agents/reader_test.go`, `internal/observer/discovery_test.go`, `internal/observer/timestamp_regression_test.go` |
| Session selection, metadata catalog and titles | `internal/sessions/*_test.go`, `internal/httpapi/server_test.go`, `internal/httpapi/stream_test.go` |
| Message binding, redaction, deduplication and hooks | `internal/messages/*_test.go`, `internal/hooks/hooks_test.go` |
| Transcript truncation/replacement and partial records | `internal/transcript/reader_regression_test.go`, `internal/sources/identity_regression_test.go` |

`internal/observer/testdata/event_contract.json` retains 32 synthetic input/expected-output steps from the earlier implementation as a fixed regression baseline. It requires no Python runtime or generator. Review changes to expected outputs independently; generating expectations from the implementation under test would conceal regressions.

Two obsolete Python hook status-file tests were removed with that helper. The Go hook does not write that diagnostic file; current message-store permission and symlink checks remain in Go. A truncated or incomplete source header preserves the selected session's last snapshot until a valid matching header arrives; current Go identity tests enforce this intentional behavior.

Python remains for packaging, plugin assembly, isolated binary smoke checks, and the optional native-capture diagnostic. `tests/test_release_package.py` and `tests/test_native_probe.py` test those tools, not a second server. Browser layout and interaction assertions remain in `checks/initial_render.cjs`, which exercises the shipped JavaScript.

After building, run `python3 scripts/smoke_release.py ./bin/agent-observer` and its `--clean-profile` variant. These start the actual executable with owned synthetic transcripts, verify hook registration and session selection, and stop it. The clean profile is process isolation, not a fresh operating system or a Gatekeeper check. Changes to UI interactions also require a real rendered-browser check; unit and DOM contracts alone do not prove the screen works.
