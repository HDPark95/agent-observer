# Contributing

Start with a narrow change and an observable acceptance criterion. Keep functions responsible for one coherent operation; split mixed I/O and policy when that makes testing clearer. Do not add a service/repository framework just to imitate another language. Use Go formatting and [Google Go Style](https://google.github.io/styleguide/go/guide); lint is only part of review.

The Go module owns source normalization, sessions, HTTP/SSE, local messaging, and worker/export adapters. `internal/workers` depends on runtime contracts; provider details live under `internal/adapters`. `agent_observer/index.html` is embedded in the binary. Python is a reference/compatibility implementation used by the tests, not the current SSE server.

Run `make check` with Go 1.26.4, Python 3.9+, Node 22 and Make. This installs pinned Staticcheck v0.7.0 if missing. Do not skip hooks or inject dummy service settings into normal verification. Tests use repository-defined synthetic fixtures; never add real transcripts or tokens. UI changes also need a rendered-browser check.

Run `python3 scripts/smoke_release.py ./bin/agent-observer` after `make build`. Packaging is an explicit file allowlist: additions need a reviewed `release/source-files.txt` entry. CI runs `python3 scripts/package_release.py check-tree` to reject tracked files outside that list and missing listed files. Do not stage the entire local workspace or publish its private history. See [release preparation](docs/public/releases.md).
