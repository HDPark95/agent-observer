# Getting started

## Clone and build

The primary installation path is **Git clone → local Go build → local viewer**. This guide is validated on macOS Apple Silicon with Go 1.26.4. Other OS/architecture combinations have not completed the same first-use checks. See the [Korean macOS guide](macos-install-ko.md) for a step-by-step walkthrough.

Install Git and Go first. On macOS, `git --version` may offer to install Apple's Command Line Tools; follow the [Git installation guide](https://git-scm.com/install/mac). Install Go using its [official guide](https://go.dev/doc/install), open a new terminal, and run:

```sh
git --version
go version
```

Use Go 1.26.4 for the tested path. Go 1.22 in `go.mod` is the language baseline, not evidence that the minimum toolchain works on every current Mac. The first build downloads modules listed in `go.mod`/`go.sum`, so internet access is required. Building and viewing do not require Python, Node.js, Docker, API keys, or external orchestration settings.

Choose a parent folder where you want the project, then run one command at a time, stopping on errors:

```sh
git clone https://github.com/kochungcheon/agent-observer.git
cd agent-observer
go build -trimpath -o bin/agent-observer ./cmd/agent-observer
./bin/agent-observer version
```

A successful build is normally silent and creates `bin/agent-observer`, including the UI. `make build` is an optional equivalent if Make is installed. `dev` is expected for a local source build; version output also identifies the Go version and target. Stop if execution fails. A hang or `Killed` message alone does not establish its cause; do not disable OS protections to continue. The downloaded-prebuilt-binary Gatekeeper experiment is a separate distribution path described in [release preparation](releases.md#optional-prebuilt-binaries).

## Try the sample

From the cloned repository folder:

```sh
./bin/agent-observer demo
```

Open the URL printed by `demo`, normally `http://127.0.0.1:8767/?demo=1`. The **합성 샘플** banner identifies synthetic data. The message arrows are selectable, and agent groups expand/collapse. **모든 세션 → 내 세션 연결하기** lets you connect your chosen session on the same running viewer. Alternatively, stop the demo with **Ctrl-C in its Terminal window**, run `./bin/agent-observer` from the repository folder, and open its printed URL. Ordinary startup has no selected transcript and no automatic discovery. The terminal stays occupied while serving; this is expected. Use another terminal for diagnostic commands. If port 8767 is occupied, use `./bin/agent-observer demo --port 18767` (or `./bin/agent-observer --port 18767` for ordinary startup).

No `init`, hook, provider login, or saved settings are needed for basic viewing. Go is needed to build; the resulting executable runs without a Go installation or the source checkout. No background service is installed. The examples below run from the cloned repository and use `./bin/agent-observer`.

## Find and connect a session

Codex normally keeps local transcript files under `$CODEX_HOME/sessions`, or `~/.codex/sessions` when that setting is absent. Locations and event formats depend on the Codex version.

1. In Finder press **Shift-Command-G**, enter `~/.codex/sessions` (or your configured Codex home followed by `/sessions`), and open the conversation's date folder.
2. Select the `.jsonl` file for the main conversation you want to inspect. Do not choose a directory. A rollout filename contains its session ID; the main conversation and its subagents can have separate files. If the intended conversation is unclear, use the opt-in metadata catalog below to select a title instead of guessing or opening every transcript.
3. Press **Option-Command-C** to copy the selected file's pathname. In Observer choose **내 세션 연결하기**, paste the path, optionally enter a name, and click **연결**. Spaces inside the pathname are supported; no shell escaping is needed in this form.

To start with one selected transcript from Terminal instead, quote paths containing spaces:

```sh
./bin/agent-observer --source "/absolute/path/to/rollout.jsonl"
```

The source file is read, never modified. A file path is not uploaded to a hosted service. Sessions registered through the screen are held in memory; after a server restart, reconnect or enable the catalog below. Each browser tab selects its own session. If no readable event has been written yet, an empty graph is expected.

## Optional saved settings

These commands create a new configuration and never replace an existing file:

```sh
./bin/agent-observer init --port 8767
./bin/agent-observer
./bin/agent-observer doctor --connect
```

Run `doctor --connect` in a second terminal from the repository folder while the server is running. `init` prints where the configuration was created. If port 8767 is occupied, initialize with another port (for example `init --port 18767`) before starting, or use `--port 18767` for one server run. A one-run `--port` override does not change the saved port that `doctor` checks.

Configuration lives in `os.UserConfigDir()/agent-observer/config.json` (on macOS: `~/Library/Application Support/agent-observer/config.json`). It contains only `version`, `port`, and optional `catalog_root`. Use `--user-config /absolute/private/config.json` consistently for an alternative location. The directory must be private; the file is mode 0600. CLI flags override saved settings, including an explicitly supplied `--port 8767`.

To create the first configuration with metadata discovery enabled:

```sh
./bin/agent-observer init --catalog-root /absolute/codex/sessions --allow-catalog
```

Initialization records this choice without enumerating the directory. At runtime the catalog reads recent parent-session headers, timestamps, and the sibling `session_index.jsonl` title metadata. A clicked session is then opened for graph content. Existing configurations can be edited locally or replaced deliberately by their owner; `init` does not overwrite them. An empty `--catalog-root ''` overrides a saved catalog for one run. `demo` bypasses all user configuration and accepts only `--port`.

## Optional Codex hook

A hook is optional; manual selection works without it. Register an **absolute executable path** to `agent-observer hook` in your Codex hook configuration for `SessionStart`, `UserPromptSubmit`, and, if desired, `PostToolUse` targeting supported `send_message` calls. Follow the current [Codex hook documentation](https://learn.chatgpt.com/docs/hooks) and review/trust the definition in Codex. Observer does not modify trust settings or install a hook automatically.

The Go hook reads the same saved port as the server. For a custom configuration, include `--user-config /absolute/private/config.json` in both commands, or pass the same `--port`. Its options are `--user-config`, `--port`, `--source-provider`, and `--hook-output` (Claude metadata capture).

Once the executable starts, handled option, configuration, and collection failures return exit code **0** with an advisory `systemMessage`. HTTP collection waits at most 800 ms. It never emits `decision: block` or `continue: false`; a stopped Observer must not prevent a conversation. This does not guarantee recovery from a missing executable, OS termination, or a host-level hook timeout. Keep the executable at the registered path and remove the optional hook when uninstalling.

**Upgrading from Python:** remove every old Observer hook definition, including commands using `session_hook.py`, `message_hook.py`, or `python -m agent_observer...`, and replace it with the Go command. Check both user and project hook configurations: matching definitions run together, so adding a new one does not replace a stale one. A missing Python script exits **2**, which Codex interprets as a prompt rejection for `UserPromptSubmit`. Review and trust changed definitions in `/hooks`; Observer never changes trust settings. Check a fresh session after migrating. The Codex plugin itself installs no hooks.

Automatic registration offers session metadata in the list; opening that session selects its transcript for the graph. Hook completion is not evidence that a native message contained plaintext. Native encrypted bodies remain unavailable.

## Diagnose and stop

- `./bin/agent-observer version` prints the executable version and target.
- `./bin/agent-observer doctor` reads local settings without scanning transcripts or contacting providers.
- `./bin/agent-observer doctor --connect` additionally identifies the local `/healthz` endpoint. It does not test provider login, managed execution, exporter receipt, or hook trust.
- “cannot bind loopback port”: another process already occupies the port. Stop the server you own or choose another port; Observer never kills it or silently switches ports.
- Empty catalog: automatic discovery is disabled by default. Connect one file or explicitly enable the catalog.
- Cannot connect a file: use a readable `.jsonl` file, not a directory or relative filename. Copy its Finder pathname instead of a `file://` URL or a shell-escaped string. Source contents and error details are not uploaded to an external service.
- After restarting, an old `?session=…` browser URL can refer to an unregistered session. Click **모든 세션** and reconnect its file, or start with `--source` / an explicitly enabled catalog.
- “본문 미제공”: the source did not provide a body. Use the managed execution path for new team messages; past encrypted text cannot be recovered.
- “파일 알림 복구 중”: filesystem notifications are unavailable; bounded recovery checks remain. Check source access and whether the directory still exists.

Stop with Ctrl-C in the server terminal. There is no installed background daemon. To update, inspect `git status` for your changes, run `git pull --ff-only`, and repeat the Go build command before restarting. Resolve any Git warning without discarding your changes. To uninstall, preserve any work you need and remove the cloned folder. Separately remove any hook, user configuration, or explicitly configured message store you created. Transcript source files belong to Codex and are not part of Observer uninstall.
