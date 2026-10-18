# Codex plugin

The local plugin bundles one skill and a native stdio MCP server. It starts the viewer only when requested and connects only the session you select. No hooks or API keys are required for the Observer tools.

## Build and install

You need Git, Go, Make, Python 3.9+, and a working Codex CLI with plugin support. First check:

```sh
codex --version
codex plugin --help
```

If `codex` reports `env: node: No such file or directory`, its npm launcher needs Node.js on `PATH`. Fix the CLI installation first; Observer's bundled Go executable itself does not need Node.js. These installation commands were verified with Codex CLI 0.142.2 on macOS Apple Silicon.

From the **root of the cloned repository**, build and install for the current machine:

```sh
make plugin
codex plugin marketplace add ./dist/codex-plugin
codex plugin add agent-observer@agent-observer-local
```

The package includes its Go executable; it does not run `go build` when Codex starts. Install on the same OS and CPU architecture as the build machine. macOS Apple Silicon is the currently verified target. This local installation does not publish the plugin to a public directory.

Open a new Codex chat after installation so it loads the plugin. If the current desktop app does not refresh its plugin list, restart the app. In the plugin picker, select **Agent Observer**, then ask it to start Observer and show your selected session. Supply the absolute `.jsonl` path if the host cannot resolve the exact session you chose.

The CLI and desktop app must use the same Codex profile. If you deliberately set `CODEX_HOME`, create that directory first and use the same value for installation and the host that loads the plugin. Installing into a separate test profile does not install it into your usual desktop profile. Confirm `codex plugin list` shows `agent-observer@agent-observer-local` as **installed, enabled**.

For example, ask: “Start Observer on an available port, connect `/absolute/path/to/my-session.jsonl`, and open its viewer.” An available port (`observer_start` with `port: 0`) keeps a separate verification viewer from reusing an already-running Observer on port 8767. No `init`, hooks, Docker, or exporter setup is required. Using Observer MCP tools needs no API key; asking a Codex model to operate them still requires your normal Codex sign-in.

## Workflow and lifetime

- `observer_start`: start on port 8767, reuse an existing Observer, or use `port: 0` for an available port. An unrelated service is never stopped.
- `observer_connect_session`: connect an explicit absolute JSONL path; an optional expected session ID is checked against the file.
- `observer_open_view`: return the selected viewer URL. The skill asks the host to open that URL; a returned URL alone is not evidence that a browser opened.
- `observer_status`: health and connection count only, with no transcript discovery.

The viewer binds to `127.0.0.1`. Plugin-owned viewers stop when their MCP connection closes. Reused standalone viewers remain running. The plugin does not load your Observer export/catalog settings. Native message bodies remain subject to host availability; see [privacy and capabilities](privacy.md).

## Managed teams

Ask the plugin to run a managed UX/DX team in your selected workspace. This requires an installed, signed-in Codex CLI and uses your normal Codex model allowance.

The CLI must support your configured model. By default, teams use `codex` on PATH. If the desktop app works but an older CLI rejects the model, update the CLI or pass an explicitly selected compatible installed binary as `codex_executable` to `observer_team_create`. The plugin does not change your model or silently replay a failed request. Compatibility failures return `runtime-upgrade-required` with recovery guidance.

- `observer_team_create`: bind a team to the connected session and its current observed turn. UX/DX are the defaults; custom teams support 1–8 members. The response includes a team ID and a dedicated viewer URL.
- `observer_team_send`: store one message and queue its recipient automatically. Workers use their bound `observer_send_message` tool to reply to each other; no separate capture or inbox-read call is needed.
- `observer_team_status`: separate stored bodies, accepted execution and completion. Click a message connection in the team viewer to read the conversation.
- `observer_team_stop`: stop the team's workers and viewer while retaining stored messages.

Teams run in read-only workspaces. Workers cannot use plugins or native subagent delegation. A team belongs to one session/turn; a new turn gets a new team. Identical message retries return the existing receipt, while changed content under the same ID is rejected. Up to four teams may be open per MCP connection.

Team bodies are stored privately under your OS user configuration directory in `agent-observer/plugin-teams`, independently of the installed plugin cache. Keep the MCP connection open while using its team viewer. Closing it stops owned workers and viewers; uncertain executions are not automatically repeated. Recreating the same session/turn/workspace/runtime/members reconnects stored records. The existing port 8767 server is left running with its own data and settings; use the returned team URL to see this team's conversations.

## Update or remove

Build updates into a fresh directory and replace this plugin's marketplace registration before reinstalling. Existing output directories are preserved.

```sh
make plugin PLUGIN_OUTPUT=dist/codex-plugin-v2
codex plugin marketplace remove agent-observer-local
codex plugin marketplace add ./dist/codex-plugin-v2
codex plugin add agent-observer@agent-observer-local
```

Reopen the chat after updating; keep the marketplace directory available until you remove the installation. To uninstall:

```sh
codex plugin remove agent-observer@agent-observer-local
```

Official references: [plugin packaging](https://developers.openai.com/plugins/build/plugins), [local MCP plugins](https://developers.openai.com/plugins/deploy/desktop-only). Public directory submission of a local MCP plugin requires separate eligibility/review; local installation is independently supported.
