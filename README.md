# Agent Observer

A local viewer for Codex agent workflows. See tasks, tools, skill reads, and agent groups update as recorded events arrive.

## Codex plugin

Requires Git, Go, Make, Python 3.9+, and a working Codex CLI with plugin support. Install using the same Codex profile as your desktop app.

```sh
git clone https://github.com/kochungcheon/agent-observer.git
cd agent-observer
make plugin
codex plugin marketplace add ./dist/codex-plugin
codex plugin add agent-observer@agent-observer-local
codex plugin list
```

Confirm the plugin is **installed, enabled**, then open a **new Codex chat** and select **Agent Observer**. Restart Codex if it does not appear. Ask:

> Start Observer, connect this session, and open its viewer.

If Codex cannot locate the selected session, provide the absolute path to its `.jsonl` file. No hooks or Docker setup is required. See the [plugin guide](docs/public/codex-plugin.md) for troubleshooting and updates.

## Standalone viewer

Requires Git and Go. Tested on macOS Apple Silicon with Go 1.26.4.

```sh
git clone https://github.com/kochungcheon/agent-observer.git
cd agent-observer
go build -trimpath -o bin/agent-observer ./cmd/agent-observer
./bin/agent-observer
```

Open the printed URL, normally `http://127.0.0.1:8767`. Stop with **Ctrl-C**. If the port is occupied, run `./bin/agent-observer --port 18767`.

### Connect a session

Choose **내 세션 연결하기** and paste the absolute path of your Codex conversation's `.jsonl` file, normally under `~/.codex/sessions`. New recorded events appear without refreshing. Reconnect after restarting the server.

To explore a sample without a conversation file, run `./bin/agent-observer demo` instead.

## Documentation

- [Installation and troubleshooting](docs/public/getting-started.md)
- [Codex plugin](docs/public/codex-plugin.md)
- [한국어 설치 안내](docs/public/macos-install-ko.md)
- [Capabilities, message availability, and privacy](docs/public/privacy.md)
- [Contributing](CONTRIBUTING.md)

[MIT License](LICENSE)
