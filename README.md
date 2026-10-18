# Agent Observer

A local viewer for Codex agent workflows. See tasks, tools, skill reads, and agent groups update as recorded events arrive.

## Quick start

Requires Git and Go. Tested on macOS Apple Silicon with Go 1.26.4.

```sh
git clone https://github.com/kochungcheon/agent-observer.git
cd agent-observer
go build -trimpath -o bin/agent-observer ./cmd/agent-observer
./bin/agent-observer
```

Open the printed URL, normally `http://127.0.0.1:8767`. Stop with **Ctrl-C**. If the port is occupied, run `./bin/agent-observer --port 18767`.

## Connect a session

Choose **내 세션 연결하기** and paste the absolute path of your Codex conversation's `.jsonl` file, normally under `~/.codex/sessions`. New recorded events appear without refreshing. Reconnect after restarting the server.

To explore a sample without a conversation file, run `./bin/agent-observer demo` instead.

## Documentation

- [Installation and troubleshooting](docs/public/getting-started.md)
- [Codex plugin](docs/public/codex-plugin.md)
- [한국어 설치 안내](docs/public/macos-install-ko.md)
- [Capabilities, message availability, and privacy](docs/public/privacy.md)
- [Contributing](CONTRIBUTING.md)

[MIT License](LICENSE)
