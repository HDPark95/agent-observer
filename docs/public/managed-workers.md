# Managed workers

Use this optional feature when new team messages must have locally stored bodies and recipients should start automatically. Native Codex subagents are not intercepted. Worker execution requires an installed, authenticated Codex CLI and can consume provider quota. Workers in this release are read-only reviewers; a coordinator applies their proposed changes.

Create a private parent directory (mode 0700). Save a `team.json` there using absolute executable/project paths:

```json
{
  "automatic": true,
  "provider": "codex",
  "owner": "agent",
  "executable": "/absolute/path/to/codex",
  "cwd": "/absolute/selected/project",
  "workers": [
    {"id":"managed:ux","name":"UX","task":"UX review","instructions":"Review the requested interface. Use observer_send_message for necessary team messages. Do not edit files."},
    {"id":"managed:dx","name":"DX","task":"DX review","instructions":"Review the requested developer workflow. Use observer_send_message for necessary team messages. Do not edit files."}
  ]
}
```

Start the server with the selected parent transcript, an unused private mailbox location, and that configuration:

```sh
./bin/agent-observer --source /absolute/selected-rollout.jsonl \
  --mailbox-store /absolute/private/mailbox \
  --workers-config /absolute/private/team.json
```

Then configure the local client once. The mailbox creates its host-owned `admin.token`; never paste the token itself into prompts or settings:

```sh
./bin/agent-observer team configure --admin-file /absolute/private/mailbox/admin.token
./bin/agent-observer team start --session SELECTED_SESSION_ID \
  --id UNIQUE_TASK_ID --to managed:ux --body-stdin < task.txt
```

Inside Codex, `--session` can use the host-provided `CODEX_THREAD_ID` default. Outside it, pass the session you selected, not the latest conversation. The server resolves the observed turn and creates/reuses that scope's team. Use a fresh task ID for new work. If a response is uncertain, retry only the exact same ID, target, and body; do not silently create another task.

`team configure` defaults to the shared user-config port and accepts `--user-config` or an explicit `--endpoint`. Its separate `--config` flag selects the team client file, not the server settings. Once created, the client keeps its endpoint: changing the server port does not silently redirect existing authenticated clients. Configure a new client file for another endpoint and use that same `--config` with `team start`.

Workers receive `observer_send_message`. That one call validates scope, stores the body, and queues the recipient; no separate mailbox polling task is needed. Click a group-to-group connection to inspect the two-way conversation and worker result. Verify `observer-managed` provenance, body presence, provider acceptance, and completion independently. Stored tasks with an uncertain execution state are not automatically replayed on restart.

The current product does not rewrite native agent tools or force every host to use this entry point. Agent instructions must explicitly select `team start` for observable teams. Use actual local delivery evidence, not the existence of these instructions, to decide whether a team used the managed path.
