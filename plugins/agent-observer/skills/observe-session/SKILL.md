---
name: observe-session
description: Observe a selected agent session or run an Observer-managed team whose messages appear in the live graph. Use for Observer viewing, team execution, and conversation visibility.
---

# Observe a session

Use the plugin's `observer` MCP tools. Tool discovery alone does not start a viewer or read session files.

1. Call `observer_start`. It defaults to loopback port 8767 and can reuse an existing Observer. If that port belongs to another service, use `port: 0` for an available port. Do not stop unrelated services. Confirm `running: true`.
2. Connect the session the user selected using `observer_connect_session` with its absolute `.jsonl` path. Pass `session_id` when its exact ID is known so a mismatched file is rejected. `provider` defaults to `codex`.
   - Reuse a path explicitly supplied by the user or by trusted host context for the selected session.
   - If only an exact Codex session ID is known, resolve only a filename ending in that ID within the configured Codex sessions directory. Do not read other session bodies, choose the newest file, or enable a whole-home catalog.
   - If the selected path remains unknown, ask for that path. Never ask the user to paste transcript contents.
3. Call `observer_open_view` with the returned `sessionId`. Open its returned loopback URL using the host's browser-opening capability (in Codex desktop, `open_in_codex` with a browser target). If opening is unavailable, provide the clickable URL and say it has not been opened.
4. When browser inspection is available, confirm that the selected session is shown. Report connection and visible rendering separately. `browserOpened: false` is intentional: the MCP tool prepares the URL; the host opens it.

`observer_status` checks health without discovering sessions. MCP results contain connection metadata, not transcript or message bodies. A connected transcript can contain private data; keep the viewer local and avoid copying that data into reports.

## Run a team with visible conversations

When the user asks for an Observer team or visible inter-agent conversations, complete steps 1–2 above, then use the managed team tools:

1. Call `observer_team_create` with the connected `session_id` and the absolute workspace selected for this task. Omit `members` for UX and DX, or supply 1–8 members with `managed:` IDs, names, tasks and role instructions. Creation binds the observed owner and turn; retain its `teamId`. It opens a dedicated team viewer so an existing standalone server does not need tokens or configuration changes. Use the returned **team URL** for this workflow.
2. Call `observer_team_send` with `team_id`, a session-unique `message_id`, `target` (for example `managed:ux`) and the task `body`. Ask UX to send its findings to DX and DX to reply to UX through their `observer_send_message` tool. Specify a finite exchange and a stopping condition. Do not add a separate body-forwarding call or mailbox polling step.
3. Use `observer_team_status` to distinguish body persistence, provider acceptance and completion. Retain the exact team, ID, recipient and body for an uncertain retry. A changed task needs a new ID. Inspect the team URL and click the UX↔DX connection to verify actual bodies. A status receipt alone does not prove browser rendering.
4. Keep the team's MCP connection active while the user is using its viewer. `observer_team_stop` stops only that team's workers and viewer, preserving stored messages. Use it when the team is no longer needed. Closing the MCP connection also stops owned teams; interrupted executions remain uncertain and are not automatically repeated.

Team creation uses the installed, signed-in Codex CLI and can incur the user's normal model usage. It defaults to `codex` on PATH. If `runtime-upgrade-required` is returned, report the retained request; update the CLI or select a compatible installed binary from trusted host context using the optional absolute `codex_executable` argument. Do not change the model or replay the request automatically. Workers are read-only and receive only their assigned messages and workspace. Their Codex runtime disables plugins and native subagent delegation, leaving the identity-bound message tool as their team communication path. Never substitute `collaboration.spawn_agent` or native messaging for an Observer team; if managed startup fails, show the failure and retained delivery state.

Teams are fixed to the selected session and observed turn. Create a new team for a new turn; use an existing `teamId` for follow-ups to that team's original scope. Recreating the same session/turn/workspace/runtime/members reconnects its private store; completed or uncertain deliveries are not replayed. Up to four teams may be open per MCP connection. Local team bodies persist across plugin updates; no exporter receives them.

The plugin has no hooks, auto-catalog or exporters. Native Codex message bodies may be unavailable; managed teams do not recover past native messages. A viewer started by the plugin lasts for its MCP connection. A reused standalone Observer keeps running. Reconnect the selected session explicitly if the MCP connection restarts.
