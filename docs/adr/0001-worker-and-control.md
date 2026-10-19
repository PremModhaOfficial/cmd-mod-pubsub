# ADR 0001: Worker transport and control topology

Status: accepted
Date: 2026-10-20
Owner: Prem Modha
Related: `SPEC.md`, `GLOSSARY.md`, issue #3

## Context

The mod runs async subagents that must survive the parent session, stream progress, accept live messages and permission decisions, and fork the main conversation. The old worker is a one-shot `cmd -p` process with a spool signal.

The 2026-10-20 spike showed that ACP provides the session surface the spec needs. `cmd --mod <file> acp` loads the mod. `session/new` accepts `cwd` and `mcpServers`. The server reports five permission modes, `model` and `effort` config options, streaming updates, permission requests, and `session/load`, `session/resume`, `session/list`, and `session/close`.

ACP has no fork method. This ADR records the transport that does.

## Decision

Use ACP for non-fork subagents. Run one detached adapter per job. The adapter owns one `cmd acp` child, writes the job log, writes one spool signal, and reads a control file for send and cancel.

Use the CLI fork path for forks. Run one `cmd --fork-session --resume <main> -n <name> -p <prompt>` turn to create the fork. Then use `cmd --resume <fork> -p` for each follow-up. Do not use ACP `session/load` for follow-ups. It replays history, but a follow-up prompt did not complete in the probe.

Keep the existing spool as the result channel and the existing owner token as the delivery boundary.

## Why

- Durability. The adapter is detached, so a finished job still signals after the parent exits. A long-lived server owned by the parent would die with it.
- Isolation. One ACP server per job gives one session, one log, and one signal. A malformed job cannot corrupt another.
- Least new state. The spool, owner token, and job record remain. The adapter replaces the `cmd -p` argv builder.
- Native permissions. `session/request_permission` replaces the waiting-input heuristic. The adapter bridges the request to the parent and returns the decision.
- Fork parity. ACP cannot fork, but the CLI fork copies the session, the only path that can share its prompt cache. A one-shot `cmd --resume -p` continues that fork. ACP `session/load` replays history but did not complete a follow-up prompt in the probe.

## Topology

Accept one detached adapter per job.

Reject one long-lived ACP server per parent session. Jobs would die with the parent, which breaks the core promise.

Reject a detached long-lived server with a reconnecting client. It adds supervisor state and a reconnect protocol for behavior the spec does not require.

## Adapter lifecycle

1. The parent writes the job spec and spawns the adapter detached.
2. The adapter starts `cmd acp` in the job cwd.
3. The adapter sends `initialize`, then `session/new` with `cwd` and `mcpServers`.
4. The adapter sends `session/set_mode` for the permission mode and `session/set_config_option` for model and effort.
5. The adapter sends `session/prompt` with the task.
6. The adapter appends `session/update` events to `logs/<id>.log`.
7. On `session/request_permission`, the adapter writes a request to the control directory and waits for a decision. It responds with the selected option.
8. On the prompt result, the adapter writes the signal to the spool and exits.

A control file carries `send` and `cancel`. A `send` that arrives while a prompt runs waits for the prompt result, because ACP rejects a second concurrent prompt. A `cancel` sends `session/cancel`.

## Fork path

ACP alone cannot fork. `cmd --fork-session --resume <main> acp` starts, but `session/new` creates a fresh session with no history. Verified on 2026-10-20: the fresh session answered "I don't have one" to a question about a secret from the main session.

The CLI fork works. `cmd --fork-session --resume <main> -n <name> -p "<prompt>"` answered `ORANGE` from the main session. Verified on 2026-10-20.

The fork session is found with `session/list` by the `-n` name.

`session/load` restores its history. A `session/prompt` on the loaded session streamed `ORANGE`, but no JSON-RPC completion arrived in a 180-second probe or a 300-second probe. Do not use ACP `session/load` for live fork follow-ups.

Use `cmd --resume <fork> -p "<message>"` for each follow-up. Verified on 2026-10-20: a resume of the source session answered `ORANGE`. The fork is a normal session, and T10 tests the fork id.

A follow-up cannot arrive while a `-p` turn runs. Queue it and start the next `-p` turn after the current process exits. That is the next safe point. A running fork shows its log and can be stopped by killing the process. Mid-turn steering is a Phase 15 gap.

The first fork turn runs through `cmd -p`. It uses the main session's system prompt, tools, model, and permissions. The host fork path is the only one that can share the main session's prompt cache. T10 measures the cache reuse. The mod must set `BETTER_SUBAGENTS_FORK=1` and skip the definition's system prompt and tool filter for that turn.

The first fork turn is not streamed. The task view sees the result at the end. Follow-ups are also one-shot, so the task view sees each result at the end. Record this as a Phase 15 gap.

## ACP method map

| Need | Method |
| --- | --- |
| Start the client | `initialize` |
| Create a session | `session/new` with `cwd`, `mcpServers` |
| Set the permission mode | `session/set_mode` |
| Set model or effort | `session/set_config_option` |
| Run a turn | `session/prompt` |
| Stream output | `session/update` notifications |
| Ask for permission | `session/request_permission` |
| Stop a turn | `session/cancel` |
| Load a fork or resume | `session/load`, `session/resume` |
| Find a fork | `session/list` |
| End a session | `session/close` |

## Control and delivery

The spool signal remains the only result channel. The owner token remains the only delivery boundary.

The control directory holds three files per job: `send.jsonl`, `cancel`, and `permission.json`. The adapter reads them. The parent writes them.

The adapter writes `permission.json` with the request id, the tool call, and the options. The parent writes `permission-response.json` with the selected option. The adapter maps the selection to the ACP response.

## Deviations

- `cacheTtl` and `autoCompactWindow` are not ACP config options. Use host defaults. Record the lost per-subagent control in the parity ledger.
- `--agents` and `--append-subagent-system-prompt` are not host flags. Use `--mod` and the mod hooks.
- Host file agents default `tools` to none. The spec inherits. The mod fills the default.
- `cmd.addAgent` skips a duplicate name, so a project definition cannot shadow a user definition. Record the precedence gap.
- ACP has no fork method. The CLI fork plus one-shot `cmd --resume -p` follow-ups is the accepted path. ACP `session/load` is a host risk for prompt completion.
- The first fork turn is not streamed, and fork follow-ups are one-shot. Mid-turn steering is a Phase 15 gap.

## Verification

- ACP handshake, `session/new`, modes, and config options: verified 2026-10-20.
- Fork creation through the CLI: verified 2026-10-20. ACP `session/load` replay: verified, but a follow-up prompt did not complete in 180 or 300 seconds. `cmd --resume -p` follow-up: verified 2026-10-20. Prompt-cache reuse: T10 measures it.
- Adapter queue: design requirement. ACP rejects a concurrent prompt, verified 2026-10-20. T1 tests the queue.
- `appendSystemPrompt` and `beforeToolCall` inside the ACP server: T1 tests these.
- `session/load` after an adapter restart: T1 tests this.

## Consequences

- T1 can build the definition loader and typed spawn on the adapter spec.
- T3 uses `session/set_config_option` for model and effort.
- T4 uses the prompt result for foreground and the detached adapter for background.
- T5 uses `session/set_mode` and `session/request_permission`.
- T10 implements the CLI fork, the one-shot `cmd --resume -p` follow-up, and the fork task view. ACP `session/load` is a host risk.
- T13 uses `session/load` and `session/resume` for resume.
