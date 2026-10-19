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

- `cacheTtl` and `autoCompactWindow` are not ACP config options. Use host defaults. Record the lost per-subagent control in the parity ledger. The mod still resolves both and logs them on the job; a resume carries the recorded values forward.
- ACP exposes no prompt-cache split. The mod records `promptCache: shared` for a fork (the host fork copies the main cache) and `own` for every other agent, carries it on the spec, and logs it. The actual cache is the host default; the deviation is visible in the log and the record.
- ACP exposes no `maxTurns` option. The ACP argv carries `--max-turns <n>` (the host accepts the flag; whether it applies it to ACP is a host detail), and the lifecycle reads the `stopReason`: `max_turn_requests` or `max_tokens` marks the result `partial` and keeps the job resumable. The cap is never inferred from tool-call counts.
- ACP has no compaction event. The adapter treats the host's `usage_update` crossing the resolved `autoCompactWindow` as the boundary: it records `compactBoundary {trigger: 'auto', preTokens}`, appends it to the transcript, and carries it on the signal. No synthetic compaction is performed; the host still owns the summarization.
- ACP provides no transcript file. The worker appends the `session/update` stream (plus the result and any compact boundary) to `transcripts/<id>.jsonl` in the spool, so the transcript survives a restart and is independent of the main session's transcript. Retention sweeps it with the record and log.
- `permissionMode: auto` has no ACP mode. The worker sets the prompting mode (`default`), so nothing is auto-allowed, and logs the mapping once on the job. The host classifier and its final-report review are not reproduced. Evidence: smoke 102.
- ACP has no per-session tool filter. The worker enforces the resolved `tools` set in its `beforeToolCall`, blocking and naming an unlisted call; the mod's own names match their specifiers (`subagent_spawn` is `Agent`, `subagent_send` is `SendMessage`). Evidence: smoke 91.
- Definition `hooks` are carried on the worker spec and registered in the ACP child as `beforeToolCall`, `afterToolCall`, and `onStop`; the parent never inherits them. Evidence: smoke 91-92.
- Retention keeps a changed worktree and its git metadata. Only a clean worktree is removed, through `git worktree remove`, so the repository's worktree metadata clears with it; a worktree whose git state cannot be read is kept. The foreground path finalizes the same way before returning. Evidence: smoke 96-97.
- `--agents` and `--append-subagent-system-prompt` are not host flags. Use `--mod` and the mod hooks.
  T12 spells them `agents`, `agent`, `append_subagent_system_prompt`, and
  `append_subagent_system_prompt_file`. The environment aliases (`BETTER_SUBAGENTS_AGENT`,
  `BETTER_SUBAGENTS_APPEND_SYSTEM_PROMPT`, and the `CLAUDE_CODE_*` forms) carry the same values into a
  nested worker. `initialPrompt` is submitted through `cmd.prompt.submit`, with `transformInput` as the
  fallback when the host exposes no submit surface.
- Definition `hooks` are host hooks (`beforeToolCall`, `afterToolCall`, `onStop` with `Stop` reported as
  `SubagentStop`). The host has no project hook file, so `SubagentStart`/`SubagentStop` are read from
  `.commandcode/hooks.json` and run by the mod, once per job.
- Host file agents default `tools` to none. The spec inherits. The mod fills the default.
- `cmd.addAgent` skips a duplicate name, so a project definition cannot shadow a user definition. Record the precedence gap.
- ACP has no fork method. The CLI fork plus one-shot `cmd --resume -p` follow-ups is the accepted path. ACP `session/load` is a host risk for prompt completion.
- The first fork turn is not streamed, and fork follow-ups are one-shot. Mid-turn steering is a Phase 15 gap.
- `statusline-setup` and `claude-code-guide` are registry built-ins with restricted tools and models. The host's own copies keep their names; the mod does not shadow them through `addAgent`.
- The 15,000-token description limit warning uses the documented four-characters-per-token estimate because the host API exposes no tokenizer. The warning reports the estimate and the threshold; it does not block a spawn.
- `@mention` is a typeahead list, not a forced spawn. The mod supplies every valid custom, built-in, plugin-scoped, and named agent; prompt submission and the typeahead widget remain host-owned, so a mention reaches the model as a name.
- `/compact`, `/clear`, and `/rewind` are host commands with no interception seam. The mod does not act on a fork from those commands, and `/model` and `/fast` do not mutate it.
- `permissions.deny` for a built-in agent type is host policy. The mod honours `CLAUDE_AGENT_SDK_DISABLE_BUILTIN_AGENTS` and `CLAUDE_CODE_DISABLE_EXPLORE_PLAN_AGENTS`, which cover the same intent.
- Explore thoroughness is carried in the definition body ("Be quick by default; go deeper only when the task needs it"). The host exposes no per-spawn thoroughness option, so there is no separate parameter.

## Verification

- ACP handshake, `session/new`, modes, and config options: verified 2026-10-20.
- Fork creation through the CLI: verified 2026-10-20. ACP `session/load` replay: verified, but a follow-up prompt did not complete in 180 or 300 seconds. `cmd --resume -p` follow-up: verified 2026-10-20. Prompt-cache reuse: T10 measures it.
- Adapter queue: design requirement. ACP rejects a concurrent prompt, verified 2026-10-20. T1 tests the queue.
- `appendSystemPrompt` and `beforeToolCall` inside the ACP server: T1 tests these.
- `session/load` after an adapter restart: T1 tests this.
- `session/resume` for a finished or partial agent job: smoke check 35 and 86. `usage_update` boundaries and transcript persistence: smoke 35 and 85. Retention of all three artifacts: smoke 83.

## Consequences

- T1 can build the definition loader and typed spawn on the adapter spec.
- T3 uses `session/set_config_option` for model and effort.
- T4 uses the prompt result for foreground and the detached adapter for background.
- T5 uses `session/set_mode` and `session/request_permission`.
- T10 implements the CLI fork, the one-shot `cmd --resume -p` follow-up, and the fork task view. ACP `session/load` is a host risk.
- T13 uses `session/resume` for resume. ACP `session/load` remains a host risk, not the resume path.
