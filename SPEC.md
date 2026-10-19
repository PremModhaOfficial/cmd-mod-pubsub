# SPEC: cmd-mod-better-sub-agents

Status: draft
Owner: Prem Modha
Date: 2026-10-19
Supersedes: cmd-mod-pubsub 0.1.0

## Document map

- `SPEC.md` is the single decision record for this change.
- `CHECKLIST.md` is the execution plan. Its phases are tickets, not separate specs.
- `GLOSSARY.md` is created only if a new term is settled.
- `docs/adr/NNNN-*.md` is created only when a decision is hard to reverse, surprising, and a real trade-off.

## Problem

Claude Code's async subagents let the main conversation continue while a subagent works in its own context and returns a result later. Command Code exposes an `agent` tool, but the mod-level async surface is fire-and-forget only. It has a global spool, no agent definitions, no background or foreground policy, no resume, no worktree isolation, and no concurrency control.

`cmd-mod-pubsub` proved the transport. A worker writes a signal, a drain delivers it, and the session never blocks. This mod turns that transport into a subagent runtime that follows Claude Code's documented behavior.

## Users

- A developer running long research, audit, or code tasks who wants to keep working.
- A developer who wants named, reusable subagents with their own prompts, tools, models, and permissions.
- A maintainer who needs per-session isolation so one session never receives another session's result.

## Goals

1. Follow Claude Code's documented subagent behavior inside the Command Code mod API.
2. Use one transport, one state machine, and one owner model.
3. Prove per-session owner isolation with a two-session test.
4. Run in the background by default, with an explicit foreground mode when the caller needs the result.
5. Provide spawn, list, output, cancel, and resume.
6. Load agent definitions from `.commandcode/agents/*.md` and `~/.commandcode/agents/*.md`.

## Non-goals

- Native interactive permission dialogs inside the parent session. A detached worker cannot surface one.
- Claude Code agent teams and the `SendMessage` sibling roster.
- Forking the full parent conversation. A worker starts fresh or resumes its own session.
- Frontmatter fields with no Command Code equivalent. Unsupported fields are ignored with a warning.

## Claude Code behavior this mod follows

### Definition

- A subagent has its own context window, system prompt, tool access, model, and permissions.
- The parent sees only the final result.
- The `description` field drives delegation.
- Scopes: project, user, CLI, managed, and plugin. This mod supports project and user.
- Required fields: `name`, `description`.
- Supported frontmatter: `name`, `description`, `tools`, `disallowedTools`, `model`, `permissionMode`, `maxTurns`, `skills`, `isolation`, `background`, `color`, `omitClaudeMd`, `effort`.
- Invalid files are skipped with a warning. Duplicate names resolve by scope, project first.

### Invocation

- Automatic delegation by description.
- Explicit `@mention` or a direct tool call.
- Built-ins: `explore`, `plan`, `general-purpose`, `claude`.
- `explore` and `plan` are read-only and one-shot. They return no resumable session.

### Async behavior

- Foreground blocks the main conversation. Permission prompts pass through immediately.
- Background runs concurrently. The main conversation continues.
- Background by default.
- A definition with `background: true` stays in the background even when the caller asks for the foreground.
- `BETTER_SUBAGENTS_DISABLE_BACKGROUND=1` forces every run into the foreground.
- The parent can list, inspect, cancel, and resume a background job.
- A worker that needs approval enters `waiting-input` and signals the parent. It does not hang.

### Lifecycle

`queued -> running -> waiting-input | done | failed | canceled`

- `queued`: the job record exists and the detached worker is being spawned.
- `running`: the worker has written the child pid.
- `waiting-input`: the worker ended with a question or a permission request.
- `done` or `failed`: the child exited. The signal carries the exit code, duration, and log tail.
- `canceled`: terminal. Cancellation kills the child and the worker, then emits one signal.
- A completed job keeps its worker session id. Resume uses that id.
- Reaching `maxTurns` marks the result partial and resumable.
- A restart reconciles a dead `queued` or `running` job to `failed` after a 30 second grace.

### Concurrency and nesting

- Default concurrent jobs per session: 20.
- Override: `BETTER_SUBAGENTS_MAX_CONCURRENT`.
- The 21st spawn fails with `Concurrent subagent limit reached`.
- Default nesting depth: 3.
- Override: `BETTER_SUBAGENTS_MAX_DEPTH`.
- A worker at the depth limit cannot spawn another worker.

### Isolation

- A worker starts in the caller's working directory.
- `isolation: worktree` creates a temporary git worktree from the default branch.
- Commands that resolve to the main checkout fail.
- The worktree is removed when the worker made no changes.
- The worktree is kept and reported when the worker made changes.

### Output

- Worker output is scanned before it reaches the parent.
- Text that imitates a system tag gets a backslash inserted.
- Permission-setting mentions get a `[harness: subagent output matched instruction-shaped pattern(s): ...]` marker.
- A failed worker returns its last output. Partial work is not discarded.
- The parent never sees intermediate tool calls.

### Limits

- Transcripts are kept for 30 days by default.
- Override: `BETTER_SUBAGENTS_RETENTION_DAYS`.
- Main conversation compaction does not affect worker transcripts.
- A worker's context window is sized by its own model.

## Design

### Process model

Each job is one detached worker process. The worker launches either `cmd -p <prompt>` for an agent job or `/bin/sh -c <command>` for a shell job. The worker writes one signal envelope and exits. The parent drains the spool and delivers only its own signals.

### Data shapes

```ts
type AgentDefinition = {
  name: string;
  description: string;
  prompt: string;
  tools?: string[];
  disallowedTools?: string[];
  model?: string;
  permissionMode?: 'default' | 'acceptEdits' | 'auto' | 'dontAsk' | 'bypassPermissions' | 'plan' | 'manual';
  maxTurns?: number;
  skills?: string[];
  isolation?: 'worktree';
  background?: boolean;
  color?: string;
  omitClaudeMd?: boolean;
  effort?: 'low' | 'medium' | 'high' | 'xhigh' | 'max';
};

type Job = {
  id: string;
  owner: string;
  sessionId: string;
  agentType: string;
  description: string;
  prompt?: string;
  command?: string;
  state: 'queued' | 'running' | 'waiting-input' | 'done' | 'failed' | 'canceled';
  pid?: number;
  workerPid?: number;
  workerSessionId?: string;
  cwd: string;
  worktree?: string;
  startedAt: number;
  endedAt?: number;
  exitCode?: number;
  logPath: string;
  signalPath?: string;
};

type Signal = {
  kind: 'subagent';
  jobId: string;
  owner: string;
  state: Job['state'];
  exitCode?: number;
  durationMs?: number;
  logTail?: string;
  error?: string;
  scanned?: boolean;
};
```

### Ownership

The spool is shared by every session on the machine.

- The owner token is minted once per session and persisted with `cmd.session.appendCustomEntry({customType: 'subagent/owner', data: {token}})`.
- The token is read with `cmd.session.getCustomEntries({customType: 'subagent/owner'})`, so it survives resume and hot reload.
- Every job and every signal carries the owner token.
- A drain delivers a signal only when its owner matches.
- A foreign signal is left in `inbox/` for its owner.
- An untagged legacy signal is moved to `orphaned/`.
- A worker runs with `BETTER_SUBAGENTS_WORKER=1` so its own mod instance never claims an unaddressed signal.
- Delivery is exactly once per job id. Claim by atomic rename into `claimed/`. Seed the in-memory set from a durable `delivered.jsonl` ledger.
- `onStop` drains the race window.

### Storage

Under `subagent_dir`, default `~/.commandcode/subagents`:

```
inbox/          workers drop <timestamp>-<id>.json
claimed/        atomic rename target during delivery
orphaned/       untagged legacy signals
logs/           one log per job
jobs/           one JSON record per job
delivered.jsonl durable delivery ledger
worktrees/      isolated checkouts
```

### Surfaces

Tools:

| Tool | Arguments | Behavior |
| --- | --- | --- |
| `subagent_spawn` | `agent_type`, `description`, `prompt`, `model`, `run_in_background`, `isolation`, `max_turns`, `tools`, `permission_mode` | Starts a job. Returns the job id and pid. |
| `subagent_list` | `state?`, `limit?` | Lists jobs with state, model, elapsed time, and owner. |
| `subagent_output` | `id`, `wait?`, `offset?`, `max_chars?` | Returns the log tail or the final signal. |
| `subagent_cancel` | `id` | Sends SIGTERM, then SIGKILL after 1 second. Emits one canceled signal. |
| `subagent_resume` | `id`, `prompt` | Resumes the worker session with full context. |

Command: `/subagents [list|jobs|cancel <id>|open <id>]`.

UI: jobs pane, footer count of running jobs.

Compatibility: `pubsub_*` tools remain as deprecated aliases for one release. They delegate to the `subagent_*` implementations.

## Seams and acceptance tests

- Two sessions get different owner tokens. A signal from session A is never delivered in session B.
- Resume and hot reload restore the same owner token.
- A foreign signal stays in `inbox/` and is delivered when its owner drains.
- An untagged signal moves to `orphaned/`.
- A background spawn returns immediately.
- A foreground spawn waits and returns the result.
- The 21st concurrent spawn fails with `Concurrent subagent limit reached`.
- Nesting stops at the configured depth.
- Cancel produces one canceled signal and no later result.
- Restart reconciliation marks a dead job failed.
- A worktree with no changes is removed. A worktree with changes is kept and reported.
- Output scanning inserts the backslash and the marker.
- A real `cmd -p` worker returns a result.
- The delivered ledger prevents a replay after restart.

## Open decisions

- Repository rename from `cmd-mod-pubsub` to `cmd-mod-better-sub-agents`. Recommended yes. GitHub keeps a redirect.
- Package version 2.0.0 because the tool namespace changes.

## References

- https://code.claude.com/docs/en/sub-agents
- https://code.claude.com/docs/en/tools-reference
