# cmd-mod-better-sub-agents

A [Command Code](https://github.com/PremModhaOfficial) mod that turns a blocking "fire an agent and wait"
into **fire-and-forget**: you start a worker, your session carries on immediately, and when the worker
finishes its result **lands back in the session that fired it** as a separate message on its own.

Neither side blocks. Nothing polls. The wake-up *is* the message arriving.

```
you ──subagent_spawn──▶ detached worker  ──spool file──▶  drain  ──sendUserMessage──▶ you
       (returns now)     (own process)                    (400ms)  (starts the turn)
```

## Why

`agent`, `task` and friends are fire-and-*wait*: the parent has to hold a turn open, poll, or ask for the
result later. Every one of those costs a turn while nothing is happening. This mod uses
`cmd.sendUserMessage(...)` as the seam instead — it starts a turn when the session is idle and queues
behind the active turn otherwise, so a result can arrive at any time without the model waiting on it.

Transport is a **spool directory**, not a socket: no ports, no auth, no reconnect logic. A worker writes
one JSON file and exits. If a signal lands while no session is running, it is still there next time.

## Requirements

- **Command Code 1.84+** (for `sendUserMessage({deliverAs})`, `session.appendCustomEntry` /
  `getCustomEntries`, and the `ui.pane` host surface)
- A signed-in `cmd` for agent jobs, and `-r/--resume` support for `subagent_resume`
- Node 22.18+ if you want to run the smoke test with native TypeScript type stripping (no build step)

## Install

```bash
# as a global mod
cp better-sub-agents.ts ~/.commandcode/mods/

# or load it for one run, without installing
cmd --mod ./better-sub-agents.ts
```

No dependencies, no build step, no `npm install`. The mod is a single file: the detached worker's source
is inlined into it, so `better-sub-agents.ts` is the whole mod.

## Tools

| Tool | Arguments | What it does |
| --- | --- | --- |
| `subagent_spawn` | `description`, plus **either** `prompt` **or** `command`; optional `name`, `subagent_type` (or the deprecated `agent_type`), `model`, `effort`, `tools`, `disallowed_tools`, `max_turns`, `timeout_ms`, `cwd` | Starts a detached worker and returns immediately. `command` runs under `/bin/sh -c`; `prompt` runs a subagent definition over an ACP session. `subagent_type` defaults to `general-purpose`; `cwd` defaults to the mod workspace root. A `name` makes the agent addressable with `subagent_send`. |
| `subagent_list` | `state?`, `limit?` | Spawned jobs with state, model, effort, color, elapsed time and owner, plus how many signals are still waiting for this session. |
| `subagent_output` | `id`, `wait?`, `wait_ms?`, `offset?`, `max_chars?` | One job's state, exit code, duration, owner and agent id, plus a bounded slice of its log (or its transcript when the log is gone) and the transcript path. `wait` blocks briefly until the job finishes — opt in only when you actually need it. |
| `subagent_cancel` | `id` | Cancels a `queued` or `running` job (`SIGTERM`, then `SIGKILL` after 1s). The firing session receives one `canceled` signal. |
| `subagent_resume` | `id`, `prompt` | Continues a finished **agent** job in its ACP session with a new instruction. `id` may be the agent id or the job id. The resumed run is a new job with its own signal; model, effort, cache TTL and the compact window carry over. |
| `subagent_send` | `to`, `message` | Live message to a **named** agent or `main`. A running target queues the message and answers at its next safe point; an idle target starts a turn; a finished target resumes its ACP session with full context. The reply arrives as its own message signal. |
| `subagent_agents` | `scope?` | Agent definitions with scope, model, tools, validity, and source file. |
| `ListAgents` | none | Where cross-session messaging is on: `main` plus every named agent this conversation can message with `subagent_send`. Foreground agents only; a background agent does not keep it. |
| `SubagentHandback` | `report` | Inside an agent: hand the final report to the conversation that receives the result. The wrapper then suppresses its duplicate end-of-turn signal. |

`subagent_spawn` is the whole point: it returns in milliseconds with the job id and pid, and the result
arrives later **as a signal, never as a tool result** — so there is nothing to wait for.

```jsonc
// fire a shell command
{"description": "typecheck", "command": "tsc --noEmit", "cwd": "/repo"}

// fire a headless agent, with a hard timeout
{"description": "audit-deps", "prompt": "Audit dependencies for unused packages and report.", "timeout_ms": 600000}

// continue it afterwards
{"id": "m3k2-9f1a2b", "prompt": "Now open issues for the three worst offenders."}
```


### Agent definitions

A definition is a Markdown file with YAML frontmatter; the body is the worker's system prompt. Only
`name` and `description` are required.

```markdown
---
name: code-reviewer
description: Reviews code for quality, security, and maintainability
tools: Read, Grep, Glob, Bash
model: sonnet
permissionMode: default
---

You are a senior code reviewer. Report findings, not edits.
```

Discovery walks up from the working directory to the repository root (the closest
`.commandcode/agents/` wins), then scans `~/.commandcode/agents/`, then adds the built-ins. Directories
are scanned recursively, and a project definition overrides a user or built-in definition by name.

Every Claude Code scope is loaded, highest priority first:

| Priority | Scope | Location |
| --- | --- | --- |
| 1 | `managed` | `<managed_dir>/agents/**/*.md` (organization-wide) |
| 2 | `cli` | `--mod-option agents=<inline JSON \| JSON file>` (this session only) |
| 3 | `project` | `.commandcode/agents/**/*.md` from the cwd up to the repository root, plus every `--add-dir` directory |
| 4 | `user` | `~/.commandcode/agents/**/*.md` |
| 5 | `plugin` | `<plugin root>/<plugin>/agents/**/*.md`; a subfolder becomes `plugin:review:security` |
| 6 | `builtin` | the four built-ins below, overridable by any definition above |

A same-directory duplicate loads the first file in read order; the rest are reported by `/doctor`.
A CLI definition is session-only and accepts the same fields as frontmatter except `color` and
`experimental`; an interactive session refuses a file path and requires inline JSON. A plugin definition
ignores `hooks`, `mcpServers`, and `permissionMode`, and a plugin file with no name (or unparseable
frontmatter) still loads under its filename.

Built-ins: `explore` and `plan` (read-only, one-shot), `general-purpose` (the fallback) and `claude`.
`general` is accepted as the host-reserved alias for `general-purpose`.

Parsed fields: `name`, `description`, `tools`, `disallowedTools`, `model`, `permissionMode`, `maxTurns`,
`skills`, `memory`, `isolation`, `background`, `color`, `omitClaudeMd`, `effort`, `mcpServers`, `hooks`,
`initialPrompt`, `autoCompactWindow`, and `experimental.cacheTtl`. Unknown fields are ignored, including
unknown nested keys. A file with a missing name or description, a bad first line, a colon or
over-256-character name, or unparseable YAML is skipped and written to `<subagent_dir>/agents.log`;
`/agents` lists every definition and every skip with its reason, and `/doctor` reports each duplicate and
invalid file with the fix.

`mcpServers` is either a map of names to inline definitions (`command`/`args`/`env` or `url`) or a list of
names and inline servers. A name resolves from `.commandcode/mcp.json`; the resolved set is passed to
`session/new`, so it is scoped to that agent. An inline server needs a trusted folder (the workspace,
home, managed directory, or an `--add-dir`); `CLAUDE_CODE_STRICT_MCP_CONFIG=1` keeps only configured
references, `CLAUDE_CODE_BARE=1` loads none, and `BETTER_SUBAGENTS_MCP_ALLOW`/`..._DENY` (comma lists,
`*`) narrow the result. A plugin ignores the field.

`hooks` holds `PreToolUse`, `PostToolUse`, and `Stop` rules (`matcher` plus `command`, optionally
`timeout` in seconds). The worker registers them as the host's `beforeToolCall`, `afterToolCall`, and
`onStop`; `Stop` is reported as `SubagentStop`. Exit 2 blocks a tool, adds PostToolUse context, or asks a
stopping subagent for another turn; every other exit is non-blocking. Each command gets
`BETTER_SUBAGENTS_HOOK_EVENT`, `..._AGENT`, `..._TOOL_NAME`, `..._TOOL_INPUT` (JSON) and `..._CWD`.
Project hooks in `.commandcode/hooks.json` (`SubagentStart` / `SubagentStop`) run once per job, and a
plugin ignores the field.

`initialPrompt` is auto-submitted as the first turn only when the definition runs the whole session
(`agent` flag or `BETTER_SUBAGENTS_AGENT`/`CLAUDE_CODE_AGENT`), never for a subagent, and a plugin
ignores it. `autoCompactWindow` is clamped to 100,000–1,000,000 and can only lower the model's inherited
window; it has no effect on the main session. `experimental.cacheTtl` accepts `5m` or `1h`. ACP exposes
neither the window nor the TTL as a per-session option, so the adapter logs each resolved value as a
recorded deviation (`docs/adr/0001-worker-and-control.md`) and the host default still governs. The
prompt-cache split is recorded at spawn: a fork carries `promptCache: "shared"`, every other agent
`"own"`.

`--append-subagent-system-prompt` (text, `@path`, or the `append_subagent_system_prompt_file` flag) is
appended to every subagent's system prompt. It is delivered through the mod flag or
`BETTER_SUBAGENTS_APPEND_SYSTEM_PROMPT`/`CLAUDE_CODE_APPEND_SUBAGENT_SYSTEM_PROMPT`, so a nested worker
inherits it; a fork is exempt because it reuses the main conversation's prompt.

`isolation: worktree` runs the job in a temporary `git worktree` branched from the repository's default
branch (`origin/HEAD`, then `main`, then `master`) — never the caller's HEAD. Inside the worker, Bash and
Monitor calls are refused when their working directory resolves to the main checkout, and when a redirect,
`cd`, `git -C`/`--work-tree`/`--git-dir`, `GIT_DIR` or `git --prefix` would leave the worktree;
PowerShell gets the working-directory check. A job that leaves no changes removes its worktree; a job
that changes files keeps it and the delivered result names the path. A session that itself runs in a
linked worktree applies the same checks to its own tools and to every subagent it spawns.

`subagent_type` is canonical; `agent_type` is a deprecated alias. Omitting the type uses
`general-purpose`; with no `general-purpose` definition the call fails with `subagent_type is required`.

With the mod installed, the body is injected through the host's `appendSystemPrompt` hook in the ACP
worker. When the mod is loaded only with `--mod` (development), this host's `cmd acp` ignores `--mod`, so
the worker carries the body at the front of the task prompt instead; the job log says which path ran.

### Startup context

Every agent job starts from a bundle built in the parent, not from the parent's conversation:

- the definition body, the working directory, platform, date, model, and that model's context window;
- the CLAUDE.md hierarchy (managed policy, user, each project directory from the root down, `CLAUDE.local.md`)
  and `AGENTS.md`, loaded as project instructions;
- a bounded `git status --short --branch` snapshot;
- the full content of every skill named in `skills`;
- the sibling roster, the output style, and the agent's own `MEMORY.md`.

Explore and Plan skip the instruction hierarchy and the git snapshot. `omitClaudeMd` skips the user,
project, and local CLAUDE.md files and `AGENTS.md`, keeping managed policy unless the agent is managed. A
CLAUDE.md in a subdirectory the agent works with still loads in both cases. A subagent never inherits the
parent's auto memory.

`memory: user | project | local` creates `~/.commandcode/agent-memory/<name>/`,
`<cwd>/.commandcode/agent-memory/<name>/`, or `<cwd>/.commandcode/agent-memory-local/<name>/`, and injects
the first 200 lines or 25KB of its `MEMORY.md`, whichever comes first. The worker's system prompt carries
the bundle when the `appendSystemPrompt` hook is live; otherwise it precedes the task in the prompt.

`permission_mode` is resolved in the parent (see Permission modes); `isolation` is **refused
explicitly** rather than silently ignored.

### Foreground and background

A run is background by default: `subagent_spawn` returns the moment the worker is detached and the result
arrives later as a signal. `run_in_background: false` is a foreground run: the call waits (bounded to 30
minutes) and returns the agent's output as its result, and the same result is not delivered again as a
signal. Precedence is fixed: `CLAUDE_CODE_DISABLE_BACKGROUND_TASKS=1` (alias
`BETTER_SUBAGENTS_DISABLE_BACKGROUND=1`) forces every run into the foreground, a definition's
`background: true` forces background over the caller's request, and otherwise the request decides.

A foreground run cut off by an API error (rate limit, overload, server error) returns the text it already
produced plus a cut-off note. With no agent text — nothing, or only tool calls — the call fails with
`Agent terminated early due to an API error` and the error detail. A background run that hits the same
error is marked `failed`, and its delivered message names the error and carries the last output. Partial
work is never discarded.

**Fork mode** is on by default in an interactive session and off in a non-interactive run or the Agent
SDK (`CLAUDE_AGENT_SDK=1`). `--mod-option fork_mode=on|off` or `BETTER_SUBAGENTS_FORK_MODE` (alias
`CLAUDE_CODE_FORK_MODE`) controls it in either direction. Where it is on, a model-spawned agent runs in
the background and `run_in_background` is **removed from the tool schema**, so the model cannot ask for
the foreground. Ctrl+B is still the user's escape hatch on a foreground run: the tool call returns and the
worker keeps going, its result arriving as a normal signal.

### Live messaging

A spawn with `name` is addressable. Sending to it is one tool call:

```jsonc
{"to": "Tester", "message": "Check the auth path before you finish."}
```

The target's state decides the path:

- **running**: the message is appended to `control/<job>/send.jsonl` while the ACP turn finishes
  normally. The worker reads the queue at the next safe point and runs the message as its own prompt
  (`session/prompt`); the live turn is never restarted.
- **idle**: the worker is alive with no active turn, so the message starts a turn immediately.
- **finished**: a new worker resumes the recorded ACP session (`session/resume`) with the same
  definition, model, effort, permissions, and full context. The message is the new turn.

Each live turn reports back as its own `kind: "message"` signal containing only that turn's reply, so the
sender sees the answer without waiting for the agent to exit. An idle live agent waits 30 seconds for a
message (`BETTER_SUBAGENTS_LIVE_IDLE_MS` overrides it) and then exits; a later message resumes it.
`main` is always addressable: from an agent it writes a signal owned by the parent session; from the main
session it is delivered as a note.

A named agent starts with a **sibling roster** snapshot when at least one other named agent exists:
`main` plus the other named agents, with their state and job id. `ListAgents` exposes the same set. A
message from a worker to a peer is accepted only for a name in that roster.

`SubagentHandback` is the explicit final-report path for an agent that reports through it: the report is
delivered as one signal and the wrapper suppresses the duplicate end-of-turn result.

### Forks and agent teams

`subagent_type: "fork"` starts a fork instead of a fresh subagent. A fork inherits the main
conversation's history, system prompt, tools, model, permissions, and prompt cache — it is not filtered
and gets no fresh startup bundle. ACP has no fork primitive, so the first turn is the host CLI's
`--fork-session --resume <main>` with `--output-format json`; the result line names the new session, which
each follow-up resumes with `cmd --resume <fork> -p`. Follow-ups are one-shot turns: a message sent while a
turn is running is queued as the next turn, not injected mid-turn.

`/subtask <prompt>` starts a fork when the agent view is off; `/fork` stays the host's command. The
`/tasks` view (and the jobs pane) moves between rows (`next`/`prev`), opens a transcript (`enter`/`open`),
sends a follow-up (`send <id> <message>`), stops a live row or dismisses a finished one (`x`), and returns
to the prompt (`esc`). A successful row disappears at once and leaves a `/tasks` footer hint for 30
seconds; a failed or stopped row stays 30 seconds until `x` clears it.

`team_name` on `subagent_spawn` makes the run a teammate. A teammate is always foreground: its definition
may not set `background: true`, and `run_in_background: true` is refused while fork mode is off. Teammates
keep `TaskCreate`/`TaskGet`/`TaskList`/`TaskUpdate` and `CronCreate`/`CronDelete`/`CronList` even when the
definition's `tools` or `disallowedTools` would remove them.

`/tasks mention <prefix>` lists the `@mention` candidates: every valid custom, built-in, and
plugin-scoped definition plus every named agent in the session. The host owns the prompt's typeahead
widget; the mod supplies the candidate list, not a replacement widget.
### Tool access, model, and effort

`tools` and `disallowedTools` are comma strings or YAML lists, and the spawn arguments `tools` /
`disallowed_tools` override the definition for one invocation.

- `tools` only: only the listed tools remain. `disallowedTools` only: the listed tools are removed from
  the inherited set. Both: `disallowedTools` wins, so a tool in both lists is removed.
- `mcp__server__*` selects one server's tools, `mcp__*` selects every MCP tool, and `mcp__server` or
  `mcp__server__*` in `disallowedTools` withholds them. A specifier such as `Bash(git push *)` removes
  the whole tool.
- Every subagent loses `AskUserQuestion`, `EndConversation`, `EnterPlanMode`, `ScheduleWakeup`,
  `WaitForMcpServers`, `Workflow`, and `ExitPlanMode` unless its permission mode is `plan`.
- A background subagent keeps every MCP tool but only the allowlisted built-ins, so `ListAgents` is
  foreground-only.
- A `tools` list that resolves to nothing fails the launch and names the entries.

Model: the per-invocation `model`, then the definition's `model`, then `BETTER_SUBAGENTS_SUBAGENT_MODEL`
(or `CLAUDE_CODE_SUBAGENT_MODEL`), then the main model. `inherit` selects the main model.
`BETTER_SUBAGENTS_SUBAGENT_MODEL_FORCE=1` (or the Claude Code alias) makes the environment value win over
the parameter and the definition. Family aliases resolve against the session's model catalog; a value the
allowlist blocks falls back to the inherited model and is logged.

Effort: the per-invocation `effort`, then the definition's `effort`, then `BETTER_SUBAGENTS_EFFORT_LEVEL`
(or `CLAUDE_CODE_EFFORT_LEVEL`). Effort is `low`, `medium`, `high`, `xhigh`, or `max`. Extended thinking
is inherited from the main conversation; there is no per-subagent thinking setting. The definition's
`color` appears in the job list and the jobs pane.

### Permission modes

A definition's `permissionMode` is resolved with the main conversation's mode before the worker starts:
`default`, `acceptEdits`, `auto`, `dontAsk`, `bypassPermissions`, `plan`; `manual` is an alias for
`default`. The main mode wins when it is `acceptEdits`, `auto`, or `bypassPermissions`; otherwise the
definition's mode applies, except that `bypassPermissions` still requires the main conversation to be in
it. `permission_mode` on `subagent_spawn` is a per-call override that follows the same rules. An unknown
definition mode is ignored (default); an unknown per-call mode is refused.

The mode reaches the worker with ACP `session/set_mode`: `acceptEdits` → `auto-accept`, `dontAsk` →
`dont-ask`, `bypassPermissions` → `bypass`, `plan` → `plan`, `default`/`manual` → `default`. `auto` is the
host classifier, which has no ACP equivalent, so it is left to the host. The same table carries the
`cmd --permission-mode` spelling (`standard`, `accept-edits`, `yolo`, `plan`) for the CLI paths.

A worker that needs approval lands in `waiting-input` with the request on its signal
(`permissionRequest` and a human-readable `question`); the job keeps its ACP session id, so it stays
resumable. A foreground run writes the request to `<subagent_dir>/control/<job id>/permission.json` and
waits, bounded (30 seconds by default), for `permission-response.json`:
`{"outcome":"selected","optionId":"…"}` approves, `{"outcome":"cancelled"}` denies. Esc is the cancelled
answer: it denies that one call and the turn continues. If nobody answers, the run settles `waiting-input`
instead of hanging. `dontAsk` and `plan` deny approval requests; `bypassPermissions` allows them.
Background prompts are delivered in the main session as a signal that names the agent.

### Deprecated aliases (one release)

`pubsub_fire`, `pubsub_list` and `pubsub_cancel` are still registered, marked `[deprecated: use
subagent_…]`, and delegate to the **same run function**, so an installed prompt, a script, or a habit that
still calls the old name cannot drift. So are the `/pubsub` command and the `pubsub_dir` /
`pubsub_deliver` flags.

`pubsub_send` is not an alias: it is the 0.1.0 **note** channel. It takes `title`/`body` and publishes a
note into this session; `subagent_send` takes `to`/`message` and delivers a live message to another agent.

## Command

```
/subagents list          # jobs, states, unread results, pending signal count
/subagents jobs          # open the async-jobs dashboard pane
/subagents cancel <id>   # cancel a queued/running job
/subagents open <id>     # one job: state, exit code, duration, log
/tasks                   # the agent panel: next, prev, enter, send, x, stop, esc, mention
/subtask <prompt>        # start a fork of the main conversation when the agent view is off
/agents [name|path]      # subagent definitions: scope, validity, source file
/doctor [plugins]        # duplicate, invalid, and plugin-file problems, with the fix
```

The `jobs` pane lists recent jobs with a state marker (`◌` queued, `●` running, `?` waiting-input,
`✓` done, `✗` failed, `⊘` canceled). A row can be selected (`↑`/`↓`), opened (`↵`), stopped or
dismissed (`x`), and left (`esc`); **Refresh** (`r`) / **Cancel newest** (`c`) remain. Success rows
vanish immediately, failed/stopped rows stay for 30 seconds, and Ctrl+B moves a running foreground task
to the background. The footer keeps the running count and a `/tasks` hint for 30 seconds after a result.

## Flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `subagent_dir` | `~/.commandcode/subagents` | Where the spool, logs and job records live. Point several sessions at one shared spool (the default) or give a project its own. |
| `subagent_deliver` | `steer` | How a signal reaches the session: `steer` delivers right after the current tool batch; `follow-up` waits until the run would otherwise stop. |
| `managed_dir` | `~/.commandcode/managed` | The managed settings directory; `agents/` under it wins every other scope. |
| `agents` | empty | Session-only definitions as inline JSON, or a JSON file path in a non-interactive session (`--mod-option agents=...`). |
| `add_dir` | empty | Comma-separated extra directories whose `.commandcode/agents/` is loaded (the host's `--add-dir`). |
| `plugin_dir` | empty | Comma-separated plugin roots; every child directory with an `agents/` directory is a plugin. |
| `fork_mode` | `auto` | `auto` = interactive on, non-interactive/SDK off; `on`/`off` override. Where it is on, model-spawned agents run in the background and `run_in_background` is removed. |
| `agent` | empty | Run the whole session as this agent definition (`--agent` is not a host flag, so use the mod option or `BETTER_SUBAGENTS_AGENT`). Its body becomes the main system prompt and its `initialPrompt` is the first turn. |
| `append_subagent_system_prompt` | empty | Text or `@path` appended to every subagent's system prompt (`BETTER_SUBAGENTS_APPEND_SYSTEM_PROMPT` is the environment form). |
| `append_subagent_system_prompt_file` | empty | File whose text is appended to every subagent's system prompt. |

`pubsub_dir` and `pubsub_deliver` are deprecated aliases, recognised for one release when set to
anything other than their own default.

## Lifecycle

```
queued → running → waiting-input | done | failed | canceled
```

- **queued** — the job record is written, the detached worker is being spawned.
- **running** — the worker has written its child's pid.
- **waiting-input** — a shell job exited with a question (its last log line ends in `?`). A headless
  worker cannot stay alive waiting for input, so answering it means firing a follow-up job. An ACP
  agent job ends its turn (`end_turn`) and reports `done` instead.
- **done** / **failed** — the child exited, with code, duration and a log tail carried in the signal.
- **canceled** — terminal. `subagent_cancel` kills the child and worker and emits a `canceled` signal.
  A worker that notices the cancellation while exiting will not overwrite the state.

Crashes are reconciled, not ignored: a `queued`/`running` record whose pid and worker pid are both dead,
with no signal on disk and past a 30s grace, is marked `failed` (`lost (no live process)` or
`worker never started`) **and emits one failed signal**, so the session that fired it learns the result is
gone instead of waiting forever. A detached worker survives the harness, so a live pid always means keep
waiting.

A job that overruns `timeout_ms` is `SIGTERM`ed and then `SIGKILL`ed, and signals `failed` with
`timed out after …`. An ACP turn reports its `stopReason`: `end_turn` is `done`; `cancelled` is `failed`
with the reason. `max_tokens` and `max_turn_requests` (the `maxTurns` cap) keep the output already
produced: the record and signal carry `partial: true`, the foreground returns it with a `[partial: …]`
note, and the agent id lets a later `subagent_resume` continue it. A rejected `model` or `effort`
config option is logged and the turn continues on the session default. A process that exits before the
prompt completes is `failed` with `ACP exited before the prompt completed`, so an API or auth failure
is visible rather than silent.

A completed agent job returns an **agent id**; `subagent_resume` accepts either that id or the job id.
`explore` and `plan` are one-shot: they return no agent id and refuse a resume.

Finished jobs keep their record, their log, and their transcript for **30 days**, then all three are
swept together. `BETTER_SUBAGENTS_RETENTION_DAYS` (alias `CLAUDE_CODE_CLEANUP_PERIOD_DAYS`) or
`cleanupPeriodDays` in `.commandcode/settings.json` sets the window. A running job is never swept,
however old its record is.

### Resume, transcripts, and compaction

- Every agent job persists its conversation to `transcripts/<id>.jsonl` as the ACP `session/update`
  stream arrives. It lives outside the main session file, so compacting the main transcript cannot
  touch it, and a restart reads the same file.
- `subagent_resume` continues the recorded ACP session (`session/resume`) and keeps the per-invocation
  model, effort, `maxTurns`, `autoCompactWindow`, and `cacheTtl`. A partial `maxTurns` result is
  resumable for the same reason.
- Each subagent has its own context window. The adapter watches the host's `usage_update`; when the used
  count crosses the resolved window it records and logs `compact_boundary` with `trigger: "auto"` and
  the pre-compaction token count, and carries it on the signal. ACP emits no compaction event of its
  own, so the crossing is the observable boundary (recorded deviation).
- Prompt cache: a fork records `promptCache: "shared"` (the host fork copies the main session's cache);
  every other agent records `"own"`. `cacheTtl` is carried and logged on the spec, but ACP has no option
  for it, so the host default governs.

## Ownership — why another session's result never lands in yours

The spool is shared by every session on the machine, so **every job and every signal carries an owner
token**, and a drain only delivers signals whose owner matches.

- The token is minted once per session and persisted in the session transcript:

  ```js
  cmd.session.appendCustomEntry({customType: 'subagent/owner', data: {token}})
  ```

  It is read back with `cmd.session.getCustomEntries({customType: 'subagent/owner'})`, so it **survives
  resume and hot reload** — the same session keeps the same identity. The 0.1.0 entry (`pubsub/owner`) is
  read as a fallback, so a session that was live across the rename keeps the identity its older jobs were
  stamped with.

- A drain that claims a file it does not own **puts it back** in `inbox/` rather than deleting it, so the
  owning session can still collect it later.
- **Untagged legacy signals** (written by older versions, with no `owner` field) are never delivered to
  "whoever happens to be running". They are quarantined to `orphaned/`.
- A headless worker is itself a Command Code process; it runs with `BETTER_SUBAGENTS_WORKER=1` so its own
  mod instance never steals an unaddressed signal.

Delivery is also exactly-once per job id: signals are claimed by atomic `rename` into `claimed/`
(exactly one drainer can win a file), job ids are deduplicated in memory, and the check is seeded from a
durable ledger so a restart cannot replay what a previous process already delivered.

## Spool layout

Under `subagent_dir` (default `~/.commandcode/subagents`):

```
inbox/            workers drop <timestamp>-<id>.json here
claimed/          rename target during delivery, so two drains cannot deliver one signal twice
orphaned/         untagged legacy signals, quarantined instead of leaked
logs/             one log per fired job
transcripts/      one append-only JSONL conversation per agent job, addressed by agent id
jobs/             one JSON record per job (for subagent_list and the jobs pane)
control/          one directory per live agent: send.jsonl, cancel, state.json (and permission files)
delivered.jsonl   durable delivery ledger, so a restart cannot replay a signal
worktrees/        `isolation: worktree` jobs; a changed one is kept and reported, an unchanged one removed
```

`claimed/` and `orphaned/` are swept on session start (1h and 7d TTLs), the ledger is compacted past
512 KB, and every loop is bounded (50 files per drain, 200 job records per reconcile, 4 KB log tail).

**The worker is a detached, headless OS process** (`spawn(process.execPath, ['-e', WORKER_SOURCE], {detached: true, stdio: 'ignore'})`
+ `unref()`). It outlives the harness on purpose: if Command Code exits mid-job, the worker still finishes
and still writes its signal, and the next session with the matching owner drains it.

The failure modes are all closed:

- spawn failure (sync throw, async `error` event, or a missing pid) → job marked `failed`, signal emitted.
  It never sits at `queued` forever and never becomes an uncaught exception in the host.
- no session bound at delivery time → the signal is put back in `inbox/` instead of being dropped.
- a worker that finishes in the gap before the next tick → the `onStop` hook claims what is still on disk
  and continues the run with it, so the session never falls idle holding an unread result.
- a drain that throws → swallowed; the next tick retries. A mod must never take the session down.

## Security

**This mod runs arbitrary commands — the ones you ask it to run.** `subagent_spawn` with `command` passes
your string to `/bin/sh -c`, and with `prompt` starts a full headless Command Code agent that can use its
own tools. Both run **detached and headless, with no TUI to prompt you and no approval gate**, in the
`cwd` you give them and with the mod's environment (`SUBAGENT_SPEC` carries the JSON job spec, so quotes,
`$` and newlines in a prompt or command cannot break the wrapper — there is no shell interpolation of it).

Treat `subagent_spawn` exactly like handing a shell to the model: only enable the mod where the model is
allowed to run background commands, and do not point it at untrusted prompts. Job records and logs are
written to `subagent_dir` in plain text and are readable by anything that can read that directory.

`subagent_send` appends to the target agent's control queue (or resumes its session); `pubsub_send`
injects a note into your own session's agent loop. Only signals whose owner token matches the session are
delivered, and untagged signals are quarantined, but **the token is an isolation mechanism, not an
authorization boundary**: any local process that can write to the shared spool can write a signal tagged
with a token it has read from disk. A live agent accepts a peer message only for a name in the roster the
parent handed it. Use a private `subagent_dir` if that matters.

No network, no telemetry, no credentials: the mod never reads `auth.json`, opens no socket, and makes no
outbound connection.

## Internals

The mod ships as one file, but it is not one flat script: each seam is a small section behind a narrow
interface, and the smoke test drives every one of them.

| Seam | Interface | Why it is a module |
| --- | --- | --- |
| Runtime | `Runtime`: `spawn`, `kill`, `alive`, `now`, `sleep`, `pid`, `execPath`, `env`, `cwd`, `home`, `fs`, `run` | The OS is one adapter, the test is the other. No other code names `node:fs`, `node:child_process` or the clock. Worktree git goes through `run` via `gitSeam`. |
| States | `isState`, `normalizeState`, `canTransition`, `isTerminal` | The six states and their edges in one table. An invalid edge is rejected; an unknown state is loud. |
| Envelope | `signalOf(record, result)`, `patchOf(signal)` | One contract for "this job reached this state", in both directions. |
| Spool | `next() -> Claim`, `settle(claim, 'accepted' \| 'held' \| 'quarantined')` | One claim policy for the drain, `onStop` and the pending count. A signal is never deleted because it belongs to someone else. |
| Launch | `fire(intent)`, `failJob(job, reason)` | One failure contract: one record, one signal, one delivery. The next guard reuses it. |
| Owner | `id()`, verdict `mine \| foreign \| orphan \| pending` | One identity rule. `pending` is a state, and there is no invented `process-<pid>` identity. |
| Ledger | `has(id)`, `accept(id)`, `retain(n)` | The replay guard and its retention, in memory and on disk together. |
| Surface | `addSurface(name, …, run, {aliases})` | One implementation per tool; `pubsub_*` is registered from the same `run`, so the deprecated names cannot drift. |

Follow-up: the detached worker is still an inline string (`WORKER_SOURCE`) because the mod ships as one
file with nothing to install. Extracting it into a real module would let the worker share the envelope and
state code and would make the `waiting-input` heuristic a unit test, but it needs a different ship model
(two files, or a build step), so it is not part of this pass.

## Tests

```bash
node test/smoke.mjs   # the unit/behaviour suite: prints PASS, never touches the network
node test/e2e.mjs     # the real thing: a real printf job, a real ACP job, a real timeout, a resume
```

`test/e2e.mjs` needs `cmd` on PATH (or `COMMANDCODE_BIN`), a signed-in Command Code and network, so CI runs
only the smoke suite. It prints the spool path it used, and leaves the artifacts there as evidence:
`jobs/<id>.json`, `logs/<id>.log`, `transcripts/<id>.jsonl` and the `inbox/` signal for each job.

The smoke test imports `better-sub-agents.ts` directly with Node's native TypeScript type stripping and drives it with
a fake `ModApi` and a fake `Runtime` (both in `test/harness.mjs`) — no dependencies, no build, no network,
no process. It covers the
public surface (tools, command, flags, renderer, pane, hooks, timers) and each seam on its own: the runtime
seam is really used (one spawn, both kills through the injected `kill`, the injected clock on the record),
the state table's edges, the envelope's record/signal agreement, a foreign signal surviving a failed
restore, exactly one signal from each spawn-failure mode, the owner token being retried rather than
replaced by `process-<pid>`, and the ledger's retention and restart behaviour. The T13 checks cover the
agent id and the one-shot refusal, resume by agent id with model/effort/cache/window preserved, the
fork/own prompt-cache split, transcript persistence across a restart, retention sweeping record + log +
transcript together, and a `maxTurns` stop that is partial and emits `compact_boundary`. CI runs it on
Node 24.

## License

MIT © 2026 Prem Modha
