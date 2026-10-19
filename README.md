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
| `subagent_spawn` | `description`, plus **either** `prompt` **or** `command`; optional `subagent_type` (or the deprecated `agent_type`), `model`, `max_turns`, `timeout_ms`, `cwd` | Starts a detached worker and returns immediately. `command` runs under `/bin/sh -c`; `prompt` runs a subagent definition over an ACP session. `subagent_type` defaults to `general-purpose`; `cwd` defaults to the mod workspace root. |
| `subagent_list` | `state?`, `limit?` | Spawned jobs with state, model, elapsed time and owner, plus how many signals are still waiting for this session. |
| `subagent_output` | `id`, `wait?`, `wait_ms?`, `offset?`, `max_chars?` | One job's state, exit code, duration and owner, plus a bounded slice of its log (tail by default). `wait` blocks briefly until the job finishes — opt in only when you actually need it. |
| `subagent_cancel` | `id` | Cancels a `queued` or `running` job (`SIGTERM`, then `SIGKILL` after 1s). The firing session receives one `canceled` signal. |
| `subagent_resume` | `id`, `prompt` | Continues a finished **agent** job in its ACP session with a new instruction. The resumed run is a new job with its own signal. |
| `subagent_send` | `title`, `body` | Publishes a signal into the agent loop of **this** session. The agent-to-agent channel: worker and peer agents signal a session so neither blocks or polls. Delivered as a visible user turn. |

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
`skills`, `isolation`, `background`, `color`, `omitClaudeMd`, `effort`. Unknown fields are ignored. A file
with a missing name or description, a bad first line, a colon or over-256-character name, or unparseable
YAML is skipped and written to `<subagent_dir>/agents.log`; `/agents` lists every definition and every
skip with its reason, and `/doctor` reports each duplicate and invalid file with the fix.

`subagent_type` is canonical; `agent_type` is a deprecated alias. Omitting the type uses
`general-purpose`; with no `general-purpose` definition the call fails with `subagent_type is required`.

With the mod installed, the body is injected through the host's `appendSystemPrompt` hook in the ACP
worker. When the mod is loaded only with `--mod` (development), this host's `cmd acp` ignores `--mod`, so
the worker carries the body at the front of the task prompt instead; the job log says which path ran.

`permission_mode`, `isolation` and `tools` are **refused explicitly** rather than silently ignored: they
arrive in phase 8. A foreground run (`run_in_background: false`) is refused for the same reason — the
background path is the only one that exists yet.

### Deprecated aliases (one release)

`pubsub_fire`, `pubsub_list`, `pubsub_cancel` and `pubsub_send` are still registered and marked
`[deprecated: use subagent_…]`. They are registered from the **same run function**, so an installed
prompt, a script, or a habit that still calls the old name behaves identically and cannot drift. So are
the `/pubsub` command and the `pubsub_dir` / `pubsub_deliver` flags.

## Command

```
/subagents list          # jobs, states, unread results, pending signal count
/subagents jobs          # open the async-jobs dashboard pane
/subagents cancel <id>   # cancel a queued/running job
/subagents open <id>     # one job: state, exit code, duration, log
/agents [name|path]      # subagent definitions: scope, validity, source file
/doctor [plugins]        # duplicate, invalid, and plugin-file problems, with the fix
```

The `jobs` pane lists recent jobs with a state marker (`◌` queued, `●` running, `?` waiting-input,
`✓` done, `✗` failed, `⊘` canceled) and has **Refresh** (`r`) / **Cancel newest** (`c`) buttons.

## Flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `subagent_dir` | `~/.commandcode/subagents` | Where the spool, logs and job records live. Point several sessions at one shared spool (the default) or give a project its own. |
| `subagent_deliver` | `steer` | How a signal reaches the session: `steer` delivers right after the current tool batch; `follow-up` waits until the run would otherwise stop. |
| `managed_dir` | `~/.commandcode/managed` | The managed settings directory; `agents/` under it wins every other scope. |
| `agents` | empty | Session-only definitions as inline JSON, or a JSON file path in a non-interactive session (`--mod-option agents=...`). |
| `add_dir` | empty | Comma-separated extra directories whose `.commandcode/agents/` is loaded (the host's `--add-dir`). |
| `plugin_dir` | empty | Comma-separated plugin roots; every child directory with an `agents/` directory is a plugin. |

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
`timed out after …`. An ACP turn reports its `stopReason`: `end_turn` is `done`; `cancelled`,
`max_tokens` and `max_turn_requests` are `failed` with the reason. A rejected `model` or `effort`
config option is logged and the turn continues on the session default. A process that exits before the
prompt completes is `failed` with `ACP exited before the prompt completed`, so an API or auth failure
is visible rather than silent.

Finished jobs keep their record and their log for **30 days** (`BETTER_SUBAGENTS_RETENTION_DAYS` overrides
it), then both are swept. A running job is never swept, however old its record is.

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
jobs/             one JSON record per job (for subagent_list and the jobs pane)
delivered.jsonl   durable delivery ledger, so a restart cannot replay a signal
worktrees/        reserved for `isolation: worktree` (phase 7)
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

`subagent_send` injects a message into your own session's agent loop. Only signals whose owner token matches
the session are delivered, and untagged signals are quarantined, but **the token is an isolation
mechanism, not an authorization boundary**: any local process that can write to the shared spool can
write a signal tagged with a token it has read from disk. Use a private `subagent_dir` if that matters.

No network, no telemetry, no credentials: the mod never reads `auth.json`, opens no socket, and makes no
outbound connection.

## Internals

The mod ships as one file, but it is not one flat script: each seam is a small section behind a narrow
interface, and the smoke test drives every one of them.

| Seam | Interface | Why it is a module |
| --- | --- | --- |
| Runtime | `Runtime`: `spawn`, `kill`, `alive`, `now`, `sleep`, `pid`, `execPath`, `env`, `cwd`, `home`, `fs` | The OS is one adapter, the test is the other. No other code names `node:fs`, `node:child_process` or the clock. |
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
`jobs/<id>.json`, `logs/<id>.log` and the `inbox/` signal for each job.

The smoke test imports `better-sub-agents.ts` directly with Node's native TypeScript type stripping and drives it with
a fake `ModApi` and a fake `Runtime` (both in `test/harness.mjs`) — no dependencies, no build, no network,
no process. It covers the
public surface (tools, command, flags, renderer, pane, hooks, timers) and each seam on its own: the runtime
seam is really used (one spawn, both kills through the injected `kill`, the injected clock on the record),
the state table's edges, the envelope's record/signal agreement, a foreign signal surviving a failed
restore, exactly one signal from each spawn-failure mode, the owner token being retried rather than
replaced by `process-<pid>`, and the ledger's retention and restart behaviour. CI runs it on Node 24.

## License

MIT © 2026 Prem Modha
