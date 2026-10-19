# cmd-mod-pubsub

A [Command Code](https://github.com/PremModhaOfficial) mod that turns a blocking "fire an agent and wait"
into **fire-and-forget**: you start a worker, your session carries on immediately, and when the worker
finishes its result **lands back in the session that fired it** as a separate message on its own.

Neither side blocks. Nothing polls. The wake-up *is* the message arriving.

```
you ──pubsub_fire──▶ detached worker  ──spool file──▶  drain  ──sendUserMessage──▶ you
       (returns now)  (own process)                    (400ms)  (starts the turn)
```

## Why

`agent`, `task` and friends are fire-and-*wait*: the parent has to hold a turn open, poll, or ask for the
result later. Every one of those costs a turn while nothing is happening. `pubsub` uses
`cmd.sendUserMessage(...)` as the seam instead — it starts a turn when the session is idle and queues
behind the active turn otherwise, so a result can arrive at any time without the model waiting on it.

Transport is a **spool directory**, not a socket: no ports, no auth, no reconnect logic. A worker writes
one JSON file and exits. If a signal lands while no session is running, it is still there next time.

## Requirements

- **Command Code 1.84+** (for `sendUserMessage({deliverAs})`, `session.appendCustomEntry` /
  `getCustomEntries`, and the `ui.pane` host surface)
- Node 22.18+ if you want to run the smoke test with native TypeScript type stripping (no build step)

## Install

```bash
# as a global mod
cp pubsub.ts ~/.commandcode/mods/

# or load it for one run, without installing
cmd --mod ./pubsub.ts
```

No dependencies, no build step, no `npm install`. The mod is a single file: the detached worker's source
is inlined into it, so `pubsub.ts` is the whole mod.

## Tools

| Tool | Arguments | What it does |
| --- | --- | --- |
| `pubsub_fire` | `label`, plus **either** `command` **or** `prompt`, and optional `cwd` | Starts a detached worker and returns immediately. `command` runs under `/bin/sh -c`; `prompt` runs a headless `cmd -p` in a fresh process. `cwd` defaults to the mod workspace root. |
| `pubsub_send` | `title`, `body` | Publishes a signal into the agent loop of **this** session. The agent-to-agent channel: worker and peer agents signal a session so neither blocks or polls. Delivered as a visible user turn. |
| `pubsub_list` | – | Fired jobs, their lifecycle states, unread results, and how many signals are still waiting for this session. |
| `pubsub_cancel` | `id` | Cancels a `queued` or `running` job (`SIGTERM`, then `SIGKILL` after 1s). The firing session receives a `canceled` signal. |

`pubsub_fire` is the whole point: it returns in milliseconds with the job id and pid, and the result
arrives later **as a signal, never as a tool result** — so there is nothing to wait for.

```jsonc
// fire a shell command
{"label": "typecheck", "command": "tsc --noEmit", "cwd": "/repo"}

// fire a headless agent
{"label": "audit-deps", "prompt": "Audit dependencies for unused packages and report."}
```

## Command

```
/pubsub list          # jobs, states, unread results, pending signal count
/pubsub cancel <id>   # cancel a queued/running job
/pubsub jobs          # open the async-jobs dashboard pane
```

The `jobs` pane lists recent jobs with a state marker (`◌` queued, `●` running, `?` waiting-input,
`✓` done, `✗` failed, `⊘` canceled) and has **Refresh** (`r`) / **Cancel newest** (`c`) buttons.

## Flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `pubsub_dir` | `~/.commandcode/pubsub` | Where the spool, logs and job records live. Point several sessions at one shared spool (the default) or give a project its own. |
| `pubsub_deliver` | `steer` | How a signal reaches the session: `steer` delivers right after the current tool batch; `follow-up` waits until the run would otherwise stop. |

## Lifecycle

```
queued → running → waiting-input | done | failed | canceled
```

- **queued** — the job record is written, the detached worker is being spawned.
- **running** — the worker has written its child's pid.
- **waiting-input** — the worker exited with a question (its last log line ends in `?`). A headless
  worker cannot stay alive waiting for input, so answering it means firing a follow-up job.
- **done** / **failed** — the child exited, with code, duration and a log tail carried in the signal.
- **canceled** — terminal. `pubsub_cancel` kills the child and worker and emits a `canceled` signal.
  A worker that notices the cancellation while exiting will not overwrite the state.

Crashes are reconciled, not ignored: a `queued`/`running` record whose pid and worker pid are both dead,
with no signal on disk and past a 30s grace, is marked `failed` (`lost (no live process)` or
`worker never started`). A detached worker survives the harness, so a live pid always means keep waiting.

## Ownership — why another session's result never lands in yours

The spool is shared by every session on the machine, so **every job and every signal carries an owner
token**, and a drain only delivers signals whose owner matches.

- The token is minted once per session and persisted in the session transcript:

  ```js
  cmd.session.appendCustomEntry({customType: 'pubsub/owner', data: {token}})
  ```

  It is read back with `cmd.session.getCustomEntries({customType: 'pubsub/owner'})`, so it **survives
  resume and hot reload** — the same session keeps the same identity.

- A drain that claims a file it does not own **puts it back** in `inbox/` rather than deleting it, so the
  owning session can still collect it later.
- **Untagged legacy signals** (written by older versions, with no `owner` field) are never delivered to
  "whoever happens to be running". They are quarantined to `orphaned/`.
- A headless worker is itself a Command Code process; it runs with `PUBSUB_WORKER=1` so its own mod
  instance never steals an unaddressed signal.

Delivery is also exactly-once per job id: signals are claimed by atomic `rename` into `claimed/`
(exactly one drainer can win a file), job ids are deduplicated in memory, and the check is seeded from a
durable ledger so a restart cannot replay what a previous process already delivered.

## Spool layout

Under `pubsub_dir` (default `~/.commandcode/pubsub`):

```
inbox/            workers drop <timestamp>-<id>.json here
claimed/          rename target during delivery, so two drains cannot deliver one signal twice
orphaned/         untagged legacy signals, quarantined instead of leaked
logs/             one log per fired job
jobs/             one JSON record per job (for pubsub_list and the jobs pane)
delivered.jsonl   durable delivery ledger, so a restart cannot replay a signal
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

**This mod runs arbitrary commands — the ones you ask it to run.** `pubsub_fire` with `command` passes
your string to `/bin/sh -c`, and with `prompt` starts a full headless Command Code agent that can use its
own tools. Both run **detached and headless, with no TUI to prompt you and no approval gate**, in the
`cwd` you give them and with the mod's environment (`PUBSUB_SPEC` carries the JSON job spec, so quotes,
`$` and newlines in a prompt or command cannot break the wrapper — there is no shell interpolation of it).

Treat `pubsub_fire` exactly like handing a shell to the model: only enable the mod where the model is
allowed to run background commands, and do not point it at untrusted prompts. Job records and logs are
written to `pubsub_dir` in plain text and are readable by anything that can read that directory.

`pubsub_send` injects a message into your own session's agent loop. Only signals whose owner token matches
the session are delivered, and untagged signals are quarantined, but **the token is an isolation
mechanism, not an authorization boundary**: any local process that can write to the shared spool can
write a signal tagged with a token it has read from disk. Use a private `pubsub_dir` if that matters.

No network, no telemetry, no credentials: the mod never reads `auth.json`, opens no socket, and makes no
outbound connection.

## Internals

The mod ships as one file, but it is not one flat script: each seam is a small section behind a narrow
interface, and the smoke test drives every one of them.

| Seam | Interface | Why it is a module |
| --- | --- | --- |
| Runtime | `Runtime`: `spawn`, `kill`, `alive`, `now`, `pid`, `execPath`, `env`, `fs` | The OS is one adapter, the test is the other. No other code names `node:fs`, `node:child_process` or the clock. |
| States | `isState`, `normalizeState`, `canTransition`, `isTerminal` | The six states and their edges in one table. An invalid edge is rejected; an unknown state is loud. |
| Envelope | `signalOf(record, result)`, `patchOf(signal)` | One contract for "this job reached this state", in both directions. |
| Spool | `next() -> Claim`, `settle(claim, 'accepted' \| 'held' \| 'quarantined')` | One claim policy for the drain, `onStop` and the pending count. A signal is never deleted because it belongs to someone else. |
| Launch | `fire(intent)`, `failJob(job, reason)` | One failure contract: one record, one signal, one delivery. The next guard reuses it. |
| Owner | `id()`, verdict `mine \| foreign \| orphan \| pending` | One identity rule. `pending` is a state, and there is no invented `process-<pid>` identity. |
| Ledger | `has(id)`, `accept(id)`, `retain(n)` | The replay guard and its retention, in memory and on disk together. |

Follow-up: the detached worker is still an inline string (`WORKER_SOURCE`) because the mod ships as one
file with nothing to install. Extracting it into a real module would let the worker share the envelope and
state code and would make the `waiting-input` heuristic a unit test, but it needs a different ship model
(two files, or a build step), so it is not part of this pass.

## Tests

```bash
node test/smoke.mjs   # prints PASS
```

The smoke test imports `pubsub.ts` directly with Node's native TypeScript type stripping and drives it with
a fake `ModApi` and a fake `Runtime` — no dependencies, no build, no network, no process. It covers the
public surface (tools, command, flags, renderer, pane, hooks, timers) and each seam on its own: the runtime
seam is really used (one spawn, both kills through the injected `kill`, the injected clock on the record),
the state table's edges, the envelope's record/signal agreement, a foreign signal surviving a failed
restore, exactly one signal from each spawn-failure mode, the owner token being retried rather than
replaced by `process-<pid>`, and the ledger's retention and restart behaviour. CI runs it on Node 24.

## License

MIT © 2026 Prem Modha
