# Work order: fix the review findings on PR#2

Status: open. Branch: `better-sub-agents`. Tip reviewed: `36d034f`.
Every finding below is reproduced by a script in this file. No guessing.

Read `SPEC.md` for the behavior contract, `AGENTS.md` for the build rules, and
`docs/audit-2026-10-20.md` for the house style of a finding (what it is /
problem / fix / which commit).

Ground rules for whoever picks this up:

- Edit `src/`, never the root `better-sub-agents.ts`. Run `npm run build`.
- `npm run gate` must pass before you commit. It runs the build, the stale-bundle
  check, the typecheck, the import check, the bundle contract and the 137 smoke
  checks.
- One finding per commit, so a bisect can find the culprit.
- Every fix needs a test that is **red before it is green**. The smoke suite is
  `test/smoke.mjs`; the real-host suite is `test/e2e.mjs` (needs credentials and
  network, so it may not be runnable for you — say so in the commit).
- Update `docs/audit-2026-10-20.md` with the finding, the fix, and the commit
  hash, in the format the file already uses.

---

## 1. MUST FIX: a resumed isolated agent loses its worktree, and the resume fails

Files: `src/plan.ts:526-581` (`planFromRecord`), `src/spawn.ts:381-384` and
`src/spawn.ts:437-500` (`fire`).

### What this is

`planFromRecord` rebuilds a plan from a finished job's record when the user
resumes it or messages it. It restores the model, the effort, the tool list, the
deny list, the MCP servers and the compact window. It does **not** restore
`isolation`, `worktree` or `mainCheckouts`.

`fire()` only creates a checkout when `intent.isolation === 'worktree'`, and
only fills `spec.worktree` / `spec.mainCheckouts` when that creation succeeded.
So a resumed isolated agent gets no checkout and no isolation guard.

### The problem

The resumed job's working directory is the **old** `record.cwd`, which is the
checkout that was already deleted when the first result was delivered. A worker
started in a directory that does not exist gets `ENOENT` from `spawn()`, comes
back with no pid, and `startWorker` fails the job with "the worker process did
not start".

So an isolated agent is resumable only while its checkout happens to be dirty.
`SPEC.md:326` says "The parent MUST be able to resume it with full context."

Three separate breaks, all from the same missing field:

1. **The resume fails** when the checkout was clean and got removed.
2. **The record loses the checkout** — `recordFromPlan` writes `isolation` from
   the plan, so the new record has `worktree: undefined`. The original record
   has `worktreeKept: true`, and `discardCleanWorktree`
   (`src/worktree-lifecycle.ts:163-169`) returns early on that flag, so the
   checkout is never swept and no signal ever reports it as kept. It leaks.
3. **The isolation guard is gone.** The resumed worker's spec has no `worktree`
   and no `mainCheckouts`, so `isolationRoots` (`src/worktree.ts:193-207`) finds
   no worktree and registers no guard. The subagent can reach the main checkout.

`subagent_send` has the same defect: `sendToRecord` (`src/tools.ts:446`) calls
the same `planFromRecord` with mode `'message'`.

### Reproduce it

Save this as `repro-1.mjs` in the repo root and run `node repro-1.mjs`:

```js
import {mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {execFileSync} from 'node:child_process';
import mod from './better-sub-agents.ts';
import {makeApi, makeRuntime, makeSession, toolOf} from './test/harness.mjs';

const repo = mkdtempSync(join(tmpdir(), 'repro-repo-'));
const g = (...a) => execFileSync('git', a, {cwd: repo, encoding: 'utf8', stdio: ['ignore','pipe','pipe']});
g('init', '-q', '-b', 'main');
writeFileSync(join(repo, 'README.md'), 'x\n');
g('add', '.');
g('-c', 'user.email=a@b.c', '-c', 'user.name=t', '-c', 'commit.gpgsign=false', 'commit', '-q', '-m', 'init');
g('update-ref', 'refs/remotes/origin/main', 'HEAD');
mkdirSync(join(repo, '.commandcode', 'agents'), {recursive: true});
writeFileSync(join(repo, '.commandcode', 'agents', 'iso.md'),
  '---\nname: iso\ndescription: isolated worker\nisolation: worktree\n---\n\nYou are isolated.\n');

const spool = mkdtempSync(join(tmpdir(), 'repro-spool-'));
const run = (c, a, o) => {
  try { return {ok: true, code: 0, stdout: execFileSync(c, a, {cwd: o?.cwd ?? repo, encoding: 'utf8', stdio: ['ignore','pipe','pipe']}), stderr: ''}; }
  catch (e) { return {ok: false, code: e.status ?? 1, stdout: String(e.stdout ?? ''), stderr: String(e.stderr ?? '')}; }
};
const runtime = makeRuntime({cwd: repo, now: () => 1000, run});
const api = makeApi({spoolDir: spool, session: makeSession(), runtime: runtime.runtime});
mod(api.api, runtime.runtime);

const res = await toolOf(api, 'subagent_spawn').run({input: {description: 'iso job', prompt: 'work', subagent_type: 'iso'}});
const id = res.content[0].text.match(/spawned (\S+)/)[1];
const rec = JSON.parse(readFileSync(join(spool, 'jobs', `${id}.json`), 'utf8'));

// The job finishes and its result is delivered: the clean checkout is removed.
const done = {...rec, state: 'done', exitCode: 0, finishedAt: 1000, durationMs: 5,
  acpSessionId: 'acp-1', live: false, pid: undefined, workerPid: undefined};
writeFileSync(join(spool, 'jobs', `${id}.json`), JSON.stringify(done, null, 2));
writeFileSync(join(spool, 'inbox', `1-${id}.json`), JSON.stringify({
  id, owner: rec.owner, description: 'iso job', kind: 'agent', state: 'done', exitCode: 0,
  startedAt: 1000, finishedAt: 1000, durationMs: 5, title: 'iso job done', body: 'all done',
}));
api.tickClock();
console.log('worktree exists after delivery =', existsSync(rec.worktree));   // false

await toolOf(api, 'subagent_resume').run({input: {id, prompt: 'continue'}});
const spec = JSON.parse(runtime.spawnCalls.at(-1).options.env.SUBAGENT_SPEC);
console.log('resumed cwd exists =', existsSync(spec.cwd));                    // false  <- the bug
console.log('resumed spec.worktree =', spec.worktree);                       // undefined <- the bug
console.log('resumed spec.mainCheckouts =', JSON.stringify(spec.mainCheckouts)); // undefined <- the bug
```

### The fix

In `planFromRecord`, carry the isolation the run resolved into the plan:
`record.isolation`, `record.worktree`, `record.worktreeRepo` and
`record.mainCheckouts`.

In `fire()`, when the plan names a checkout that still exists, reuse it and set
`spec.worktree` and `spec.mainCheckouts` from the record instead of creating a
new one. When the checkout is gone, create a fresh one and point the new record
at it, so the record and the spec agree.

`recordFromPlan` must then write the new `worktree`, `worktreeRepo`,
`mainCheckouts` and `worktreeHead` onto the record, the way `fire()` already
does for a first run (`src/spawn.ts:393-399`).

Keep the existing `worktreeFinalized` / `worktreeKept` semantics: a checkout
that is kept must still be reported as kept, and one that is removed must still
be removed through `git worktree remove`.

Do not special-case `mode`. Resume and message take the same path and must both
work.

### The test

Add a smoke check next to the other worktree checks (around `test/smoke.mjs`
line 3985, the "fire() creates the worktree" check):

- Spawn a definition with `isolation: worktree`.
- Mark the job done, deliver its signal so the checkout is finalized.
- Resume it.
- Assert the resumed spec carries `worktree` and `mainCheckouts`.
- Assert the resumed spec's `cwd` exists on disk.
- Assert the resumed record's `worktree` is set.

Add a second check for `subagent_send` on a finished isolated agent, because it
shares `planFromRecord`.

The e2e suite has the same gap: it resumes a plain `general-purpose` agent
(`test/e2e.mjs`), never an isolated one. Add an isolated resume there if you
can run it.

---

## 2. MUST FIX: `subagent_cancel` does not stop a command's children

File: `src/index.ts:3452-3457` (`cancelJob`).

### What this is

The worker puts every command in its own process group
(`spawn(spec.bin, spec.args, {detached: true})` in `WORKER_SOURCE`). Its
timeout path signals the **group** (`process.kill(-child.pid, signal)`) and
waits for the group to actually be gone before it writes the result.

The cancel path in the parent only does:

```ts
killPid(job.pid, 'SIGTERM');
killPid(job.workerPid, 'SIGTERM');
```

`job.pid` is the shell's pid, not the group's, and killing the worker first
removes the worker's own cleanup.

### The problem

A command that backgrounds itself survives the cancel. The user pressed cancel,
got "canceled", and the work kept running with nobody watching it.

### Reproduce it

Save as `repro-2.mjs` and run `node repro-2.mjs`:

```js
import {mkdtempSync, existsSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {spawn} from 'node:child_process';
import mod from './better-sub-agents.ts';
import {makeApi, makeSession, toolOf, realFs} from './test/harness.mjs';

async function scenario(label, useTimeout) {
  const spool = mkdtempSync(join(tmpdir(), 'repro-'));
  const marker = join(spool, 'late.txt');
  const runtime = {
    ...realFs, home: spool, cwd: spool, env: process.env, execPath: process.execPath,
    pid: process.pid, interactive: false, now: () => Date.now(),
    spawn: (c, a, o) => spawn(c, a, o),
    kill: (pid, sig) => process.kill(pid, sig),
    alive: (pid) => { try { process.kill(pid, 0); return true; } catch (e) { return e.code === 'EPERM'; } },
    sleep: (ms) => new Promise(r => setTimeout(r, ms)),
    later: (ms, fn) => { const t = setTimeout(fn, ms); t.unref(); },
    run: () => ({ok: false, code: 1, stdout: '', stderr: 'no git'}),
  };
  const api = makeApi({spoolDir: spool, session: makeSession(), runtime});
  mod(api.api, runtime);

  const input = {description: 'cmd', command: `(sleep 5; echo late > ${marker}) & echo started; sleep 30`};
  if (useTimeout) input.timeout_ms = 1500;
  const res = await toolOf(api, 'subagent_spawn').run({input});
  const id = res.content[0].text.match(/spawned (\S+)/)[1];
  await new Promise(r => setTimeout(r, 500));
  if (!useTimeout) await toolOf(api, 'subagent_cancel').run({input: {id}});
  await new Promise(r => setTimeout(r, 7000));
  console.log(`${label}: grandchild survived = ${existsSync(marker)}`);
}

await scenario('TIMEOUT (timeout_ms=1500)', true);   // false
await scenario('CANCEL                    ', false);  // true  <- the bug
```

### The fix

The worker owns the process group, so the worker should own the teardown.

Give `WORKER_SOURCE` a `SIGTERM` handler that runs the group teardown it already
has (`drainGroup` / `shutdown`: SIGTERM the group, escalate to SIGKILL, then
write the canceled result) and then exits. The parent signals the worker and
lets the worker tear down its own group.

The parent's `killPid(job.pid, ...)` can stay as a backstop for a worker that is
already dead.

The cancel must still emit exactly **one** canceled signal
(`SPEC.md:271`, "Cancel MUST be terminal"), and the escalated SIGKILL must still
go through the runtime seam — `test/smoke.mjs:6028` asserts the escalation
kills both pids through `runtime.later`.

### The test

Add a smoke check that asserts the worker stops the group on cancel. The
e2e suite is the right place for the real-process version: spawn a command that
backgrounds a sleep, cancel it, and assert the grandchild is gone. There is
already a timeout equivalent in `test/e2e.mjs` (the "timeout job killed and
reported" check) — the cancel case belongs next to it.

---

## 3. MUST FIX: the isolation guard's `$VAR` hole is silent

File: `src/worktree.ts:263`.

### What this is

`commandEscapesWorktree` parses the `cd`, redirect and `git -C` targets out of a
command and blocks any that resolve outside the worktree. One line skips any
target that starts with `$`:

```ts
if (target.startsWith('$')) continue;
```

### The problem

Every shell-expanded path to the main checkout walks straight through:

```
BLOCK  cd /home/u/proj
ALLOW  cd $HOME/proj
ALLOW  git -C $HOME/proj status
ALLOW  echo x > $HOME/proj/f
```

The literal-path rules are thorough (`GIT_DIR=`, `--prefix`, `-C`,
`--work-tree`, redirects, `cd`), and this one character undoes all of them.
`SPEC.md:302` requires that Bash commands "MUST also be checked for redirect
vectors and command-shape rules that escape the worktree" — a skipped target is
not a check.

The current behaviour is the worst of both: it looks like a check and is a
hole, and it says nothing to the model about why.

### Reproduce it

```js
import {guardToolCall} from './better-sub-agents.ts';
const roots = {worktree: '/tmp/wt', mainCheckouts: ['/home/u/proj']};
for (const c of ['cd /home/u/proj', 'cd $HOME/proj', 'git -C $HOME/proj status', 'echo x > $HOME/proj/f']) {
  const v = guardToolCall('Bash', {command: c}, roots);
  console.log(v ? 'BLOCK' : 'ALLOW', c);
}
```

### The fix

Do not try to parse the shell. Two acceptable shapes:

- Block a `cd` / `git -C` / redirect target that starts with `$` or `${` when
  the command also names a forbidden root. The command is refused with a
  reason that names the root.
- Or refuse to expand: treat an unexpandable target as a possible escape and
  block the call, with a reason that names the root.

Either way the block reason must say which root was reached. The `Read` tool
and other non-shell tools are unaffected (`guardToolCall` returns `undefined`
for them already).

### The test

Add to the guard check in `test/smoke.mjs` (around line 3868, the "command
guard" check): the four commands above, asserting the literal ones are blocked
and the `$HOME` ones are blocked too, with a reason that names the checkout.

Note: `SPEC.md:302` also says "PowerShell gets the working-directory check", so
the `$` rule is a Bash rule. Keep it that way.

---

## 4. SHOULD FIX: the pending-signal count renames every inbox file, every call

File: `src/delivery.ts:439-458` (`countPending`).

### What this is

`countPending` walks the inbox through the same claim/settle path the drain
uses, so a foreign signal gets renamed into `claimed/` and renamed back. It is
called from the status line (`src/index.ts:3323`), from `listText`
(`src/index.ts:3371`) and from the jobs-pane render (`src/index.ts:3536`).

### The problem

The budget bounds the *work per pass*, but the work is repeated on every call,
and a pane render is not a timer — it happens on every UI redraw. Over a
40-file foreign backlog:

```
ONE jobs-pane render: renames = 80
10 pane renders:      renames = 800
```

Each rename is a claim plus a put-back, so 40 files cost 80 renames per
render. The audit's earlier fix (item 4, commit `21eaa65`) bounded the per-pass
cost; this is the per-*call* cost.

### Reproduce it

```js
import {mkdtempSync, writeFileSync, readdirSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import mod from './better-sub-agents.ts';
import {makeApi, makeRuntime, makeSession, ownerEntries, realFs} from './test/harness.mjs';

const spool = mkdtempSync(join(tmpdir(), 'repro-'));
const sessionA = makeSession();
const apiA = makeApi({spoolDir: spool, session: sessionA, runtime: makeRuntime().runtime});
mod(apiA.api, apiA.runtime);
const ownerA = ownerEntries(sessionA)[0];
for (let i = 0; i < 40; i++) {
  writeFileSync(join(spool, 'inbox', `1000-a-${i}.json`), JSON.stringify({
    id: `a-${i}`, owner: ownerA, description: `a${i}`, kind: 'command', state: 'done', title: `a${i}`, body: 'x',
  }));
}
let ops = [];
const countingFs = {
  ...realFs,
  rename(from, to) { ops.push('rename'); realFs.rename(from, to); },
  readdir(dir) { ops.push('readdir'); return realFs.readdir(dir); },
};
const apiB = makeApi({spoolDir: spool, session: makeSession(), runtime: makeRuntime({fs: countingFs}).runtime});
mod(apiB.api, apiB.runtime);
const pane = apiB.paneDefs.find(p => p.id === 'jobs');
ops = []; pane.render();
console.log('renames for ONE render =', ops.filter(o => o === 'rename').length);   // 80
```

### The fix

The count does not need to claim anything. It delivers nothing, so there is no
race to win. Read the inbox names, read each file, and count the ones this
session owns. The claim exists to stop two drains delivering one signal; a
counter has no reason to claim.

That is a few lines in `countPending`, and it removes the churn entirely. Keep
the per-pass bound so a huge inbox still costs bounded work.

Watch out: `next()` and `settle()` are shared with the drain, and the drain's
exactly-once behavior depends on them. Do not change `next()` or `settle()`.
Give `countPending` its own read-only path.

### The test

Extend the existing "foreign backlog" check (`test/smoke.mjs` around line 407)
to assert that a **count** does not rename anything, only a drain does.

---

## 5. SHOULD FIX: the isolated-agent resume path has no test

See finding 1. The tests for worktree isolation (checks 60-64) and for resume
(check 82) are separate, and the bug in finding 1 lives exactly in the gap
between them.

Add the two checks described in finding 1. This is the test half of that fix —
if you already added them there, this item is done.

---

## 6. SHOULD FIX: the evidence in the tree is stale

Files: `SPEC.md:602`, `README.md:549`, `CHECKLIST.md:235`.

### What this is

Three files say the suite is "102 checks green". The suite is 137. The PR body
says "112 checks on `0da9544`"; the tip is `36d034f`.

`CHECKLIST.md` also still has 19 unchecked boxes, and most of them are already
done and tested: lines 105-108 (frontmatter parsing, the built-ins, the
read-only/one-shot marks, the invalid-file skip, the duplicate resolution),
lines 114-118 (the concurrency pool, the env var, the exact error message, the
depth limit, the disable env), line 147 (hot reload recovery) and line 122.

### The problem

These counts are the numbers a reviewer uses to judge coverage, and the audit's
own "Evidence" lines cite them. A stale count reads as a coverage claim that is
not true. An unchecked box on finished work reads as unfinished work.

### The fix

- Update the three check counts to the real number. If your fixes add checks,
  use the number after your commits.
- Tick the CHECKLIST boxes that are done and tested, and leave the genuinely
  human-gated ones open with their existing note: the repository rename (lines
  18 and 159) and the `v2.0.0` tag (lines 160 and 262).

---

## 7. NICE TO HAVE: `mcpServerNames` is exported but only the test uses it

File: `src/index.ts:2221`.

`mcpServerNames` is a one-line wrapper over `mcpServerConfigs` that returns
`Object.keys(...)`. No production code calls it. Only
`test/smoke.mjs:2965` does.

Delete it and let the test call `Object.keys(mcpServerConfigs(...))` directly.
Also check `src/index.ts`'s export list for anything else with the same shape.

---

## Order of work

1. Finding 1 (resume + isolation). It breaks a `SPEC.md` MUST and silently
   disables isolation. Fix and test in one commit.
2. Finding 2 (cancel + process group). Fix and test in one commit.
3. Finding 3 (the `$` hole). Fix and test in one commit.
4. Finding 4 (the pending count). Fix and test in one commit.
5. Finding 5 (the missing test) — done with finding 1.
6. Finding 6 (the stale evidence) — do it last, after the check counts settle.
7. Finding 7 (delete `mcpServerNames`) — any time.

## Not in scope

- The fork path. It needs a live `cmd` session and was not exercised.
- The context objects are too wide (`ToolsContext` has 30 fields, the repo's
  own `CODING_STANDARDS.md` rule 6 says about ten). That is a refactor, not a
  fix, and it is not urgent.
- The comment-to-code ratio. The comments are good; they are just verbose.
  Moving prose into `docs/` is a separate piece of work.
