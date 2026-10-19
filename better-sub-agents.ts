// Mod: better-sub-agents — async subagents that signal back on their own.
//
// The problem this solves: the `agent` tool is a fire-and-wait. A background worker
// finishes and the parent has to ask for the result, poll for it, or hold a blocking
// wait open. Every one of those costs a turn while nothing is happening.
//
// The seam is `cmd.sendUserMessage(...)` — it starts the turn when the session is
// idle and queues behind the active turn otherwise. Neither blocks the model, and
// neither costs a poll: the wake-up is the message arriving.
//
// Transport is a spool directory, not a socket. A worker writes one JSON file and
// exits; this mod drains the directory on a timer and delivers what it finds. No
// ports, no auth, no reconnect logic, nothing to leak if the harness dies — a signal
// that lands while no session is running is still there next time.
//
// OWNERSHIP. The spool is shared by every session on the machine, so every job and
// signal carries an owner token. The token is persisted in the session transcript
// (`appendCustomEntry({customType: 'subagent/owner'})`) and read back with
// `getCustomEntries`, so it survives resume and hot reload. A drain delivers only
// signals whose owner matches; another session's signal is left in the inbox for its
// owner. Untagged signals from older versions are quarantined to `orphaned/` — never
// delivered to whoever happens to be running.
//
// Polling here is free. The distinction matters: a timer inside the mod process spends
// no turns, whereas `cron_create` costs a full model turn per tick because a tick is a
// prompt handed to the agent.
//
// Layout under SUBAGENT_DIR (default ~/.commandcode/subagents):
//   inbox/       workers drop `<timestamp>-<id>.json` here
//   claimed/     rename-target during delivery, so two drains cannot deliver one signal twice
//   orphaned/    untagged legacy signals, quarantined instead of leaked
//   logs/        one log per fired job
//   jobs/        one JSON record per fired job, for subagent_list and the jobs pane
//   delivered.jsonl   durable delivery ledger, so a restart cannot replay a signal
//
// Lifecycle (jobs/<id>.json .state):
//   queued → running → waiting-input | done | failed | canceled
//
// A headless worker cannot stay alive waiting for input, so `waiting-input` means
// "the worker exited with a question"; answering it means firing a follow-up job.
//
// Inside, the mod is a handful of small sections rather than one flat script: the
// runtime seam (every OS call), the state table, the envelope, the spool's
// claim/settle, launch/failure, owner identity and the delivery ledger. See
// "Internals" in the README.

import type {ModApi} from '@commandcode/harness';
import {spawn, spawnSync} from 'node:child_process';
import {randomUUID} from 'node:crypto';
import {
	appendFileSync,
	mkdirSync,
	readdirSync,
	readFileSync,
	renameSync,
	rmSync,
	statSync,
	writeFileSync,
} from 'node:fs';
import {homedir} from 'node:os';
import {dirname, isAbsolute, join, resolve, sep} from 'node:path';
import {fileURLToPath} from 'node:url';

// Bounds. Every loop and buffer below is capped on purpose.
const DRAIN_INTERVAL_MS = 400;
const REFRESH_INTERVAL_MS = 1000;
const FILES_PER_DRAIN_MAX = 50;
const LOG_TAIL_BYTES_MAX = 4000;
// A claimed file is normally removed in the same tick. One abandoned by a crashed
// drain (or left behind when the restore rename failed) is swept after this long.
const CLAIMED_TTL_MS = 60 * 60 * 1000;
const ORPHANED_TTL_MS = 7 * 24 * 60 * 60 * 1000;
// A finished job keeps its record and its log for this long, then is swept. Running
// jobs are never swept, however old their record is.
const RETENTION_DAYS_DEFAULT = 30;
const RETENTION_SWEEP_INTERVAL_MS = 60 * 60 * 1000;
// A queued/running record with no live pid is only declared lost after this grace,
// so a worker that has been spawned but has not written its pid yet is not failed.
const LOST_GRACE_MS = 30 * 1000;
// Reconcile and list only the newest records; the spool is long-lived.
const RECONCILE_SCAN_MAX = 200;
const LIST_SCAN_MAX = 50;
const DELIVERED_LEDGER_MAX = 5000;
const DELIVERED_LEDGER_KEEP = 2000;
const LEDGER_COMPACT_BYTES = 512 * 1024;
const PANE_ROWS_MAX = 12;
const PENDING_SCAN_MAX = 500;
// subagent_output's optional blocking read. Bounded, because a tool that waits is a
// turn held open - the whole point of the mod is that you never have to.
const WAIT_DEFAULT_MS = 10 * 1000;
const WAIT_MAX_MS = 60 * 1000;
const WAIT_POLL_MS = 200;
const OUTPUT_MAX_CHARS_DEFAULT = 4000;
const OUTPUT_MAX_CHARS_CAP = 100 * 1000;
// Live messaging. A running agent polls its control/send.jsonl; an agent with no
// message waits this long for a turn before it exits and a send resumes it.
const LIVE_POLL_MS = 100;
const LIVE_IDLE_MS_DEFAULT = 30 * 1000;
const ROSTER_MAIN = 'main';

// Task rows. A success disappears immediately; a failure or stop stays visible for
// this window so the user can read it, and `x` clears it early.
const TASK_ROW_GRACE_MS = 30 * 1000;
// Fork mode. On by default for a human, off for a non-interactive/SDK run. The
// flag wins over the environment, which wins over the default.
const FORK_MODE_FLAG = 'fork_mode';
const FORK_MODE_ENV = 'BETTER_SUBAGENTS_FORK_MODE';
const FORK_MODE_ENV_ALIAS = 'CLAUDE_CODE_FORK_MODE';
const SDK_ENV = 'CLAUDE_AGENT_SDK';
const SDK_ENV_ALIAS = 'BETTER_SUBAGENTS_SDK';
const FORK_AGENT_TYPE = 'fork';
// A teammate keeps the shared task list and the cron scheduler even when its
// definition's tools list or disallowedTools would remove them.
export const TEAM_TOOL_NAMES: string[] = [
	'TaskCreate',
	'TaskGet',
	'TaskList',
	'TaskUpdate',
	'CronCreate',
	'CronDelete',
	'CronList',
];

// Agent definitions. A definition is a Markdown file with YAML frontmatter; its
// body is the worker's system prompt. Discovery follows the scope table: managed,
// CLI, project (closest directory upward, then --add-dir), user, plugin, built-in.
const AGENT_NAME_MAX = 256;
const AGENT_SCAN_MAX = 1000;
const AGENT_SCAN_DEPTH_MAX = 8;
const AGENT_LOG_FILE = 'agents.log';
const WORKER_PROMPT_ENV = 'BETTER_SUBAGENTS_AGENT_PROMPT';
// The host reserves these names for its own agents. We never register *our*
// built-ins under them; `general` resolves to our `general-purpose` instead.
const HOST_RESERVED_AGENT_NAMES: Record<string, true> = {
	explore: true,
	plan: true,
	review: true,
	general: true,
};
// The mod file the worker passes back to `cmd --mod <file> acp`. ACP is the
// accepted transport (docs/adr/0001-worker-and-control.md). The host loads a mod
// through jiti under a virtual filename (`<real>.load-<n>.<ext>`), so the shim is
// stripped to recover a path that exists on disk.
const MOD_PATH = fileURLToPath(import.meta.url).replace(/\.load-\d+(?=\.[^./]+$)/, '');

const OWNER_TYPE = 'subagent/owner';
// The 0.1.0 transcript entry. Read once so an installed session keeps its identity
// across the rename; never written again.
const OWNER_TYPE_LEGACY = 'pubsub/owner';
const JOB_ENTRY_TYPE = 'subagent/job';
const WORKER_ENV = 'BETTER_SUBAGENTS_WORKER';
const WORKER_SPEC_ENV = 'SUBAGENT_SPEC';
const RETENTION_ENV = 'BETTER_SUBAGENTS_RETENTION_DAYS';
// `cleanupPeriodDays` is the Claude Code name for the same window. The host has no
// such setting, so the alias arrives through the environment, and a project or user
// `.commandcode/settings.json` is read as the documented settings surface.
const RETENTION_ENV_ALIAS = 'CLAUDE_CODE_CLEANUP_PERIOD_DAYS';
const CLEANUP_PERIOD_SETTING = 'cleanupPeriodDays';
// Limits. The concurrency limit is per session; the depth limit caps nesting.
const MAX_CONCURRENT_DEFAULT = 20;
const MAX_CONCURRENT_ENV = 'BETTER_SUBAGENTS_MAX_CONCURRENT';
const MAX_CONCURRENT_ENV_ALIAS = 'CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS';
const MAX_DEPTH_DEFAULT = 3;
const MAX_DEPTH_ENV = 'BETTER_SUBAGENTS_MAX_DEPTH';
const MAX_DEPTH_ENV_ALIAS = 'CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH';
// The pool scans recent records; the spool is long-lived and a session's active
// jobs are always among the newest.
const ACTIVE_JOB_SCAN_MAX = 500;
const SUBAGENT_DIR_DEFAULT = join(homedir(), '.commandcode', 'subagents');
const PUBSUB_DIR_DEFAULT = join(homedir(), '.commandcode', 'pubsub');
// Definition scopes. The host has no `--agents`; the CLI scope arrives as
// `--mod-option agents=<json|path>`. The managed settings directory is the host's
// organization-wide config root; `agents/` under it beats every other scope.
const MANAGED_SETTINGS_DEFAULT = join(homedir(), '.commandcode', 'managed');
const CLI_AGENTS_OPTION = 'agents';
const ADD_DIRS_OPTION = 'add_dir';
const PLUGIN_DIRS_OPTION = 'plugin_dir';

// Execution policy. A run is either foreground (the tool call waits and returns
// the result) or background (the tool returns and the result arrives as a signal).
// The disable envs force the foreground, so they win over everything; a definition's
// `background: true` wins over the caller but never over the env.
const BACKGROUND_DISABLE_ENV = 'CLAUDE_CODE_DISABLE_BACKGROUND_TASKS';
const BACKGROUND_DISABLE_ENV_ALIAS = 'BETTER_SUBAGENTS_DISABLE_BACKGROUND';
// Foreground is bounded: a tool call must not hold the turn open forever. Past the
// bound the run is demoted to background, so its result still reaches the session.
const FOREGROUND_WAIT_MAX_MS = 30 * 60 * 1000;
const FOREGROUND_POLL_MS = 200;
// The worker settles an ACP turn cut off by a rate limit, an overload, or a server
// error with this exact lead. The parent uses the lead to tell a partial result
// apart from any other failure. Keep the text verbatim: it is the documented error.
export const AGENT_API_ERROR = 'Agent terminated early due to an API error';
// ---------------------------------------------------------------------------
// Output scanning
//
// A subagent's report is data, never instructions. Before the text becomes a
// signal it is scanned once: an imitation of the host's own output gets a
// backslash (so `<system-reminder>` reads as text), and a tag imitation or a
// permission-setting mention gets a marker the parent cannot miss. Nothing else
// is rewritten - a permission mention stays exactly as written.
//
// Self-contained on purpose: WORKER_SOURCE embeds this function's source, so the
// worker and the parent run one rule table (the same contract as signalEnvelope).
// ---------------------------------------------------------------------------

export interface ScanResult {
	readonly text: string;
	readonly matches: string[];
	readonly marked: boolean;
}

export function scanOutput(text: string): ScanResult {
	// Inline the marker and the rule names: this body is embedded in WORKER_SOURCE,
	// so it cannot read module constants.
	const marker = '[harness: subagent output matched instruction-shaped pattern(s): ';
	const settings = [
		['bypassPermissions', 'bypassPermissions permission setting'],
		['--dangerously-skip-permissions', '--dangerously-skip-permissions permission setting'],
	];
	const matches: string[] = [];
	let marked = false;
	let out = String(text ?? '');
	// A tag imitation is escaped, never removed: the text stays readable and can no
	// longer be mistaken for host output.
	out = out.replace(/<\/?system-reminder\b[^>]*>/gi, match => {
		if (!matches.includes('system-reminder tag')) matches.push('system-reminder tag');
		marked = true;
		return '\\' + match;
	});
	// A role label only counts at the start of a line, after indentation.
	out = out.replace(/^([ \t]*)(Human|Assistant):/gm, (_match, space: string, label: string) => {
		const name = label + ': role label';
		if (!matches.includes(name)) matches.push(name);
		return space + '\\' + label + ':';
	});
	// A permission-setting mention is reported, not rewritten: rewriting a flag
	// would hide what the subagent actually said.
	for (const [needle, name] of settings) {
		if (out.includes(needle) && !matches.includes(name)) {
			matches.push(name);
			marked = true;
		}
	}
	if (marked) out = marker + matches.join(', ') + ']\n' + out;
	return {text: out, matches, marked};
}

// The report the parent reads: the worker's own progress lines (`[tool] ...`) are
// not part of it. They stay in the log for the task view; a tool call is never a
// result the parent is asked to act on. Shell output is returned as written.
export function reportText(text: string, kind: string): string {
	const visible =
		kind === 'agent'
			? text.split('\n').filter(line => !/^\[tool[ \]]/.test(line)).join('\n')
			: text;
	return scanOutput(visible).text;
}

// The detached worker. Kept inline so the mod is one file with nothing to install.
// The spec arrives in the environment: no shell interpolation, so a prompt or command
// containing quotes, `$`, or a newline cannot break the wrapper.
export const WORKER_SOURCE = `
const {spawn} = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');
const spec = JSON.parse(process.env.SUBAGENT_SPEC);
const recordPath = path.join(spec.jobsDir, spec.id + '.json');

const readRecord = () => {
  try { return JSON.parse(fs.readFileSync(recordPath, 'utf8')); } catch (e) { return {}; }
};

// The persisted conversation for this agent. Append-only JSONL: the ACP update
// stream in order, plus a result line and any compact boundary. It lives outside
// the main session, so the main transcript compacting can never touch it.
const transcriptPath = typeof spec.transcriptPath === 'string' ? spec.transcriptPath : '';
let transcriptFd;
const transcriptWrite = entry => {
  if (transcriptPath.length === 0) return;
  try {
    if (transcriptFd === undefined) {
      fs.mkdirSync(path.dirname(transcriptPath), {recursive: true});
      transcriptFd = fs.openSync(transcriptPath, 'a');
    }
    fs.writeSync(transcriptFd, JSON.stringify(entry) + '\\n');
  } catch (e) {}
};

// Canceled is terminal. Re-read inside this call (Node is single-threaded, so the
// read+write pair cannot interleave with the cancel tool) and force the state back
// if a cancellation landed while the child was exiting.
const writeRecord = patch => {
  try {
    const record = readRecord();
    const next = Object.assign({}, record, patch);
    if (record.state === 'canceled') next.state = 'canceled';
    next.updatedAt = Date.now();
    const tmp = recordPath + '.worker-' + process.pid;
    fs.writeFileSync(tmp, JSON.stringify(next, null, 2));
    fs.renameSync(tmp, recordPath);
  } catch (e) {}
};

const emit = message => {
  try {
    const tmp = path.join(spec.inboxDir, '.tmp-' + spec.id + '-' + process.pid + '-' + Date.now());
    fs.writeFileSync(tmp, JSON.stringify(message, null, 2));
    fs.renameSync(tmp, path.join(spec.inboxDir, Date.now() + '-' + spec.id + '-' + process.pid + '.json'));
  } catch (e) {}
};

const readTail = () => {
  try { return fs.readFileSync(spec.logPath, 'utf8').slice(-spec.tailMax); } catch (e) { return ''; }
};

// The envelope is the parent's signalEnvelope, injected here as source. Both
// sides therefore construct a signal from the same field list.
const signalEnvelope = ${signalEnvelope.toString()};

// The scanner is the parent's scanOutput, injected the same way: one rule table.
const scanOutput = ${scanOutput.toString()};

// The report tail drops the worker's own tool-call progress. Those lines stay in
// the log for the task view; the parent is never asked to act on a tool call.
// Shell output is passed through as written.
const readReportTail = () => {
  try {
    const tail = readTail();
    if (spec.kind !== 'agent') return tail;
    return tail.split('\\n').filter(line => !/^\\[tool[ \\]]/.test(line)).join('\\n');
  } catch (e) { return ''; }
};

let settled = false;

// One constructor for "this job reached this state": the record patch and the
// signal are derived from the same facts, so they cannot disagree. The optional
// extra argument carries the waiting-input payload (question, permissionRequest).
const settle = (state, detail, code, tail, extra) => {
  if (settled) return;
  settled = true;
  const finishedAt = Date.now();
  const durationMs = Math.max(0, finishedAt - spec.startedAt);
  const failed = state === 'failed';
  // SubagentHandback already sent the report to the parent. Record the terminal
  // state, but do not emit a second signal for the same run.
  if (readRecord().handbackAt) {
    writeRecord({
      state: state,
      exitCode: typeof code === 'number' ? code : undefined,
      finishedAt: finishedAt,
      durationMs: durationMs,
      failureReason: failed ? detail : undefined,
      awaitingInput: state === 'waiting-input' ? true : undefined
    });
    return;
  }
  const payload = extra || {};
  // Scan is the last step before the text can reach the parent; the record and
  // the signal carry the same facts.
  const scan = scanOutput(detail + (tail ? '\\n\\n' + tail : ''));
  writeRecord({
    state: state,
    exitCode: typeof code === 'number' ? code : undefined,
    finishedAt: finishedAt,
    durationMs: durationMs,
    failureReason: failed ? detail : undefined,
    awaitingInput: state === 'waiting-input' ? true : undefined,
    question: typeof payload.question === 'string' ? payload.question : undefined,
    permissionRequest: payload.permissionRequest,
    partial: payload.partial === true ? true : undefined,
    compactBoundary: payload.compactBoundary,
    scanned: true,
    scanMatches: scan.matches
  });
  transcriptWrite({
    at: finishedAt,
    type: 'result',
    state: state,
    exitCode: typeof code === 'number' ? code : null,
    partial: payload.partial === true,
    compactBoundary: payload.compactBoundary
  });
  emit(signalEnvelope({
    id: spec.id, label: spec.label, kind: spec.kind, owner: spec.owner,
    startedAt: spec.startedAt, logPath: spec.logPath, sessionId: spec.sessionId,
    ...(typeof spec.agentId === 'string' ? {agentId: spec.agentId} : {})
  }, Object.assign({
    state: state,
    exitCode: typeof code === 'number' ? code : null,
    finishedAt: finishedAt,
    durationMs: durationMs,
    body: scan.text,
    scanned: true,
    scanMatches: scan.matches
  }, payload)));
};

// A failed worker still returns its last output: partial work is never discarded.
const fail = (reason, code) => settle('failed', reason, code, readReportTail());
// A cut-off is not a plain failure: the last output is part of the result, so the
// foreground can return it as partial work and the background can deliver it.
const failApi = detail => settle('failed', ${JSON.stringify(AGENT_API_ERROR)} + (detail ? ': ' + detail : ''), null, readReportTail());

let out;
try {
  out = fs.openSync(spec.logPath, 'w');
} catch (e) {
  fail('cannot open log: ' + String(e && e.message || e), null);
  process.exit(0);
}
const logWrite = text => { try { fs.writeSync(out, text); } catch (e) {} };

// The fork path: the host CLI runs one print turn. stdout is the NDJSON event
// stream, so it is parsed instead of logged; the result line names the fork
// session, which is what a follow-up resumes.
const isFork = spec.kind === 'fork';
let forkStdout = '';
let forkSessionId = typeof spec.forkSessionId === 'string' ? spec.forkSessionId : undefined;

const child = isFork
  ? spawn(spec.bin, spec.args || [], {cwd: spec.cwd, stdio: ['ignore', 'pipe', 'pipe']})
  : spec.kind === 'agent'
    ? spawn(spec.bin, spec.args || [], {cwd: spec.cwd, stdio: ['pipe', 'pipe', 'pipe']})
    : spawn(spec.bin, spec.args || [], {cwd: spec.cwd, stdio: ['ignore', out, out]});

if (isFork && child.stdout && child.stderr) {
  child.stdout.on('data', chunk => { forkStdout += chunk.toString(); });
  child.stderr.on('data', chunk => logWrite(chunk.toString()));
}

let timedOut = false;
let killTimer;
let hardTimer;

// A timeout is still one result: SIGTERM, SIGKILL a second later, and a failed
// signal that says why. Nothing is left running and nothing is left silent.
if (typeof spec.timeoutMs === 'number' && spec.timeoutMs > 0) {
  killTimer = setTimeout(() => {
    timedOut = true;
    try { child.kill('SIGTERM'); } catch (e) {}
    hardTimer = setTimeout(() => { try { child.kill('SIGKILL'); } catch (e) {} }, 1000);
    if (hardTimer.unref) hardTimer.unref();
  }, spec.timeoutMs);
  if (killTimer.unref) killTimer.unref();
}

const clearTimers = () => {
  clearTimeout(killTimer);
  clearTimeout(hardTimer);
};

// The ACP child is long-lived only for the turn. After the signal is written,
// close its stdin, then make sure it cannot outlive the worker.
const shutdown = () => {
  try { child.stdin.end(); } catch (e) {}
  try { child.kill('SIGTERM'); } catch (e) {}
  setTimeout(() => {
    try { child.kill('SIGKILL'); } catch (e) {}
    process.exit(0);
  }, 1000);
};

const finish = (code, spawnError, signal) => {
  if (settled) return;
  clearTimers();
  const record = readRecord();
  // The cancel tool has already written the record and emitted its own signal.
  if (record.state === 'canceled') {
    settled = true;
    shutdown();
    return;
  }
  if (spec.kind === 'fork') {
    finishFork(code, spawnError, signal);
    return;
  }

  const tail = readTail();
  const timeoutLabel = spec.timeoutMs >= 1000 ? Math.round(spec.timeoutMs / 1000) + 's' : spec.timeoutMs + 'ms';
  if (spec.kind === 'agent') {
    const detail = timedOut
      ? 'timed out after ' + timeoutLabel
      : spawnError !== undefined
        ? 'spawn error: ' + String(spawnError && spawnError.message || spawnError)
        : 'ACP exited before the prompt completed (exit ' + String(code) + ')';
    if (timedOut || spawnError !== undefined) fail(detail, code); else failApi(detail);
    shutdown();
    return;
  }

  const lastLine = tail.trimEnd().split('\\n').filter(line => line.trim()).pop() || '';
  const failed = timedOut || spawnError !== undefined || (code !== 0 && code !== null);
  const wantsInput = !failed && /\\?\\s*$/.test(lastLine);
  const detail = timedOut
    ? 'timed out after ' + timeoutLabel
    : spawnError !== undefined
      ? 'spawn error: ' + String(spawnError && spawnError.message || spawnError)
      : typeof code === 'number' && code !== 0
        ? 'exit ' + code
        : signal
          ? 'terminated by ' + signal
          : 'exit 0';
  settle(failed ? 'failed' : wantsInput ? 'waiting-input' : 'done', detail, code, readReportTail(), wantsInput ? {question: lastLine} : undefined);
};

// One NDJSON parser for the fork path. Event lines become tool progress in the
// transcript; the result line carries finalText, the stop reason, and the new
// session id. Anything else is ignored, never guessed at.
const parseForkLines = text => {
  let result;
  const toolLines = [];
  for (const line of String(text).split('\\n')) {
    if (!line.trim()) continue;
    let parsed;
    try { parsed = JSON.parse(line); } catch (e) { continue; }
    if (parsed && parsed.type === 'event' && parsed.event) {
      const event = parsed.event;
      const name = String(event.name || event.toolName || event.tool || 'call');
      if (event.type === 'tool_running') toolLines.push('[tool] ' + name);
      else if (event.type === 'tool_completed') toolLines.push('[tool completed] ' + name);
    } else if (parsed && parsed.type === 'result') {
      result = parsed;
    }
  }
  return {result: result, toolLines: toolLines};
};

const forkPollInterval = () => (typeof spec.controlPollMs === 'number' && spec.controlPollMs > 0 ? spec.controlPollMs : 100);

// The fork keeps the process alive after the first turn, exactly like a live ACP
// worker. A message cannot preempt the turn that is running - the host CLI has no
// mid-turn steering - so it becomes the next --resume <fork> -p turn.
const forkLiveLoop = async () => {
  const controlDir = typeof spec.controlDir === 'string' ? spec.controlDir : '';
  if (controlDir.length === 0) return;
  const sendPath = path.join(controlDir, 'send.jsonl');
  const cancelPath = path.join(controlDir, 'cancel');
  const statePath = path.join(controlDir, 'state.json');
  let offset = 0;
  let partial = '';
  let canceled = false;
  let turnChild;
  let messageCount = 0;
  const queue = [];
  const writeState = busy => {
    try {
      fs.mkdirSync(controlDir, {recursive: true});
      const tmp = statePath + '.tmp-' + process.pid;
      fs.writeFileSync(tmp, JSON.stringify({busy: !!busy, turns: messageCount, updatedAt: Date.now()}));
      fs.renameSync(tmp, statePath);
    } catch (e) {}
  };
  const readSend = () => {
    let size;
    try { size = fs.statSync(sendPath).size; } catch (e) { return; }
    if (size <= offset) return;
    let chunk = '';
    try {
      const fd = fs.openSync(sendPath, 'r');
      const length = size - offset;
      const buffer = Buffer.alloc(length);
      fs.readSync(fd, buffer, 0, length, offset);
      fs.closeSync(fd);
      offset = size;
      chunk = buffer.toString('utf8');
    } catch (e) { return; }
    const lines = (partial + chunk).split('\\n');
    partial = lines.pop() || '';
    for (const line of lines) {
      if (!line.trim()) continue;
      try {
        const entry = JSON.parse(line);
        if (entry && typeof entry.message === 'string' && entry.message.length > 0) queue.push(entry);
      } catch (e) {}
    }
  };
  const timer = setInterval(() => {
    readSend();
    if (!canceled) {
      try {
        if (fs.existsSync(cancelPath)) {
          canceled = true;
          logWrite('[live] cancel requested\\n');
          try { if (turnChild) turnChild.kill('SIGTERM'); } catch (e) {}
        }
      } catch (e) {}
    }
  }, forkPollInterval());
  if (timer.unref) timer.unref();
  writeState(false);
  const idleMs = typeof spec.idleMs === 'number' && spec.idleMs >= 0 ? spec.idleMs : 30000;
  while (!canceled) {
    if (queue.length === 0) {
      const deadline = Date.now() + idleMs;
      while (queue.length === 0 && !canceled && Date.now() < deadline) {
        const {promise, resolve} = Promise.withResolvers();
        setTimeout(resolve, Math.min(50, forkPollInterval()));
        await promise;
      }
      if (queue.length === 0) {
        if (!canceled) logWrite('[live] idle window elapsed\\n');
        break;
      }
    }
    if (canceled) break;
    const entry = queue.shift();
    const from = typeof entry.from === 'string' ? entry.from : 'main';
    const text = String(entry.message);
    logWrite('\\n[live] message from ' + from + ': ' + text + '\\n');
    writeState(true);
    const startedAt = Date.now();
    let turnStdout = '';
    let turnCode = null;
    let turnError;
    try {
      turnChild = spawn(spec.bin, ['--resume', String(forkSessionId || ''), '-p', text, '--output-format', 'json'], {cwd: spec.cwd, stdio: ['ignore', 'pipe', 'pipe']});
      if (turnChild.stdout) turnChild.stdout.on('data', chunk => { turnStdout += chunk.toString(); });
      if (turnChild.stderr) turnChild.stderr.on('data', chunk => logWrite(chunk.toString()));
      const {promise, resolve} = Promise.withResolvers();
      turnChild.on('close', code => { turnCode = code; resolve(); });
      turnChild.on('error', error => { turnError = error; resolve(); });
      await promise;
    } catch (error) {
      turnError = error;
    }
    turnChild = undefined;
    if (canceled) break;
    const parsed = parseForkLines(turnStdout);
    if (parsed.result && typeof parsed.result.sessionId === 'string' && parsed.result.sessionId.length > 0) {
      forkSessionId = parsed.result.sessionId;
      writeRecord({forkSessionId: forkSessionId});
    }
    for (const line of parsed.toolLines) logWrite(line + '\\n');
    const reply = parsed.result && typeof parsed.result.finalText === 'string' ? parsed.result.finalText.trim() : '';
    if (reply.length > 0) {
      logWrite(reply + '\\n');
      transcriptWrite({at: Date.now(), update: {sessionUpdate: 'agent_message_chunk', content: {type: 'text', text: reply + '\\n'}}});
    }
    const failure = turnError !== undefined
      ? 'live turn failed: ' + String(turnError && turnError.message || turnError)
      : !parsed.result
        ? 'live turn failed: fork exited before a result (exit ' + String(turnCode) + ')'
        : parsed.result.subtype === 'error' || (typeof parsed.result.error === 'string' && parsed.result.error.length > 0)
          ? 'live turn failed: ' + String(parsed.result.error || 'API error')
          : undefined;
    messageCount += 1;
    const scan = scanOutput((failure === undefined ? 'reply to ' + from + ':' : failure + '\\nreply to ' + from + ':') + '\\n' + reply);
    emit(signalEnvelope({
      id: spec.id + '-msg-' + messageCount,
      label: spec.label + ' (message)',
      kind: 'message',
      owner: spec.owner,
      startedAt: startedAt,
      logPath: spec.logPath,
      sessionId: spec.sessionId,
      ...(typeof spec.agentId === 'string' ? {agentId: spec.agentId} : {})
    }, {
      state: failure === undefined ? 'done' : 'failed',
      exitCode: failure === undefined ? 0 : null,
      finishedAt: Date.now(),
      durationMs: 0,
      title: (spec.name || spec.label) + (failure === undefined ? ' replied' : ' failed a live turn'),
      body: scan.text,
      scanned: true,
      scanMatches: scan.matches
    }));
    writeState(false);
  }
  clearInterval(timer);
  try { if (turnChild) turnChild.kill('SIGTERM'); } catch (e) {}
};

// The first fork turn. A result line is success; an event stream with no result,
// a timeout, or an error subtype is a failure that still carries the last text.
const finishFork = (code, spawnError, signal) => {
  if (settled) return;
  clearTimers();
  const record = readRecord();
  if (record.state === 'canceled') {
    settled = true;
    shutdown();
    return;
  }
  const parsed = parseForkLines(forkStdout);
  const result = parsed.result;
  if (result && typeof result.sessionId === 'string' && result.sessionId.length > 0) {
    forkSessionId = result.sessionId;
    writeRecord({forkSessionId: forkSessionId});
  }
  for (const line of parsed.toolLines) logWrite(line + '\\n');
  const text = result && typeof result.finalText === 'string' ? result.finalText.trim() : '';
  if (text.length > 0) {
    logWrite(text.charAt(text.length - 1) === '\\n' ? text : text + '\\n');
    transcriptWrite({at: Date.now(), update: {sessionUpdate: 'agent_message_chunk', content: {type: 'text', text: text + '\\n'}}});
  }
  const timeoutLabel = spec.timeoutMs >= 1000 ? Math.round(spec.timeoutMs / 1000) + 's' : spec.timeoutMs + 'ms';
  const errorText = result && typeof result.error === 'string' ? result.error : '';
  const stopReason = result && typeof result.stopReason === 'string' ? result.stopReason : '';
  const detail = timedOut
    ? 'timed out after ' + timeoutLabel
    : spawnError !== undefined
      ? 'spawn error: ' + String(spawnError && spawnError.message || spawnError)
      : !result
        ? 'fork exited before a result (exit ' + String(code) + ')'
        : errorText.length > 0
          ? errorText
          : 'API error';
  if (timedOut || spawnError !== undefined || !result) {
    fail(detail, code);
    shutdown();
    return;
  }
  if (result.subtype === 'error' || errorText.length > 0) {
    failApi(detail);
    shutdown();
    return;
  }
  if (stopReason === 'max_turn_requests' || stopReason === 'max_tokens' || stopReason === 'max_turns') {
    settle('failed', 'max turns reached; result is partial', code, text, {partial: true});
  } else {
    settle('done', 'exit 0', code === null ? 0 : code, text);
  }
  if (spec.live === true) {
    forkLiveLoop().then(shutdown).catch(() => shutdown());
    return;
  }
  shutdown();
};

if (typeof child.pid === 'number') {
  writeRecord({state: 'running', pid: child.pid, workerPid: process.pid});
}

child.on('exit', (code, signal) => { if (!isFork) finish(code, undefined, signal); });
if (isFork) child.on('close', (code, signal) => finishFork(code, undefined, signal));
child.on('error', err => finish(null, err));

// The ACP client. One child, one session, one prompt. session/new creates the
// session and returns its id, modes, and model/effort config options; the order
// is initialize, session/new (or session/resume), set_mode, set_config_option,
// session/prompt. Updates stream to the job log.
if (spec.kind === 'agent' && child.stdout && child.stderr) {
  let nextId = 1;
  const pending = new Map();
  const sendMessage = message => {
    try { child.stdin.write(JSON.stringify(Object.assign({jsonrpc: '2.0'}, message)) + '\\n'); } catch (e) {}
  };
  const request = (method, params) => {
    const {promise, resolve, reject} = Promise.withResolvers();
    const id = nextId++;
    pending.set(id, {resolve: resolve, reject: reject});
    sendMessage({id: id, method: method, params: params});
    return promise;
  };
  const reply = (id, result) => sendMessage({id: id, result: result});
  const replyError = (id, code, message) => sendMessage({id: id, error: {code: code, message: message}});

  // Permission prompts. ACP asks; the user decides. A background run cannot ask,
  // so it settles waiting-input with the request on the signal and exits (the job
  // stays resumable). A foreground run writes the request to its control dir and
  // waits, bounded, for the parent's answer; Esc answers cancelled, which denies
  // one call and lets the turn continue. dontAsk and plan deny without asking;
  // bypass allows, because the main session already did.
  const permissionFile = spec.controlDir ? path.join(spec.controlDir, 'permission.json') : '';
  const permissionResponseFile = spec.controlDir ? path.join(spec.controlDir, 'permission-response.json') : '';

  const writeJsonAtomic = (file, value) => {
    try {
      fs.mkdirSync(path.dirname(file), {recursive: true});
      const tmp = file + '.tmp-' + process.pid;
      fs.writeFileSync(tmp, JSON.stringify(value, null, 2));
      fs.renameSync(tmp, file);
      return true;
    } catch (e) { return false; }
  };

  const readPermissionResponse = () => {
    if (permissionResponseFile.length === 0) return undefined;
    try {
      const response = JSON.parse(fs.readFileSync(permissionResponseFile, 'utf8'));
      try { fs.rmSync(permissionResponseFile, {force: true}); } catch (e) {}
      try { fs.rmSync(permissionFile, {force: true}); } catch (e) {}
      return response;
    } catch (e) { return undefined; }
  };

  const waitForPermissionResponse = () => {
    const timeout = typeof spec.permissionTimeoutMs === 'number' && spec.permissionTimeoutMs > 0
      ? spec.permissionTimeoutMs
      : 30000;
    const deadline = Date.now() + timeout;
    const {promise, resolve} = Promise.withResolvers();
    const tick = () => {
      const response = readPermissionResponse();
      if (response !== undefined) return resolve(response);
      if (Date.now() >= deadline) return resolve(undefined);
      const timer = setTimeout(tick, 25);
      if (timer.unref) timer.unref();
    };
    tick();
    return promise;
  };

  const answerPermission = (message, decision, options) => {
    if (decision && decision.outcome === 'cancelled') {
      reply(message.id, {outcome: {outcome: 'cancelled'}});
      return 'denied';
    }
    const wanted = decision && typeof decision.optionId === 'string' ? decision.optionId : undefined;
    const selected = wanted !== undefined
      ? options.find(option => option.optionId === wanted)
      : options.find(option => option.kind === 'allow_once')
        || options.find(option => option.kind === 'allow_always')
        || options.find(option => option.kind.indexOf('allow') === 0);
    if (selected && selected.optionId.length > 0) {
      reply(message.id, {outcome: {outcome: 'selected', optionId: selected.optionId}});
      return 'allowed';
    }
    reply(message.id, {outcome: {outcome: 'cancelled'}});
    return 'denied';
  };

  const handlePermission = async message => {
    const rawOptions = message.params && Array.isArray(message.params.options) ? message.params.options : [];
    const options = rawOptions.map(option => ({
      optionId: String(option.optionId || ''),
      kind: String(option.kind || ''),
      name: String(option.name || '')
    }));
    const toolCall = message.params && message.params.toolCall;
    const rawTitle = toolCall ? String(toolCall.title || toolCall.kind || '') : '';
    const title = rawTitle.length > 0 ? rawTitle : 'a tool call';
    const mode = spec.permissionMode;
    logWrite('\\n[permission] ' + title + '\\n');
    if (mode === 'bypassPermissions') {
      logWrite('[permission] bypassPermissions -> ' + answerPermission(message, undefined, options) + '\\n');
      return;
    }
    if (mode === 'dontAsk' || mode === 'plan') {
      logWrite('[permission] ' + mode + ' -> ' + answerPermission(message, {outcome: 'cancelled'}, options) + '\\n');
      return;
    }
    if (spec.interactive === true && permissionFile.length > 0) {
      const request = {
        requestId: message.id,
        title: title,
        toolCall: toolCall ? {title: title, kind: String(toolCall.kind || '')} : undefined,
        options: options
      };
      if (writeJsonAtomic(permissionFile, request)) {
        // A response left over from an earlier prompt must not answer this one.
        try { fs.rmSync(permissionResponseFile, {force: true}); } catch (e) {}
        logWrite('[permission] waiting for the main session\\n');
        const response = await waitForPermissionResponse();
        if (response !== undefined) {
          logWrite('[permission] ' + answerPermission(message, response, options) + ' by the main session\\n');
          return;
        }
        logWrite('[permission] no answer; waiting-input\\n');
      }
    }
    const names = options.map(option => option.name).filter(name => name.length > 0);
    settle('waiting-input', 'approval required: ' + title + (names.length > 0 ? ' (' + names.join(', ') + ')' : ''), null, '', {
      question: 'Approve "' + title + '"?' + (names.length > 0 ? ' Options: ' + names.join(', ') + '.' : ''),
      permissionRequest: {requestId: message.id, title: title, options: options}
    });
    shutdown();
  };

  // The host publishes usage after each turn. Auto-compaction itself is host-side;
  // the observable event is the crossing of this agent's window with the token
  // count from before it. A new high-water mark per turn keeps one crossing from
  // emitting duplicates. Each subagent has its own session, so windows never mix.
  let compaction;
  const maybeCompact = update => {
    if (!update || update.sessionUpdate !== 'usage_update') return;
    const used = Number(update.used);
    if (!Number.isFinite(used) || used <= 0) return;
    const window = typeof spec.autoCompactWindow === 'number' ? spec.autoCompactWindow : undefined;
    if (window === undefined || used < window) return;
    if (compaction !== undefined && used <= compaction.preTokens) return;
    compaction = {trigger: 'auto', preTokens: Math.round(used), at: Date.now()};
    writeRecord({compactBoundary: compaction});
    transcriptWrite({at: compaction.at, type: 'compact_boundary', trigger: 'auto', preTokens: compaction.preTokens});
    logWrite('\\n[harness: compact_boundary ' + JSON.stringify(compaction) + ']\\n');
  };

  const handleLine = line => {
    let message;
    try { message = JSON.parse(line); } catch (e) { return; }
    if (message && typeof message.method === 'string') {
      if (message.method === 'session/update') {
        const update = message.params && message.params.update;
        if (!update) return;
        // The transcript is append-only and independent of the log, so a restart can
        // read the conversation even if the log was rotated or the session compacted.
        transcriptWrite({at: Date.now(), update: update});
        maybeCompact(update);
        if (update.sessionUpdate === 'agent_message_chunk' && update.content && typeof update.content.text === 'string') {
          logWrite(update.content.text);
        } else if (update.sessionUpdate === 'tool_call') {
          logWrite('\\n[tool] ' + String(update.title || update.kind || 'call') + '\\n');
        } else if (update.sessionUpdate === 'tool_call_update' && update.status) {
          logWrite('[tool ' + String(update.status) + '] ' + String(update.title || '') + '\\n');
        }
        return;
      }
      if (message.method === 'session/request_permission') {
        handlePermission(message).catch(error => {
          logWrite('[permission] error: ' + String(error && error.message || error) + '\\n');
          settle('waiting-input', 'approval required', null, '', {
            question: 'Approve a tool call?',
            permissionRequest: {requestId: message.id, title: 'a tool call', options: []}
          });
          shutdown();
        });
        return;
      }
      if (message.method === 'fs/read_text_file') {
        try { reply(message.id, {content: fs.readFileSync(message.params.path, 'utf8')}); }
        catch (e) { replyError(message.id, -32000, String(e && e.message || e)); }
        return;
      }
      if (message.method === 'fs/write_text_file') {
        try {
          fs.mkdirSync(path.dirname(message.params.path), {recursive: true});
          fs.writeFileSync(message.params.path, String(message.params.content || ''));
          reply(message.id, null);
        } catch (e) { replyError(message.id, -32000, String(e && e.message || e)); }
        return;
      }
      if (message.id !== undefined) replyError(message.id, -32601, 'method not found: ' + message.method);
      return;
    }
    if (message && message.id !== undefined && pending.has(message.id)) {
      const entry = pending.get(message.id);
      pending.delete(message.id);
      if (message.error) entry.reject(new Error(String(message.error.message || 'ACP error')));
      else entry.resolve(message.result);
    }
  };

  let lineBuffer = '';
  child.stdout.on('data', chunk => {
    lineBuffer += chunk.toString();
    let index;
    while ((index = lineBuffer.indexOf('\\n')) >= 0) {
      const line = lineBuffer.slice(0, index).trim();
      lineBuffer = lineBuffer.slice(index + 1);
      if (line.length > 0) handleLine(line);
    }
  });
  child.stderr.on('data', chunk => logWrite(chunk.toString().replace(/\\s+$/, '') + '\\n'));

  // Spec permissionMode -> ACP mode id. auto is deliberately absent: it is the
  // host classifier, which has no ACP equivalent, so the adapter leaves ACP in its
  // own mode and logs it. Every other mode maps to a real ACP mode.
  const modeIds = {
    default: 'default',
    acceptEdits: 'auto-accept',
    dontAsk: 'dont-ask',
    plan: 'plan',
    manual: 'default',
    bypassPermissions: 'bypass'
  };

  // ---- live messaging ----
  // One control directory per job: send.jsonl is appended by the sender, cancel is
  // a marker, and state.json says whether a turn is active. Polling (not watch)
  // keeps this portable; the send file is append-only and a byte offset means each
  // line is read exactly once, even while a turn is running.
  const controlDir = typeof spec.controlDir === 'string' ? spec.controlDir : '';
  const sendPath = controlDir.length > 0 ? path.join(controlDir, 'send.jsonl') : '';
  const cancelPath = controlDir.length > 0 ? path.join(controlDir, 'cancel') : '';
  const statePath = controlDir.length > 0 ? path.join(controlDir, 'state.json') : '';
  const queue = [];
  let sendOffset = 0;
  let sendPartial = '';
  let canceled = false;
  let cancelSent = false;
  let activeSessionId;
  let turnCount = 0;
  let waiting = null;
  let childGone = false;

  const wake = () => {
    const resolve = waiting;
    waiting = null;
    if (resolve) resolve();
  };

  // The parent reads this to tell "queued for the next safe point" from "starts a
  // turn now". Atomic rename, so a reader never sees a half-written file.
  const writeState = busy => {
    if (statePath.length === 0) return;
    try {
      fs.mkdirSync(controlDir, {recursive: true});
      const tmp = statePath + '.tmp-' + process.pid;
      fs.writeFileSync(tmp, JSON.stringify({busy: !!busy, turns: turnCount, updatedAt: Date.now()}));
      fs.renameSync(tmp, statePath);
    } catch (e) {}
  };

  const readSend = () => {
    if (sendPath.length === 0) return;
    let size;
    try { size = fs.statSync(sendPath).size; } catch (e) { return; }
    if (size <= sendOffset) return;
    let chunk = '';
    try {
      const fd = fs.openSync(sendPath, 'r');
      const length = size - sendOffset;
      const buffer = Buffer.alloc(length);
      fs.readSync(fd, buffer, 0, length, sendOffset);
      fs.closeSync(fd);
      sendOffset = size;
      chunk = buffer.toString('utf8');
    } catch (e) { return; }
    const lines = (sendPartial + chunk).split('\\n');
    sendPartial = lines.pop() || '';
    let added = false;
    for (const line of lines) {
      if (!line.trim()) continue;
      try {
        const entry = JSON.parse(line);
        if (entry && typeof entry.message === 'string' && entry.message.length > 0) {
          queue.push(entry);
          added = true;
        }
      } catch (e) {}
    }
    if (added) wake();
  };

  const pollControl = () => {
    if (cancelPath.length > 0 && !canceled) {
      try {
        if (fs.existsSync(cancelPath)) {
          canceled = true;
          logWrite('[live] cancel requested\\n');
          wake();
        }
      } catch (e) {}
    }
    readSend();
    if (canceled && !cancelSent && typeof activeSessionId === 'string') {
      cancelSent = true;
      sendMessage({id: nextId++, method: 'session/cancel', params: {sessionId: activeSessionId}});
    }
    if (child.exitCode !== null || child.signalCode !== null) {
      childGone = true;
      wake();
    }
  };
  const pollMs = typeof spec.controlPollMs === 'number' && spec.controlPollMs > 0 ? spec.controlPollMs : 100;
  const controlTimer = setInterval(pollControl, pollMs);
  if (controlTimer.unref) controlTimer.unref();
  pollControl();

  // A turn's terminal reason, or undefined for a completed turn. The first turn
  // settles the job (existing behavior); a live turn emits a message signal and
  // leaves the session open.
  const turnOutcome = result => {
    clearTimers();
    const stopReason = result && result.stopReason;
    if (stopReason === 'end_turn') return undefined;
    if (stopReason === 'cancelled') return 'cancelled';
    if (stopReason === 'max_tokens') return 'max tokens reached; result is partial';
    if (stopReason === 'max_turn_requests') return 'max turns reached; result is partial';
    return 'ACP turn ended: ' + String(stopReason || 'unknown');
  };

  const emitTurn = (from, start, failure) => {
    turnCount += 1;
    let tail = '';
    try { tail = fs.readFileSync(spec.logPath, 'utf8').slice(start); } catch (e) { tail = ''; }
    const prefix = failure === undefined
      ? 'reply to ' + String(from || 'main') + ':'
      : failure + '\\nreply to ' + String(from || 'main') + ':';
    const scan = scanOutput(prefix + '\\n' + tail);
    const partial = typeof failure === 'string' && /result is partial/.test(failure);
    emit(signalEnvelope({
      id: spec.id + '-msg-' + turnCount,
      label: spec.label + ' (message)',
      kind: 'message',
      owner: spec.owner,
      startedAt: Date.now(),
      logPath: spec.logPath,
      sessionId: spec.sessionId,
      ...(typeof spec.agentId === 'string' ? {agentId: spec.agentId} : {})
    }, Object.assign({
      state: failure === undefined ? 'done' : 'failed',
      exitCode: failure === undefined ? 0 : null,
      finishedAt: Date.now(),
      durationMs: 0,
      title: (spec.name || spec.label) + (failure === undefined ? ' replied' : ' failed a live turn'),
      body: scan.text,
      scanned: true,
      scanMatches: scan.matches
    }, partial ? {partial: true} : {})));
  };

  // After the first turn: wait for a message, run it as its own prompt, and only
  // exit once the idle window passes. A message that arrives mid-turn is queued by
  // the poller and picked up here, so the running turn is never restarted.
  const liveLoop = async () => {
    if (spec.live !== true) return;
    const idleMs = typeof spec.idleMs === 'number' && spec.idleMs >= 0 ? spec.idleMs : 30000;
    writeState(false);
    while (!canceled && !childGone) {
      if (queue.length === 0) {
        const {promise, resolve} = Promise.withResolvers();
        waiting = resolve;
        if (queue.length > 0 || canceled || childGone) {
          waiting = null;
          resolve();
        }
        const deadline = setTimeout(resolve, idleMs);
        if (deadline.unref) deadline.unref();
        await promise;
        clearTimeout(deadline);
        waiting = null;
        if (queue.length === 0) {
          if (!canceled && !childGone) logWrite('[live] idle window elapsed\\n');
          return;
        }
      }
      if (canceled || childGone) return;
      const entry = queue.shift();
      const from = typeof entry.from === 'string' ? entry.from : 'main';
      const text = String(entry.message);
      logWrite('\\n[live] message from ' + from + ': ' + text + '\\n');
      writeState(true);
      let start = 0;
      try { start = fs.statSync(spec.logPath).size; } catch (e) { start = 0; }
      let result;
      try {
        result = await request('session/prompt', {sessionId: activeSessionId, prompt: [{type: 'text', text: text}]});
      } catch (error) {
        logWrite('[live] turn failed: ' + String(error && error.message || error) + '\\n');
        emitTurn(from, start, 'live turn failed: ' + String(error && error.message || error));
        writeState(false);
        continue;
      }
      const outcome = turnOutcome(result);
      if (canceled && (outcome === 'cancelled' || outcome === undefined)) return;
      emitTurn(from, start, outcome);
      writeState(false);
      if (outcome !== undefined) return;
    }
  };

  const runPrompt = async () => {
    await request('initialize', {
      protocolVersion: 1,
      clientCapabilities: {fs: {readTextFile: true, writeTextFile: true}, terminal: false}
    });
    // session/resume returns the mode/config state, not a sessionId: the resumed id
    // is the one the worker sent. session/new is the only place an id is discovered.
    let sessionId;
    let configOptions = [];
    // The MCP servers are resolved in the parent and scoped to this agent;
    // session/new is where ACP attaches them to this job's session.
    const mcpServers = Array.isArray(spec.mcpServers) ? spec.mcpServers : [];
    if (mcpServers.length > 0) logWrite('[acp] mcp servers ' + mcpServers.map(server => server.name).join(', ') + '\\n');
    // ACP exposes none of these as a per-session option. Record the resolved values
    // so the deviation is visible in the job log instead of silent; the host default
    // still governs.
    if (typeof spec.promptCache === 'string') {
      logWrite('[acp] prompt cache ' + spec.promptCache + (spec.promptCache === 'shared' ? ' (the main conversation)' : ' (this agent)') + '\\n');
    }
    if (typeof spec.autoCompactWindow === 'number') {
      logWrite('[acp] auto-compact window ' + spec.autoCompactWindow + ' requested; ACP has no option (host default applies)\\n');
    }
    if (typeof spec.cacheTtl === 'string') {
      logWrite('[acp] cache TTL ' + spec.cacheTtl + ' requested; ACP has no option (host default applies)\\n');
    }
    if (typeof spec.resumeSession === 'string' && spec.resumeSession.length > 0) {
      try {
        const state = await request('session/resume', {sessionId: spec.resumeSession, cwd: spec.cwd, mcpServers: mcpServers});
        sessionId = spec.resumeSession;
        configOptions = state && Array.isArray(state.configOptions) ? state.configOptions : [];
        logWrite('[acp] resumed ' + sessionId + '\\n');
      } catch (error) {
        logWrite('[acp] resume failed: ' + String(error && error.message || error) + '\\n');
      }
    }
    if (sessionId === undefined) {
      const session = await request('session/new', {cwd: spec.cwd, mcpServers: mcpServers});
      if (!session || typeof session.sessionId !== 'string') throw new Error('session/new returned no sessionId');
      sessionId = session.sessionId;
      configOptions = Array.isArray(session.configOptions) ? session.configOptions : [];
    }
    writeRecord({acpSessionId: sessionId});
    activeSessionId = sessionId;
    logWrite('[acp] session ' + sessionId + ' cwd ' + spec.cwd + '\\n');
    if (typeof spec.permissionMode === 'string') {
      if (modeIds[spec.permissionMode]) {
        try {
          await request('session/set_mode', {sessionId: sessionId, modeId: modeIds[spec.permissionMode]});
        } catch (error) {
          logWrite('[acp] mode not set: ' + String(error && error.message || error) + '\\n');
        }
      } else if (spec.permissionMode === 'auto' || spec.permissionMode === 'bypassPermissions') {
        // auto is a classifier and bypass requires the main conversation to be in
        // it. Neither is safe to infer here; phase 8 applies the inheritance rule.
        logWrite('[acp] permissionMode ' + spec.permissionMode + ' is not applied in this phase\\n');
      }
    }
    // The tool set is resolved in the parent: the worker only makes it visible and
    // never widens it.
    if (Array.isArray(spec.tools)) {
      logWrite('[acp] tools ' + spec.tools.length + (spec.tools.length > 0 ? ': ' + spec.tools.join(', ') : '') + '\\n');
    }
    // Family aliases resolve to the newest model the session offers, matching the
    // host's catalog order. A full id passes through unchanged. When the catalog is
    // known and blocks the value, the inherited (main) model is used and the
    // substitution is logged: resolution never silently picks another model.
    const modelOption = configOptions.find(option => option && option.id === 'model');
    const modelOptions = (modelOption && modelOption.options) || [];
    const inheritedModel = modelOption && typeof modelOption.currentValue === 'string' ? modelOption.currentValue : undefined;
    const modelAliases = {sonnet: /sonnet/i, opus: /opus/i, haiku: /haiku/i, fable: /fable/i};
    let modelValue = spec.model;
    if (typeof modelValue === 'string' && modelAliases[modelValue.toLowerCase()]) {
      const match = modelOptions.find(option => option && modelAliases[modelValue.toLowerCase()].test(String(option.name || '')));
      if (match && typeof match.value === 'string') {
        logWrite('[acp] model alias ' + modelValue + ' -> ' + match.value + '\\n');
        modelValue = match.value;
      } else if (modelOptions.length > 0) {
        logWrite('[acp] model alias ' + modelValue + ' is not in the allowed catalog; inheriting ' + String(inheritedModel || 'the session default') + '\\n');
        modelValue = inheritedModel;
      }
    } else if (typeof modelValue === 'string' && modelOptions.length > 0 && !modelOptions.some(option => option && String(option.value) === modelValue)) {
      logWrite('[acp] model ' + modelValue + ' is not in the allowed catalog; inheriting ' + String(inheritedModel || 'the session default') + '\\n');
      modelValue = inheritedModel;
    }
    for (const option of ['model', 'effort']) {
      const value = option === 'model' ? modelValue : spec[option];
      if (typeof value !== 'string' || value.length === 0 || value === 'inherit') continue;
      try {
        await request('session/set_config_option', {sessionId: sessionId, configId: option, value: value});
      } catch (error) {
        logWrite('[acp] ' + option + ' not set: ' + String(error && error.message || error) + '\\n');
      }
    }
    // The definition body belongs in the system prompt. That path depends on the mod
    // being loaded by the ACP child (installed), which writes promptMarkerPath from
    // its factory. When the marker is absent, the body is carried in the prompt so
    // it can never silently disappear.
    const body = typeof spec.systemPrompt === 'string' ? spec.systemPrompt : '';
    const rosterText = typeof spec.roster === 'string' ? spec.roster : '';
    const startupText = typeof spec.startup === 'string' ? spec.startup : '';
    let hookReady = false;
    if (typeof spec.promptMarkerPath === 'string' && spec.promptMarkerPath.length > 0) {
      try { hookReady = fs.existsSync(spec.promptMarkerPath); } catch (e) { hookReady = false; }
    }
    let promptText = spec.prompt;
    const preamble = [];
    if (body.length > 0 && !hookReady) preamble.push('AGENT DEFINITION (your system prompt):\\n' + body);
    if (rosterText.length > 0 && !hookReady) preamble.push(rosterText);
    if (startupText.length > 0 && !hookReady) preamble.push(startupText);
    if (preamble.length > 0) {
      promptText = preamble.join('\\n\\n') + '\\n\\nTASK:\\n' + String(spec.prompt);
    }
    if (body.length > 0 && !hookReady) {
      logWrite('[acp] mod not loaded in the ACP child; the definition body is in the prompt\\n');
    }
    if (startupText.length > 0) {
      logWrite('[acp] startup context ' + startupText.length + ' chars (hook ' + (hookReady ? 'live' : 'absent') + ')\\n');
    }
    const result = await request('session/prompt', {sessionId: sessionId, prompt: [{type: 'text', text: promptText}]});
    const stopReason = result && typeof result.stopReason === 'string' ? result.stopReason : '';
    // maxTurns and max_tokens stop with what the agent already produced. The result
    // is partial, not lost: the record and signal say so and the job stays resumable.
    const partial = stopReason === 'max_turns' || stopReason === 'max_turn_requests' || stopReason === 'max_tokens';
    const outcome = turnOutcome(result);
    if (outcome !== undefined) {
      settle('failed', outcome, null, readReportTail(), Object.assign(
        partial ? {partial: true} : {},
        compaction === undefined ? {} : {compactBoundary: compaction}
      ));
      shutdown();
      return;
    }
    // The first turn is the job's result, as before. With live messaging the
    // session then stays open: the record is done, but the process (and its ACP
    // session) keeps accepting messages until the idle window passes.
    settle('done', 'exit 0', 0, readReportTail(), compaction === undefined ? undefined : {compactBoundary: compaction});
    await liveLoop();
    shutdown();
  };
  runPrompt().catch(error => {
    failApi('ACP session failed: ' + String(error && error.message || error));
    shutdown();
  });
}

process.on('uncaughtException', err => {
  try { fail('worker crash: ' + String(err && err.message || err), null); } catch (e) {}
  process.exit(0);
});
`;

// ---------------------------------------------------------------------------
// Runtime seam
//
// Every OS call the mod makes goes through one interface: the clock, spawning and
// killing processes, and the spool filesystem. Production wires the OS adapter
// below; a test passes fakes, so spawn failure, kill escalation and clock-driven
// recovery are drivable without a real process, a real wait or a real crash.
// ---------------------------------------------------------------------------

export interface ChildHandle {
	readonly pid?: number;
	on(event: 'error', handler: (error: unknown) => void): void;
	on(event: 'exit', handler: (code: number | null, signal: string | null) => void): void;
	unref(): void;
	kill(signal?: string): void;
}

export interface SpawnOptions {
	cwd?: string;
	detached?: boolean;
	stdio?: 'ignore' | Array<'ignore' | number>;
	env?: Record<string, string | undefined>;
}

export interface SpoolFs {
	mkdir(dir: string): void;
	readdir(dir: string): string[];
	readFile(file: string): string;
	writeFile(file: string, data: string): void;
	appendFile(file: string, data: string): void;
	rename(from: string, to: string): void;
	remove(file: string): void;
	stat(file: string): {size: number; mtimeMs: number};
	exists(file: string): boolean;
}

/** One bounded command's result: a value, never a throw. */
export interface CommandResult {
	ok: boolean;
	stdout: string;
	code?: number;
	stderr?: string;
}

export interface Runtime {
	now(): number;
	spawn(command: string, args: string[], options: SpawnOptions): ChildHandle;
	kill(pid: number, signal: string): void;
	alive(pid: unknown): boolean;
	sleep(ms: number): Promise<void>;
	readonly pid: number;
	readonly execPath: string;
	readonly env: Record<string, string | undefined>;
	/** The workspace root: project discovery starts here. */
	readonly cwd: string;
	/** The user home: `~/.commandcode/agents` lives under it. */
	readonly home: string;
	/** Whether a human is driving. A CLI agent file is refused when one is. */
	readonly interactive: boolean;
	readonly fs: SpoolFs;
	/**
	 * One bounded, synchronous command. Optional so a test can leave the OS out; a
	 * failure is a value, never a throw.
	 */
	run?(command: string, args: string[], options: {cwd?: string; timeoutMs?: number; env?: Record<string, string | undefined>}): CommandResult;
}

export type RuntimeOverrides = Partial<Omit<Runtime, 'fs'>> & {fs?: Partial<SpoolFs>};

function osSpoolFs(): SpoolFs {
	return {
		mkdir: dir => {
			mkdirSync(dir, {recursive: true});
		},
		readdir: dir => readdirSync(dir),
		readFile: file => readFileSync(file, 'utf8'),
		writeFile: (file, data) => writeFileSync(file, data),
		appendFile: (file, data) => appendFileSync(file, data),
		rename: (from, to) => renameSync(from, to),
		remove: file => {
			rmSync(file, {force: true, recursive: true});
		},
		stat: file => {
			const stats = statSync(file);
			return {size: stats.size, mtimeMs: stats.mtimeMs};
		},
		exists: file => {
			try {
				return statSync(file).isDirectory();
			} catch {
				return false;
			}
		},
	};
}

// The production adapter. Only this object is allowed to name node:fs,
// node:child_process or process.
const OS_RUNTIME: Runtime = {
	now: () => Date.now(),
	spawn: (command, args, options) => spawn(command, args, options as never) as unknown as ChildHandle,
	kill: (pid, signal) => {
		process.kill(pid, signal as never);
	},
	alive: pid => {
		if (typeof pid !== 'number' || !Number.isInteger(pid) || pid <= 0) return false;
		try {
			process.kill(pid, 0);
			return true;
		} catch (error) {
			return (error as {code?: string}).code === 'EPERM';
		}
	},
	// The only wait in the mod, used by subagent_output's opt-in blocking read.
	sleep: ms => new Promise<void>(resolve => setTimeout(resolve, ms)),
	pid: process.pid,
	execPath: process.execPath,
	env: process.env,
	cwd: process.cwd(),
	home: homedir(),
	interactive: process.stdin.isTTY === true,
	fs: osSpoolFs(),
	// One bounded command. Everything it returns is plain data, so a failed git (not a
	// repository, no ref) is a value, never a throw here.
	run: (command, args, options) => {
		const result = spawnSync(command, args, {
			cwd: options.cwd,
			encoding: 'utf8',
			timeout: options.timeoutMs ?? 2000,
			maxBuffer: 64 * 1024,
			...(options.env !== undefined ? {env: {...process.env, ...options.env}} : {}),
		});
		return {
			ok: result.status === 0,
			code: typeof result.status === 'number' ? result.status : 1,
			stdout: String(result.stdout ?? ''),
			stderr: String(result.stderr ?? result.error?.message ?? ''),
		};
	},
};

// The test adapter is the same interface with functions swapped out.
function osRuntime(overrides: RuntimeOverrides = {}): Runtime {
	return {
		...OS_RUNTIME,
		...overrides,
		fs: {...OS_RUNTIME.fs, ...overrides.fs},
	};
}

// ---------------------------------------------------------------------------
// Agent definitions
//
// A definition is Markdown with YAML frontmatter. The body is the system prompt,
// not the host's. Only `name` and `description` are required; every other T1 field
// is parsed and carried on the definition even when a later phase applies it.
//
// Discovery order is precedence order: the managed settings directory, the CLI
// definitions, project directories from the working directory upward (closest
// first) and every --add-dir directory, the user directory, plugin roots, and
// finally the built-ins. The first definition of a name wins. Invalid files never
// fail the session: they are recorded with a reason, written to agents.log, and
// reported by `/agents` and `/doctor`.
// ---------------------------------------------------------------------------

// The scopes, highest priority first. The order is the precedence rule: managed
// wins over CLI, CLI over project, project over user, user over plugin, and every
// one over a built-in. It is also the display order in `/agents`.
export const AGENT_SCOPES = ['managed', 'cli', 'project', 'user', 'plugin', 'builtin'] as const;

export type AgentScope = (typeof AGENT_SCOPES)[number];

export type AgentEntryKind = 'valid' | 'invalid' | 'duplicate';

/** One MCP server an agent scopes to itself: referenced by name or inline. */
export interface AgentMcpServer {
	name: string;
	/** Standard input server: a command and its arguments. */
	command?: string;
	args?: string[];
	env?: Record<string, string>;
	/** HTTP server. A server has either `command` or `url`. */
	url?: string;
}

/** One command a definition's hooks run. A hook blocks on exit 2. */
export interface AgentHookRule {
	/** Tool-name filter: `*`, an exact name, `mcp__srv__*`, or `/regex/`. */
	matcher?: string;
	command: string;
	timeoutMs?: number;
}

/** The definition hooks the worker registers while this agent is active. */
export interface AgentHooks {
	PreToolUse?: AgentHookRule[];
	PostToolUse?: AgentHookRule[];
	/** The spec's `Stop`; the runtime reports it as `SubagentStop`. */
	Stop?: AgentHookRule[];
}

export interface AgentDefinition {
	name: string;
	description: string;
	systemPrompt: string;
	source: AgentScope;
	path?: string;
	/** The plugin whose `agents/` directory provided this definition. */
	plugin?: string;
	tools?: string[];
	disallowedTools?: string[];
	model?: string;
	permissionMode?: string;
	maxTurns?: number;
	skills?: string[];
	/** `user`, `project`, or `local`: the agent's own memory directory. */
	memory?: AgentMemoryScope;
	isolation?: string;
	background?: boolean;
	color?: string;
	omitClaudeMd?: boolean;
	effort?: string;
	/** Inline or referenced MCP servers, scoped to this agent. */
	mcpServers?: AgentMcpServer[];
	/** PreToolUse/PostToolUse/Stop rules, active only while this agent runs. */
	hooks?: AgentHooks;
	/** Auto-submitted as the first user turn when this agent runs the session. */
	initialPrompt?: string;
	/** The requested auto-compact window; it can only lower the inherited one. */
	autoCompactWindow?: number;
	/** `experimental.cacheTtl`: `5m` or `1h`. */
	cacheTtl?: CacheTtl;
	readOnly?: boolean;
	oneShot?: boolean;
	/** The `fork` type: inherits the main conversation instead of a fresh context. */
	fork?: boolean;
}

export type CacheTtl = '5m' | '1h';

export interface AgentEntry {
	name: string;
	source: AgentScope;
	path: string;
	valid: boolean;
	kind: AgentEntryKind;
	reason?: string;
	/** A definition that loaded but has a problem worth reporting (`/doctor`). */
	warning?: string;
	/** For a duplicate: the file whose definition was kept. */
	duplicateOf?: string;
	definition?: AgentDefinition;
}

export interface AgentRegistry {
	entries(): AgentEntry[];
	list(): AgentDefinition[];
	get(name: string): AgentDefinition | undefined;
	refresh(): void;
}

export interface AgentRegistryDeps {
	readonly fs: SpoolFs;
	readonly cwd: string;
	readonly home: string;
	readonly builtins: AgentDefinition[];
	readonly log: (message: string) => void;
	/** The managed settings root; `agents/` under it wins every other scope. */
	readonly managedDir?: string;
	/** Plugin roots: each child directory is a plugin with an `agents/` directory. */
	readonly pluginRoots?: string[];
	/** `--mod-option agents=<json|path>`, plus whether a human is driving. */
	readonly cli?: {readonly value: string; readonly interactive: boolean};
	/** `--add-dir` directories: `.commandcode/agents/` under each is project scope. */
	readonly addDirs?: string[];
}

export type FrontmatterResult =
	| {ok: true; fields: Record<string, unknown>; body: string}
	| {ok: false; reason: string};

// A conservative YAML subset: top-level `key: value`, inline and block lists,
// quoted and block scalars, nested maps (ignored, because unknown fields must not
// error). Anything outside it is reported as bad YAML, which is the documented
// skip rule. No dependency, no eval, no way to run a value.
function parseYamlScalar(raw: string): {ok: true; value: unknown} | {ok: false; reason: string} {
	const text = raw.trim();
	if (text.length === 0) return {ok: true, value: undefined};
	if (text.startsWith('[')) {
		if (!text.endsWith(']')) return {ok: false, reason: 'unclosed flow sequence'};
		const inner = text.slice(1, -1).trim();
		if (inner.length === 0) return {ok: true, value: []};
		const items: string[] = [];
		let current = '';
		let quote = '';
		for (const char of inner) {
			if (quote.length > 0) {
				current += char;
				if (char === quote) quote = '';
				continue;
			}
			if (char === '"' || char === "'") {
				quote = char;
				current += char;
				continue;
			}
			if (char === ',') {
				items.push(current.trim());
				current = '';
				continue;
			}
			current += char;
		}
		if (quote.length > 0) return {ok: false, reason: 'unterminated quoted scalar'};
		items.push(current.trim());
		const values: unknown[] = [];
		for (const item of items) {
			if (item.length === 0) return {ok: false, reason: 'empty flow sequence item'};
			const parsed = parseYamlScalar(item);
			if (!parsed.ok) return parsed;
			values.push(parsed.value);
		}
		return {ok: true, value: values};
	}
	if (text.startsWith('{')) {
		if (!text.endsWith('}')) return {ok: false, reason: 'unclosed flow mapping'};
		return {ok: true, value: null};
	}
	if (text.startsWith('"')) {
		if (text.length < 2 || !text.endsWith('"')) return {ok: false, reason: 'unterminated double-quoted scalar'};
		try {
			return {ok: true, value: JSON.parse(text)};
		} catch {
			return {ok: false, reason: 'bad escape in double-quoted scalar'};
		}
	}
	if (text.startsWith("'")) {
		if (text.length < 2 || !text.endsWith("'")) return {ok: false, reason: 'unterminated single-quoted scalar'};
		return {ok: true, value: text.slice(1, -1).replaceAll("''", "'")};
	}
	const comment = text.search(/\s#/);
	const bare = (comment >= 0 ? text.slice(0, comment) : text).trim();
	if (bare === 'true') return {ok: true, value: true};
	if (bare === 'false') return {ok: true, value: false};
	if (bare === 'null' || bare === '~') return {ok: true, value: undefined};
	return {ok: true, value: bare};
}

// A nested value is the same indentation language as the document, one level
// down: mappings, block sequences, and scalars. It exists because `hooks`,
// `mcpServers`, and `experimental` are maps, not opaque unknowns; every other
// nested key is still accepted and simply never read.
interface NestedLine {
	indent: number;
	text: string;
}

type NestedResult = {ok: true; value: unknown; next: number} | {ok: false; reason: string};

function normalizeNestedLines(raw: string[]): NestedLine[] {
	const lines = raw
		.filter(line => line.trim().length > 0 && !line.trim().startsWith('#'))
		.map(line => {
			const indent = line.length - line.trimStart().length;
			return {indent, text: line.slice(indent).replace(/\s+$/, '')};
		});
	if (lines.length === 0) return lines;
	const margin = Math.min(...lines.map(line => line.indent));
	return lines.map(line => ({indent: line.indent - margin, text: line.text}));
}

const isSequenceLine = (text: string): boolean => text === '-' || text.startsWith('- ');

// `key: value`, where the colon must be followed by whitespace or end of line, so
// `- https://example.test` stays a scalar and does not become a map.
const MAP_LINE = /^([A-Za-z0-9_.-]+):(?:[ \t]+(.*))?$/;

function parseYamlNode(lines: NestedLine[], start: number, indent: number): NestedResult {
	const first = lines[start];
	if (first === undefined) return {ok: true, value: undefined, next: start};
	return isSequenceLine(first.text) ? parseYamlSequence(lines, start, indent) : parseYamlMapping(lines, start, indent);
}

function parseYamlSequence(lines: NestedLine[], start: number, indent: number): NestedResult {
	const values: unknown[] = [];
	let index = start;
	while (index < lines.length) {
		const line = lines[index];
		if (line.indent < indent) break;
		if (line.indent > indent) return {ok: false, reason: `unexpected indentation`};
		if (!isSequenceLine(line.text)) break;
		const rest = line.text === '-' ? '' : line.text.slice(2);
		const trimmed = rest.trim();
		if (trimmed.length === 0) {
			const next = lines[index + 1];
			if (next !== undefined && next.indent > indent) {
				const nested = parseYamlNode(lines, index + 1, next.indent);
				if (!nested.ok) return nested;
				values.push(nested.value);
				index = nested.next;
			} else {
				values.push(null);
				index += 1;
			}
			continue;
		}
		// A sequence item can open a mapping: `- matcher: Bash`, then sibling keys at
		// the same content column. The synthesized first line sits at that column, and
		// the following raw lines are already indented relative to it.
		const map = MAP_LINE.exec(trimmed);
		if (map !== null) {
			// The content column is where the first non-space after `-` starts.
			const base = indent + (line.text.length - trimmed.length);
			const itemLines: NestedLine[] = [{indent: base, text: trimmed}];
			let cursor = index + 1;
			while (cursor < lines.length && lines[cursor].indent > indent) {
				itemLines.push(lines[cursor]);
				cursor += 1;
			}
			const nested = parseYamlNode(itemLines, 0, base);
			if (!nested.ok) return nested;
			values.push(nested.value);
			index = cursor;
			continue;
		}
		const parsed = parseYamlScalar(trimmed);
		if (!parsed.ok) return parsed;
		values.push(parsed.value);
		index += 1;
	}
	return {ok: true, value: values, next: index};
}

function parseYamlMapping(lines: NestedLine[], start: number, indent: number): NestedResult {
	const value: Record<string, unknown> = {};
	let index = start;
	while (index < lines.length) {
		const line = lines[index];
		if (line.indent < indent) break;
		if (line.indent > indent) return {ok: false, reason: 'unexpected indentation'};
		const match = MAP_LINE.exec(line.text);
		if (match === null) return {ok: false, reason: `expected key: value, got "${line.text.slice(0, 40)}"`};
		const key = match[1];
		const inline = (match[2] ?? '').trim();
		if (Object.hasOwn(value, key)) return {ok: false, reason: `duplicate key "${key}"`};
		const next = lines[index + 1];
		if (inline.length > 0) {
			if (next !== undefined && next.indent > indent) {
				return {ok: false, reason: `unexpected indentation under "${key}"`};
			}
			const parsed = parseYamlScalar(inline);
			if (!parsed.ok) return parsed;
			value[key] = parsed.value;
			index += 1;
			continue;
		}
		if (next !== undefined && next.indent > indent) {
			const nested = parseYamlNode(lines, index + 1, next.indent);
			if (!nested.ok) return nested;
			value[key] = nested.value;
			index = nested.next;
			continue;
		}
		value[key] = undefined;
		index += 1;
	}
	return {ok: true, value, next: index};
}

export function parseFrontmatter(source: string): FrontmatterResult {
	const text = source.startsWith('\uFEFF') ? source.slice(1) : source;
	const lines = text.split('\n');
	if (lines[0]?.trim() !== '---') return {ok: false, reason: 'frontmatter must start with --- on the first line'};
	let close = -1;
	for (let index = 1; index < lines.length; index += 1) {
		if (lines[index].trim() === '---') {
			close = index;
			break;
		}
	}
	if (close < 0) return {ok: false, reason: 'frontmatter is not closed'};

	// Group top-level keys with their indented lines before parsing any value, so a
	// block scalar and a block list are one case rather than interleaved state. The
	// raw lines are kept: a nested value (`mcpServers`, `hooks`, `experimental`) is
	// parsed by indentation below.
	const blocks: {key: string; inline: string; lines: string[]}[] = [];
	for (let index = 1; index < close; index += 1) {
		const line = lines[index];
		const trimmed = line.trim();
		if (trimmed.length === 0 || trimmed.startsWith('#')) {
			if (blocks.length > 0) blocks[blocks.length - 1].lines.push('');
			continue;
		}
		if (line.startsWith('\t')) return {ok: false, reason: 'tab indentation is not valid YAML'};
		const indent = line.length - line.trimStart().length;
		if (indent > 0) {
			if (blocks.length === 0) return {ok: false, reason: 'unexpected indentation'};
			blocks[blocks.length - 1].lines.push(line);
			continue;
		}
		const match = /^([A-Za-z0-9_.-]+):(.*)$/.exec(line);
		if (match === null) return {ok: false, reason: `expected key: value, got "${line.slice(0, 40)}"`};
		blocks.push({key: match[1], inline: match[2].trim(), lines: []});
	}

	const fields: Record<string, unknown> = {};
	for (const block of blocks) {
		if (Object.hasOwn(fields, block.key)) return {ok: false, reason: `duplicate key "${block.key}"`};
		const blockScalar = block.inline === '|' || block.inline === '|-' || block.inline === '>' || block.inline === '>-';
		if (blockScalar) {
			// A block scalar keeps its relative indentation; only the common margin and
			// the surrounding blank lines are removed.
			const filled = block.lines.filter(line => line.trim().length > 0);
			const margin =
				filled.length === 0
					? 0
					: Math.min(...filled.map(line => line.length - line.trimStart().length));
			const body = block.lines.map(line => (line.trim().length === 0 ? '' : line.slice(margin).replace(/\s+$/, '')));
			fields[block.key] = (block.inline.startsWith('>') ? body.join(' ') : body.join('\n')).replace(/^\n+|\n+$/g, '');
			continue;
		}
		const hasNested = block.lines.some(line => line.trim().length > 0);
		if (block.inline.length === 0 && hasNested) {
			const parsed = parseYamlNode(normalizeNestedLines(block.lines), 0, 0);
			if (!parsed.ok) return parsed;
			fields[block.key] = parsed.value;
			continue;
		}
		if (hasNested) {
			return {ok: false, reason: `unexpected indentation under "${block.key}"`};
		}
		if (block.inline.length === 0) {
			fields[block.key] = undefined;
			continue;
		}
		const parsed = parseYamlScalar(block.inline);
		if (!parsed.ok) return parsed;
		fields[block.key] = parsed.value;
	}

	return {ok: true, fields, body: lines.slice(close + 1).join('\n').replace(/^\r?\n/, '').trimEnd()};
}

function listField(value: unknown): string[] | undefined {
	if (value === undefined || value === null) return undefined;
	if (Array.isArray(value)) {
		const items = value.filter((item): item is string => typeof item === 'string').map(item => item.trim()).filter(Boolean);
		return items.length > 0 ? items : undefined;
	}
	if (typeof value !== 'string') return undefined;
	const text = value.trim();
	if (text.length === 0) return undefined;
	if (text === '*') return ['*'];
	return text.split(',').map(item => item.trim()).filter(Boolean);
}

function stringField(value: unknown): string | undefined {
	return typeof value === 'string' && value.trim().length > 0 ? value.trim() : undefined;
}

function booleanField(value: unknown): boolean | undefined {
	if (typeof value === 'boolean') return value;
	if (value === 'true') return true;
	if (value === 'false') return false;
	return undefined;
}

function numberField(value: unknown): number | undefined {
	const numeric = typeof value === 'number' ? value : typeof value === 'string' ? Number(value) : Number.NaN;
	return Number.isFinite(numeric) && numeric > 0 ? Math.round(numeric) : undefined;
}

function stringMapField(value: unknown): Record<string, string> | undefined {
	if (value === null || typeof value !== 'object' || Array.isArray(value)) return undefined;
	const out: Record<string, string> = {};
	for (const [key, raw] of Object.entries(value as Record<string, unknown>)) {
		if (typeof raw !== 'string') continue;
		out[key] = raw;
	}
	return Object.keys(out).length > 0 ? out : undefined;
}

function serverFields(value: Record<string, unknown>): Omit<AgentMcpServer, 'name'> | undefined {
	const command = stringField(value.command);
	const url = stringField(value.url);
	const args = listField(value.args);
	const env = stringMapField(value.env);
	if (command === undefined && url === undefined) return undefined;
	return {
		...(command !== undefined ? {command} : {}),
		...(args !== undefined ? {args} : {}),
		...(env !== undefined ? {env} : {}),
		...(url !== undefined ? {url} : {}),
	};
}

// `mcpServers` accepts the host's map form, a list of names (references to the
// configured servers), and a list of inline servers. A plugin's servers are
// ignored by contract, so parsing is skipped there rather than resolved later.
export function parseMcpServers(value: unknown): AgentMcpServer[] | undefined {
	const out: AgentMcpServer[] = [];
	if (typeof value === 'string') {
		const name = stringField(value);
		return name === undefined ? undefined : [{name}];
	}
	if (Array.isArray(value)) {
		for (const item of value) {
			if (typeof item === 'string') {
				const name = stringField(item);
				if (name !== undefined) out.push({name});
				continue;
			}
			if (item === null || typeof item !== 'object') continue;
			const record = item as Record<string, unknown>;
			const name = stringField(record.name) ?? stringField(record.server);
			if (name === undefined) continue;
			out.push({name, ...serverFields(record)});
		}
	} else if (value !== null && typeof value === 'object') {
		for (const [name, config] of Object.entries(value as Record<string, unknown>)) {
			const clean = name.trim();
			if (clean.length === 0) continue;
			if (config === null || config === undefined || config === true) {
				out.push({name: clean});
				continue;
			}
			if (typeof config === 'string') {
				out.push({name: clean, command: config});
				continue;
			}
			if (typeof config === 'object' && !Array.isArray(config)) {
				out.push({name: clean, ...serverFields(config as Record<string, unknown>)});
			}
		}
	}
	return out.length > 0 ? out : undefined;
}

// A hook rule is a command string, or a map with `command` plus an optional
// `matcher` and `timeout` (seconds). The standard nested form
// (`{matcher, hooks: [{type, command}]}`) is flattened, inheriting the matcher.
function hookRule(value: unknown, inheritedMatcher?: string): AgentHookRule[] {
	if (typeof value === 'string') {
		const command = value.trim();
		return command.length === 0 ? [] : [{command, ...(inheritedMatcher !== undefined ? {matcher: inheritedMatcher} : {})}];
	}
	if (value === null || typeof value !== 'object' || Array.isArray(value)) return [];
	const record = value as Record<string, unknown>;
	const matcher = stringField(record.matcher) ?? inheritedMatcher;
	const nested = record.hooks;
	if (nested !== undefined) {
		// The wrapper only groups; the commands are the rules.
		return Array.isArray(nested) ? nested.flatMap(item => hookRule(item, matcher)) : hookRule(nested, matcher);
	}
	const command = stringField(record.command);
	if (command === undefined) return [];
	const seconds = numberField(record.timeout);
	return [
		{
			command,
			...(matcher !== undefined ? {matcher} : {}),
			...(seconds !== undefined ? {timeoutMs: seconds * 1000} : {}),
		},
	];
}

function hookList(value: unknown): AgentHookRule[] | undefined {
	const rules = (Array.isArray(value) ? value : [value]).flatMap(item => hookRule(item));
	return rules.length > 0 ? rules : undefined;
}

// Only the events the frontmatter reference defines are read. `SubagentStop` is
// accepted as the runtime spelling of `Stop`; anything else is an unknown field.
export function parseHooks(value: unknown): AgentHooks | undefined {
	if (value === null || typeof value !== 'object' || Array.isArray(value)) return undefined;
	const byEvent: Record<string, AgentHookRule[]> = {};
	for (const [event, raw] of Object.entries(value as Record<string, unknown>)) {
		const normalized = event.replace(/[^a-z]/gi, '').toLowerCase();
		const key =
			normalized === 'pretooluse'
				? 'PreToolUse'
				: normalized === 'posttooluse'
					? 'PostToolUse'
					: normalized === 'stop' || normalized === 'subagentstop'
						? 'Stop'
						: undefined;
		if (key === undefined) continue;
		const rules = hookList(raw);
		if (rules !== undefined) byEvent[key] = [...(byEvent[key] ?? []), ...rules];
	}
	return Object.keys(byEvent).length > 0 ? (byEvent as AgentHooks) : undefined;
}

export function parseCacheTtl(value: unknown): CacheTtl | undefined {
	if (value === null || typeof value !== 'object' || Array.isArray(value)) return undefined;
	const ttl = stringField((value as Record<string, unknown>).cacheTtl);
	return ttl === '5m' || ttl === '1h' ? ttl : undefined;
}

const AUTO_COMPACT_MIN = 100_000;
const AUTO_COMPACT_MAX = 1_000_000;

// The field may only LOWER the inherited window: a value outside the documented
// range is clamped into it, and the result is the smaller of the two. The main
// session never has one, by contract.
export function resolveAutoCompactWindow(
	inherited: number,
	requested: number | undefined,
	isMain: boolean,
): number | undefined {
	if (isMain || requested === undefined || !Number.isFinite(requested)) return undefined;
	const bounded = Math.min(AUTO_COMPACT_MAX, Math.max(AUTO_COMPACT_MIN, Math.round(requested)));
	return Math.min(inherited, bounded);
}

export interface AppendPromptInput {
	readonly flag?: string;
	readonly file?: string;
	readonly env?: Record<string, string | undefined>;
	readonly fs: Pick<SpoolFs, 'readFile'>;
	readonly cwd: string;
}

// `--append-subagent-system-prompt` has no host flag, so it arrives as a mod flag
// or an environment value (the ACP child inherits the environment, which is how a
// nested subagent gets it too). The file form and the `@path` form both read text.
export function resolveAppendSystemPrompt(input: AppendPromptInput): string {
	const env = input.env ?? {};
	const file = (
		input.file ||
		env.BETTER_SUBAGENTS_APPEND_SYSTEM_PROMPT_FILE ||
		env.CLAUDE_CODE_APPEND_SUBAGENT_SYSTEM_PROMPT_FILE ||
		''
	).trim();
	const value = (
		input.flag ||
		env.BETTER_SUBAGENTS_APPEND_SYSTEM_PROMPT ||
		env.CLAUDE_CODE_APPEND_SUBAGENT_SYSTEM_PROMPT ||
		''
	).trim();
	const read = (path: string): string => {
		try {
			return input.fs.readFile(isAbsolute(path) ? path : join(input.cwd, path)).trim();
		} catch {
			return '';
		}
	};
	const parts: string[] = [];
	if (file.length > 0) {
		const text = read(file);
		if (text.length > 0) parts.push(text);
	}
	if (value.startsWith('@')) {
		const text = read(value.slice(1).trim());
		if (text.length > 0) parts.push(text);
	} else if (value.length > 0) {
		parts.push(value);
	}
	return parts.join('\n\n');
}

// A matcher is `*`, an exact tool name, a pipe list, a `/regex/`, or a glob such
// as `mcp__server__*`. Tool names are case-insensitive, like the host's.
export function hookMatches(matcher: string | undefined, toolName: string): boolean {
	const pattern = (matcher ?? '').trim();
	if (pattern.length === 0 || pattern === '*') return true;
	if (pattern.length > 2 && pattern.startsWith('/') && pattern.endsWith('/')) {
		try {
			return new RegExp(pattern.slice(1, -1), 'i').test(toolName);
		} catch {
			return false;
		}
	}
	const glob = (value: string): boolean => {
		const escaped = value.replace(/[.+?^${}()|[\]\\]/g, '\\$&').replaceAll('*', '.*');
		return new RegExp(`^${escaped}$`, 'i').test(toolName);
	};
	return pattern.split('|').some(part => glob(part.trim()));
}

export interface HookDetail {
	readonly event: 'PreToolUse' | 'PostToolUse' | 'SubagentStart' | 'SubagentStop';
	readonly cwd: string;
	readonly agent?: string;
	readonly toolName?: string;
	readonly input?: unknown;
	readonly result?: unknown;
}

export interface HookOutcome {
	/** A PreToolUse hook requested a block; the text becomes the tool result. */
	block?: string;
	/** A PostToolUse hook's message for the model. */
	additionalContext?: string;
	/** A SubagentStop hook requested another turn. */
	continue?: boolean;
	reason?: string;
	/** How many commands actually ran. */
	ran: number;
}

const HOOK_TIMEOUT_MS = 10_000;

// The command-hook contract: exit 0 allows, exit 2 blocks (PreToolUse), feeds the
// model (PostToolUse), or continues the run (SubagentStop), and any other exit is a
// non-blocking failure. Every rule gets the event, agent, tool and payload in its
// environment, so one script can serve all events.
export function runHookRules(
	run: Runtime['run'],
	rules: readonly AgentHookRule[] | undefined,
	detail: HookDetail,
): HookOutcome {
	const outcome: HookOutcome = {ran: 0};
	if (run === undefined || rules === undefined || rules.length === 0) return outcome;
	const contexts: string[] = [];
	for (const rule of rules) {
		if ((detail.event === 'PreToolUse' || detail.event === 'PostToolUse') && !hookMatches(rule.matcher, detail.toolName ?? '')) {
			continue;
		}
		const env: Record<string, string> = {
			BETTER_SUBAGENTS_HOOK_EVENT: detail.event,
			BETTER_SUBAGENTS_HOOK_AGENT: detail.agent ?? '',
			BETTER_SUBAGENTS_HOOK_TOOL_NAME: detail.toolName ?? '',
			BETTER_SUBAGENTS_HOOK_TOOL_INPUT: JSON.stringify(detail.input ?? {}),
			BETTER_SUBAGENTS_HOOK_RESULT: JSON.stringify(detail.result ?? null),
			BETTER_SUBAGENTS_HOOK_CWD: detail.cwd,
		};
		let result: CommandResult;
		try {
			result = run('/bin/sh', ['-c', rule.command], {
				cwd: detail.cwd,
				timeoutMs: rule.timeoutMs ?? HOOK_TIMEOUT_MS,
				env,
			});
		} catch {
			continue;
		}
		outcome.ran += 1;
		const code = typeof result.code === 'number' ? result.code : result.ok ? 0 : 1;
		if (code !== 2) continue;
		const message = (result.stderr || result.stdout || '').trim();
		if (detail.event === 'PreToolUse') {
			outcome.block = message.length > 0 ? message : 'Blocked by a PreToolUse hook.';
			return outcome;
		}
		if (detail.event === 'PostToolUse') {
			if (message.length > 0) contexts.push(message);
			continue;
		}
		if (detail.event === 'SubagentStop') {
			outcome.continue = true;
			outcome.reason = message.length > 0 ? message : 'SubagentStop hook requested another turn.';
			return outcome;
		}
	}
	if (contexts.length > 0) outcome.additionalContext = contexts.join('\n\n');
	return outcome;
}

export interface AgentDefinitionOptions {
	/** A plugin definition: an unparseable file or one without a name still loads. */
	plugin?: boolean;
	/** The plugin that owns the file, recorded on the definition. */
	pluginName?: string;
	/** The scoped prefix the raw name is appended to, e.g. `my-plugin:review:`. */
	scopedPrefix?: string;
	/** The filename stem, used as the name when frontmatter provides none. */
	fallbackName?: string;
}

export type AgentDefinitionResult =
	| {ok: true; definition: AgentDefinition; warning?: string}
	| {ok: false; reason: string};

/** Every name rule in one place, so a file and a CLI definition agree. */
function validateAgentName(name: string): string | undefined {
	if ([...name].length > AGENT_NAME_MAX) return `name: must be at most ${AGENT_NAME_MAX} characters`;
	if (name.includes(':')) return 'name: must not contain ":" (plugin-scoped identifiers reserve it)';
	if (name.startsWith('-')) return 'name: must not start with "-"';
	return undefined;
}

// The body of a file whose frontmatter did not parse: everything after the closing
// `---` when one is findable, otherwise the whole file.
function fallbackBody(source: string): string {
	const text = source.startsWith('\uFEFF') ? source.slice(1) : source;
	const lines = text.split('\n');
	if (lines[0]?.trim() !== '---') return text.trim();
	for (let index = 1; index < lines.length; index += 1) {
		if (lines[index].trim() === '---') return lines.slice(index + 1).join('\n').trim();
	}
	return text.trim();
}

function fallbackDescription(body: string, name: string): string {
	const line = body
		.split('\n')
		.map(part => part.trim())
		.find(part => part.length > 0);
	return line !== undefined ? line.slice(0, 200) : `Plugin agent ${name}`;
}

function pluginFallbackDefinition(body: string, path: string, options: AgentDefinitionOptions): AgentDefinition {
	const raw = options.fallbackName !== undefined && options.fallbackName.length > 0 ? options.fallbackName : 'agent';
	const name = `${options.scopedPrefix ?? ''}${raw}`;
	const definition: AgentDefinition = {
		name,
		description: fallbackDescription(body, name),
		systemPrompt: body,
		source: 'plugin',
		path,
	};
	if (options.pluginName !== undefined) definition.plugin = options.pluginName;
	return definition;
}

export function agentDefinitionFromSource(
	source: string,
	path: string,
	scope: AgentScope,
	options: AgentDefinitionOptions = {},
): AgentDefinitionResult {
	const frontmatter = parseFrontmatter(source);
	if (!frontmatter.ok) {
		if (options.plugin !== true) return frontmatter;
		// A plugin contract differs from a project one: the file still loads under
		// its filename, and the parse failure is reported instead of hidden.
		return {
			ok: true,
			definition: pluginFallbackDefinition(fallbackBody(source), path, options),
			warning: `frontmatter: ${frontmatter.reason}; loaded under its filename`,
		};
	}
	const fields = frontmatter.fields;
	const fallbackDefinition = (warning: string): AgentDefinitionResult => {
		const definition = pluginFallbackDefinition(frontmatter.body, path, options);
		// A usable description in an otherwise unusable file is still worth keeping.
		const parsedDescription = stringField(fields.description);
		if (parsedDescription !== undefined) definition.description = parsedDescription;
		return {ok: true, definition, warning};
	};

	const rawName = stringField(fields.name) ?? '';
	if (rawName.length === 0) {
		if (options.plugin !== true) return {ok: false, reason: 'name: required'};
		return fallbackDefinition('name: required; loaded under its filename');
	}
	const nameError = validateAgentName(rawName);
	if (nameError !== undefined) {
		if (options.plugin !== true) return {ok: false, reason: nameError};
		return fallbackDefinition(`${nameError}; loaded under its filename`);
	}
	const name = options.scopedPrefix !== undefined ? `${options.scopedPrefix}${rawName}` : rawName;
	const description = stringField(fields.description);
	if (description === undefined) return {ok: false, reason: 'description: required'};

	const definition: AgentDefinition = {
		name,
		description,
		systemPrompt: frontmatter.body,
		source: scope,
		path,
	};
	if (options.pluginName !== undefined) definition.plugin = options.pluginName;
	const tools = listField(fields.tools);
	const disallowed = listField(fields.disallowedTools);
	const skills = listField(fields.skills);
	if (tools !== undefined) definition.tools = tools;
	if (disallowed !== undefined) definition.disallowedTools = disallowed;
	const model = stringField(fields.model);
	const permissionMode = stringField(fields.permissionMode);
	const maxTurns = numberField(fields.maxTurns);
	const isolation = stringField(fields.isolation);
	const color = stringField(fields.color);
	const effort = stringField(fields.effort);
	if (model !== undefined) definition.model = model;
	// A plugin's permissionMode is ignored by contract; the host decides its policy.
	if (permissionMode !== undefined && options.plugin !== true) definition.permissionMode = permissionMode;
	if (maxTurns !== undefined) definition.maxTurns = maxTurns;
	if (skills !== undefined) definition.skills = skills;
	if (isolation !== undefined) definition.isolation = isolation;
	if (color !== undefined) definition.color = color;
	if (effort !== undefined) definition.effort = effort;
	const background = booleanField(fields.background);
	const omitClaudeMd = booleanField(fields.omitClaudeMd);
	const memoryValue = fields.memory;
	const memory = memoryValue === 'user' || memoryValue === 'project' || memoryValue === 'local' ? memoryValue : undefined;
	if (background !== undefined) definition.background = background;
	if (omitClaudeMd !== undefined) definition.omitClaudeMd = omitClaudeMd;
	if (memory !== undefined) definition.memory = memory;
	// A plugin ignores mcpServers, hooks, and initialPrompt by contract: the host
	// owns a plugin's servers, hooks, and first turn. autoCompactWindow and
	// experimental.cacheTtl still apply, because the agent still runs as a subagent.
	if (options.plugin !== true) {
		const mcpServers = parseMcpServers(fields.mcpServers);
		const hooks = parseHooks(fields.hooks);
		const initialPrompt = stringField(fields.initialPrompt);
		if (mcpServers !== undefined) definition.mcpServers = mcpServers;
		if (hooks !== undefined) definition.hooks = hooks;
		if (initialPrompt !== undefined) definition.initialPrompt = initialPrompt;
	}
	const autoCompactWindow = numberField(fields.autoCompactWindow);
	const cacheTtl = parseCacheTtl(fields.experimental);
	if (autoCompactWindow !== undefined) definition.autoCompactWindow = autoCompactWindow;
	if (cacheTtl !== undefined) definition.cacheTtl = cacheTtl;
	return {ok: true, definition};
}

// The T1 built-ins. `general-purpose` is the fallback and `claude` the catch-all.
// `explore` and `plan` mirror the host's reserved agents (read-only, one-shot) but
// live in this registry: the mod resolves them, so the host's copies are never
// shadowed through addAgent.
export const BUILTIN_AGENT_DEFINITIONS: AgentDefinition[] = [
	{
		name: 'explore',
		description: 'Read-only codebase exploration: find files, symbols, and call paths.',
		systemPrompt:
			'You are a read-only codebase explorer. Find the files, symbols, and call paths that answer the question. Do not edit anything. Report concrete paths with line numbers and a short summary of the call flow. Be quick by default; go deeper only when the task needs it.',
		source: 'builtin',
		path: '',
		tools: ['Read', 'Grep', 'Glob'],
		model: 'inherit',
		permissionMode: 'plan',
		readOnly: true,
		oneShot: true,
	},
	{
		name: 'plan',
		description: 'Read-only design: turn findings into an implementation plan.',
		systemPrompt:
			'You are a planning agent. Explore the codebase read-only, identify the constraints, then return a concrete implementation plan: the files to change, the ordered steps, the risks, and the tests. Do not implement.',
		source: 'builtin',
		path: '',
		tools: ['Read', 'Grep', 'Glob'],
		model: 'inherit',
		permissionMode: 'plan',
		readOnly: true,
		oneShot: true,
	},
	{
		name: 'general-purpose',
		description: 'General-purpose agent for multi-step tasks. The fallback when no subagent_type is given.',
		systemPrompt:
			'You are a general-purpose autonomous agent. You receive one self-contained task, complete it fully, and report back once at the end. Work autonomously: you cannot ask follow-up questions, so make reasonable assumptions and state them in the final report. Use the minimum set of tool calls that gets the job done correctly.',
		source: 'builtin',
		path: '',
	},
	{
		name: 'claude',
		description: 'Catch-all coding agent for a task that fits no specialized definition.',
		systemPrompt:
			'You are a general-purpose coding agent. Complete the assigned task end to end, then report what changed and how it was verified.',
		source: 'builtin',
		path: '',
	},
	{
		name: FORK_AGENT_TYPE,
		description:
			'Fork the current conversation: full history, the main system prompt, tools, model, permissions, and prompt cache. Use it when the task needs the context already in this session.',
		systemPrompt: '',
		source: 'builtin',
		path: '',
		fork: true,
	},
];

export function builtinAgentDefinitions(env: Record<string, string | undefined>): AgentDefinition[] {
	if (env.CLAUDE_AGENT_SDK_DISABLE_BUILTIN_AGENTS === '1' || env.BETTER_SUBAGENTS_DISABLE_BUILTIN_AGENTS === '1') return [];
	if (env.CLAUDE_CODE_DISABLE_EXPLORE_PLAN_AGENTS === '1') {
		return BUILTIN_AGENT_DEFINITIONS.filter(definition => definition.name !== 'explore' && definition.name !== 'plan');
	}
	return [...BUILTIN_AGENT_DEFINITIONS];
}

// A CLI definition is the same shape as frontmatter, in JSON. `color` and
// `experimental` are ignored there by contract; the object key is the name, so an
// inline `{reviewer: {...}}` and an array of `{name: ...}` both work.
function cliAgentDefinition(
	rawName: string,
	spec: Record<string, unknown>,
	sourcePath: string,
): {ok: true; definition: AgentDefinition} | {ok: false; reason: string} {
	const name = rawName.length > 0 ? rawName : (stringField(spec.name) ?? '');
	if (name.length === 0) return {ok: false, reason: 'name: required'};
	const nameError = validateAgentName(name);
	if (nameError !== undefined) return {ok: false, reason: nameError};
	const description = stringField(spec.description);
	if (description === undefined) return {ok: false, reason: 'description: required'};
	const prompt = typeof spec.prompt === 'string' ? spec.prompt : typeof spec.systemPrompt === 'string' ? spec.systemPrompt : '';

	const definition: AgentDefinition = {name, description, systemPrompt: prompt, source: 'cli', path: sourcePath};
	const tools = listField(spec.tools);
	const disallowed = listField(spec.disallowedTools);
	const skills = listField(spec.skills);
	if (tools !== undefined) definition.tools = tools;
	if (disallowed !== undefined) definition.disallowedTools = disallowed;
	if (skills !== undefined) definition.skills = skills;
	const model = stringField(spec.model);
	const permissionMode = stringField(spec.permissionMode);
	const maxTurns = numberField(spec.maxTurns);
	const isolation = stringField(spec.isolation);
	const effort = stringField(spec.effort);
	if (model !== undefined) definition.model = model;
	if (permissionMode !== undefined) definition.permissionMode = permissionMode;
	if (maxTurns !== undefined) definition.maxTurns = maxTurns;
	if (isolation !== undefined) definition.isolation = isolation;
	if (effort !== undefined) definition.effort = effort;
	const background = booleanField(spec.background);
	const omitClaudeMd = booleanField(spec.omitClaudeMd);
	if (background !== undefined) definition.background = background;
	if (omitClaudeMd !== undefined) definition.omitClaudeMd = omitClaudeMd;
	const mcpServers = parseMcpServers(spec.mcpServers);
	const hooks = parseHooks(spec.hooks);
	const initialPrompt = stringField(spec.initialPrompt);
	const autoCompactWindow = numberField(spec.autoCompactWindow);
	if (mcpServers !== undefined) definition.mcpServers = mcpServers;
	if (hooks !== undefined) definition.hooks = hooks;
	if (initialPrompt !== undefined) definition.initialPrompt = initialPrompt;
	if (autoCompactWindow !== undefined) definition.autoCompactWindow = autoCompactWindow;
	// `color` and `experimental` are ignored for a CLI definition by contract.
	return {ok: true, definition};
}

export function createAgentRegistry(deps: AgentRegistryDeps): AgentRegistry {
	let entries: AgentEntry[] = [];
	// Each (file, problem) is logged once per process, so a refresh from `/agents`
	// cannot flood the debug log with the same skip.
	const loggedSkips = new Set<string>();
	// A plugin file that loaded under its filename still carries its problem, which
	// `/doctor` reports. Keyed by the definition so a precedence skip does not lose it.
	const definitionWarnings = new WeakMap<AgentDefinition, string>();

	const managedDir = (): string => deps.managedDir ?? join(deps.home, '.commandcode', 'managed');

	function pushEntry(entry: AgentEntry): void {
		entries.push(entry);
		const problem = entry.warning ?? entry.reason;
		if (problem === undefined) return;
		const key = `${entry.path}\u0000${problem}`;
		if (loggedSkips.has(key)) return;
		loggedSkips.add(key);
		deps.log(`${entry.path.length > 0 ? entry.path : entry.name}: ${problem}`);
	}

	function scanFile(path: string, scope: AgentScope, options: AgentDefinitionOptions = {}): AgentDefinition | undefined {
		let source = '';
		try {
			source = deps.fs.readFile(path);
		} catch (error) {
			pushEntry({
				name: '',
				source: scope,
				path,
				valid: false,
				kind: 'invalid',
				reason: `unreadable: ${String((error as Error)?.message ?? error)}`,
			});
			return undefined;
		}
		const parsed = agentDefinitionFromSource(source, path, scope, options);
		if (!parsed.ok) {
			pushEntry({name: '', source: scope, path, valid: false, kind: 'invalid', reason: parsed.reason});
			return undefined;
		}
		if (parsed.warning !== undefined) definitionWarnings.set(parsed.definition, parsed.warning);
		return parsed.definition;
	}

	function pushDuplicate(name: string, scope: AgentScope, path: string, kept: string): void {
		pushEntry({
			name,
			source: scope,
			path,
			valid: false,
			kind: 'duplicate',
			duplicateOf: kept,
			reason: `duplicate name "${name}": ${kept} was read first; rename or remove this file`,
		});
	}

	// Recursive scan. Subdirectories never change a project or user identity: the
	// name comes from the frontmatter only. Bounded by depth and entry count. A
	// duplicate is an error only within one agents root; a farther root or a built-in
	// with the same name is ordinary precedence and is skipped silently.
	function scanDir(
		dir: string,
		scope: AgentScope,
		depth: number,
		found: AgentDefinition[],
		localNames: Map<string, string>,
	): void {
		if (depth > AGENT_SCAN_DEPTH_MAX || entries.length >= AGENT_SCAN_MAX || !deps.fs.exists(dir)) return;
		let names: string[] = [];
		try {
			names = deps.fs.readdir(dir).sort();
		} catch {
			return;
		}
		for (const name of names) {
			if (entries.length >= AGENT_SCAN_MAX) return;
			const full = join(dir, name);
			if (name.toLowerCase().endsWith('.md')) {
				const definition = scanFile(full, scope);
				if (definition === undefined) continue;
				const kept = localNames.get(definition.name);
				if (kept !== undefined) {
					pushDuplicate(definition.name, scope, full, kept);
					continue;
				}
				localNames.set(definition.name, full);
				found.push(definition);
				continue;
			}
			if (deps.fs.exists(full)) scanDir(full, scope, depth + 1, found, localNames);
		}
	}

	// A plugin's subdirectory is part of its identifier, so it is scanned with the
	// trail that builds `plugin:review:security`. Unlike a project, the plugin file
	// contract loads a nameless or unparseable file under its filename.
	function scanPluginDir(
		dir: string,
		pluginName: string,
		trail: string[],
		depth: number,
		found: AgentDefinition[],
		localNames: Map<string, string>,
	): void {
		if (depth > AGENT_SCAN_DEPTH_MAX || entries.length >= AGENT_SCAN_MAX || !deps.fs.exists(dir)) return;
		let children: string[] = [];
		try {
			children = deps.fs.readdir(dir).sort();
		} catch {
			return;
		}
		for (const child of children) {
			if (entries.length >= AGENT_SCAN_MAX) return;
			const full = join(dir, child);
			if (child.toLowerCase().endsWith('.md')) {
				const definition = scanFile(full, 'plugin', {
					plugin: true,
					pluginName,
					scopedPrefix: `${[pluginName, ...trail].join(':')}:`,
					fallbackName: child.replace(/\.md$/i, ''),
				});
				if (definition === undefined) continue;
				const kept = localNames.get(definition.name);
				if (kept !== undefined) {
					pushDuplicate(definition.name, 'plugin', full, kept);
					continue;
				}
				localNames.set(definition.name, full);
				found.push(definition);
				continue;
			}
			scanPluginDir(full, pluginName, [...trail, child], depth + 1, found, localNames);
		}
	}

	function scanPlugins(root: string, found: AgentDefinition[]): void {
		if (!deps.fs.exists(root)) return;
		let pluginNames: string[] = [];
		try {
			pluginNames = deps.fs.readdir(root).sort();
		} catch {
			return;
		}
		for (const pluginName of pluginNames) {
			// Presence under a plugin root is enablement; the host owns the plugin
			// manager. Only plugins that ship agents are scanned.
			const agentsDir = join(root, pluginName, 'agents');
			if (deps.fs.exists(agentsDir)) scanPluginDir(agentsDir, pluginName, [], 0, found, new Map());
		}
	}

	// The project bases from the working directory upward, closest first. Walk-up
	// stops at the repository root (the directory holding .git) or the filesystem
	// root, so a nested `.commandcode/agents` closest to the cwd wins.
	function projectBases(): string[] {
		const bases: string[] = [];
		let current = deps.cwd;
		for (let depth = 0; depth <= 64; depth += 1) {
			bases.push(current);
			if (deps.fs.exists(join(current, '.git'))) break;
			const parent = dirname(current);
			if (parent === current) break;
			current = parent;
		}
		return bases;
	}

	function pluginRoots(): string[] {
		const roots = [...(deps.pluginRoots ?? [])];
		roots.push(join(managedDir(), 'plugins'));
		for (const base of projectBases()) roots.push(join(base, '.commandcode', 'plugins'));
		roots.push(join(deps.home, '.commandcode', 'plugins'));
		return roots;
	}

	// `--mod-option agents=<inline JSON | JSON file path>`. A file is refused in an
	// interactive session: a definition a human cannot see at launch cannot be
	// reviewed, so the session must pass inline JSON instead.
	function scanCli(found: AgentDefinition[]): void {
		const value = (deps.cli?.value ?? '').trim();
		if (value.length === 0) return;
		let text = value;
		let sourcePath = 'cli';
		if (!value.startsWith('{') && !value.startsWith('[')) {
			const file = value.startsWith('@') ? value.slice(1) : value;
			if (deps.cli?.interactive === true) {
				pushEntry({
					name: '',
					source: 'cli',
					path: file,
					valid: false,
					kind: 'invalid',
					reason: 'an interactive session cannot load --mod-option agents from a file; pass inline JSON',
				});
				return;
			}
			try {
				text = deps.fs.readFile(file);
			} catch (error) {
				pushEntry({
					name: '',
					source: 'cli',
					path: file,
					valid: false,
					kind: 'invalid',
					reason: `unreadable: ${String((error as Error)?.message ?? error)}`,
				});
				return;
			}
			sourcePath = file;
		}
		let parsed: unknown;
		try {
			parsed = JSON.parse(text);
		} catch (error) {
			pushEntry({
				name: '',
				source: 'cli',
				path: sourcePath,
				valid: false,
				kind: 'invalid',
				reason: `agents JSON: ${String((error as Error)?.message ?? error)}`,
			});
			return;
		}
		const specs: {name: string; spec: Record<string, unknown>}[] = [];
		if (Array.isArray(parsed)) {
			for (const item of parsed) {
				if (item === null || typeof item !== 'object' || Array.isArray(item)) {
					pushEntry({
						name: '',
						source: 'cli',
						path: sourcePath,
						valid: false,
						kind: 'invalid',
						reason: 'agents JSON: every array item must be an object',
					});
					continue;
				}
				const record = item as Record<string, unknown>;
				specs.push({name: typeof record.name === 'string' ? record.name : '', spec: record});
			}
		} else if (parsed !== null && typeof parsed === 'object') {
			for (const [name, item] of Object.entries(parsed)) {
				if (item === null || typeof item !== 'object' || Array.isArray(item)) {
					pushEntry({
						name,
						source: 'cli',
						path: sourcePath,
						valid: false,
						kind: 'invalid',
						reason: 'agents JSON: every definition must be an object',
					});
					continue;
				}
				specs.push({name, spec: item as Record<string, unknown>});
			}
		} else {
			pushEntry({
				name: '',
				source: 'cli',
				path: sourcePath,
				valid: false,
				kind: 'invalid',
				reason: 'agents JSON: expected an object of definitions or an array',
			});
			return;
		}

		const localNames = new Map<string, string>();
		for (const {name, spec} of specs) {
			const parsedDefinition = cliAgentDefinition(name, spec, sourcePath);
			if (!parsedDefinition.ok) {
				pushEntry({
					name,
					source: 'cli',
					path: sourcePath,
					valid: false,
					kind: 'invalid',
					reason: parsedDefinition.reason,
				});
				continue;
			}
			const kept = localNames.get(parsedDefinition.definition.name);
			if (kept !== undefined) {
				pushDuplicate(parsedDefinition.definition.name, 'cli', sourcePath, kept);
				continue;
			}
			localNames.set(parsedDefinition.definition.name, sourcePath);
			found.push(parsedDefinition.definition);
		}
	}

	function refresh(): void {
		entries = [];
		const found: AgentDefinition[] = [];
		// Precedence order, highest first: a managed definition wins over CLI, CLI over
		// project, project over user, user over plugin, and every one over a built-in.
		scanDir(join(managedDir(), 'agents'), 'managed', 0, found, new Map());
		scanCli(found);
		for (const base of projectBases()) {
			scanDir(join(base, '.commandcode', 'agents'), 'project', 0, found, new Map());
		}
		for (const dir of deps.addDirs ?? []) {
			scanDir(join(dir, '.commandcode', 'agents'), 'project', 0, found, new Map());
		}
		scanDir(join(deps.home, '.commandcode', 'agents'), 'user', 0, found, new Map());
		for (const root of pluginRoots()) scanPlugins(root, found);
		found.push(...deps.builtins);

		// Precedence, not an error: the first loaded definition of a name wins. A
		// duplicate inside one agents root was already reported by scanDir.
		const seen = new Set<string>();
		for (const definition of found) {
			if (seen.has(definition.name)) continue;
			seen.add(definition.name);
			const warning = definitionWarnings.get(definition);
			entries.push({
				name: definition.name,
				source: definition.source,
				path: definition.path ?? '',
				valid: true,
				kind: 'valid',
				...(warning !== undefined ? {warning} : {}),
				definition,
			});
		}
	}

	refresh();
	return {
		entries: () => [...entries],
		list: () => entries.flatMap(entry => (entry.valid && entry.definition !== undefined ? [entry.definition] : [])),
		get: name => entries.find(entry => entry.valid && entry.definition?.name === name)?.definition,
		refresh,
	};
}

// ---------------------------------------------------------------------------
// Startup context and agent memory
//
// What a subagent sees at startup is a bundle, not the host's full prompt: the
// definition body, the environment, the task, the CLAUDE.md/AGENTS.md hierarchy,
// a git status snapshot, the full content of the named skills, a sibling roster,
// the output style, and the agent's own MEMORY.md. The bundle is built in the
// parent from the injected filesystem, so the omit rules live in one function
// instead of in conditionals spread through spawn. A subagent never inherits the
// parent's auto memory; `memory` gives it a directory and a MEMORY.md of its own.
// ---------------------------------------------------------------------------

export type AgentMemoryScope = 'user' | 'project' | 'local';

const MEMORY_INDEX_FILE = 'MEMORY.md';
const MEMORY_INDEX_LINES_MAX = 200;
const MEMORY_INDEX_BYTES_MAX = 25 * 1024;
const GIT_STATUS_MAX_CHARS = 2000;
const STARTUP_WALK_DEPTH_MAX = 8;
// The window the ACP session is told it has. A family alias resolves to its
// documented window; an explicit override wins, and an unknown model gets the
// common default rather than an unbounded number.
const CONTEXT_WINDOW_DEFAULT = 200000;
const CONTEXT_WINDOW_MAX = 10 * 1000 * 1000;
const CONTEXT_WINDOW_FAMILIES: ReadonlyArray<readonly [RegExp, number]> = [
	[/\[1m\]|-1m\b/i, 1000000],
	[/opus|sonnet|haiku|fable/i, 200000],
];
const EXPLORE_PLAN_NAMES: Readonly<Record<string, true>> = {explore: true, plan: true};

export interface StartupFile {
	readonly path: string;
	readonly content: string;
}

export interface StartupSkill extends StartupFile {
	readonly name: string;
}

export interface StartupMemory {
	readonly scope: AgentMemoryScope;
	readonly dir: string;
	readonly path: string;
	readonly content: string;
	readonly lines: number;
	readonly truncated: boolean;
}

export interface StartupBundle {
	readonly systemPrompt: string;
	readonly environmentText: string;
	readonly task: string;
	readonly claudeMd: StartupFile[];
	readonly agentsMd: StartupFile[];
	readonly gitStatus?: string;
	readonly skills: StartupSkill[];
	readonly roster?: string[];
	readonly outputStyle?: string;
	readonly memory?: StartupMemory;
	readonly contextWindow: number;
	/** A subagent never inherits the parent's auto memory. */
	readonly parentMemory: false;
	/** The rendered context the worker receives. */
	readonly text: string;
}

/** The definition fields startup reads. Structural, so AgentDefinition fits. */
export interface StartupDefinition {
	readonly name?: string;
	readonly systemPrompt?: string;
	readonly skills?: readonly string[];
	readonly memory?: AgentMemoryScope;
	readonly omitClaudeMd?: boolean;
	/** `managed` skips the managed policy, matching the spec's managed agents. */
	readonly scope?: string;
	readonly outputStyle?: string;
}

export interface StartupOptions {
	readonly cwd: string;
	readonly home: string;
	readonly fs: SpoolFs;
	readonly env?: Record<string, string | undefined>;
	readonly definition?: StartupDefinition;
	readonly agentType?: string;
	readonly prompt?: string;
	readonly model?: string;
	readonly outputStyle?: string;
	readonly roster?: readonly string[];
	/** Files the agent works with; their subdirectory CLAUDE.md still loads. */
	readonly paths?: readonly string[];
	/** A snapshot already taken by the caller, or undefined for no snapshot. */
	readonly gitStatus?: string;
	readonly now?: number;
	readonly platform?: string;
}

function readStartupFile(fs: SpoolFs, path: string): string | undefined {
	try {
		return fs.readFile(path);
	} catch {
		return undefined;
	}
}

function startupFile(fs: SpoolFs, path: string): StartupFile | undefined {
	const content = readStartupFile(fs, path);
	return content === undefined ? undefined : {path, content};
}

// Project discovery walks up from the working directory and renders outermost
// first, so broad instructions come before the nearest ones.
function projectDirsUpward(cwd: string): string[] {
	const dirs: string[] = [];
	let current = cwd;
	for (let depth = 0; depth < STARTUP_WALK_DEPTH_MAX; depth += 1) {
		dirs.push(current);
		const parent = dirname(current);
		if (parent === current) break;
		current = parent;
	}
	return dirs.reverse();
}

// A path-scoped instruction file is loaded even when the hierarchy is omitted:
// the agent is working with files in that directory, so the rule applies.
function subdirectoryClaudeMd(fs: SpoolFs, cwd: string, paths: readonly string[]): StartupFile[] {
	const seen = new Set<string>();
	const files: StartupFile[] = [];
	for (const path of paths) {
		const absolute = isAbsolute(path) ? path : join(cwd, path);
		let dir = dirname(absolute);
		for (let depth = 0; depth < STARTUP_WALK_DEPTH_MAX; depth += 1) {
			if (dir === cwd || dir === dirname(dir) || !dir.startsWith(cwd)) break;
			const filePath = join(dir, 'CLAUDE.md');
			if (seen.has(filePath)) break;
			seen.add(filePath);
			const file = startupFile(fs, filePath);
			if (file !== undefined) files.push(file);
			dir = dirname(dir);
		}
	}
	return files;
}

export function claudeMdHierarchy(options: StartupOptions): {claudeMd: StartupFile[]; agentsMd: StartupFile[]} {
	const env = options.env ?? {};
	const fs = options.fs;
	const definition = options.definition;
	const agentType = (options.agentType ?? definition?.name ?? '').toLowerCase();
	const claudeMd: StartupFile[] = [];
	const agentsMd: StartupFile[] = [];
	// Explore and Plan start without project instructions, but a path-scoped
	// CLAUDE.md still loads because the agent works with files there.
	const skipHierarchy = EXPLORE_PLAN_NAMES[agentType] === true;
	// Managed policy loads even under omitClaudeMd, except for a managed agent.
	if (!skipHierarchy && definition?.scope !== 'managed') {
		const managedPath =
			env.BETTER_SUBAGENTS_MANAGED_CLAUDE_MD ?? join(options.home, '.commandcode', 'managed', 'CLAUDE.md');
		const managed = startupFile(fs, managedPath);
		if (managed !== undefined) claudeMd.push(managed);
	}
	if (!skipHierarchy && definition?.omitClaudeMd !== true) {
		const user = startupFile(
			fs,
			env.BETTER_SUBAGENTS_USER_CLAUDE_MD ?? join(options.home, '.commandcode', 'CLAUDE.md'),
		);
		if (user !== undefined) claudeMd.push(user);
		for (const dir of projectDirsUpward(options.cwd)) {
			const project = startupFile(fs, join(dir, 'CLAUDE.md'));
			if (project !== undefined) claudeMd.push(project);
			const local = startupFile(fs, join(dir, 'CLAUDE.local.md'));
			if (local !== undefined) claudeMd.push(local);
		}
		// AGENTS.md is project instructions too, so it follows the same omit rule.
		for (const dir of projectDirsUpward(options.cwd)) {
			const agents = startupFile(fs, join(dir, 'AGENTS.md'));
			if (agents !== undefined) agentsMd.push(agents);
		}
	}
	claudeMd.push(...subdirectoryClaudeMd(fs, options.cwd, options.paths ?? []));
	return {claudeMd, agentsMd};
}

export function skillFileFor(
	name: string,
	options: {cwd: string; home: string; fs: SpoolFs},
): string | undefined {
	if (name.length === 0) return undefined;
	const candidates: string[] = [];
	for (const dir of projectDirsUpward(options.cwd)) {
		candidates.push(join(dir, '.commandcode', 'skills', name, 'SKILL.md'));
		candidates.push(join(dir, '.claude', 'skills', name, 'SKILL.md'));
	}
	candidates.push(join(options.home, '.commandcode', 'skills', name, 'SKILL.md'));
	candidates.push(join(options.home, '.claude', 'skills', name, 'SKILL.md'));
	for (const candidate of candidates) {
		if (readStartupFile(options.fs, candidate) !== undefined) return candidate;
	}
	return undefined;
}

export function memoryDirFor(scope: AgentMemoryScope, name: string, options: {cwd: string; home: string}): string {
	const root =
		scope === 'user'
			? join(options.home, '.commandcode', 'agent-memory')
			: scope === 'project'
				? join(options.cwd, '.commandcode', 'agent-memory')
				: join(options.cwd, '.commandcode', 'agent-memory-local');
	return join(root, name);
}

// The first 200 lines or 25KB of MEMORY.md, whichever comes first. Bytes, not
// UTF-16 code units: the bound is on what is injected into the context.
export function readMemoryIndex(
	path: string,
	fs: SpoolFs,
): {content: string; lines: number; truncated: boolean} {
	let source: string;
	try {
		source = fs.readFile(path);
	} catch {
		return {content: '', lines: 0, truncated: false};
	}
	const allLines = source.split('\n');
	// A trailing newline is not a line of content, so a 200-line file that ends
	// with one is not reported as truncated.
	if (allLines.length > 0 && allLines[allLines.length - 1] === '') allLines.pop();
	const lineBound = allLines.slice(0, MEMORY_INDEX_LINES_MAX).join('\n');
	let content = lineBound;
	let truncatedByBytes = false;
	if (Buffer.byteLength(lineBound, 'utf8') > MEMORY_INDEX_BYTES_MAX) {
		const buffer = Buffer.from(lineBound, 'utf8').subarray(0, MEMORY_INDEX_BYTES_MAX);
		content = buffer.toString('utf8').replace(/\uFFFD$/, '');
		truncatedByBytes = true;
	}
	const truncatedByLines = allLines.length > MEMORY_INDEX_LINES_MAX;
	return {
		content,
		lines: content.length === 0 ? 0 : content.split('\n').length,
		truncated: truncatedByLines || truncatedByBytes,
	};
}

// MEMORY.md is the agent's own memory. The parent's auto memory never follows a
// non-fork subagent, so this is the only memory that reaches the bundle.
function startupMemory(options: StartupOptions): StartupMemory | undefined {
	const scope = options.definition?.memory;
	const name = options.definition?.name;
	if (scope === undefined || name === undefined || name.length === 0) return undefined;
	const dir = memoryDirFor(scope, name, {cwd: options.cwd, home: options.home});
	try {
		options.fs.mkdir(dir);
	} catch {
		// A read-only location must not fail the spawn: the agent still starts.
	}
	const path = join(dir, MEMORY_INDEX_FILE);
	const index = readMemoryIndex(path, options.fs);
	return {scope, dir, path, content: index.content, lines: index.lines, truncated: index.truncated};
}

export function contextWindowForModel(
	model: string | undefined,
	env: Record<string, string | undefined> = {},
): number {
	const override = Number(env.BETTER_SUBAGENTS_CONTEXT_WINDOW ?? env.CLAUDE_CODE_CONTEXT_WINDOW);
	if (Number.isFinite(override) && override >= 1000) return Math.min(CONTEXT_WINDOW_MAX, Math.round(override));
	const value = (model ?? '').toLowerCase();
	for (const [pattern, window] of CONTEXT_WINDOW_FAMILIES) {
		if (pattern.test(value)) return window;
	}
	return CONTEXT_WINDOW_DEFAULT;
}

// A memory definition must be able to read and write its own MEMORY.md.
export function withMemoryTools(
	tools: readonly string[] | undefined,
	memory: AgentMemoryScope | undefined,
): string[] | undefined {
	if (memory === undefined) return tools === undefined ? undefined : [...tools];
	const result = tools === undefined ? [] : [...tools];
	for (const tool of ['Read', 'Write', 'Edit']) if (!result.includes(tool)) result.push(tool);
	return result;
}

export function gitStatusSnapshot(runtime: Pick<Runtime, 'run'>, cwd: string): string | undefined {
	const run = runtime.run;
	if (typeof run !== 'function') return undefined;
	try {
		const result = run('git', ['status', '--short', '--branch'], {cwd});
		return result.ok ? result.stdout : undefined;
	} catch {
		return undefined;
	}
}

function trimGitStatus(status: string | undefined): string | undefined {
	if (status === undefined) return undefined;
	const text = status.replace(/\s+$/, '');
	if (text.length === 0) return undefined;
	return text.length > GIT_STATUS_MAX_CHARS ? `${text.slice(0, GIT_STATUS_MAX_CHARS)}\n(truncated)` : text;
}

function renderStartup(bundle: Omit<StartupBundle, 'text'>): string {
	const sections: string[] = [bundle.environmentText];
	if (bundle.claudeMd.length > 0) {
		const files = bundle.claudeMd.map(file => `## ${file.path}\n${file.content}`);
		sections.push(['# Project instructions (CLAUDE.md)', ...files].join('\n\n'));
	}
	if (bundle.agentsMd.length > 0) {
		const files = bundle.agentsMd.map(file => `## ${file.path}\n${file.content}`);
		sections.push(['# AGENTS.md', ...files].join('\n\n'));
	}
	if (bundle.gitStatus !== undefined) sections.push(`# Git status\n${bundle.gitStatus}`);
	if (bundle.skills.length > 0) {
		const skills = bundle.skills.map(skill => `## ${skill.name} (${skill.path})\n${skill.content}`);
		sections.push(['# Skills (full content)', ...skills].join('\n\n'));
	}
	if (bundle.roster !== undefined && bundle.roster.length > 0) {
		sections.push(`# Sibling agents\n${bundle.roster.map(name => `- ${name}`).join('\n')}`);
	}
	if (bundle.outputStyle !== undefined) sections.push(`# Output style\n${bundle.outputStyle}`);
	if (bundle.memory !== undefined) {
		const bound = bundle.memory.truncated ? ' (truncated to the first 200 lines or 25KB)' : '';
		sections.push(`# Agent memory\n${bundle.memory.path}${bound}\n${bundle.memory.content}`);
	}
	return sections.join('\n\n');
}

export function buildStartupBundle(options: StartupOptions): StartupBundle {
	const env = options.env ?? {};
	const definition = options.definition;
	const agentType = (options.agentType ?? definition?.name ?? '').toLowerCase();
	const contextWindow = contextWindowForModel(options.model, env);
	const {claudeMd, agentsMd} = claudeMdHierarchy(options);
	// Explore and Plan skip the git status snapshot along with the instructions.
	const gitStatus = EXPLORE_PLAN_NAMES[agentType] === true ? undefined : trimGitStatus(options.gitStatus);
	// The full content of every named skill; an unlisted skill stays invocable
	// through the Skill tool and is not preloaded here.
	const skills: StartupSkill[] = [];
	for (const name of definition?.skills ?? []) {
		const path = skillFileFor(name, options);
		const content = path === undefined ? undefined : readStartupFile(options.fs, path);
		if (path !== undefined && content !== undefined) skills.push({name, path, content});
	}
	const roster = options.roster === undefined ? undefined : [...options.roster];
	const outputStyle = options.outputStyle ?? definition?.outputStyle;
	const memory = startupMemory(options);
	const date = new Date(options.now ?? Date.now()).toISOString().slice(0, 10);
	const environment = [
		'# Environment',
		`working directory: ${options.cwd}`,
		`platform: ${options.platform ?? 'unknown'}`,
		`date: ${date}`,
	];
	if (options.model !== undefined) environment.push(`model: ${options.model}`);
	environment.push(`context window: ${contextWindow} tokens`);
	const base: Omit<StartupBundle, 'text'> = {
		systemPrompt: definition?.systemPrompt ?? '',
		environmentText: environment.join('\n'),
		task: options.prompt ?? '',
		claudeMd,
		agentsMd,
		...(gitStatus !== undefined ? {gitStatus} : {}),
		skills,
		...(roster !== undefined ? {roster} : {}),
		...(outputStyle !== undefined ? {outputStyle} : {}),
		...(memory !== undefined ? {memory} : {}),
		contextWindow,
		parentMemory: false,
	};
	return {...base, text: renderStartup(base)};
}

// ---------------------------------------------------------------------------
// Worker spec
//
// The worker is one detached Node process. `buildSpec` is the only place a spec
// is shaped and `buildArgs` the only place an argv is built, so replacing the
// transport (the old `cmd -p` argv, now ACP) touches one function. The spec is
// passed in the environment: no shell interpolation.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Live messaging: the roster
//
// A named spawn is addressable. Every agent that has the send tool starts with a
// snapshot of its siblings, so it can message them without a discovery call. The
// snapshot lists main and the OTHER named agents only: an unnamed job has no
// address, and an agent is never its own peer. The newest record for a name wins,
// so a resumed agent replaces the job it continued.
// ---------------------------------------------------------------------------

export interface RosterEntry {
	name?: string;
	state?: string;
	jobId?: string;
}

export function buildRoster(entries: readonly RosterEntry[], selfName?: string): string {
	const named = new Map<string, RosterEntry>();
	for (const entry of entries) {
		const name = typeof entry.name === 'string' ? entry.name.trim() : '';
		if (name.length === 0 || name === selfName) continue;
		if (!named.has(name)) named.set(name, entry);
	}
	if (named.size === 0) return '';
	const lines = [
		'SIBLING ROSTER (snapshot taken at start; address a peer with subagent_send({to, message}))',
		`${ROSTER_MAIN} — the conversation that started you`,
	];
	for (const [name, entry] of named) {
		const state = typeof entry.state === 'string' && entry.state.length > 0 ? ` (${entry.state})` : '';
		const job = typeof entry.jobId === 'string' && entry.jobId.length > 0 ? ` job ${entry.jobId}` : '';
		lines.push(`${name}${state}${job}`);
	}
	return lines.join('\n');
}

// ListAgents is a foreground tool: a foreground agent inherits it when cross-session
// messaging is on; a background agent does not keep it. One pure predicate, so the
// tool registration and the tests cannot disagree about visibility.
export function listAgentsVisible(options: {background?: boolean; crossSessionMessaging?: boolean}): boolean {
	if (options.crossSessionMessaging === false) return false;
	return options.background !== true;
}

export interface WorkerSpecInput {
	id: string;
	label: string;
	kind: 'command' | 'agent' | 'fork';
	owner?: string;
	cwd: string;
	logPath: string;
	inboxDir: string;
	jobsDir: string;
	startedAt: number;
	tailMax: number;
	bin: string;
	sessionId?: string;
	/** This job's nesting level: the caller's depth plus one. */
	depth?: number;
	agentType?: string;
	/** The durable resume handle; absent for a one-shot agent. */
	agentId?: string;
	/** Where the worker appends the persisted conversation. */
	transcriptPath?: string;
	/** A fork: inherit the main conversation's prompt, tools, model, and cache. */
	fork?: boolean;
	/** The main conversation the fork copies; absent for a non-fork. */
	mainSessionId?: string;
	/** The session name the host assigns to the new fork. */
	sessionName?: string;
	/** `shared` for a fork, `own` for a non-fork. */
	promptCache?: 'shared' | 'own';
	/** The team a teammate belongs to. */
	teamName?: string;
	systemPrompt?: string;
	model?: string;
	effort?: string;
	/** The resolved tool capabilities; the worker logs them and never widens them. */
	tools?: string[];
	disallowedTools?: string[];
	/** The MCP servers scoped to this agent; `session/new` receives exactly these. */
	mcpServers?: AgentMcpServer[];
	/** The resolved auto-compact window, never larger than inherited. */
	autoCompactWindow?: number;
	/** `experimental.cacheTtl`: `5m` or `1h`. ACP has no option; the adapter logs it. */
	cacheTtl?: CacheTtl;
	permissionMode?: string;
	/** how long an inline prompt waits before settling waiting-input; default 30s */
	permissionTimeoutMs?: number;
	maxTurns?: number;
	timeoutMs?: number;
	resumeSession?: string;
	prompt?: string;
	command?: string;
	/** The resolved execution policy; the worker and ListAgents gating read it. */
	background?: boolean;
	/** Foreground runs answer permission prompts inline; background runs settle. */
	interactive?: boolean;
	controlDir?: string;
	/** Written by the mod when the ACP child loaded it; proves the prompt hook is live. */
	promptMarkerPath?: string;
	/** The worktree this job runs in, when isolation created one. */
	worktree?: string;
	/** Checkout roots this job must never enter. */
	mainCheckouts?: string[];
	/** Addressable name (`subagent_spawn.name`); absent for an anonymous agent. */
	name?: string;
	/** Rendered sibling snapshot (main + other named agents), taken at spawn. */
	roster?: string;
	/** Roster names, for ListAgents and for validating a worker's `to`. */
	rosterNames?: string[];
	/** Accept live messages after the first turn instead of exiting. */
	live?: boolean;
	/** How long an idle live agent waits for a message before it exits. */
	idleMs?: number;
	/** Poll interval for the control channel. */
	controlPollMs?: number;
	/** The rendered startup bundle: CLAUDE.md, git status, skills, memory, roster. */
	startup?: string;
}

export interface WorkerSpec extends WorkerSpecInput {
	args: string[];
}

// ACP is the accepted worker transport: `cmd --mod <file> acp` loads this mod in
// the worker so appendSystemPrompt can return the definition body, and the adapter
// speaks JSON-RPC over stdout/stdin. Shell jobs keep the /bin/sh argv.
export function buildArgs(spec: WorkerSpec, modPath: string): string[] {
	if (spec.kind === 'command') return ['-c', String(spec.command ?? '')];
	if (spec.kind === 'fork') {
		// ACP has no fork primitive; the host CLI fork copies the session and its
		// prompt cache. `--output-format json` is what makes the new session id
		// readable from the result line, so a follow-up can resume it.
		const prompt = String(spec.prompt ?? '');
		if (typeof spec.resumeSession === 'string' && spec.resumeSession.length > 0) {
			return ['--resume', spec.resumeSession, '-p', prompt, '--output-format', 'json'];
		}
		const name = String(spec.sessionName ?? `subagent-${spec.id}`);
		return [
			'--fork-session',
			'--resume',
			String(spec.mainSessionId ?? ''),
			'-n',
			name,
			'-p',
			prompt,
			'--output-format',
			'json',
		];
	}
	// maxTurns is a host cap. The flag is shared with -p mode; ACP may ignore it,
	// but the stop reason is what the lifecycle contract reads, and a host that
	// honors it stops with max_turn_requests, which the worker marks partial.
	const maxTurns = typeof spec.maxTurns === 'number' && spec.maxTurns > 0 ? String(spec.maxTurns) : '';
	return [...(maxTurns.length > 0 ? ['--max-turns', maxTurns] : []), '--mod', modPath, 'acp'];
}

export function buildSpec(input: WorkerSpecInput, modPath: string): WorkerSpec {
	const spec: WorkerSpec = {...input, args: []};
	spec.args = buildArgs(spec, modPath);
	return spec;
}

// ---------------------------------------------------------------------------
// Worktree isolation
//
// `isolation: worktree` runs one job in its own `git worktree`, branched from the
// repository's default branch - never the caller's HEAD. Creation and cleanup are
// parent-owned and every git call goes through the runtime seam, so a fake git
// makes the whole lifecycle drivable. Finalizing is idempotent: a drain, an
// onStop claim, and a restart reconcile can each ask without removing twice.
// ---------------------------------------------------------------------------

export interface GitResult {
	readonly code: number;
	readonly stdout: string;
	readonly stderr: string;
}

/** The git half of the runtime seam. */
export interface GitSeam {
	git(args: string[], cwd: string): GitResult;
}

/**
 * git over the runtime's one bounded `run`. The worktree code speaks GitResult, so
 * the OS call and its fake stay one seam.
 */
export function gitSeam(runtime: Pick<Runtime, 'run'>): GitSeam {
	return {
		git(args, cwd) {
			const reply = runtime.run?.('git', args, {cwd, timeoutMs: 30_000});
			if (reply === undefined) return {code: 1, stdout: '', stderr: 'git is not available'};
			return {
				code: typeof reply.code === 'number' ? reply.code : reply.ok ? 0 : 1,
				stdout: String(reply.stdout ?? ''),
				stderr: String(reply.stderr ?? ''),
			};
		},
	};
}

/** The filesystem calls creation needs. */
export interface WorktreeSeam extends GitSeam {
	readonly fs: {mkdir(dir: string): void};
}

export type IsolationResult =
	| {readonly ok: true; readonly path: string; readonly base: string; readonly mainCheckouts: string[]}
	| {readonly ok: false; readonly error: string};

export interface WorktreeDecision {
	readonly kept: boolean;
	readonly path: string;
	readonly error?: string;
}

export interface IsolationRoots {
	/** The checkout a command must stay inside. */
	readonly worktree: string;
	/** Checkouts a command must never enter: the launch repo, plus the main checkout. */
	readonly mainCheckouts: readonly string[];
}

export interface IsolationSpec {
	readonly worktree?: string;
	readonly mainCheckouts?: string[];
}

function isInside(parent: string, child: string): boolean {
	const root = resolve(parent);
	const target = resolve(child);
	return target === root || target.startsWith(root.endsWith(sep) ? root : root + sep);
}

function firstLine(value: string): string {
	return value.trim().split('\n')[0].trim();
}

// The repository containing `cwd`, and the main checkout when `cwd` is a linked
// worktree. Both are off limits to a command: `git worktree` puts a linked
// worktree's `.git` file under the main checkout's common dir, which is how the
// second root is discovered.
export function mainCheckoutRoots(seam: GitSeam, cwd: string): string[] {
	const top = seam.git(['rev-parse', '--show-toplevel'], cwd);
	if (top.code !== 0) return [];
	const topLevel = firstLine(top.stdout);
	if (topLevel.length === 0) return [];
	const roots = [topLevel];
	const common = seam.git(['rev-parse', '--path-format=absolute', '--git-common-dir'], topLevel);
	// Older git has no --path-format; the plain answer is relative to the top level.
	let commonDir = '';
	if (common.code === 0) {
		commonDir = firstLine(common.stdout);
	} else {
		const plain = seam.git(['rev-parse', '--git-common-dir'], topLevel);
		if (plain.code === 0) commonDir = resolve(topLevel, firstLine(plain.stdout));
	}
	// `<top>/.git` is the main checkout's own directory. Anything else is a linked
	// worktree's file; the main checkout is the common dir's parent.
	if (commonDir.length > 0 && !isInside(topLevel, commonDir)) roots.push(dirname(commonDir));
	return roots;
}

// The default branch, never the caller's HEAD: origin's HEAD first, then the two
// conventional local names. Absent all three, isolation fails rather than quietly
// branching from wherever the parent happened to be.
export function defaultBranch(seam: GitSeam, cwd: string): string | undefined {
	const candidates: string[] = [];
	const head = seam.git(['symbolic-ref', '--short', 'refs/remotes/origin/HEAD'], cwd);
	if (head.code === 0) {
		const name = firstLine(head.stdout);
		if (name.length > 0) candidates.push(name);
	}
	candidates.push('main', 'master');
	for (const candidate of candidates) {
		if (seam.git(['rev-parse', '--verify', '--quiet', candidate], cwd).code === 0) return candidate;
	}
	return undefined;
}

export function createJobWorktree(seam: WorktreeSeam, opts: {cwd: string; path: string}): IsolationResult {
	const roots = mainCheckoutRoots(seam, opts.cwd);
	if (roots.length === 0) {
		return {ok: false, error: 'isolation: worktree needs a git repository (git rev-parse --show-toplevel failed)'};
	}
	const base = defaultBranch(seam, opts.cwd);
	if (base === undefined) {
		return {
			ok: false,
			error: 'isolation: worktree cannot resolve the default branch (no origin/HEAD, main, or master)',
		};
	}
	try {
		seam.fs.mkdir(dirname(opts.path));
	} catch (error) {
		const message = String((error as Error)?.message ?? error);
		return {ok: false, error: `isolation: worktree cannot create ${dirname(opts.path)}: ${message}`};
	}
	const added = seam.git(['worktree', 'add', '--detach', opts.path, base], roots[0]);
	if (added.code !== 0) {
		const reason = firstLine(added.stderr) || firstLine(added.stdout) || `exit ${added.code}`;
		return {ok: false, error: `isolation: worktree add failed: ${reason}`};
	}
	return {ok: true, path: opts.path, base, mainCheckouts: roots};
}

// `undefined` is "git could not tell", which must keep the worktree: deleting on
// uncertainty would discard a user's work.
export function worktreeChanged(seam: GitSeam, path: string): boolean | undefined {
	const status = seam.git(['status', '--porcelain'], path);
	if (status.code !== 0) return undefined;
	return status.stdout.trim().length > 0;
}

/**
 * The one cleanup decision for a finished job. Unchanged: remove. Changed (or not
 * knowable): keep and report the path. A record already finalized returns
 * `undefined`, so the drain, the stop hook and the restart reconcile cannot act
 * twice.
 */
export function finalizeWorktree(
	seam: GitSeam,
	record: {worktree?: string; worktreeRepo?: string; worktreeFinalized?: boolean},
): WorktreeDecision | undefined {
	if (typeof record.worktree !== 'string' || record.worktree.length === 0 || record.worktreeFinalized === true) {
		return undefined;
	}
	const path = record.worktree;
	const changed = worktreeChanged(seam, path);
	if (changed === undefined) return {kept: true, path, error: 'git status failed; kept the worktree'};
	if (changed) return {kept: true, path};
	// Run in the repository root: `git worktree remove` from inside the worktree it
	// is deleting can trip over its own cwd.
	const removed = seam.git(['worktree', 'remove', '--force', path], record.worktreeRepo ?? dirname(path));
	if (removed.code !== 0) {
		return {kept: true, path, error: `git worktree remove failed: ${firstLine(removed.stderr) || `exit ${removed.code}`}`};
	}
	return {kept: false, path};
}

// A session is isolated when it is a subagent with a worktree, or when the session
// itself runs in a linked worktree. A normal checkout session is not.
export function isolationRoots(seam: GitSeam, cwd: string, spec?: IsolationSpec): IsolationRoots | undefined {
	const supplied = (spec?.mainCheckouts ?? []).filter((root) => typeof root === 'string' && root.length > 0);
	const mainCheckouts = supplied.length > 0 ? supplied : mainCheckoutRoots(seam, cwd);
	const worktree = typeof spec?.worktree === 'string' && spec.worktree.length > 0 ? spec.worktree : undefined;
	if (worktree !== undefined) {
		// A spool configured inside the repository puts the worktree under a forbidden
		// root. The root stays forbidden; the guard allows the worktree subtree inside it.
		return {worktree, mainCheckouts};
	}
	// A linked worktree reports two roots; the session's own checkout is the first.
	if (mainCheckouts.length > 1 && isInside(mainCheckouts[0], cwd)) {
		return {worktree: mainCheckouts[0], mainCheckouts};
	}
	return undefined;
}

// The shell tools the host exposes. Monitor is bash-shaped: it gets both checks.
const SHELL_TOOL_NAMES: Record<string, true> = {
	bash: true,
	shell: true,
	sh: true,
	monitor: true,
	powershell: true,
	pwsh: true,
};

// Redirect targets that discard output rather than write a file.
const NULL_DEVICES: Record<string, true> = {
	'/dev/null': true,
	'/dev/stdout': true,
	'/dev/stderr': true,
	'/dev/tty': true,
};

function namedCwd(input: Record<string, unknown>): string | undefined {
	for (const key of ['cwd', 'workdir', 'working_directory', 'workingDirectory']) {
		const value = input[key];
		if (typeof value === 'string' && value.trim().length > 0) return value.trim();
	}
	return undefined;
}

// Command-shape rules for Bash and Monitor. PowerShell deliberately gets only the
// working-directory check: parsing another shell's grammar here would be guessing.
export function commandEscapesWorktree(command: string, roots: IsolationRoots): string | undefined {
	if (/\b(?:GIT_DIR|GIT_WORK_TREE|GIT_COMMON_DIR)\s*=/.test(command)) {
		return 'the command overrides the git directory or work tree';
	}
	if (/\bgit\b[^\n;&|]*\s--prefix[=\s]/.test(command)) return 'the command overrides git --prefix';
	// Naming the worktree itself is not naming the main checkout, even when the spool
	// puts the worktree inside the repository.
	const outsideWorktree = command.split(roots.worktree).join('');
	for (const main of roots.mainCheckouts) {
		if (outsideWorktree.includes(main)) return `the command names the main checkout (${main})`;
	}
	const targets: {kind: string; target: string}[] = [];
	for (const match of command.matchAll(/(?:^|[;&|]\s*|\s)(?:cd|pushd|set-location|sl)\s+(['"]?)([^\s'";|&]+)\1/gi)) {
		targets.push({kind: 'cd', target: match[2]});
	}
	for (const match of command.matchAll(/(?:\d?|&)>>?\|?\s*(['"]?)([^\s'";|&<>]+)\1/g)) {
		targets.push({kind: 'redirect', target: match[2]});
	}
	for (const match of command.matchAll(/\bgit\b[^\n;&|]*(?:-C\s+|--work-tree[=\s]+|--git-dir[=\s]+)(['"]?)([^\s'";|&]+)\1/g)) {
		targets.push({kind: 'git path', target: match[2]});
	}
	for (const {kind, target} of targets) {
		if (kind === 'cd' && (target === '-' || target.startsWith('~'))) {
			return `a cd to ${target} leaves the worktree`;
		}
		if (target.startsWith('$')) continue;
		const resolved = resolve(roots.worktree, target);
		if (kind === 'cd' && !isInside(roots.worktree, resolved)) {
			return `a cd to ${target} leaves the worktree (${resolved})`;
		}
		// Discarding output is not writing a file; every other redirect target must stay
		// inside the worktree.
		if (kind === 'redirect' && NULL_DEVICES[resolved] === true) continue;
		if (kind !== 'cd' && !isInside(roots.worktree, resolved)) {
			return `a ${kind} to ${target} escapes the worktree (${resolved})`;
		}
	}
	return undefined;
}

/**
 * The one tool guard. `undefined` means "allowed". The block reason becomes the
 * tool result the model sees, so it names the worktree and the reason.
 */
export function guardToolCall(
	toolName: string,
	input: Record<string, unknown>,
	roots: IsolationRoots,
	toolCwd: string = roots.worktree,
): {block: true; reason: string} | undefined {
	const name = toolName.toLowerCase();
	if (SHELL_TOOL_NAMES[name] !== true) return undefined;
	const named = namedCwd(input);
	const cwd = named === undefined ? toolCwd : resolve(toolCwd, named);
	const insideWorktree = isInside(roots.worktree, cwd);
	// The worktree itself can sit inside the repository (a custom subagent_dir). The
	// worktree subtree is the allowed area; the rest of that checkout is not.
	if (!insideWorktree) {
		for (const main of roots.mainCheckouts) {
			if (!isInside(main, cwd)) continue;
			return {
				block: true,
				reason:
					`Blocked: this subagent is isolated in its worktree (${roots.worktree}); ` +
					`a command in the main checkout (${cwd}) is not allowed. Run it inside the worktree.`,
			};
		}
	}
	if (name === 'powershell' || name === 'pwsh') return undefined;
	const command = [input.command, input.script, input.cmd].find((value) => typeof value === 'string') as
		| string
		| undefined;
	if (command === undefined) return undefined;
	const escape = commandEscapesWorktree(command, roots);
	if (escape === undefined) return undefined;
	return {block: true, reason: `Blocked: ${escape}. This subagent is isolated in ${roots.worktree}.`};
}
// Tool access, model, and effort resolution
//
// Resolution happens once, in the parent, before a worker is spawned: the worker
// spec carries the result, so two jobs built from the same definition cannot
// disagree. The functions are pure and exported, because each branch below is a
// documented Claude Code rule and is tested directly.
//
// `available` is the tool pool a subagent inherits. The host does not expose its
// tool list through the mod API, so the built-in part is the documented set; MCP
// servers are read from the same config files the host reads.
// ---------------------------------------------------------------------------

export const SUBAGENT_TOOL_UNIVERSE: string[] = [
	'Agent',
	'Artifact',
	'AskUserQuestion',
	'Bash',
	'Edit',
	'EndConversation',
	'EnterPlanMode',
	'EnterWorktree',
	'ExitPlanMode',
	'ExitWorktree',
	'Glob',
	'Grep',
	'LSP',
	'ListAgents',
	'Monitor',
	'NotebookEdit',
	'PowerShell',
	'Read',
	'ScheduleWakeup',
	'SendMessage',
	'Skill',
	'SubagentHandback',
	'TaskStop',
	'TodoWrite',
	'ToolSearch',
	'WaitForMcpServers',
	'WebFetch',
	'WebSearch',
	'Workflow',
	'Write',
];

// Filter one: removed from every subagent, even when the definition lists them.
// `ExitPlanMode` is handled separately because plan mode keeps it; `Agent` stays
// listed and reports the depth error itself.
export const UNIVERSAL_TOOL_REMOVALS: string[] = [
	'AskUserQuestion',
	'EndConversation',
	'EnterPlanMode',
	'ScheduleWakeup',
	'WaitForMcpServers',
	'Workflow',
];

// Filter two: the built-in tools a background subagent keeps. Every MCP tool is
// kept regardless; `ListAgents` is deliberately absent, so it is foreground-only.
// Teammates on a team additionally keep the task and cron tools (phase 15).
export const BACKGROUND_BUILTIN_TOOLS: string[] = [
	'Read',
	'Grep',
	'Glob',
	'LSP',
	'Bash',
	'PowerShell',
	'Edit',
	'Write',
	'NotebookEdit',
	'WebFetch',
	'WebSearch',
	'TodoWrite',
	'Skill',
	'ToolSearch',
	'EnterWorktree',
	'ExitWorktree',
	'Monitor',
	'TaskStop',
	'SendMessage',
	'Artifact',
	'SubagentHandback',
];

export const EFFORT_LEVELS = ['low', 'medium', 'high', 'xhigh', 'max'] as const;

const MCP_PREFIX = 'mcp__';

function unique(values: string[]): string[] {
	const out: string[] = [];
	for (const value of values) if (!out.includes(value)) out.push(value);
	return out;
}

const isMcpTool = (name: string): boolean => name.startsWith(MCP_PREFIX);

// `Bash(git push *)` names a tool plus a constraint; the whole tool is removed.
// `mcp__server__*` stays whole because the wildcard is a server, not a tool name.
export function toolNameOfSpecifier(specifier: string): string {
	const text = String(specifier ?? '').trim();
	const open = text.indexOf('(');
	return (open >= 0 ? text.slice(0, open) : text).trim();
}

function mcpCapability(server: string): string {
	return `${MCP_PREFIX}${server}__*`;
}

function mcpParts(name: string): {server: string; tool?: string} {
	const rest = name.startsWith(MCP_PREFIX) ? name.slice(MCP_PREFIX.length) : name;
	const separator = rest.indexOf('__');
	if (separator < 0) return {server: rest};
	return {server: rest.slice(0, separator), tool: rest.slice(separator + 2)};
}

// One specifier -> the concrete capabilities it selects out of the inherited pool.
// An empty result means the specifier resolved to nothing.
function expandSpecifier(specifier: string, available: string[], mcpServers: string[]): string[] {
	const name = toolNameOfSpecifier(specifier);
	if (name.length === 0) return [];
	const capabilities = mcpServers.map(mcpCapability);
	if (name === '*') return [...available, ...capabilities];
	if (name === `${MCP_PREFIX}*`) return capabilities;
	if (isMcpTool(name)) {
		const {server, tool} = mcpParts(name);
		if (!mcpServers.includes(server)) return [];
		if (tool === undefined || tool === '*') return [mcpCapability(server)];
		return [`${MCP_PREFIX}${server}__${tool}`];
	}
	return available.includes(name) ? [name] : [];
}

function withheldBy(specifier: string, capability: string): boolean {
	const name = toolNameOfSpecifier(specifier);
	if (name.length === 0) return false;
	if (name === '*') return true;
	if (name === `${MCP_PREFIX}*`) return isMcpTool(capability);
	if (isMcpTool(name)) {
		if (!isMcpTool(capability)) return false;
		const wanted = mcpParts(name);
		const actual = mcpParts(capability);
		if (wanted.server !== actual.server) return false;
		if (wanted.tool === undefined || wanted.tool === '*') return true;
		return actual.tool === wanted.tool;
	}
	return capability === name;
}

export interface ToolFilterInput {
	/** The inherited tool pool. Defaults to the documented built-in universe. */
	available?: string[];
	/** Configured MCP server names; each contributes `mcp__<server>__*`. */
	mcpServers?: string[];
	/** A `tools` list. Omitted means inherit. */
	tools?: string[];
	/** A `disallowedTools` list. It wins over `tools`. */
	disallowedTools?: string[];
	/** `plan` keeps `ExitPlanMode`; every other mode removes it. */
	permissionMode?: string;
	/** A background agent keeps every MCP tool and only the allowlisted built-ins. */
	background?: boolean;
	/** A teammate keeps the shared task and cron tools, even when filtered out. */
	team?: boolean;
}

export type ToolFilterResult =
	| {ok: true; tools: string[]; removed: string[]}
	| {ok: false; error: string};

export function filterTools(input: ToolFilterInput = {}): ToolFilterResult {
	const available = unique([
		...(input.available ?? SUBAGENT_TOOL_UNIVERSE),
		...(input.team === true ? TEAM_TOOL_NAMES : []),
	]);
	const mcpServers = unique(input.mcpServers ?? []);
	const requested = input.tools === undefined || input.tools.length === 0 ? undefined : input.tools;

	let selected: string[];
	if (requested === undefined) {
		// Inherit every tool available to subagents, including every MCP server.
		selected = [...available, ...mcpServers.map(mcpCapability)];
	} else {
		selected = [];
		for (const specifier of requested) selected.push(...expandSpecifier(specifier, available, mcpServers));
		selected = unique(selected);
	}

	for (const specifier of input.disallowedTools ?? []) {
		selected = selected.filter(capability => !withheldBy(specifier, capability));
	}

	const removed: string[] = [];
	const kept = selected.filter(capability => {
		if (isMcpTool(capability)) return true;
		if (UNIVERSAL_TOOL_REMOVALS.includes(capability)) {
			removed.push(capability);
			return false;
		}
		if (capability === 'ExitPlanMode' && input.permissionMode !== 'plan') {
			removed.push(capability);
			return false;
		}
		if (input.background === true && !BACKGROUND_BUILTIN_TOOLS.includes(capability)) {
			removed.push(capability);
			return false;
		}
		return true;
	});

	// A teammate keeps the task and cron tools even when its `disallowedTools` or
	// its background filter would have removed them.
	if (input.team === true) {
		for (const tool of TEAM_TOOL_NAMES) if (!kept.includes(tool)) kept.push(tool);
	}

	if (requested !== undefined && kept.length === 0) {
		// Name the entries, not the derived names: the author must be able to find the
		// line in the definition.
		return {ok: false, error: `tools list resolved to no available tool: ${requested.join(', ')}`};
	}
	return {ok: true, tools: kept, removed: removed.filter(tool => !kept.includes(tool))};
}

function nonEmpty(value: unknown): string | undefined {
	return typeof value === 'string' && value.trim().length > 0 ? value.trim() : undefined;
}

export type ModelSource = 'parameter' | 'definition' | 'environment' | 'main' | 'inherit' | 'forced';

export interface ModelResolutionInput {
	/** The per-invocation `model` parameter. */
	requested?: string;
	/** The definition's `model` frontmatter. */
	definition?: string;
	env?: Record<string, string | undefined>;
	/** The main conversation's model; absent means the child inherits its default. */
	main?: string;
}

export interface ModelResolution {
	model: string | undefined;
	source: ModelSource;
}

// Parameter, frontmatter, environment, main. `inherit` (either place) selects the
// main model outright, so it outranks the environment default. FORCE makes the
// environment value win over the parameter and the frontmatter.
export function resolveModel(input: ModelResolutionInput = {}): ModelResolution {
	const env = input.env ?? {};
	const envModel =
		nonEmpty(env.BETTER_SUBAGENTS_SUBAGENT_MODEL) ?? nonEmpty(env.CLAUDE_CODE_SUBAGENT_MODEL);
	const forced =
		env.BETTER_SUBAGENTS_SUBAGENT_MODEL_FORCE === '1' ||
		env.BETTER_SUBAGENTS_SUBAGENT_MODEL_FORCE === 'true' ||
		env.CLAUDE_CODE_SUBAGENT_MODEL_FORCE === '1' ||
		env.CLAUDE_CODE_SUBAGENT_MODEL_FORCE === 'true';
	const main = nonEmpty(input.main);
	if (forced && envModel !== undefined) return {model: envModel, source: 'forced'};

	const requested = nonEmpty(input.requested);
	if (requested === 'inherit') return {model: main, source: 'inherit'};
	if (requested !== undefined) return {model: requested, source: 'parameter'};

	const definition = nonEmpty(input.definition);
	if (definition === 'inherit') return {model: main, source: 'inherit'};
	if (definition !== undefined) return {model: definition, source: 'definition'};

	if (envModel !== undefined) return {model: envModel, source: 'environment'};
	return {model: main, source: 'main'};
}

export type EffortSource = 'parameter' | 'definition' | 'environment' | 'none';

export interface EffortResolutionInput {
	/** The per-invocation `effort` parameter. */
	requested?: string;
	/** The definition's `effort` frontmatter. */
	definition?: string;
	env?: Record<string, string | undefined>;
}

export interface EffortResolution {
	effort: string | undefined;
	source: EffortSource;
}

function effortLevel(value: unknown): string | undefined {
	const level = nonEmpty(value)?.toLowerCase();
	return level !== undefined && (EFFORT_LEVELS as readonly string[]).includes(level) ? level : undefined;
}

// Per-invocation, frontmatter, environment. There is no effort inheritance from the
// main conversation: the runtime default applies when nothing is set.
export function resolveEffort(input: EffortResolutionInput = {}): EffortResolution {
	const env = input.env ?? {};
	const requested = effortLevel(input.requested);
	if (requested !== undefined) return {effort: requested, source: 'parameter'};
	const definition = effortLevel(input.definition);
	if (definition !== undefined) return {effort: definition, source: 'definition'};
	const fromEnv =
		effortLevel(env.BETTER_SUBAGENTS_EFFORT_LEVEL) ?? effortLevel(env.CLAUDE_CODE_EFFORT_LEVEL);
	if (fromEnv !== undefined) return {effort: fromEnv, source: 'environment'};
	return {effort: undefined, source: 'none'};
}

export interface LaunchResolutionInput {
	requestedModel?: string;
	definitionModel?: string;
	requestedEffort?: string;
	definitionEffort?: string;
	env?: Record<string, string | undefined>;
	mainModel?: string;
	tools?: string[];
	disallowedTools?: string[];
	permissionMode?: string;
	background?: boolean;
	availableTools?: string[];
	mcpServers?: string[];
	team?: boolean;
}

export type LaunchResolution =
	| {
			ok: true;
			model: string | undefined;
			modelSource: ModelSource;
			effort: string | undefined;
			effortSource: EffortSource;
			tools: string[];
			removed: string[];
	  }
	| {ok: false; error: string};

// The one spawn-time resolution: tools, model, and effort from the same inputs, so
// the spec and the record cannot disagree about what was resolved.
export function resolveLaunch(input: LaunchResolutionInput = {}): LaunchResolution {
	const tools = filterTools({
		...(input.availableTools !== undefined ? {available: input.availableTools} : {}),
		...(input.mcpServers !== undefined ? {mcpServers: input.mcpServers} : {}),
		...(input.tools !== undefined ? {tools: input.tools} : {}),
		...(input.disallowedTools !== undefined ? {disallowedTools: input.disallowedTools} : {}),
		...(input.permissionMode !== undefined ? {permissionMode: input.permissionMode} : {}),
		...(input.background !== undefined ? {background: input.background} : {}),
		...(input.team !== undefined ? {team: input.team} : {}),
	});
	if (!tools.ok) return tools;
	const model = resolveModel({
		...(input.requestedModel !== undefined ? {requested: input.requestedModel} : {}),
		...(input.definitionModel !== undefined ? {definition: input.definitionModel} : {}),
		...(input.env !== undefined ? {env: input.env} : {}),
		...(input.mainModel !== undefined ? {main: input.mainModel} : {}),
	});
	const effort = resolveEffort({
		...(input.requestedEffort !== undefined ? {requested: input.requestedEffort} : {}),
		...(input.definitionEffort !== undefined ? {definition: input.definitionEffort} : {}),
		...(input.env !== undefined ? {env: input.env} : {}),
	});
	return {
		ok: true,
		model: model.model,
		modelSource: model.source,
		effort: effort.effort,
		effortSource: effort.source,
		tools: tools.tools,
		removed: tools.removed,
	};
}

export interface McpServerConfig {
	command?: string;
	args?: string[];
	env?: Record<string, string>;
	url?: string;
}

// MCP server configs come from the host's config files. Discovery is best-effort:
// a missing or malformed file means "no MCP server known", never a session error.
// The first directory wins, matching the host's config precedence.
export function mcpServerConfigs(
	fs: Pick<SpoolFs, 'readFile'>,
	dirs: string[],
): Record<string, McpServerConfig> {
	const configs: Record<string, McpServerConfig> = {};
	for (const dir of dirs.slice(0, 4)) {
		if (typeof dir !== 'string' || dir.length === 0) continue;
		try {
			const parsed = JSON.parse(fs.readFile(join(dir, '.commandcode', 'mcp.json'))) as {
				mcpServers?: Record<string, Record<string, unknown> | undefined>;
			};
			for (const [name, config] of Object.entries(parsed?.mcpServers ?? {})) {
				if (name.length === 0 || config?.enabled === false || Object.hasOwn(configs, name)) continue;
				if (config === null || typeof config !== 'object') continue;
				const command = stringField(config.command);
				const url = stringField(config.url);
				const args = listField(config.args);
				const env = stringMapField(config.env);
				configs[name] = {
					...(command !== undefined ? {command} : {}),
					...(args !== undefined ? {args} : {}),
					...(env !== undefined ? {env} : {}),
					...(url !== undefined ? {url} : {}),
				};
			}
		} catch {
			/* no MCP config here */
		}
	}
	return configs;
}

export function mcpServerNames(fs: Pick<SpoolFs, 'readFile'>, dirs: string[]): string[] {
	return Object.keys(mcpServerConfigs(fs, dirs));
}

export interface McpTrustRoots {
	readonly cwd: string;
	readonly home: string;
	readonly managedDir?: string;
	readonly addDirs?: readonly string[];
}

// An inline server runs a command, so the folder that declares it must be one the
// session already trusts: the workspace, home, the managed directory, or an
// --add-dir. A plugin is never trusted with an inline server.
export function agentFolderTrusted(
	definition: {readonly source?: string; readonly path?: string},
	roots: McpTrustRoots,
): boolean {
	if (definition.source === 'plugin') return false;
	const path = definition.path;
	if (path === undefined || path.length === 0) return true;
	const absolute = isAbsolute(path) ? path : join(roots.cwd, path);
	const trustedRoots = [
		roots.cwd,
		roots.home,
		roots.managedDir ?? join(roots.home, '.commandcode', 'managed'),
		...(roots.addDirs ?? []),
	];
	return trustedRoots.some(root => root.length > 0 && isInside(root, absolute));
}

export interface McpResolutionInput {
	/** The definition's authored `mcpServers`. */
	readonly defined?: readonly AgentMcpServer[];
	readonly configured?: Record<string, McpServerConfig>;
	readonly definition?: {readonly source?: string; readonly path?: string};
	readonly env?: Record<string, string | undefined>;
	/** The caller's trust decision; `false` drops every inline server. */
	readonly trusted?: boolean;
}

export interface McpDrop {
	readonly name: string;
	readonly reason: string;
}

export interface McpResolution {
	readonly servers: AgentMcpServer[];
	readonly dropped: McpDrop[];
}

function commaList(value: string | undefined): string[] {
	return String(value ?? '')
		.split(',')
		.map(item => item.trim())
		.filter(Boolean);
}

// The one resolution: a name resolves from configured servers, an inline entry
// needs trust, and strict/bare/allow/deny policies apply afterwards. The result
// is what `session/new` receives, so the scope is exactly this agent's.
export function resolveAgentMcpServers(input: McpResolutionInput = {}): McpResolution {
	const env = input.env ?? {};
	const defined = input.defined ?? [];
	const dropped: McpDrop[] = [];
	if (input.definition?.source === 'plugin') {
		for (const spec of defined) dropped.push({name: spec.name, reason: 'plugin definitions ignore mcpServers'});
		return {servers: [], dropped};
	}
	const bare = env.BETTER_SUBAGENTS_BARE ?? env.CLAUDE_CODE_BARE;
	if (bare === '1' || bare === 'true') {
		for (const spec of defined) dropped.push({name: spec.name, reason: 'bare mode disables MCP'});
		return {servers: [], dropped};
	}
	const strictConfig = env.BETTER_SUBAGENTS_STRICT_MCP_CONFIG ?? env.CLAUDE_CODE_STRICT_MCP_CONFIG;
	const strict = strictConfig === '1' || strictConfig === 'true';
	const allow = commaList(env.BETTER_SUBAGENTS_MCP_ALLOW ?? env.CLAUDE_CODE_MCP_ALLOW);
	const deny = commaList(env.BETTER_SUBAGENTS_MCP_DENY ?? env.CLAUDE_CODE_MCP_DENY);
	const trusted = input.trusted !== false;
	const servers: AgentMcpServer[] = [];
	const seen = new Set<string>();
	for (const spec of defined) {
		const name = spec.name.trim();
		if (name.length === 0 || seen.has(name)) continue;
		const inline = spec.command !== undefined || spec.url !== undefined;
		let resolved: AgentMcpServer;
		if (inline) {
			if (!trusted) {
				dropped.push({name, reason: 'inline server outside a trusted folder'});
				continue;
			}
			if (strict) {
				dropped.push({name, reason: 'strict MCP config ignores inline servers'});
				continue;
			}
			resolved = {...spec, name};
		} else {
			const config = input.configured?.[name];
			if (config === undefined) {
				dropped.push({name, reason: 'not configured'});
				continue;
			}
			resolved = {name, ...config};
		}
		if (deny.includes('*') || deny.includes(name)) {
			dropped.push({name, reason: 'denied by the MCP policy'});
			continue;
		}
		if (allow.length > 0 && !allow.includes('*') && !allow.includes(name)) {
			dropped.push({name, reason: 'not in the MCP allow policy'});
			continue;
		}
		seen.add(name);
		servers.push(resolved);
	}
	return {servers, dropped};
}

// ---------------------------------------------------------------------------
// Permission modes
//
// One table drives both transports: `acp` is the `session/set_mode` id the ACP
// adapter sends, `cli` is the `cmd --permission-mode` value for the CLI paths.
// The definition's mode and the main session's mode are combined in exactly one
// place, so the inheritance rules cannot drift between callers.
// ---------------------------------------------------------------------------

export const PERMISSION_MODE_LIST = [
	'default',
	'acceptEdits',
	'auto',
	'dontAsk',
	'bypassPermissions',
	'plan',
] as const;

export type PermissionMode = (typeof PERMISSION_MODE_LIST)[number] | 'manual';
export type CanonicalPermissionMode = (typeof PERMISSION_MODE_LIST)[number];

export interface PermissionModeRow {
	/** the ACP `session/set_mode` id; absent when ACP has no equivalent */
	readonly acp?: string;
	/** the `cmd --permission-mode` value; absent when the CLI cannot express it */
	readonly cli?: string;
	/** read-only exploration: writes are refused */
	readonly readOnly?: boolean;
	/** approval prompts are auto-denied; explicit allows still work */
	readonly autoDeny?: boolean;
	/** this mode wins over the definition's permissionMode when the main session is in it */
	readonly inherits?: boolean;
	/** this mode skips prompts and may only be used when the main session is in it */
	readonly bypass?: boolean;
}

const PERMISSION_MODE_TABLE: Readonly<Record<CanonicalPermissionMode, PermissionModeRow>> = {
	default: {acp: 'default', cli: 'standard'},
	acceptEdits: {acp: 'auto-accept', cli: 'accept-edits', inherits: true},
	auto: {inherits: true},
	dontAsk: {acp: 'dont-ask', autoDeny: true},
	bypassPermissions: {acp: 'bypass', cli: 'yolo', inherits: true, bypass: true},
	plan: {acp: 'plan', cli: 'plan', readOnly: true},
};

// Every spelling the host accepts: the frontmatter names, the CLI names, and the
// ACP ids. `manual` is the documented alias for `default`.
const PERMISSION_MODE_ALIASES: Readonly<Record<string, CanonicalPermissionMode>> = {
	manual: 'default',
	standard: 'default',
	'accept-edits': 'acceptEdits',
	'auto-accept': 'acceptEdits',
	'dont-ask': 'dontAsk',
	yolo: 'bypassPermissions',
	bypass: 'bypassPermissions',
};

export function canonicalPermissionMode(value: unknown): CanonicalPermissionMode | undefined {
	if (typeof value !== 'string' || value.length === 0) return undefined;
	const alias = PERMISSION_MODE_ALIASES[value];
	if (alias !== undefined) return alias;
	return Object.prototype.hasOwnProperty.call(PERMISSION_MODE_TABLE, value)
		? (value as CanonicalPermissionMode)
		: undefined;
}

export type PermissionResolution =
	| {readonly ok: true; readonly mode: CanonicalPermissionMode}
	| {readonly ok: false; readonly error: string};

// The inheritance table in SPEC "Permission modes": a main session in
// bypassPermissions, acceptEdits, or auto wins; otherwise the definition's mode
// applies, except that bypassPermissions still requires the main session to be
// in it. An unknown definition mode is ignored (default), never a launch error.
export function resolvePermissionMode(definitionMode: unknown, mainMode: unknown): PermissionResolution {
	const main = canonicalPermissionMode(mainMode) ?? 'default';
	const definition = canonicalPermissionMode(definitionMode) ?? 'default';
	if (PERMISSION_MODE_TABLE[main].inherits === true) return {ok: true, mode: main};
	if (PERMISSION_MODE_TABLE[definition].bypass === true) {
		return {
			ok: false,
			error: 'permissionMode "bypassPermissions" requires the main session to be in bypassPermissions',
		};
	}
	return {ok: true, mode: definition};
}

export const permissionModes = {
	list: PERMISSION_MODE_LIST,
	table: PERMISSION_MODE_TABLE,
	canonical: canonicalPermissionMode,
	resolve: resolvePermissionMode,
};

// An ACP `session/request_permission` as the parent sees it: enough to show the
// prompt, name the tool call, and return the chosen option.
export interface PermissionRequestOption {
	readonly optionId: string;
	readonly kind?: string;
	readonly name?: string;
}

export interface PermissionRequest {
	readonly requestId?: number | string;
	readonly title: string;
	readonly toolCall?: {readonly title?: string; readonly kind?: string};
	readonly options: PermissionRequestOption[];
}

// The identity a job record and its signal share. Declared once, so the two
// shapes cannot drift: the record adds durable bookkeeping, the signal adds the
// message that reaches the session.
interface JobIdentity {
	id: string;
	/** The durable resume handle. Absent for a one-shot agent (Explore, Plan). */
	agentId?: string;
	owner?: string;
	label?: string;
	kind?: string;
	state?: string;
	startedAt?: number;
	finishedAt?: number;
	durationMs?: number;
	logPath?: string;
	sessionId?: string;
}

/** One compaction observed in a subagent's own context window. */
export interface CompactBoundary {
	readonly trigger: 'auto' | 'manual';
	/** The token count before compaction ran. */
	readonly preTokens: number;
	readonly at: number;
}

interface Signal extends JobIdentity {
	readonly exitCode?: number | null;
	readonly title?: string;
	readonly body?: string;
	/** The report was scanned before it could reach the parent. */
	readonly scanned?: boolean;
	readonly scanMatches?: string[];
	readonly question?: string;
	readonly permissionRequest?: PermissionRequest;
	/** A maxTurns or max_tokens stop: the output is partial and the job is resumable. */
	readonly partial?: boolean;
	readonly compactBoundary?: CompactBoundary;
}

interface JobRecord extends JobIdentity {
	command?: string;
	prompt?: string;
	cwd?: string;
	/** The persisted conversation for this agent; retention sweeps it with the record. */
	transcriptPath?: string;
	/** The maxTurns or max_tokens stop that produced a partial result. */
	partial?: boolean;
	/** The last compaction observed for this agent's own context window. */
	compactBoundary?: CompactBoundary;
	/** This job's nesting level, and the job that spawned it. */
	depth?: number;
	parentJobId?: string;
	agentType?: string;
	/** The addressable name from the spawn; absent for an anonymous agent. */
	name?: string;
	/** A fork: the main conversation's history, prompt, tools, model, cache. */
	fork?: boolean;
	/** The main conversation a fork copied. */
	mainSessionId?: string;
	/** The host's id for the fork session; follow-ups resume it. */
	forkSessionId?: string;
	/** `shared` for a fork, `own` for every other agent. */
	promptCache?: string;
	/** The team this teammate belongs to. */
	teamName?: string;
	/** Ctrl+B moved a foreground run to the background. */
	backgroundedAt?: number;
	/** Set by SubagentHandback: the report was already emitted, so the wrapper's
	 *  own end-of-turn signal is suppressed. */
	handbackAt?: number;
	/** The resolved execution policy: false is a foreground run. */
	background?: boolean;
	model?: string;
	tools?: string[];
	/** The MCP servers scoped to this agent; a resume keeps the same scope. */
	mcpServers?: AgentMcpServer[];
	autoCompactWindow?: number;
	cacheTtl?: CacheTtl;
	color?: string;
	maxTurns?: number;
	timeoutMs?: number;
	sessionName?: string;
	resumedFrom?: string;
	/** The ACP session the worker created; a resume targets this id. */
	acpSessionId?: string;
	effort?: string;
	permissionMode?: string;
	isolation?: string;
	/** The worktree this job ran in, when isolation created one. */
	worktree?: string;
	/** The repository root, used to remove the worktree. */
	worktreeRepo?: string;
	/** Checkout roots the worker must never enter. */
	mainCheckouts?: string[];
	/** Set once the project SubagentStop hook ran for this terminal job. */
	subagentStopHook?: boolean;
	/** Set once cleanup decided: the worktree was removed, or kept. */
	worktreeFinalized?: boolean;
	worktreeKept?: boolean;
	exitCode?: number;
	pid?: number;
	workerPid?: number;
	deliveredAt?: number;
	updatedAt?: number;
	failureReason?: string;
	canceledAt?: number;
	awaitingInput?: boolean;
	question?: string;
	permissionRequest?: PermissionRequest;
	scanned?: boolean;
	scanMatches?: string[];
}

// The record write a signal implies.
interface JobPatch {
	state?: string;
	exitCode?: number;
	finishedAt?: number;
	durationMs?: number;
	failureReason?: string;
	awaitingInput?: boolean;
	question?: string;
	permissionRequest?: PermissionRequest;
	scanned?: boolean;
	scanMatches?: string[];
	partial?: boolean;
	compactBoundary?: CompactBoundary;
}

// ---------------------------------------------------------------------------
// Job states
//
// The six states and their edges in one table. Every writer asks this table
// instead of spelling a state string, an edge that is not in the table is
// rejected, and a state the table does not know renders as a warning - never as
// a silent dot.
// ---------------------------------------------------------------------------

const JOB_STATES = ['queued', 'running', 'waiting-input', 'done', 'failed', 'canceled'] as const;
type JobState = (typeof JOB_STATES)[number];

// A signal may also carry a note (subagent_send), which is not a job state.
type SignalState = JobState | 'note';

// Legacy aliases, normalized in exactly one place.
const STATE_ALIASES: Readonly<Record<string, JobState>> = {
	completed: 'done',
	'agent-done': 'done',
	'command-done': 'done',
};

const STATE_TRANSITIONS: Readonly<Record<JobState, readonly JobState[]>> = {
	queued: ['running', 'failed', 'canceled'],
	running: ['waiting-input', 'done', 'failed', 'canceled'],
	'waiting-input': [],
	done: [],
	failed: [],
	canceled: [],
};

const STATE_MARKERS: Readonly<Record<SignalState, string>> = {
	queued: '◌',
	running: '●',
	'waiting-input': '?',
	done: '✓',
	failed: '✗',
	canceled: '⊘',
	note: '◈',
};

const STATE_COLORS: Readonly<Record<SignalState, string>> = {
	queued: 'gray',
	running: 'cyan',
	'waiting-input': 'yellow',
	done: 'green',
	failed: 'red',
	canceled: 'gray',
	note: 'gray',
};

function isState(value: unknown): value is JobState {
	return typeof value === 'string' && (JOB_STATES as readonly string[]).includes(value);
}

// Alias in, state out. One normalization point for every reader and writer.
function normalizeState(value: unknown): JobState | undefined {
	if (typeof value !== 'string' || value.length === 0) return undefined;
	const alias = STATE_ALIASES[value];
	if (alias !== undefined) return alias;
	return isState(value) ? value : undefined;
}

function isSignalState(value: unknown): value is SignalState {
	return value === 'note' || isState(value);
}

function canTransition(from: JobState, to: JobState): boolean {
	// Writing the state a job already has is not a transition; every other edge must
	// be in the table.
	if (from === to) return true;
	return STATE_TRANSITIONS[from].includes(to);
}

function isTerminal(state: JobState): boolean {
	return STATE_TRANSITIONS[state].length === 0;
}

// Terminal for a value that may be a legacy alias or an unknown string.
function isTerminalState(value: unknown): boolean {
	const state = normalizeState(value);
	return state !== undefined && isTerminal(state);
}

// Display: a legacy alias shows as its state, an unknown value shows as itself so
// it cannot hide.
function canonicalState(value: unknown): string {
	return normalizeState(value) ?? String(value ?? 'unknown');
}

function stateMarker(state: unknown): string {
	return isSignalState(state) ? STATE_MARKERS[state] : '⚠';
}

function stateColor(state: unknown): string {
	return isSignalState(state) ? STATE_COLORS[state] : 'red';
}

// Exported for the tests, which assert edges rather than inferring them from
// English. The harness never reads it.
export const jobStates = {
	list: JOB_STATES,
	isState,
	normalize: normalizeState,
	canTransition,
	isTerminal,
	marker: stateMarker,
	color: stateColor,
};

// ---------------------------------------------------------------------------
// Execution policy
//
// One rule set for "does this run block the conversation or report back later".
// Pure, so the precedence table is asserted without spawning a worker: the disable
// env wins over everything, a definition's `background: true` wins over the caller,
// and the caller's `run_in_background` wins over the default. The default is
// background, because a background run never holds the turn open.
// ---------------------------------------------------------------------------

export type ExecutionMode = 'foreground' | 'background';

export interface ExecutionPolicyInput {
	/** The caller's `run_in_background`; undefined means "no preference". */
	readonly requested?: boolean;
	/** The resolved definition's frontmatter `background`. */
	readonly definitionBackground?: boolean;
	readonly env: Record<string, string | undefined>;
}

export function resolveExecutionPolicy(input: ExecutionPolicyInput): ExecutionMode {
	const isSet = (value: string | undefined): boolean => {
		if (value === undefined) return false;
		const normalized = value.trim().toLowerCase();
		return normalized.length > 0 && normalized !== '0' && normalized !== 'false' && normalized !== 'no';
	};
	if (isSet(input.env[BACKGROUND_DISABLE_ENV]) || isSet(input.env[BACKGROUND_DISABLE_ENV_ALIAS])) return 'foreground';
	if (input.definitionBackground === true) return 'background';
	if (input.requested === false) return 'foreground';
	return 'background';
}

// ---------------------------------------------------------------------------
// Fork mode and agent teams
//
// Fork mode is the setting that decides whether the agents the model spawns run
// in the background. The default is the session shape: a human driving gets it,
// a non-interactive run and the Agent SDK do not. The flag and the environment
// override the default in both directions, so an SDK caller can opt in.
// ---------------------------------------------------------------------------

export type ForkMode = 'on' | 'off';

export interface ForkModeInput {
	readonly interactive?: boolean;
	/** The Agent SDK: off by default even when a TTY is attached. */
	readonly sdk?: boolean;
	readonly env?: Record<string, string | undefined>;
	/** `--mod-option fork_mode=on|off|auto`. An explicit on/off wins. */
	readonly flag?: string;
}

function forkToggle(value: unknown): ForkMode | undefined {
	if (typeof value !== 'string') return undefined;
	const normalized = value.trim().toLowerCase();
	if (['on', 'true', '1', 'yes'].includes(normalized)) return 'on';
	if (['off', 'false', '0', 'no'].includes(normalized)) return 'off';
	return undefined;
}

export function resolveForkMode(input: ForkModeInput = {}): ForkMode {
	const flag = forkToggle(input.flag);
	if (flag !== undefined) return flag;
	const env = input.env ?? {};
	const fromEnv = forkToggle(env[FORK_MODE_ENV]) ?? forkToggle(env[FORK_MODE_ENV_ALIAS]);
	if (fromEnv !== undefined) return fromEnv;
	const inSdk =
		input.sdk === true ||
		forkToggle(env[SDK_ENV]) === 'on' ||
		forkToggle(env[SDK_ENV_ALIAS]) === 'on' ||
		forkToggle(env.COMMANDCODE_SDK) === 'on';
	if (inSdk) return 'off';
	return input.interactive === true ? 'on' : 'off';
}

export interface TeamPolicyInput {
	readonly teammate: boolean;
	readonly definitionBackground?: boolean;
	readonly requested?: boolean;
	readonly forkMode: ForkMode;
}

export type TeamPolicy = {readonly ok: true; readonly background: false} | {readonly ok: false; readonly error: string};

// A teammate is always foreground. Its definition cannot pin it to the background,
// and a caller may only ask for the background when fork mode is off (where
// `run_in_background` still exists); with fork mode on the parameter is gone.
export function resolveTeamPolicy(input: TeamPolicyInput): TeamPolicy {
	if (input.teammate !== true) return {ok: true, background: false};
	if (input.definitionBackground === true) {
		return {
			ok: false,
			error:
				`a teammate cannot run in the background: its definition sets background: true; ` +
				'remove that field to join a team',
		};
	}
	if (input.requested === true && input.forkMode === 'off') {
		return {
			ok: false,
			error:
				'a teammate cannot run in the background: remove run_in_background: true (teammates run in the foreground)',
		};
	}
	return {ok: true, background: false};
}

export interface ForegroundJob {
	readonly state?: unknown;
	readonly failureReason?: unknown;
	readonly label?: unknown;
	readonly id?: unknown;
	readonly kind?: unknown;
	/** A maxTurns or max_tokens stop: the result is partial and resumable. */
	readonly partial?: unknown;
}

export type ForegroundResult =
	| {readonly ok: true; readonly text: string; readonly partial: boolean}
	| {readonly ok: false; readonly error: string};

// A line that is only worker plumbing is not agent output. "Produced nothing, or
// only tool calls" is decided here, once, for both the partial and the empty path.
const PLUMBING_LINE = /^\s*\[(tool|permission|acp)\b/;

export function hasAgentOutput(log: string): boolean {
	for (const line of log.split('\n')) {
		if (line.trim().length === 0) continue;
		if (PLUMBING_LINE.test(line)) continue;
		return true;
	}
	return false;
}

// The foreground result is the job's own result, shaped for a tool call: success
// returns the output, a cut-off returns the partial output plus a note, and any
// other failure returns the reason. Nothing is invented and nothing is dropped.
export function foregroundOutcome(job: ForegroundJob, log: string): ForegroundResult {
	const state = canonicalState(job.state);
	const label = String(job.label ?? job.id ?? 'subagent');
	const full = reportText(log, String(job.kind ?? 'agent')).trim();
	// The tool result is the model's context, so the inline result is bounded like
	// every other read. The full log stays at the job's log path.
	const text = full.length > OUTPUT_MAX_CHARS_DEFAULT ? `…${full.slice(-OUTPUT_MAX_CHARS_DEFAULT)}` : full;
	if (state === 'done') {
		return {ok: true, partial: false, text: text.length > 0 ? text : `${label} finished with no output.`};
	}
	if (state === 'waiting-input') {
		return {
			ok: true,
			partial: true,
			text: `${text.length > 0 ? `${text}\n\n` : ''}${label} stopped with a question; answer it here or resume the job.`,
		};
	}
	const reason =
		typeof job.failureReason === 'string' && job.failureReason.length > 0 ? job.failureReason : `${label} ${state}`;
	if (state === 'failed' && job.partial === true && hasAgentOutput(log)) {
		// A maxTurns stop is a partial success: the text is the result, and the job
		// keeps its agent id so a resume can continue it.
		return {ok: true, partial: true, text: `${text}\n\n[partial: ${reason}]`};
	}
	if (state === 'failed' && reason.startsWith(AGENT_API_ERROR) && hasAgentOutput(log)) {
		return {ok: true, partial: true, text: `${text}\n\n[cut off: ${reason}]`};
	}
	return {ok: false, error: reason};
}

// ---------------------------------------------------------------------------
// Task rows and the agent panel
//
// A row is a job shaped for the task list. Success disappears immediately (the
// result is already in the feed); failure and stop stay for 30 seconds so the
// user can read why, and `x` clears one early. Everything here is pure so the
// lifecycle is asserted without a timer.
// ---------------------------------------------------------------------------

export interface TaskRowLike {
	readonly id?: unknown;
	readonly state?: unknown;
	readonly startedAt?: unknown;
	readonly finishedAt?: unknown;
}

export interface TaskRowOptions {
	readonly now: number;
	/** Ids the user dismissed with `x`. */
	readonly dismissed?: ReadonlySet<string>;
}

export function taskRowVisible(job: TaskRowLike, options: TaskRowOptions): boolean {
	const id = String(job.id ?? '');
	if (id.length > 0 && options.dismissed?.has(id) === true) return false;
	const state = canonicalState(job.state);
	if (state === 'done') return false;
	if (state === 'failed' || state === 'canceled') {
		const finishedAt = typeof job.finishedAt === 'number' ? job.finishedAt : undefined;
		if (finishedAt === undefined) return true;
		return options.now - finishedAt <= TASK_ROW_GRACE_MS;
	}
	return true;
}

export function taskPanelRows<T extends TaskRowLike>(jobs: readonly T[], options: TaskRowOptions): T[] {
	return jobs.filter(job => taskRowVisible(job, options));
}

// Move the selection through the visible rows. With no selection, forward starts
// at the first row and backward at the last; the ends clamp instead of wrapping, so
// a held key cannot spin the selection.
export function moveTaskSelection(rows: readonly TaskRowLike[], selectedId: string | undefined, delta: number): string | undefined {
	if (rows.length === 0) return undefined;
	const current = rows.findIndex(row => String(row.id ?? '') === selectedId);
	if (current === -1) return String((delta < 0 ? rows[rows.length - 1] : rows[0])?.id ?? '');
	const next = Math.min(rows.length - 1, Math.max(0, current + (delta < 0 ? -1 : 1)));
	return String(rows[next]?.id ?? '');
}

// The @mention typeahead is a name list, not a spawn: every valid custom,
// built-in, and plugin-scoped definition, plus every named agent in this session.
// Sorted and de-duplicated so the list reads the same on every open.
export function mentionCandidates(
	definitions: readonly {readonly name?: string; readonly valid?: boolean}[],
	namedAgents: readonly string[] = [],
): string[] {
	const names = new Set<string>();
	for (const definition of definitions) {
		const name = String(definition.name ?? '').trim();
		if (name.length > 0 && definition.valid !== false) names.add(name);
	}
	for (const agent of namedAgents) {
		const name = String(agent ?? '').trim();
		if (name.length > 0) names.add(name);
	}
	return [...names].sort();
}

// The footer keeps a `/tasks` hint for the same 30 seconds a failed row stays (and
// a moment after a success), so a vanished success is still discoverable.
export function tasksHintVisible(jobs: readonly TaskRowLike[], now: number): boolean {
	return jobs.some(job => {
		const state = canonicalState(job.state);
		const finishedAt = typeof job.finishedAt === 'number' ? job.finishedAt : undefined;
		if (finishedAt === undefined) return false;
		if (state !== 'done' && state !== 'failed' && state !== 'canceled') return false;
		return now - finishedAt <= TASK_ROW_GRACE_MS;
	});
}

// ---------------------------------------------------------------------------
// Envelope
//
// One contract for "this job reached this state", in both directions:
// signalOf(record, result) is the signal, patchOf(signal) is the record write the
// signal implies. Both are pure, so no writer can invent its own field list.
// ---------------------------------------------------------------------------

function signalOf(
	record: JobRecord,
	result: {
		state: string;
		finishedAt?: number;
		durationMs?: number;
		exitCode?: number | null;
		title?: string;
		body?: string;
		scanned?: boolean;
		scanMatches?: string[];
	},
): Signal {
	return signalEnvelope(record, {...result, state: canonicalState(result.state)});
}

// The envelope itself, shared with the detached worker: WORKER_SOURCE embeds this
// function's source at module load, so the parent and the worker cannot construct
// a signal with different fields. Keep the body plain JS and self-contained.
export function signalEnvelope(
	record: JobIdentity,
	result: {
		state: string;
		finishedAt?: number;
		durationMs?: number;
		exitCode?: number | null;
		title?: string;
		body?: string;
		question?: string;
		permissionRequest?: PermissionRequest;
		scanned?: boolean;
		scanMatches?: string[];
		partial?: boolean;
		compactBoundary?: CompactBoundary;
	},
): Signal {
	const label = String(record.label ?? record.id);
	const state = String(result.state);
	return {
		id: record.id,
		...(record.agentId !== undefined ? {agentId: record.agentId} : {}),
		owner: record.owner,
		label,
		kind: String(record.kind ?? 'command'),
		state,
		exitCode: typeof result.exitCode === 'number' ? result.exitCode : null,
		startedAt: record.startedAt,
		finishedAt: result.finishedAt,
		durationMs: result.durationMs,
		logPath: record.logPath,
		sessionId: record.sessionId,
		title: result.title ?? `${label} ${state}`,
		body: result.body,
		...(typeof result.question === 'string' ? {question: result.question} : {}),
		...(result.permissionRequest !== undefined ? {permissionRequest: result.permissionRequest} : {}),
		// Omitted when the writer did not scan, so a legacy signal keeps its shape.
		...(result.scanned === undefined
			? {}
			: {scanned: result.scanned === true, scanMatches: result.scanMatches ?? []}),
		...(result.partial === true ? {partial: true} : {}),
		...(result.compactBoundary !== undefined ? {compactBoundary: result.compactBoundary} : {}),
	};
}

// One claimed inbox file: the signal, plus the two paths settle() needs to undo
// or finish the claim. Ownership is what the drain decides; Settled is how the
// claim ends.
interface Claim {
	readonly name: string;
	readonly signal: Signal;
	readonly live: string;
	readonly claimed: string;
}

type Ownership = 'mine' | 'foreign' | 'orphan' | 'pending';
type Settled = 'accepted' | 'held' | 'quarantined';

function patchOf(signal: Signal): JobPatch {
	const patch: JobPatch = {
		exitCode: typeof signal.exitCode === 'number' ? signal.exitCode : undefined,
		finishedAt: signal.finishedAt,
		durationMs: signal.durationMs,
	};
	if (signal.state !== undefined) {
		const state = canonicalState(signal.state);
		patch.state = state;
		if (state === 'failed' || state === 'canceled') patch.failureReason = signal.body;
		if (state === 'waiting-input') patch.awaitingInput = true;
	}
	if (typeof signal.question === 'string') patch.question = signal.question;
	if (signal.permissionRequest !== undefined) patch.permissionRequest = signal.permissionRequest;
	if (signal.scanned !== undefined) {
		patch.scanned = signal.scanned;
		patch.scanMatches = signal.scanMatches ?? [];
	}
	if (signal.partial === true) patch.partial = true;
	if (signal.compactBoundary !== undefined) patch.compactBoundary = signal.compactBoundary;
	return patch;
}

// ---------------------------------------------------------------------------
// Ledger
//
// Exactly-once delivery needs a replay guard that survives a restart, and a spool
// that does not grow forever. Both are the ledger's business: has(id) is the
// guard, accept(id) makes the fact durable, and retain(n) is the only compaction.
// Memory and disk are pruned together, so a size cap can never silently drop the
// guard the way a clear() of the in-memory set did.
// ---------------------------------------------------------------------------

export interface Ledger {
	has(id: string): boolean;
	accept(id: string): void;
	retain(limit: number): void;
	size(): number;
}

export interface LedgerDeps {
	readonly fs: SpoolFs;
	readonly path: string;
	readonly now: () => number;
	/** facts kept before retention kicks in at accept() time */
	readonly max: number;
	/** facts kept when retention compacts */
	readonly keep: number;
	/** file size that triggers a compaction on load */
	readonly compactBytes: number;
}

function createLedger(deps: LedgerDeps): Ledger {
	const ids = new Map<string, number>(); // id -> accepted at
	const ordered = (): [string, number][] => [...ids.entries()].sort((left, right) => left[1] - right[1]);

	function write(): void {
		const body = ordered()
			.map(([id, at]) => JSON.stringify({id, at}))
			.join('\n');
		try {
			deps.fs.writeFile(deps.path, body.length > 0 ? `${body}\n` : '');
		} catch {
			/* the in-memory guard still holds; the next accept retries the file */
		}
	}

	// Keep the newest `limit` facts - in memory and on disk, in one step. Pruning only
	// one of the two would either lose the guard at restart or let a restart replay.
	function retain(limit: number): void {
		const keep = ordered().slice(-Math.max(0, limit));
		ids.clear();
		for (const [id, at] of keep) ids.set(id, at);
		write();
	}

	// Load before the first drain: a restart must not replay what a previous process
	// already delivered.
	try {
		for (const line of deps.fs.readFile(deps.path).split('\n').filter(Boolean).slice(-deps.max)) {
			try {
				const entry = JSON.parse(line) as {id?: unknown; at?: unknown};
				if (typeof entry.id === 'string') ids.set(entry.id, typeof entry.at === 'number' ? entry.at : 0);
			} catch {
				/* ignore a torn last line */
			}
		}
	} catch {
		/* no ledger yet */
	}
	try {
		if (deps.fs.stat(deps.path).size > deps.compactBytes) retain(deps.keep);
	} catch {
		/* no ledger, or another process won the rewrite */
	}

	return {
		has: id => ids.has(id),
		accept: id => {
			if (ids.has(id)) return;
			const at = deps.now();
			ids.set(id, at);
			try {
				deps.fs.appendFile(deps.path, JSON.stringify({id, at}) + '\n');
			} catch {
				/* the in-memory guard still protects this process */
			}
			// Never clear(): drop the oldest facts, in both places, when the ledger is full.
			if (ids.size > deps.max) retain(deps.keep);
		},
		retain,
		size: () => ids.size,
	};
}

// Exported for the tests, which prove the guard survives retention and a restart.
export const ledgerFactory = createLedger;

// ---------------------------------------------------------------------------
// Limits and the shared TTL sweeper
//
// Two limits bound a session: how many jobs may run at once and how deep
// subagents may nest. Both are resolved from the environment in one place, with
// the Claude Code spelling accepted as an alias. The spawn guard lives in fire(),
// so a refused spawn writes no record and emits no signal.
// ---------------------------------------------------------------------------

export type LimitEnv = Record<string, string | undefined>;

function positiveInt(raw: string | undefined, fallback: number): number {
	const value = Number(raw);
	return Number.isFinite(value) && value >= 1 ? Math.floor(value) : fallback;
}

export function resolveMaxConcurrent(env: LimitEnv): number {
	return positiveInt(env[MAX_CONCURRENT_ENV] || env[MAX_CONCURRENT_ENV_ALIAS], MAX_CONCURRENT_DEFAULT);
}

export function resolveMaxDepth(env: LimitEnv): number {
	return positiveInt(env[MAX_DEPTH_ENV] || env[MAX_DEPTH_ENV_ALIAS], MAX_DEPTH_DEFAULT);
}

// A session's nesting depth: 0 for the main conversation, the job's own depth for
// a worker (the spec is the worker's environment).
function depthOf(spec: WorkerSpec | undefined): number {
	const depth = Number(spec?.depth);
	return Number.isFinite(depth) && depth >= 0 ? Math.floor(depth) : 0;
}

// The environment for a worker: the caller's env minus every worker marker, then
// this job's spec and flag. A nested spawn therefore cannot inherit the parent
// spec or a stale worker flag.
export function workerEnv(
	parent: Record<string, string | undefined>,
	spec: WorkerSpec,
): Record<string, string | undefined> {
	const env = {...parent};
	delete env[WORKER_SPEC_ENV];
	delete env[WORKER_ENV];
	delete env.PUBSUB_WORKER;
	env[WORKER_SPEC_ENV] = JSON.stringify(spec);
	env[WORKER_ENV] = '1';
	return env;
}

// One TTL sweeper for every directory the spool grows: claimed/orphaned signals,
// job records, logs and worktrees. `expired` covers entries whose age lives in
// their contents (a job record's finishedAt); `ttlMs` covers the rest.
export interface SweepIo {
	readdir(dir: string): string[];
	stat(file: string): {mtimeMs: number};
	remove(file: string): void;
}

export interface SweepOptions {
	ttlMs?: number;
	expired?: (name: string) => boolean;
	/** Keep an entry even when it is past the TTL (a live job owns its log). */
	skip?: (name: string) => boolean;
}

export function sweepExpiredEntries(io: SweepIo, dir: string, now: number, options: SweepOptions = {}): string[] {
	let names: string[];
	try {
		names = io.readdir(dir);
	} catch {
		return [];
	}
	const removed: string[] = [];
	for (const name of names) {
		const file = join(dir, name);
		try {
			if (options.skip?.(name) === true) continue;
			const expired = options.expired?.(name) === true;
			const old = options.ttlMs !== undefined && now - io.stat(file).mtimeMs > options.ttlMs;
			if (!expired && !old) continue;
			io.remove(file);
			removed.push(name);
		} catch {
			/* another process won it */
		}
	}
	return removed;
}

export default function (cmd: ModApi, overrides: RuntimeOverrides = {}): void {
	const runtime = osRuntime(overrides);
	// Below this line the spool is touched only through the injected filesystem.
	const spool = runtime.fs;
	// One git seam for worktree creation, cleanup and the isolation guard.
	const git = gitSeam(runtime);
	cmd.addFlag('subagent_dir', {
		type: 'string',
		default: SUBAGENT_DIR_DEFAULT,
		description: 'Where the spool, logs and job records live.',
	});
	cmd.addFlag('subagent_deliver', {
		type: 'string',
		default: 'steer',
		description: "How a signal reaches the session: 'steer' (after the current tool batch) or 'follow-up' (only when the run would stop).",
	});
	// 0.1.0 flag names. Recognized only when explicitly set to something other than
	// their own default, so an upgraded machine keeps working for one release.
	cmd.addFlag('pubsub_dir', {
		type: 'string',
		default: PUBSUB_DIR_DEFAULT,
		description: 'Deprecated alias for subagent_dir.',
	});
	cmd.addFlag('pubsub_deliver', {
		type: 'string',
		default: 'steer',
		description: 'Deprecated alias for subagent_deliver.',
	});
	cmd.addFlag('managed_dir', {
		type: 'string',
		default: MANAGED_SETTINGS_DEFAULT,
		description: 'Managed settings directory; agents/ under it wins every other scope.',
	});
	cmd.addFlag(CLI_AGENTS_OPTION, {
		type: 'string',
		default: '',
		description:
			'Session-only agent definitions as inline JSON, or a JSON file path in a non-interactive session (--mod-option agents=...).',
	});
	cmd.addFlag(ADD_DIRS_OPTION, {
		type: 'string',
		default: '',
		description: 'Comma-separated extra directories; .commandcode/agents/ under each is project scope (--add-dir).',
	});
	cmd.addFlag(PLUGIN_DIRS_OPTION, {
		type: 'string',
		default: '',
		description: 'Comma-separated plugin roots; every child directory with an agents/ dir is a plugin.',
	});
	cmd.addFlag(FORK_MODE_FLAG, {
		type: 'string',
		default: 'auto',
		description: 'Fork mode: auto (interactive on, non-interactive/SDK off), on, or off.',
	});
	// The host has no `--agent` or `--append-subagent-system-prompt` flag, so both
	// arrive as mod options (or the documented environment aliases). Recorded in
	// docs/adr/0001-worker-and-control.md.
	cmd.addFlag('agent', {
		type: 'string',
		default: '',
		description: 'Run this session as the named agent definition (--agent <name>).',
	});
	cmd.addFlag('append_subagent_system_prompt', {
		type: 'string',
		default: '',
		description: 'Text, or @path, appended to every subagent system prompt (--append-subagent-system-prompt).',
	});
	cmd.addFlag('append_subagent_system_prompt_file', {
		type: 'string',
		default: '',
		description: 'File whose text is appended to every subagent system prompt.',
	});

	const readFlag = (name: string): string => String(cmd.getFlag(name) ?? '');
	// A flag still sitting on its default is not an override; anything else is.
	const pick = (primary: string, primaryDefault: string, legacy: string, legacyDefault: string): string => {
		const value = readFlag(primary);
		if (value.length > 0 && value !== primaryDefault) return value;
		const old = readFlag(legacy);
		if (old.length > 0 && old !== legacyDefault) return old;
		return value.length > 0 ? value : legacyDefault;
	};
	const splitFlag = (name: string): string[] =>
		readFlag(name)
			.split(',')
			.map(value => value.trim())
			.filter(value => value.length > 0);
	const root = pick('subagent_dir', SUBAGENT_DIR_DEFAULT, 'pubsub_dir', PUBSUB_DIR_DEFAULT);
	const forkMode = resolveForkMode({
		interactive: runtime.interactive === true,
		env: runtime.env,
		flag: readFlag(FORK_MODE_FLAG),
	});
	const forkModeEnabled = forkMode === 'on';
	const inboxDir = join(root, 'inbox');
	const claimedDir = join(root, 'claimed');
	const orphanedDir = join(root, 'orphaned');
	const logsDir = join(root, 'logs');
	const jobsDir = join(root, 'jobs');
	const transcriptsDir = join(root, 'transcripts');
	const worktreesDir = join(root, 'worktrees');
	const controlRoot = join(root, 'control');
	const ledgerPath = join(root, 'delivered.jsonl');
	for (const dir of [root, inboxDir, claimedDir, orphanedDir, logsDir, jobsDir, transcriptsDir, worktreesDir, controlRoot]) {
		spool.mkdir(dir);
	}

	// A headless worker loads this mod too. It has its own owner token and must never
	// steal an unaddressed signal from the session that fired it.
	const workerMode = runtime.env[WORKER_ENV] === '1' || runtime.env.PUBSUB_WORKER === '1';

	// The main conversation's permission mode. There is no getter in the host API:
	// the mode arrives through `permission_mode_changed` and the system-prompt hook
	// context, and the environment override covers a host that knows it before the
	// first event (and the suite). Absent means `default`, the safe reading: a
	// definition can then never grant itself bypass.
	let mainPermissionMode: CanonicalPermissionMode | undefined = canonicalPermissionMode(
		runtime.env.BETTER_SUBAGENTS_MAIN_PERMISSION_MODE,
	);
	function noteMainPermissionMode(value: unknown): void {
		const mode = canonicalPermissionMode(value);
		if (mode !== undefined) mainPermissionMode = mode;
	}

	// Definitions are discovered once at bind, refreshed at each run start and by
	// `/agents`, so a new file is picked up without a restart. Invalid files are
	// skipped with an agents.log entry and remain visible (with the reason) in
	// `/agents`.
	const registry = createAgentRegistry({
		fs: spool,
		cwd: runtime.cwd,
		home: runtime.home,
		builtins: builtinAgentDefinitions(runtime.env),
		managedDir: readFlag('managed_dir') || MANAGED_SETTINGS_DEFAULT,
		addDirs: splitFlag(ADD_DIRS_OPTION),
		pluginRoots: splitFlag(PLUGIN_DIRS_OPTION),
		cli: {value: readFlag(CLI_AGENTS_OPTION), interactive: runtime.interactive === true},
		log: message => {
			try {
				spool.appendFile(join(root, AGENT_LOG_FILE), `[${new Date(runtime.now()).toISOString()}] ${message}\n`);
			} catch {
				/* the session still runs; the skip is visible in /agents too */
			}
		},
	});

	// The worker process reads the spec from its environment. Only the worker uses
	// this: the parent already has the definition.
	let workerSpec: WorkerSpec | undefined;
	if (workerMode) {
		try {
			workerSpec = JSON.parse(String(runtime.env[WORKER_SPEC_ENV] ?? '')) as WorkerSpec;
		} catch {
			workerSpec = undefined;
		}
	}

	// `cmd acp` in this host version loads installed mods but ignores `--mod`. The
	// marker tells the worker whether appendSystemPrompt is actually live in the ACP
	// child; without it the worker carries the body in the prompt instead.
	if (workerMode && workerSpec?.systemPrompt !== undefined && workerSpec.promptMarkerPath !== undefined) {
		try {
			spool.writeFile(workerSpec.promptMarkerPath, '1');
		} catch {
			/* the worker falls back to the prompt body */
		}
	}

	function resolveAgentDefinition(name?: string): {ok: true; definition: AgentDefinition} | {ok: false; error: string} {
		if (name === undefined) {
			const fallback = registry.get('general-purpose') ?? registry.get('general');
			if (fallback !== undefined) return {ok: true, definition: fallback};
			return {ok: false, error: 'subagent_type is required'};
		}
		const exact = registry.get(name);
		if (exact !== undefined) return {ok: true, definition: exact};
		// `general` is the host's reserved name for the same agent. Resolve it here
		// rather than registering a second agent under a name the host owns.
		if (name === 'general') {
			const general = registry.get('general-purpose');
			if (general !== undefined) return {ok: true, definition: general};
		}
		const available = registry.list().map(definition => definition.name).sort().join(', ');
		return {
			ok: false,
			error: `unknown subagent_type "${name}"${available.length > 0 ? `; available: ${available}` : ''}`,
		};
	}

	// The session-wide agent: `--agent <name>` as a mod option, or the environment
	// alias. The host has no such flag, so a mod flag or env is the documented
	// deviation. A worker never sets one: it is already an agent.
	const sessionAgentName = (): string => {
		if (workerMode) return '';
		return (readFlag('agent') || runtime.env.BETTER_SUBAGENTS_AGENT || runtime.env.CLAUDE_CODE_AGENT || '').trim();
	};
	function sessionAgent(): AgentDefinition | undefined {
		const name = sessionAgentName();
		return name.length === 0 ? undefined : registry.get(name);
	}
	let initialPromptSubmitted = false;

	// initialPrompt is the main session's first turn, never a subagent's. The host
	// exposes `cmd.prompt.submit`; when it is absent, transformInput prepends the
	// value to the first typed prompt instead.
	function submitInitialPrompt(): void {
		if (initialPromptSubmitted || workerMode) return;
		const text = sessionAgent()?.initialPrompt;
		if (text === undefined || !('prompt' in cmd)) return;
		const promptApi = cmd.prompt;
		if (promptApi === null || typeof promptApi !== 'object' || !('submit' in promptApi)) return;
		const submit = promptApi.submit;
		if (typeof submit !== 'function') return;
		submit(text);
		initialPromptSubmitted = true;
	}

	// The text every subagent's system prompt gets, from the flag, the file form,
	// the @path form, or the environment alias. A fork is exempt because it reuses
	// the main conversation's prompt (phase 15).
	function subagentSystemPrompt(base: string | undefined): string | undefined {
		const appended = resolveAppendSystemPrompt({
			flag: readFlag('append_subagent_system_prompt'),
			file: readFlag('append_subagent_system_prompt_file'),
			env: runtime.env,
			fs: spool,
			cwd: runtime.cwd,
		});
		const parts = [base ?? '', appended].filter(part => part.length > 0);
		return parts.length > 0 ? parts.join('\n\n') : undefined;
	}

	// The worker's system prompt: the definition body, the rendered sibling roster
	// (supplied by the live session), then the startup bundle. The parent builds
	// all three, so the hook is registered only in worker mode and only when there
	// is something to append.
	function workerSystemPrompt(): string {
		const roster = (workerSpec as {roster?: unknown} | undefined)?.roster;
		const parts = [
			typeof workerSpec?.systemPrompt === 'string' ? workerSpec.systemPrompt : '',
			typeof roster === 'string' ? roster : '',
			typeof workerSpec?.startup === 'string' ? workerSpec.startup : '',
		].filter(part => part.length > 0);
		return parts.join('\n\n');
	}

	// The startup bundle for one agent job, built from the same definition the
	// worker receives. It is rendered in the parent from the injected filesystem.
	// The roster is supplied by the live session (T9); a subagent never inherits
	// the parent's auto memory, only its own MEMORY.md.
	function startupFor(
		definition: AgentDefinition | undefined,
		cwd: string,
		prompt: string | undefined,
		model: string | undefined,
	): string | undefined {
		if (definition === undefined || prompt === undefined) return undefined;
		return buildStartupBundle({
			cwd,
			home: runtime.home,
			env: runtime.env,
			fs: runtime.fs,
			definition,
			agentType: definition.name,
			prompt,
			model,
			gitStatus: gitStatusSnapshot(runtime, cwd),
			now: runtime.now(),
		}).text;
	}

	// Project hooks: `.commandcode/hooks.json` from the working directory up to the
	// repository root. Nearest first, so a nested project's hooks run before its
	// parent's. A missing or malformed file contributes nothing, never an error.
	function projectHookRules(event: 'SubagentStart' | 'SubagentStop'): AgentHookRule[] {
		const rules: AgentHookRule[] = [];
		let current = runtime.cwd;
		for (let depth = 0; depth < STARTUP_WALK_DEPTH_MAX; depth += 1) {
			try {
				const parsed = JSON.parse(spool.readFile(join(current, '.commandcode', 'hooks.json'))) as Record<string, unknown>;
				const found = hookList(parsed?.[event]);
				if (found !== undefined) rules.push(...found);
			} catch {
				/* no hooks here */
			}
			try {
				// `stat` covers both a normal `.git` directory and a linked worktree's file.
				spool.stat(join(current, '.git'));
				break;
			} catch {
				/* treat an unreadable marker as absent */
			}
			const parent = dirname(current);
			if (parent === current) break;
			current = parent;
		}
		return rules;
	}

	const deliverAs = (): 'steer' | 'follow-up' =>
		(pick('subagent_deliver', 'steer', 'pubsub_deliver', 'steer') === 'follow-up' ? 'follow-up' : 'steer');

	// Bound once the harness binds. Diagnostic only; routing is by owner token.
	let sessionId: string | undefined;

	// ------------------------------------------------------------------ owner
	//
	// The spool is shared, so every job and signal carries an owner token. The token
	// is minted once per session and persisted in the transcript, so a resume or a hot
	// reload keeps the same identity. id() resolves it, verdict() decides what a
	// signal's owner means, and there is deliberately no process-<pid> fallback: an
	// invented identity is one the session can never recognise again, which is how a
	// session's own result used to turn into a foreign signal.

	let ownerToken: string | undefined;
	let ownerSaved = false;

	function saveOwner(token: string): void {
		if (ownerSaved || cmd.session === undefined) return;
		try {
			cmd.session.appendCustomEntry({customType: OWNER_TYPE, data: {token}});
			ownerSaved = true;
		} catch {
			// Retried on the next id() call. The token still identifies this process, so
			// its own results stay collectable in the meantime.
		}
	}

	function ownerId(): string | undefined {
		if (ownerToken !== undefined) {
			saveOwner(ownerToken);
			return ownerToken;
		}
		// No session, no identity. A token minted here could never be read back, and a
		// token that cannot be read back is exactly how a session's own result turns into
		// a foreign signal. The caller treats this as 'pending' and waits.
		if (cmd.session === undefined) return undefined;
		// The current entry type first, then the 0.1.0 one: a session that was live
		// across the rename keeps the identity its older jobs were stamped with.
		for (const customType of [OWNER_TYPE, OWNER_TYPE_LEGACY]) {
			try {
				const entries = cmd.session.getCustomEntries({customType}) as
					| {data?: {token?: unknown}}[]
					| undefined;
				for (let index = (entries?.length ?? 0) - 1; index >= 0; index -= 1) {
					const token = entries?.[index]?.data?.token;
					if (typeof token === 'string' && token.length > 0) {
						ownerToken = token;
						ownerSaved = true;
						return ownerToken;
					}
				}
			} catch {
				/* fall through and mint one */
			}
		}
		ownerToken = `owner-${randomUUID()}`;
		saveOwner(ownerToken);
		return ownerToken;
	}

	// mine: deliver. foreign: another session's, leave it. orphan: untagged legacy data,
	// quarantine it. pending: this session does not know its identity yet - a real
	// state, so the signal waits instead of being dropped or delivered on a guess.
	function ownerVerdict(signalOwner: unknown): Ownership {
		if (ownerToken === undefined) return 'pending';
		if (typeof signalOwner === 'string' && signalOwner.length > 0) {
			return signalOwner === ownerToken ? 'mine' : 'foreign';
		}
		return workerMode ? 'foreign' : 'orphan';
	}

	// A renderer so a signal shows up as a real feed row rather than raw JSON. The
	// `pubsub` id stays registered for one release so entries written by 0.1.0 render.
	const renderSignal = (data: unknown): string[] => {
		const signal = data as Partial<Signal>;
		const state = signal.state === undefined ? '' : ` · ${signal.state}`;
		return [`${stateMarker(signal.state)} subagent · ${String(signal.kind ?? 'signal')}${state} · ${String(signal.title ?? '(untitled)')}`];
	};
	cmd.addRenderer('subagent', renderSignal);
	cmd.addRenderer('pubsub', renderSignal);

	// One writer at a time. A tick that finds this flag set returns immediately rather
	// than racing the previous tick over the same files.
	let draining = false;

	// A signal claimed twice (a lost claimed/ file replayed, or an onStop drain racing a
	// timer tick) must not land twice. The guard is durable, so a restart cannot replay
	// what a previous process already delivered.
	const ledger = createLedger({
		fs: spool,
		path: ledgerPath,
		now: runtime.now,
		max: DELIVERED_LEDGER_MAX,
		keep: DELIVERED_LEDGER_KEEP,
		compactBytes: LEDGER_COMPACT_BYTES,
	});

	// One cleanup decision per finished job, made wherever the terminal state is first
	// observed - a delivered signal or a restart reconcile. The record write makes it
	// idempotent, so a kept worktree is not reported twice.
	function finalizeJobWorktree(job: JobRecord | undefined): WorktreeDecision | undefined {
		if (job === undefined) return undefined;
		const state = normalizeState(job.state);
		if (state !== 'done' && state !== 'failed' && state !== 'canceled') return undefined;
		if (typeof job.worktree !== 'string' || job.worktree.length === 0) return undefined;
		// Already decided by the restart reconcile or an earlier format: report the kept
		// path again without touching git, so delivery never loses the note.
		if (job.worktreeFinalized === true) return {kept: job.worktreeKept === true, path: job.worktree};
		const decision = finalizeWorktree(git, job);
		if (decision === undefined) return undefined;
		job.worktreeFinalized = true;
		job.worktreeKept = decision.kept;
		job.updatedAt = runtime.now();
		writeJob(job);
		return decision;
	}

	// A2A-style envelope: the job identity and state travel WITH the result, so the
	// model can tell which worker finished, whether it passed, and where the full log is.
	function formatSignal(message: Signal): string {
		const state = canonicalState(message.state ?? message.kind ?? 'signal');
		const worktree = finalizeJobWorktree(readJob(String(message.id ?? '')));
		const kept =
			worktree?.kept === true
				? `\nworktree kept at ${worktree.path}${worktree.error === undefined ? '' : ` (${worktree.error})`}`
				: '';
		const facts = [
			`job ${String(message.id ?? 'unknown')}`,
			typeof message.agentId === 'string' ? `agent ${message.agentId}` : '',
			String(message.kind ?? 'signal'),
			state,
			message.partial === true ? 'partial' : '',
			message.exitCode === null || message.exitCode === undefined ? '' : `exit ${message.exitCode}`,
			typeof message.durationMs === 'number' ? `${(message.durationMs / 1000).toFixed(1)}s` : '',
		].filter(Boolean).join(' · ');
		const log = typeof message.logPath === 'string' ? `\nlog ${message.logPath}` : '';
		const boundary =
			message.compactBoundary === undefined
				? ''
				: `\ncompact_boundary: ${message.compactBoundary.trigger} at ${message.compactBoundary.preTokens} tokens`;
		const body = typeof message.body === 'string' ? message.body.trim() : '';
		const title = String(message.title ?? message.label ?? message.id ?? '(signal)');
		// The worker names the agent (the title) and carries the request: the parent
		// does not have to read the log to know what is being asked.
		const request = message.permissionRequest;
		const options = request === undefined ? '' : request.options.map(option => option.name ?? option.kind ?? option.optionId).filter(Boolean).join(', ');
		const ask =
			state === 'waiting-input'
				? request !== undefined
					? `\n\nApproval required: ${request.title}` + (options.length > 0 ? `\noptions: ${options}` : '')
					: typeof message.question === 'string' && message.question.length > 0
						? `\n\n${message.question}`
						: ''
				: '';
		const next =
			state === 'waiting-input'
				? 'The job is resumable: answer here, or fire a follow-up job. Nothing else arrives until then.'
				: state === 'canceled'
					? 'The job was canceled; nothing else will arrive for it.'
					: message.partial === true && typeof message.agentId === 'string'
						? `The result is partial: resume it with subagent_resume ${message.agentId}.`
						: 'This result arrived automatically. Read it now and use it in your current work; ' +
							'do not poll for it and do not wait for it.';
		return `[subagent] ${title}\n${facts}${log}${boundary}${kept}` + (body.length > 0 ? `\n\n${body}` : '') + ask + `\n\n${next}`;
	}

	function readJob(id: string): JobRecord | undefined {
		try {
			const parsed = JSON.parse(spool.readFile(join(jobsDir, `${id}.json`))) as JobRecord;
			return parsed && typeof parsed.id === 'string' ? parsed : undefined;
		} catch {
			return undefined;
		}
	}

	function writeJob(record: JobRecord): void {
		try {
			const file = join(jobsDir, `${record.id}.json`);
			const tmp = `${file}.mod-${runtime.pid}-${runtime.now()}`;
			spool.writeFile(tmp, JSON.stringify(record, null, 2));
			spool.rename(tmp, file);
		} catch {
			/* the in-memory list still shows it */
		}
	}

	// One state-change choke point for the parent. The durable record is the source of
	// truth, so a stale in-memory copy cannot skip a step, and an edge the state table
	// does not allow is rejected instead of written.
	function applyPatch(id: string, patch: JobPatch, fallback: JobRecord, stamp: number): boolean {
		const current = readJob(id) ?? fallback;
		const from = normalizeState(current.state);
		const to = patch.state === undefined ? from : normalizeState(patch.state);
		if (patch.state !== undefined && to === undefined) return false;
		if (from !== undefined && to !== undefined && !canTransition(from, to)) return false;
		writeJob({...current, ...patch, ...(to !== undefined ? {state: to} : {}), updatedAt: stamp});
		return true;
	}

	function readRecentJobs(limit: number): JobRecord[] {
		let names: string[];
		try {
			names = spool.readdir(jobsDir).filter(name => name.endsWith('.json') && !name.includes('.tmp-'));
		} catch {
			return [];
		}
		names.sort();
		const jobs: JobRecord[] = [];
		for (const name of names.slice(-limit).reverse()) {
			try {
				jobs.push(JSON.parse(spool.readFile(join(jobsDir, name))) as JobRecord);
			} catch {
				/* skip a torn record */
			}
		}
		return jobs;
	}

	// ------------------------------------------------------------------ live
	//
	// One live-message queue per job. A sender appends a JSON line; the worker
	// consumes it at the next safe point (see WORKER_SOURCE). A worker's state.json
	// says whether a turn is active, so the tool can report "queued" vs "starts a
	// turn" without guessing. Names resolve inside this session's jobs only: another
	// session's roster is not this session's to message.

	function rosterNames(): string[] {
		if (workerMode) return Array.isArray(workerSpec?.rosterNames) ? workerSpec.rosterNames : [];
		const own = ownerId();
		const names: string[] = [];
		for (const job of readRecentJobs(AGENT_SCAN_MAX)) {
			if (typeof job.name !== 'string' || job.name.length === 0) continue;
			if (own !== undefined && job.owner !== own) continue;
			if (!names.includes(job.name)) names.push(job.name);
		}
		return names;
	}

	function namedJob(name: string): JobRecord | undefined {
		const wanted = name.trim();
		if (wanted.length === 0) return undefined;
		const own = workerMode ? workerSpec?.owner : ownerId();
		const allowed = workerMode ? rosterNames() : undefined;
		let fallback: JobRecord | undefined;
		for (const job of readRecentJobs(AGENT_SCAN_MAX)) {
			if (job.name !== wanted) continue;
			if (!workerMode && own !== undefined && job.owner !== undefined && job.owner !== own) continue;
			if (allowed !== undefined && !allowed.includes(wanted)) continue;
			// Prefer the record whose worker is still alive: a resumed job shares the
			// name with the record it continued, and the live one is the address.
			if (runtime.alive(job.workerPid) || runtime.alive(job.pid)) return job;
			if (fallback === undefined) fallback = job;
		}
		return fallback;
	}

	// `alive` decides queue-vs-resume; `busy` decides the wording of a queue. No
	// state file means the worker has not published yet, so a live worker is assumed
	// to be in a turn.
	function liveState(record: JobRecord): {alive: boolean; busy: boolean} {
		const alive = runtime.alive(record.workerPid) || runtime.alive(record.pid);
		let busy = true;
		try {
			const state = JSON.parse(spool.readFile(join(controlRoot, record.id, 'state.json'))) as {busy?: unknown};
			busy = state.busy !== false;
		} catch {
			/* no state file yet */
		}
		return {alive, busy};
	}

	function appendLive(id: string, from: string, message: string): boolean {
		const dir = join(controlRoot, id);
		try {
			spool.mkdir(dir);
			spool.appendFile(join(dir, 'send.jsonl'), `${JSON.stringify({from, message, at: runtime.now()})}\n`);
			return true;
		} catch {
			return false;
		}
	}

	// The snapshot a new agent receives: main plus every other named agent owned by
	// this session, newest record per name. `rosterNames` is the same set as data, so
	// the worker can validate a `to` without parsing the rendered text.
	function rosterFor(selfName: string | undefined): {roster?: string; rosterNames: string[]} {
		const own = workerMode ? workerSpec?.owner : ownerId();
		const entries: RosterEntry[] = [];
		const names: string[] = [];
		for (const job of readRecentJobs(AGENT_SCAN_MAX)) {
			if (typeof job.name !== 'string' || job.name.length === 0) continue;
			if (job.name === selfName) continue;
			if (own !== undefined && job.owner !== own) continue;
			if (names.includes(job.name)) continue;
			names.push(job.name);
			entries.push({name: job.name, state: canonicalState(job.state), jobId: job.id});
		}
		// A nested agent inherits its parent's view of the team plus its parent itself.
		if (workerMode) {
			const inherited = [String(workerSpec?.name ?? ''), ...(Array.isArray(workerSpec?.rosterNames) ? workerSpec.rosterNames : [])];
			for (const name of inherited) {
				if (name.length === 0 || name === selfName || names.includes(name)) continue;
				names.push(name);
				entries.push({name});
			}
		}
		const roster = buildRoster(entries, selfName);
		return {...(roster.length > 0 ? {roster} : {}), rosterNames: names};
	}

	function enqueueSignal(message: Signal): void {
		try {
			const stamp = `${runtime.now()}-${message.id}-${runtime.pid}-${Math.random().toString(36).slice(2, 6)}.json`;
			const tmp = join(inboxDir, `.tmp-${stamp}`);
			spool.writeFile(tmp, JSON.stringify(message, null, 2));
			spool.rename(tmp, join(inboxDir, stamp));
		} catch {
			/* a durable signal is best effort; the job record still has the result */
		}
	}

	function markDelivered(id: string): void {
		const job = readJob(id);
		if (job === undefined) return;
		job.deliveredAt = runtime.now();
		job.updatedAt = job.deliveredAt;
		writeJob(job);
	}

	// `queueMessage` only steers a run that is already going - it never STARTS one,
	// so an idle session would leave the result unread until someone asked for it.
	// `sendUserMessage` starts the turn when idle and queues behind the active turn
	// otherwise. This is the A2A delivery path.
	function deliver(message: Signal): void {
		cmd.sendUserMessage(formatSignal(message), {deliverAs: deliverAs()});
		cmd.showEntry('subagent', message);
	}

	// ------------------------------------------------------------------ spool
	//
	// One file in inbox/ is one undelivered signal, and rename() is the claim: it is
	// atomic, so exactly one drainer wins a file even with several sessions on one
	// spool. next() and settle() are the only two operations on a claimed file, so
	// drain, onStop and the pending count cannot disagree about a signal - and in
	// particular a signal this session does not own is put back, never deleted.

	function inboxNames(): string[] {
		try {
			return spool.readdir(inboxDir).filter(name => name.endsWith('.json'));
		} catch {
			return [];
		}
	}

	// Claim the next signal this session may deliver. Everything else - torn, already
	// delivered, untagged, foreign, or not classifiable yet - is settled right here,
	// so the caller only ever sees its own work. `seen` bounds one pass.
	function next(seen: Set<string>): Claim | undefined {
		for (const name of inboxNames()) {
			if (seen.has(name)) continue;
			seen.add(name);
			const live = join(inboxDir, name);
			const claimed = join(claimedDir, name);
			try {
				spool.rename(live, claimed);
			} catch {
				/* another drainer won it */
				continue;
			}
			let signal: Signal | undefined;
			try {
				signal = JSON.parse(spool.readFile(claimed)) as Signal;
			} catch {
				signal = undefined;
			}
			if (signal === undefined || typeof signal.id !== 'string' || signal.id.length === 0) {
				spool.remove(claimed);
				continue;
			}
			const claim: Claim = {name, signal, live, claimed};
			if (ledger.has(signal.id)) {
				// Delivered once already (a replayed claimed/ file): drop this copy.
				settle(claim, 'accepted');
				continue;
			}
			const owner = ownerVerdict(signal.owner);
			if (owner === 'orphan') {
				settle(claim, 'quarantined');
				continue;
			}
			if (owner !== 'mine') {
				settle(claim, 'held');
				continue;
			}
			// A foreground run already returned this exact result inside the tool call.
			// Its durable signal is the same result, so accept it here: delivering it
			// again would report the work twice.
			const settledJob = readJob(signal.id);
			if (settledJob !== undefined && settledJob.background === false) {
				markDelivered(signal.id);
				settle(claim, 'accepted');
				continue;
			}
			return claim;
		}
		return undefined;
	}

	function settle(claim: Claim, outcome: Settled): void {
		if (outcome === 'accepted') {
			ledger.accept(claim.signal.id);
			spool.remove(claim.claimed);
			return;
		}
		if (outcome === 'quarantined') {
			try {
				spool.rename(
					claim.claimed,
					join(orphanedDir, `${runtime.now()}-${Math.random().toString(36).slice(2, 6)}-${claim.name}`),
				);
			} catch {
				/* leave it in claimed/; sweepClaimed bounds it. Never delete a signal here. */
			}
			return;
		}
		// Held: put it back for its owner. If the rename cannot happen, write the
		// signal back into inbox/ instead of deleting the only durable copy.
		try {
			spool.rename(claim.claimed, claim.live);
		} catch {
			try {
				spool.writeFile(claim.live, JSON.stringify(claim.signal, null, 2));
				spool.remove(claim.claimed);
			} catch {
				/* it stays in claimed/ and is swept later. Never deleted. */
			}
		}
	}

	function drain(): void {
		if (draining) return;
		draining = true;
		const seen = new Set<string>();
		try {
			for (let handled = 0; handled < FILES_PER_DRAIN_MAX; handled += 1) {
				const claim = next(seen);
				if (claim === undefined) return;
				// No bound session is a state, not an exception: sending here would
				// degrade to a silent queue push in a headless run. Hold it instead.
				if (cmd.session === undefined) {
					settle(claim, 'held');
					continue;
				}
				try {
					deliver(claim.signal);
				} catch {
					settle(claim, 'held');
					continue;
				}
				markDelivered(claim.signal.id);
				settle(claim, 'accepted');
			}
		} finally {
			draining = false;
		}
	}

	// A failed drain must never take the session down with it.
	const safeDrain = (): void => {
		try {
			drain();
		} catch {
			/* the next tick tries again */
		}
	};

	// Claim up to `limit` signals without delivering them; the caller decides how they
	// reach the model (onStop continues the run with them as the reason). An unsettled
	// claim stays in claimed/ and is swept later.
	function claimPending(limit: number): Claim[] {
		const seen = new Set<string>();
		const claims: Claim[] = [];
		while (claims.length < limit) {
			const claim = next(seen);
			if (claim === undefined) break;
			claims.push(claim);
		}
		return claims;
	}

	// claimed/ is a rename target, not an archive. Sweep files abandoned by a drain
	// that died between claim and remove so a long-lived spool cannot grow forever.
	function sweepClaimed(): void {
		sweepExpiredEntries(spool, claimedDir, runtime.now(), {ttlMs: CLAIMED_TTL_MS});
	}

	function sweepOrphaned(): void {
		if (workerMode) return;
		sweepExpiredEntries(spool, orphanedDir, runtime.now(), {ttlMs: ORPHANED_TTL_MS});
	}

	// Retention. A finished job keeps its record, its log and its transcript for
	// RETENTION_DAYS_DEFAULT (or BETTER_SUBAGENTS_RETENTION_DAYS, its
	// CLAUDE_CODE_CLEANUP_PERIOD_DAYS alias, or `cleanupPeriodDays` in settings), so
	// `subagent_output` still works on a job from last week. A running job is never
	// swept, however old its record is. The three artifacts are swept together.
	function cleanupPeriodDays(): number | undefined {
		// Project settings win over user settings, the same precedence the definition
		// registry uses. A missing or malformed file contributes nothing.
		for (const base of [runtime.cwd, runtime.home]) {
			if (typeof base !== 'string' || base.length === 0) continue;
			try {
				const parsed = JSON.parse(spool.readFile(join(base, '.commandcode', 'settings.json'))) as Record<string, unknown>;
				const value = Number(parsed?.[CLEANUP_PERIOD_SETTING]);
				if (Number.isFinite(value) && value > 0) return value;
			} catch {
				/* no settings here */
			}
		}
		return undefined;
	}

	function retentionMs(): number {
		const raw = runtime.env[RETENTION_ENV] ?? runtime.env[RETENTION_ENV_ALIAS];
		const days = Number(raw);
		const configured = Number.isFinite(days) && days > 0 ? days : cleanupPeriodDays();
		const value =
			configured !== undefined && Number.isFinite(configured) && configured > 0
				? configured
				: RETENTION_DAYS_DEFAULT;
		return value * 24 * 60 * 60 * 1000;
	}

	function sweepExpired(): void {
		if (workerMode) return;
		const now = runtime.now();
		const ttlMs = retentionMs();
		const cutoff = now - ttlMs;
		const expired: {id: string; transcriptPath?: string}[] = [];
		// A record's age lives in its timestamps, not its mtime: one sweeper, but the
		// predicate decides. A running job is never expired.
		sweepExpiredEntries(spool, jobsDir, now, {
			expired: name => {
				if (!name.endsWith('.json') || name.includes('.tmp-')) return false;
				try {
					const job = JSON.parse(spool.readFile(join(jobsDir, name))) as JobRecord;
					const state = normalizeState(job.state);
					if (state === undefined || !isTerminal(state)) return false;
					const settledAt = Number(job.finishedAt ?? job.updatedAt ?? job.startedAt ?? 0) || 0;
					if (settledAt === 0 || settledAt > cutoff) return false;
					if (typeof job.id === 'string' && job.id.length > 0) {
						expired.push({
							id: job.id,
							...(typeof job.transcriptPath === 'string' && job.transcriptPath.length > 0
								? {transcriptPath: job.transcriptPath}
								: {}),
						});
					}
					return true;
				} catch {
					return false;
				}
			},
		});
		for (const {id, transcriptPath} of expired) {
			try {
				spool.remove(join(logsDir, `${id}.log`));
			} catch {
				/* the log is already gone */
			}
			try {
				spool.remove(transcriptPath ?? join(transcriptsDir, `${id}.jsonl`));
			} catch {
				/* the transcript is already gone */
			}
			try {
				spool.remove(join(worktreesDir, id));
			} catch {
				/* the worktree is already gone */
			}
		}
		// An abandoned log, transcript or worktree with no live record is bounded by
		// the same TTL.
		sweepExpiredEntries(spool, logsDir, now, {
			ttlMs,
			skip: name => {
				const job = readJob(name.replace(/\.log$/, ''));
				if (job === undefined) return false;
				const state = normalizeState(job.state);
				return state === undefined || !isTerminal(state);
			},
		});
		sweepExpiredEntries(spool, transcriptsDir, now, {
			ttlMs,
			// A transcript belongs to a job, so a running job's transcript is never
			// swept even when the file itself is old.
			skip: name => {
				const job = readJob(name.replace(/\.jsonl$/, ''));
				if (job === undefined) return false;
				const state = normalizeState(job.state);
				return state === undefined || !isTerminal(state);
			},
		});
		sweepExpiredEntries(spool, worktreesDir, now, {
			ttlMs,
			// A worktree belongs to a job, so the same "never sweep a running job" rule
			// applies: retention may remove a kept worktree only after its job is terminal.
			skip: name => {
				const job = readJob(name);
				if (job === undefined) return false;
				const state = normalizeState(job.state);
				return state === undefined || !isTerminal(state);
			},
		});
	}

	function hasPendingSignal(id: string): boolean {
		try {
			return (
				spool.readdir(inboxDir).some(name => name.includes(id)) ||
				spool.readdir(claimedDir).some(name => name.includes(id))
			);
		} catch {
			return false;
		}
	}

	// A crash leaves records in queued/running. Reconcile them by pid liveness: a
	// detached worker survives the harness, so a live pid means keep waiting; a dead
	// one with no signal on disk means the result is gone.
	function reconcileJobs(): void {
		if (workerMode) return;
		const now = runtime.now();
		for (const job of readRecentJobs(RECONCILE_SCAN_MAX)) {
			// Another session's job is its own business: it reconciles and signals it.
			// Touching it here would let one session write another's record and emit a
			// signal on its behalf.
			if (typeof job.owner === 'string' && job.owner.length > 0 && job.owner !== ownerToken) continue;
			const state = normalizeState(job.state);
			// A job that finished while no session was running still needs its worktree
			// decision; the record makes this idempotent.
			if (state === 'done' || state === 'failed' || state === 'canceled') finalizeJobWorktree(job);
			if (state === 'queued' || state === 'running') {
				if (runtime.alive(job.pid) || runtime.alive(job.workerPid)) continue;
				if (hasPendingSignal(job.id)) continue;
				const startedAt = Number(job.startedAt ?? 0) || 0;
				if (now - startedAt < LOST_GRACE_MS) continue;
				// One record, one signal, one delivery - the same contract every other
				// failure uses. A job already reconciled is terminal, so a second session
				// start cannot replay it.
				failJob(job, state === 'queued' ? 'worker never started' : 'lost (no live process)');
			} else if (state !== undefined && state !== job.state) {
				// A legacy alias is rewritten as its state, once.
				applyPatch(job.id, {state}, job, now);
			}
		}
	}

	let jobCache: JobRecord[] = [];
	let lastJobsSignature = '';
	let lastStatus = '';

	// How many signals are waiting for this session. It claims through the same
	// next()/settle() pair the drain uses, so the count cannot drift from delivery.
	function countPending(): number {
		const seen = new Set<string>();
		let count = 0;
		while (count < PENDING_SCAN_MAX) {
			const claim = next(seen);
			if (claim === undefined) break;
			count += 1;
			settle(claim, 'held');
		}
		return count;
	}

	function countOrphaned(): number {
		try {
			return spool.readdir(orphanedDir).length;
		} catch {
			return 0;
		}
	}

	function updateStatus(): void {
		try {
			const ui = (cmd as unknown as {ui?: {setStatus?: (text: string | null) => void; capabilities?: {status?: boolean}}}).ui;
			if (ui === undefined || typeof ui.setStatus !== 'function') return;
			if (ui.capabilities !== undefined && ui.capabilities.status === false) return;
			const counts: Record<string, number> = {};
			for (const job of jobCache) {
				const state = canonicalState(job.state);
				counts[state] = (counts[state] ?? 0) + 1;
			}
			const pending = countPending();
			const parts: string[] = [];
			if (counts.running) parts.push(`${counts.running} running`);
			if (counts.queued) parts.push(`${counts.queued} queued`);
			if (counts['waiting-input']) parts.push(`${counts['waiting-input']} waiting`);
			if (pending) parts.push(`${pending} signal${pending === 1 ? '' : 's'}`);
			// A success leaves the footer hint for 30 seconds after its row disappears.
			if (tasksHintVisible(jobCache, runtime.now())) parts.push('/tasks');
			const text = parts.length > 0 ? `◈ ${parts.join(' · ')}` : '';
			if (text === lastStatus) return;
			lastStatus = text;
			ui.setStatus(text.length > 0 ? text : null);
		} catch {
			/* status is a convenience, never load-bearing */
		}
	}

	// SubagentStop is the counterpart to SubagentStart. The parent observes the
	// terminal record and fires once; the marker is on the record, so a refresh, a
	// restart reconcile, and a second session cannot fire it twice.
	function sweepSubagentStopHooks(): void {
		if (workerMode) return;
		const owner = ownerId();
		for (const job of jobCache) {
			const state = normalizeState(job.state);
			if (state === undefined || !isTerminal(state) || job.subagentStopHook === true) continue;
			if (typeof job.owner === 'string' && job.owner.length > 0 && job.owner !== owner) continue;
			runHookRules(runtime.run, projectHookRules('SubagentStop'), {
				event: 'SubagentStop',
				cwd: String(job.cwd ?? runtime.cwd),
				agent: job.agentType,
				input: {id: job.id, state},
			});
			writeJob({...job, subagentStopHook: true, updatedAt: runtime.now()});
		}
	}

	function refreshJobs(): void {
		jobCache = readRecentJobs(LIST_SCAN_MAX);
		sweepSubagentStopHooks();
		updateStatus();
		const signature = jobCache.map(job => `${job.id}:${canonicalState(job.state)}`).join('|');
		if (signature !== lastJobsSignature) {
			lastJobsSignature = signature;
			try {
				cmd.state.set('jobsTick', runtime.now());
			} catch {
				/* pre-bind or a host without state */
			}
		}
	}

	function listText(filter: {state?: string; limit?: number} = {}): string {
		const all = readRecentJobs(LIST_SCAN_MAX);
		const wanted = filter.state === undefined ? undefined : canonicalState(filter.state);
		const matching = wanted === undefined ? all : all.filter(job => canonicalState(job.state) === wanted);
		const shown = matching.slice(0, filter.limit ?? LIST_SCAN_MAX);
		const counts: Record<string, number> = {};
		for (const job of all) {
			const state = canonicalState(job.state);
			counts[state] = (counts[state] ?? 0) + 1;
		}
		const summary = Object.entries(counts).map(([state, count]) => `${count} ${state}`).join(', ') || 'none';
		const pending = countPending();
		const orphaned = countOrphaned();
		const lines = shown.map(job => {
			const state = canonicalState(job.state);
			const exit = typeof job.exitCode === 'number' ? ` exit ${job.exitCode}` : '';
			const elapsed =
				typeof job.durationMs === 'number'
					? `${(job.durationMs / 1000).toFixed(1)}s`
					: typeof job.startedAt === 'number'
						? `${Math.max(0, Math.round((runtime.now() - job.startedAt) / 1000))}s…`
						: '';
			const duration = elapsed.length > 0 ? ` ${elapsed}` : '';
			const model = typeof job.model === 'string' && job.model.length > 0 ? ` · ${job.model}` : '';
			const effort = typeof job.effort === 'string' && job.effort.length > 0 ? ` · effort=${job.effort}` : '';
			const color = typeof job.color === 'string' && job.color.length > 0 ? ` · color=${job.color}` : '';
			const owner = typeof job.owner === 'string' && job.owner.length > 0 ? ` · ${job.owner.slice(-6)}` : '';
			const unread = isTerminalState(state) && job.deliveredAt === undefined ? ' · unread' : '';
			// The type is the resolved definition name (canonical `subagent_type`); the
			// agent name is the spawn label. Shell jobs have neither.
			const agentType =
				typeof job.agentType === 'string' && job.agentType.length > 0
					? ` · subagent_type=${job.agentType}`
					: job.kind === 'agent'
						? ' · subagent_type=general-purpose'
						: '';
			return `  ${stateMarker(state)} ${job.id}  ${String(job.label ?? '(unlabelled)')}  [${state}${exit}${duration}]${agentType}${model}${effort}${color}${owner}${unread}`;
		});
		const orphanNote = orphaned > 0 ? ` ${orphaned} orphaned signal(s) quarantined.` : '';
		const filterNote = wanted === undefined ? '' : ` matching ${wanted}`;
		return (
			`${matching.length} job(s)${filterNote}, ${all.length} recent job(s): ${summary}. ` +
			`${pending} signal(s) waiting for this session.${orphanNote}\n` +
			(lines.length > 0 ? lines.join('\n') : '  (none)')
		);
	}

	function killPid(pid: unknown, signal: string): void {
		if (typeof pid !== 'number' || !Number.isInteger(pid) || pid <= 0) return;
		try {
			runtime.kill(pid, signal as never);
		} catch {
			/* already gone */
		}
	}

	function cancelJob(id: string): {ok: boolean; message: string} {
		const job = readJob(id);
		if (job === undefined) return {ok: false, message: `subagent: no job ${id}`};
		const state = normalizeState(job.state);
		if (state !== undefined && isTerminal(state)) {
			// A live agent's record is terminal after its first turn, but its worker is
			// still addressable. Stopping it is a process stop, not a state change.
			if (!liveState(job).alive) return {ok: false, message: `subagent: job ${id} is already ${state}`};
			try {
				spool.mkdir(join(controlRoot, id));
				spool.writeFile(join(controlRoot, id, 'cancel'), '1');
			} catch {
				/* the kill below still stops it */
			}
			killPid(job.pid, 'SIGTERM');
			killPid(job.workerPid, 'SIGTERM');
			return {ok: true, message: `subagent: stopped live agent ${id} (${String(job.label ?? 'unlabelled')})`};
		}

		const now = runtime.now();
		const startedAt = Number(job.startedAt ?? now) || now;
		// Signal from the record, record write from the signal: one envelope.
		const signal = signalOf(
			{
				...job,
				owner: typeof job.owner === 'string' && job.owner.length > 0 ? job.owner : ownerId(),
				startedAt,
			},
			{state: 'canceled', exitCode: null, finishedAt: now, durationMs: Math.max(0, now - startedAt), body: 'canceled by request'},
		);
		if (!applyPatch(id, {...patchOf(signal), canceledAt: now}, job, now)) {
			return {ok: false, message: `subagent: job ${id} is already ${canonicalState(job.state)}`};
		}

		killPid(job.pid, 'SIGTERM');
		killPid(job.workerPid, 'SIGTERM');
		const escalate = (): void => {
			killPid(job.pid, 'SIGKILL');
			killPid(job.workerPid, 'SIGKILL');
		};
		try {
			cmd.clock.after(1000, escalate);
		} catch {
			setTimeout(escalate, 1000).unref?.();
		}

		enqueueSignal(signal);
		safeDrain();
		refreshJobs();
		return {ok: true, message: `subagent: canceled ${id} (${String(job.label ?? 'unlabelled')})`};
	}

	// ------------------------------------------------------------------ launch
	//
	// fire(intent) is the only way a job starts and failJob(job, reason) is the only
	// way one fails: both write the record and emit the signal, so every failure mode
	// (and every guard a later phase adds) produces exactly one result. A guard
	// rejects before the spawn and calls failJob(); nothing writes a failed record by
	// hand.

	// Ctrl+B: a foreground tool call that is already waiting. The next poll sees the
	// id here and returns control to the model; the worker keeps running.
	const backgroundedForegrounds = new Set<string>();
	const dismissedTasks = new Set<string>();
	let selectedTaskId: string | undefined;
	let agentViewOpen = false;
	let panelNotice: string | undefined;

	interface JobIntent {
		readonly label: string;
		readonly kind: 'command' | 'agent' | 'fork';
		readonly command?: string;
		readonly prompt?: string;
		readonly cwd: string;
		/** the resolved definition name: the canonical `subagent_type` */
		readonly agentType?: string;
		/** Explore and Plan are one-shot: no agent id, no resume. */
		readonly oneShot?: boolean;
		/** A fork: inherit the main conversation instead of building a fresh one. */
		readonly fork?: boolean;
		/** The main conversation the fork copies. */
		readonly mainSessionId?: string;
		/** The team this teammate belongs to. */
		readonly teamName?: string;
		/** This job's nesting level, and the job that spawned it. */
		readonly depth?: number;
		readonly parentJobId?: string;
		/** The definition body, delivered as the worker's system prompt */
		readonly systemPrompt?: string;
		/** The rendered startup bundle: instructions, git status, skills, memory */
		readonly startup?: string;
		readonly model?: string;
		readonly maxTurns?: number;
		readonly effort?: string;
		/** The resolved tool capabilities for an agent job. */
		readonly tools?: string[];
		readonly disallowedTools?: string[];
		/** The MCP servers scoped to this agent. */
		readonly mcpServers?: AgentMcpServer[];
		/** The resolved auto-compact window; only ever lowers the inherited one. */
		readonly autoCompactWindow?: number;
		readonly cacheTtl?: CacheTtl;
		readonly color?: string;
		readonly permissionMode?: string;
		readonly isolation?: string;
		/** The resolved execution policy: false waits for the result in the tool call. */
		readonly background?: boolean;
		/** SIGTERM then SIGKILL after this long; 0 or absent means no timeout */
		readonly timeoutMs?: number;
		/** resume this ACP session id first */
		readonly resumeSession?: string;
		readonly resumedFrom?: string;
		/** addressable name; a named agent can be messaged after it finishes */
		readonly name?: string;
		/** override the owner (a worker resuming a session it did not create) */
		readonly owner?: string;
		/** keep the ACP session open for live messages after the first turn */
		readonly live?: boolean;
	}

	type Launch =
		| {readonly ok: true; readonly id: string; readonly pid: number; readonly result?: ForegroundResult}
		| {readonly ok: false; readonly error: string};

	function failJob(job: JobRecord, reason: string): void {
		const now = runtime.now();
		const signal = signalOf(job, {
			state: 'failed',
			finishedAt: now,
			durationMs: Math.max(0, now - Number(job.startedAt ?? now)),
			body: reason,
		});
		// The state table decides whether this job may still fail: a terminal job keeps
		// whatever result it already has, and emits nothing.
		if (!applyPatch(job.id, patchOf(signal), job, now)) return;
		enqueueSignal(signal);
		refreshJobs();
	}

	// The worker session name, deterministic from the job id. It stays on the record
	// for display and for records written before ACP; a resume targets acpSessionId.
	function sessionNameOf(id: string): string {
		return `subagent-${id}`;
	}

	// What the job is: one record, before anything is spawned.
	function makeRecord(
		intent: JobIntent,
		id: string,
		logPath: string,
		startedAt: number,
		owner: string | undefined,
	): JobRecord {
		const isAgent = intent.prompt !== undefined;
		return {
			id,
			owner,
			label: intent.label,
			kind: intent.kind,
			...(intent.name !== undefined ? {name: intent.name} : {}),
			state: 'queued',
			// The resolved policy, not the raw `run_in_background`: env and definition
			// overrides are already applied by the caller.
			background: intent.background !== false,
			cwd: intent.cwd,
			logPath,
			startedAt,
			sessionId,
			updatedAt: startedAt,
			...(intent.depth !== undefined ? {depth: intent.depth} : {}),
			...(intent.parentJobId !== undefined ? {parentJobId: intent.parentJobId} : {}),
			...(isAgent
				? {
						// The durable resume handle. A one-shot agent has none, so nothing can
						// invite a resume that the lifecycle forbids.
						...(intent.oneShot === true ? {} : {agentId: id}),
						agentType: intent.agentType ?? 'general-purpose',
						sessionName: sessionNameOf(id),
						transcriptPath: join(transcriptsDir, `${id}.jsonl`),
						...(intent.resumeSession !== undefined ? {resumedFrom: intent.resumedFrom} : {}),
					}
				: {}),
			...(intent.fork === true
				? {fork: true, promptCache: 'shared', ...(intent.mainSessionId !== undefined ? {mainSessionId: intent.mainSessionId} : {})}
				: isAgent
					? {promptCache: 'own'}
					: {}),
			...(intent.teamName !== undefined ? {teamName: intent.teamName} : {}),
			...(intent.model !== undefined ? {model: intent.model} : {}),
			...(intent.tools !== undefined ? {tools: intent.tools} : {}),
			...(intent.mcpServers !== undefined ? {mcpServers: intent.mcpServers} : {}),
			...(intent.autoCompactWindow !== undefined ? {autoCompactWindow: intent.autoCompactWindow} : {}),
			...(intent.cacheTtl !== undefined ? {cacheTtl: intent.cacheTtl} : {}),
			...(intent.color !== undefined ? {color: intent.color} : {}),
			...(intent.maxTurns !== undefined ? {maxTurns: intent.maxTurns} : {}),
			...(intent.effort !== undefined ? {effort: intent.effort} : {}),
			...(intent.permissionMode !== undefined ? {permissionMode: intent.permissionMode} : {}),
			...(intent.isolation !== undefined ? {isolation: intent.isolation} : {}),
			...(intent.timeoutMs !== undefined && intent.timeoutMs > 0 ? {timeoutMs: intent.timeoutMs} : {}),
			...(intent.prompt !== undefined ? {prompt: intent.prompt} : {command: intent.command}),
		};
	}

	// How it runs: one detached worker, whose spec is built only by buildSpec.
	function startWorker(record: JobRecord, spec: WorkerSpec): Launch {
		const id = record.id;
		let worker: ChildHandle;
		try {
			worker = runtime.spawn(runtime.execPath, ['-e', WORKER_SOURCE], {
				cwd: spec.cwd,
				detached: true,
				stdio: 'ignore',
				env: workerEnv(runtime.env, spec),
			});
		} catch (error) {
			const reason = `spawn error: ${String((error as Error)?.message ?? error)}`;
			failJob(record, reason);
			return {ok: false, error: reason};
		}

		// A spawn failure is also an async 'error' event; without a listener it would be
		// an uncaught exception in the host, and the job would sit at `queued` forever.
		worker.on('error', error => {
			failJob(readJob(id) ?? record, `worker spawn error: ${String((error as Error)?.message ?? error)}`);
		});
		worker.unref();

		if (typeof worker.pid !== 'number') {
			const reason = 'the worker process did not start';
			failJob(readJob(id) ?? record, reason);
			return {ok: false, error: reason};
		}

		// Merge the pid onto the freshest record: the worker may already have written
		// `running`, and a blind overwrite would drag it back to `queued`.
		const latest = readJob(id) ?? record;
		latest.workerPid = worker.pid;
		latest.updatedAt = runtime.now();
		writeJob(latest);
		// SubagentStart is a project hook: it runs once the job actually started.
		runHookRules(runtime.run, projectHookRules('SubagentStart'), {
			event: 'SubagentStart',
			cwd: spec.cwd,
			agent: spec.agentType,
			input: {id, label: record.label, kind: record.kind},
		});
		refreshJobs();
		return {ok: true, id, pid: worker.pid};
	}

	// The pool is per session: only this session's queued and running jobs occupy
	// slots. Records are the source of truth, so a cancel in another process frees
	// one, and a job waiting for input has no worker process to count.
	function activeJobCount(): number {
		const owner = ownerId();
		let count = 0;
		for (const job of readRecentJobs(ACTIVE_JOB_SCAN_MAX)) {
			if (job.owner !== owner) continue;
			const state = normalizeState(job.state);
			if (state !== undefined && state !== 'queued' && state !== 'running') continue;
			count += 1;
		}
		return count;
	}

	async function fire(intent: JobIntent): Promise<Launch> {
		// Both limits are checked before a record exists, so a refused spawn leaves no
		// job behind and emits no signal.
		const depth = depthOf(workerSpec) + 1;
		if (intent.prompt !== undefined) {
			const maxDepth = resolveMaxDepth(runtime.env);
			if (depth > maxDepth) {
				return {
					ok: false,
					error: `Subagent nesting depth limit reached (${maxDepth}). This subagent cannot spawn children; do not retry.`,
				};
			}
		}
		const maxConcurrent = resolveMaxConcurrent(runtime.env);
		if (activeJobCount() >= maxConcurrent) {
			return {
				ok: false,
				error: `Concurrent subagent limit reached (${maxConcurrent}). Do not retry; wait for a running subagent to finish or cancel one first.`,
			};
		}
		const id = `${runtime.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
		const logPath = join(logsDir, `${id}.log`);
		const startedAt = runtime.now();
		// Worktree isolation is created before anything runs: the record, the worker
		// cwd and the guard all carry one path. A creation failure is one failed job.
		const isolation =
			intent.isolation === 'worktree'
				? createJobWorktree({git: git.git, fs: spool}, {cwd: intent.cwd, path: join(worktreesDir, id)})
				: undefined;
		const owner = intent.owner ?? ownerId();
		const record = makeRecord(
			{...intent, depth, ...(workerSpec?.id !== undefined ? {parentJobId: workerSpec.id} : {})},
			id,
			logPath,
			startedAt,
			owner,
		);
		if (isolation?.ok === true) {
			record.worktree = isolation.path;
			record.worktreeRepo = isolation.mainCheckouts[0];
			record.mainCheckouts = isolation.mainCheckouts;
			record.cwd = isolation.path;
		}
		writeJob(record);
		if (isolation !== undefined && !isolation.ok) {
			failJob(record, isolation.error);
			return {ok: false, error: isolation.error};
		}
		try {
			cmd.session?.appendCustomEntry({
				customType: JOB_ENTRY_TYPE,
				data: {id, label: intent.label, kind: intent.kind, state: 'queued', owner, sessionId},
			});
		} catch {
			/* audit trail is optional */
		}

		const isAgent = intent.prompt !== undefined;
		const live = isAgent && intent.live !== false;
		// The roster is for an agent that can actually message a peer. Host definitions
		// name the tool SendMessage; the mod registers it as subagent_send. A fork
		// already has the conversation (and its roster) and gets no fresh bundle.
		const canSend =
			intent.tools === undefined || intent.tools.includes('SendMessage') || intent.tools.includes('subagent_send');
		const roster = isAgent && intent.fork !== true && canSend ? rosterFor(intent.name) : {rosterNames: []};
		const idleMs = Number(runtime.env.BETTER_SUBAGENTS_LIVE_IDLE_MS);
		if (live) {
			try {
				spool.mkdir(join(controlRoot, id));
			} catch {
				/* the first send retries the mkdir */
			}
		}
		const spec = buildSpec(
			{
				id,
				label: intent.label,
				kind: intent.kind,
				owner,
				cwd: isolation?.ok === true ? isolation.path : intent.cwd,
				logPath,
				inboxDir,
				jobsDir,
				startedAt,
				tailMax: LOG_TAIL_BYTES_MAX,
				bin: isAgent ? (runtime.env.COMMANDCODE_BIN ?? 'cmd') : '/bin/sh',
				sessionId,
				...(intent.name !== undefined ? {name: intent.name} : {}),
				background: intent.background !== false,
				// A foreground caller answers permission prompts inline; a background run
				// settles with the request instead of holding a worker that nobody watches.
				...(intent.background === false
					? {interactive: true, controlDir: join(jobsDir, '..', 'control', id)}
					: {}),
				depth,
				...(intent.agentType !== undefined ? {agentType: intent.agentType} : {}),
				...(record.agentId !== undefined ? {agentId: record.agentId} : {}),
				...(record.transcriptPath !== undefined ? {transcriptPath: record.transcriptPath} : {}),
				...(intent.fork === true
					? {
							fork: true,
							promptCache: 'shared',
							sessionName: sessionNameOf(id),
							...(intent.mainSessionId !== undefined ? {mainSessionId: intent.mainSessionId} : {}),
						}
					: isAgent
						? {promptCache: 'own'}
						: {}),
				...(intent.teamName !== undefined ? {teamName: intent.teamName} : {}),
				...(intent.systemPrompt !== undefined ? {systemPrompt: intent.systemPrompt} : {}),
				...(intent.startup !== undefined ? {startup: intent.startup} : {}),
				...(intent.model !== undefined ? {model: intent.model} : {}),
				...(intent.tools !== undefined ? {tools: intent.tools} : {}),
				...(intent.disallowedTools !== undefined ? {disallowedTools: intent.disallowedTools} : {}),
				...(intent.mcpServers !== undefined ? {mcpServers: intent.mcpServers} : {}),
				...(intent.autoCompactWindow !== undefined ? {autoCompactWindow: intent.autoCompactWindow} : {}),
				...(intent.cacheTtl !== undefined ? {cacheTtl: intent.cacheTtl} : {}),
				...(intent.maxTurns !== undefined ? {maxTurns: intent.maxTurns} : {}),
				...(intent.effort !== undefined ? {effort: intent.effort} : {}),
				...(intent.permissionMode !== undefined ? {permissionMode: intent.permissionMode} : {}),
				...(intent.timeoutMs !== undefined && intent.timeoutMs > 0 ? {timeoutMs: intent.timeoutMs} : {}),
				...(intent.resumeSession !== undefined ? {resumeSession: intent.resumeSession} : {}),
				...(intent.prompt !== undefined ? {prompt: intent.prompt} : {command: intent.command}),
				...(roster.roster !== undefined ? {roster: roster.roster} : {}),
				...(roster.rosterNames.length > 0 ? {rosterNames: roster.rosterNames} : {}),
				...(intent.background !== undefined ? {background: intent.background} : {}),
				...(live ? {live: true, controlDir: join(controlRoot, id), ...(Number.isFinite(idleMs) && idleMs > 0 ? {idleMs} : {})} : {}),
				...(isAgent ? {promptMarkerPath: join(logsDir, `${id}.mod`)} : {}),
				...(isolation?.ok === true ? {worktree: isolation.path, mainCheckouts: isolation.mainCheckouts} : {}),
			},
			MOD_PATH,
		);
		const launched = startWorker(record, spec);
		if (!launched.ok || intent.background !== false) return launched;
		return settleForeground(launched);
	}

	// Foreground is a bounded wait on the same durable record a background job
	// leaves behind. Completion is the record, never the process: a worker that died
	// after writing its result must still return it.
	async function settleForeground(launched: {readonly id: string; readonly pid: number}): Promise<Launch> {
		const rounds = Math.ceil(FOREGROUND_WAIT_MAX_MS / FOREGROUND_POLL_MS) + 1;
		let record = readJob(launched.id);
		for (let round = 0; round < rounds && !isTerminalState(record?.state); round += 1) {
			if (backgroundedForegrounds.has(launched.id)) {
				// Ctrl+B: stop waiting, but leave the worker alone. Its result now arrives
				// as a normal signal, exactly like a spawn that asked for the background.
				refreshJobs();
				return {
					ok: true,
					id: launched.id,
					pid: launched.pid,
					result: {
						ok: true,
						partial: true,
						text: `${String(record?.label ?? launched.id)} continues in the background; its result will arrive as a signal.`,
					},
				};
			}
			await runtime.sleep(FOREGROUND_POLL_MS);
			record = readJob(launched.id);
		}
		if (record === undefined || !isTerminalState(record.state)) {
			// Past the bound the run keeps going. Demote it so its result is delivered as
			// a signal instead of being lost with the tool call that gave up on it.
			if (record !== undefined) writeJob({...record, background: true, updatedAt: runtime.now()});
			refreshJobs();
			return {
				ok: false,
				error:
					`foreground subagent ${launched.id} did not finish within ${Math.round(FOREGROUND_WAIT_MAX_MS / 1000)}s; ` +
					'it continues in the background, and its result will arrive as a signal',
			};
		}
		const log = readLog(String(record.logPath ?? join(logsDir, `${launched.id}.log`)));
		return {...launched, result: foregroundOutcome(record, log)};
	}

	// ------------------------------------------------------------------ dashboard

	// The task view reads the same 30-second rule the row lifecycle does. Selection
	// is an id, not an index, so a row finishing during a move cannot shift the
	// selection onto its neighbour.
	function visibleTaskRows(): JobRecord[] {
		return taskPanelRows(jobCache, {now: runtime.now(), dismissed: dismissedTasks});
	}

	function selectedTask(): JobRecord | undefined {
		if (selectedTaskId === undefined) return undefined;
		return visibleTaskRows().find(job => job.id === selectedTaskId);
	}

	function selectTask(delta: number): JobRecord | undefined {
		const rows = visibleTaskRows();
		selectedTaskId = moveTaskSelection(rows, selectedTaskId, delta);
		cmd.ui.refreshWidgets();
		return selectedTask();
	}

	function dismissOrStopTask(id: string): {ok: boolean; message: string} {
		const job = readJob(id);
		if (job === undefined) return {ok: false, message: `subagent: no job ${id}`};
		const state = canonicalState(job.state);
		if (state === 'queued' || state === 'running') return cancelJob(id);
		dismissedTasks.add(id);
		selectedTaskId = selectedTaskId === id ? undefined : selectedTaskId;
		refreshJobs();
		cmd.ui.refreshWidgets();
		return {ok: true, message: `subagent: dismissed ${id} from the task view`};
	}

	// Ctrl+B. The newest foreground run is the one the user is waiting on; a
	// background spawn is already background and is skipped.
	function backgroundForegroundTask(): JobRecord | undefined {
		const job = jobCache.find(candidate => {
			const state = canonicalState(candidate.state);
			return candidate.background === false && (state === 'queued' || state === 'running');
		});
		if (job === undefined) return undefined;
		writeJob({...job, background: true, backgroundedAt: runtime.now(), updatedAt: runtime.now()});
		backgroundedForegrounds.add(job.id);
		refreshJobs();
		return job;
	}

	cmd.ui.pane({
		id: 'jobs',
		title: 'Async subagents',
		render: () => {
			try {
				const rows = visibleTaskRows();
				const nodes: unknown[] = [
					{
						type: 'row',
						children: [
							{type: 'text', text: '◈ subagents', bold: true},
							{type: 'text', text: ` ${rows.length} task${rows.length === 1 ? '' : 's'} · ${countPending()} waiting`, dim: true},
						],
					},
					{type: 'divider'},
				];
				if (rows.length === 0) {
					nodes.push({type: 'text', text: 'No tasks running or recently failed.', dim: true});
				}
				for (const job of rows.slice(0, PANE_ROWS_MAX)) {
					const state = canonicalState(job.state);
					const duration = typeof job.durationMs === 'number' ? ` ${(job.durationMs / 1000).toFixed(1)}s` : '';
					const color = typeof job.color === 'string' && job.color.length > 0 ? job.color : '';
					const selected = job.id === selectedTaskId ? '▸ ' : '  ';
					nodes.push({
						type: 'text',
						text: `${selected}${stateMarker(state)} ${job.id}  ${String(job.label ?? '(unlabelled)').slice(0, 36)}  ${state}${duration}${color.length > 0 ? `  ${color}` : ''}`,
						// The definition color is the agent's identity; the state color is the
						// fallback, so an uncoloured job still reports its lifecycle.
						color: color.length > 0 ? color : stateColor(state),
					});
				}
				if (panelNotice !== undefined) {
					nodes.push({type: 'divider'}, {type: 'text', text: panelNotice});
				}
				nodes.push({
					type: 'row',
					children: [
						{
							type: 'button',
							label: 'Prev',
							hotkey: '↑',
							key: 'subagent-prev',
							onPress: () => selectTask(-1),
						},
						{
							type: 'button',
							label: 'Next',
							hotkey: '↓',
							key: 'subagent-next',
							onPress: () => selectTask(1),
						},
						{
							type: 'button',
							label: 'Enter',
							hotkey: '↵',
							key: 'subagent-open',
							onPress: () => {
								const job = selectedTask();
								if (job === undefined) return;
								agentViewOpen = true;
								const shown = outputText(job.id, {});
								panelNotice = shown.ok
									? String(shown.content?.[0]?.text ?? '').slice(0, 4000)
									: String(shown.error ?? 'cannot read the task');
								cmd.ui.refreshWidgets();
							},
						},
						{
							type: 'button',
							label: 'x',
							hotkey: 'x',
							key: 'subagent-dismiss',
							onPress: () => {
								const job = selectedTask();
								if (job !== undefined) dismissOrStopTask(job.id);
							},
						},
						{
							type: 'button',
							label: 'Esc',
							hotkey: 'esc',
							key: 'subagent-back',
							onPress: () => {
								selectedTaskId = undefined;
								agentViewOpen = false;
								cmd.ui.refreshWidgets();
							},
						},
						{
							type: 'button',
							label: 'Refresh',
							hotkey: 'r',
							key: 'subagent-refresh',
							onPress: () => {
								refreshJobs();
								cmd.ui.refreshWidgets();
							},
						},
						{
							type: 'button',
							label: 'Cancel newest',
							hotkey: 'c',
							key: 'subagent-cancel',
							onPress: () => {
								const active = jobCache.find(job => {
									const state = canonicalState(job.state);
									return state === 'queued' || state === 'running';
								});
								if (active !== undefined) cancelJob(active.id);
								refreshJobs();
								cmd.ui.refreshWidgets();
							},
						},
					],
				});
				return nodes as never;
			} catch {
				return ['subagent: cannot render the jobs pane'] as never;
			}
		},
	});

	// ------------------------------------------------------------------ tools
	//
	// `subagent_*` is the surface. `pubsub_*` is registered from the same run function
	// for one release: an installed prompt, a script, or a habit that still calls the
	// old name keeps working, and cannot drift from the new one because there is only
	// one implementation.
	type ToolResult = {ok: boolean; error?: string; content?: {type: string; text: string}[]};
	const okText = (value: string): ToolResult => ({ok: true, content: [{type: 'text', text: value}]});
	const badInput = (error: string): ToolResult => ({ok: false, error});

	function addSurface(
		name: string,
		description: string,
		inputSchema: Record<string, unknown>,
		run: (args: {input: Record<string, unknown>}) => Promise<ToolResult>,
		options: {readOnly?: boolean; aliases?: string[]} = {},
	): void {
		const readOnly = options.readOnly === true;
		cmd.addTool({schema: {name, description, input_schema: inputSchema}, ...(readOnly ? {readOnly} : {}), run} as never);
		for (const alias of options.aliases ?? []) {
			cmd.addTool({
				schema: {name: alias, description: `[deprecated: use ${name}] ${description}`, input_schema: inputSchema},
				...(readOnly ? {readOnly} : {}),
				run,
			} as never);
		}
	}

	// The fire-and-forget tool. Returns the moment the worker is detached; the result
	// arrives later as a signal, never as a tool result.
	const spawnRun = async ({input}: {input: Record<string, unknown>}): Promise<ToolResult> => {
		const label = String(input.description ?? input.label ?? '').trim();
		if (label.length === 0) return badInput('description is required and must not be empty');
		const command = typeof input.command === 'string' && input.command.trim().length > 0 ? input.command : undefined;
		const prompt = typeof input.prompt === 'string' && input.prompt.trim().length > 0 ? input.prompt : undefined;
		if (command === undefined && prompt === undefined) {
			return badInput('pass either prompt (a headless agent) or command (a shell command)');
		}
		if (command !== undefined && prompt !== undefined) return badInput('pass prompt or command, not both');
		// permission_mode is resolved below instead of being rejected, and tools are
		// resolved by resolveLaunch. Isolation is chosen by a definition's frontmatter.
		if (input.isolation !== undefined) {
			return badInput("isolation is chosen by a definition's frontmatter (`isolation: worktree`), not per call; omit it");
		}
		const requestedMode =
			typeof input.permission_mode === 'string' && input.permission_mode.trim().length > 0
				? input.permission_mode.trim()
				: undefined;
		if (requestedMode !== undefined && canonicalPermissionMode(requestedMode) === undefined) {
			return badInput(`unknown permission_mode "${requestedMode}"`);
		}
		if (requestedMode !== undefined && prompt === undefined) {
			return badInput('permission_mode applies to agent jobs; a shell command has no permission mode');
		}
		const model = typeof input.model === 'string' && input.model.trim().length > 0 ? input.model.trim() : undefined;
		const name = typeof input.name === 'string' && input.name.trim().length > 0 ? input.name.trim() : undefined;
		const teamName =
			typeof input.team_name === 'string' && input.team_name.trim().length > 0 ? input.team_name.trim() : undefined;
		const requestedMaxTurns = Number(input.max_turns);
		const timeoutMs = Number(input.timeout_ms);
		// `subagent_type` is canonical; `agent_type` is the 1.x alias and must behave
		// identically. Passing both with different values is ambiguous, not a merge.
		const subagentType =
			typeof input.subagent_type === 'string' && input.subagent_type.trim().length > 0
				? input.subagent_type.trim()
				: undefined;
		const aliasType =
			typeof input.agent_type === 'string' && input.agent_type.trim().length > 0 ? input.agent_type.trim() : undefined;
		if (subagentType !== undefined && aliasType !== undefined && subagentType !== aliasType) {
			return badInput('pass subagent_type or its deprecated alias agent_type, not both');
		}

		let definition: AgentDefinition | undefined;
		if (prompt !== undefined) {
			// Omitted type resolves to general-purpose, or fails with the documented
			// error when no general-purpose agent exists.
			const resolved = resolveAgentDefinition(subagentType ?? aliasType);
			if (!resolved.ok) return badInput(resolved.error);
			definition = resolved.definition;
		}
		const forkRequested = definition?.fork === true;
		const teammate = prompt !== undefined && teamName !== undefined;
		const requestedBackground = typeof input.run_in_background === 'boolean' ? input.run_in_background : undefined;
		// A teammate's definition may not pin it to the background, and a caller may
		// not ask for it while fork mode still exposes the parameter.
		const teamPolicy = resolveTeamPolicy({
			teammate,
			definitionBackground: definition?.background,
			...(requestedBackground !== undefined ? {requested: requestedBackground} : {}),
			forkMode,
		});
		if (!teamPolicy.ok) return badInput(teamPolicy.error);
		const requestedTools =
			prompt === undefined || forkRequested ? undefined : (listField(input.tools) ?? definition?.tools);
		const requestedDisallowed =
			prompt === undefined || forkRequested ? undefined : (listField(input.disallowed_tools) ?? definition?.disallowedTools);
		// The definition's mode and the main session's mode are combined here, once.
		// A definition asking for bypass in a default session is refused before the
		// record exists, so no job is left queued behind a mode that cannot run. A
		// fork skips this: it inherits the main session's permissions.
		let permissionMode: string | undefined;
		if (prompt !== undefined) {
			if (forkRequested) {
				permissionMode = mainPermissionMode ?? 'default';
			} else {
				const modeResolution = resolvePermissionMode(requestedMode ?? definition?.permissionMode, mainPermissionMode);
				if (!modeResolution.ok) return badInput(modeResolution.error);
				permissionMode = modeResolution.mode;
			}
		}
		// Three inputs, one precedence table: the disable env, the definition's pinned
		// background, then the caller's request. Fork mode overrides all of them for a
		// model-spawned agent (the parameter is gone from the schema) and a teammate
		// is always foreground.
		const background = teammate
			? false
			: forkModeEnabled
				? true
				: resolveExecutionPolicy({
						...(requestedBackground !== undefined ? {requested: requestedBackground} : {}),
						definitionBackground: definition?.background,
						env: runtime.env,
					}) === 'background';
		// MCP servers are resolved once, for this agent: a reference reads the host's
		// config, an inline entry needs a trusted folder, and bare/strict/allow/deny
		// still apply. The result scopes `session/new`; the tool pool is widened by
		// the same names so a definition can list `mcp__server__*`. A fork inherits
		// the main session's servers and skips this.
		const configuredMcp = mcpServerConfigs(spool, [runtime.home, runtime.cwd]);
		const mcp = resolveAgentMcpServers({
			defined: definition?.mcpServers ?? [],
			configured: configuredMcp,
			definition,
			env: runtime.env,
			trusted:
				definition === undefined ||
				agentFolderTrusted(definition, {
					cwd: runtime.cwd,
					home: runtime.home,
					managedDir: readFlag('managed_dir') || MANAGED_SETTINGS_DEFAULT,
					addDirs: splitFlag(ADD_DIRS_OPTION),
				}),
		});
		const mcpNames = Object.keys(configuredMcp);
		for (const server of mcp.servers) if (!mcpNames.includes(server.name)) mcpNames.push(server.name);
		const launch = forkRequested
			? {ok: true as const, model: undefined, modelSource: 'inherit' as const, effort: undefined, effortSource: 'none' as const, tools: [], removed: []}
			: resolveLaunch({
					...(model !== undefined ? {requestedModel: model} : {}),
					...(definition?.model !== undefined ? {definitionModel: definition.model} : {}),
					...(typeof input.effort === 'string' ? {requestedEffort: input.effort} : {}),
					...(definition?.effort !== undefined ? {definitionEffort: definition.effort} : {}),
					env: runtime.env,
					// The mod API exposes no main-model getter: an absent model means the ACP
					// child inherits the user's configured model, which is the main one.
					...(requestedTools !== undefined ? {tools: requestedTools} : {}),
					...(requestedDisallowed !== undefined ? {disallowedTools: requestedDisallowed} : {}),
					...(permissionMode !== undefined ? {permissionMode} : {}),
					background,
					...(teammate ? {team: true} : {}),
					mcpServers: mcpNames,
				});
		if (!launch.ok) return badInput(launch.error);
		if (forkRequested && (sessionId === undefined || sessionId.length === 0)) {
			return badInput('a fork needs the main session id; the session has not bound yet');
		}
		// Memory definitions auto-enable Read, Write, and Edit. A fork keeps the main
		// tool pool untouched, so it gets no filter and no memory additions.
		const launchTools = prompt === undefined || forkRequested ? undefined : withMemoryTools(launch.tools, definition?.memory);
		const maxTurns =
			prompt === undefined || forkRequested
				? undefined
				: Number.isFinite(requestedMaxTurns) && requestedMaxTurns > 0
					? Math.round(requestedMaxTurns)
					: definition?.maxTurns;
		const runCwd = typeof input.cwd === 'string' && input.cwd.length > 0 ? input.cwd : cmd.cwd;
		// The auto-compact window only lowers what this agent's own model gives it.
		const compactWindow = resolveAutoCompactWindow(
			contextWindowForModel(launch.model, runtime.env),
			definition?.autoCompactWindow,
			false,
		);
		const launched = await fire({
			label,
			kind: forkRequested ? 'fork' : prompt !== undefined ? 'agent' : 'command',
			command,
			prompt,
			background,
			cwd: runCwd,
			...(name !== undefined ? {name} : {}),
			...(teamName !== undefined ? {teamName} : {}),
			agentType: definition?.name,
			oneShot: definition?.oneShot,
			...(forkRequested
				? {fork: true, ...(sessionId !== undefined ? {mainSessionId: sessionId} : {})}
				: {}),
			systemPrompt: forkRequested ? undefined : subagentSystemPrompt(definition?.systemPrompt),
			startup: forkRequested ? undefined : startupFor(definition, runCwd, prompt, launch.model),
			model: prompt === undefined || forkRequested ? undefined : launch.model,
			tools: launchTools,
			...(requestedDisallowed !== undefined ? {disallowedTools: requestedDisallowed} : {}),
			...(definition?.color !== undefined ? {color: definition.color} : {}),
			...(prompt !== undefined && !forkRequested ? {mcpServers: mcp.servers} : {}),
			...(prompt !== undefined && !forkRequested && compactWindow !== undefined ? {autoCompactWindow: compactWindow} : {}),
			...(prompt !== undefined && !forkRequested && definition?.cacheTtl !== undefined ? {cacheTtl: definition.cacheTtl} : {}),
			maxTurns,
			effort: prompt === undefined || forkRequested ? undefined : launch.effort,
			permissionMode,
			isolation: forkRequested ? undefined : definition?.isolation,
			...(prompt === undefined ? {} : {background}),
			timeoutMs: Number.isFinite(timeoutMs) && timeoutMs > 0 ? Math.round(timeoutMs) : undefined,
		});
		if (!launched.ok) return badInput(launched.error);
		// A one-shot (Explore/Plan) has no agent id: the record is the only handle.
		const spawnedAgentId = readJob(launched.id)?.agentId;
		// A foreground run returns its result here; a background run returns the moment
		// the worker is detached.
		if (launched.result !== undefined) {
			if (!launched.result.ok) return badInput(launched.result.error);
			return okText(
				spawnedAgentId !== undefined ? `agent id ${spawnedAgentId}\n${launched.result.text}` : launched.result.text,
			);
		}
		const kind =
			definition !== undefined
				? `${forkRequested ? 'fork' : 'agent'} (${definition.name})${teamName !== undefined ? ` on team ${teamName}` : ''}`
				: 'shell (/bin/sh -c)';
		return okText(
			`spawned ${launched.id} "${label}" as ${kind}, pid ${launched.pid}.\n` +
				(spawnedAgentId !== undefined ? `agent id ${spawnedAgentId}\n` : '') +
				`log ${join(logsDir, `${launched.id}.log`)}\n` +
				`The result arrives here as a signal when it exits - do not wait for it. ` +
				`subagent_output ${launched.id} reads the live log; subagent_cancel ${launched.id} stops it.`,
		);
	};

	const listRun = async ({input}: {input: Record<string, unknown>}): Promise<ToolResult> => {
		refreshJobs();
		const limit = Number(input.limit);
		return okText(
			listText({
				state: typeof input.state === 'string' && input.state.length > 0 ? input.state : undefined,
				limit: Number.isFinite(limit) && limit > 0 ? Math.round(limit) : undefined,
			}),
		);
	};

	function readLog(file: string): string {
		try {
			return spool.readFile(file);
		} catch {
			return '';
		}
	}

	// The persisted conversation, rendered from the worker's JSONL stream. A
	// malformed line is skipped; it never hides the rest of the transcript.
	function transcriptText(id: string, record: JobRecord): string {
		const file = String(record.transcriptPath ?? join(transcriptsDir, `${id}.jsonl`));
		let raw = '';
		try {
			raw = spool.readFile(file);
		} catch {
			return '';
		}
		const parts: string[] = [];
		for (const line of raw.split('\n')) {
			if (!line.trim()) continue;
			try {
				const entry = JSON.parse(line) as {
					type?: unknown;
					line?: unknown;
					update?: {sessionUpdate?: unknown; content?: {text?: unknown}; title?: unknown; kind?: unknown};
				};
				const update = entry.update;
				if (update?.sessionUpdate === 'agent_message_chunk' && typeof update.content?.text === 'string') {
					parts.push(update.content.text);
				} else if (update?.sessionUpdate === 'tool_call') {
					parts.push(`[tool] ${String(update.title ?? update.kind ?? 'call')}\n`);
				} else if (typeof entry.line === 'string') {
					parts.push(`${entry.line}\n`);
				}
			} catch {
				/* one malformed line does not hide the transcript */
			}
		}
		return parts.join('');
	}

	// The transcript of one job: the record's facts, the log and transcript paths,
	// and a bounded slice of the conversation. A finished job falls back to its
	// transcript when the log is gone; `offset` is a character offset from the start.
	function outputText(id: string, input: Record<string, unknown>): ToolResult {
		const record = readJob(id);
		if (record === undefined) return badInput(`subagent: no job ${id}`);
		const state = canonicalState(record.state);
		const file = String(record.logPath ?? join(logsDir, `${id}.log`));
		const transcriptPath = String(record.transcriptPath ?? join(transcriptsDir, `${id}.jsonl`));
		const transcript = record.kind === 'agent' || record.kind === 'fork' ? transcriptText(id, record) : '';
		const logText = readLog(file);
		const log = logText.length > 0 ? logText : transcript;
		const requested = Number(input.max_chars);
		const maxChars = Math.min(
			OUTPUT_MAX_CHARS_CAP,
			Number.isFinite(requested) && requested > 0 ? Math.max(200, Math.round(requested)) : OUTPUT_MAX_CHARS_DEFAULT,
		);
		const offset = Number(input.offset);
		let body: string;
		let note = '';
		if (Number.isFinite(offset) && offset > 0) {
			body = log.slice(offset, offset + maxChars);
			note = `\n[chars ${offset}-${Math.min(log.length, offset + maxChars)} of ${log.length}]`;
		} else if (log.length > maxChars) {
			body = log.slice(-maxChars);
			note = `\n[last ${maxChars} of ${log.length} chars]`;
		} else {
			body = log;
		}
		const facts = [
			state,
			record.partial === true ? 'partial' : '',
			typeof record.agentId === 'string' ? `agent ${record.agentId}` : '',
			typeof record.exitCode === 'number' ? `exit ${record.exitCode}` : '',
			typeof record.durationMs === 'number' ? `${(record.durationMs / 1000).toFixed(1)}s` : '',
			typeof record.model === 'string' && record.model.length > 0 ? record.model : '',
			typeof record.agentType === 'string' ? record.agentType : '',
			typeof record.owner === 'string' && record.owner.length > 0 ? `owner ${record.owner.slice(-6)}` : '',
		].filter(Boolean).join(' · ');
		const timing =
			state === 'queued' || state === 'running'
				? `\n(running ${Math.max(0, Math.round((runtime.now() - Number(record.startedAt ?? runtime.now())) / 1000))}s; the result will arrive as a signal)`
				: '';
		// Reading the log is still worker output reaching the parent: scan it here too,
		// and hide the worker's own tool-call progress.
		const visible = reportText(body, String(record.kind ?? 'command'));
		return okText(
			`${stateMarker(record.state)} ${id} "${String(record.label ?? '(unlabelled)')}" · ${facts}\nlog ${file}` +
				(record.transcriptPath !== undefined ? `\ntranscript ${transcriptPath}` : '') +
				`${timing}\n\n` +
				(visible.length > 0 ? visible : '(no output yet)') +
				note,
		);
	}

	const outputRun = async ({input}: {input: Record<string, unknown>}): Promise<ToolResult> => {
		const id = String(input.id ?? '').trim();
		if (id.length === 0) return badInput('id is required');
		if (readJob(id) === undefined) return badInput(`subagent: no job ${id}`);
		const waitMs = Number(input.wait_ms);
		const wantsWait = input.wait === true || (Number.isFinite(waitMs) && waitMs > 0);
		if (wantsWait) {
			// Bounded on purpose: a tool that waits is a turn held open, so the default is
			// short and the cap is hard. The loop is counted, not clock-only, so a frozen
			// clock cannot spin it forever.
			const budget = Math.min(WAIT_MAX_MS, Number.isFinite(waitMs) && waitMs > 0 ? waitMs : WAIT_DEFAULT_MS);
			const rounds = Math.ceil(budget / WAIT_POLL_MS) + 1;
			for (let round = 0; round < rounds; round += 1) {
				const record = readJob(id);
				if (record === undefined || isTerminalState(record.state)) break;
				await runtime.sleep(WAIT_POLL_MS);
			}
		}
		return outputText(id, input);
	};

	const cancelRun = async ({input}: {input: Record<string, unknown>}): Promise<ToolResult> => {
		const id = String(input.id ?? '').trim();
		if (id.length === 0) return badInput('id is required');
		const result = cancelJob(id);
		return result.ok ? okText(result.message) : badInput(result.message);
	};

	const resumeRun = async ({input}: {input: Record<string, unknown>}): Promise<ToolResult> => {
		const id = String(input.id ?? '').trim();
		if (id.length === 0) return badInput('id is required');
		const instruction = String(input.prompt ?? '').trim();
		if (instruction.length === 0) return badInput('prompt is required: it is the instruction for the resumed run');
		// The agent id is the durable handle; the job id also resolves, so a caller
		// holding either from the result can resume.
		const record =
			readJob(id) ??
			readRecentJobs(ACTIVE_JOB_SCAN_MAX).find(job => typeof job.agentId === 'string' && job.agentId === id);
		if (record === undefined) return badInput(`subagent: no job or agent ${id}`);
		const jobId = record.id;
		const state = normalizeState(record.state);
		if (state !== undefined && !isTerminal(state)) return badInput(`subagent: job ${jobId} is still ${state}`);
		if (record.prompt === undefined) {
			return badInput(`subagent: job ${jobId} is a shell job; there is no agent session to resume`);
		}
		// Explore and Plan are one-shot: the lifecycle gives them no agent id, so a
		// resume would pretend to continue a context that was never addressable. An
		// older record written before agent ids is refused for the same reason.
		if (record.fork !== true && record.agentId === undefined) {
			return badInput(
				`subagent: job ${jobId} has no agent id (${String(record.agentType ?? 'explore or plan')} is one-shot, or the record predates agent ids); it cannot be resumed`,
			);
		}
		// A live worker already owns this session. Starting a second worker would race
		// the first over it, so the live path is the message path.
		if (liveState(record).alive) {
			return badInput(
				`subagent: job ${jobId} is still running; send it a live message with subagent_send` +
					(record.name === undefined ? ' once it finishes, or wait for its result' : `({to: "${record.name}", message}) instead`),
			);
		}
		// Resume the ACP session the first run created, so the new instruction continues
		// with the context the worker already has. An older record without one starts a
		// fresh session (the definition body still applies). A fork resumes its fork
		// session instead; forking again would lose the context it already has.
		if (record.fork === true) {
			if (record.forkSessionId === undefined) {
				return badInput(`subagent: job ${jobId} is a fork with no recorded session; spawn it again`);
			}
			const forkLaunched = await fire({
				label: `${String(record.label ?? jobId)} (resume)`,
				kind: 'fork',
				prompt: instruction,
				background: record.teamName !== undefined ? false : true,
				cwd: String(record.cwd ?? cmd.cwd),
				agentType: FORK_AGENT_TYPE,
				fork: true,
				...(record.mainSessionId !== undefined ? {mainSessionId: record.mainSessionId} : {}),
				resumeSession: record.forkSessionId,
				resumedFrom: jobId,
				...(record.permissionMode !== undefined ? {permissionMode: record.permissionMode} : {}),
				...(record.name !== undefined ? {name: record.name} : {}),
				...(record.teamName !== undefined ? {teamName: record.teamName} : {}),
				...(record.owner !== undefined ? {owner: record.owner} : {}),
			});
			if (!forkLaunched.ok) return badInput(forkLaunched.error);
			const forkAgentId = readJob(forkLaunched.id)?.agentId;
			if (forkLaunched.result !== undefined) {
				if (!forkLaunched.result.ok) return badInput(forkLaunched.result.error);
				return okText(
					forkAgentId !== undefined ? `agent id ${forkAgentId}\n${forkLaunched.result.text}` : forkLaunched.result.text,
				);
			}
			return okText(
				`resumed ${jobId} as ${forkLaunched.id} "${String(record.label ?? jobId)} (resume)", pid ${forkLaunched.pid}.\n` +
					(forkAgentId !== undefined ? `agent id ${forkAgentId}\n` : '') +
					`log ${join(logsDir, `${forkLaunched.id}.log`)}\n` +
					`The resumed fork reports back here as its own signal.`,
			);
		}
		const definition = record.agentType === undefined ? undefined : resolveAgentDefinition(record.agentType);
		const resumeCwd = String(record.cwd ?? cmd.cwd);
		const resumeDefinition = definition?.ok === true ? definition.definition : undefined;
		// A resume is a fresh job: it follows the default (background) and the
		// definition's pinned background, not the mode of the run it resumes.
		const background =
			resolveExecutionPolicy({
				definitionBackground: definition?.ok === true ? definition.definition.background : undefined,
				env: runtime.env,
			}) === 'background';
		const resumedMcp =
			resumeDefinition === undefined
				? (record.mcpServers ?? [])
				: resolveAgentMcpServers({
						defined: resumeDefinition.mcpServers ?? [],
						configured: mcpServerConfigs(spool, [runtime.home, runtime.cwd]),
						definition: resumeDefinition,
						env: runtime.env,
						trusted: agentFolderTrusted(resumeDefinition, {
							cwd: runtime.cwd,
							home: runtime.home,
							managedDir: readFlag('managed_dir') || MANAGED_SETTINGS_DEFAULT,
							addDirs: splitFlag(ADD_DIRS_OPTION),
						}),
					}).servers;
		const resumeCompact = record.autoCompactWindow ?? resolveAutoCompactWindow(
			contextWindowForModel(record.model, runtime.env),
			resumeDefinition?.autoCompactWindow,
			false,
		);
		const resumeCacheTtl = record.cacheTtl ?? resumeDefinition?.cacheTtl;
		const launched = await fire({
			label: `${String(record.label ?? jobId)} (resume)`,
			kind: 'agent',
			prompt: instruction,
			background,
			cwd: resumeCwd,
			agentType: record.agentType,
			systemPrompt: subagentSystemPrompt(resumeDefinition?.systemPrompt),
			startup: startupFor(resumeDefinition, resumeCwd, instruction, record.model),
			model: record.model,
			...(record.tools !== undefined ? {tools: record.tools} : {}),
			...(resumedMcp.length > 0 ? {mcpServers: resumedMcp} : {}),
			...(resumeCompact !== undefined ? {autoCompactWindow: resumeCompact} : {}),
			...(resumeCacheTtl !== undefined ? {cacheTtl: resumeCacheTtl} : {}),
			...(record.color !== undefined ? {color: record.color} : {}),
			maxTurns: record.maxTurns,
			// The per-invocation model and effort are what make a resume the same run
			// continued, not a fresh one with the definition's defaults.
			effort: record.effort,
			permissionMode: record.permissionMode,
			timeoutMs: record.timeoutMs,
			resumeSession: record.acpSessionId,
			resumedFrom: jobId,
			...(record.name !== undefined ? {name: record.name} : {}),
			...(record.owner !== undefined ? {owner: record.owner} : {}),
		});
		if (!launched.ok) return badInput(launched.error);
		const resumedAgentId = readJob(launched.id)?.agentId;
		if (launched.result !== undefined) {
			if (!launched.result.ok) return badInput(launched.result.error);
			return okText(
				resumedAgentId !== undefined ? `agent id ${resumedAgentId}\n${launched.result.text}` : launched.result.text,
			);
		}
		return okText(
			`resumed ${jobId} as ${launched.id} "${String(record.label ?? jobId)} (resume)", pid ${launched.pid}.\n` +
				(resumedAgentId !== undefined ? `agent id ${resumedAgentId}\n` : '') +
				`log ${join(logsDir, `${launched.id}.log`)}\n` +
				`The resumed run reports back here as its own signal.`,
		);
	};

	// The read side of the team: definitions, addressable agents, and the agent's
	// explicit final report. `ListAgents` is registered only where it belongs.
	function agentsToolText(scope?: string): string {
		registry.refresh();
		const wanted = String(scope ?? '').trim().toLowerCase();
		const entries = registry.entries().filter(entry => wanted.length === 0 || entry.source === wanted);
		if (entries.length === 0) return `no${wanted.length > 0 ? ` ${wanted}` : ''} agent definitions`;
		const lines = entries.map(entry => {
			const state = entry.valid ? 'valid' : `invalid: ${entry.reason ?? 'unknown'}`;
			const model = entry.definition?.model !== undefined ? ` model=${entry.definition.model}` : '';
			const tools = entry.definition?.tools !== undefined ? ` tools=${entry.definition.tools.join(',')}` : '';
			const source = entry.path.length > 0 ? ` · ${entry.path}` : '';
			return `  ${entry.name.length > 0 ? entry.name : '(unnamed)'} · ${entry.source} · ${state}${model}${tools}${source}`;
		});
		return `${entries.length} definition(s)${wanted.length > 0 ? ` in ${wanted}` : ''}:\n${lines.join('\n')}`;
	}

	function listAgentsText(): string {
		const lines = [`${ROSTER_MAIN} — this conversation`];
		for (const name of rosterNames()) {
			const record = namedJob(name);
			if (record === undefined) continue;
			lines.push(`${name} — ${canonicalState(record.state)} (job ${record.id})`);
		}
		return lines.join('\n');
	}

	const noteRun = async ({input}: {input: Record<string, unknown>}): Promise<ToolResult> => {
		const title = typeof input.title === 'string' ? input.title : '';
		if (title.length === 0) return badInput('title is required');
		const body = typeof input.body === 'string' ? input.body : '';
		const owner = ownerId();
		const id = `direct-${runtime.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
		enqueueSignal({
			id,
			owner,
			label: title,
			kind: 'note',
			state: 'note',
			exitCode: null,
			title,
			body,
			sessionId,
		});
		safeDrain();
		refreshJobs();
		return okText(ledger.has(id) ? 'delivered' : 'queued for this session');
	};

	// Live messaging. The target is a name, never a pid: the record decides whether
	// the message is queued to a live worker or starts a new turn in its finished
	// session, so a resume keeps the session the agent already has.
	async function sendToRecord(record: JobRecord, message: string, from: string): Promise<ToolResult> {
		const label = String(record.name ?? record.id);
		const {alive, busy} = liveState(record);
		if (alive) {
			if (!appendLive(record.id, from, message)) return badInput(`cannot reach ${label}: its control queue is not writable`);
			return okText(
				`${busy ? `queued for ${label}'s next safe point` : `sent to idle ${label}; it starts a turn`} (job ${record.id}).\n` +
					`The reply arrives here as its own message signal.`,
			);
		}
		const resumeCwd = String(record.cwd ?? cmd.cwd);
		// A finished fork resumes the fork session the host created. It is the same
		// session (and cache), not a second fork.
		if (record.fork === true) {
			if (record.forkSessionId === undefined) {
				return badInput(`${label} has finished and its fork session was never recorded; spawn it again`);
			}
			const launched = await fire({
				label: `${String(record.label ?? record.id)} (message)`,
				kind: 'fork',
				prompt: message,
				background: record.teamName !== undefined ? false : true,
				cwd: resumeCwd,
				agentType: FORK_AGENT_TYPE,
				fork: true,
				...(record.mainSessionId !== undefined ? {mainSessionId: record.mainSessionId} : {}),
				resumeSession: record.forkSessionId,
				resumedFrom: record.id,
				...(record.permissionMode !== undefined ? {permissionMode: record.permissionMode} : {}),
				...(record.name !== undefined ? {name: record.name} : {}),
				...(record.teamName !== undefined ? {teamName: record.teamName} : {}),
				...(record.owner !== undefined ? {owner: record.owner} : {}),
			});
			if (!launched.ok) return badInput(launched.error);
			return okText(
				`resumed ${label} as ${launched.id} "${String(record.label ?? record.id)} (message)", pid ${launched.pid}.\n` +
					`The reply arrives here as its own signal.`,
			);
		}
		if (record.acpSessionId === undefined) {
			return badInput(`${label} has finished and has no resumable session; spawn it again`);
		}
		// A finished agent resumes the ACP session it already has, with the same
		// definition, startup context, tools, model, effort and permissions. The
		// message is the new turn.
		const definition = record.agentType === undefined ? undefined : resolveAgentDefinition(record.agentType);
		const resumeDefinition = definition?.ok === true ? definition.definition : undefined;
		const launched = await fire({
			label: `${String(record.label ?? record.id)} (message)`,
			kind: 'agent',
			prompt: message,
			cwd: resumeCwd,
			agentType: record.agentType,
			systemPrompt: resumeDefinition?.systemPrompt,
			startup: startupFor(resumeDefinition, resumeCwd, message, record.model),
			model: record.model,
			...(record.tools !== undefined ? {tools: record.tools} : {}),
			...(record.autoCompactWindow !== undefined ? {autoCompactWindow: record.autoCompactWindow} : {}),
			...(record.cacheTtl !== undefined ? {cacheTtl: record.cacheTtl} : {}),
			...(record.color !== undefined ? {color: record.color} : {}),
			maxTurns: record.maxTurns,
			effort: record.effort,
			permissionMode: record.permissionMode,
			timeoutMs: record.timeoutMs,
			resumeSession: record.acpSessionId,
			resumedFrom: record.id,
			name: record.name,
			owner: record.owner,
			...(record.teamName !== undefined ? {background: false, teamName: record.teamName} : {}),
		});
		if (!launched.ok) return badInput(launched.error);
		return okText(
			`resumed ${label} as ${launched.id} "${String(record.label ?? record.id)} (message)", pid ${launched.pid}.\n` +
				`The reply arrives here as its own signal.`,
		);
	}

	const sendLiveRun = async ({input}: {input: Record<string, unknown>}): Promise<ToolResult> => {
		const to = String(input.to ?? '').trim();
		const message = typeof input.message === 'string' ? input.message : '';
		if (to.length === 0) return badInput('to is required: a named agent or main');
		if (message.trim().length === 0) return badInput('message is required and must not be empty');
		const from = workerMode ? String(workerSpec?.name ?? 'worker') : ROSTER_MAIN;
		if (to === ROSTER_MAIN) {
			const id = `live-${runtime.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
			const scan = scanOutput(message);
			enqueueSignal({
				id,
				owner: workerMode ? workerSpec?.owner : ownerId(),
				label: `message from ${from}`,
				kind: 'message',
				state: 'note',
				exitCode: null,
				title: `message from ${from}`,
				body: scan.text,
				sessionId: workerMode ? workerSpec?.sessionId : sessionId,
				...(workerMode ? {scanned: true, scanMatches: scan.matches} : {}),
			});
			if (!workerMode) {
				safeDrain();
				refreshJobs();
			}
			return okText(`sent a message to ${ROSTER_MAIN} from ${from}`);
		}
		const record = namedJob(to);
		if (record === undefined) {
			const names = rosterNames();
			return badInput(
				`no agent named "${to}"${names.length > 0 ? `; addressable: ${names.join(', ')}` : '; no named agents in this session'}`,
			);
		}
		return sendToRecord(record, message, from);
	};

	addSurface(
		'subagent_spawn',
		'Start a subagent. With `run_in_background: false` the call waits and returns the result; otherwise it returns immediately and the result arrives later as a separate message on its own - do not poll for it, and do not wait. Pass `prompt` for a headless Command Code agent, or `command` for a shell command. A definition with `background: true` or CLAUDE_CODE_DISABLE_BACKGROUND_TASKS can override the requested mode. Pass `team_name` to join an agent team as a foreground teammate. Pass `subagent_type: fork` to inherit the main conversation.' +
			(forkModeEnabled ? ' Fork mode is on: agent jobs run in the background and `run_in_background` is not accepted.' : ''),
		{
			type: 'object',
			properties: (() => {
				const properties: Record<string, unknown> = {
					description: {type: 'string', description: 'Short name for this job, used in the signal title.'},
					label: {type: 'string', description: 'Alias for description.'},
					name: {type: 'string', description: 'Address this agent later with subagent_send. A named agent appears in its siblings\' roster.'},
					team_name: {type: 'string', description: 'Join this agent team. A teammate runs in the foreground and keeps the task and cron tools.'},
					prompt: {type: 'string', description: 'Prompt for a detached headless Command Code agent.'},
					command: {type: 'string', description: 'Shell command to run detached under /bin/sh -c.'},
					agent_type: {type: 'string', description: "Deprecated alias for subagent_type. Use subagent_type."},
					subagent_type: {
						type: 'string',
						description:
							"Agent definition to run, by name. Omit for 'general-purpose'. A project or user definition in .commandcode/agents/ overrides the built-ins.",
					},
					model: {type: 'string', description: "Model for an agent job. Overrides the definition's model; 'inherit' means the session model."},
					effort: {type: 'string', description: "Reasoning effort for an agent job: low, medium, high, xhigh, or max. Overrides the definition's effort."},
					tools: {type: 'string', description: 'Comma-separated tool list (or an array). Only these tools remain; overrides the definition.'},
					disallowed_tools: {type: 'string', description: 'Comma-separated tool list (or an array) to remove. Wins over tools.'},
					max_turns: {type: 'number', description: 'Cap the agent conversation turns.'},
					permission_mode: {
						type: 'string',
						description:
							"Permission mode for an agent job: default, acceptEdits, auto, dontAsk, bypassPermissions, plan, or manual. The main session's mode wins when it is acceptEdits, auto, or bypassPermissions; bypassPermissions otherwise requires the main session to be in it.",
					},
					timeout_ms: {type: 'number', description: 'Kill the job after this many milliseconds and report a timeout.'},
					run_in_background: {
						type: 'boolean',
						description:
							'false waits for the run and returns its result in this call; true (or omitted) returns immediately and delivers the result as a signal. A definition pinned to background, or CLAUDE_CODE_DISABLE_BACKGROUND_TASKS, can override this.',
					},
					cwd: {type: 'string', description: 'Working directory. Defaults to the mod workspace root.'},
					};
				// Where fork mode is on, the model cannot ask for the foreground: the
				// parameter is absent, not merely ignored.
				if (forkModeEnabled) delete properties.run_in_background;
				return properties;
			})(),
			required: [],
		},
		spawnRun,
		{aliases: ['pubsub_fire']},
	);

	addSurface(
		'subagent_list',
		'List spawned jobs with their lifecycle states, subagent_type, agent name, model, elapsed time and owner, and count the signals still waiting for this session.',
		{
			type: 'object',
			properties: {
				state: {type: 'string', description: 'Only jobs in this state (queued, running, waiting-input, done, failed, canceled).'},
				limit: {type: 'number', description: 'Show at most this many jobs.'},
			},
			required: [],
		},
		listRun,
		{readOnly: true, aliases: ['pubsub_list']},
	);

	addSurface(
		'subagent_output',
		'Read one job: its state, exit code, duration and owner, plus a bounded slice of its log. By default the tail; `offset` reads from a character offset, `max_chars` bounds the slice.',
		{
			type: 'object',
			properties: {
				id: {type: 'string', description: 'Job id from subagent_spawn or subagent_list.'},
				wait: {type: 'boolean', description: 'Block briefly until the job finishes (bounded, 10s by default).'},
				wait_ms: {type: 'number', description: 'How long to wait, capped at 60s.'},
				offset: {type: 'number', description: 'Character offset into the log.'},
				max_chars: {type: 'number', description: 'Maximum characters to return.'},
			},
			required: ['id'],
		},
		outputRun,
		{readOnly: true},
	);

	addSurface(
		'subagent_cancel',
		'Cancel a queued or running subagent job by id (SIGTERM, then SIGKILL after 1s). The firing session receives one canceled signal.',
		{
			type: 'object',
			properties: {id: {type: 'string', description: 'Job id from subagent_list or subagent_spawn.'}},
			required: ['id'],
		},
		cancelRun,
		{aliases: ['pubsub_cancel']},
	);

	addSurface(
		'subagent_resume',
		"Resume a finished agent job in its own worker session and continue it with a new instruction. The resumed run is a new job with its own signal.",
		{
			type: 'object',
			properties: {
				id: {type: 'string', description: 'Job id of the finished agent job.'},
				prompt: {type: 'string', description: 'The instruction for the resumed run.'},
			},
			required: ['id', 'prompt'],
		},
		resumeRun,
	);

	addSurface(
		'subagent_agents',
		'List the available agent definitions: scope, model, tools, validity, and the file that provides each one. Pass `scope` to filter to project, user, or builtin.',
		{
			type: 'object',
			properties: {
				scope: {type: 'string', description: 'Only definitions from this scope (project, user, builtin).'},
			},
			required: [],
		},
		async ({input}: {input: Record<string, unknown>}) =>
			okText(agentsToolText(typeof input.scope === 'string' ? input.scope : undefined)),
		{readOnly: true},
	);

	const crossSessionMessaging = runtime.env.BETTER_SUBAGENTS_CROSS_SESSION !== '0';
	if (listAgentsVisible({background: workerMode ? workerSpec?.background === true : false, crossSessionMessaging})) {
		addSurface(
			'ListAgents',
			'List the agents this conversation can message with subagent_send: main plus every named agent, with its current state and job id.',
			{type: 'object', properties: {}, required: []},
			async () => okText(listAgentsText()),
			{readOnly: true},
		);
	}

	if (workerSpec !== undefined) {
		addSurface(
			'SubagentHandback',
			'Hand your final report back to the conversation that receives this run\'s result. The report is delivered as its own message; end the turn afterwards. The wrapper suppresses the duplicate end-of-turn result.',
			{
				type: 'object',
				properties: {
					report: {type: 'string', description: 'The final report for the parent conversation.'},
				},
				required: ['report'],
			},
			async ({input}: {input: Record<string, unknown>}): Promise<ToolResult> => {
				const report = String(input.report ?? '').trim();
				if (report.length === 0) return badInput('report is required and must not be empty');
				const id = String(workerSpec?.id ?? '');
				const record = id.length === 0 ? undefined : readJob(id);
				if (record === undefined) return badInput(`subagent: no job ${id || '(worker)'}`);
				const now = runtime.now();
				const scan = scanOutput(report);
				record.handbackAt = now;
				record.updatedAt = now;
				writeJob(record);
				enqueueSignal(
					signalOf({...record, id: `${id}-handback-${now.toString(36)}`}, {
						state: 'done',
						finishedAt: now,
						durationMs: Math.max(0, now - Number(record.startedAt ?? now)),
						body: scan.text,
						scanned: true,
						scanMatches: scan.matches,
						title: `${String(record.label ?? id)} handback`,
					}),
				);
				refreshJobs();
				return okText(`handed the report back (job ${id}); it reaches the parent as one message`);
			},
		);
	}

	addSurface(
		'subagent_send',
		'Send a live message to a named agent (or "main"). A running agent queues it and answers at its next safe point; an idle agent starts a turn; a finished agent resumes its session with full context. The reply arrives later as its own message signal, so do not wait for it here.',
		{
			type: 'object',
			properties: {
				to: {type: 'string', description: 'A named agent, or "main" for this conversation.'},
				message: {type: 'string', description: 'The message to deliver. The target acts on it in its own session.'},
			},
			required: ['to', 'message'],
		},
		sendLiveRun,
	);

	// The 0.1.0 note channel. It is NOT an alias of live messaging: it publishes a
	// note into this session, while subagent_send delivers to another agent. It is
	// removed one release after the rename.
	addSurface(
		'pubsub_send',
		'[deprecated: live messaging moved to subagent_send] Publish a note into the agent loop of THIS session. It is delivered as a visible user turn. This is not live messaging to another agent.',
		{
			type: 'object',
			properties: {
				title: {type: 'string', description: 'One line, shown in the signal header.'},
				body: {type: 'string', description: 'The detail.'},
			},
			required: ['title'],
		},
		noteRun,
	);

	// ------------------------------------------------------------------ command

	// `/agents` is the definition surface: what loaded, from where, and what did
	// not. It refreshes discovery first, so an edited file shows up without a
	// restart.
	function agentsText(filter?: string): string {
		registry.refresh();
		const entries = registry.entries();
		const wanted = String(filter ?? '').trim().toLowerCase();
		const shown =
			wanted.length === 0
				? entries
				: entries.filter(entry => entry.name.toLowerCase() === wanted || entry.path.toLowerCase().includes(wanted));
		const counts: Partial<Record<AgentScope, number>> = {};
		let valid = 0;
		for (const entry of entries) {
			if (!entry.valid) continue;
			valid += 1;
			counts[entry.source] = (counts[entry.source] ?? 0) + 1;
		}
		// Scope order, so the summary reads the way precedence does.
		const summary = AGENT_SCOPES.filter(scope => (counts[scope] ?? 0) > 0)
			.map(scope => `${counts[scope]} ${scope === 'builtin' ? 'built-in' : scope}`)
			.join(', ');
		const lines = shown.map(entry => {
			const flags: string[] = [];
			if (entry.definition?.readOnly === true) flags.push('read-only');
			if (entry.definition?.oneShot === true) flags.push('one-shot');
			if (entry.definition?.plugin !== undefined) flags.push(`plugin ${entry.definition.plugin}`);
			if (entry.source !== 'builtin' && Object.hasOwn(HOST_RESERVED_AGENT_NAMES, entry.name)) {
				flags.push('shadows a host-reserved name');
			}
			if (entry.warning !== undefined) flags.push(`warning: ${entry.warning}`);
			const state =
				entry.kind === 'duplicate'
					? `duplicate: ${entry.reason ?? 'unknown'}`
					: entry.valid
						? 'valid'
						: `invalid: ${entry.reason ?? 'unknown'}`;
			const location = entry.path.length > 0 ? `  ${entry.path}` : '';
			// What the definition resolves to. Tools are shown as authored so a missing
			// tool is visible before a spawn fails on it.
			const settings = [
				entry.definition?.model !== undefined ? `model=${entry.definition.model}` : '',
				entry.definition?.effort !== undefined ? `effort=${entry.definition.effort}` : '',
				entry.definition?.color !== undefined ? `color=${entry.definition.color}` : '',
				entry.definition?.tools !== undefined ? `tools=${entry.definition.tools.join('+')}` : '',
			].filter(Boolean).join(' ');
			return `  ${entry.name.length > 0 ? entry.name : '(unnamed)'}  ${entry.source}  ${state}${flags.length > 0 ? ` · ${flags.join(' · ')}` : ''}${settings.length > 0 ? ` · ${settings}` : ''}${location}`;
		});
		return (
			`${valid} valid definition(s)${summary.length > 0 ? ` (${summary})` : ''}, ` +
			`${entries.length - valid} invalid.\n` +
			(lines.length > 0 ? lines.join('\n') : '  (none)')
		);
	}

	// `/doctor` is the diagnosis surface: every duplicate with the file that was
	// kept, every invalid file, and every plugin file that loaded under its
	// filename because its frontmatter could not be read.
	function doctorText(filter?: string): string {
		registry.refresh();
		const entries = registry.entries();
		if (String(filter ?? '').trim().toLowerCase() === 'plugins') {
			const pluginEntries = entries.filter(entry => entry.source === 'plugin');
			const problems = pluginEntries.filter(entry => !entry.valid || entry.warning !== undefined);
			const lines = pluginEntries.map(entry => {
				const state =
					entry.kind === 'valid'
						? entry.warning !== undefined
							? `warning: ${entry.warning}`
							: 'valid'
						: `${entry.kind}: ${entry.reason ?? 'unknown'}`;
				return `  ${entry.name.length > 0 ? entry.name : '(unnamed)'}  ${state}  ${entry.path}`;
			});
			return (
				`plugin validation: ${pluginEntries.length} file(s), ${problems.length} problem(s)\n` +
				(lines.length > 0 ? lines.join('\n') : '  (no plugin agents)')
			);
		}
		const duplicates = entries.filter(entry => entry.kind === 'duplicate');
		const invalid = entries.filter(entry => entry.kind === 'invalid');
		const warnings = entries.filter(entry => entry.valid && entry.warning !== undefined);
		const pluginAgents = entries
			.filter(entry => entry.source === 'plugin' && entry.valid && entry.definition !== undefined)
			.map(entry => entry.name);
		const lines: string[] = [];
		for (const entry of duplicates) {
			lines.push(`  duplicate ${entry.name}: kept ${entry.duplicateOf ?? '?'}, skipped ${entry.path} - rename or remove one`);
		}
		for (const entry of invalid) {
			lines.push(`  invalid ${entry.path.length > 0 ? entry.path : entry.name}: ${entry.reason ?? 'unknown'}`);
		}
		for (const entry of warnings) lines.push(`  warning ${entry.path}: ${entry.warning}`);
		if (pluginAgents.length > 0) lines.push(`  plugin agents: ${pluginAgents.join(', ')}`);
		const summary = `${duplicates.length} duplicate(s), ${invalid.length} invalid, ${warnings.length} warning(s)`;
		return (
			`subagents doctor: ${lines.length > 0 ? summary : 'ok'}\n` +
			(lines.length > 0 ? lines.join('\n') : '  no duplicate or invalid definitions')
		);
	}

	function commandHandler({args}: {args?: string}): {message: string} {
		const [sub, ...rest] = String(args ?? '').trim().split(/\s+/).filter(Boolean);
		if (sub === 'list') return {message: listText()};
		if (sub === 'cancel' && rest[0] !== undefined) return {message: cancelJob(rest[0]).message};
		if (sub === 'open' && rest[0] !== undefined) {
			const shown = outputText(rest[0], {});
			return {message: shown.ok ? String(shown.content?.[0]?.text ?? '') : String(shown.error ?? '')};
		}
		agentViewOpen = true;
		try {
			cmd.ui.openPane('jobs');
		} catch {
			/* headless */
		}
		return {message: 'subagents: opened the async subagents pane (list | jobs | cancel <id> | open <id>)'};
	}

	function recordForTarget(target: string | undefined): JobRecord | undefined {
		const wanted = String(target ?? '').trim();
		if (wanted.length === 0) return undefined;
		return readJob(wanted) ?? namedJob(wanted);
	}

	function mentionText(prefix: string): string {
		const definitions = registry.entries().map(entry => ({name: entry.name, valid: entry.valid}));
		const typed = String(prefix ?? '').replace(/^@/, '').toLowerCase();
		const names = mentionCandidates(definitions, rosterNames()).filter(name => name.toLowerCase().startsWith(typed));
		return names.length > 0 ? names.map(name => `@${name}`).join('  ') : 'no matching @mention';
	}

	// `/tasks` is the keyboard surface for the panel: next/prev move the row,
	// enter/open reads the transcript, send starts a follow-up, x stops or dismisses,
	// and esc returns to the prompt. `x` on a live row stops it; on a terminal row it
	// clears the row.
	async function tasksHandler({args}: {args?: string}): Promise<{message: string}> {
		const [sub, ...rest] = String(args ?? '').trim().split(/\s+/).filter(Boolean);
		if (sub === undefined) {
			agentViewOpen = true;
			try {
				cmd.ui.openPane('jobs');
			} catch {
				/* headless */
			}
			return {
				message:
					'tasks: opened the task view (next | prev | enter [message] | open <id> | send <id> <message> | x <id> | stop <id> | esc | mention <prefix>)',
			};
		}
		if (sub === 'next' || sub === 'prev') {
			const row = selectTask(sub === 'next' ? 1 : -1);
			return {message: row === undefined ? 'tasks: no visible rows' : `tasks: selected ${row.id} (${canonicalState(row.state)})`};
		}
		if (sub === 'open' || sub === 'enter') {
			const target = rest[0] ?? selectedTaskId;
			const record = recordForTarget(target);
			if (record === undefined) return {message: 'tasks: no task selected'};
			selectedTaskId = record.id;
			const shown = outputText(record.id, {});
			if (sub === 'enter' && rest.length > 1) {
				const sent = await sendToRecord(record, rest.slice(1).join(' '), ROSTER_MAIN);
				return {message: `${shown.ok ? String(shown.content?.[0]?.text ?? '') : String(shown.error ?? '')}\n\n${sent.ok ? String(sent.content?.[0]?.text ?? '') : String(sent.error ?? '')}`};
			}
			return {message: shown.ok ? String(shown.content?.[0]?.text ?? '') : String(shown.error ?? '')};
		}
		if (sub === 'send') {
			const record = recordForTarget(rest[0] ?? selectedTaskId);
			const message = rest.slice(1).join(' ');
			if (record === undefined) return {message: 'tasks: no task selected'};
			if (message.trim().length === 0) return {message: 'tasks: usage: send <id> <message>'};
			const sent = await sendToRecord(record, message, ROSTER_MAIN);
			return {message: sent.ok ? String(sent.content?.[0]?.text ?? '') : String(sent.error ?? '')};
		}
		if (sub === 'stop' || sub === 'cancel') {
			const record = recordForTarget(rest[0] ?? selectedTaskId);
			if (record === undefined) return {message: 'tasks: no task selected'};
			return {message: cancelJob(record.id).message};
		}
		if (sub === 'x' || sub === 'dismiss') {
			const record = recordForTarget(rest[0] ?? selectedTaskId);
			if (record === undefined) return {message: 'tasks: no task selected'};
			return {message: dismissOrStopTask(record.id).message};
		}
		if (sub === 'esc' || sub === 'back') {
			selectedTaskId = undefined;
			agentViewOpen = false;
			return {message: 'tasks: returned to the prompt'};
		}
		if (sub === 'mention') return {message: mentionText(rest.join(' '))};
		return {message: 'tasks: usage: next | prev | enter [message] | open <id> | send <id> <message> | x <id> | stop <id> | esc'};
	}

	// `/subtask` is the user's fork when the agent view is closed. With the view
	// open, a fork is already on screen; the user steers it from the row instead.
	async function subtaskHandler({args}: {args?: string}): Promise<{message: string}> {
		const prompt = String(args ?? '').trim();
		if (prompt.length === 0) return {message: 'subtask: usage: /subtask <prompt>'};
		if (agentViewOpen) return {message: 'subtask: the agent view is on; select a task row (or Esc) before starting another'};
		if (sessionId === undefined || sessionId.length === 0) return {message: 'subtask: the main session has not bound yet; try again'};
		const launched = await fire({
			label: 'subtask',
			kind: 'fork',
			prompt,
			background: true,
			cwd: cmd.cwd,
			agentType: FORK_AGENT_TYPE,
			fork: true,
			mainSessionId: sessionId,
		});
		if (!launched.ok) return {message: `subtask: ${launched.error}`};
		return {
			message:
				`subtask: started fork ${launched.id} from the main conversation, pid ${launched.pid}.\n` +
				`/tasks open ${launched.id} reads it; /tasks send ${launched.id} <message> steers it.`,
		};
	}

	cmd.addCommand({
		name: 'subagents',
		description: 'Async subagents: list, jobs, cancel <id>, open <id>',
		argumentHint: 'list|jobs|cancel <id>|open <id>',
		handler: commandHandler,
	});
	cmd.addCommand({
		name: 'pubsub',
		description: '[deprecated: use /subagents] Async subagents: list, jobs, cancel <id>, open <id>',
		argumentHint: 'list|jobs|cancel <id>|open <id>',
		handler: commandHandler,
	});
	cmd.addCommand({
		name: 'tasks',
		description: 'Agent panel: list running and recently failed tasks, open a transcript, send a follow-up, stop or dismiss a row',
		argumentHint: 'next|prev|enter [message]|open <id>|send <id> <message>|x <id>|stop <id>|esc|mention <prefix>',
		handler: tasksHandler,
	});
	cmd.addCommand({
		name: 'subtask',
		description: 'Start a fork of the main conversation when the agent view is off',
		argumentHint: '<prompt>',
		handler: subtaskHandler,
	});
	cmd.addCommand({
		name: 'agents',
		description: 'List subagent definitions: scope, validity, and the file that provides each one',
		argumentHint: '[name|path]',
		handler: ({args}: {args?: string}) => ({message: agentsText(args)}),
	});
	cmd.addCommand({
		name: 'doctor',
		description: 'Report duplicate, invalid, and plugin agent definitions, with the fix for each one',
		argumentHint: '[plugins]',
		handler: ({args}: {args?: string}) => ({message: doctorText(args)}),
	});

	// Ctrl+B is the host shortcut for "I no longer want to wait for this run". The
	// worker keeps running; only the tool call returns early.
	try {
		cmd.addShortcut('ctrl+b', {
			description: 'Move the running foreground subagent task to the background',
			handler: () => {
				const job = backgroundForegroundTask();
				if (job === undefined) {
					cmd.ui.notify('no foreground subagent task to background', 'info');
					return;
				}
				cmd.ui.notify(`${String(job.label ?? job.id)} continues in the background; /tasks shows it`, 'info');
			},
		});
	} catch {
		/* a host without shortcuts */
	}

	// ------------------------------------------------------------------ lifecycle

	// Worktree isolation is enforced where the tools run. This mod is loaded both in
	// the session and in the ACP worker, so one guard covers both: a subagent with a
	// worktree, and any session that is itself a linked worktree.
	const isolation = isolationRoots(git, String(workerSpec?.cwd ?? runtime.cwd), workerSpec);
	// The definition's hooks live only in its worker; the parent never inherits them.
	const workerHooks = workerMode ? workerSpec?.hooks : undefined;
	const workerHookCwd = String(workerSpec?.cwd ?? runtime.cwd);
	const workerHookAgent = workerSpec?.agentType;

	cmd.hooks({
		// The worker process loads this mod with `cmd --mod <file> acp`. Its whole
		// purpose there is this hook: the definition body and the startup bundle are
		// the worker's system prompt. The parent already has both, so only worker mode
		// registers it, and only when there is something to append.
		// In the main session the same hook is a read-only tap: the host passes the
		// live permission mode, and an empty string appends nothing.
		...(workerSystemPrompt().length > 0
			? {appendSystemPrompt: () => workerSystemPrompt()}
			: !workerMode
				? {
						// The session-wide agent's body is the main session's prompt; with no
						// `--agent` this is a read-only tap for the permission mode.
						appendSystemPrompt: (context?: {permissionMode?: unknown}) => {
							noteMainPermissionMode(context?.permissionMode);
							return sessionAgent()?.systemPrompt ?? '';
						},
					}
				: {}),
		// initialPrompt is a main-session first turn. If the host exposes
		// `cmd.prompt.submit`, it is submitted there; otherwise the first typed
		// prompt is prefixed, so the value is never silently dropped.
		...(!workerMode && sessionAgent()?.initialPrompt !== undefined
			? {
					transformInput: (input: {text?: string}) => {
						const text = sessionAgent()?.initialPrompt;
						if (text === undefined || initialPromptSubmitted) return undefined;
						initialPromptSubmitted = true;
						return {action: 'transform', text: `${text}\n\n${String(input?.text ?? '')}`};
					},
				}
			: {}),
		// A blocked tool returns the reason as its result, so the model learns where it
		// is allowed to work instead of silently retrying. The definition's PreToolUse
		// hooks run after the isolation guard: a hook cannot widen a worktree.
		...(isolation !== undefined || workerHooks?.PreToolUse !== undefined
			? {
					beforeToolCall: (call: {toolName?: string; input?: Record<string, unknown>}) => {
						const verdict =
							isolation === undefined
								? undefined
								: guardToolCall(String(call?.toolName ?? ''), call?.input ?? {}, isolation);
						if (verdict !== undefined) return {block: true, additionalContext: verdict.reason};
						const outcome = runHookRules(runtime.run, workerHooks?.PreToolUse, {
							event: 'PreToolUse',
							cwd: workerHookCwd,
							agent: workerHookAgent,
							toolName: String(call?.toolName ?? ''),
							input: call?.input ?? {},
						});
						return outcome.block === undefined ? undefined : {block: true, additionalContext: outcome.block};
					},
				}
			: {}),
		// PostToolUse's exit 2 message reaches the model as an extra text block.
		...(workerHooks?.PostToolUse !== undefined
			? {
					afterToolCall: (call: {toolName?: string; input?: Record<string, unknown>; result?: unknown}) => {
						const outcome = runHookRules(runtime.run, workerHooks.PostToolUse, {
							event: 'PostToolUse',
							cwd: workerHookCwd,
							agent: workerHookAgent,
							toolName: String(call?.toolName ?? ''),
							input: call?.input ?? {},
							result: call?.result,
						});
						return outcome.additionalContext === undefined
							? undefined
							: {additionalContext: outcome.additionalContext};
					},
				}
			: {}),
		onSessionStart: (info: {source?: string; sessionId?: string}) => {
			if (typeof info?.sessionId === 'string' && info.sessionId.length > 0) {
				sessionId = info.sessionId;
			}
			// Restore (or mint) the owner token before any drain runs.
			ownerId();
			registry.refresh();
			sweepClaimed();
			sweepOrphaned();
			sweepExpired();
			reconcileJobs();
			refreshJobs();
			safeDrain();
			// A resume already had its first turn; only a fresh start submits one.
			if (info?.source !== 'resume' && info?.source !== 'reload') submitInitialPrompt();
		},
		// The race guard. A worker can finish in the window between the last turn and the
		// next drain tick, and the session would then fall idle with the result unread.
		// Pull whatever is still on disk into THIS run as the continuation reason (A2A's
		// onStop inbox drain). Anything already delivered is no longer in the inbox, so
		// this cannot double-report a signal.
		onStop: () => {
			// In a worker the definition's Stop hook is the subagent's SubagentStop:
			// exit 2 asks for one more turn with the hook's message as the reason.
			if (workerMode) {
				const outcome = runHookRules(runtime.run, workerHooks?.Stop, {
					event: 'SubagentStop',
					cwd: workerHookCwd,
					agent: workerHookAgent,
				});
				return outcome.continue === true
					? {continue: true, reason: outcome.reason ?? 'SubagentStop hook requested another turn.'}
					: undefined;
			}
			const pending = claimPending(5);
			if (pending.length === 0) return undefined;
			for (const claim of pending) {
				cmd.showEntry('subagent', claim.signal);
				markDelivered(claim.signal.id);
				settle(claim, 'accepted');
			}
			refreshJobs();
			return {continue: true, reason: pending.map(claim => formatSignal(claim.signal)).join('\n\n')};
		},
	});

	// Belt and suspenders: `run_start` also carries the session id.
	cmd.on('run_start', (event: {sessionId?: string}) => {
		if (typeof event?.sessionId === 'string' && event.sessionId.length > 0) {
			sessionId = event.sessionId;
			ownerId();
			registry.refresh();
			safeDrain();
		}
	});

	// The live permission mode. Shift+Tab, `/mode`, and the plan tools move it; the
	// spawn path reads the latest value when a definition is resolved.
	cmd.on('permission_mode_changed', (event: {mode?: unknown}) => {
		noteMainPermissionMode(event?.mode);
	});

	cmd.clock.every(DRAIN_INTERVAL_MS, safeDrain);
	cmd.clock.every(REFRESH_INTERVAL_MS, refreshJobs);
	cmd.clock.every(RETENTION_SWEEP_INTERVAL_MS, sweepExpired);

	// Seed the dashboard and reconcile anything a previous process left behind.
	ownerId();
	reconcileJobs();
	refreshJobs();
}
