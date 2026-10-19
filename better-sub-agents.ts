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
import {spawn} from 'node:child_process';
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
import {dirname, join} from 'node:path';
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

// Agent definitions. A definition is a Markdown file with YAML frontmatter; its
// body is the worker's system prompt. Project discovery walks up from the working
// directory, so the closest `.commandcode/agents/` wins, then user, then built-ins.
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
// signal are derived from the same facts, so they cannot disagree.
const settle = (state, detail, code, tail) => {
  if (settled) return;
  settled = true;
  const finishedAt = Date.now();
  const durationMs = Math.max(0, finishedAt - spec.startedAt);
  const failed = state === 'failed';
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
    scanned: true,
    scanMatches: scan.matches
  });
  emit(signalEnvelope({
    id: spec.id, label: spec.label, kind: spec.kind, owner: spec.owner,
    startedAt: spec.startedAt, logPath: spec.logPath, sessionId: spec.sessionId
  }, {
    state: state,
    exitCode: typeof code === 'number' ? code : null,
    finishedAt: finishedAt,
    durationMs: durationMs,
    body: scan.text,
    scanned: true,
    scanMatches: scan.matches
  }));
};

// A failed worker still returns its last output: partial work is never discarded.
const fail = (reason, code) => settle('failed', reason, code, readReportTail());

let out;
try {
  out = fs.openSync(spec.logPath, 'w');
} catch (e) {
  fail('cannot open log: ' + String(e && e.message || e), null);
  process.exit(0);
}
const logWrite = text => { try { fs.writeSync(out, text); } catch (e) {} };

const child = spec.kind === 'agent'
  ? spawn(spec.bin, spec.args || [], {cwd: spec.cwd, stdio: ['pipe', 'pipe', 'pipe']})
  : spawn(spec.bin, spec.args || [], {cwd: spec.cwd, stdio: ['ignore', out, out]});

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

  const tail = readTail();
  const timeoutLabel = spec.timeoutMs >= 1000 ? Math.round(spec.timeoutMs / 1000) + 's' : spec.timeoutMs + 'ms';
  if (spec.kind === 'agent') {
    const detail = timedOut
      ? 'timed out after ' + timeoutLabel
      : spawnError !== undefined
        ? 'spawn error: ' + String(spawnError && spawnError.message || spawnError)
        : 'ACP exited before the prompt completed (exit ' + String(code) + ')';
    fail(detail, code);
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
  settle(failed ? 'failed' : wantsInput ? 'waiting-input' : 'done', detail, code, readReportTail());
};

if (typeof child.pid === 'number') {
  writeRecord({state: 'running', pid: child.pid, workerPid: process.pid});
}

child.on('exit', (code, signal) => finish(code, undefined, signal));
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

  const handleLine = line => {
    let message;
    try { message = JSON.parse(line); } catch (e) { return; }
    if (message && typeof message.method === 'string') {
      if (message.method === 'session/update') {
        const update = message.params && message.params.update;
        if (!update) return;
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
        const options = (message.params && message.params.options) || [];
        const title = message.params && message.params.toolCall ? String(message.params.toolCall.title || '') : '';
        const allow = options.find(option => option.kind === 'allow_once')
          || options.find(option => option.kind === 'allow_always')
          || options[0];
        if (allow && typeof allow.optionId === 'string') {
          logWrite('\\n[permission] ' + title + ' -> ' + String(allow.name || allow.optionId) + '\\n');
          reply(message.id, {outcome: {outcome: 'selected', optionId: allow.optionId}});
        } else {
          logWrite('\\n[permission] ' + title + ' -> denied (no allowed option)\\n');
          reply(message.id, {outcome: {outcome: 'cancelled'}});
        }
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

  // Spec permissionMode -> ACP mode id. auto and bypassPermissions are deliberately
  // absent: auto has no ACP equivalent and bypass is only inherited (phase 8).
  const modeIds = {
    default: 'default',
    acceptEdits: 'auto-accept',
    dontAsk: 'dont-ask',
    plan: 'plan',
    manual: 'default'
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
    if (typeof spec.resumeSession === 'string' && spec.resumeSession.length > 0) {
      try {
        const state = await request('session/resume', {sessionId: spec.resumeSession, cwd: spec.cwd, mcpServers: []});
        sessionId = spec.resumeSession;
        configOptions = state && Array.isArray(state.configOptions) ? state.configOptions : [];
        logWrite('[acp] resumed ' + sessionId + '\\n');
      } catch (error) {
        logWrite('[acp] resume failed: ' + String(error && error.message || error) + '\\n');
      }
    }
    if (sessionId === undefined) {
      const session = await request('session/new', {cwd: spec.cwd, mcpServers: []});
      if (!session || typeof session.sessionId !== 'string') throw new Error('session/new returned no sessionId');
      sessionId = session.sessionId;
      configOptions = Array.isArray(session.configOptions) ? session.configOptions : [];
    }
    writeRecord({acpSessionId: sessionId});
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
    let hookReady = false;
    if (typeof spec.promptMarkerPath === 'string' && spec.promptMarkerPath.length > 0) {
      try { hookReady = fs.existsSync(spec.promptMarkerPath); } catch (e) { hookReady = false; }
    }
    let promptText = spec.prompt;
    if (body.length > 0 && !hookReady) {
      promptText = 'AGENT DEFINITION (your system prompt):\\n' + body + '\\n\\nTASK:\\n' + String(spec.prompt);
      logWrite('[acp] mod not loaded in the ACP child; the definition body is in the prompt\\n');
    }
    const result = await request('session/prompt', {sessionId: sessionId, prompt: [{type: 'text', text: promptText}]});
    clearTimers();
    const stopReason = result && result.stopReason;
    if (stopReason === 'end_turn') settle('done', 'exit 0', 0, readReportTail());
    else if (stopReason === 'cancelled') fail('cancelled', null);
    else if (stopReason === 'max_tokens') fail('max tokens reached; result is partial', null);
    else if (stopReason === 'max_turn_requests') fail('max turns reached; result is partial', null);
    else fail('ACP turn ended: ' + String(stopReason || 'unknown'), null);
    shutdown();
  };
  runPrompt().catch(error => {
    fail('ACP session failed: ' + String(error && error.message || error), null);
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
	readonly fs: SpoolFs;
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
	fs: osSpoolFs(),
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
// Discovery order is precedence order: project directories from the working
// directory upward (closest first), then the user directory, then built-ins. The
// first definition of a name wins. Invalid files never fail the session: they are
// recorded with a reason, written to agents.log, and reported by `/agents`.
// ---------------------------------------------------------------------------

export type AgentScope = 'project' | 'user' | 'builtin';

export interface AgentDefinition {
	name: string;
	description: string;
	systemPrompt: string;
	source: AgentScope;
	path?: string;
	tools?: string[];
	disallowedTools?: string[];
	model?: string;
	permissionMode?: string;
	maxTurns?: number;
	skills?: string[];
	isolation?: string;
	background?: boolean;
	color?: string;
	omitClaudeMd?: boolean;
	effort?: string;
	readOnly?: boolean;
	oneShot?: boolean;
}

export interface AgentEntry {
	name: string;
	source: AgentScope;
	path: string;
	valid: boolean;
	reason?: string;
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
	// block scalar and a block list are one case rather than interleaved state.
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
			blocks[blocks.length - 1].lines.push(line.slice(Math.min(indent, 2)));
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
			const body = block.lines.map(line => line.replace(/\s+$/, ''));
			fields[block.key] = (block.inline.startsWith('>') ? body.join(' ') : body.join('\n')).replace(/^\n+|\n+$/g, '');
			continue;
		}
		if (block.inline.length === 0 && block.lines.length > 0) {
			const first = block.lines.find(line => line.trim().length > 0);
			if (first !== undefined && first.trim().startsWith('- ')) {
				const values: unknown[] = [];
				for (const line of block.lines) {
					const item = line.trim();
					if (item.length === 0) continue;
					if (!item.startsWith('- ')) return {ok: false, reason: `expected a list item, got "${item.slice(0, 40)}"`};
					const parsed = parseYamlScalar(item.slice(2));
					if (!parsed.ok) return parsed;
					values.push(parsed.value);
				}
				fields[block.key] = values;
				continue;
			}
			// A nested map. Unknown fields are ignored by contract, so its contents do
			// not need to be understood; storing null keeps the key known.
			fields[block.key] = null;
			continue;
		}
		if (block.lines.some(line => line.trim().length > 0)) {
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

export function agentDefinitionFromSource(
	source: string,
	path: string,
	scope: AgentScope,
): {ok: true; definition: AgentDefinition} | {ok: false; reason: string} {
	const frontmatter = parseFrontmatter(source);
	if (!frontmatter.ok) return frontmatter;
	const fields = frontmatter.fields;

	const name = stringField(fields.name) ?? '';
	if (name.length === 0) return {ok: false, reason: 'name: required'};
	// Characters, not UTF-16 code units: the limit is on the name a human sees.
	if ([...name].length > AGENT_NAME_MAX) {
		return {ok: false, reason: `name: must be at most ${AGENT_NAME_MAX} characters`};
	}
	if (name.includes(':')) return {ok: false, reason: 'name: must not contain ":" (plugin-scoped identifiers reserve it)'};
	if (name.startsWith('-')) return {ok: false, reason: 'name: must not start with "-"'};
	const description = stringField(fields.description);
	if (description === undefined) return {ok: false, reason: 'description: required'};

	const definition: AgentDefinition = {
		name,
		description,
		systemPrompt: frontmatter.body,
		source: scope,
		path,
	};
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
	if (permissionMode !== undefined) definition.permissionMode = permissionMode;
	if (maxTurns !== undefined) definition.maxTurns = maxTurns;
	if (skills !== undefined) definition.skills = skills;
	if (isolation !== undefined) definition.isolation = isolation;
	if (color !== undefined) definition.color = color;
	if (effort !== undefined) definition.effort = effort;
	const background = booleanField(fields.background);
	const omitClaudeMd = booleanField(fields.omitClaudeMd);
	if (background !== undefined) definition.background = background;
	if (omitClaudeMd !== undefined) definition.omitClaudeMd = omitClaudeMd;
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
];

export function builtinAgentDefinitions(env: Record<string, string | undefined>): AgentDefinition[] {
	if (env.CLAUDE_AGENT_SDK_DISABLE_BUILTIN_AGENTS === '1' || env.BETTER_SUBAGENTS_DISABLE_BUILTIN_AGENTS === '1') return [];
	if (env.CLAUDE_CODE_DISABLE_EXPLORE_PLAN_AGENTS === '1') {
		return BUILTIN_AGENT_DEFINITIONS.filter(definition => definition.name !== 'explore' && definition.name !== 'plan');
	}
	return [...BUILTIN_AGENT_DEFINITIONS];
}

export function createAgentRegistry(deps: AgentRegistryDeps): AgentRegistry {
	let entries: AgentEntry[] = [];
	// Each (file, reason) is logged once per process, so a refresh from `/agents`
	// cannot flood the debug log with the same skip.
	const loggedSkips = new Set<string>();

	function reportInvalid(entry: AgentEntry): void {
		const key = `${entry.path}\u0000${entry.reason ?? ''}`;
		if (loggedSkips.has(key)) return;
		loggedSkips.add(key);
		deps.log(`${entry.path}: ${entry.reason ?? 'invalid'}`);
	}

	function scanFile(path: string, scope: AgentScope): AgentDefinition | undefined {
		let source = '';
		try {
			source = deps.fs.readFile(path);
		} catch (error) {
			const entry: AgentEntry = {
				name: '',
				source: scope,
				path,
				valid: false,
				reason: `unreadable: ${String((error as Error)?.message ?? error)}`,
			};
			entries.push(entry);
			reportInvalid(entry);
			return undefined;
		}
		const parsed = agentDefinitionFromSource(source, path, scope);
		if (!parsed.ok) {
			const entry: AgentEntry = {name: '', source: scope, path, valid: false, reason: parsed.reason};
			entries.push(entry);
			reportInvalid(entry);
			return undefined;
		}
		return parsed.definition;
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
		localNames: Set<string>,
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
				if (localNames.has(definition.name)) {
					const entry: AgentEntry = {
						name: definition.name,
						source: scope,
						path: full,
						valid: false,
						reason: `duplicate name "${definition.name}" (the first definition in this directory wins)`,
					};
					entries.push(entry);
					reportInvalid(entry);
					continue;
				}
				localNames.add(definition.name);
				found.push(definition);
				continue;
			}
			if (deps.fs.exists(full)) scanDir(full, scope, depth + 1, found, localNames);
		}
	}

	// The project dirs from the working directory upward, closest first. Walk-up
	// stops at the repository root (the directory holding .git) or the filesystem
	// root, so a nested `.commandcode/agents` closest to the cwd wins.
	function projectDirs(): string[] {
		const dirs: string[] = [];
		let current = deps.cwd;
		for (let depth = 0; depth <= 64; depth += 1) {
			const candidate = join(current, '.commandcode', 'agents');
			if (deps.fs.exists(candidate)) dirs.push(candidate);
			if (deps.fs.exists(join(current, '.git'))) break;
			const parent = dirname(current);
			if (parent === current) break;
			current = parent;
		}
		return dirs;
	}

	function refresh(): void {
		entries = [];
		const found: AgentDefinition[] = [];
		for (const dir of projectDirs()) scanDir(dir, 'project', 0, found, new Set());
		scanDir(join(deps.home, '.commandcode', 'agents'), 'user', 0, found, new Set());
		found.push(...deps.builtins);

		// Precedence, not an error: the first loaded definition of a name wins. A
		// duplicate inside one agents root was already reported by scanDir.
		const seen = new Set<string>();
		for (const definition of found) {
			if (seen.has(definition.name)) continue;
			seen.add(definition.name);
			entries.push({
				name: definition.name,
				source: definition.source,
				path: definition.path ?? '',
				valid: true,
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
// Worker spec
//
// The worker is one detached Node process. `buildSpec` is the only place a spec
// is shaped and `buildArgs` the only place an argv is built, so replacing the
// transport (the old `cmd -p` argv, now ACP) touches one function. The spec is
// passed in the environment: no shell interpolation.
// ---------------------------------------------------------------------------

export interface WorkerSpecInput {
	id: string;
	label: string;
	kind: 'command' | 'agent';
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
	systemPrompt?: string;
	model?: string;
	effort?: string;
	/** The resolved tool capabilities; the worker logs them and never widens them. */
	tools?: string[];
	disallowedTools?: string[];
	permissionMode?: string;
	maxTurns?: number;
	timeoutMs?: number;
	resumeSession?: string;
	prompt?: string;
	command?: string;
	/** Written by the mod when the ACP child loaded it; proves the prompt hook is live. */
	promptMarkerPath?: string;
}

export interface WorkerSpec extends WorkerSpecInput {
	args: string[];
}

// ACP is the accepted worker transport: `cmd --mod <file> acp` loads this mod in
// the worker so appendSystemPrompt can return the definition body, and the adapter
// speaks JSON-RPC over stdout/stdin. Shell jobs keep the /bin/sh argv.
export function buildArgs(spec: WorkerSpec, modPath: string): string[] {
	if (spec.kind === 'command') return ['-c', String(spec.command ?? '')];
	return ['--mod', modPath, 'acp'];
}

export function buildSpec(input: WorkerSpecInput, modPath: string): WorkerSpec {
	const spec: WorkerSpec = {...input, args: []};
	spec.args = buildArgs(spec, modPath);
	return spec;
}

// ---------------------------------------------------------------------------
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
}

export type ToolFilterResult =
	| {ok: true; tools: string[]; removed: string[]}
	| {ok: false; error: string};

export function filterTools(input: ToolFilterInput = {}): ToolFilterResult {
	const available = unique(input.available ?? SUBAGENT_TOOL_UNIVERSE);
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

	if (requested !== undefined && kept.length === 0) {
		// Name the entries, not the derived names: the author must be able to find the
		// line in the definition.
		return {ok: false, error: `tools list resolved to no available tool: ${requested.join(', ')}`};
	}
	return {ok: true, tools: kept, removed};
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

// MCP server names come from the host's config files. Discovery is best-effort: a
// missing or malformed file means "no MCP tools known", never a session error.
export function mcpServerNames(fs: Pick<SpoolFs, 'readFile'>, dirs: string[]): string[] {
	const names: string[] = [];
	for (const dir of dirs.slice(0, 4)) {
		if (typeof dir !== 'string' || dir.length === 0) continue;
		try {
			const parsed = JSON.parse(fs.readFile(join(dir, '.commandcode', 'mcp.json'))) as {
				mcpServers?: Record<string, {enabled?: boolean} | undefined>;
			};
			for (const [name, config] of Object.entries(parsed?.mcpServers ?? {})) {
				if (name.length === 0 || config?.enabled === false) continue;
				if (!names.includes(name)) names.push(name);
			}
		} catch {
			/* no MCP config here */
		}
	}
	return names;
}

// The identity a job record and its signal share. Declared once, so the two
// shapes cannot drift: the record adds durable bookkeeping, the signal adds the
// message that reaches the session.
interface JobIdentity {
	id: string;
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

interface Signal extends JobIdentity {
	readonly exitCode?: number | null;
	readonly title?: string;
	readonly body?: string;
	/** The report was scanned before it could reach the parent. */
	readonly scanned?: boolean;
	readonly scanMatches?: string[];
}

interface JobRecord extends JobIdentity {
	command?: string;
	prompt?: string;
	cwd?: string;
	/** This job's nesting level, and the job that spawned it. */
	depth?: number;
	parentJobId?: string;
	agentType?: string;
	model?: string;
	tools?: string[];
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
	exitCode?: number;
	pid?: number;
	workerPid?: number;
	deliveredAt?: number;
	updatedAt?: number;
	failureReason?: string;
	canceledAt?: number;
	awaitingInput?: boolean;
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
	scanned?: boolean;
	scanMatches?: string[];
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
		scanned?: boolean;
		scanMatches?: string[];
	},
): Signal {
	const label = String(record.label ?? record.id);
	const state = String(result.state);
	return {
		id: record.id,
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
		// Omitted when the writer did not scan, so a legacy signal keeps its shape.
		...(result.scanned === undefined
			? {}
			: {scanned: result.scanned === true, scanMatches: result.scanMatches ?? []}),
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
	if (signal.scanned !== undefined) {
		patch.scanned = signal.scanned;
		patch.scanMatches = signal.scanMatches ?? [];
	}
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

	const readFlag = (name: string): string => String(cmd.getFlag(name) ?? '');
	// A flag still sitting on its default is not an override; anything else is.
	const pick = (primary: string, primaryDefault: string, legacy: string, legacyDefault: string): string => {
		const value = readFlag(primary);
		if (value.length > 0 && value !== primaryDefault) return value;
		const old = readFlag(legacy);
		if (old.length > 0 && old !== legacyDefault) return old;
		return value.length > 0 ? value : legacyDefault;
	};
	const root = pick('subagent_dir', SUBAGENT_DIR_DEFAULT, 'pubsub_dir', PUBSUB_DIR_DEFAULT);
	const inboxDir = join(root, 'inbox');
	const claimedDir = join(root, 'claimed');
	const orphanedDir = join(root, 'orphaned');
	const logsDir = join(root, 'logs');
	const jobsDir = join(root, 'jobs');
	const worktreesDir = join(root, 'worktrees');
	const ledgerPath = join(root, 'delivered.jsonl');
	for (const dir of [root, inboxDir, claimedDir, orphanedDir, logsDir, jobsDir, worktreesDir]) {
		spool.mkdir(dir);
	}

	// A headless worker loads this mod too. It has its own owner token and must never
	// steal an unaddressed signal from the session that fired it.
	const workerMode = runtime.env[WORKER_ENV] === '1' || runtime.env.PUBSUB_WORKER === '1';

	// Definitions are discovered once at bind, refreshed at each run start and by
	// `/agents`, so a new file is picked up without a restart. Invalid files are
	// skipped with an agents.log entry and remain visible (with the reason) in
	// `/agents`.
	const registry = createAgentRegistry({
		fs: spool,
		cwd: runtime.cwd,
		home: runtime.home,
		builtins: builtinAgentDefinitions(runtime.env),
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

	// A2A-style envelope: the job identity and state travel WITH the result, so the
	// model can tell which worker finished, whether it passed, and where the full log is.
	function formatSignal(message: Signal): string {
		const state = canonicalState(message.state ?? message.kind ?? 'signal');
		const facts = [
			`job ${String(message.id ?? 'unknown')}`,
			String(message.kind ?? 'signal'),
			state,
			message.exitCode === null || message.exitCode === undefined ? '' : `exit ${message.exitCode}`,
			typeof message.durationMs === 'number' ? `${(message.durationMs / 1000).toFixed(1)}s` : '',
		].filter(Boolean).join(' · ');
		const log = typeof message.logPath === 'string' ? `\nlog ${message.logPath}` : '';
		const body = typeof message.body === 'string' ? message.body.trim() : '';
		const title = String(message.title ?? message.label ?? message.id ?? '(signal)');
		const next =
			state === 'waiting-input'
				? 'The worker stopped with a question. Answer it here, or fire a follow-up job.'
				: state === 'canceled'
					? 'The job was canceled; nothing else will arrive for it.'
					: 'This result arrived automatically. Read it now and use it in your current work; ' +
						'do not poll for it and do not wait for it.';
		return `[subagent] ${title}\n${facts}${log}` + (body.length > 0 ? `\n\n${body}` : '') + `\n\n${next}`;
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

	// Retention. A finished job keeps its record, its log and its worktree for
	// RETENTION_DAYS_DEFAULT (or BETTER_SUBAGENTS_RETENTION_DAYS), so
	// `subagent_output` still works on a job from last week. A running job is never
	// swept, however old its record is.
	function retentionMs(): number {
		const days = Number(runtime.env[RETENTION_ENV]);
		const value = Number.isFinite(days) && days > 0 ? days : RETENTION_DAYS_DEFAULT;
		return value * 24 * 60 * 60 * 1000;
	}

	function sweepExpired(): void {
		if (workerMode) return;
		const now = runtime.now();
		const ttlMs = retentionMs();
		const cutoff = now - ttlMs;
		const expired: string[] = [];
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
					if (typeof job.id === 'string' && job.id.length > 0) expired.push(job.id);
					return true;
				} catch {
					return false;
				}
			},
		});
		for (const id of expired) {
			try {
				spool.remove(join(logsDir, `${id}.log`));
			} catch {
				/* the log is already gone */
			}
			try {
				spool.remove(join(worktreesDir, id));
			} catch {
				/* the worktree is already gone */
			}
		}
		// An abandoned log or worktree with no live record is bounded by the same TTL.
		sweepExpiredEntries(spool, logsDir, now, {
			ttlMs,
			skip: name => {
				const job = readJob(name.replace(/\.log$/, ''));
				if (job === undefined) return false;
				const state = normalizeState(job.state);
				return state === undefined || !isTerminal(state);
			},
		});
		sweepExpiredEntries(spool, worktreesDir, now, {ttlMs});
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
			const text = parts.length > 0 ? `◈ ${parts.join(' · ')}` : '';
			if (text === lastStatus) return;
			lastStatus = text;
			ui.setStatus(text.length > 0 ? text : null);
		} catch {
			/* status is a convenience, never load-bearing */
		}
	}

	function refreshJobs(): void {
		jobCache = readRecentJobs(LIST_SCAN_MAX);
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
			return {ok: false, message: `subagent: job ${id} is already ${state}`};
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

	interface JobIntent {
		readonly label: string;
		readonly kind: 'command' | 'agent';
		readonly command?: string;
		readonly prompt?: string;
		readonly cwd: string;
		/** the resolved definition name: the canonical `subagent_type` */
		readonly agentType?: string;
		/** This job's nesting level, and the job that spawned it. */
		readonly depth?: number;
		readonly parentJobId?: string;
		/** the definition body, delivered as the worker's system prompt */
		readonly systemPrompt?: string;
		readonly model?: string;
		readonly maxTurns?: number;
		readonly effort?: string;
		/** The resolved tool capabilities for an agent job. */
		readonly tools?: string[];
		readonly disallowedTools?: string[];
		readonly color?: string;
		readonly permissionMode?: string;
		readonly isolation?: string;
		/** SIGTERM then SIGKILL after this long; 0 or absent means no timeout */
		readonly timeoutMs?: number;
		/** resume this ACP session id first */
		readonly resumeSession?: string;
		readonly resumedFrom?: string;
	}

	type Launch = {readonly ok: true; readonly id: string; readonly pid: number} | {readonly ok: false; readonly error: string};

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
			state: 'queued',
			cwd: intent.cwd,
			logPath,
			startedAt,
			sessionId,
			updatedAt: startedAt,
			...(intent.depth !== undefined ? {depth: intent.depth} : {}),
			...(intent.parentJobId !== undefined ? {parentJobId: intent.parentJobId} : {}),
			...(isAgent
				? {
						agentType: intent.agentType ?? 'general-purpose',
						sessionName: sessionNameOf(id),
						...(intent.resumeSession !== undefined ? {resumedFrom: intent.resumedFrom} : {}),
					}
				: {}),
			...(intent.model !== undefined ? {model: intent.model} : {}),
			...(intent.tools !== undefined ? {tools: intent.tools} : {}),
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

	function fire(intent: JobIntent): Launch {
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
		const owner = ownerId();
		const record = makeRecord(
			{...intent, depth, ...(workerSpec?.id !== undefined ? {parentJobId: workerSpec.id} : {})},
			id,
			logPath,
			startedAt,
			owner,
		);
		writeJob(record);
		try {
			cmd.session?.appendCustomEntry({
				customType: JOB_ENTRY_TYPE,
				data: {id, label: intent.label, kind: intent.kind, state: 'queued', owner, sessionId},
			});
		} catch {
			/* audit trail is optional */
		}

		const isAgent = intent.prompt !== undefined;
		const spec = buildSpec(
			{
				id,
				label: intent.label,
				kind: intent.kind,
				owner,
				cwd: intent.cwd,
				logPath,
				inboxDir,
				jobsDir,
				startedAt,
				tailMax: LOG_TAIL_BYTES_MAX,
				bin: isAgent ? (runtime.env.COMMANDCODE_BIN ?? 'cmd') : '/bin/sh',
				sessionId,
				depth,
				...(intent.agentType !== undefined ? {agentType: intent.agentType} : {}),
				...(intent.systemPrompt !== undefined ? {systemPrompt: intent.systemPrompt} : {}),
				...(intent.model !== undefined ? {model: intent.model} : {}),
				...(intent.tools !== undefined ? {tools: intent.tools} : {}),
				...(intent.disallowedTools !== undefined ? {disallowedTools: intent.disallowedTools} : {}),
				...(intent.maxTurns !== undefined ? {maxTurns: intent.maxTurns} : {}),
				...(intent.effort !== undefined ? {effort: intent.effort} : {}),
				...(intent.permissionMode !== undefined ? {permissionMode: intent.permissionMode} : {}),
				...(intent.timeoutMs !== undefined && intent.timeoutMs > 0 ? {timeoutMs: intent.timeoutMs} : {}),
				...(intent.resumeSession !== undefined ? {resumeSession: intent.resumeSession} : {}),
				...(intent.prompt !== undefined ? {prompt: intent.prompt} : {command: intent.command}),
				...(isAgent ? {promptMarkerPath: join(logsDir, `${id}.mod`)} : {}),
			},
			MOD_PATH,
		);
		return startWorker(record, spec);
	}

	// ------------------------------------------------------------------ dashboard

	cmd.ui.pane({
		id: 'jobs',
		title: 'Async subagents',
		render: () => {
			try {
				const nodes: unknown[] = [
					{
						type: 'row',
						children: [
							{type: 'text', text: '◈ subagents', bold: true},
							{type: 'text', text: ` ${jobCache.length} recent · ${countPending()} waiting`, dim: true},
						],
					},
					{type: 'divider'},
				];
				if (jobCache.length === 0) {
					nodes.push({type: 'text', text: 'No jobs fired in this spool yet.', dim: true});
				}
				for (const job of jobCache.slice(0, PANE_ROWS_MAX)) {
					const state = canonicalState(job.state);
					const duration = typeof job.durationMs === 'number' ? ` ${(job.durationMs / 1000).toFixed(1)}s` : '';
					const color = typeof job.color === 'string' && job.color.length > 0 ? job.color : '';
					nodes.push({
						type: 'text',
						text: `${stateMarker(state)} ${job.id}  ${String(job.label ?? '(unlabelled)').slice(0, 36)}  ${state}${duration}${color.length > 0 ? `  ${color}` : ''}`,
						// The definition color is the agent's identity; the state color is the
						// fallback, so an uncoloured job still reports its lifecycle.
						color: color.length > 0 ? color : stateColor(state),
					});
				}
				nodes.push({
					type: 'row',
					children: [
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
		// Explicitly refused rather than silently ignored: a caller must not believe a
		// foreground wait or an isolation mode happened when neither landed yet.
		if (input.run_in_background === false) {
			return badInput(
				'foreground runs are not implemented in this release; omit run_in_background (background is the default)',
			);
		}
		for (const unsupported of ['permission_mode', 'isolation']) {
			if (input[unsupported] !== undefined) return badInput(`${unsupported} is not implemented in this release; omit it`);
		}
		const model = typeof input.model === 'string' && input.model.trim().length > 0 ? input.model.trim() : undefined;
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
		const requestedTools =
			prompt === undefined ? undefined : (listField(input.tools) ?? definition?.tools);
		const requestedDisallowed =
			prompt === undefined ? undefined : (listField(input.disallowed_tools) ?? definition?.disallowedTools);
		const launch = resolveLaunch({
			...(model !== undefined ? {requestedModel: model} : {}),
			...(definition?.model !== undefined ? {definitionModel: definition.model} : {}),
			...(typeof input.effort === 'string' ? {requestedEffort: input.effort} : {}),
			...(definition?.effort !== undefined ? {definitionEffort: definition.effort} : {}),
			env: runtime.env,
			// The mod API exposes no main-model getter: an absent model means the ACP
			// child inherits the user's configured model, which is the main one.
			...(requestedTools !== undefined ? {tools: requestedTools} : {}),
			...(requestedDisallowed !== undefined ? {disallowedTools: requestedDisallowed} : {}),
			...(definition?.permissionMode !== undefined ? {permissionMode: definition.permissionMode} : {}),
			// The execution mode decides this once foreground runs land; until then every
			// job is background.
			background: input.run_in_background !== false,
			mcpServers: mcpServerNames(spool, [runtime.home, cmd.cwd]),
		});
		if (!launch.ok) return badInput(launch.error);
		// Memory definitions auto-enable Read, Write, and Edit here once memory lands
		// (phase 14): `withMemoryTools(launch.tools, definition?.memory)`.
		const launchTools = launch.tools;
		const maxTurns =
			prompt === undefined
				? undefined
				: Number.isFinite(requestedMaxTurns) && requestedMaxTurns > 0
					? Math.round(requestedMaxTurns)
					: definition?.maxTurns;
		const launched = fire({
			label,
			kind: prompt !== undefined ? 'agent' : 'command',
			command,
			prompt,
			cwd: typeof input.cwd === 'string' && input.cwd.length > 0 ? input.cwd : cmd.cwd,
			agentType: definition?.name,
			systemPrompt: definition?.systemPrompt,
			model: prompt === undefined ? undefined : launch.model,
			tools: prompt === undefined ? undefined : launchTools,
			...(requestedDisallowed !== undefined ? {disallowedTools: requestedDisallowed} : {}),
			...(definition?.color !== undefined ? {color: definition.color} : {}),
			maxTurns,
			effort: prompt === undefined ? undefined : launch.effort,
			permissionMode: definition?.permissionMode,
			isolation: definition?.isolation,
			timeoutMs: Number.isFinite(timeoutMs) && timeoutMs > 0 ? Math.round(timeoutMs) : undefined,
		});
		if (!launched.ok) return badInput(launched.error);
		const kind = definition !== undefined ? `agent (${definition.name})` : 'shell (/bin/sh -c)';
		return okText(
			`spawned ${launched.id} "${label}" as ${kind}, pid ${launched.pid}.\n` +
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

	// The transcript of one job: the record's facts, the log path, and a bounded slice
	// of the log. `offset` is a character offset from the start; the default is the tail.
	function outputText(id: string, input: Record<string, unknown>): ToolResult {
		const record = readJob(id);
		if (record === undefined) return badInput(`subagent: no job ${id}`);
		const state = canonicalState(record.state);
		const file = String(record.logPath ?? join(logsDir, `${id}.log`));
		const log = readLog(file);
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
			`${stateMarker(record.state)} ${id} "${String(record.label ?? '(unlabelled)')}" · ${facts}\nlog ${file}${timing}\n\n` +
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
		const record = readJob(id);
		if (record === undefined) return badInput(`subagent: no job ${id}`);
		const state = normalizeState(record.state);
		if (state !== undefined && !isTerminal(state)) return badInput(`subagent: job ${id} is still ${state}`);
		if (record.prompt === undefined) {
			return badInput(`subagent: job ${id} is a shell job; there is no agent session to resume`);
		}
		// Resume the ACP session the first run created, so the new instruction continues
		// with the context the worker already has. An older record without one starts a
		// fresh session (the definition body still applies).
		const definition = record.agentType === undefined ? undefined : resolveAgentDefinition(record.agentType);
		const launched = fire({
			label: `${String(record.label ?? id)} (resume)`,
			kind: 'agent',
			prompt: instruction,
			cwd: String(record.cwd ?? cmd.cwd),
			agentType: record.agentType,
			systemPrompt: definition?.ok === true ? definition.definition.systemPrompt : undefined,
			model: record.model,
			...(record.tools !== undefined ? {tools: record.tools} : {}),
			...(record.color !== undefined ? {color: record.color} : {}),
			maxTurns: record.maxTurns,
			effort: record.effort,
			permissionMode: record.permissionMode,
			timeoutMs: record.timeoutMs,
			resumeSession: record.acpSessionId,
			resumedFrom: id,
		});
		if (!launched.ok) return badInput(launched.error);
		return okText(
			`resumed ${id} as ${launched.id} "${String(record.label ?? id)} (resume)", pid ${launched.pid}.\n` +
				`log ${join(logsDir, `${launched.id}.log`)}\n` +
				`The resumed run reports back here as its own signal.`,
		);
	};

	const sendRun = async ({input}: {input: Record<string, unknown>}): Promise<ToolResult> => {
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

	addSurface(
		'subagent_spawn',
		'Start a background subagent and return immediately. When it finishes, its result is delivered to this session as a separate message on its own - do not poll for it, and do not wait. Pass `prompt` for a headless Command Code agent, or `command` for a shell command. The result returns later as an agent-to-agent signal, so neither side blocks on the other; it is not this tool result.',
		{
			type: 'object',
			properties: {
				description: {type: 'string', description: 'Short name for this job, used in the signal title.'},
				label: {type: 'string', description: 'Alias for description.'},
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
				timeout_ms: {type: 'number', description: 'Kill the job after this many milliseconds and report a timeout.'},
				run_in_background: {type: 'boolean', description: 'Only true is supported in this release.'},
				cwd: {type: 'string', description: 'Working directory. Defaults to the mod workspace root.'},
			},
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
		'subagent_send',
		'Publish a signal into the agent loop of THIS session. This is the AGENT-TO-AGENT channel: worker and peer agents signal a session so neither side blocks on the other or polls for a result. It is delivered as a visible user turn in the session (echoed in the TUI), and it is durable if no session is bound yet. NEVER use it to notify the human - to reach the human, reply in the session.',
		{
			type: 'object',
			properties: {
				title: {type: 'string', description: 'One line, shown in the signal header.'},
				body: {type: 'string', description: 'The detail.'},
			},
			required: ['title'],
		},
		sendRun,
		{aliases: ['pubsub_send']},
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
		const counts: Record<string, number> = {project: 0, user: 0, builtin: 0};
		let valid = 0;
		for (const entry of entries) {
			if (!entry.valid) continue;
			valid += 1;
			counts[entry.source] = (counts[entry.source] ?? 0) + 1;
		}
		const lines = shown.map(entry => {
			const flags: string[] = [];
			if (entry.definition?.readOnly === true) flags.push('read-only');
			if (entry.definition?.oneShot === true) flags.push('one-shot');
			if (entry.source !== 'builtin' && Object.hasOwn(HOST_RESERVED_AGENT_NAMES, entry.name)) {
				flags.push('shadows a host-reserved name');
			}
			const state = entry.valid ? 'valid' : `invalid: ${entry.reason ?? 'unknown'}`;
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
			`${valid} valid definition(s) (${counts.project} project, ${counts.user} user, ${counts.builtin} built-in), ` +
			`${entries.length - valid} invalid.\n` +
			(lines.length > 0 ? lines.join('\n') : '  (none)')
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
		try {
			cmd.ui.openPane('jobs');
		} catch {
			/* headless */
		}
		return {message: 'subagents: opened the async subagents pane (list | jobs | cancel <id> | open <id>)'};
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
		name: 'agents',
		description: 'List subagent definitions: scope, validity, and the file that provides each one',
		argumentHint: '[name|path]',
		handler: ({args}: {args?: string}) => ({message: agentsText(args)}),
	});

	// ------------------------------------------------------------------ lifecycle

	cmd.hooks({
		// The worker process loads this mod with `cmd --mod <file> acp`. Its whole
		// purpose there is this hook: the definition body is the worker's system
		// prompt. The parent already has the definition, so only worker mode registers
		// it, and only when a body was passed.
		...(workerSpec?.systemPrompt !== undefined && workerSpec.systemPrompt.length > 0
			? {appendSystemPrompt: () => String(workerSpec?.systemPrompt ?? '')}
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
		},
		// The race guard. A worker can finish in the window between the last turn and the
		// next drain tick, and the session would then fall idle with the result unread.
		// Pull whatever is still on disk into THIS run as the continuation reason (A2A's
		// onStop inbox drain). Anything already delivered is no longer in the inbox, so
		// this cannot double-report a signal.
		onStop: () => {
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

	cmd.clock.every(DRAIN_INTERVAL_MS, safeDrain);
	cmd.clock.every(REFRESH_INTERVAL_MS, refreshJobs);
	cmd.clock.every(RETENTION_SWEEP_INTERVAL_MS, sweepExpired);

	// Seed the dashboard and reconcile anything a previous process left behind.
	ownerId();
	reconcileJobs();
	refreshJobs();
}
