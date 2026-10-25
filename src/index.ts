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
import {dirname, isAbsolute, join, resolve} from 'node:path';
import {
	ADD_DIRS_OPTION,
	AGENT_API_ERROR,
	AGENT_LOG_FILE,
	AGENT_SCAN_MAX,
	CLI_AGENTS_OPTION,
	DELIVERED_LEDGER_KEEP,
	DELIVERED_LEDGER_MAX,
	DRAIN_INTERVAL_MS,
	FORK_AGENT_TYPE,
	FORK_MODE_FLAG,
	HOST_RESERVED_AGENT_NAMES,
	LEDGER_COMPACT_BYTES,
	LIST_SCAN_MAX,
	LOST_GRACE_MS,
	MANAGED_SETTINGS_DEFAULT,
	MOD_PATH,
	OUTPUT_MAX_CHARS_DEFAULT,
	OWNER_TYPE,
	OWNER_TYPE_LEGACY,
	PANE_ROWS_MAX,
	PLUGIN_DIRS_OPTION,
	PUBSUB_DIR_DEFAULT,
	RECONCILE_SCAN_MAX,
	REFRESH_INTERVAL_MS,
	RETENTION_SWEEP_INTERVAL_MS,
	ROSTER_MAIN,
	SUBAGENT_DIR_DEFAULT,
	TASK_ROW_GRACE_MS,
	TEAM_TOOL_NAMES,
	WORKER_SPEC_ENV,
} from './constants.ts';
import type {
	ChildHandle,
	CommandResult,
	JobPatch,
	Runtime,
	RuntimeOverrides,
	Signal,
	SpoolFs,
} from './types.ts';
import {
	AGENT_SCOPES,
	builtinAgentDefinitions,
	createAgentRegistry,
	descriptionLimit,
	hasGitMarker,
	hookList,
	listField,
	stringField,
	stringMapField,
} from './definitions.ts';
import type {AgentDefinition, AgentHookRule, AgentMcpServer, AgentMemoryScope, AgentScope} from './definitions.ts';
import {canTransition, canonicalState, isTerminal, isTerminalState, normalizeState, stateColor, stateMarker} from './state.ts';
import {resolveAppendSystemPrompt, resolveAutoCompactWindow} from './append.ts';
import {createJobStore} from './jobs.ts';
import type {JobRecord} from './jobs.ts';
import {createLedger} from './ledger.ts';
import {createDelivery} from './delivery.ts';
import {patchOf, signalEnvelope, signalOf} from './signals.ts';
import type {Ownership} from './signals.ts';
import {EFFORT_LEVELS, filterTools, nonEmpty, withheldBy, workerToolAllowed} from './tool-filter.ts';
import {
	agentNameFromEnv,
	agentSdk,
	backgroundDisabled,
	contextWindowOverride,
	effortLevel,
	effortLevelFromEnv,
	forkModeFromEnv,
	managedSettingsDir,
	managedSettingsHome,
	mainPermissionModeFromEnv,
	mcpPolicy,
	optionList,
	optionValue,
	pickOption,
	policyFiles,
	subagentModel,
	subagentModelForced,
	toggle,
	workerModeFromEnv,
} from './config.ts';
import {gitSeam, guardToolCall, isInside, isolationRoots} from './worktree.ts';
import {createWorktreeLifecycle, sweepExpiredEntries} from './worktree-lifecycle.ts';
import {recordFromPlan, sessionNameOf} from './plan.ts';
import {createTools} from './tools.ts';
import type {PlanContext} from './plan.ts';
import {answerPendingPermission as answerPermission, failJob, fire} from './spawn.ts';
import type {ForegroundResult, PermissionContext, SpawnContext, WorkerSpec} from './spawn.ts';

export {buildArgs, buildSpec, resolveMaxConcurrent, resolveMaxDepth, workerEnv} from './spawn.ts';
export type {ForegroundResult, LimitEnv, WorkerSpec, WorkerSpecInput} from './spawn.ts';

export type {
	ChildHandle,
	CommandResult,
	CompactBoundary,
	PermissionRequest,
	PermissionRequestOption,
	Runtime,
	RuntimeOverrides,
	SpawnOptions,
	SpoolFs,
} from './types.ts';
export type {AppendPromptInput} from './append.ts';
export type {Ledger, LedgerDeps} from './ledger.ts';
export {signalEnvelope} from './signals.ts';
export type {ToolFilterInput, ToolFilterResult} from './tool-filter.ts';
export type {GitResult, GitSeam, IsolationResult, IsolationRoots, IsolationSpec, WorktreeDecision, WorktreeSeam} from './worktree.ts';
export {jobStates} from './state.ts';
export {
	commandEscapesWorktree,
	createJobWorktree,
	defaultBranch,
	finalizeWorktree,
	gitSeam,
	guardToolCall,
	isolationRoots,
	mainCheckoutRoots,
	worktreeChanged,
} from './worktree.ts';
export {resolveAppendSystemPrompt, resolveAutoCompactWindow} from './append.ts';
export {ledgerFactory} from './ledger.ts';
export {sweepExpiredEntries};
export {createWorktreeLifecycle};
export type {SweepIo, SweepOptions} from './worktree-lifecycle.ts';
export {
	BACKGROUND_BUILTIN_TOOLS,
	EFFORT_LEVELS,
	SUBAGENT_TOOL_UNIVERSE,
	UNIVERSAL_TOOL_REMOVALS,
	filterTools,
	toolNameOfSpecifier,
	workerToolAllowed,
} from './tool-filter.ts';
export {
	AGENT_SCOPES,
	BUILTIN_AGENT_DEFINITIONS,
	DESCRIPTION_LIMIT_TOKENS,
	agentDefinitionFromSource,
	builtinAgentDefinitions,
	createAgentRegistry,
	descriptionLimit,
	parseCacheTtl,
	parseFrontmatter,
	parseHooks,
	parseMcpServers,
} from './definitions.ts';
export type {
	AgentDefinition,
	AgentDefinitionOptions,
	AgentDefinitionResult,
	AgentEntry,
	AgentEntryKind,
	AgentHookRule,
	AgentHooks,
	AgentMcpServer,
	AgentMemoryScope,
	AgentRegistry,
	AgentRegistryDeps,
	AgentScope,
	CacheTtl,
	FrontmatterResult,
} from './definitions.ts';
export {AGENT_API_ERROR, TEAM_TOOL_NAMES};

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

// The adapter's own cadence. The parent sets both on every spec; the fallbacks
// are for a spec written by an older build.
const CONTROL_POLL_MS = typeof spec.controlPollMs === 'number' && spec.controlPollMs > 0 ? spec.controlPollMs : 100;
const PERMISSION_TIMEOUT_MS = typeof spec.permissionTimeoutMs === 'number' && spec.permissionTimeoutMs > 0 ? spec.permissionTimeoutMs : 30000;

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
    id: spec.id, description: spec.description, kind: spec.kind, owner: spec.owner,
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

// A command runs in its OWN process group. The timeout has to be able to stop
// everything the command started, not just the shell: a command that
// backgrounds itself - "(sleep 30; ...) &" - left that grandchild running
// after the job reported a timeout, so the work carried on past the bound the
// caller set. The group is led by the shell, and the group is what gets
// signalled.
const isCommand = spec.kind !== 'fork' && spec.kind !== 'agent';
const child = isFork
  ? spawn(spec.bin, spec.args || [], {cwd: spec.cwd, stdio: ['ignore', 'pipe', 'pipe']})
  : spec.kind === 'agent'
    ? spawn(spec.bin, spec.args || [], {cwd: spec.cwd, stdio: ['pipe', 'pipe', 'pipe']})
    : spawn(spec.bin, spec.args || [], {cwd: spec.cwd, stdio: ['ignore', out, out], detached: true});

if (isFork && child.stdout && child.stderr) {
  child.stdout.on('data', chunk => { forkStdout += chunk.toString(); });
  child.stderr.on('data', chunk => logWrite(chunk.toString()));
}

let timedOut = false;
let killTimer;
let hardTimer;

// A command's whole process group is signalled, not just the shell. The
// negative pid addresses the group, so a grandchild the command started dies
// with it. A command that is not detached has no group of its own, and the
// pid alone is the right target there.
const signalChild = (signal) => {
  try {
    if (isCommand && typeof child.pid === 'number') process.kill(-child.pid, signal);
    else child.kill(signal);
  } catch (e) {
    try { child.kill(signal); } catch (e2) {}
  }
};

// A timeout is still one result: SIGTERM, SIGKILL a second later, and a failed
// signal that says why. Nothing is left running and nothing is left silent.
if (typeof spec.timeoutMs === 'number' && spec.timeoutMs > 0) {
  killTimer = setTimeout(() => {
    timedOut = true;
    signalChild('SIGTERM');
    hardTimer = setTimeout(() => { signalChild('SIGKILL'); }, 1000);
    if (hardTimer.unref) hardTimer.unref();
  }, spec.timeoutMs);
  if (killTimer.unref) killTimer.unref();
}

const clearTimers = () => {
  clearTimeout(killTimer);
  clearTimeout(hardTimer);
};

// Is anything left in the command's process group? A command can exit at once
// and leave a background child behind, so the shell's exit does not mean the
// work is done. The group is asked, not guessed.
const groupAlive = () => {
  if (!isCommand || typeof child.pid !== 'number') return false;
  try {
    // Signal 0 tests for existence without delivering anything.
    process.kill(-child.pid, 0);
    return true;
  } catch (e) {
    return false;
  }
};

// The group has to actually stop before the result is reported. A command that
// exits at once - "(sleep 30; ...) &" - leaves a background child running, and
// the caller would get its result while that work kept going. The group is
// signalled, then given a moment to stop, then killed outright. The result is
// only written once the group is gone.
const drainGroup = (onDone) => {
  if (!groupAlive()) {
    onDone();
    return;
  }
  let escalated = false;
  signalChild('SIGTERM');
  const poll = setInterval(() => {
    if (!groupAlive()) {
      clearInterval(poll);
      onDone();
      return;
    }
    if (!escalated) {
      escalated = true;
      signalChild('SIGKILL');
    }
  }, 100);
  if (poll.unref) poll.unref();
  // A group that cannot be killed must not hang the worker forever.
  setTimeout(() => {
    clearInterval(poll);
    onDone();
  }, 3000);
};

// The ACP child is long-lived only for the turn. After the signal is written,
// close its stdin, then make sure it cannot outlive the worker.
const shutdown = () => {
  try { child.stdin.end(); } catch (e) {}
  signalChild('SIGTERM');
  setTimeout(() => {
    signalChild('SIGKILL');
    process.exit(0);
  }, 1000);
};

const finish = (code, spawnError, signal) => {
  if (settled) return;
  const record = readRecord();
  // The cancel tool has already written the record and emitted its own signal.
  if (record.state === 'canceled') {
    settled = true;
    clearTimers();
    shutdown();
    return;
  }
  if (spec.kind === 'fork') {
    clearTimers();
    finishFork(code, spawnError, signal);
    return;
  }

  // A command's group is stopped first. The result is not reported until the
  // group is gone, so the caller never gets "done" while the command's work is
  // still running. The timeout's own escalation is left alone: it is the thing
  // that stops a group that ignores SIGTERM.
  if (isCommand) {
    drainGroup(() => {
      clearTimers();
      finishCommand(code, spawnError, signal);
    });
    return;
  }
  clearTimers();
  finishCommand(code, spawnError, signal);
};

const finishCommand = (code, spawnError, signal) => {
  if (settled) return;
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
  const failed = timedOut || spawnError !== undefined || code !== 0 || (typeof signal === 'string' && signal.length > 0);
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
  }, CONTROL_POLL_MS);
  if (timer.unref) timer.unref();
  writeState(false);
  const idleMs = typeof spec.idleMs === 'number' && spec.idleMs >= 0 ? spec.idleMs : 30000;
  while (!canceled) {
    if (queue.length === 0) {
      const deadline = Date.now() + idleMs;
      while (queue.length === 0 && !canceled && Date.now() < deadline) {
        const {promise, resolve} = Promise.withResolvers();
        setTimeout(resolve, Math.min(50, CONTROL_POLL_MS));
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
      description: spec.description + ' (message)',
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
      title: (spec.name || spec.description) + (failure === undefined ? ' replied' : ' failed a live turn'),
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
    const timeout = PERMISSION_TIMEOUT_MS;


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
    bypassPermissions: 'bypass',
    // No ACP classifier exists; prompting is the closest observable behavior.
    auto: 'default'
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

  const controlTimer = setInterval(pollControl, CONTROL_POLL_MS);
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
      description: spec.description + ' (message)',
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
      title: (spec.name || spec.description) + (failure === undefined ? ' replied' : ' failed a live turn'),
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
        const detail = String(error && error.message || error);
        logWrite('[acp] resume failed: ' + detail + '\\n');
        throw new Error('session/resume failed: ' + detail);
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
        if (spec.permissionMode === 'auto') {
          // The host's auto classifier has no ACP mode. The prompting mode is the
          // closest observable behavior: nothing is auto-allowed (ADR 0001).
          logWrite('[acp] permissionMode auto -> default (no ACP classifier; the prompting mode is the closest behavior)\\n');
        }
        try {
          await request('session/set_mode', {sessionId: sessionId, modeId: modeIds[spec.permissionMode]});
        } catch (error) {
          logWrite('[acp] mode not set: ' + String(error && error.message || error) + '\\n');
        }
      }
    }
    // The tool set is resolved in the parent and enforced by this mod's
    // beforeToolCall in the ACP child; the log records the same set.
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
    // The tool check lives in this mod's beforeToolCall. If the ACP child did not
    // load the mod, a restricted agent would keep every host tool; refuse it
    // rather than run with a false sense of enforcement.
    if (spec.restrictedTools === true && !hookReady) {
      settle('failed', 'restricted tools require the worker hook; the ACP child did not load the mod', null, '', {});
      shutdown();
      return;
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
	// The fallback for a host without `cmd.clock.after`, and the seam the tests drive.
	later: (ms, fn) => {
		const timer = setTimeout(fn, ms);
		if (typeof timer === 'object' && timer !== null && 'unref' in timer && typeof timer.unref === 'function') timer.unref();
	},
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
// Hook execution
//
// A hook is a matcher and a command, run through the injected `run` with the
// event, agent, tool, and payload in its environment. The rules are parsed with
// the definition (`./definitions.ts`); this section only decides what a run means.
// ---------------------------------------------------------------------------

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
	readonly prompt?: string;
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

// Project discovery walks up from the working directory to the repository root and
// renders outermost first, so broad instructions come before the nearest ones.
function projectDirsUpward(cwd: string, fs?: Pick<SpoolFs, 'stat'>): string[] {
	const dirs: string[] = [];
	let current = cwd;
	for (let depth = 0; depth < STARTUP_WALK_DEPTH_MAX; depth += 1) {
		dirs.push(current);
		if (fs !== undefined && hasGitMarker(fs, current)) break;
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
	const policy = policyFiles(env, options.home);
	// Managed policy loads even under omitClaudeMd, except for a managed agent.
	if (!skipHierarchy && definition?.scope !== 'managed') {
		const managed = startupFile(fs, policy.managed);
		if (managed !== undefined) claudeMd.push(managed);
	}
	if (!skipHierarchy && definition?.omitClaudeMd !== true) {
		const user = startupFile(fs, policy.user);
		if (user !== undefined) claudeMd.push(user);
		for (const dir of projectDirsUpward(options.cwd, options.fs)) {
			const project = startupFile(fs, join(dir, 'CLAUDE.md'));
			if (project !== undefined) claudeMd.push(project);
			const local = startupFile(fs, join(dir, 'CLAUDE.local.md'));
			if (local !== undefined) claudeMd.push(local);
		}
		// AGENTS.md is project instructions too, so it follows the same omit rule.
		for (const dir of projectDirsUpward(options.cwd, options.fs)) {
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
	for (const dir of projectDirsUpward(options.cwd, options.fs)) {
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
	const override = contextWindowOverride(env);
	if (override !== undefined && override >= 1000) return Math.min(CONTEXT_WINDOW_MAX, Math.round(override));
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
	const environment = [
		'# Environment',
		`working directory: ${options.cwd}`,
		`platform: ${options.platform ?? 'unknown'}`,
	];
	// The date is informational and only rendered when the caller supplied the
	// seam's timestamp; nothing here reads the OS clock.
	if (options.now !== undefined) environment.push(`date: ${new Date(options.now).toISOString().slice(0, 10)}`);
	if (options.model !== undefined) environment.push(`model: ${options.model}`);
	environment.push(`context window: ${contextWindow} tokens`);
	const base: Omit<StartupBundle, 'text'> = {
		systemPrompt: definition?.prompt ?? '',
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
	// `main` is always addressable, so the roster is never empty: a lone named
	// agent still learns how to reach the conversation that started it.
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
	const envModel = subagentModel(env);
	const forced = subagentModelForced(env);
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

// Per-invocation, frontmatter, environment. There is no effort inheritance from the
// main conversation: the runtime default applies when nothing is set.
export function resolveEffort(input: EffortResolutionInput = {}): EffortResolution {
	const env = input.env ?? {};
	const requested = effortLevel(input.requested, EFFORT_LEVELS);
	if (requested !== undefined) return {effort: requested, source: 'parameter'};
	const definition = effortLevel(input.definition, EFFORT_LEVELS);
	if (definition !== undefined) return {effort: definition, source: 'definition'};
	const fromEnv = effortLevelFromEnv(env, EFFORT_LEVELS);
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
	if (tools.ok === false) return tools;
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
	definition: {readonly scope?: string; readonly sourcePath?: string},
	roots: McpTrustRoots,
): boolean {
	if (definition.scope === 'plugin') return false;
	const path = definition.sourcePath;
	if (path === undefined || path.length === 0) return true;
	const absolute = isAbsolute(path) ? path : join(roots.cwd, path);
	const trustedRoots = [
		roots.cwd,
		roots.home,
		roots.managedDir ?? managedSettingsHome(roots.home),
		...(roots.addDirs ?? []),
	];
	return trustedRoots.some(root => root.length > 0 && isInside(root, absolute));
}

export interface McpResolutionInput {
	/** The definition's authored `mcpServers`. */
	readonly defined?: readonly AgentMcpServer[];
	readonly configured?: Record<string, McpServerConfig>;
	readonly definition?: {readonly scope?: string; readonly path?: string};
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

// The one resolution: a name resolves from configured servers, an inline entry
// needs trust, and strict/bare/allow/deny policies apply afterwards. The result
// is what `session/new` receives, so the scope is exactly this agent's.
export function resolveAgentMcpServers(input: McpResolutionInput = {}): McpResolution {
	const env = input.env ?? {};
	const defined = input.defined ?? [];
	const dropped: McpDrop[] = [];
	if (input.definition?.scope === 'plugin') {
		for (const spec of defined) dropped.push({name: spec.name, reason: 'plugin definitions ignore mcpServers'});
		return {servers: [], dropped};
	}
	const policy = mcpPolicy(env);
	if (policy.bare) {
		for (const spec of defined) dropped.push({name: spec.name, reason: 'bare mode disables MCP'});
		return {servers: [], dropped};
	}
	const strict = policy.strict;
	const allow = policy.allow;
	const deny = policy.deny;
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
	if (backgroundDisabled(input.env)) return 'foreground';
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

export function resolveForkMode(input: ForkModeInput = {}): ForkMode {
	const flag = toggle(input.flag);
	if (flag !== undefined) return flag;
	const env = input.env ?? {};
	const fromEnv = forkModeFromEnv(env);
	if (fromEnv !== undefined) return fromEnv;
	const inSdk = input.sdk === true || agentSdk(env);
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
	readonly description?: unknown;
	readonly id?: unknown;
	readonly kind?: unknown;
	/** A maxTurns or max_tokens stop: the result is partial and resumable. */
	readonly partial?: unknown;
}

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
	const label = String(job.description ?? job.id ?? 'subagent');
	const full = reportText(log, String(job.kind ?? 'agent')).trim();
	// The tool result is the model's context, so the inline result is bounded like
	// every other read. The full log stays at the job's log path.
	const text = full.length > OUTPUT_MAX_CHARS_DEFAULT ? `…${full.slice(-OUTPUT_MAX_CHARS_DEFAULT)}` : full;
	if (state === 'done') {
		return {ok: true, partial: false, text: text.length > 0 ? text : `${label} finished with no output.`};
	}
	if (state === 'waiting-input') {
		const followUp =
			String(job.kind ?? 'agent') === 'command'
				? `inspect it with subagent_output ${String(job.id)}`
				: `resume it with subagent_resume ${String(job.id)}`;
		return {
			ok: true,
			partial: true,
			text: `${text.length > 0 ? `${text}\n\n` : ''}${label} stopped with a question; ${followUp}.`,
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
		if (!isTerminalState(state)) return false;
		return now - finishedAt <= TASK_ROW_GRACE_MS;
	});
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

	// The flag's directories plus any `/add-dir` the user ran. One array, mutated in
	// place, so the registry, the MCP trust roots and every refresh agree.
	const addDirs = optionList(cmd, ADD_DIRS_OPTION);
	// A headless worker loads this mod too. It has its own owner token and must never
	// steal an unaddressed signal from the session that fired it.
	const workerMode = workerModeFromEnv(runtime.env);
	// The worker carries the parent's absolute spool paths in its spec; derive the
	// root from them so send, handback, and nested spawns use the same spool.
	const workerRoot = (() => {
		if (!workerMode) return undefined;
		try {
			const spec = JSON.parse(String(runtime.env[WORKER_SPEC_ENV] ?? '')) as WorkerSpec;
			return typeof spec.inboxDir === 'string' ? dirname(spec.inboxDir) : undefined;
		} catch {
			return undefined;
		}
	})();
	const root = workerRoot ?? resolve(pickOption(cmd, 'subagent_dir', SUBAGENT_DIR_DEFAULT, 'pubsub_dir', PUBSUB_DIR_DEFAULT));
	const forkMode = resolveForkMode({
		interactive: runtime.interactive === true,
		env: runtime.env,
		flag: optionValue(cmd, FORK_MODE_FLAG),
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

	// The main conversation's permission mode. There is no getter in the host API:
	// the mode arrives through `permission_mode_changed` and the system-prompt hook
	// context, and the environment override covers a host that knows it before the
	// first event (and the suite). Absent means `default`, the safe reading: a
	// definition can then never grant itself bypass.
	let mainPermissionMode: CanonicalPermissionMode | undefined = canonicalPermissionMode(
		mainPermissionModeFromEnv(runtime.env),
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
		managedDir: managedSettingsDir(cmd),
		addDirs,
		pluginRoots: optionList(cmd, PLUGIN_DIRS_OPTION),
		cli: {value: optionValue(cmd, CLI_AGENTS_OPTION), interactive: runtime.interactive === true},
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
	// marker tells the worker whether the mod is actually live in the ACP child;
	// without it the worker carries the body in the prompt instead, and a
	// restricted agent refuses to run at all.
	//
	// The marker says "the mod loaded", not "the definition has a body". An agent
	// with an empty definition body and no appended text has no system prompt, and
	// gating the marker on that left the worker believing the mod had not loaded:
	// it stopped before the task, and the tool hook was never enforced.
	if (workerMode && workerSpec?.promptMarkerPath !== undefined) {
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
		return (optionValue(cmd, 'agent') || agentNameFromEnv(runtime.env)).trim();
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
	// The worker's spec already carries the resolved text. Reading it back through
	// the `@path`-aware resolver would treat a body that starts with `@` as a file.
	const appendPromptText =
		workerMode && typeof workerSpec?.appendSystemPrompt === 'string'
			? workerSpec.appendSystemPrompt
			: resolveAppendSystemPrompt({
					flag: optionValue(cmd, 'append_subagent_system_prompt'),
					file: optionValue(cmd, 'append_subagent_system_prompt_file'),
					env: runtime.env,
					fs: spool,
					cwd: cmd.cwd,
				});

	function subagentSystemPrompt(base: string | undefined): string | undefined {
		const parts = [base ?? '', appendPromptText].filter(part => part.length > 0);
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
		(pickOption(cmd, 'subagent_deliver', 'steer', 'pubsub_deliver', 'steer') === 'follow-up' ? 'follow-up' : 'steer');

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

	// The identity this process's jobs and signals carry. A worker is not a session
	// of its own: it continues the parent's identity, so a nested job's signal is the
	// parent's to deliver and the parent's pool counts it. Minting a token in a
	// worker would make every nested result foreign.
	function currentOwner(): string | undefined {
		if (workerMode && typeof workerSpec?.owner === 'string' && workerSpec.owner.length > 0) return workerSpec.owner;
		return ownerId();
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
	const renderSignalRow = (data: unknown): string[] => {
		const signal = data as Partial<Signal>;
		const state = signal.state === undefined ? '' : ` · ${signal.state}`;
		return [`${stateMarker(signal.state)} subagent · ${String(signal.kind ?? 'signal')}${state} · ${String(signal.title ?? '(untitled)')}`];
	};
	cmd.addRenderer('subagent', renderSignalRow);
	cmd.addRenderer('pubsub', renderSignalRow);

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

	// The durable record store. The spool and the jobs path arrive explicitly, so
	// the store names no ambient state and every read, write and scan agrees.
	const {readJob, writeJob, readRecentJobs} = createJobStore({
		fs: spool,
		jobsDir,
		pid: runtime.pid,
		now: () => runtime.now(),
	});

	// The worktree decision and retention live in their own leaf. It receives the
	// spool, the git seam, the clock, liveness and the record store, so the names it
	// returns are the ones the rest of the entry calls.
	const {finalizeJobWorktree, sweepExpired, liveState} = createWorktreeLifecycle({
		fs: spool,
		git,
		now: () => runtime.now(),
		alive: pid => runtime.alive(pid),
		env: runtime.env,
		cwd: runtime.cwd,
		home: runtime.home,
		jobsDir,
		logsDir,
		transcriptsDir,
		worktreesDir,
		controlRoot,
		workerMode,
		readJob,
		writeJob,
	});

	// The permission handshake reads only the spool, the clock, and the human; the
	// two callers (the drain and the foreground wait) share one context.
	//
	// They also share one set of requests already put to the human. The drain
	// polls every 400ms and the foreground wait polls the same control dir, so a
	// request must reach the human once, not once per poller.
	const askedPermissions = new Set<string>();
	const permissionCtx: PermissionContext = {
		fs: spool,
		now: () => runtime.now(),
		pid: process.pid,
		get ui() {
			return cmd.ui;
		},
	};

	// The drain, the signal envelope and the refresh tick live in their own module.
	// It receives every seam it touches instead of closing over this file, so the
	// names it returns are the ones the rest of the entry calls.
	const {
		safeDrain,
		claimPending,
		settle,
		markDelivered,
		formatSignalMessage,
		countPending,
		countOrphaned,
		refreshJobs,
		sweepClaimed,
		sweepOrphaned,
	} = createDelivery({
		fs: spool,
		inboxDir,
		claimedDir,
		orphanedDir,
		controlRoot,
		ledger,
		now: () => runtime.now(),
		readJob,
		writeJob,
		readRecentJobs,
		finalizeWorktree: finalizeJobWorktree,
		ownerVerdict,
		workerMode,
		hasSession: () => cmd.session !== undefined,
		deliverAs,
		sendUserMessage: (text, options) => cmd.sendUserMessage(text, options),
		showEntry: (type, data) => cmd.showEntry(type, data),
		answerPendingPermission: (controlDir, answered) => answerPermission(controlDir, answered, permissionCtx),
		askedPermissions,
		jobs: () => jobCache,
		setJobs: jobs => {
			jobCache = jobs;
		},
		sweepSubagentStopHooks,
		updateStatus,
		markJobsTick: () => {
			try {
				cmd.state.set('jobsTick', runtime.now());
			} catch {
				/* pre-bind or a host without state */
			}
		},
		sweep: (dir, ttlMs) => sweepExpiredEntries(spool, dir, runtime.now(), {ttlMs}),
	});

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

	// ------------------------------------------------------------------ live
	//
	// One live-message queue per job. A sender appends a JSON line; the worker
	// consumes it at the next safe point (see WORKER_SOURCE). A worker's state.json
	// says whether a turn is active, so the tool can report "queued" vs "starts a
	// turn" without guessing. Names resolve inside this session's jobs only: another
	// session's roster is not this session's to message.

	// One scan of this session's named jobs, newest first. The roster, ListAgents
	// and subagent_send all ask this, so they cannot disagree about which record a
	// name means. The parent filters by its owner; a worker is gated by the roster
	// the parent handed it instead, so a peer owned by another session is still the
	// peer that roster names. A legacy record with no owner belongs to whoever names it.
	function ownedNamedJobs(): JobRecord[] {
		const owner = workerMode ? undefined : currentOwner();
		return readRecentJobs(AGENT_SCAN_MAX).filter(job => {
			if (typeof job.name !== 'string' || job.name.length === 0) return false;
			return owner === undefined || job.owner === undefined || job.owner === owner;
		});
	}

	function rosterNames(): string[] {
		if (workerMode) return Array.isArray(workerSpec?.rosterNames) ? workerSpec.rosterNames : [];
		const names: string[] = [];
		for (const job of ownedNamedJobs()) {
			const name = job.name;
			if (typeof name === 'string' && !names.includes(name)) names.push(name);
		}
		return names;
	}

	function namedJob(name: string): JobRecord | undefined {
		const wanted = name.trim();
		if (wanted.length === 0) return undefined;
		const allowed = workerMode ? rosterNames() : undefined;
		if (allowed !== undefined && !allowed.includes(wanted)) return undefined;
		const matches = ownedNamedJobs().filter(job => job.name === wanted);
		// Prefer the record whose worker is still alive: a resumed job shares the
		// name with the record it continued, and the live one is the address.
		return matches.find(job => runtime.alive(job.workerPid) || runtime.alive(job.pid)) ?? matches[0];
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
		const entries: RosterEntry[] = [];
		const names: string[] = [];
		for (const job of ownedNamedJobs()) {
			const name = job.name;
			if (typeof name !== 'string' || name === selfName || names.includes(name)) continue;
			names.push(name);
			entries.push({name, state: canonicalState(job.state), jobId: job.id});
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
			if (state !== undefined && isTerminal(state)) finalizeJobWorktree(job);
			if (state === 'queued' || state === 'running') {
				if (runtime.alive(job.pid) || runtime.alive(job.workerPid)) continue;
				if (hasPendingSignal(job.id)) continue;
				const startedAt = Number(job.startedAt ?? 0) || 0;
				if (now - startedAt < LOST_GRACE_MS) continue;
				// One record, one signal, one delivery - the same contract every other
				// failure uses. A job already reconciled is terminal, so a second session
				// start cannot replay it.
				failJob(job, state === 'queued' ? 'worker never started' : 'lost (no live process)', spawnCtx);
			} else if (state !== undefined && state !== job.state) {
				// A legacy alias is rewritten as its state, once.
				applyPatch(job.id, {state}, job, now);
			}
		}
	}

	let jobCache: JobRecord[] = [];
	let lastStatus = '';

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
		const owner = currentOwner();
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
			const agentId = typeof job.agentId === 'string' && job.agentId.length > 0 ? ` · agent id ${job.agentId}` : '';
			const name = typeof job.name === 'string' && job.name.length > 0 ? ` · name=${job.name}` : '';
			return `  ${stateMarker(state)} ${job.id}  ${String(job.description ?? '(unlabelled)')}  [${state}${exit}${duration}]${agentType}${agentId}${name}${model}${effort}${color}${owner}${unread}`;
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
			return {ok: true, message: `subagent: stopped live agent ${id} (${String(job.description ?? 'unlabelled')})`};
		}

		const now = runtime.now();
		const startedAt = Number(job.startedAt ?? now) || now;
		// Signal from the record, record write from the signal: one envelope.
		const signal = signalOf(
			{
				...job,
				owner: typeof job.owner === 'string' && job.owner.length > 0 ? job.owner : currentOwner(),
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
			runtime.later(1000, escalate);
		}

		enqueueSignal(signal);
		safeDrain();
		refreshJobs();
		return {ok: true, message: `subagent: canceled ${id} (${String(job.description ?? 'unlabelled')})`};
	}

	// Ctrl+B: a foreground tool call that is already waiting. The next poll sees the
	// id here and returns control to the model; the worker keeps running.
	const backgroundedForegrounds = new Set<string>();
	const dismissedTasks = new Set<string>();
	let selectedTaskId: string | undefined;
	let agentViewOpen = false;
	let panelNotice: string | undefined;

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
						text: `${selected}${stateMarker(state)} ${job.id}  ${String(job.description ?? '(unlabelled)').slice(0, 36)}  ${state}${duration}${color.length > 0 ? `  ${color}` : ''}`,
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
	// The tool surface lives in src/tools.ts. The entry builds the context the
	// module needs and owns the two contexts the plan and the spawn read.

	// One log file, read through the spool. The spawn reads it to settle a
	// foreground run; the tool surface reads it to show the output.
	function readLog(file: string): string {
		try {
			return spool.readFile(file);
		} catch {
			return '';
		}
	}

	// The plan and the spawn read this session's state through these contexts, so
	// neither module closes over the composition root.
	const planCtx: PlanContext = {
		fs: spool,
		env: runtime.env,
		cwd: cmd.cwd,
		runtimeCwd: runtime.cwd,
		home: runtime.home,
		transcriptsDir,
		appendPromptText,
		forkMode,
		forkModeEnabled,
		managedDir: managedSettingsDir(cmd),
		addDirs,
		sessionId: () => sessionId,
		mainPermissionMode: () => mainPermissionMode,
		resolveAgentDefinition,
		systemPromptFor: subagentSystemPrompt,
		startupFor: (definition, cwd, prompt, model) =>
			startupFor(definition as unknown as AgentDefinition | undefined, cwd, prompt, model),
		canonicalPermissionMode,
		resolveTeamPolicy,
		resolvePermissionMode,
		resolveExecutionPolicy,
		mcpServerConfigs,
		resolveAgentMcpServers: input =>
			resolveAgentMcpServers({
				defined: input.defined,
				configured: input.configured as Record<string, McpServerConfig>,
				definition: input.definition as unknown as {scope?: string; path?: string} | undefined,
				env: input.env,
				trusted: input.trusted,
			}),
		agentFolderTrusted: (definition, roots) =>
			agentFolderTrusted(
				definition as unknown as {readonly scope?: string; readonly sourcePath?: string} | undefined,
				roots,
			),
		resolveLaunch: input => resolveLaunch(input as LaunchResolutionInput),
		withMemoryTools,
		contextWindowForModel,
		resolveAutoCompactWindow,
		listField,
	};

	const spawnCtx: SpawnContext = {
		fs: spool,
		env: runtime.env,
		execPath: runtime.execPath,
		spawn: runtime.spawn,
		now: () => runtime.now(),
		sleep: runtime.sleep,
		workerSpec,
		workerSource: WORKER_SOURCE,
		modPath: MOD_PATH,
		inboxDir,
		jobsDir,
		logsDir,
		worktreesDir,
		controlRoot,
		git,
		sessionId: () => sessionId,
		owner: currentOwner,
		recordFromPlan: (intent, id, logPath, startedAt, owner) =>
			recordFromPlan(intent, id, logPath, startedAt, owner, planCtx),
		sessionNameOf,
		readJob,
		writeJob,
		readRecentJobs,
		applyPatch,
		enqueueSignal,
		refreshJobs,
		finalizeJobWorktree,
		foregroundOutcome,
		readLog,
		rosterFor,
		runProjectHooks: (event, cwd, agent, input) =>
			runHookRules(runtime.run, projectHookRules(event), {event, cwd, agent, input}),
		appendJobEntry: entry => {
			cmd.session?.appendCustomEntry(entry as never);
		},
		answerPendingPermission: (controlDir, answered) => answerPermission(controlDir, answered, permissionCtx),
		askedPermissions,
		backgrounded: backgroundedForegrounds,
	};

	// The tool surface. The module owns the tools; the entry owns the state they
	// read. `sessionId` and `workerSpec` are bound after this point, so they are
	// read through functions rather than captured by value.
	const tools = createTools({
		cmd,
		planCtx,
		spawnCtx,
		runtime,
		spool,
		logsDir,
		transcriptsDir,
		readJob,
		writeJob,
		readRecentJobs,
		refreshJobs,
		enqueueSignal,
		safeDrain,
		cancelJob,
		registry,
		ledger,
		currentOwner,
		ownerId,
		rosterNames,
		namedJob,
		liveState,
		appendLive,
		workerMode,
		workerSpec: () => workerSpec,
		sessionId: () => sessionId,
		forkModeEnabled,
		listText,
		scanOutput,
		reportText,
		listAgentsVisible,
	});
	tools.register();
	// The panel and the commands read a job and message a named agent through the
	// same two functions the tools use.
	const {outputText, sendToRecord} = tools;

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
				: entries.filter(entry => entry.name.toLowerCase() === wanted || entry.sourcePath.toLowerCase().includes(wanted));
		const counts: Partial<Record<AgentScope, number>> = {};
		let valid = 0;
		for (const entry of entries) {
			if (!entry.valid) continue;
			valid += 1;
			counts[entry.scope] = (counts[entry.scope] ?? 0) + 1;
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
			if (entry.scope !== 'builtin' && Object.hasOwn(HOST_RESERVED_AGENT_NAMES, entry.name)) {
				flags.push('shadows a host-reserved name');
			}
			if (entry.warning !== undefined) flags.push(`warning: ${entry.warning}`);
			const state =
				entry.kind === 'duplicate'
					? `duplicate: ${entry.reason ?? 'unknown'}`
					: entry.valid
						? 'valid'
						: `invalid: ${entry.reason ?? 'unknown'}`;
			const location = entry.sourcePath.length > 0 ? `  ${entry.sourcePath}` : '';
			// What the definition resolves to. Tools are shown as authored so a missing
			// tool is visible before a spawn fails on it.
			const settings = [
				entry.definition?.model !== undefined ? `model=${entry.definition.model}` : '',
				entry.definition?.effort !== undefined ? `effort=${entry.definition.effort}` : '',
				entry.definition?.color !== undefined ? `color=${entry.definition.color}` : '',
				entry.definition?.tools !== undefined ? `tools=${entry.definition.tools.join('+')}` : '',
			].filter(Boolean).join(' ');
			return `  ${entry.name.length > 0 ? entry.name : '(unnamed)'}  ${entry.scope}  ${state}${flags.length > 0 ? ` · ${flags.join(' · ')}` : ''}${settings.length > 0 ? ` · ${settings}` : ''}${location}`;
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
			const pluginEntries = entries.filter(entry => entry.scope === 'plugin');
			const problems = pluginEntries.filter(entry => !entry.valid || entry.warning !== undefined);
			const lines = pluginEntries.map(entry => {
				const state =
					entry.kind === 'valid'
						? entry.warning !== undefined
							? `warning: ${entry.warning}`
							: 'valid'
						: `${entry.kind}: ${entry.reason ?? 'unknown'}`;
				return `  ${entry.name.length > 0 ? entry.name : '(unnamed)'}  ${state}  ${entry.sourcePath}`;
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
			.filter(entry => entry.scope === 'plugin' && entry.valid && entry.definition !== undefined)
			.map(entry => entry.name);
		const lines: string[] = [];
		for (const entry of duplicates) {
			lines.push(`  duplicate ${entry.name}: kept ${entry.duplicateOf ?? '?'}, skipped ${entry.sourcePath} - rename or remove one`);
		}
		for (const entry of invalid) {
			lines.push(`  invalid ${entry.sourcePath.length > 0 ? entry.sourcePath : entry.name}: ${entry.reason ?? 'unknown'}`);
		}
		for (const entry of warnings) lines.push(`  warning ${entry.sourcePath}: ${entry.warning}`);
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
		const launched = await fire(
			{
				description: 'subtask',
				kind: 'fork',
				prompt,
				background: true,
				cwd: cmd.cwd,
				agentType: FORK_AGENT_TYPE,
				fork: true,
				mainSessionId: sessionId,
			},
			spawnCtx,
		);
		if (launched.ok === false) return {message: `subtask: ${launched.error}`};
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
	// The host exposes no `/add-dir`, so a directory added mid-session arrives here.
	// It joins the same list the bind-time flag fills and the registry rescans, so
	// its `.commandcode/agents/` loads without a restart.
	cmd.addCommand({
		name: 'add-dir',
		description: 'Load subagent definitions from .commandcode/agents/ under an added directory',
		argumentHint: '<path>',
		handler: ({args}: {args?: string}) => {
			const wanted = String(args ?? '').trim();
			if (wanted.length === 0) return {message: 'add-dir: usage: /add-dir <path>'};
			const dir = isAbsolute(wanted) ? wanted : join(cmd.cwd, wanted);
			if (!addDirs.includes(dir)) addDirs.push(dir);
			registry.refresh();
			const loaded = registry
				.entries()
				.filter(entry => entry.scope === 'project' && isInside(dir, entry.sourcePath));
			return {message: `add-dir: ${loaded.length} definition(s) loaded from ${dir}`};
		},
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
				cmd.ui.notify(`${String(job.description ?? job.id)} continues in the background; /tasks shows it`, 'info');
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
	// The resolved pool, enforced where the tools run. ACP has no per-session tool
	// filter, so this is the only place the definition's `tools` can hold.
	const workerTools = workerMode && Array.isArray(workerSpec?.tools) ? workerSpec.tools : undefined;
	const workerDisallowed =
		workerMode && Array.isArray(workerSpec?.disallowedTools) ? workerSpec.disallowedTools : undefined;
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
							return sessionAgent()?.prompt ?? '';
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
		...(isolation !== undefined || workerHooks?.PreToolUse !== undefined || workerTools !== undefined || workerDisallowed !== undefined
			? {
					beforeToolCall: (call: {toolName?: string; input?: Record<string, unknown>}) => {
						const toolName = String(call?.toolName ?? '');
						const denied =
							workerTools !== undefined
								? !workerToolAllowed(
										toolName,
										workerTools,
										workerDisallowed ?? [],
										workerSpec.teamName !== undefined,
									)
								: (workerDisallowed ?? []).some(specifier => withheldBy(specifier, toolName));
						if (denied) {
							return {
								block: true,
								additionalContext:
									`subagent: "${toolName}" is not in this agent's resolved tool set` +
									(workerTools !== undefined && workerTools.length > 0 ? ` (${workerTools.join(', ')})` : '') +
									'. Use only the listed tools.',
							};
						}
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
			const limit = descriptionLimit(registry.list());
			if (limit.over) {
				try {
					cmd.ui.notify(
						`agent descriptions are about ${limit.tokens} tokens, over the ${limit.maxTokens}-token description limit; shorten them so the model keeps choosing the right agent`,
						'warn',
					);
				} catch {
					/* the warning is advisory; a host without notify still runs */
				}
			}
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
			return {continue: true, reason: pending.map(claim => formatSignalMessage(claim.signal)).join('\n\n')};
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
