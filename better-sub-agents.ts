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
import {join} from 'node:path';

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
const RECORD_SWEEP_MAX = 500;
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

const OWNER_TYPE = 'subagent/owner';
// The 0.1.0 transcript entry. Read once so an installed session keeps its identity
// across the rename; never written again.
const OWNER_TYPE_LEGACY = 'pubsub/owner';
const JOB_ENTRY_TYPE = 'subagent/job';
const WORKER_ENV = 'BETTER_SUBAGENTS_WORKER';
const WORKER_SPEC_ENV = 'SUBAGENT_SPEC';
const RETENTION_ENV = 'BETTER_SUBAGENTS_RETENTION_DAYS';
const SUBAGENT_DIR_DEFAULT = join(homedir(), '.commandcode', 'subagents');
const PUBSUB_DIR_DEFAULT = join(homedir(), '.commandcode', 'pubsub');

// The detached worker. Kept inline so the mod is one file with nothing to install.
// The spec arrives in the environment: no shell interpolation, so a prompt or command
// containing quotes, `$`, or a newline cannot break the wrapper.
const WORKER_SOURCE = `
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

// One constructor for "this job reached this state": the record patch and the
// signal are derived from the same facts, so they cannot disagree. The parent has
// the same contract in signalOf/patchOf; this copy lives here because the worker
// runs in its own process. Sharing it outright needs the worker extracted from
// the string, which is follow-up work.
const settle = (state, detail, code, tail) => {
  const finishedAt = Date.now();
  const durationMs = Math.max(0, finishedAt - spec.startedAt);
  const failed = state === 'failed';
  writeRecord({
    state: state,
    exitCode: typeof code === 'number' ? code : undefined,
    finishedAt: finishedAt,
    durationMs: durationMs,
    failureReason: failed ? detail : undefined,
    awaitingInput: state === 'waiting-input' ? true : undefined
  });
  emit({
    id: spec.id, label: spec.label, kind: spec.kind, owner: spec.owner, state: state,
    exitCode: typeof code === 'number' ? code : null,
    startedAt: spec.startedAt, finishedAt: finishedAt, durationMs: durationMs,
    logPath: spec.logPath, sessionId: spec.sessionId,
    title: spec.label + ' ' + state,
    body: detail + (tail ? '\\n\\n' + tail : '')
  });
};

const fail = (reason, code) => settle('failed', reason, code, '');

// The child's argv. An agent job resumes its own worker session when the parent
// asked for a resume, and pins its session name so a later resume can find it.
const childArgs = () => {
  if (spec.prompt === undefined) return ['-c', spec.command];
  const args = [];
  if (typeof spec.resumeSession === 'string' && spec.resumeSession.length > 0) args.push('-r', spec.resumeSession);
  args.push('-p', spec.prompt);
  if (typeof spec.sessionName === 'string' && spec.sessionName.length > 0) args.push('-n', spec.sessionName);
  if (typeof spec.model === 'string' && spec.model.length > 0) args.push('-m', spec.model);
  if (typeof spec.maxTurns === 'number' && spec.maxTurns > 0) args.push('--max-turns', String(spec.maxTurns));
  return args;
};

let out;
try {
  out = fs.openSync(spec.logPath, 'w');
} catch (e) {
  fail('cannot open log: ' + String(e && e.message || e));
  process.exit(0);
}

const child = spec.prompt !== undefined
  ? spawn(spec.bin, childArgs(), {cwd: spec.cwd, stdio: ['ignore', out, out]})
  : spawn('/bin/sh', ['-c', spec.command], {cwd: spec.cwd, stdio: ['ignore', out, out]});

let settled = false;
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
  if (killTimer) clearTimeout(killTimer);
  if (hardTimer) clearTimeout(hardTimer);
};

const finish = (code, spawnError, signal) => {
  if (settled) return;
  settled = true;
  clearTimers();
  try { fs.closeSync(out); } catch (e) {}
  const record = readRecord();
  // The cancel tool has already written the record and emitted its own signal.
  if (record.state === 'canceled') return;

  const tail = readTail();
  const spy = spec.prompt !== undefined;
  const lastLine = tail.trimEnd().split('\\n').filter(line => line.trim()).pop() || '';
  const failed = timedOut || spawnError !== undefined || (code !== 0 && code !== null);
  const wantsInput = !failed && spy && /\\?\\s*$/.test(lastLine);
  const timeoutLabel = spec.timeoutMs >= 1000 ? Math.round(spec.timeoutMs / 1000) + 's' : spec.timeoutMs + 'ms';
  const detail = timedOut
    ? 'timed out after ' + timeoutLabel
    : spawnError !== undefined
      ? 'spawn error: ' + String(spawnError && spawnError.message || spawnError)
      : code === 8 && spy
        ? 'exit 8 (max turns reached; result is partial)'
        : typeof code === 'number' && code !== 0
          ? 'exit ' + code
          : signal
            ? 'terminated by ' + signal
            : 'exit 0';
  settle(failed ? 'failed' : wantsInput ? 'waiting-input' : 'done', detail, code, tail);
};

if (typeof child.pid === 'number') {
  writeRecord({state: 'running', pid: child.pid, workerPid: process.pid});
}

child.on('exit', (code, signal) => finish(code, undefined, signal));
child.on('error', err => finish(null, err));

process.on('uncaughtException', err => {
  try { fail('worker crash: ' + String(err && err.message || err)); } catch (e) {}
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
}

interface JobRecord extends JobIdentity {
	command?: string;
	prompt?: string;
	cwd?: string;
	agentType?: string;
	model?: string;
	maxTurns?: number;
	timeoutMs?: number;
	sessionName?: string;
	resumedFrom?: string;
	exitCode?: number;
	pid?: number;
	workerPid?: number;
	deliveredAt?: number;
	updatedAt?: number;
	failureReason?: string;
	canceledAt?: number;
	awaitingInput?: boolean;
}

// The record write a signal implies.
interface JobPatch {
	state?: string;
	exitCode?: number;
	finishedAt?: number;
	durationMs?: number;
	failureReason?: string;
	awaitingInput?: boolean;
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
	result: {state: string; finishedAt?: number; durationMs?: number; exitCode?: number | null; title?: string; body?: string},
): Signal {
	const label = String(record.label ?? record.id);
	const state = canonicalState(result.state);
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
		const now = runtime.now();
		let names: string[];
		try {
			names = spool.readdir(claimedDir);
		} catch {
			return;
		}
		for (const name of names) {
			const file = join(claimedDir, name);
			try {
				if (now - spool.stat(file).mtimeMs > CLAIMED_TTL_MS) spool.remove(file);
			} catch {
				/* another drain won it */
			}
		}
	}

	function sweepOrphaned(): void {
		if (workerMode) return;
		const now = runtime.now();
		let names: string[];
		try {
			names = spool.readdir(orphanedDir);
		} catch {
			return;
		}
		for (const name of names) {
			const file = join(orphanedDir, name);
			try {
				if (now - spool.stat(file).mtimeMs > ORPHANED_TTL_MS) spool.remove(file);
			} catch {
				/* another process won it */
			}
		}
	}

	// Retention. A finished job keeps its record and its log for RETENTION_DAYS_DEFAULT
	// (or BETTER_SUBAGENTS_RETENTION_DAYS), so `subagent_output` still works on a job
	// from last week. A running job is never swept, however old its record is.
	function retentionMs(): number {
		const days = Number(runtime.env[RETENTION_ENV]);
		const value = Number.isFinite(days) && days > 0 ? days : RETENTION_DAYS_DEFAULT;
		return value * 24 * 60 * 60 * 1000;
	}

	function sweepExpired(): void {
		if (workerMode) return;
		const cutoff = runtime.now() - retentionMs();
		for (const job of readRecentJobs(RECORD_SWEEP_MAX)) {
			const state = normalizeState(job.state);
			if (state === undefined || !isTerminal(state)) continue;
			const settledAt = Number(job.finishedAt ?? job.updatedAt ?? job.startedAt ?? 0) || 0;
			if (settledAt === 0 || settledAt > cutoff) continue;
			try {
				spool.remove(join(jobsDir, `${job.id}.json`));
			} catch {
				/* another process won it */
			}
			try {
				spool.remove(join(logsDir, `${job.id}.log`));
			} catch {
				/* the log is already gone */
			}
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
			const owner = typeof job.owner === 'string' && job.owner.length > 0 ? ` · ${job.owner.slice(-6)}` : '';
			const unread = isTerminalState(state) && job.deliveredAt === undefined ? ' · unread' : '';
			return `  ${stateMarker(state)} ${job.id}  ${String(job.label ?? '(unlabelled)')}  [${state}${exit}${duration}]${model}${owner}${unread}`;
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
		/** agent jobs only; recorded now, used when the registry lands (phase 6) */
		readonly agentType?: string;
		readonly model?: string;
		readonly maxTurns?: number;
		/** SIGTERM then SIGKILL after this long; 0 or absent means no timeout */
		readonly timeoutMs?: number;
		/** resume this worker session first (`cmd -r <name> -p <prompt>`) */
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

	// The worker session name, deterministic from the job id: a resume can find the
	// worker's own conversation by name without the parent having to parse it out of
	// the worker's output.
	function sessionNameOf(id: string): string {
		return `subagent-${id}`;
	}

	function fire(intent: JobIntent): Launch {
		const id = `${runtime.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
		const logPath = join(logsDir, `${id}.log`);
		const startedAt = runtime.now();
		const owner = ownerId();
		const isAgent = intent.prompt !== undefined;
		const record: JobRecord = {
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
			...(isAgent
				? {
						agentType: intent.agentType ?? 'general-purpose',
						sessionName: sessionNameOf(id),
						...(intent.resumeSession !== undefined ? {resumedFrom: intent.resumedFrom} : {}),
					}
				: {}),
			...(intent.model !== undefined ? {model: intent.model} : {}),
			...(intent.maxTurns !== undefined ? {maxTurns: intent.maxTurns} : {}),
			...(intent.timeoutMs !== undefined && intent.timeoutMs > 0 ? {timeoutMs: intent.timeoutMs} : {}),
			...(intent.prompt !== undefined ? {prompt: intent.prompt} : {command: intent.command}),
		};
		writeJob(record);
		try {
			cmd.session?.appendCustomEntry({
				customType: JOB_ENTRY_TYPE,
				data: {id, label: intent.label, kind: intent.kind, state: 'queued', owner, sessionId},
			});
		} catch {
			/* audit trail is optional */
		}

		const spec = {
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
			bin: runtime.env.COMMANDCODE_BIN ?? 'cmd',
			sessionId,
			sessionName: isAgent ? sessionNameOf(id) : undefined,
			model: intent.model,
			maxTurns: intent.maxTurns,
			timeoutMs: intent.timeoutMs !== undefined && intent.timeoutMs > 0 ? intent.timeoutMs : undefined,
			resumeSession: intent.resumeSession,
			...(intent.prompt !== undefined ? {prompt: intent.prompt} : {command: intent.command}),
		};

		let worker: ChildHandle;
		try {
			worker = runtime.spawn(runtime.execPath, ['-e', WORKER_SOURCE], {
				cwd: intent.cwd,
				detached: true,
				stdio: 'ignore',
				env: {...runtime.env, [WORKER_SPEC_ENV]: JSON.stringify(spec), [WORKER_ENV]: '1'},
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
					nodes.push({
						type: 'text',
						text: `${stateMarker(state)} ${job.id}  ${String(job.label ?? '(unlabelled)').slice(0, 36)}  ${state}${duration}`,
						color: stateColor(state),
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
		for (const unsupported of ['permission_mode', 'isolation', 'tools']) {
			if (input[unsupported] !== undefined) return badInput(`${unsupported} is not implemented in this release; omit it`);
		}
		const model = typeof input.model === 'string' && input.model.trim().length > 0 ? input.model.trim() : undefined;
		const maxTurns = Number(input.max_turns);
		const timeoutMs = Number(input.timeout_ms);
		const agentType =
			typeof input.agent_type === 'string' && input.agent_type.trim().length > 0 ? input.agent_type.trim() : undefined;
		const launched = fire({
			label,
			kind: prompt !== undefined ? 'agent' : 'command',
			command,
			prompt,
			cwd: typeof input.cwd === 'string' && input.cwd.length > 0 ? input.cwd : cmd.cwd,
			agentType,
			model: prompt === undefined ? undefined : model,
			maxTurns: prompt === undefined || !Number.isFinite(maxTurns) || maxTurns <= 0 ? undefined : Math.round(maxTurns),
			timeoutMs: Number.isFinite(timeoutMs) && timeoutMs > 0 ? Math.round(timeoutMs) : undefined,
		});
		if (!launched.ok) return badInput(launched.error);
		const kind = prompt !== undefined ? `agent (${agentType ?? 'general-purpose'})` : 'shell (/bin/sh -c)';
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
		return okText(
			`${stateMarker(record.state)} ${id} "${String(record.label ?? '(unlabelled)')}" · ${facts}\nlog ${file}${timing}\n\n` +
				(body.length > 0 ? body : '(no output yet)') +
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
		// Resume the worker's OWN conversation by its deterministic session name, so the
		// new instruction continues with the context the worker already has.
		const launched = fire({
			label: `${String(record.label ?? id)} (resume)`,
			kind: 'agent',
			prompt: instruction,
			cwd: String(record.cwd ?? cmd.cwd),
			agentType: record.agentType,
			model: record.model,
			maxTurns: record.maxTurns,
			timeoutMs: record.timeoutMs,
			resumeSession: record.sessionName ?? sessionNameOf(id),
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
				agent_type: {type: 'string', description: "Agent type, recorded for the job (default 'general-purpose')."},
				model: {type: 'string', description: 'Model for an agent job, passed to the worker as -m.'},
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
		'List spawned jobs with their lifecycle states, model, elapsed time and owner, and count the signals still waiting for this session.',
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

	// ------------------------------------------------------------------ lifecycle

	cmd.hooks({
		onSessionStart: (info: {source?: string; sessionId?: string}) => {
			if (typeof info?.sessionId === 'string' && info.sessionId.length > 0) {
				sessionId = info.sessionId;
			}
			// Restore (or mint) the owner token before any drain runs.
			ownerId();
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
