// Mod: pubsub — fire-and-forget agents that signal back on their own.
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
// (`appendCustomEntry({customType: 'pubsub/owner'})`) and read back with
// `getCustomEntries`, so it survives resume and hot reload. A drain delivers only
// signals whose owner matches; another session's signal is left in the inbox for its
// owner. Untagged signals from older versions are quarantined to `orphaned/` — never
// delivered to whoever happens to be running.
//
// Polling here is free. The distinction matters: a timer inside the mod process spends
// no turns, whereas `cron_create` costs a full model turn per tick because a tick is a
// prompt handed to the agent.
//
// Layout under PUBSUB_DIR (default ~/.commandcode/pubsub):
//   inbox/       workers drop `<timestamp>-<id>.json` here
//   claimed/     rename-target during delivery, so two drains cannot deliver one signal twice
//   orphaned/    untagged legacy signals, quarantined instead of leaked
//   logs/        one log per fired job
//   jobs/        one JSON record per fired job, for pubsub_list and the jobs pane
//   delivered.jsonl   durable delivery ledger, so a restart cannot replay a signal
//
// Lifecycle (jobs/<id>.json .state):
//   queued → running → waiting-input | done | failed | canceled
//
// A headless worker cannot stay alive waiting for input, so `waiting-input` means
// "the worker exited with a question"; answering it means firing a follow-up job.

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
// A queued/running record with no live pid is only declared lost after this grace,
// so a worker that has been spawned but has not written its pid yet is not failed.
const LOST_GRACE_MS = 30 * 1000;
// Reconcile and list only the newest records; the spool is long-lived.
const RECONCILE_SCAN_MAX = 200;
const LIST_SCAN_MAX = 50;
const DELIVERED_LEDGER_MAX = 5000;
const LEDGER_COMPACT_BYTES = 512 * 1024;
const PANE_ROWS_MAX = 12;
const PENDING_SCAN_MAX = 500;
const OWNER_TYPE = 'pubsub/owner';


// The detached worker. Kept inline so the mod is one file with nothing to install.
// The spec arrives in the environment: no shell interpolation, so a prompt or command
// containing quotes, `$`, or a newline cannot break the wrapper.
const WORKER_SOURCE = `
const {spawn} = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');
const spec = JSON.parse(process.env.PUBSUB_SPEC);
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

let out;
try {
  out = fs.openSync(spec.logPath, 'w');
} catch (e) {
  fail('cannot open log: ' + String(e && e.message || e));
  process.exit(0);
}

const child = spec.prompt !== undefined
  ? spawn(spec.bin, ['-p', spec.prompt], {cwd: spec.cwd, stdio: ['ignore', out, out]})
  : spawn('/bin/sh', ['-c', spec.command], {cwd: spec.cwd, stdio: ['ignore', out, out]});

let settled = false;
const finish = (code, spawnError, signal) => {
  if (settled) return;
  settled = true;
  try { fs.closeSync(out); } catch (e) {}
  const record = readRecord();
  // The cancel tool has already written the record and emitted its own signal.
  if (record.state === 'canceled') return;

  const tail = readTail();
  const failed = spawnError !== undefined || code !== 0;
  const lastLine = tail.trimEnd().split('\\n').filter(line => line.trim()).pop() || '';
  const wantsInput = !failed && spec.prompt !== undefined && /\\?\\s*$/.test(lastLine);
  const detail = spawnError !== undefined
    ? 'spawn error: ' + String(spawnError && spawnError.message || spawnError)
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

// A signal may also carry a note (pubsub_send), which is not a job state.
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

export default function (cmd: ModApi, overrides: RuntimeOverrides = {}): void {
	const runtime = osRuntime(overrides);
	// Below this line the spool is touched only through the injected filesystem.
	const spool = runtime.fs;
	cmd.addFlag('pubsub_dir', {
		type: 'string',
		default: join(homedir(), '.commandcode', 'pubsub'),
		description: 'Where the spool, logs and job records live.',
	});
	cmd.addFlag('pubsub_deliver', {
		type: 'string',
		default: 'steer',
		description: "How a signal reaches the session: 'steer' (after the current tool batch) or 'follow-up' (only when the run would stop).",
	});

	const root = String(cmd.getFlag('pubsub_dir') ?? join(homedir(), '.commandcode', 'pubsub'));
	const inboxDir = join(root, 'inbox');
	const claimedDir = join(root, 'claimed');
	const orphanedDir = join(root, 'orphaned');
	const logsDir = join(root, 'logs');
	const jobsDir = join(root, 'jobs');
	const ledgerPath = join(root, 'delivered.jsonl');
	for (const dir of [root, inboxDir, claimedDir, orphanedDir, logsDir, jobsDir]) {
		spool.mkdir(dir);
	}

	// A headless worker loads this mod too. It has its own owner token and must never
	// steal an unaddressed signal from the session that fired it.
	const workerMode = runtime.env.PUBSUB_WORKER === '1';

	const deliverAs = (): 'steer' | 'follow-up' =>
		cmd.getFlag('pubsub_deliver') === 'follow-up' ? 'follow-up' : 'steer';

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
		try {
			const entries = cmd.session.getCustomEntries({customType: OWNER_TYPE}) as
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

	// A renderer so a signal shows up as a real feed row rather than raw JSON.
	cmd.addRenderer('pubsub', data => {
		const signal = data as Partial<Signal>;
		const state = signal.state === undefined ? '' : ` · ${signal.state}`;
		return [`◈ pubsub · ${String(signal.kind ?? 'signal')}${state} · ${String(signal.title ?? '(untitled)')}`];
	});

	// One writer at a time. A tick that finds this flag set returns immediately rather
	// than racing the previous tick over the same files.
	let draining = false;

	// A signal claimed twice (a lost claimed/ file replayed, or an onStop drain racing
	// a timer tick) must not land twice. Seeded from the durable ledger so a restart
	// cannot replay what a previous process already delivered.
	const deliveredIds = new Set<string>();
	const rememberDelivered = (id: string): void => {
		deliveredIds.add(id);
		try {
			spool.appendFile(ledgerPath, JSON.stringify({id, at: runtime.now()}) + '\n');
		} catch {
			/* the in-memory set still protects this process */
		}
		if (deliveredIds.size > DELIVERED_LEDGER_MAX) deliveredIds.clear();
	};

	try {
		const raw = spool.readFile(ledgerPath);
		for (const line of raw.split('\n').filter(Boolean).slice(-DELIVERED_LEDGER_MAX)) {
			try {
				const entry = JSON.parse(line) as {id?: unknown};
				if (typeof entry.id === 'string') deliveredIds.add(entry.id);
			} catch {
				/* ignore a torn last line */
			}
		}
	} catch {
		/* no ledger yet */
	}
	try {
		if (spool.stat(ledgerPath).size > LEDGER_COMPACT_BYTES) {
			const keep = [...deliveredIds].slice(-2000);
			spool.writeFile(ledgerPath, keep.map(id => JSON.stringify({id, at: runtime.now()})).join('\n') + '\n');
		}
	} catch {
		/* no ledger, or another process won the rewrite */
	}

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
		return `[pubsub] ${title}\n${facts}${log}` + (body.length > 0 ? `\n\n${body}` : '') + `\n\n${next}`;
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
		cmd.showEntry('pubsub', message);
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
			if (deliveredIds.has(signal.id)) {
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
			rememberDelivered(claim.signal.id);
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
			const state = normalizeState(job.state);
			if (state === 'queued' || state === 'running') {
				if (runtime.alive(job.pid) || runtime.alive(job.workerPid)) continue;
				if (hasPendingSignal(job.id)) continue;
				const startedAt = Number(job.startedAt ?? 0) || 0;
				if (now - startedAt < LOST_GRACE_MS) continue;
				applyPatch(
					job.id,
					{
						state: 'failed',
						failureReason: state === 'queued' ? 'worker never started' : 'lost (no live process)',
						finishedAt: now,
						durationMs: Math.max(0, now - startedAt),
					},
					job,
					now,
				);
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

	function listText(): string {
		const jobs = readRecentJobs(LIST_SCAN_MAX);
		const counts: Record<string, number> = {};
		for (const job of jobs) {
			const state = canonicalState(job.state);
			counts[state] = (counts[state] ?? 0) + 1;
		}
		const summary = Object.entries(counts).map(([state, count]) => `${count} ${state}`).join(', ') || 'none';
		const pending = countPending();
		const orphaned = countOrphaned();
		const lines = jobs.slice(0, 20).map(job => {
			const state = canonicalState(job.state);
			const exit = typeof job.exitCode === 'number' ? ` exit ${job.exitCode}` : '';
			const duration = typeof job.durationMs === 'number' ? ` ${(job.durationMs / 1000).toFixed(1)}s` : '';
			const unread = isTerminalState(state) && job.deliveredAt === undefined ? ' · unread' : '';
			return `  ${stateMarker(state)} ${job.id}  ${String(job.label ?? '(unlabelled)')}  [${state}${exit}${duration}]${unread}`;
		});
		const orphanNote = orphaned > 0 ? ` ${orphaned} orphaned signal(s) quarantined.` : '';
		return (
			`${jobs.length} recent job(s): ${summary}. ${pending} signal(s) waiting for this session.${orphanNote}\n` +
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

	function cancelJob(id: string): string {
		const job = readJob(id);
		if (job === undefined) return `pubsub: no job ${id}`;
		const state = normalizeState(job.state);
		if (state !== undefined && isTerminal(state)) return `pubsub: job ${id} is already ${state}`;

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
			return `pubsub: job ${id} is already ${canonicalState(job.state)}`;
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
		return `pubsub: canceled ${id} (${String(job.label ?? 'unlabelled')})`;
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

	function fire(intent: JobIntent): Launch {
		const id = `${runtime.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
		const logPath = join(logsDir, `${id}.log`);
		const startedAt = runtime.now();
		const owner = ownerId();
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
			...(intent.prompt !== undefined ? {prompt: intent.prompt} : {command: intent.command}),
		};
		writeJob(record);
		try {
			cmd.session?.appendCustomEntry({
				customType: 'pubsub/job',
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
			...(intent.prompt !== undefined ? {prompt: intent.prompt} : {command: intent.command}),
		};

		let worker: ChildHandle;
		try {
			worker = runtime.spawn(runtime.execPath, ['-e', WORKER_SOURCE], {
				cwd: intent.cwd,
				detached: true,
				stdio: 'ignore',
				env: {...runtime.env, PUBSUB_SPEC: JSON.stringify(spec), PUBSUB_WORKER: '1'},
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
		title: 'Async jobs',
		render: () => {
			try {
				const nodes: unknown[] = [
					{
						type: 'row',
						children: [
							{type: 'text', text: '◈ pubsub', bold: true},
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
							key: 'pubsub-refresh',
							onPress: () => {
								refreshJobs();
								cmd.ui.refreshWidgets();
							},
						},
						{
							type: 'button',
							label: 'Cancel newest',
							hotkey: 'c',
							key: 'pubsub-cancel',
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
				return ['pubsub: cannot render the jobs pane'] as never;
			}
		},
	});

	// ------------------------------------------------------------------ tools

	// The fire-and-forget tool. Returns the moment the worker is detached; the result
	// arrives later as a signal, never as a tool result.
	cmd.addTool({
		schema: {
			name: 'pubsub_fire',
			description:
				'Start a background worker and return immediately. When it finishes, its result is delivered to this session as a separate message on its own - do not poll for it, and do not wait. Pass `command` for a shell command, or `prompt` for a headless Command Code agent to run that prompt. The result returns later as an agent-to-agent signal, so neither side blocks on the other; it is not this tool result.',
			input_schema: {
				type: 'object',
				properties: {
					label: {type: 'string', description: 'Short name for this job, used in the signal title.'},
					command: {type: 'string', description: 'Shell command to run detached.'},
					prompt: {type: 'string', description: 'Prompt for a detached headless Command Code run.'},
					cwd: {type: 'string', description: 'Working directory. Defaults to the mod workspace root.'},
				},
				required: ['label'],
			},
		},
		run: async ({input}) => {
			const label = typeof input.label === 'string' ? input.label.trim() : '';
			if (label.length === 0) return {ok: false, error: 'label is required and must not be empty'};
			const command = typeof input.command === 'string' && input.command.trim().length > 0 ? input.command : undefined;
			const prompt = typeof input.prompt === 'string' && input.prompt.trim().length > 0 ? input.prompt : undefined;
			if (command === undefined && prompt === undefined) {
				return {ok: false, error: 'pass either command or prompt'};
			}
			if (command !== undefined && prompt !== undefined) {
				return {ok: false, error: 'pass command or prompt, not both'};
			}

			const launched = fire({
				label,
				kind: prompt !== undefined ? 'agent' : 'command',
				command,
				prompt,
				cwd: typeof input.cwd === 'string' && input.cwd.length > 0 ? input.cwd : cmd.cwd,
			});
			if (!launched.ok) return {ok: false, error: launched.error};
			return {
				ok: true,
				content: [
					{
						type: 'text',
						text:
							`fired ${launched.id} (${label}), pid ${launched.pid}.\n` +
							`It will report back here when it exits. Do not wait for it; carry on.`,
					},
				],
			};
		},
	});

	// Publish a signal directly, from the model or from any script that can write a file.
	cmd.addTool({
		schema: {
			name: 'pubsub_send',
			description:
				'Publish a signal into the agent loop of THIS session. This is the AGENT-TO-AGENT channel: worker and peer agents signal a session so neither side blocks on the other or polls for a result. It is delivered as a visible user turn in the session (echoed in the TUI), and it is durable if no session is bound yet. NEVER use it to notify the human - to reach the human, reply in the session.',
			input_schema: {
				type: 'object',
				properties: {
					title: {type: 'string', description: 'One line, shown in the signal header.'},
					body: {type: 'string', description: 'The detail.'},
				},
				required: ['title'],
			},
		},
		run: async ({input}) => {
			const title = typeof input.title === 'string' ? input.title : '';
			if (title.length === 0) return {ok: false, error: 'title is required'};
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
			return {
				ok: true,
				content: [{type: 'text', text: deliveredIds.has(id) ? 'delivered' : 'queued for this session'}],
			};
		},
	});

	cmd.addTool({
		schema: {
			name: 'pubsub_list',
			description: 'List fired jobs, their lifecycle states, and count signals still waiting for this session.',
			input_schema: {type: 'object', properties: {}, required: []},
		},
		readOnly: true,
		run: async () => {
			refreshJobs();
			return {ok: true, content: [{type: 'text', text: listText()}]};
		},
	});

	cmd.addTool({
		schema: {
			name: 'pubsub_cancel',
			description: 'Cancel a queued or running pubsub job by id. The firing session receives a canceled signal.',
			input_schema: {
				type: 'object',
				properties: {id: {type: 'string', description: 'Job id from pubsub_list or pubsub_fire.'}},
				required: ['id'],
			},
		},
		run: async ({input}) => {
			const id = typeof input.id === 'string' ? input.id.trim() : '';
			if (id.length === 0) return {ok: false, error: 'id is required'};
			const message = cancelJob(id);
			return message.startsWith('pubsub: no job') || message.includes('already')
				? {ok: false, error: message}
				: {ok: true, content: [{type: 'text', text: message}]};
		},
	});

	// ------------------------------------------------------------------ command

	cmd.addCommand({
		name: 'pubsub',
		description: 'Async jobs: list, cancel <id>, or open the dashboard',
		argumentHint: 'list|cancel <id>|jobs',
		handler: ({args}) => {
			const [sub, ...rest] = String(args ?? '').trim().split(/\s+/).filter(Boolean);
			if (sub === 'list') return {message: listText()};
			if (sub === 'cancel' && rest[0] !== undefined) return {message: cancelJob(rest[0])};
			try {
				cmd.ui.openPane('jobs');
			} catch {
				/* headless */
			}
			return {message: 'pubsub: opened the async jobs pane (list | cancel <id>)'};
		},
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
				cmd.showEntry('pubsub', claim.signal);
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

	// Seed the dashboard and reconcile anything a previous process left behind.
	ownerId();
	reconcileJobs();
	refreshJobs();
}
