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
const OWNER_TYPE = 'pubsub/owner';

const TERMINAL_STATES = new Set(['waiting-input', 'done', 'failed', 'canceled']);

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

const fail = (reason, code) => {
  const now = Date.now();
  const durationMs = Math.max(0, now - spec.startedAt);
  writeRecord({state: 'failed', exitCode: typeof code === 'number' ? code : undefined, finishedAt: now, durationMs, failureReason: reason});
  emit({
    id: spec.id, label: spec.label, kind: spec.kind, owner: spec.owner, state: 'failed',
    exitCode: typeof code === 'number' ? code : null,
    startedAt: spec.startedAt, finishedAt: now, durationMs, logPath: spec.logPath,
    sessionId: spec.sessionId, title: spec.label + ' failed',
    body: reason
  });
};

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

  let tail = '';
  try { tail = fs.readFileSync(spec.logPath, 'utf8').slice(-spec.tailMax); } catch (e) {}
  const finishedAt = Date.now();
  const failed = spawnError !== undefined || code !== 0;
  const lastLine = tail.trimEnd().split('\\n').filter(line => line.trim()).pop() || '';
  const wantsInput = !failed && spec.prompt !== undefined && /\\?\\s*$/.test(lastLine);
  const state = failed ? 'failed' : wantsInput ? 'waiting-input' : 'done';
  const durationMs = Math.max(0, finishedAt - spec.startedAt);
  const detail = spawnError !== undefined
    ? 'spawn error: ' + String(spawnError && spawnError.message || spawnError)
    : typeof code === 'number' && code !== 0
      ? 'exit ' + code
      : signal
        ? 'terminated by ' + signal
        : 'exit 0';
  const failureReason = failed ? detail : undefined;

  writeRecord({
    state: state,
    exitCode: typeof code === 'number' ? code : undefined,
    finishedAt: finishedAt,
    durationMs: durationMs,
    failureReason: failureReason,
    awaitingInput: wantsInput || undefined
  });
  emit({
    id: spec.id,
    label: spec.label,
    kind: spec.kind,
    owner: spec.owner,
    state: state,
    exitCode: typeof code === 'number' ? code : null,
    startedAt: spec.startedAt,
    finishedAt: finishedAt,
    durationMs: durationMs,
    logPath: spec.logPath,
    sessionId: spec.sessionId,
    title: spec.label + ' ' + state,
    body: detail + (tail ? '\\n\\n' + tail : '')
  });
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

interface Signal {
	readonly id: string;
	readonly owner?: string;
	readonly label?: string;
	readonly kind?: string;
	readonly state?: string;
	readonly exitCode?: number | null;
	readonly startedAt?: number;
	readonly finishedAt?: number;
	readonly durationMs?: number;
	readonly title?: string;
	readonly body?: string;
	readonly logPath?: string;
	readonly sessionId?: string;
}

interface JobRecord {
	id: string;
	owner?: string;
	label?: string;
	kind?: string;
	state?: string;
	command?: string;
	prompt?: string;
	cwd?: string;
	logPath?: string;
	startedAt?: number;
	finishedAt?: number;
	durationMs?: number;
	exitCode?: number;
	pid?: number;
	workerPid?: number;
	sessionId?: string;
	deliveredAt?: number;
	updatedAt?: number;
	failureReason?: string;
	canceledAt?: number;
	awaitingInput?: boolean;
}

export default function (cmd: ModApi): void {
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
		mkdirSync(dir, {recursive: true});
	}

	// A headless worker loads this mod too. It has its own owner token and must never
	// steal an unaddressed signal from the session that fired it.
	const workerMode = process.env.PUBSUB_WORKER === '1';

	const deliverAs = (): 'steer' | 'follow-up' =>
		cmd.getFlag('pubsub_deliver') === 'follow-up' ? 'follow-up' : 'steer';

	// Bound once the harness binds. Diagnostic only; routing is by owner token.
	let sessionId: string | undefined;

	// Per-session ownership. Restored from the transcript so resume and hot reload keep
	// the same token; generated and appended once when the session has no token yet.
	let ownerToken: string | undefined;
	function ensureOwner(): string | undefined {
		if (ownerToken !== undefined) return ownerToken;
		const session = cmd.session;
		if (session === undefined) return undefined;
		try {
			const entries = session.getCustomEntries({customType: OWNER_TYPE}) as
				| {data?: {token?: unknown}}[]
				| undefined;
			for (let index = (entries?.length ?? 0) - 1; index >= 0; index -= 1) {
				const token = entries?.[index]?.data?.token;
				if (typeof token === 'string' && token.length > 0) {
					ownerToken = token;
					return ownerToken;
				}
			}
		} catch {
			/* fall through and mint one */
		}
		const token = `owner-${randomUUID()}`;
		try {
			session.appendCustomEntry({customType: OWNER_TYPE, data: {token}});
			ownerToken = token;
			return ownerToken;
		} catch {
			return undefined;
		}
	}
	function requireOwner(): string {
		return ensureOwner() ?? `process-${process.pid}`;
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
			appendFileSync(ledgerPath, JSON.stringify({id, at: Date.now()}) + '\n');
		} catch {
			/* the in-memory set still protects this process */
		}
		if (deliveredIds.size > DELIVERED_LEDGER_MAX) deliveredIds.clear();
	};

	try {
		const raw = readFileSync(ledgerPath, 'utf8');
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
		if (statSync(ledgerPath).size > LEDGER_COMPACT_BYTES) {
			const keep = [...deliveredIds].slice(-2000);
			writeFileSync(ledgerPath, keep.map(id => JSON.stringify({id, at: Date.now()})).join('\n') + '\n');
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

	function canonicalState(value: unknown): string {
		const state = String(value ?? 'unknown');
		if (state === 'completed' || state === 'agent-done' || state === 'command-done') return 'done';
		return state;
	}

	function readJob(id: string): JobRecord | undefined {
		try {
			const parsed = JSON.parse(readFileSync(join(jobsDir, `${id}.json`), 'utf8')) as JobRecord;
			return parsed && typeof parsed.id === 'string' ? parsed : undefined;
		} catch {
			return undefined;
		}
	}

	function writeJob(record: JobRecord): void {
		try {
			const file = join(jobsDir, `${record.id}.json`);
			const tmp = `${file}.mod-${process.pid}-${Date.now()}`;
			writeFileSync(tmp, JSON.stringify(record, null, 2));
			renameSync(tmp, file);
		} catch {
			/* the in-memory list still shows it */
		}
	}

	function readRecentJobs(limit: number): JobRecord[] {
		let names: string[];
		try {
			names = readdirSync(jobsDir).filter(name => name.endsWith('.json') && !name.includes('.tmp-'));
		} catch {
			return [];
		}
		names.sort();
		const jobs: JobRecord[] = [];
		for (const name of names.slice(-limit).reverse()) {
			try {
				jobs.push(JSON.parse(readFileSync(join(jobsDir, name), 'utf8')) as JobRecord);
			} catch {
				/* skip a torn record */
			}
		}
		return jobs;
	}

	function enqueueSignal(message: Signal): void {
		try {
			const stamp = `${Date.now()}-${message.id}-${process.pid}-${Math.random().toString(36).slice(2, 6)}.json`;
			const tmp = join(inboxDir, `.tmp-${stamp}`);
			writeFileSync(tmp, JSON.stringify(message, null, 2));
			renameSync(tmp, join(inboxDir, stamp));
		} catch {
			/* a durable signal is best effort; the job record still has the result */
		}
	}

	// mine: deliver. foreign: leave it for another session. orphan: untagged legacy
	// data, quarantine it. pending: ownership is not known yet (pre-bind), wait.
	function classify(message: Signal): 'mine' | 'foreign' | 'orphan' | 'pending' {
		if (ownerToken === undefined) return 'pending';
		if (typeof message.owner === 'string' && message.owner.length > 0) {
			return message.owner === ownerToken ? 'mine' : 'foreign';
		}
		return workerMode ? 'foreign' : 'orphan';
	}

	function quarantine(file: string, name: string): void {
		try {
			renameSync(file, join(orphanedDir, `${Date.now()}-${Math.random().toString(36).slice(2, 6)}-${name}`));
		} catch {
			rmSync(file, {force: true});
		}
	}

	function markDelivered(id: string): void {
		const job = readJob(id);
		if (job === undefined) return;
		job.deliveredAt = Date.now();
		job.updatedAt = job.deliveredAt;
		writeJob(job);
	}

	function deliver(message: Signal): void {
		// No bound session means sendUserMessage would degrade to a silent queue push
		// (headless post-run) and we would delete the only durable copy. Throw so the
		// drain puts the signal back in inbox/ for the next bound session.
		if (cmd.session === undefined) throw new Error('no session bound');
		// `queueMessage` only steers a run that is already going - it never STARTS one,
		// so an idle session would leave the result unread until someone asked for it.
		// `sendUserMessage` starts the turn when idle and queues behind the active turn
		// otherwise. This is the A2A delivery path.
		cmd.sendUserMessage(formatSignal(message), {deliverAs: deliverAs()});
		cmd.showEntry('pubsub', message);
		rememberDelivered(message.id);
		markDelivered(message.id);
	}

	function drain(): void {
		if (draining) return;
		draining = true;
		try {
			const names = readdirSync(inboxDir).filter(name => name.endsWith('.json'));
			for (const name of names.slice(0, FILES_PER_DRAIN_MAX)) {
				const live = join(inboxDir, name);
				const claimed = join(claimedDir, name);
				// Claim by rename first: rename is atomic, so exactly one drainer wins a
				// given file even if two sessions are running against one spool.
				try {
					renameSync(live, claimed);
				} catch {
					continue;
				}
				let message: Signal;
				try {
					message = JSON.parse(readFileSync(claimed, 'utf8')) as Signal;
				} catch {
					rmSync(claimed, {force: true});
					continue;
				}
				if (typeof message.id !== 'string' || message.id.length === 0) {
					rmSync(claimed, {force: true});
					continue;
				}
				if (deliveredIds.has(message.id)) {
					rmSync(claimed, {force: true});
					continue;
				}
				const owner = classify(message);
				if (owner === 'orphan') {
					quarantine(claimed, name);
					continue;
				}
				if (owner !== 'mine') {
					// Another session's result, or ownership not known yet. Put it back.
					try {
						renameSync(claimed, live);
					} catch {
						/* it stays claimable and will be swept later if abandoned */
					}
					continue;
				}
				try {
					deliver(message);
					rmSync(claimed, {force: true});
				} catch {
					// No session bound yet. Put it back so the next tick can deliver it
					// instead of dropping the signal on the floor.
					try {
						renameSync(claimed, live);
					} catch {
						/* keep it in claimed/; a later run can still drain that */
					}
				}
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

	// Claim and remove up to `limit` signals without delivering them. The caller decides
	// how they reach the model (onStop continues the run with them as the reason).
	function claimPending(limit: number): Signal[] {
		const out: Signal[] = [];
		let names: string[];
		try {
			names = readdirSync(inboxDir).filter(name => name.endsWith('.json'));
		} catch {
			return out;
		}
		for (const name of names.slice(0, limit)) {
			const live = join(inboxDir, name);
			const claimed = join(claimedDir, name);
			try {
				renameSync(live, claimed);
			} catch {
				continue;
			}
			let message: Signal | undefined;
			try {
				message = JSON.parse(readFileSync(claimed, 'utf8')) as Signal;
			} catch {
				rmSync(claimed, {force: true});
				continue;
			}
			if (typeof message.id !== 'string' || message.id.length === 0) {
				rmSync(claimed, {force: true});
				continue;
			}
			if (deliveredIds.has(message.id)) {
				rmSync(claimed, {force: true});
				continue;
			}
			const owner = classify(message);
			if (owner === 'orphan') {
				quarantine(claimed, name);
				continue;
			}
			if (owner !== 'mine') {
				try {
					renameSync(claimed, live);
				} catch {
					rmSync(claimed, {force: true});
				}
				continue;
			}
			rememberDelivered(message.id);
			markDelivered(message.id);
			out.push(message);
			rmSync(claimed, {force: true});
		}
		return out;
	}

	// claimed/ is a rename target, not an archive. Sweep files abandoned by a drain
	// that died between claim and remove so a long-lived spool cannot grow forever.
	function sweepClaimed(): void {
		const now = Date.now();
		let names: string[];
		try {
			names = readdirSync(claimedDir);
		} catch {
			return;
		}
		for (const name of names) {
			const file = join(claimedDir, name);
			try {
				if (now - statSync(file).mtimeMs > CLAIMED_TTL_MS) rmSync(file, {force: true});
			} catch {
				/* another drain won it */
			}
		}
	}

	function sweepOrphaned(): void {
		if (workerMode) return;
		const now = Date.now();
		let names: string[];
		try {
			names = readdirSync(orphanedDir);
		} catch {
			return;
		}
		for (const name of names) {
			const file = join(orphanedDir, name);
			try {
				if (now - statSync(file).mtimeMs > ORPHANED_TTL_MS) rmSync(file, {force: true});
			} catch {
				/* another process won it */
			}
		}
	}

	function pidAlive(pid: unknown): boolean {
		if (typeof pid !== 'number' || !Number.isInteger(pid) || pid <= 0) return false;
		try {
			process.kill(pid, 0);
			return true;
		} catch (error) {
			return (error as {code?: string}).code === 'EPERM';
		}
	}

	function hasPendingSignal(id: string): boolean {
		try {
			return (
				readdirSync(inboxDir).some(name => name.includes(id)) ||
				readdirSync(claimedDir).some(name => name.includes(id))
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
		const now = Date.now();
		for (const job of readRecentJobs(RECONCILE_SCAN_MAX)) {
			const state = canonicalState(job.state);
			if (state === 'queued' || state === 'running') {
				if (pidAlive(job.pid) || pidAlive(job.workerPid)) continue;
				if (hasPendingSignal(job.id)) continue;
				const startedAt = Number(job.startedAt ?? 0) || 0;
				if (now - startedAt < LOST_GRACE_MS) continue;
				job.state = 'failed';
				job.failureReason = state === 'queued' ? 'worker never started' : 'lost (no live process)';
				job.finishedAt = now;
				job.durationMs = Math.max(0, now - startedAt);
				job.updatedAt = now;
				writeJob(job);
			} else if (state !== job.state) {
				job.state = state;
				job.updatedAt = now;
				writeJob(job);
			}
		}
	}

	let jobCache: JobRecord[] = [];
	let lastJobsSignature = '';
	let lastStatus = '';

	function countPending(): number {
		let names: string[];
		try {
			names = readdirSync(inboxDir).filter(name => name.endsWith('.json'));
		} catch {
			return 0;
		}
		let count = 0;
		for (const name of names.slice(0, 500)) {
			try {
				const message = JSON.parse(readFileSync(join(inboxDir, name), 'utf8')) as Signal;
				if (typeof message.id === 'string' && !deliveredIds.has(message.id) && classify(message) === 'mine') count++;
			} catch {
				/* torn file */
			}
		}
		return count;
	}

	function countOrphaned(): number {
		try {
			return readdirSync(orphanedDir).length;
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
				cmd.state.set('jobsTick', Date.now());
			} catch {
				/* pre-bind or a host without state */
			}
		}
	}

	function stateMarker(state: string): string {
		switch (state) {
			case 'queued':
				return '◌';
			case 'running':
				return '●';
			case 'waiting-input':
				return '?';
			case 'done':
				return '✓';
			case 'failed':
				return '✗';
			case 'canceled':
				return '⊘';
			default:
				return '·';
		}
	}

	function stateColor(state: string): string {
		switch (state) {
			case 'running':
				return 'cyan';
			case 'waiting-input':
				return 'yellow';
			case 'done':
				return 'green';
			case 'failed':
				return 'red';
			default:
				return 'gray';
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
			const unread = TERMINAL_STATES.has(state) && job.deliveredAt === undefined ? ' · unread' : '';
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
			process.kill(pid, signal as never);
		} catch {
			/* already gone */
		}
	}

	function cancelJob(id: string): string {
		const job = readJob(id);
		if (job === undefined) return `pubsub: no job ${id}`;
		const state = canonicalState(job.state);
		if (TERMINAL_STATES.has(state)) return `pubsub: job ${id} is already ${state}`;

		const now = Date.now();
		const startedAt = Number(job.startedAt ?? now) || now;
		job.state = 'canceled';
		job.canceledAt = now;
		job.finishedAt = now;
		job.durationMs = Math.max(0, now - startedAt);
		job.failureReason = 'canceled by request';
		job.updatedAt = now;
		writeJob(job);

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

		enqueueSignal({
			id: job.id,
			owner: typeof job.owner === 'string' && job.owner.length > 0 ? job.owner : requireOwner(),
			label: String(job.label ?? job.id),
			kind: String(job.kind ?? 'command'),
			state: 'canceled',
			exitCode: null,
			startedAt,
			finishedAt: now,
			durationMs: job.durationMs,
			logPath: typeof job.logPath === 'string' ? job.logPath : undefined,
			sessionId: typeof job.sessionId === 'string' ? job.sessionId : undefined,
			title: `${String(job.label ?? job.id)} canceled`,
			body: 'canceled by request',
		});
		safeDrain();
		refreshJobs();
		return `pubsub: canceled ${id} (${String(job.label ?? 'unlabelled')})`;
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

			const owner = requireOwner();
			const id = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
			const cwd = typeof input.cwd === 'string' && input.cwd.length > 0 ? input.cwd : cmd.cwd;
			const logPath = join(logsDir, `${id}.log`);
			const startedAt = Date.now();
			const kind = prompt !== undefined ? 'agent' : 'command';
			const spec = {
				id,
				label,
				kind,
				owner,
				cwd,
				logPath,
				inboxDir,
				jobsDir,
				startedAt,
				tailMax: LOG_TAIL_BYTES_MAX,
				bin: process.env.COMMANDCODE_BIN ?? 'cmd',
				sessionId,
				...(prompt !== undefined ? {prompt} : {command}),
			};
			const record: JobRecord = {
				id,
				owner,
				label,
				kind,
				state: 'queued',
				cwd,
				logPath,
				startedAt,
				sessionId,
				updatedAt: startedAt,
				...(prompt !== undefined ? {prompt} : {command}),
			};
			writeJob(record);
			try {
				cmd.session?.appendCustomEntry({customType: 'pubsub/job', data: {id, label, kind, state: 'queued', owner, sessionId}});
			} catch {
				/* audit trail is optional */
			}

			let worker;
			try {
				worker = spawn(process.execPath, ['-e', WORKER_SOURCE], {
					cwd,
					detached: true,
					stdio: 'ignore',
					env: {...process.env, PUBSUB_SPEC: JSON.stringify(spec), PUBSUB_WORKER: '1'},
				});
			} catch (error) {
				const now = Date.now();
				record.state = 'failed';
				record.failureReason = `spawn error: ${String((error as Error)?.message ?? error)}`;
				record.finishedAt = now;
				record.updatedAt = now;
				record.durationMs = now - startedAt;
				writeJob(record);
				enqueueSignal({
					id, owner, label, kind, state: 'failed', exitCode: null, startedAt,
					finishedAt: now, durationMs: record.durationMs, logPath,
					sessionId, title: `${label} failed`, body: record.failureReason,
				});
				refreshJobs();
				return {ok: false, error: record.failureReason};
			}

			// A spawn failure is an async 'error' event; without a listener it is an
			// uncaught exception in the host. Record it and emit a signal so the job
			// does not sit at `queued` forever.
			worker.on('error', error => {
				const current = readJob(id);
				if (current !== undefined && canonicalState(current.state) === 'canceled') return;
				const now = Date.now();
				writeJob({
					...(current ?? record),
					id,
					owner,
					label,
					kind,
					state: 'failed',
					failureReason: `worker spawn error: ${String((error as Error)?.message ?? error)}`,
					finishedAt: now,
					durationMs: Math.max(0, now - startedAt),
					updatedAt: now,
				});
				enqueueSignal({
					id, owner, label, kind, state: 'failed', exitCode: null, startedAt, finishedAt: now,
					durationMs: Math.max(0, now - startedAt), logPath, sessionId,
					title: `${label} failed`, body: `worker spawn error: ${String((error as Error)?.message ?? error)}`,
				});
				refreshJobs();
			});
			worker.unref();

			if (typeof worker.pid !== 'number') {
				const now = Date.now();
				record.state = 'failed';
				record.failureReason = 'the worker process did not start';
				record.finishedAt = now;
				record.durationMs = now - startedAt;
				record.updatedAt = now;
				writeJob(record);
				enqueueSignal({
					id, owner, label, kind, state: 'failed', exitCode: null, startedAt, finishedAt: now,
					durationMs: record.durationMs, logPath, sessionId,
					title: `${label} failed`, body: record.failureReason,
				});
				refreshJobs();
				return {ok: false, error: record.failureReason};
			}

			// Merge the pid onto the freshest record: the worker may already have written
			// `running`, and a blind overwrite would drag it back to `queued`.
			const latest = readJob(id) ?? record;
			latest.workerPid = worker.pid;
			latest.updatedAt = Date.now();
			writeJob(latest);
			refreshJobs();
			return {
				ok: true,
				content: [
					{
						type: 'text',
						text:
							`fired ${id} (${label}), pid ${worker.pid}.\n` +
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
			const owner = requireOwner();
			const id = `direct-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
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
			ensureOwner();
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
			for (const message of pending) cmd.showEntry('pubsub', message);
			refreshJobs();
			return {continue: true, reason: pending.map(formatSignal).join('\n\n')};
		},
	});

	// Belt and suspenders: `run_start` also carries the session id.
	cmd.on('run_start', (event: {sessionId?: string}) => {
		if (typeof event?.sessionId === 'string' && event.sessionId.length > 0) {
			sessionId = event.sessionId;
			ensureOwner();
			safeDrain();
		}
	});

	cmd.clock.every(DRAIN_INTERVAL_MS, safeDrain);
	cmd.clock.every(REFRESH_INTERVAL_MS, refreshJobs);

	// Seed the dashboard and reconcile anything a previous process left behind.
	ensureOwner();
	reconcileJobs();
	refreshJobs();
}
