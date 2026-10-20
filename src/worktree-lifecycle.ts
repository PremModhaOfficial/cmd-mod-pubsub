// ---------------------------------------------------------------------------
// Worktree lifecycle and retention
//
// One place decides what happens to a job's checkout. `finalizeJobWorktree` is
// the single keep/remove decision for a finished job: it asks git once, writes
// the record's `worktreeFinalized` / `worktreeKept` flags, so a later delivery
// reports the same answer instead of re-deciding. `sweepExpired` is retention:
// a finished record past the TTL takes its log, transcript and - only when git
// says it is clean - its worktree with it. A live job and a changed worktree are
// never swept.
//
// The module is a leaf. The spool, the git seam, the clock, the liveness probe
// and the record store arrive in a WorktreeLifecycleContext; nothing here names
// the session, the host UI or the entry module.
// ---------------------------------------------------------------------------

import {dirname, join} from 'node:path';
import {
	CLEANUP_PERIOD_SETTING,
	RETENTION_DAYS_DEFAULT,
	RETENTION_ENV,
	RETENTION_ENV_ALIAS,
} from './constants.ts';
import {isTerminal, normalizeState} from './state.ts';
import {finalizeWorktree, mainCheckoutRoots, worktreeChanged} from './worktree.ts';
import type {GitSeam, WorktreeDecision} from './worktree.ts';
import type {JobRecord} from './jobs.ts';
import type {SpoolFs} from './types.ts';

// ---------------------------------------------------------------------------
// The shared TTL sweeper
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// The lifecycle
// ---------------------------------------------------------------------------

/** What the lifecycle reads and writes: the spool, git, the clock, liveness, the record store. */
export interface WorktreeLifecycleContext {
	/** The spool filesystem every record, log, transcript and worktree is reached through. */
	readonly fs: SpoolFs;
	/** Git is already a seam: the lifecycle never spawns a process itself. */
	readonly git: GitSeam;
	/** The clock, as a value so a test can pin it. */
	readonly now: () => number;
	/** Whether a job's worker pid is still running. */
	readonly alive: (pid: unknown) => boolean;
	readonly env: Record<string, string | undefined>;
	/** Where retention settings are read from: project first, then home. */
	readonly cwd: string;
	readonly home: string;
	/** The spool directories retention owns. */
	readonly jobsDir: string;
	readonly logsDir: string;
	readonly transcriptsDir: string;
	readonly worktreesDir: string;
	/** Where a live worker publishes `state.json` (alive/busy). */
	readonly controlRoot: string;
	/** A worker never sweeps: retention belongs to the owning session. */
	readonly workerMode: boolean;
	readonly readJob: (id: string) => JobRecord | undefined;
	readonly writeJob: (job: JobRecord) => void;
}

export interface WorktreeLifecycle {
	/** The one cleanup decision per finished job; idempotent through the record. */
	finalizeJobWorktree(job: JobRecord | undefined): WorktreeDecision | undefined;
	/** Retention: finished records, their artifacts, and clean worktrees. */
	sweepExpired(): void;
	/** `alive` decides queue-vs-resume; `busy` decides the wording of a queue. */
	liveState(record: JobRecord): {alive: boolean; busy: boolean};
}

export function createWorktreeLifecycle(ctx: WorktreeLifecycleContext): WorktreeLifecycle {
	const spool = ctx.fs;
	const git = ctx.git;
	const now = ctx.now;

	// Retention. A finished job keeps its record, its log and its transcript for
	// RETENTION_DAYS_DEFAULT (or BETTER_SUBAGENTS_RETENTION_DAYS, its
	// CLAUDE_CODE_CLEANUP_PERIOD_DAYS alias, or `cleanupPeriodDays` in settings), so
	// `subagent_output` still works on a job from last week. A running job is never
	// swept, however old its record is. The three artifacts are swept together.
	function cleanupPeriodDays(): number | undefined {
		// Project settings win over user settings, the same precedence the definition
		// registry uses. A missing or malformed file contributes nothing.
		for (const base of [ctx.cwd, ctx.home]) {
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
		const raw = ctx.env[RETENTION_ENV] ?? ctx.env[RETENTION_ENV_ALIAS];
		const days = Number(raw);
		const configured = Number.isFinite(days) && days > 0 ? days : cleanupPeriodDays();
		const value =
			configured !== undefined && Number.isFinite(configured) && configured > 0
				? configured
				: RETENTION_DAYS_DEFAULT;
		return value * 24 * 60 * 60 * 1000;
	}

	// A worktree is removable only when git says it is clean. A changed (or
	// unknowable) one is the user's work, so retention keeps it and its git
	// metadata. Removal goes through `git worktree remove`, which also clears that
	// metadata; a bare directory delete would leave a stale `git worktree list` row.
	function discardCleanWorktree(job: JobRecord | undefined, path: string): void {
		if (job?.worktreeKept === true) return;
		if (worktreeChanged(git, path) !== false) return;
		const roots = mainCheckoutRoots(git, path);
		const repo = job?.worktreeRepo ?? roots[1] ?? roots[0] ?? dirname(path);
		git.git(['worktree', 'remove', '--force', path], repo);
	}

	// One cleanup decision per finished job, made wherever the terminal state is first
	// observed - a delivered signal or a restart reconcile. The record write makes it
	// idempotent, so a kept worktree is not reported twice.
	function finalizeJobWorktree(job: JobRecord | undefined): WorktreeDecision | undefined {
		if (job === undefined) return undefined;
		const state = normalizeState(job.state);
		if (state === undefined || !isTerminal(state)) return undefined;
		if (typeof job.worktree !== 'string' || job.worktree.length === 0) return undefined;
		// Already decided by the restart reconcile or an earlier format: report the kept
		// path again without touching git, so delivery never loses the note.
		if (job.worktreeFinalized === true) return {kept: job.worktreeKept === true, path: job.worktree};
		// The record's `live` flag outlives the worker. Pass its actual liveness: a
		// follow-up needs the checkout, but after exit a clean one is removed and a
		// changed one is kept.
		const live = job.live === true && liveState(job).alive;
		const decision = finalizeWorktree(git, {...job, live});
		if (decision === undefined) return undefined;
		job.worktreeFinalized = true;
		job.worktreeKept = decision.kept;
		job.updatedAt = now();
		ctx.writeJob(job);
		return decision;
	}

	// `alive` decides queue-vs-resume; `busy` decides the wording of a queue. No
	// state file means the worker has not published yet, so a live worker is assumed
	// to be in a turn.
	function liveState(record: JobRecord): {alive: boolean; busy: boolean} {
		const alive = ctx.alive(record.workerPid) || ctx.alive(record.pid);
		let busy = true;
		try {
			const state = JSON.parse(spool.readFile(join(ctx.controlRoot, record.id, 'state.json'))) as {busy?: unknown};
			busy = state.busy !== false;
		} catch {
			/* no state file yet */
		}
		return {alive, busy};
	}

	function sweepExpired(): void {
		if (ctx.workerMode) return;
		const stamp = now();
		const ttlMs = retentionMs();
		const cutoff = stamp - ttlMs;
		const expired: JobRecord[] = [];
		// A record's age lives in its timestamps, not its mtime: one sweeper, but the
		// predicate decides. A running job is never expired.
		sweepExpiredEntries(spool, ctx.jobsDir, stamp, {
			expired: name => {
				if (!name.endsWith('.json') || name.includes('.tmp-')) return false;
				try {
					const job = JSON.parse(spool.readFile(join(ctx.jobsDir, name))) as JobRecord;
					const state = normalizeState(job.state);
					if (state === undefined || !isTerminal(state)) return false;
					const settledAt = Number(job.finishedAt ?? job.updatedAt ?? job.startedAt ?? 0) || 0;
					if (settledAt === 0 || settledAt > cutoff) return false;
					if (typeof job.id === 'string' && job.id.length > 0) expired.push(job);
					return true;
				} catch {
					return false;
				}
			},
		});
		for (const job of expired) {
			const id = job.id;
			try {
				spool.remove(join(ctx.logsDir, `${id}.log`));
			} catch {
				/* the log is already gone */
			}
			try {
				spool.remove(
					typeof job.transcriptPath === 'string' && job.transcriptPath.length > 0
						? job.transcriptPath
						: join(ctx.transcriptsDir, `${id}.jsonl`),
				);
			} catch {
				/* the transcript is already gone */
			}
			discardCleanWorktree(
				job,
				typeof job.worktree === 'string' && job.worktree.length > 0 ? job.worktree : join(ctx.worktreesDir, id),
			);
		}
		// An abandoned log, transcript or worktree with no live record is bounded by
		// the same TTL.
		sweepExpiredEntries(spool, ctx.logsDir, stamp, {
			ttlMs,
			skip: name => {
				const job = ctx.readJob(name.replace(/\.log$/, ''));
				if (job === undefined) return false;
				const state = normalizeState(job.state);
				return state === undefined || !isTerminal(state);
			},
		});
		sweepExpiredEntries(spool, ctx.transcriptsDir, stamp, {
			ttlMs,
			// A transcript belongs to a job, so a running job's transcript is never
			// swept even when the file itself is old.
			skip: name => {
				const job = ctx.readJob(name.replace(/\.jsonl$/, ''));
				if (job === undefined) return false;
				const state = normalizeState(job.state);
				return state === undefined || !isTerminal(state);
			},
		});
		// A worktree is never removed by mtime alone: a changed one is the user's work
		// and a running job's is still in use. Only a clean worktree past the TTL goes,
		// through `git worktree remove`, which clears its metadata too.
		let worktreeNames: string[] = [];
		try {
			worktreeNames = spool.readdir(ctx.worktreesDir);
		} catch {
			worktreeNames = [];
		}
		for (const name of worktreeNames) {
			const job = ctx.readJob(name);
			if (job !== undefined) {
				const state = normalizeState(job.state);
				if (state === undefined || !isTerminal(state)) continue;
			}
			const path = join(ctx.worktreesDir, name);
			let old = false;
			try {
				old = stamp - spool.stat(path).mtimeMs > ttlMs;
			} catch {
				old = false;
			}
			if (old) discardCleanWorktree(job, path);
		}
	}

	return {finalizeJobWorktree, sweepExpired, liveState};
}
