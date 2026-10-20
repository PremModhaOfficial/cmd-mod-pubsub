// ---------------------------------------------------------------------------
// Delivery
//
// One file in inbox/ is one undelivered signal, and rename() is the claim: it is
// atomic, so exactly one drainer wins a file even with several sessions on one
// spool. next() and settle() are the only two operations on a claimed file, so the
// drain, the onStop claim and the pending count cannot disagree about a signal -
// and in particular a signal this session does not own is put back, never deleted.
//
// The module also owns the two periodic jobs that keep a session current: drain()
// turns claimed signals into messages, and refreshJobs() re-reads the record cache
// that the status line and the SubagentStop hooks observe. Every seam arrives
// through DeliveryDeps - the spool, the clock, the claim verdict, the session sink
// and the worktree decision - so the module names no ambient state.
// ---------------------------------------------------------------------------

import {join} from 'node:path';
import {CLAIMED_TTL_MS, FILES_PER_DRAIN_MAX, LIST_SCAN_MAX, ORPHANED_TTL_MS, PENDING_SCAN_MAX} from './constants.ts';
import type {JobRecord} from './jobs.ts';
import type {Ledger} from './ledger.ts';
import type {Claim, Ownership, Settled} from './signals.ts';
import {canonicalState} from './state.ts';
import type {Signal, SpoolFs} from './types.ts';

export interface DeliveryDeps {
	/** The spool filesystem every inbox file is read, renamed and removed through. */
	readonly fs: SpoolFs;
	/** The three spool directories delivery owns. */
	readonly inboxDir: string;
	readonly claimedDir: string;
	readonly orphanedDir: string;
	/** Where a live background worker's permission request is answered. */
	readonly controlRoot: string;
	/** The durable exactly-once guard, shared with the rest of the session. */
	readonly ledger: Ledger;
	readonly now: () => number;
	readonly readJob: (id: string) => JobRecord | undefined;
	readonly writeJob: (record: JobRecord) => void;
	readonly readRecentJobs: (limit: number) => JobRecord[];
	/** The one cleanup decision per finished job; delivery only reports a kept worktree. */
	readonly finalizeWorktree: (
		job: JobRecord | undefined,
	) => {readonly kept: boolean; readonly path: string; readonly error?: string} | undefined;
	/** mine: deliver. foreign: leave it. orphan: quarantine it. pending: wait. */
	readonly ownerVerdict: (owner: unknown) => Ownership;
	readonly workerMode: boolean;
	/** Without a bound session a signal is held, not sent. */
	readonly hasSession: () => boolean;
	readonly deliverAs: () => 'steer' | 'follow-up';
	/** The A2A path: starts a turn when idle, queues behind the active turn. */
	readonly sendUserMessage: (text: string, options: {deliverAs: 'steer' | 'follow-up'}) => void;
	readonly showEntry: (type: string, data: unknown) => void;
	readonly answerPendingPermission: (controlDir: string, answered: Set<string>) => Promise<void>;
	/** The session's record cache: refreshed here, read by the status line and hooks. */
	readonly jobs: () => readonly JobRecord[];
	readonly setJobs: (jobs: JobRecord[]) => void;
	/** Run by a refresh, in this order: the SubagentStop hooks, then the status line. */
	readonly sweepSubagentStopHooks: () => void;
	readonly updateStatus: () => void;
	/** Record that the jobs cache changed, so the host re-renders. */
	readonly markJobsTick: () => void;
	/** Remove the files past `ttlMs` from one spool directory. */
	readonly sweep: (dir: string, ttlMs: number) => void;
}

export interface Delivery {
	drain(): void;
	safeDrain(): void;
	claimPending(limit: number): Claim[];
	settle(claim: Claim, outcome: Settled): void;
	markDelivered(id: string): void;
	formatSignalMessage(message: Signal): string;
	countPending(): number;
	countOrphaned(): number;
	refreshJobs(): void;
	sweepClaimed(): void;
	sweepOrphaned(): void;
}

export function createDelivery(deps: DeliveryDeps): Delivery {
	// Permission requests already answered in this process, so repeatedly polling the
	// same control dir cannot ask the human twice.
	const answeredPermissions = new Set<string>();

	// One writer at a time. A tick that finds this flag set returns immediately rather
	// than racing the previous tick over the same files.
	let draining = false;
	// The jobs signature seen last, so a refresh only ticks the host when it changes.
	let lastJobsSignature = '';

	function markDelivered(id: string): void {
		const job = deps.readJob(id);
		if (job === undefined) return;
		job.deliveredAt = deps.now();
		job.updatedAt = job.deliveredAt;
		deps.writeJob(job);
	}

	// A2A-style envelope: the job identity and state travel WITH the result, so the
	// model can tell which worker finished, whether it passed, and where the full log is.
	function formatSignalMessage(message: Signal): string {
		const state = canonicalState(message.state ?? message.kind ?? 'signal');
		const worktree = deps.finalizeWorktree(deps.readJob(String(message.id ?? '')));
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
		const title = String(message.title ?? message.description ?? message.id ?? '(signal)');
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
				? typeof message.agentId === 'string'
					? `The job is resumable: subagent_resume {id: "${message.agentId}", prompt} continues it. Nothing else arrives until then.`
					: 'The job is not resumable (no agent id); fire a follow-up job. Nothing else arrives until then.'
				: state === 'canceled'
					? 'The job was canceled; nothing else will arrive for it.'
					: message.partial === true && typeof message.agentId === 'string'
						? `The result is partial: resume it with subagent_resume ${message.agentId}.`
						: 'This result arrived automatically. Read it now and use it in your current work; ' +
							'do not poll for it and do not wait for it.';
		return `[subagent] ${title}\n${facts}${log}${boundary}${kept}` + (body.length > 0 ? `\n\n${body}` : '') + ask + `\n\n${next}`;
	}

	// `queueMessage` only steers a run that is already going - it never STARTS one,
	// so an idle session would leave the result unread until someone asked for it.
	// `sendUserMessage` starts the turn when idle and queues behind the active turn
	// otherwise. This is the A2A delivery path.
	function deliver(message: Signal): void {
		deps.sendUserMessage(formatSignalMessage(message), {deliverAs: deps.deliverAs()});
		deps.showEntry('subagent', message);
	}

	function inboxNames(): string[] {
		try {
			return deps.fs.readdir(deps.inboxDir).filter(name => name.endsWith('.json'));
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
			const live = join(deps.inboxDir, name);
			const claimed = join(deps.claimedDir, name);
			try {
				deps.fs.rename(live, claimed);
			} catch {
				/* another drainer won it */
				continue;
			}
			let signal: Signal | undefined;
			try {
				signal = JSON.parse(deps.fs.readFile(claimed)) as Signal;
			} catch {
				signal = undefined;
			}
			if (signal === undefined || typeof signal.id !== 'string' || signal.id.length === 0) {
				deps.fs.remove(claimed);
				continue;
			}
			const claim: Claim = {name, signal, live, claimed};
			if (deps.ledger.has(signal.id)) {
				// Delivered once already (a replayed claimed/ file): drop this copy.
				settle(claim, 'accepted');
				continue;
			}
			const owner = deps.ownerVerdict(signal.owner);
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
			const settledJob = deps.readJob(signal.id);
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
			deps.ledger.accept(claim.signal.id);
			deps.fs.remove(claim.claimed);
			return;
		}
		if (outcome === 'quarantined') {
			try {
				deps.fs.rename(
					claim.claimed,
					join(deps.orphanedDir, `${deps.now()}-${Math.random().toString(36).slice(2, 6)}-${claim.name}`),
				);
			} catch {
				/* leave it in claimed/; sweepClaimed bounds it. Never delete a signal here. */
			}
			return;
		}
		// Held: put it back for its owner. If the rename cannot happen, write the
		// signal back into inbox/ instead of deleting the only durable copy.
		try {
			deps.fs.rename(claim.claimed, claim.live);
		} catch {
			try {
				deps.fs.writeFile(claim.live, JSON.stringify(claim.signal, null, 2));
				deps.fs.remove(claim.claimed);
			} catch {
				/* it stays in claimed/ and is swept later. Never deleted. */
			}
		}
	}

	// A live background worker can ask for approval too; its request emits no signal,
	// so the drain is where the parent answers it. Fire-and-forget: the worker's own
	// wait is bounded and the UI promise settles later.
	function answerLivePermissions(): void {
		for (const job of deps.jobs()) {
			if (job.live !== true) continue;
			void deps.answerPendingPermission(join(deps.controlRoot, job.id), answeredPermissions);
		}
	}

	function drain(): void {
		if (draining) return;
		draining = true;
		const seen = new Set<string>();
		try {
			answerLivePermissions();
			for (let handled = 0; handled < FILES_PER_DRAIN_MAX; handled += 1) {
				const claim = next(seen);
				if (claim === undefined) return;
				// No bound session is a state, not an exception: sending here would
				// degrade to a silent queue push in a headless run. Hold it instead.
				if (!deps.hasSession()) {
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
			return deps.fs.readdir(deps.orphanedDir).length;
		} catch {
			return 0;
		}
	}

	function refreshJobs(): void {
		const jobs = deps.readRecentJobs(LIST_SCAN_MAX);
		deps.setJobs(jobs);
		deps.sweepSubagentStopHooks();
		deps.updateStatus();
		const signature = jobs.map(job => `${job.id}:${canonicalState(job.state)}`).join('|');
		if (signature !== lastJobsSignature) {
			lastJobsSignature = signature;
			deps.markJobsTick();
		}
	}

	// claimed/ is a rename target, not an archive. Sweep files abandoned by a drain
	// that died between claim and remove so a long-lived spool cannot grow forever.
	function sweepClaimed(): void {
		deps.sweep(deps.claimedDir, CLAIMED_TTL_MS);
	}

	function sweepOrphaned(): void {
		if (deps.workerMode) return;
		deps.sweep(deps.orphanedDir, ORPHANED_TTL_MS);
	}

	return {
		drain,
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
	};
}
