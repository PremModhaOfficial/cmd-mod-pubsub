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
	/**
	 * The permission requests already put to the human. The entry owns this set
	 * because the foreground wait and the delivery drain both ask: they share one
	 * set so a request reaches the human once, not once per poller.
	 */
	readonly askedPermissions: Set<string>;
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
	// Permission requests already put to the human in this process, shared with the
	// foreground wait, so repeatedly polling the same control dir cannot ask twice.
	const answeredPermissions = deps.askedPermissions;

	// One writer at a time. A tick that finds this flag set returns immediately rather
	// than racing the previous tick over the same files.
	let draining = false;
	// The jobs signature seen last, so a refresh only ticks the host when it changes.
	let lastJobsSignature = '';
	// Where the next pass starts in the inbox. A pass examines a bounded number of
	// files, and this cursor is what makes the NEXT pass carry on from there
	// instead of looking at the same head of the backlog every time.
	let inboxCursor = 0;

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

	// Where this pass starts in the inbox. The cursor carries over between
	// passes, so a long backlog is worked through instead of the same head
	// being examined every tick. It wraps around, and it resets when the
	// inbox has changed shape, so a new signal at the front is not starved
	// behind a long backlog.
	function startCursor(names: string[]): number {
		if (names.length === 0) {
			inboxCursor = 0;
			return 0;
		}
		// The inbox changed: the old position means nothing.
		if (inboxCursor >= names.length) inboxCursor = 0;
		return inboxCursor;
	}

	// Claim the next signal this session may deliver. Everything else - torn, already
	// delivered, untagged, foreign, or not classifiable yet - is settled right here,
	// so the caller only ever sees its own work. `seen` bounds one pass.
	//
	// `budget` bounds the FILES this call may look at, not the signals it returns.
	// A foreign signal costs a claim, a read and a restore, and it is not
	// returned, so a cap on returned signals left the work unbounded: an
	// offline session's backlog made every drain tick and every status tick
	// touch the whole backlog. The budget is spent whatever the file turns out
	// to be.
	//
	// `names` is the inbox as it was at the start of the pass. Re-reading the
	// directory on every call made a pass cost one read per signal, so the
	// pass was quadratic in the backlog.
	//
	// `cursor` says where the pass started, so a bounded pass still ADVANCES
	// through a long backlog. Without it, every pass looked at the same first
	// files and the ones behind them were never reached.
	//
	// The cursor must only ever point at a file the pass has actually looked
	// at. Stepping over a file that the budget never reached would skip it in
	// every later pass too, and a session's own result would never arrive.
	function next(
		seen: Set<string>,
		budget: {left: number},
		names: string[],
		cursor: {at: number},
	): Claim | undefined {
		while (cursor.at < names.length) {
			// The budget is checked BEFORE the cursor moves, so the pass stops
			// on the first file it did not look at and the next pass starts
			// there. The file is never stepped over.
			if (budget.left <= 0) return undefined;
			const name = names[cursor.at];
			cursor.at += 1;
			if (seen.has(name)) continue;
			budget.left -= 1;
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
		//
		// The write is atomic. Writing straight to the visible inbox name let
		// another session's drain claim the file while the write was still going:
		// it read half a JSON, treated it as invalid and deleted it, and this
		// writer then deleted the claimed copy too - the result was gone. The
		// temporary name does not end in ".json", so the inbox scan ignores it,
		// and the rename publishes it whole.
		try {
			deps.fs.rename(claim.claimed, claim.live);
		} catch {
			const tmp = `${claim.live}.restoring`;
			try {
				deps.fs.writeFile(tmp, JSON.stringify(claim.signal, null, 2));
			} catch {
				/* it stays in claimed/ and is swept later. Never deleted. */
				return;
			}
			try {
				deps.fs.rename(tmp, claim.live);
			} catch {
				/* the temporary copy is a durable record; the owner can still claim it */
				return;
			}
			try {
				deps.fs.remove(claim.claimed);
			} catch {
				/* the copy is published; the claimed original is swept later */
			}
		}
	}

	// A live background worker can ask for approval too; its request emits no signal,
	// so the drain is where the parent answers it. Fire-and-forget: the worker's own
	// wait is bounded and the UI promise settles later.
	//
	// The jobs cache spans every session in the spool, so a job owned by another
	// conversation is polled only to answer its approval request there. The owner
	// check is the same one the signal path uses: mine answers, foreign is left
	// alone, orphan and pending are not this session's to answer.
	function answerLivePermissions(): void {
		for (const job of deps.jobs()) {
			if (job.live !== true) continue;
			if (deps.ownerVerdict(job.owner) !== 'mine') continue;
			void deps.answerPendingPermission(join(deps.controlRoot, job.id), answeredPermissions);
		}
	}

	function drain(): void {
		if (draining) return;
		draining = true;
		const seen = new Set<string>();
		// One budget and one directory read for the whole pass: the files this
		// pass may look at, and the inbox as it is now.
		const budget = {left: FILES_PER_DRAIN_MAX};
		const names = inboxNames();
		const cursor = {at: startCursor(names)};
		try {
			answerLivePermissions();
			for (let handled = 0; handled < FILES_PER_DRAIN_MAX; handled += 1) {
				const claim = next(seen, budget, names, cursor);
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
			// The next pass carries on from the first file this pass did NOT
			// look at, so a long backlog is worked through and no file is
			// stepped over.
			inboxCursor = cursor.at;
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
		// The budget is the larger of the two bounds: a pass may look at more
		// files than it claims, because a foreign file is looked at and put back
		// without being claimed.
		const budget = {left: Math.max(limit, FILES_PER_DRAIN_MAX)};
		const names = inboxNames();
		const cursor = {at: startCursor(names)};
		const claims: Claim[] = [];
		while (claims.length < limit) {
			const claim = next(seen, budget, names, cursor);
			if (claim === undefined) break;
			claims.push(claim);
		}
		inboxCursor = cursor.at;
		return claims;
	}

	// How many signals are waiting for this session. It claims through the same
	// next()/settle() pair the drain uses, so the count cannot drift from delivery.
	function countPending(): number {
		const seen = new Set<string>();
		// The count is a number for the status line, not a promise to look at the
		// whole inbox. It runs on every status refresh, so it gets the same
		// per-pass bound the drain does: a foreign backlog must not make the
		// status line cost more than the drain does.
		const budget = {left: FILES_PER_DRAIN_MAX};
		const names = inboxNames();
		const cursor = {at: startCursor(names)};
		let count = 0;
		while (count < PENDING_SCAN_MAX) {
			const claim = next(seen, budget, names, cursor);
			if (claim === undefined) break;
			count += 1;
			settle(claim, 'held');
		}
		inboxCursor = cursor.at;
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
