// ---------------------------------------------------------------------------
// Ledger
//
// Exactly-once delivery needs a replay guard that survives a restart, and a spool
// that does not grow forever. Both are the ledger's business: has(id) is the
// guard, accept(id) makes the fact durable, and retain(n) is the only compaction.
// Memory and disk are pruned together, so a size cap can never silently drop the
// guard the way a clear() of the in-memory set did.
// ---------------------------------------------------------------------------

import type {SpoolFs} from './types.ts';

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

export function createLedger(deps: LedgerDeps): Ledger {
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
