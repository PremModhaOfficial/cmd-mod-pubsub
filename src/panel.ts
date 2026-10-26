import {TASK_ROW_GRACE_MS} from './constants.ts';
import {canonicalState, isTerminalState} from './state.ts';

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
	});}
