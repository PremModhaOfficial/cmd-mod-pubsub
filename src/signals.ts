// ---------------------------------------------------------------------------
// Envelope
//
// One contract for "this job reached this state", in both directions:
// signalOf(record, result) is the signal, patchOf(signal) is the record write the
// signal implies. Both are pure, so no writer can invent its own field list.
//
// signalEnvelope is also shared with the detached worker: WORKER_SOURCE embeds
// this function's source at module load, so the parent and the worker cannot
// construct a signal with different fields. Keep the body plain JS and
// self-contained - it cannot reference anything else in this module.
// ---------------------------------------------------------------------------

import {canonicalState} from './state.ts';
import type {CompactBoundary, JobIdentity, JobPatch, PermissionRequest, Signal} from './types.ts';

// A record is enough: the envelope only reads the identity fields it shares with
// the signal, so a JobRecord passes without this module naming the record shape.
export function signalOf(
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
	return signalEnvelope(record, {...result, state: canonicalState(result.state)});
}

export function signalEnvelope(
	record: JobIdentity,
	result: {
		state: string;
		finishedAt?: number;
		durationMs?: number;
		exitCode?: number | null;
		title?: string;
		body?: string;
		question?: string;
		permissionRequest?: PermissionRequest;
		scanned?: boolean;
		scanMatches?: string[];
		partial?: boolean;
		compactBoundary?: CompactBoundary;
	},
): Signal {
	const description = String(record.description ?? record.id);
	const state = String(result.state);
	return {
		id: record.id,
		...(record.agentId !== undefined ? {agentId: record.agentId} : {}),
		owner: record.owner,
		description,
		kind: String(record.kind ?? 'command'),
		state,
		exitCode: typeof result.exitCode === 'number' ? result.exitCode : null,
		startedAt: record.startedAt,
		finishedAt: result.finishedAt,
		durationMs: result.durationMs,
		logPath: record.logPath,
		sessionId: record.sessionId,
		title: result.title ?? `${description} ${state}`,
		body: result.body,
		...(typeof result.question === 'string' ? {question: result.question} : {}),
		...(result.permissionRequest !== undefined ? {permissionRequest: result.permissionRequest} : {}),
		// Omitted when the writer did not scan, so a legacy signal keeps its shape.
		...(result.scanned === undefined
			? {}
			: {scanned: result.scanned === true, scanMatches: result.scanMatches ?? []}),
		...(result.partial === true ? {partial: true} : {}),
		...(result.compactBoundary !== undefined ? {compactBoundary: result.compactBoundary} : {}),
	};
}

// One claimed inbox file: the signal, plus the two paths settle() needs to undo
// or finish the claim. Ownership is what the drain decides; Settled is how the
// claim ends.
export interface Claim {
	readonly name: string;
	readonly signal: Signal;
	readonly live: string;
	readonly claimed: string;
}

export type Ownership = 'mine' | 'foreign' | 'orphan' | 'pending';
// `discarded` is a duplicate copy of a signal that is still waiting to be
// delivered. The copy is removed, but the id is NOT recorded as delivered:
// the first copy is still in the inbox, and recording the id would make the
// next drain delete that first copy as already delivered. The result would
// never arrive.
export type Settled = 'accepted' | 'held' | 'quarantined' | 'discarded';

export function patchOf(signal: Signal): JobPatch {
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
	if (typeof signal.question === 'string') patch.question = signal.question;
	if (signal.permissionRequest !== undefined) patch.permissionRequest = signal.permissionRequest;
	if (signal.scanned !== undefined) {
		patch.scanned = signal.scanned;
		patch.scanMatches = signal.scanMatches ?? [];
	}
	if (signal.partial === true) patch.partial = true;
	if (signal.compactBoundary !== undefined) patch.compactBoundary = signal.compactBoundary;
	return patch;
}
