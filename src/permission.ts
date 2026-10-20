// ---------------------------------------------------------------------------
// Answering a worker's permission request
//
// A worker cannot ask the human itself: it writes its permission request to its
// control dir and waits, bounded. This process owns the human, so it puts the
// request to them and writes the answer back. The protocol is two files in the
// control dir: permission.json (the worker's question) and
// permission-response.json (our answer), the latter written atomically through
// a temporary file and a rename. No answer (headless, dismissed) leaves the
// worker's own bound in charge instead of inventing one.
//
// The module is a leaf: the spool, the clock, the pid, and the UI arrive in a
// PermissionContext. The worker-side handler is a different process, so it
// stays embedded in WORKER_SOURCE in the entry.
// ---------------------------------------------------------------------------

import type {PermissionRequest, SpoolFs} from './types.ts';
import {join} from 'node:path';

/** What answerPendingPermission reads: the spool, the clock, and the human. */
export interface PermissionContext {
	readonly fs: SpoolFs;
	now(): number;
	readonly pid: number;
	readonly ui: unknown;
}

// A foreground worker cannot ask the human itself: it writes its permission
// request to its control dir and waits, bounded. This process owns the human, so
// it puts the request to them and writes the answer back. No answer (headless,
// dismissed) leaves the worker's own bound in charge instead of inventing one.
//
// The `asked` set is shared by every caller in this process: the foreground wait
// and the delivery drain both poll the same control dir, and a request must
// reach the human once. The key is reserved BEFORE the dialog opens, so a
// second poll cannot open a competing dialog for the same request.
export async function answerPendingPermission(
	controlDir: string,
	asked: Set<string>,
	ctx: PermissionContext,
): Promise<void> {
	let request: PermissionRequest;
	try {
		request = JSON.parse(ctx.fs.readFile(join(controlDir, 'permission.json'))) as PermissionRequest;
	} catch {
		return;
	}
	const key = String(request.requestId ?? '');
	if (key.length === 0 || asked.has(key)) return;
	const options = request.options ?? [];
	const choices = options.map((option, index) => ({
		label: option.name || option.kind || option.optionId || `option ${index + 1}`,
		index,
	}));
	const ui = ctx.ui as unknown as {
		select?: (ask: {title: string; options: Array<{label: string}>}) => Promise<string | undefined>;
	};
	if (typeof ui.select !== 'function') return;
	// Reserve before asking. The dialog is open for as long as the human takes to
	// answer it, and the drain polls every 400ms; without the reservation the same
	// request would open a second dialog and two answers would race.
	asked.add(key);
	let chosen: string | undefined;
	try {
		chosen = await ui.select({
			title: `Approve "${String(request.title)}"?`,
			options: choices.map(choice => ({label: choice.label})),
		});
	} catch {
		// A UI that cannot ask leaves the worker's own bound in charge.
		return;
	}
	if (typeof chosen !== 'string') return;
	const picked = choices.find(choice => choice.label === chosen);
	const option = picked === undefined ? undefined : options[picked.index];
	const response =
		option === undefined
			? {requestId: request.requestId, outcome: 'cancelled'}
			: {requestId: request.requestId, optionId: option.optionId};
	const file = join(controlDir, 'permission-response.json');
	const tmp = `${file}.mod-${ctx.pid}-${ctx.now()}`;
	ctx.fs.writeFile(tmp, JSON.stringify(response));
	ctx.fs.rename(tmp, file);
}
