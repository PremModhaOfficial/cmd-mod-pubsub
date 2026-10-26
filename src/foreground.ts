// The foreground outcome: what the caller sees when a run holds the turn open.
// Pure over the job record and the log, so "partial", "empty" and "failed" are
// decided by one function instead of by every caller.

import {AGENT_API_ERROR, OUTPUT_MAX_CHARS_DEFAULT} from './constants.ts';
import {reportText} from './output.ts';
import {canonicalState} from './state.ts';
import type {ForegroundResult} from './spawn.ts';

export interface ForegroundJob {
	readonly state?: unknown;
	readonly failureReason?: unknown;
	readonly description?: unknown;
	readonly id?: unknown;
	readonly kind?: unknown;
	/** A maxTurns or max_tokens stop: the result is partial and resumable. */
	readonly partial?: unknown;
}

// A line that is only worker plumbing is not agent output. "Produced nothing, or
// only tool calls" is decided here, once, for both the partial and the empty path.
const PLUMBING_LINE = /^\s*\[(tool|permission|acp)\b/;

export function hasAgentOutput(log: string): boolean {
	for (const line of log.split('\n')) {
		if (line.trim().length === 0) continue;
		if (PLUMBING_LINE.test(line)) continue;
		return true;
	}
	return false;
}

// The foreground result is the job's own result, shaped for a tool call: success
// returns the output, a cut-off returns the partial output plus a note, and any
// other failure returns the reason. Nothing is invented and nothing is dropped.
export function foregroundOutcome(job: ForegroundJob, log: string): ForegroundResult {
	const state = canonicalState(job.state);
	const label = String(job.description ?? job.id ?? 'subagent');
	const full = reportText(log, String(job.kind ?? 'agent')).trim();
	// The tool result is the model's context, so the inline result is bounded like
	// every other read. The full log stays at the job's log path.
	const text = full.length > OUTPUT_MAX_CHARS_DEFAULT ? `…${full.slice(-OUTPUT_MAX_CHARS_DEFAULT)}` : full;
	if (state === 'done') {
		return {ok: true, partial: false, text: text.length > 0 ? text : `${label} finished with no output.`};
	}
	if (state === 'waiting-input') {
		const followUp =
			String(job.kind ?? 'agent') === 'command'
				? `inspect it with subagent_output ${String(job.id)}`
				: `resume it with subagent_resume ${String(job.id)}`;
		return {
			ok: true,
			partial: true,
			text: `${text.length > 0 ? `${text}\n\n` : ''}${label} stopped with a question; ${followUp}.`,
		};
	}
	const reason =
		typeof job.failureReason === 'string' && job.failureReason.length > 0 ? job.failureReason : `${label} ${state}`;
	if (state === 'failed' && job.partial === true && hasAgentOutput(log)) {
		// A maxTurns stop is a partial success: the text is the result, and the job
		// keeps its agent id so a resume can continue it.
		return {ok: true, partial: true, text: `${text}\n\n[partial: ${reason}]`};
	}
	if (state === 'failed' && reason.startsWith(AGENT_API_ERROR) && hasAgentOutput(log)) {
		return {ok: true, partial: true, text: `${text}\n\n[cut off: ${reason}]`};
	}
	return {ok: false, error: reason};
}
