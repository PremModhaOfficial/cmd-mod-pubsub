// ---------------------------------------------------------------------------
// Append prompt and auto-compact window
//
// Two resolvers over explicit input: the append-system-prompt text (a mod flag,
// an environment value, a file, or the `@path` form) and the auto-compact
// window, which may only lower the inherited context window. Neither reads a
// runtime or a session, so both are pure apart from the injected `fs`.
// ---------------------------------------------------------------------------

import type {SpoolFs} from './types.ts';
import {isAbsolute, join} from 'node:path';

export const AUTO_COMPACT_MIN = 100_000;
export const AUTO_COMPACT_MAX = 1_000_000;

// The field may only LOWER the inherited window: a value outside the documented
// range is clamped into it, and the result is the smaller of the two. The main
// session never has one, by contract.
export function resolveAutoCompactWindow(
	inherited: number,
	requested: number | undefined,
	isMain: boolean,
): number | undefined {
	if (isMain || requested === undefined || !Number.isFinite(requested)) return undefined;
	const bounded = Math.min(AUTO_COMPACT_MAX, Math.max(AUTO_COMPACT_MIN, Math.round(requested)));
	return Math.min(inherited, bounded);
}

export interface AppendPromptInput {
	readonly flag?: string;
	readonly file?: string;
	readonly env?: Record<string, string | undefined>;
	readonly fs: Pick<SpoolFs, 'readFile'>;
	readonly cwd: string;
}

// `--append-subagent-system-prompt` has no host flag, so it arrives as a mod flag
// or an environment value (the ACP child inherits the environment, which is how a
// nested subagent gets it too). The file form and the `@path` form both read text.
export function resolveAppendSystemPrompt(input: AppendPromptInput): string {
	const env = input.env ?? {};
	const file = (
		input.file ||
		env.BETTER_SUBAGENTS_APPEND_SYSTEM_PROMPT_FILE ||
		env.CLAUDE_CODE_APPEND_SUBAGENT_SYSTEM_PROMPT_FILE ||
		''
	).trim();
	const value = (
		input.flag ||
		env.BETTER_SUBAGENTS_APPEND_SYSTEM_PROMPT ||
		env.CLAUDE_CODE_APPEND_SUBAGENT_SYSTEM_PROMPT ||
		''
	).trim();
	const read = (path: string): string => {
		try {
			return input.fs.readFile(isAbsolute(path) ? path : join(input.cwd, path)).trim();
		} catch {
			return '';
		}
	};
	const parts: string[] = [];
	if (file.length > 0) {
		const text = read(file);
		if (text.length > 0) parts.push(text);
	}
	if (value.startsWith('@')) {
		const text = read(value.slice(1).trim());
		if (text.length > 0) parts.push(text);
	} else if (value.length > 0) {
		parts.push(value);
	}
	return parts.join('\n\n');
}
