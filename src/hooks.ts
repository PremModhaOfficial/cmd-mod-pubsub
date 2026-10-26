// Hook execution: how a matcher selects a tool and what a hook's exit code
// means for a run. A hook is a matcher plus a command, run through the injected
// `run` with the event, agent, tool and payload in its environment, so this
// module needs no OS access of its own.

import type {CommandResult, Runtime} from './types.ts';
import type {AgentHookRule} from './definitions.ts';

// A matcher is `*`, an exact tool name, a pipe list, a `/regex/`, or a glob such
// as `mcp__server__*`. Tool names are case-insensitive, like the host's.
export function hookMatches(matcher: string | undefined, toolName: string): boolean {
	const pattern = (matcher ?? '').trim();
	if (pattern.length === 0 || pattern === '*') return true;
	if (pattern.length > 2 && pattern.startsWith('/') && pattern.endsWith('/')) {
		try {
			return new RegExp(pattern.slice(1, -1), 'i').test(toolName);
		} catch {
			return false;
		}
	}
	const glob = (value: string): boolean => {
		const escaped = value.replace(/[.+?^${}()|[\]\\]/g, '\\$&').replaceAll('*', '.*');
		return new RegExp(`^${escaped}$`, 'i').test(toolName);
	};
	return pattern.split('|').some(part => glob(part.trim()));
}

export interface HookDetail {
	readonly event: 'PreToolUse' | 'PostToolUse' | 'SubagentStart' | 'SubagentStop';
	readonly cwd: string;
	readonly agent?: string;
	readonly toolName?: string;
	readonly input?: unknown;
	readonly result?: unknown;
}

export interface HookOutcome {
	/** A PreToolUse hook requested a block; the text becomes the tool result. */
	block?: string;
	/** A PostToolUse hook's message for the model. */
	additionalContext?: string;
	/** A SubagentStop hook requested another turn. */
	continue?: boolean;
	reason?: string;
	/** How many commands actually ran. */
	ran: number;
}

const HOOK_TIMEOUT_MS = 10_000;

// The command-hook contract: exit 0 allows, exit 2 blocks (PreToolUse), feeds the
// model (PostToolUse), or continues the run (SubagentStop), and any other exit is a
// non-blocking failure. Every rule gets the event, agent, tool and payload in its
// environment, so one script can serve all events.
export function runHookRules(
	run: Runtime['run'],
	rules: readonly AgentHookRule[] | undefined,
	detail: HookDetail,
): HookOutcome {
	const outcome: HookOutcome = {ran: 0};
	if (run === undefined || rules === undefined || rules.length === 0) return outcome;
	const contexts: string[] = [];
	for (const rule of rules) {
		if ((detail.event === 'PreToolUse' || detail.event === 'PostToolUse') && !hookMatches(rule.matcher, detail.toolName ?? '')) {
			continue;
		}
		const env: Record<string, string> = {
			BETTER_SUBAGENTS_HOOK_EVENT: detail.event,
			BETTER_SUBAGENTS_HOOK_AGENT: detail.agent ?? '',
			BETTER_SUBAGENTS_HOOK_TOOL_NAME: detail.toolName ?? '',
			BETTER_SUBAGENTS_HOOK_TOOL_INPUT: JSON.stringify(detail.input ?? {}),
			BETTER_SUBAGENTS_HOOK_RESULT: JSON.stringify(detail.result ?? null),
			BETTER_SUBAGENTS_HOOK_CWD: detail.cwd,
		};
		let result: CommandResult;
		try {
			result = run('/bin/sh', ['-c', rule.command], {
				cwd: detail.cwd,
				timeoutMs: rule.timeoutMs ?? HOOK_TIMEOUT_MS,
				env,
			});
		} catch {
			continue;
		}
		outcome.ran += 1;
		const code = typeof result.code === 'number' ? result.code : result.ok ? 0 : 1;
		if (code !== 2) continue;
		const message = (result.stderr || result.stdout || '').trim();
		if (detail.event === 'PreToolUse') {
			outcome.block = message.length > 0 ? message : 'Blocked by a PreToolUse hook.';
			return outcome;
		}
		if (detail.event === 'PostToolUse') {
			if (message.length > 0) contexts.push(message);
			continue;
		}
		if (detail.event === 'SubagentStop') {
			outcome.continue = true;
			outcome.reason = message.length > 0 ? message : 'SubagentStop hook requested another turn.';
			return outcome;
		}
	}
	if (contexts.length > 0) outcome.additionalContext = contexts.join('\n\n');
	return outcome;
}