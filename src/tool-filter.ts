// ---------------------------------------------------------------------------
// Tool access resolution
//
// The filter runs once, in the parent, before a worker is spawned: the worker
// spec carries the result, so two jobs built from the same definition cannot
// disagree. It is pure and exported, because each branch below is a documented
// Claude Code rule and is tested directly.
//
// `available` is the tool pool a subagent inherits. The host does not expose its
// tool list through the mod API, so the built-in part is the documented set; MCP
// servers are read from the same config files the host reads.
// ---------------------------------------------------------------------------

import {TEAM_TOOL_NAMES} from './constants.ts';

export const SUBAGENT_TOOL_UNIVERSE: string[] = [
	'Agent',
	'Artifact',
	'AskUserQuestion',
	'Bash',
	'Edit',
	'EndConversation',
	'EnterPlanMode',
	'EnterWorktree',
	'ExitPlanMode',
	'ExitWorktree',
	'Glob',
	'Grep',
	'LSP',
	'ListAgents',
	'Monitor',
	'NotebookEdit',
	'PowerShell',
	'Read',
	'ScheduleWakeup',
	'SendMessage',
	'Skill',
	'SubagentHandback',
	'TaskStop',
	'TodoWrite',
	'ToolSearch',
	'WaitForMcpServers',
	'WebFetch',
	'WebSearch',
	'Workflow',
	'Write',
];

// Filter one: removed from every subagent, even when the definition lists them.
// `ExitPlanMode` is handled separately because plan mode keeps it; `Agent` stays
// listed and reports the depth error itself.
export const UNIVERSAL_TOOL_REMOVALS: string[] = [
	'AskUserQuestion',
	'EndConversation',
	'EnterPlanMode',
	'ScheduleWakeup',
	'WaitForMcpServers',
	'Workflow',
];

// Filter two: the built-in tools a background subagent keeps. Every MCP tool is
// kept regardless; `ListAgents` is deliberately absent, so it is foreground-only.
// `Agent` and `ExitPlanMode` are exempt: they follow filter one wherever the agent
// runs, so a background agent at depth keeps `Agent` to report its own depth error.
// Teammates on a team additionally keep the task and cron tools (phase 15).
export const BACKGROUND_BUILTIN_TOOLS: string[] = [
	'Read',
	'Grep',
	'Glob',
	'LSP',
	'Bash',
	'PowerShell',
	'Edit',
	'Write',
	'NotebookEdit',
	'WebFetch',
	'WebSearch',
	'TodoWrite',
	'Skill',
	'ToolSearch',
	'EnterWorktree',
	'ExitWorktree',
	'Monitor',
	'TaskStop',
	'SendMessage',
	'Artifact',
	'SubagentHandback',
];

export const EFFORT_LEVELS = ['low', 'medium', 'high', 'xhigh', 'max'] as const;

export const MCP_PREFIX = 'mcp__';

export function unique(values: string[]): string[] {
	const out: string[] = [];
	for (const value of values) if (!out.includes(value)) out.push(value);
	return out;
}

export const isMcpTool = (name: string): boolean => name.startsWith(MCP_PREFIX);

// `Bash(git push *)` names a tool plus a constraint; the whole tool is removed.
// `mcp__server__*` stays whole because the wildcard is a server, not a tool name.
export function toolNameOfSpecifier(specifier: string): string {
	const text = String(specifier ?? '').trim();
	const open = text.indexOf('(');
	return (open >= 0 ? text.slice(0, open) : text).trim();
}

export function mcpCapability(server: string): string {
	return `${MCP_PREFIX}${server}__*`;
}

export function mcpParts(name: string): {server: string; tool?: string} {
	const rest = name.startsWith(MCP_PREFIX) ? name.slice(MCP_PREFIX.length) : name;
	const separator = rest.indexOf('__');
	if (separator < 0) return {server: rest};
	return {server: rest.slice(0, separator), tool: rest.slice(separator + 2)};
}

// One specifier -> the concrete capabilities it selects out of the inherited pool.
// An empty result means the specifier resolved to nothing.
export function expandSpecifier(specifier: string, available: string[], mcpServers: string[]): string[] {
	const name = toolNameOfSpecifier(specifier);
	if (name.length === 0) return [];
	const capabilities = mcpServers.map(mcpCapability);
	if (name === '*') return [...available, ...capabilities];
	if (name === `${MCP_PREFIX}*`) return capabilities;
	if (isMcpTool(name)) {
		const {server, tool} = mcpParts(name);
		if (!mcpServers.includes(server)) return [];
		if (tool === undefined || tool === '*') return [mcpCapability(server)];
		return [`${MCP_PREFIX}${server}__${tool}`];
	}
	return available.includes(name) ? [name] : [];
}

export function withheldBy(specifier: string, capability: string): boolean {
	const name = toolNameOfSpecifier(specifier);
	if (name.length === 0) return false;
	if (name === '*') return true;
	if (name === `${MCP_PREFIX}*`) return isMcpTool(capability);
	if (isMcpTool(name)) {
		if (!isMcpTool(capability)) return false;
		const wanted = mcpParts(name);
		const actual = mcpParts(capability);
		if (wanted.server !== actual.server) return false;
		if (wanted.tool === undefined || wanted.tool === '*') return true;
		return actual.tool === wanted.tool;
	}
	return capability === name;
}

// The mod registers its own names for the host's agent-facing tools. A resolved
// `tools` list speaks the host's vocabulary, so a call is matched through the alias.
export const WORKER_TOOL_ALIASES: Readonly<Record<string, string>> = {
	subagent_spawn: 'Agent',
	pubsub_fire: 'Agent',
	subagent_send: 'SendMessage',
};

// The worker enforces the resolved set because ACP offers no per-session tool
// filter. An MCP capability stands for every tool on its server.
export function workerToolAllowed(
	toolName: string,
	tools: readonly string[],
	disallowedTools: readonly string[] = [],
	team = false,
): boolean {
	for (const specifier of disallowedTools) {
		// A teammate keeps the shared task and cron tools, matching filterTools.
		if (team && TEAM_TOOL_NAMES.includes(toolName)) continue;
		if (withheldBy(specifier, toolName)) return false;
	}
	if (tools.includes(toolName)) return true;
	const hostName = WORKER_TOOL_ALIASES[toolName];
	if (hostName !== undefined && tools.includes(hostName)) return true;
	if (!isMcpTool(toolName)) return false;
	if (tools.includes(`${MCP_PREFIX}*`)) return true;
	return tools.includes(mcpCapability(mcpParts(toolName).server));
}

export interface ToolFilterInput {
	/** The inherited tool pool. Defaults to the documented built-in universe. */
	available?: string[];
	/** Configured MCP server names; each contributes `mcp__<server>__*`. */
	mcpServers?: string[];
	/** A `tools` list. Omitted means inherit. */
	tools?: string[];
	/** A `disallowedTools` list. It wins over `tools`. */
	disallowedTools?: string[];
	/** `plan` keeps `ExitPlanMode`; every other mode removes it. */
	permissionMode?: string;
	/** A background agent keeps every MCP tool and only the allowlisted built-ins. */
	background?: boolean;
	/** A teammate keeps the shared task and cron tools, even when filtered out. */
	team?: boolean;
}

export type ToolFilterResult =
	| {ok: true; tools: string[]; removed: string[]}
	| {ok: false; error: string};

export function filterTools(input: ToolFilterInput = {}): ToolFilterResult {
	const available = unique([
		...(input.available ?? SUBAGENT_TOOL_UNIVERSE),
		...(input.team === true ? TEAM_TOOL_NAMES : []),
	]);
	const mcpServers = unique(input.mcpServers ?? []);
	const requested = input.tools === undefined || input.tools.length === 0 ? undefined : input.tools;

	let selected: string[];
	if (requested === undefined) {
		// Inherit every tool available to subagents, including every MCP server.
		selected = [...available, ...mcpServers.map(mcpCapability)];
	} else {
		selected = [];
		for (const specifier of requested) selected.push(...expandSpecifier(specifier, available, mcpServers));
		selected = unique(selected);
	}

	for (const specifier of input.disallowedTools ?? []) {
		selected = selected.filter(capability => !withheldBy(specifier, capability));
	}

	const removed: string[] = [];
	const kept = selected.filter(capability => {
		if (isMcpTool(capability)) return true;
		if (UNIVERSAL_TOOL_REMOVALS.includes(capability)) {
			removed.push(capability);
			return false;
		}
		if (capability === 'ExitPlanMode' && input.permissionMode !== 'plan') {
			removed.push(capability);
			return false;
		}
		if (
			input.background === true &&
			capability !== 'Agent' &&
			capability !== 'ExitPlanMode' &&
			!BACKGROUND_BUILTIN_TOOLS.includes(capability)
		) {
			removed.push(capability);
			return false;
		}
		return true;
	});

	// A teammate keeps the task and cron tools even when its `disallowedTools` or
	// its background filter would have removed them.
	if (input.team === true) {
		for (const tool of TEAM_TOOL_NAMES) if (!kept.includes(tool)) kept.push(tool);
	}

	if (requested !== undefined && kept.length === 0) {
		// Name the entries, not the derived names: the author must be able to find the
		// line in the definition.
		return {ok: false, error: `tools list resolved to no available tool: ${requested.join(', ')}`};
	}
	return {ok: true, tools: kept, removed: removed.filter(tool => !kept.includes(tool))};
}
