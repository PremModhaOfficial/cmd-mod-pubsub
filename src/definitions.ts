// ---------------------------------------------------------------------------
// Agent definitions
//
// A definition is Markdown with YAML frontmatter. The body is the system prompt,
// not the host's. Only `name` and `description` are required; every other T1 field
// is parsed and carried on the definition even when a later phase applies it.
//
// Discovery order is precedence order: the managed settings directory, the CLI
// definitions, project directories from the working directory upward (closest
// first) and every --add-dir directory, the user directory, plugin roots, and
// finally the built-ins. The first definition of a name wins. Invalid files never
// fail the session: they are recorded with a reason, written to agents.log, and
// reported by `/agents` and `/doctor`.
// ---------------------------------------------------------------------------

import {dirname, join} from 'node:path';
import {AGENT_NAME_MAX, AGENT_SCAN_DEPTH_MAX, AGENT_SCAN_MAX, FORK_AGENT_TYPE} from './constants.ts';
import type {SpoolFs} from './types.ts';

// The scopes, highest priority first. The order is the precedence rule: managed
// wins over CLI, CLI over project, project over user, user over plugin, and every
// one over a built-in. It is also the display order in `/agents`.
export const AGENT_SCOPES = ['managed', 'cli', 'project', 'user', 'plugin', 'builtin'] as const;

export type AgentScope = (typeof AGENT_SCOPES)[number];

export type AgentEntryKind = 'valid' | 'invalid' | 'duplicate';

export type AgentMemoryScope = 'user' | 'project' | 'local';

// A lookup, so a value outside the three scopes is simply absent. Frontmatter and
// a CLI definition read the same table.
const MEMORY_SCOPES: Readonly<Record<string, AgentMemoryScope>> = {user: 'user', project: 'project', local: 'local'};

/** One MCP server an agent scopes to itself: referenced by name or inline. */
export interface AgentMcpServer {
	name: string;
	/** Standard input server: a command and its arguments. */
	command?: string;
	args?: string[];
	env?: Record<string, string>;
	/** HTTP server. A server has either `command` or `url`. */
	url?: string;
}

/** One command a definition's hooks run. A hook blocks on exit 2. */
export interface AgentHookRule {
	/** Tool-name filter: `*`, an exact name, `mcp__srv__*`, or `/regex/`. */
	matcher?: string;
	command: string;
	timeoutMs?: number;
}

/** The definition hooks the worker registers while this agent is active. */
export interface AgentHooks {
	PreToolUse?: AgentHookRule[];
	PostToolUse?: AgentHookRule[];
	/** The spec's `Stop`; the runtime reports it as `SubagentStop`. */
	Stop?: AgentHookRule[];
}

export interface AgentDefinition {
	name: string;
	description: string;
	prompt: string;
	scope: AgentScope;
	sourcePath?: string;
	/** The plugin whose `agents/` directory provided this definition. */
	plugin?: string;
	tools?: string[];
	disallowedTools?: string[];
	model?: string;
	permissionMode?: string;
	maxTurns?: number;
	skills?: string[];
	/** `user`, `project`, or `local`: the agent's own memory directory. */
	memory?: AgentMemoryScope;
	isolation?: string;
	background?: boolean;
	color?: string;
	omitClaudeMd?: boolean;
	effort?: string;
	/** Inline or referenced MCP servers, scoped to this agent. */
	mcpServers?: AgentMcpServer[];
	/** PreToolUse/PostToolUse/Stop rules, active only while this agent runs. */
	hooks?: AgentHooks;
	/** Auto-submitted as the first user turn when this agent runs the session. */
	initialPrompt?: string;
	/** The requested auto-compact window; it can only lower the inherited one. */
	autoCompactWindow?: number;
	/** `experimental.cacheTtl`: `5m` or `1h`. */
	cacheTtl?: CacheTtl;
	readOnly?: boolean;
	oneShot?: boolean;
	/** The `fork` type: inherits the main conversation instead of a fresh context. */
	fork?: boolean;
}

export type CacheTtl = '5m' | '1h';

export interface AgentEntry {
	name: string;
	scope: AgentScope;
	sourcePath: string;
	valid: boolean;
	kind: AgentEntryKind;
	reason?: string;
	/** A definition that loaded but has a problem worth reporting (`/doctor`). */
	warning?: string;
	/** For a duplicate: the file whose definition was kept. */
	duplicateOf?: string;
	definition?: AgentDefinition;
}

export interface AgentRegistry {
	entries(): AgentEntry[];
	list(): AgentDefinition[];
	get(name: string): AgentDefinition | undefined;
	refresh(): void;
}

export interface AgentRegistryDeps {
	readonly fs: SpoolFs;
	readonly cwd: string;
	readonly home: string;
	readonly builtins: AgentDefinition[];
	readonly log: (message: string) => void;
	/** The managed settings root; `agents/` under it wins every other scope. */
	readonly managedDir?: string;
	/** Plugin roots: each child directory is a plugin with an `agents/` directory. */
	readonly pluginRoots?: string[];
	/** `--mod-option agents=<json|path>`, plus whether a human is driving. */
	readonly cli?: {readonly value: string; readonly interactive: boolean};
	/** `--add-dir` directories: `.commandcode/agents/` under each is project scope. */
	readonly addDirs?: string[];
}

export type FrontmatterResult =
	| {ok: true; fields: Record<string, unknown>; body: string}
	| {ok: false; reason: string};

// A conservative YAML subset: top-level `key: value`, inline and block lists,
// quoted and block scalars, nested maps (ignored, because unknown fields must not
// error). Anything outside it is reported as bad YAML, which is the documented
// skip rule. No dependency, no eval, no way to run a value.
function parseYamlScalar(raw: string): {ok: true; value: unknown} | {ok: false; reason: string} {
	const text = raw.trim();
	if (text.length === 0) return {ok: true, value: undefined};
	if (text.startsWith('[')) {
		if (!text.endsWith(']')) return {ok: false, reason: 'unclosed flow sequence'};
		const inner = text.slice(1, -1).trim();
		if (inner.length === 0) return {ok: true, value: []};
		const items: string[] = [];
		let current = '';
		let quote = '';
		for (const char of inner) {
			if (quote.length > 0) {
				current += char;
				if (char === quote) quote = '';
				continue;
			}
			if (char === '"' || char === "'") {
				quote = char;
				current += char;
				continue;
			}
			if (char === ',') {
				items.push(current.trim());
				current = '';
				continue;
			}
			current += char;
		}
		if (quote.length > 0) return {ok: false, reason: 'unterminated quoted scalar'};
		items.push(current.trim());
		const values: unknown[] = [];
		for (const item of items) {
			if (item.length === 0) return {ok: false, reason: 'empty flow sequence item'};
			const parsed = parseYamlScalar(item);
			if (parsed.ok === false) return parsed;
			values.push(parsed.value);
		}
		return {ok: true, value: values};
	}
	if (text.startsWith('{')) {
		if (!text.endsWith('}')) return {ok: false, reason: 'unclosed flow mapping'};
		return {ok: true, value: null};
	}
	if (text.startsWith('"')) {
		if (text.length < 2 || !text.endsWith('"')) return {ok: false, reason: 'unterminated double-quoted scalar'};
		try {
			return {ok: true, value: JSON.parse(text)};
		} catch {
			return {ok: false, reason: 'bad escape in double-quoted scalar'};
		}
	}
	if (text.startsWith("'")) {
		if (text.length < 2 || !text.endsWith("'")) return {ok: false, reason: 'unterminated single-quoted scalar'};
		return {ok: true, value: text.slice(1, -1).replaceAll("''", "'")};
	}
	const comment = text.search(/\s#/);
	const bare = (comment >= 0 ? text.slice(0, comment) : text).trim();
	if (bare === 'true') return {ok: true, value: true};
	if (bare === 'false') return {ok: true, value: false};
	if (bare === 'null' || bare === '~') return {ok: true, value: undefined};
	return {ok: true, value: bare};
}

// A nested value is the same indentation language as the document, one level
// down: mappings, block sequences, and scalars. It exists because `hooks`,
// `mcpServers`, and `experimental` are maps, not opaque unknowns; every other
// nested key is still accepted and simply never read.
interface NestedLine {
	indent: number;
	text: string;
}

type NestedResult = {ok: true; value: unknown; next: number} | {ok: false; reason: string};

function normalizeNestedLines(raw: string[]): NestedLine[] {
	const lines = raw
		.filter(line => line.trim().length > 0 && !line.trim().startsWith('#'))
		.map(line => {
			const indent = line.length - line.trimStart().length;
			return {indent, text: line.slice(indent).replace(/\s+$/, '')};
		});
	if (lines.length === 0) return lines;
	const margin = Math.min(...lines.map(line => line.indent));
	return lines.map(line => ({indent: line.indent - margin, text: line.text}));
}

const isSequenceLine = (text: string): boolean => text === '-' || text.startsWith('- ');

// `key: value`, where the colon must be followed by whitespace or end of line, so
// `- https://example.test` stays a scalar and does not become a map.
const MAP_LINE = /^([A-Za-z0-9_.-]+):(?:[ \t]+(.*))?$/;

function parseYamlNode(lines: NestedLine[], start: number, indent: number): NestedResult {
	const first = lines[start];
	if (first === undefined) return {ok: true, value: undefined, next: start};
	return isSequenceLine(first.text) ? parseYamlSequence(lines, start, indent) : parseYamlMapping(lines, start, indent);
}

function parseYamlSequence(lines: NestedLine[], start: number, indent: number): NestedResult {
	const values: unknown[] = [];
	let index = start;
	while (index < lines.length) {
		const line = lines[index];
		if (line.indent < indent) break;
		if (line.indent > indent) return {ok: false, reason: `unexpected indentation`};
		if (!isSequenceLine(line.text)) break;
		const rest = line.text === '-' ? '' : line.text.slice(2);
		const trimmed = rest.trim();
		if (trimmed.length === 0) {
			const next = lines[index + 1];
			if (next !== undefined && next.indent > indent) {
				const nested = parseYamlNode(lines, index + 1, next.indent);
				if (!nested.ok) return nested;
				values.push(nested.value);
				index = nested.next;
			} else {
				values.push(null);
				index += 1;
			}
			continue;
		}
		// A sequence item can open a mapping: `- matcher: Bash`, then sibling keys at
		// the same content column. The synthesized first line sits at that column, and
		// the following raw lines are already indented relative to it.
		const map = MAP_LINE.exec(trimmed);
		if (map !== null) {
			// The content column is where the first non-space after `-` starts.
			const base = indent + (line.text.length - trimmed.length);
			const itemLines: NestedLine[] = [{indent: base, text: trimmed}];
			let cursor = index + 1;
			while (cursor < lines.length && lines[cursor].indent > indent) {
				itemLines.push(lines[cursor]);
				cursor += 1;
			}
			const nested = parseYamlNode(itemLines, 0, base);
			if (!nested.ok) return nested;
			values.push(nested.value);
			index = cursor;
			continue;
		}
		const parsed = parseYamlScalar(trimmed);
		if (parsed.ok === false) return parsed;
		values.push(parsed.value);
		index += 1;
	}
	return {ok: true, value: values, next: index};
}

function parseYamlMapping(lines: NestedLine[], start: number, indent: number): NestedResult {
	const value: Record<string, unknown> = {};
	let index = start;
	while (index < lines.length) {
		const line = lines[index];
		if (line.indent < indent) break;
		if (line.indent > indent) return {ok: false, reason: 'unexpected indentation'};
		const match = MAP_LINE.exec(line.text);
		if (match === null) return {ok: false, reason: `expected key: value, got "${line.text.slice(0, 40)}"`};
		const key = match[1];
		const inline = (match[2] ?? '').trim();
		if (Object.hasOwn(value, key)) return {ok: false, reason: `duplicate key "${key}"`};
		const next = lines[index + 1];
		if (inline.length > 0) {
			if (next !== undefined && next.indent > indent) {
				return {ok: false, reason: `unexpected indentation under "${key}"`};
			}
			const parsed = parseYamlScalar(inline);
			if (parsed.ok === false) return parsed;
			value[key] = parsed.value;
			index += 1;
			continue;
		}
		if (next !== undefined && next.indent > indent) {
			const nested = parseYamlNode(lines, index + 1, next.indent);
			if (!nested.ok) return nested;
			value[key] = nested.value;
			index = nested.next;
			continue;
		}
		value[key] = undefined;
		index += 1;
	}
	return {ok: true, value, next: index};
}

export function parseFrontmatter(source: string): FrontmatterResult {
	const text = source.startsWith('\uFEFF') ? source.slice(1) : source;
	const lines = text.split('\n');
	if (lines[0]?.trim() !== '---') return {ok: false, reason: 'frontmatter must start with --- on the first line'};
	let close = -1;
	for (let index = 1; index < lines.length; index += 1) {
		if (lines[index].trim() === '---') {
			close = index;
			break;
		}
	}
	if (close < 0) return {ok: false, reason: 'frontmatter is not closed'};

	// Group top-level keys with their indented lines before parsing any value, so a
	// block scalar and a block list are one case rather than interleaved state. The
	// raw lines are kept: a nested value (`mcpServers`, `hooks`, `experimental`) is
	// parsed by indentation below.
	const blocks: {key: string; inline: string; lines: string[]}[] = [];
	for (let index = 1; index < close; index += 1) {
		const line = lines[index];
		const trimmed = line.trim();
		if (trimmed.length === 0 || trimmed.startsWith('#')) {
			if (blocks.length > 0) blocks[blocks.length - 1].lines.push('');
			continue;
		}
		if (line.startsWith('\t')) return {ok: false, reason: 'tab indentation is not valid YAML'};
		const indent = line.length - line.trimStart().length;
		if (indent > 0) {
			if (blocks.length === 0) return {ok: false, reason: 'unexpected indentation'};
			blocks[blocks.length - 1].lines.push(line);
			continue;
		}
		const match = /^([A-Za-z0-9_.-]+):(.*)$/.exec(line);
		if (match === null) return {ok: false, reason: `expected key: value, got "${line.slice(0, 40)}"`};
		blocks.push({key: match[1], inline: match[2].trim(), lines: []});
	}

	const fields: Record<string, unknown> = {};
	for (const block of blocks) {
		if (Object.hasOwn(fields, block.key)) return {ok: false, reason: `duplicate key "${block.key}"`};
		const blockScalar = block.inline === '|' || block.inline === '|-' || block.inline === '>' || block.inline === '>-';
		if (blockScalar) {
			// A block scalar keeps its relative indentation; only the common margin and
			// the surrounding blank lines are removed.
			const filled = block.lines.filter(line => line.trim().length > 0);
			const margin =
				filled.length === 0
					? 0
					: Math.min(...filled.map(line => line.length - line.trimStart().length));
			const body = block.lines.map(line => (line.trim().length === 0 ? '' : line.slice(margin).replace(/\s+$/, '')));
			fields[block.key] = (block.inline.startsWith('>') ? body.join(' ') : body.join('\n')).replace(/^\n+|\n+$/g, '');
			continue;
		}
		const hasNested = block.lines.some(line => line.trim().length > 0);
		if (block.inline.length === 0 && hasNested) {
			const parsed = parseYamlNode(normalizeNestedLines(block.lines), 0, 0);
			if (parsed.ok === false) return parsed;
			fields[block.key] = parsed.value;
			continue;
		}
		if (hasNested) {
			return {ok: false, reason: `unexpected indentation under "${block.key}"`};
		}
		if (block.inline.length === 0) {
			fields[block.key] = undefined;
			continue;
		}
		const parsed = parseYamlScalar(block.inline);
		if (parsed.ok === false) return parsed;
		fields[block.key] = parsed.value;
	}

	return {ok: true, fields, body: lines.slice(close + 1).join('\n').replace(/^\r?\n/, '').trimEnd()};
}

export function listField(value: unknown): string[] | undefined {
	if (value === undefined || value === null) return undefined;
	if (Array.isArray(value)) {
		const items = value.filter((item): item is string => typeof item === 'string').map(item => item.trim()).filter(Boolean);
		return items.length > 0 ? items : undefined;
	}
	if (typeof value !== 'string') return undefined;
	const text = value.trim();
	if (text.length === 0) return undefined;
	if (text === '*') return ['*'];
	return text.split(',').map(item => item.trim()).filter(Boolean);
}

export function stringField(value: unknown): string | undefined {
	return typeof value === 'string' && value.trim().length > 0 ? value.trim() : undefined;
}

function booleanField(value: unknown): boolean | undefined {
	if (typeof value === 'boolean') return value;
	if (value === 'true') return true;
	if (value === 'false') return false;
	return undefined;
}

function numberField(value: unknown): number | undefined {
	const numeric = typeof value === 'number' ? value : typeof value === 'string' ? Number(value) : Number.NaN;
	return Number.isFinite(numeric) && numeric > 0 ? Math.round(numeric) : undefined;
}

export function stringMapField(value: unknown): Record<string, string> | undefined {
	if (value === null || typeof value !== 'object' || Array.isArray(value)) return undefined;
	const out: Record<string, string> = {};
	for (const [key, raw] of Object.entries(value as Record<string, unknown>)) {
		if (typeof raw !== 'string') continue;
		out[key] = raw;
	}
	return Object.keys(out).length > 0 ? out : undefined;
}

function serverFields(value: Record<string, unknown>): Omit<AgentMcpServer, 'name'> | undefined {
	const command = stringField(value.command);
	const url = stringField(value.url);
	const args = listField(value.args);
	const env = stringMapField(value.env);
	if (command === undefined && url === undefined) return undefined;
	return {
		...(command !== undefined ? {command} : {}),
		...(args !== undefined ? {args} : {}),
		...(env !== undefined ? {env} : {}),
		...(url !== undefined ? {url} : {}),
	};
}

// `mcpServers` accepts the host's map form, a list of names (references to the
// configured servers), and a list of inline servers. A plugin's servers are
// ignored by contract, so parsing is skipped there rather than resolved later.
export function parseMcpServers(value: unknown): AgentMcpServer[] | undefined {
	const out: AgentMcpServer[] = [];
	if (typeof value === 'string') {
		const name = stringField(value);
		return name === undefined ? undefined : [{name}];
	}
	if (Array.isArray(value)) {
		for (const item of value) {
			if (typeof item === 'string') {
				const name = stringField(item);
				if (name !== undefined) out.push({name});
				continue;
			}
			if (item === null || typeof item !== 'object') continue;
			const record = item as Record<string, unknown>;
			const name = stringField(record.name) ?? stringField(record.server);
			if (name === undefined) continue;
			out.push({name, ...serverFields(record)});
		}
	} else if (value !== null && typeof value === 'object') {
		for (const [name, config] of Object.entries(value as Record<string, unknown>)) {
			const clean = name.trim();
			if (clean.length === 0) continue;
			if (config === null || config === undefined || config === true) {
				out.push({name: clean});
				continue;
			}
			if (typeof config === 'string') {
				out.push({name: clean, command: config});
				continue;
			}
			if (typeof config === 'object' && !Array.isArray(config)) {
				out.push({name: clean, ...serverFields(config as Record<string, unknown>)});
			}
		}
	}
	return out.length > 0 ? out : undefined;
}

// A hook rule is a command string, or a map with `command` plus an optional
// `matcher` and `timeout` (seconds). The standard nested form
// (`{matcher, hooks: [{type, command}]}`) is flattened, inheriting the matcher.
function hookRule(value: unknown, inheritedMatcher?: string): AgentHookRule[] {
	if (typeof value === 'string') {
		const command = value.trim();
		return command.length === 0 ? [] : [{command, ...(inheritedMatcher !== undefined ? {matcher: inheritedMatcher} : {})}];
	}
	if (value === null || typeof value !== 'object' || Array.isArray(value)) return [];
	const record = value as Record<string, unknown>;
	const matcher = stringField(record.matcher) ?? inheritedMatcher;
	const nested = record.hooks;
	if (nested !== undefined) {
		// The wrapper only groups; the commands are the rules.
		return Array.isArray(nested) ? nested.flatMap(item => hookRule(item, matcher)) : hookRule(nested, matcher);
	}
	const command = stringField(record.command);
	if (command === undefined) return [];
	const seconds = numberField(record.timeout);
	return [
		{
			command,
			...(matcher !== undefined ? {matcher} : {}),
			...(seconds !== undefined ? {timeoutMs: seconds * 1000} : {}),
		},
	];
}

export function hookList(value: unknown): AgentHookRule[] | undefined {
	const rules = (Array.isArray(value) ? value : [value]).flatMap(item => hookRule(item));
	return rules.length > 0 ? rules : undefined;
}

// Only the events the frontmatter reference defines are read. `SubagentStop` is
// accepted as the runtime spelling of `Stop`; anything else is an unknown field.
export function parseHooks(value: unknown): AgentHooks | undefined {
	if (value === null || typeof value !== 'object' || Array.isArray(value)) return undefined;
	const byEvent: Record<string, AgentHookRule[]> = {};
	for (const [event, raw] of Object.entries(value as Record<string, unknown>)) {
		const normalized = event.replace(/[^a-z]/gi, '').toLowerCase();
		const key =
			normalized === 'pretooluse'
				? 'PreToolUse'
				: normalized === 'posttooluse'
					? 'PostToolUse'
					: normalized === 'stop' || normalized === 'subagentstop'
						? 'Stop'
						: undefined;
		if (key === undefined) continue;
		const rules = hookList(raw);
		if (rules !== undefined) byEvent[key] = [...(byEvent[key] ?? []), ...rules];
	}
	return Object.keys(byEvent).length > 0 ? (byEvent as AgentHooks) : undefined;
}

export function parseCacheTtl(value: unknown): CacheTtl | undefined {
	if (value === null || typeof value !== 'object' || Array.isArray(value)) return undefined;
	const ttl = stringField((value as Record<string, unknown>).cacheTtl);
	return ttl === '5m' || ttl === '1h' ? ttl : undefined;
}

export interface AgentDefinitionOptions {
	/** A plugin definition: an unparseable file or one without a name still loads. */
	plugin?: boolean;
	/** The plugin that owns the file, recorded on the definition. */
	pluginName?: string;
	/** The scoped prefix the raw name is appended to, e.g. `my-plugin:review:`. */
	scopedPrefix?: string;
	/** The filename stem, used as the name when frontmatter provides none. */
	fallbackName?: string;
}

export type AgentDefinitionResult =
	| {ok: true; definition: AgentDefinition; warning?: string}
	| {ok: false; reason: string};

/** Every name rule in one place, so a file and a CLI definition agree. */
function validateAgentName(name: string): string | undefined {
	if ([...name].length > AGENT_NAME_MAX) return `name: must be at most ${AGENT_NAME_MAX} characters`;
	if (name.includes(':')) return 'name: must not contain ":" (plugin-scoped identifiers reserve it)';
	if (name.startsWith('-')) return 'name: must not start with "-"';
	return undefined;
}

// The body of a file whose frontmatter did not parse: everything after the closing
// `---` when one is findable, otherwise the whole file.
function fallbackBody(source: string): string {
	const text = source.startsWith('\uFEFF') ? source.slice(1) : source;
	const lines = text.split('\n');
	if (lines[0]?.trim() !== '---') return text.trim();
	for (let index = 1; index < lines.length; index += 1) {
		if (lines[index].trim() === '---') return lines.slice(index + 1).join('\n').trim();
	}
	return text.trim();
}

function fallbackDescription(body: string, name: string): string {
	const line = body
		.split('\n')
		.map(part => part.trim())
		.find(part => part.length > 0);
	return line !== undefined ? line.slice(0, 200) : `Plugin agent ${name}`;
}

function pluginFallbackDefinition(body: string, path: string, options: AgentDefinitionOptions): AgentDefinition {
	const raw = options.fallbackName !== undefined && options.fallbackName.length > 0 ? options.fallbackName : 'agent';
	const name = `${options.scopedPrefix ?? ''}${raw}`;
	const definition: AgentDefinition = {
		name,
		description: fallbackDescription(body, name),
		prompt: body,
		scope: 'plugin',
		sourcePath: path,
	};
	if (options.pluginName !== undefined) definition.plugin = options.pluginName;
	return definition;
}

export function agentDefinitionFromSource(
	source: string,
	path: string,
	scope: AgentScope,
	options: AgentDefinitionOptions = {},
): AgentDefinitionResult {
	const frontmatter = parseFrontmatter(source);
	if (frontmatter.ok === false) {
		if (options.plugin !== true) return frontmatter;
		// A plugin contract differs from a project one: the file still loads under
		// its filename, and the parse failure is reported instead of hidden.
		return {
			ok: true,
			definition: pluginFallbackDefinition(fallbackBody(source), path, options),
			warning: `frontmatter: ${frontmatter.reason}; loaded under its filename`,
		};
	}
	const fields = frontmatter.fields;
	const fallbackDefinition = (warning: string): AgentDefinitionResult => {
		const definition = pluginFallbackDefinition(frontmatter.body, path, options);
		// A usable description in an otherwise unusable file is still worth keeping.
		const parsedDescription = stringField(fields.description);
		if (parsedDescription !== undefined) definition.description = parsedDescription;
		return {ok: true, definition, warning};
	};

	const rawName = stringField(fields.name) ?? '';
	if (rawName.length === 0) {
		if (options.plugin !== true) return {ok: false, reason: 'name: required'};
		return fallbackDefinition('name: required; loaded under its filename');
	}
	const nameError = validateAgentName(rawName);
	if (nameError !== undefined) {
		if (options.plugin !== true) return {ok: false, reason: nameError};
		return fallbackDefinition(`${nameError}; loaded under its filename`);
	}
	const name = options.scopedPrefix !== undefined ? `${options.scopedPrefix}${rawName}` : rawName;
	const description = stringField(fields.description);
	if (description === undefined) return {ok: false, reason: 'description: required'};

	const definition: AgentDefinition = {
		name,
		description,
		prompt: frontmatter.body,
		scope: scope,
		sourcePath: path,
	};
	if (options.pluginName !== undefined) definition.plugin = options.pluginName;
	const tools = listField(fields.tools);
	const disallowed = listField(fields.disallowedTools);
	const skills = listField(fields.skills);
	if (tools !== undefined) definition.tools = tools;
	if (disallowed !== undefined) definition.disallowedTools = disallowed;
	const model = stringField(fields.model);
	const permissionMode = stringField(fields.permissionMode);
	const maxTurns = numberField(fields.maxTurns);
	const isolation = stringField(fields.isolation);
	const color = stringField(fields.color);
	const effort = stringField(fields.effort);
	if (model !== undefined) definition.model = model;
	// A plugin's permissionMode is ignored by contract; the host decides its policy.
	if (permissionMode !== undefined && options.plugin !== true) definition.permissionMode = permissionMode;
	if (maxTurns !== undefined) definition.maxTurns = maxTurns;
	if (skills !== undefined) definition.skills = skills;
	if (isolation !== undefined) definition.isolation = isolation;
	if (color !== undefined) definition.color = color;
	if (effort !== undefined) definition.effort = effort;
	const background = booleanField(fields.background);
	const omitClaudeMd = booleanField(fields.omitClaudeMd);
	const memory = typeof fields.memory === 'string' ? MEMORY_SCOPES[fields.memory] : undefined;
	if (background !== undefined) definition.background = background;
	if (omitClaudeMd !== undefined) definition.omitClaudeMd = omitClaudeMd;
	if (memory !== undefined) definition.memory = memory;
	// A plugin ignores mcpServers, hooks, and initialPrompt by contract: the host
	// owns a plugin's servers, hooks, and first turn. autoCompactWindow and
	// experimental.cacheTtl still apply, because the agent still runs as a subagent.
	if (options.plugin !== true) {
		const mcpServers = parseMcpServers(fields.mcpServers);
		const hooks = parseHooks(fields.hooks);
		const initialPrompt = stringField(fields.initialPrompt);
		if (mcpServers !== undefined) definition.mcpServers = mcpServers;
		if (hooks !== undefined) definition.hooks = hooks;
		if (initialPrompt !== undefined) definition.initialPrompt = initialPrompt;
	}
	const autoCompactWindow = numberField(fields.autoCompactWindow);
	const cacheTtl = parseCacheTtl(fields.experimental);
	if (autoCompactWindow !== undefined) definition.autoCompactWindow = autoCompactWindow;
	if (cacheTtl !== undefined) definition.cacheTtl = cacheTtl;
	return {ok: true, definition};
}

// The T1 built-ins. `general-purpose` is the fallback and `claude` the catch-all.
// `explore` and `plan` mirror the host's reserved agents (read-only, one-shot) but
// live in this registry: the mod resolves them, so the host's copies are never
// shadowed through addAgent.
export const BUILTIN_AGENT_DEFINITIONS: AgentDefinition[] = [
	{
		name: 'explore',
		description: 'Read-only codebase exploration: find files, symbols, and call paths.',
		prompt:
			'You are a read-only codebase explorer. Find the files, symbols, and call paths that answer the question. Do not edit anything. Report concrete paths with line numbers and a short summary of the call flow. Be quick by default; go deeper only when the task needs it.',
		scope: 'builtin',
		sourcePath: '',
		tools: ['Read', 'Grep', 'Glob'],
		model: 'inherit',
		permissionMode: 'plan',
		readOnly: true,
		oneShot: true,
	},
	{
		name: 'plan',
		description: 'Read-only design: turn findings into an implementation plan.',
		prompt:
			'You are a planning agent. Explore the codebase read-only, identify the constraints, then return a concrete implementation plan: the files to change, the ordered steps, the risks, and the tests. Do not implement.',
		scope: 'builtin',
		sourcePath: '',
		tools: ['Read', 'Grep', 'Glob'],
		model: 'inherit',
		permissionMode: 'plan',
		readOnly: true,
		oneShot: true,
	},
	{
		name: 'general-purpose',
		description: 'General-purpose agent for multi-step tasks. The fallback when no subagent_type is given.',
		prompt:
			'You are a general-purpose autonomous agent. You receive one self-contained task, complete it fully, and report back once at the end. Work autonomously: you cannot ask follow-up questions, so make reasonable assumptions and state them in the final report. Use the minimum set of tool calls that gets the job done correctly.',
		scope: 'builtin',
		sourcePath: '',
	},
	{
		name: 'claude',
		description: 'Catch-all coding agent for a task that fits no specialized definition.',
		prompt:
			'You are a general-purpose coding agent. Complete the assigned task end to end, then report what changed and how it was verified.',
		scope: 'builtin',
		sourcePath: '',
	},
	{
		name: 'statusline-setup',
		description: 'Configure the status line. Used by /statusline; restricted to settings files.',
		prompt:
			'You configure the Command Code status line. Read the current settings, change only the statusLine entry, and report the file you edited. Do not run shell commands.',
		scope: 'builtin',
		sourcePath: '',
		model: 'sonnet',
		tools: ['Read', 'Edit'],
	},
	{
		name: 'claude-code-guide',
		description: "Answer questions about the host's features, commands, and configuration.",
		prompt:
			'You answer questions about Command Code: agents, tools, hooks, permissions, MCP, and settings. Read the local documentation first, then the official pages when the local copy is silent. Answer with the exact flag or field name.',
		scope: 'builtin',
		sourcePath: '',
		model: 'haiku',
		tools: ['Read', 'Grep', 'Glob', 'WebFetch', 'WebSearch'],
	},
	{
		name: FORK_AGENT_TYPE,
		description:
			'Fork the current conversation: full history, the main system prompt, tools, model, permissions, and prompt cache. Use it when the task needs the context already in this session.',
		prompt: '',
		scope: 'builtin',
		sourcePath: '',
		fork: true,
	},
];

export function builtinAgentDefinitions(env: Record<string, string | undefined>): AgentDefinition[] {
	if (
		env.CLAUDE_AGENT_SDK_DISABLE_BUILTIN_AGENTS === '1' ||
		env.BETTER_SUBAGENTS_DISABLE_BUILTIN_AGENTS === '1' ||
		env.BETTER_SUBAGENTS_DISABLE_BUILTINS === '1'
	) {
		return [];
	}
	if (env.CLAUDE_CODE_DISABLE_EXPLORE_PLAN_AGENTS === '1' || env.BETTER_SUBAGENTS_DISABLE_EXPLORE_PLAN === '1') {
		return BUILTIN_AGENT_DEFINITIONS.filter(definition => definition.name !== 'explore' && definition.name !== 'plan');
	}
	return [...BUILTIN_AGENT_DEFINITIONS];
}

// The description limit covers the descriptions the model reads to choose an
// agent, not the bodies, which load only when the agent runs. There is no
// tokenizer in the mod, so four characters per token is the documented estimate;
// the warning names both the estimate and the threshold so a user can see why it
// fired. Built-ins are excluded: the user cannot shorten them.
export const DESCRIPTION_LIMIT_TOKENS = 15000;

export function descriptionLimit(
	definitions: readonly {readonly description?: string; readonly scope?: string}[],
	maxTokens = DESCRIPTION_LIMIT_TOKENS,
): {tokens: number; maxTokens: number; over: boolean} {
	let characters = 0;
	for (const definition of definitions) {
		if (definition.scope === 'builtin') continue;
		characters += String(definition.description ?? '').length;
	}
	const tokens = Math.ceil(characters / 4);
	return {tokens, maxTokens, over: tokens > maxTokens};
}

// A CLI definition is the same shape as frontmatter, in JSON. `color` and
// `experimental` are ignored there by contract; the object key is the name, so an
// inline `{reviewer: {...}}` and an array of `{name: ...}` both work.
function cliAgentDefinition(
	rawName: string,
	spec: Record<string, unknown>,
	sourcePath: string,
): {ok: true; definition: AgentDefinition} | {ok: false; reason: string} {
	const name = rawName.length > 0 ? rawName : (stringField(spec.name) ?? '');
	if (name.length === 0) return {ok: false, reason: 'name: required'};
	const nameError = validateAgentName(name);
	if (nameError !== undefined) return {ok: false, reason: nameError};
	const description = stringField(spec.description);
	if (description === undefined) return {ok: false, reason: 'description: required'};
	const prompt = typeof spec.prompt === 'string' ? spec.prompt : typeof spec.systemPrompt === 'string' ? spec.systemPrompt : '';

	const definition: AgentDefinition = {name, description, prompt, scope: 'cli', sourcePath};
	const tools = listField(spec.tools);
	const disallowed = listField(spec.disallowedTools);
	const skills = listField(spec.skills);
	if (tools !== undefined) definition.tools = tools;
	if (disallowed !== undefined) definition.disallowedTools = disallowed;
	if (skills !== undefined) definition.skills = skills;
	const model = stringField(spec.model);
	const permissionMode = stringField(spec.permissionMode);
	const maxTurns = numberField(spec.maxTurns);
	const isolation = stringField(spec.isolation);
	const effort = stringField(spec.effort);
	if (model !== undefined) definition.model = model;
	if (permissionMode !== undefined) definition.permissionMode = permissionMode;
	if (maxTurns !== undefined) definition.maxTurns = maxTurns;
	if (isolation !== undefined) definition.isolation = isolation;
	if (effort !== undefined) definition.effort = effort;
	const background = booleanField(spec.background);
	const omitClaudeMd = booleanField(spec.omitClaudeMd);
	const memory = typeof spec.memory === 'string' ? MEMORY_SCOPES[spec.memory] : undefined;
	if (background !== undefined) definition.background = background;
	if (omitClaudeMd !== undefined) definition.omitClaudeMd = omitClaudeMd;
	if (memory !== undefined) definition.memory = memory;
	const mcpServers = parseMcpServers(spec.mcpServers);
	const hooks = parseHooks(spec.hooks);
	const initialPrompt = stringField(spec.initialPrompt);
	const autoCompactWindow = numberField(spec.autoCompactWindow);
	if (mcpServers !== undefined) definition.mcpServers = mcpServers;
	if (hooks !== undefined) definition.hooks = hooks;
	if (initialPrompt !== undefined) definition.initialPrompt = initialPrompt;
	if (autoCompactWindow !== undefined) definition.autoCompactWindow = autoCompactWindow;
	// `color` and `experimental` are ignored for a CLI definition by contract.
	return {ok: true, definition};
}

// `.git` is a directory in a checkout and a file in a linked worktree. `exists`
// only sees directories, so a worktree root would otherwise be walked past. The
// registry and the startup walk share this stop rule.
export function hasGitMarker(fs: Pick<SpoolFs, 'stat'>, dir: string): boolean {
	try {
		fs.stat(join(dir, '.git'));
		return true;
	} catch {
		return false;
	}
}

export function createAgentRegistry(deps: AgentRegistryDeps): AgentRegistry {
	let entries: AgentEntry[] = [];
	// Each (file, problem) is logged once per process, so a refresh from `/agents`
	// cannot flood the debug log with the same skip.
	const loggedSkips = new Set<string>();
	// A plugin file that loaded under its filename still carries its problem, which
	// `/doctor` reports. Keyed by the definition so a precedence skip does not lose it.
	const definitionWarnings = new WeakMap<AgentDefinition, string>();

	const managedDir = (): string => deps.managedDir ?? join(deps.home, '.commandcode', 'managed');

	function pushEntry(entry: AgentEntry): void {
		entries.push(entry);
		const problem = entry.warning ?? entry.reason;
		if (problem === undefined) return;
		const key = `${entry.sourcePath}\u0000${problem}`;
		if (loggedSkips.has(key)) return;
		loggedSkips.add(key);
		deps.log(`${entry.sourcePath.length > 0 ? entry.sourcePath : entry.name}: ${problem}`);
	}

	function scanFile(path: string, scope: AgentScope, options: AgentDefinitionOptions = {}): AgentDefinition | undefined {
		let source = '';
		try {
			source = deps.fs.readFile(path);
		} catch (error) {
			pushEntry({
				name: '',
				scope: scope,
				sourcePath: path,
				valid: false,
				kind: 'invalid',
				reason: `unreadable: ${String((error as Error)?.message ?? error)}`,
			});
			return undefined;
		}
		const parsed = agentDefinitionFromSource(source, path, scope, options);
		if (parsed.ok === false) {
			pushEntry({name: '', scope, sourcePath: path, valid: false, kind: 'invalid', reason: parsed.reason});
			return undefined;
		}
		if (parsed.warning !== undefined) definitionWarnings.set(parsed.definition, parsed.warning);
		return parsed.definition;
	}

	function pushDuplicate(name: string, scope: AgentScope, path: string, kept: string): void {
		pushEntry({
			name,
			scope: scope,
			sourcePath: path,
			valid: false,
			kind: 'duplicate',
			duplicateOf: kept,
			reason: `duplicate name "${name}": ${kept} was read first; rename or remove this file`,
		});
	}

	// Recursive scan. Subdirectories never change a project or user identity: the
	// name comes from the frontmatter only. Bounded by depth and entry count. A
	// duplicate is an error only within one agents root; a farther root or a built-in
	// with the same name is ordinary precedence and is skipped silently.
	function scanDir(
		dir: string,
		scope: AgentScope,
		depth: number,
		found: AgentDefinition[],
		localNames: Map<string, string>,
	): void {
		if (depth > AGENT_SCAN_DEPTH_MAX || entries.length >= AGENT_SCAN_MAX || !deps.fs.exists(dir)) return;
		let names: string[] = [];
		try {
			names = deps.fs.readdir(dir).sort();
		} catch {
			return;
		}
		for (const name of names) {
			if (entries.length >= AGENT_SCAN_MAX) return;
			const full = join(dir, name);
			if (name.toLowerCase().endsWith('.md')) {
				const definition = scanFile(full, scope);
				if (definition === undefined) continue;
				const kept = localNames.get(definition.name);
				if (kept !== undefined) {
					pushDuplicate(definition.name, scope, full, kept);
					continue;
				}
				localNames.set(definition.name, full);
				found.push(definition);
				continue;
			}
			if (deps.fs.exists(full)) scanDir(full, scope, depth + 1, found, localNames);
		}
	}

	// A plugin's subdirectory is part of its identifier, so it is scanned with the
	// trail that builds `plugin:review:security`. Unlike a project, the plugin file
	// contract loads a nameless or unparseable file under its filename.
	function scanPluginDir(
		dir: string,
		pluginName: string,
		trail: string[],
		depth: number,
		found: AgentDefinition[],
		localNames: Map<string, string>,
	): void {
		if (depth > AGENT_SCAN_DEPTH_MAX || entries.length >= AGENT_SCAN_MAX || !deps.fs.exists(dir)) return;
		let children: string[] = [];
		try {
			children = deps.fs.readdir(dir).sort();
		} catch {
			return;
		}
		for (const child of children) {
			if (entries.length >= AGENT_SCAN_MAX) return;
			const full = join(dir, child);
			if (child.toLowerCase().endsWith('.md')) {
				const definition = scanFile(full, 'plugin', {
					plugin: true,
					pluginName,
					scopedPrefix: `${[pluginName, ...trail].join(':')}:`,
					fallbackName: child.replace(/\.md$/i, ''),
				});
				if (definition === undefined) continue;
				const kept = localNames.get(definition.name);
				if (kept !== undefined) {
					pushDuplicate(definition.name, 'plugin', full, kept);
					continue;
				}
				localNames.set(definition.name, full);
				found.push(definition);
				continue;
			}
			scanPluginDir(full, pluginName, [...trail, child], depth + 1, found, localNames);
		}
	}

	function scanPlugins(root: string, found: AgentDefinition[]): void {
		if (!deps.fs.exists(root)) return;
		let pluginNames: string[] = [];
		try {
			pluginNames = deps.fs.readdir(root).sort();
		} catch {
			return;
		}
		for (const pluginName of pluginNames) {
			// Presence under a plugin root is enablement; the host owns the plugin
			// manager. Only plugins that ship agents are scanned.
			const agentsDir = join(root, pluginName, 'agents');
			if (deps.fs.exists(agentsDir)) scanPluginDir(agentsDir, pluginName, [], 0, found, new Map());
		}
	}

	// The project bases from the working directory upward, closest first. Walk-up
	// stops at the repository root (the directory holding .git) or the filesystem
	// root, so a nested `.commandcode/agents` closest to the cwd wins.
	function projectBases(): string[] {
		const bases: string[] = [];
		let current = deps.cwd;
		for (let depth = 0; depth <= 64; depth += 1) {
			bases.push(current);
			if (hasGitMarker(deps.fs, current)) break;
			const parent = dirname(current);
			if (parent === current) break;
			current = parent;
		}
		return bases;
	}

	function pluginRoots(): string[] {
		const roots = [...(deps.pluginRoots ?? [])];
		roots.push(join(managedDir(), 'plugins'));
		for (const base of projectBases()) roots.push(join(base, '.commandcode', 'plugins'));
		roots.push(join(deps.home, '.commandcode', 'plugins'));
		return roots;
	}

	// `--mod-option agents=<inline JSON | JSON file path>`. A file is refused in an
	// interactive session: a definition a human cannot see at launch cannot be
	// reviewed, so the session must pass inline JSON instead.
	function scanCli(found: AgentDefinition[]): void {
		const value = (deps.cli?.value ?? '').trim();
		if (value.length === 0) return;
		let text = value;
		let sourcePath = 'cli';
		if (!value.startsWith('{') && !value.startsWith('[')) {
			const file = value.startsWith('@') ? value.slice(1) : value;
			if (deps.cli?.interactive === true) {
				pushEntry({
					name: '',
					scope: 'cli',
					sourcePath: file,
					valid: false,
					kind: 'invalid',
					reason: 'an interactive session cannot load --mod-option agents from a file; pass inline JSON',
				});
				return;
			}
			try {
				text = deps.fs.readFile(file);
			} catch (error) {
				pushEntry({
					name: '',
					scope: 'cli',
					sourcePath: file,
					valid: false,
					kind: 'invalid',
					reason: `unreadable: ${String((error as Error)?.message ?? error)}`,
				});
				return;
			}
			sourcePath = file;
		}
		let parsed: unknown;
		try {
			parsed = JSON.parse(text);
		} catch (error) {
			pushEntry({
				name: '',
				scope: 'cli',
				sourcePath: sourcePath,
				valid: false,
				kind: 'invalid',
				reason: `agents JSON: ${String((error as Error)?.message ?? error)}`,
			});
			return;
		}
		const specs: {name: string; spec: Record<string, unknown>}[] = [];
		if (Array.isArray(parsed)) {
			for (const item of parsed) {
				if (item === null || typeof item !== 'object' || Array.isArray(item)) {
					pushEntry({
						name: '',
						scope: 'cli',
						sourcePath: sourcePath,
						valid: false,
						kind: 'invalid',
						reason: 'agents JSON: every array item must be an object',
					});
					continue;
				}
				const record = item as Record<string, unknown>;
				specs.push({name: typeof record.name === 'string' ? record.name : '', spec: record});
			}
		} else if (parsed !== null && typeof parsed === 'object') {
			for (const [name, item] of Object.entries(parsed)) {
				if (item === null || typeof item !== 'object' || Array.isArray(item)) {
					pushEntry({
						name,
						scope: 'cli',
						sourcePath: sourcePath,
						valid: false,
						kind: 'invalid',
						reason: 'agents JSON: every definition must be an object',
					});
					continue;
				}
				specs.push({name, spec: item as Record<string, unknown>});
			}
		} else {
			pushEntry({
				name: '',
				scope: 'cli',
				sourcePath: sourcePath,
				valid: false,
				kind: 'invalid',
				reason: 'agents JSON: expected an object of definitions or an array',
			});
			return;
		}

		const localNames = new Map<string, string>();
		for (const {name, spec} of specs) {
			const parsedDefinition = cliAgentDefinition(name, spec, sourcePath);
			if (parsedDefinition.ok === false) {
				pushEntry({
					name,
					scope: 'cli',
					sourcePath: sourcePath,
					valid: false,
					kind: 'invalid',
					reason: parsedDefinition.reason,
				});
				continue;
			}
			const kept = localNames.get(parsedDefinition.definition.name);
			if (kept !== undefined) {
				pushDuplicate(parsedDefinition.definition.name, 'cli', sourcePath, kept);
				continue;
			}
			localNames.set(parsedDefinition.definition.name, sourcePath);
			found.push(parsedDefinition.definition);
		}
	}

	function refresh(): void {
		entries = [];
		const found: AgentDefinition[] = [];
		// Precedence order, highest first: a managed definition wins over CLI, CLI over
		// project, project over user, user over plugin, and every one over a built-in.
		scanDir(join(managedDir(), 'agents'), 'managed', 0, found, new Map());
		scanCli(found);
		for (const base of projectBases()) {
			scanDir(join(base, '.commandcode', 'agents'), 'project', 0, found, new Map());
		}
		for (const dir of deps.addDirs ?? []) {
			scanDir(join(dir, '.commandcode', 'agents'), 'project', 0, found, new Map());
		}
		scanDir(join(deps.home, '.commandcode', 'agents'), 'user', 0, found, new Map());
		for (const root of pluginRoots()) scanPlugins(root, found);
		found.push(...deps.builtins);

		// Precedence, not an error: the first loaded definition of a name wins. A
		// duplicate inside one agents root was already reported by scanDir.
		const seen = new Set<string>();
		for (const definition of found) {
			if (seen.has(definition.name)) continue;
			seen.add(definition.name);
			const warning = definitionWarnings.get(definition);
			entries.push({
				name: definition.name,
				scope: definition.scope,
				sourcePath: definition.sourcePath ?? '',
				valid: true,
				kind: 'valid',
				...(warning !== undefined ? {warning} : {}),
				definition,
			});
		}
	}

	refresh();
	return {
		entries: () => [...entries],
		list: () => entries.flatMap(entry => (entry.valid && entry.definition !== undefined ? [entry.definition] : [])),
		get: name => entries.find(entry => entry.valid && entry.definition?.name === name)?.definition,
		refresh,
	};
}
