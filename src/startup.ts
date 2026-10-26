// The startup bundle a subagent receives: definition body, environment, task,
// CLAUDE.md/AGENTS.md hierarchy, git status snapshot, named skills, roster,
// output style, and the agent's own MEMORY.md. Also owns the rule for how much
// of a memory index is read and how a model's context window is resolved.

import {dirname, isAbsolute, join} from 'node:path';
import {contextWindowOverride, policyFiles} from './config.ts';
import {hasGitMarker} from './definitions.ts';
import type {AgentMemoryScope} from './definitions.ts';
import type {Runtime, SpoolFs} from './types.ts';

// ---------------------------------------------------------------------------
// Startup context and agent memory
//
// What a subagent sees at startup is a bundle, not the host's full prompt: the
// definition body, the environment, the task, the CLAUDE.md/AGENTS.md hierarchy,
// a git status snapshot, the full content of the named skills, a sibling roster,
// the output style, and the agent's own MEMORY.md. The bundle is built in the
// parent from the injected filesystem, so the omit rules live in one function
// instead of in conditionals spread through spawn. A subagent never inherits the
// parent's auto memory; `memory` gives it a directory and a MEMORY.md of its own.
// ---------------------------------------------------------------------------

const MEMORY_INDEX_FILE = 'MEMORY.md';
const MEMORY_INDEX_LINES_MAX = 200;
const MEMORY_INDEX_BYTES_MAX = 25 * 1024;
const GIT_STATUS_MAX_CHARS = 2000;
export const STARTUP_WALK_DEPTH_MAX = 8;
// The window the ACP session is told it has. A family alias resolves to its
// documented window; an explicit override wins, and an unknown model gets the
// common default rather than an unbounded number.
const CONTEXT_WINDOW_DEFAULT = 200000;
const CONTEXT_WINDOW_MAX = 10 * 1000 * 1000;
const CONTEXT_WINDOW_FAMILIES: ReadonlyArray<readonly [RegExp, number]> = [
	[/\[1m\]|-1m\b/i, 1000000],
	[/opus|sonnet|haiku|fable/i, 200000],
];
const EXPLORE_PLAN_NAMES: Readonly<Record<string, true>> = {explore: true, plan: true};

export interface StartupFile {
	readonly path: string;
	readonly content: string;
}

export interface StartupSkill extends StartupFile {
	readonly name: string;
}

export interface StartupMemory {
	readonly scope: AgentMemoryScope;
	readonly dir: string;
	readonly path: string;
	readonly content: string;
	readonly lines: number;
	readonly truncated: boolean;
}

export interface StartupBundle {
	readonly systemPrompt: string;
	readonly environmentText: string;
	readonly task: string;
	readonly claudeMd: StartupFile[];
	readonly agentsMd: StartupFile[];
	readonly gitStatus?: string;
	readonly skills: StartupSkill[];
	readonly roster?: string[];
	readonly outputStyle?: string;
	readonly memory?: StartupMemory;
	readonly contextWindow: number;
	/** A subagent never inherits the parent's auto memory. */
	readonly parentMemory: false;
	/** The rendered context the worker receives. */
	readonly text: string;
}

/** The definition fields startup reads. Structural, so AgentDefinition fits. */
export interface StartupDefinition {
	readonly name?: string;
	readonly prompt?: string;
	readonly skills?: readonly string[];
	readonly memory?: AgentMemoryScope;
	readonly omitClaudeMd?: boolean;
	/** `managed` skips the managed policy, matching the spec's managed agents. */
	readonly scope?: string;
	readonly outputStyle?: string;
}

export interface StartupOptions {
	readonly cwd: string;
	readonly home: string;
	readonly fs: SpoolFs;
	readonly env?: Record<string, string | undefined>;
	readonly definition?: StartupDefinition;
	readonly agentType?: string;
	readonly prompt?: string;
	readonly model?: string;
	readonly outputStyle?: string;
	readonly roster?: readonly string[];
	/** Files the agent works with; their subdirectory CLAUDE.md still loads. */
	readonly paths?: readonly string[];
	/** A snapshot already taken by the caller, or undefined for no snapshot. */
	readonly gitStatus?: string;
	readonly now?: number;
	readonly platform?: string;
}

function readStartupFile(fs: SpoolFs, path: string): string | undefined {
	try {
		return fs.readFile(path);
	} catch {
		return undefined;
	}
}

function startupFile(fs: SpoolFs, path: string): StartupFile | undefined {
	const content = readStartupFile(fs, path);
	return content === undefined ? undefined : {path, content};
}

// Project discovery walks up from the working directory to the repository root and
// renders outermost first, so broad instructions come before the nearest ones.
function projectDirsUpward(cwd: string, fs?: Pick<SpoolFs, 'stat'>): string[] {
	const dirs: string[] = [];
	let current = cwd;
	for (let depth = 0; depth < STARTUP_WALK_DEPTH_MAX; depth += 1) {
		dirs.push(current);
		if (fs !== undefined && hasGitMarker(fs, current)) break;
		const parent = dirname(current);
		if (parent === current) break;
		current = parent;
	}
	return dirs.reverse();
}

// A path-scoped instruction file is loaded even when the hierarchy is omitted:
// the agent is working with files in that directory, so the rule applies.
function subdirectoryClaudeMd(fs: SpoolFs, cwd: string, paths: readonly string[]): StartupFile[] {
	const seen = new Set<string>();
	const files: StartupFile[] = [];
	for (const path of paths) {
		const absolute = isAbsolute(path) ? path : join(cwd, path);
		let dir = dirname(absolute);
		for (let depth = 0; depth < STARTUP_WALK_DEPTH_MAX; depth += 1) {
			if (dir === cwd || dir === dirname(dir) || !dir.startsWith(cwd)) break;
			const filePath = join(dir, 'CLAUDE.md');
			if (seen.has(filePath)) break;
			seen.add(filePath);
			const file = startupFile(fs, filePath);
			if (file !== undefined) files.push(file);
			dir = dirname(dir);
		}
	}
	return files;
}

export function claudeMdHierarchy(options: StartupOptions): {claudeMd: StartupFile[]; agentsMd: StartupFile[]} {
	const env = options.env ?? {};
	const fs = options.fs;
	const definition = options.definition;
	const agentType = (options.agentType ?? definition?.name ?? '').toLowerCase();
	const claudeMd: StartupFile[] = [];
	const agentsMd: StartupFile[] = [];
	// Explore and Plan start without project instructions, but a path-scoped
	// CLAUDE.md still loads because the agent works with files there.
	const skipHierarchy = EXPLORE_PLAN_NAMES[agentType] === true;
	const policy = policyFiles(env, options.home);
	// Managed policy loads even under omitClaudeMd, except for a managed agent.
	if (!skipHierarchy && definition?.scope !== 'managed') {
		const managed = startupFile(fs, policy.managed);
		if (managed !== undefined) claudeMd.push(managed);
	}
	if (!skipHierarchy && definition?.omitClaudeMd !== true) {
		const user = startupFile(fs, policy.user);
		if (user !== undefined) claudeMd.push(user);
		for (const dir of projectDirsUpward(options.cwd, options.fs)) {
			const project = startupFile(fs, join(dir, 'CLAUDE.md'));
			if (project !== undefined) claudeMd.push(project);
			const local = startupFile(fs, join(dir, 'CLAUDE.local.md'));
			if (local !== undefined) claudeMd.push(local);
		}
		// AGENTS.md is project instructions too, so it follows the same omit rule.
		for (const dir of projectDirsUpward(options.cwd, options.fs)) {
			const agents = startupFile(fs, join(dir, 'AGENTS.md'));
			if (agents !== undefined) agentsMd.push(agents);
		}
	}
	claudeMd.push(...subdirectoryClaudeMd(fs, options.cwd, options.paths ?? []));
	return {claudeMd, agentsMd};
}

export function skillFileFor(
	name: string,
	options: {cwd: string; home: string; fs: SpoolFs},
): string | undefined {
	if (name.length === 0) return undefined;
	const candidates: string[] = [];
	for (const dir of projectDirsUpward(options.cwd, options.fs)) {
		candidates.push(join(dir, '.commandcode', 'skills', name, 'SKILL.md'));
		candidates.push(join(dir, '.claude', 'skills', name, 'SKILL.md'));
	}
	candidates.push(join(options.home, '.commandcode', 'skills', name, 'SKILL.md'));
	candidates.push(join(options.home, '.claude', 'skills', name, 'SKILL.md'));
	for (const candidate of candidates) {
		if (readStartupFile(options.fs, candidate) !== undefined) return candidate;
	}
	return undefined;
}

export function memoryDirFor(scope: AgentMemoryScope, name: string, options: {cwd: string; home: string}): string {
	const root =
		scope === 'user'
			? join(options.home, '.commandcode', 'agent-memory')
			: scope === 'project'
				? join(options.cwd, '.commandcode', 'agent-memory')
				: join(options.cwd, '.commandcode', 'agent-memory-local');
	return join(root, name);
}

// The first 200 lines or 25KB of MEMORY.md, whichever comes first. Bytes, not
// UTF-16 code units: the bound is on what is injected into the context.
export function readMemoryIndex(
	path: string,
	fs: SpoolFs,
): {content: string; lines: number; truncated: boolean} {
	let source: string;
	try {
		source = fs.readFile(path);
	} catch {
		return {content: '', lines: 0, truncated: false};
	}
	const allLines = source.split('\n');
	// A trailing newline is not a line of content, so a 200-line file that ends
	// with one is not reported as truncated.
	if (allLines.length > 0 && allLines[allLines.length - 1] === '') allLines.pop();
	const lineBound = allLines.slice(0, MEMORY_INDEX_LINES_MAX).join('\n');
	let content = lineBound;
	let truncatedByBytes = false;
	if (Buffer.byteLength(lineBound, 'utf8') > MEMORY_INDEX_BYTES_MAX) {
		const buffer = Buffer.from(lineBound, 'utf8').subarray(0, MEMORY_INDEX_BYTES_MAX);
		content = buffer.toString('utf8').replace(/\uFFFD$/, '');
		truncatedByBytes = true;
	}
	const truncatedByLines = allLines.length > MEMORY_INDEX_LINES_MAX;
	return {
		content,
		lines: content.length === 0 ? 0 : content.split('\n').length,
		truncated: truncatedByLines || truncatedByBytes,
	};
}

// MEMORY.md is the agent's own memory. The parent's auto memory never follows a
// non-fork subagent, so this is the only memory that reaches the bundle.
function startupMemory(options: StartupOptions): StartupMemory | undefined {
	const scope = options.definition?.memory;
	const name = options.definition?.name;
	if (scope === undefined || name === undefined || name.length === 0) return undefined;
	const dir = memoryDirFor(scope, name, {cwd: options.cwd, home: options.home});
	try {
		options.fs.mkdir(dir);
	} catch {
		// A read-only location must not fail the spawn: the agent still starts.
	}
	const path = join(dir, MEMORY_INDEX_FILE);
	const index = readMemoryIndex(path, options.fs);
	return {scope, dir, path, content: index.content, lines: index.lines, truncated: index.truncated};
}

export function contextWindowForModel(
	model: string | undefined,
	env: Record<string, string | undefined> = {},
): number {
	const override = contextWindowOverride(env);
	if (override !== undefined && override >= 1000) return Math.min(CONTEXT_WINDOW_MAX, Math.round(override));
	const value = (model ?? '').toLowerCase();
	for (const [pattern, window] of CONTEXT_WINDOW_FAMILIES) {
		if (pattern.test(value)) return window;
	}
	return CONTEXT_WINDOW_DEFAULT;
}

// A memory definition must be able to read and write its own MEMORY.md.
export function withMemoryTools(
	tools: readonly string[] | undefined,
	memory: AgentMemoryScope | undefined,
): string[] | undefined {
	if (memory === undefined) return tools === undefined ? undefined : [...tools];
	const result = tools === undefined ? [] : [...tools];
	for (const tool of ['Read', 'Write', 'Edit']) if (!result.includes(tool)) result.push(tool);
	return result;
}

export function gitStatusSnapshot(runtime: Pick<Runtime, 'run'>, cwd: string): string | undefined {
	const run = runtime.run;
	if (typeof run !== 'function') return undefined;
	try {
		const result = run('git', ['status', '--short', '--branch'], {cwd});
		return result.ok ? result.stdout : undefined;
	} catch {
		return undefined;
	}
}

function trimGitStatus(status: string | undefined): string | undefined {
	if (status === undefined) return undefined;
	const text = status.replace(/\s+$/, '');
	if (text.length === 0) return undefined;
	return text.length > GIT_STATUS_MAX_CHARS ? `${text.slice(0, GIT_STATUS_MAX_CHARS)}\n(truncated)` : text;
}

function renderStartup(bundle: Omit<StartupBundle, 'text'>): string {
	const sections: string[] = [bundle.environmentText];
	if (bundle.claudeMd.length > 0) {
		const files = bundle.claudeMd.map(file => `## ${file.path}\n${file.content}`);
		sections.push(['# Project instructions (CLAUDE.md)', ...files].join('\n\n'));
	}
	if (bundle.agentsMd.length > 0) {
		const files = bundle.agentsMd.map(file => `## ${file.path}\n${file.content}`);
		sections.push(['# AGENTS.md', ...files].join('\n\n'));
	}
	if (bundle.gitStatus !== undefined) sections.push(`# Git status\n${bundle.gitStatus}`);
	if (bundle.skills.length > 0) {
		const skills = bundle.skills.map(skill => `## ${skill.name} (${skill.path})\n${skill.content}`);
		sections.push(['# Skills (full content)', ...skills].join('\n\n'));
	}
	if (bundle.roster !== undefined && bundle.roster.length > 0) {
		sections.push(`# Sibling agents\n${bundle.roster.map(name => `- ${name}`).join('\n')}`);
	}
	if (bundle.outputStyle !== undefined) sections.push(`# Output style\n${bundle.outputStyle}`);
	if (bundle.memory !== undefined) {
		const bound = bundle.memory.truncated ? ' (truncated to the first 200 lines or 25KB)' : '';
		sections.push(`# Agent memory\n${bundle.memory.path}${bound}\n${bundle.memory.content}`);
	}
	return sections.join('\n\n');
}

export function buildStartupBundle(options: StartupOptions): StartupBundle {
	const env = options.env ?? {};
	const definition = options.definition;
	const agentType = (options.agentType ?? definition?.name ?? '').toLowerCase();
	const contextWindow = contextWindowForModel(options.model, env);
	const {claudeMd, agentsMd} = claudeMdHierarchy(options);
	// Explore and Plan skip the git status snapshot along with the instructions.
	const gitStatus = EXPLORE_PLAN_NAMES[agentType] === true ? undefined : trimGitStatus(options.gitStatus);
	// The full content of every named skill; an unlisted skill stays invocable
	// through the Skill tool and is not preloaded here.
	const skills: StartupSkill[] = [];
	for (const name of definition?.skills ?? []) {
		const path = skillFileFor(name, options);
		const content = path === undefined ? undefined : readStartupFile(options.fs, path);
		if (path !== undefined && content !== undefined) skills.push({name, path, content});
	}
	const roster = options.roster === undefined ? undefined : [...options.roster];
	const outputStyle = options.outputStyle ?? definition?.outputStyle;
	const memory = startupMemory(options);
	const environment = [
		'# Environment',
		`working directory: ${options.cwd}`,
		`platform: ${options.platform ?? 'unknown'}`,
	];
	// The date is informational and only rendered when the caller supplied the
	// seam's timestamp; nothing here reads the OS clock.
	if (options.now !== undefined) environment.push(`date: ${new Date(options.now).toISOString().slice(0, 10)}`);
	if (options.model !== undefined) environment.push(`model: ${options.model}`);
	environment.push(`context window: ${contextWindow} tokens`);
	const base: Omit<StartupBundle, 'text'> = {
		systemPrompt: definition?.prompt ?? '',
		environmentText: environment.join('\n'),
		task: options.prompt ?? '',
		claudeMd,
		agentsMd,
		...(gitStatus !== undefined ? {gitStatus} : {}),
		skills,
		...(roster !== undefined ? {roster} : {}),
		...(outputStyle !== undefined ? {outputStyle} : {}),
		...(memory !== undefined ? {memory} : {}),
		contextWindow,
		parentMemory: false,
	};
	return {...base, text: renderStartup(base)};
}
