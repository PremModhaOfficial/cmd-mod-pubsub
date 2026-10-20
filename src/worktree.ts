// ---------------------------------------------------------------------------
// Worktree isolation
//
// `isolation: worktree` runs one job in its own `git worktree`, branched from the
// repository's default branch - never the caller's HEAD. Creation and cleanup are
// parent-owned and every git call goes through the runtime seam, so a fake git
// makes the whole lifecycle drivable. Finalizing is idempotent: a drain, an
// onStop claim, and a restart reconcile can each ask without removing twice.
// ---------------------------------------------------------------------------

import {dirname, resolve, sep} from 'node:path';
import type {Runtime} from './types.ts';

export interface GitResult {
	readonly code: number;
	readonly stdout: string;
	readonly stderr: string;
}

/** The git half of the runtime seam. */
export interface GitSeam {
	git(args: string[], cwd: string): GitResult;
}

/**
 * git over the runtime's one bounded `run`. The worktree code speaks GitResult, so
 * the OS call and its fake stay one seam.
 */
export function gitSeam(runtime: Pick<Runtime, 'run'>): GitSeam {
	return {
		git(args, cwd) {
			const reply = runtime.run?.('git', args, {cwd, timeoutMs: 30_000});
			if (reply === undefined) return {code: 1, stdout: '', stderr: 'git is not available'};
			return {
				code: typeof reply.code === 'number' ? reply.code : reply.ok ? 0 : 1,
				stdout: String(reply.stdout ?? ''),
				stderr: String(reply.stderr ?? ''),
			};
		},
	};
}

/** The filesystem calls creation needs. */
export interface WorktreeSeam extends GitSeam {
	readonly fs: {mkdir(dir: string): void};
}

export type IsolationResult =
	| {readonly ok: true; readonly path: string; readonly base: string; readonly head?: string; readonly mainCheckouts: string[]}
	| {readonly ok: false; readonly error: string};

export interface WorktreeDecision {
	readonly kept: boolean;
	readonly path: string;
	readonly error?: string;
}

export interface IsolationRoots {
	/** The checkout a command must stay inside. */
	readonly worktree: string;
	/** Checkouts a command must never enter: the launch repo, plus the main checkout. */
	readonly mainCheckouts: readonly string[];
}

export interface IsolationSpec {
	readonly worktree?: string;
	readonly mainCheckouts?: string[];
}

export function isInside(parent: string, child: string): boolean {
	const root = resolve(parent);
	const target = resolve(child);
	return target === root || target.startsWith(root.endsWith(sep) ? root : root + sep);
}

function firstLine(value: string): string {
	return value.trim().split('\n')[0].trim();
}

// The repository containing `cwd`, and the main checkout when `cwd` is a linked
// worktree. Both are off limits to a command: `git worktree` puts a linked
// worktree's `.git` file under the main checkout's common dir, which is how the
// second root is discovered.
export function mainCheckoutRoots(seam: GitSeam, cwd: string): string[] {
	const top = seam.git(['rev-parse', '--show-toplevel'], cwd);
	if (top.code !== 0) return [];
	const topLevel = firstLine(top.stdout);
	if (topLevel.length === 0) return [];
	const roots = [topLevel];
	const common = seam.git(['rev-parse', '--path-format=absolute', '--git-common-dir'], topLevel);
	// Older git has no --path-format; the plain answer is relative to the top level.
	let commonDir = '';
	if (common.code === 0) {
		commonDir = firstLine(common.stdout);
	} else {
		const plain = seam.git(['rev-parse', '--git-common-dir'], topLevel);
		if (plain.code === 0) commonDir = resolve(topLevel, firstLine(plain.stdout));
	}
	// `<top>/.git` is the main checkout's own directory. Anything else is a linked
	// worktree's file; the main checkout is the common dir's parent.
	if (commonDir.length > 0 && !isInside(topLevel, commonDir)) roots.push(dirname(commonDir));
	return roots;
}

// The default branch, never the caller's HEAD: origin's HEAD first, then the two
// conventional local names. Absent all three, isolation fails rather than quietly
// branching from wherever the parent happened to be.
export function defaultBranch(seam: GitSeam, cwd: string): string | undefined {
	const candidates: string[] = [];
	const head = seam.git(['symbolic-ref', '--short', 'refs/remotes/origin/HEAD'], cwd);
	if (head.code === 0) {
		const name = firstLine(head.stdout);
		if (name.length > 0) candidates.push(name);
	}
	candidates.push('main', 'master');
	for (const candidate of candidates) {
		if (seam.git(['rev-parse', '--verify', '--quiet', candidate], cwd).code === 0) return candidate;
	}
	return undefined;
}

export function createJobWorktree(seam: WorktreeSeam, opts: {cwd: string; path: string}): IsolationResult {
	const roots = mainCheckoutRoots(seam, opts.cwd);
	if (roots.length === 0) {
		return {ok: false, error: 'isolation: worktree needs a git repository (git rev-parse --show-toplevel failed)'};
	}
	const base = defaultBranch(seam, opts.cwd);
	if (base === undefined) {
		return {
			ok: false,
			error: 'isolation: worktree cannot resolve the default branch (no origin/HEAD, main, or master)',
		};
	}
	try {
		seam.fs.mkdir(dirname(opts.path));
	} catch (error) {
		const message = String((error as Error)?.message ?? error);
		return {ok: false, error: `isolation: worktree cannot create ${dirname(opts.path)}: ${message}`};
	}
	const added = seam.git(['worktree', 'add', '--detach', opts.path, base], roots[0]);
	if (added.code !== 0) {
		const reason = firstLine(added.stderr) || firstLine(added.stdout) || `exit ${added.code}`;
		return {ok: false, error: `isolation: worktree add failed: ${reason}`};
	}
	const head = seam.git(['rev-parse', 'HEAD'], opts.path);
	const start = head.code === 0 ? firstLine(head.stdout) : '';
	return {ok: true, path: opts.path, base, ...(start.length > 0 ? {head: start} : {}), mainCheckouts: roots};
}

// `undefined` is "git could not tell", which must keep the worktree: deleting on
// uncertainty would discard a user's work.
export function worktreeChanged(seam: GitSeam, path: string, head?: string): boolean | undefined {
	const status = seam.git(['status', '--porcelain'], path);
	if (status.code !== 0) return undefined;
	if (status.stdout.trim().length > 0) return true;
	if (head === undefined) return false;
	const current = seam.git(['rev-parse', 'HEAD'], path);
	if (current.code !== 0) return undefined;
	return firstLine(current.stdout) !== head;
}

/**
 * The one cleanup decision for a finished job. Unchanged: remove. Changed (or not
 * knowable): keep and report the path. A record already finalized returns
 * `undefined`, so the drain, the stop hook and the restart reconcile cannot act
 * twice.
 */
export function finalizeWorktree(
	seam: GitSeam,
	record: {worktree?: string; worktreeRepo?: string; worktreeFinalized?: boolean; worktreeHead?: string; live?: boolean},
): WorktreeDecision | undefined {
	if (typeof record.worktree !== 'string' || record.worktree.length === 0 || record.worktreeFinalized === true) {
		return undefined;
	}
	const path = record.worktree;
	// A live agent can still accept a follow-up, so its checkout must survive the
	// first-turn result. The worker owns the later cleanup decision.
	if (record.live === true) return {kept: true, path};
	const changed = worktreeChanged(seam, path, record.worktreeHead);
	if (changed === undefined) return {kept: true, path, error: 'git status failed; kept the worktree'};
	if (changed) return {kept: true, path};
	// Run in the repository root: `git worktree remove` from inside the worktree it
	// is deleting can trip over its own cwd.
	const removed = seam.git(['worktree', 'remove', '--force', path], record.worktreeRepo ?? dirname(path));
	if (removed.code !== 0) {
		return {kept: true, path, error: `git worktree remove failed: ${firstLine(removed.stderr) || `exit ${removed.code}`}`};
	}
	return {kept: false, path};
}

// A session is isolated when it is a subagent with a worktree, or when the session
// itself runs in a linked worktree. A normal checkout session is not.
export function isolationRoots(seam: GitSeam, cwd: string, spec?: IsolationSpec): IsolationRoots | undefined {
	const supplied = (spec?.mainCheckouts ?? []).filter((root) => typeof root === 'string' && root.length > 0);
	const mainCheckouts = supplied.length > 0 ? supplied : mainCheckoutRoots(seam, cwd);
	const worktree = typeof spec?.worktree === 'string' && spec.worktree.length > 0 ? spec.worktree : undefined;
	if (worktree !== undefined) {
		// A spool configured inside the repository puts the worktree under a forbidden
		// root. The root stays forbidden; the guard allows the worktree subtree inside it.
		return {worktree, mainCheckouts};
	}
	// A linked worktree reports two roots; the session's own checkout is the first.
	if (mainCheckouts.length > 1 && isInside(mainCheckouts[0], cwd)) {
		return {worktree: mainCheckouts[0], mainCheckouts};
	}
	return undefined;
}

// The shell tools the host exposes. Monitor is bash-shaped: it gets both checks.
const SHELL_TOOL_NAMES: Record<string, true> = {
	bash: true,
	shell: true,
	sh: true,
	monitor: true,
	powershell: true,
	pwsh: true,
};

// Redirect targets that discard output rather than write a file.
const NULL_DEVICES: Record<string, true> = {
	'/dev/null': true,
	'/dev/stdout': true,
	'/dev/stderr': true,
	'/dev/tty': true,
};

function namedCwd(input: Record<string, unknown>): string | undefined {
	for (const key of ['cwd', 'workdir', 'working_directory', 'workingDirectory']) {
		const value = input[key];
		if (typeof value === 'string' && value.trim().length > 0) return value.trim();
	}
	return undefined;
}

// Command-shape rules for Bash and Monitor. PowerShell deliberately gets only the
// working-directory check: parsing another shell's grammar here would be guessing.
export function commandEscapesWorktree(command: string, roots: IsolationRoots): string | undefined {
	if (/\b(?:GIT_DIR|GIT_WORK_TREE|GIT_COMMON_DIR)\s*=/.test(command)) {
		return 'the command overrides the git directory or work tree';
	}
	if (/\bgit\b[^\n;&|]*\s--prefix[=\s]/.test(command)) return 'the command overrides git --prefix';
	// Naming the worktree itself is not naming the main checkout, even when the spool
	// puts the worktree inside the repository.
	const outsideWorktree = command.split(roots.worktree).join('');
	for (const main of roots.mainCheckouts) {
		if (outsideWorktree.includes(main)) return `the command names the main checkout (${main})`;
	}
	const targets: {kind: string; target: string}[] = [];
	for (const match of command.matchAll(/(?:^|[;&|]\s*|\s)(?:cd|pushd|set-location|sl)\s+(['"]?)([^\s'";|&]+)\1/gi)) {
		targets.push({kind: 'cd', target: match[2]});
	}
	for (const match of command.matchAll(/(?:\d?|&)>>?\|?\s*(['"]?)([^\s'";|&<>]+)\1/g)) {
		targets.push({kind: 'redirect', target: match[2]});
	}
	for (const match of command.matchAll(/\bgit\b[^\n;&|]*(?:-C\s+|--work-tree[=\s]+|--git-dir[=\s]+)(['"]?)([^\s'";|&]+)\1/g)) {
		targets.push({kind: 'git path', target: match[2]});
	}
	for (const {kind, target} of targets) {
		if (kind === 'cd' && (target === '-' || target.startsWith('~'))) {
			return `a cd to ${target} leaves the worktree`;
		}
		if (target.startsWith('$')) continue;
		const resolved = resolve(roots.worktree, target);
		if (kind === 'cd' && !isInside(roots.worktree, resolved)) {
			return `a cd to ${target} leaves the worktree (${resolved})`;
		}
		// Discarding output is not writing a file; every other redirect target must stay
		// inside the worktree.
		if (kind === 'redirect' && NULL_DEVICES[resolved] === true) continue;
		if (kind !== 'cd' && !isInside(roots.worktree, resolved)) {
			return `a ${kind} to ${target} escapes the worktree (${resolved})`;
		}
	}
	return undefined;
}

/**
 * The one tool guard. `undefined` means "allowed". The block reason becomes the
 * tool result the model sees, so it names the worktree and the reason.
 */
export function guardToolCall(
	toolName: string,
	input: Record<string, unknown>,
	roots: IsolationRoots,
	toolCwd: string = roots.worktree,
): {block: true; reason: string} | undefined {
	const name = toolName.toLowerCase();
	if (SHELL_TOOL_NAMES[name] !== true) return undefined;
	const named = namedCwd(input);
	const cwd = named === undefined ? toolCwd : resolve(toolCwd, named);
	const insideWorktree = isInside(roots.worktree, cwd);
	// The worktree itself can sit inside the repository (a custom subagent_dir). The
	// worktree subtree is the allowed area; the rest of that checkout is not.
	if (!insideWorktree) {
		for (const main of roots.mainCheckouts) {
			if (!isInside(main, cwd)) continue;
			return {
				block: true,
				reason:
					`Blocked: this subagent is isolated in its worktree (${roots.worktree}); ` +
					`a command in the main checkout (${cwd}) is not allowed. Run it inside the worktree.`,
			};
		}
	}
	if (name === 'powershell' || name === 'pwsh') return undefined;
	const command = [input.command, input.script, input.cmd].find((value) => typeof value === 'string') as
		| string
		| undefined;
	if (command === undefined) return undefined;
	const escape = commandEscapesWorktree(command, roots);
	if (escape === undefined) return undefined;
	return {block: true, reason: `Blocked: ${escape}. This subagent is isolated in ${roots.worktree}.`};
}
