// ---------------------------------------------------------------------------
// Spawning a job
//
// One detached worker per job: buildSpec is the only place a spec is shaped and
// buildArgs the only place an argv is built, so replacing the transport is one
// module. fire(intent) is the only way a job starts and failJob(job, reason) is
// the only way one fails: both write the record and emit the signal, so every
// failure mode (and every guard) produces exactly one result. The module closes
// over no session state: the entry passes a SpawnContext with the spool, the
// clock, the directories, the worker string, and the callbacks that still live
// in the composition root.
// ---------------------------------------------------------------------------

import {join} from 'node:path';
import {
	ACTIVE_JOB_SCAN_MAX,
	CONTROL_POLL_MS,
	FOREGROUND_PERMISSION_TIMEOUT_MS,
	FOREGROUND_POLL_MS,
	FOREGROUND_WAIT_MAX_MS,
	JOB_ENTRY_TYPE,
	LOG_TAIL_BYTES_MAX,
	MAX_CONCURRENT_DEFAULT,
	MAX_CONCURRENT_ENV,
	MAX_CONCURRENT_ENV_ALIAS,
	MAX_DEPTH_DEFAULT,
	MAX_DEPTH_ENV,
	MAX_DEPTH_ENV_ALIAS,
	PERMISSION_TIMEOUT_MS,
	WORKER_ENV,
	WORKER_SPEC_ENV,
} from './constants.ts';
import {isTerminalState, normalizeState} from './state.ts';
import {patchOf, signalOf} from './signals.ts';
import {createJobWorktree} from './worktree.ts';
import type {GitSeam, WorktreeDecision} from './worktree.ts';
import type {ChildHandle, JobPatch, Signal, SpawnOptions, SpoolFs} from './types.ts';
import type {JobRecord} from './jobs.ts';
import type {JobPlan} from './plan.ts';

// The permission round-trip lives in its own leaf; these re-exports keep the
// spawn surface (and every existing importer) unchanged.
export {answerPendingPermission} from './permission.ts';
export type {PermissionContext} from './permission.ts';

export type LimitEnv = Record<string, string | undefined>;

/**
 * The worker's spec: the one launch shape plus the transport envelope. The launch
 * fields are inherited from JobPlan, never re-stated, so plan and spec cannot
 * drift; only the envelope the child needs to start is declared here.
 */
export interface WorkerSpecInput extends JobPlan {
	id: string;
	logPath: string;
	inboxDir: string;
	jobsDir: string;
	startedAt: number;
	tailMax: number;
	bin: string;
	sessionId?: string;
	/** The durable resume handle; absent for a one-shot agent. */
	agentId?: string;
	/** Where the worker appends the persisted conversation. */
	transcriptPath?: string;
	/** The session name the host assigns to the new fork. */
	sessionName?: string;
	/** `shared` for a fork, `own` for a non-fork. */
	promptCache?: 'shared' | 'own';
	/** The parent's control-poll and permission-wait cadence. */
	controlPollMs?: number;
	permissionTimeoutMs?: number;
	/** Foreground runs answer permission prompts inline; background runs settle. */
	interactive?: boolean;
	controlDir?: string;
	/** Written by the mod when the ACP child loaded it; proves the prompt hook is live. */
	promptMarkerPath?: string;
	/** The worktree this job runs in, when isolation created one. */
	worktree?: string;
	/** Checkout roots this job must never enter. */
	mainCheckouts?: string[];
	/** Rendered sibling snapshot (main + other named agents), taken at spawn. */
	roster?: string;
	/** Roster names, for ListAgents and for validating a worker's `to`. */
	rosterNames?: string[];
	/** How long an idle live agent waits for a message before it exits. */
	idleMs?: number;
}

export interface WorkerSpec extends WorkerSpecInput {
	args: string[];
}

export type ForegroundResult =
	| {readonly ok: true; readonly text: string; readonly partial: boolean}
	| {readonly ok: false; readonly error: string};

export type Launch =
	| {readonly ok: true; readonly id: string; readonly pid: number; readonly result?: ForegroundResult}
	| {readonly ok: false; readonly error: string};

/** The activate state and levers a spawn reads, passed explicitly. */
export interface SpawnContext {
	readonly fs: SpoolFs;
	readonly env: Record<string, string | undefined>;
	readonly execPath: string;
	spawn(command: string, args: string[], options: SpawnOptions): ChildHandle;
	now(): number;
	sleep(ms: number): Promise<void>;
	readonly workerSpec: WorkerSpec | undefined;
	/** WORKER_SOURCE stays in the entry; the worker invocation receives it. */
	readonly workerSource: string;
	/** MOD_PATH stays in the entry; buildSpec receives it. */
	readonly modPath: string;
	readonly inboxDir: string;
	readonly jobsDir: string;
	readonly logsDir: string;
	readonly worktreesDir: string;
	readonly controlRoot: string;
	readonly git: GitSeam;
	sessionId(): string | undefined;
	owner(): string | undefined;
	recordFromPlan(
		intent: JobPlan,
		id: string,
		logPath: string,
		startedAt: number,
		owner: string | undefined,
	): JobRecord;
	sessionNameOf(id: string): string;
	readJob(id: string): JobRecord | undefined;
	writeJob(record: JobRecord): void;
	readRecentJobs(limit: number): JobRecord[];
	applyPatch(id: string, patch: JobPatch, fallback: JobRecord, stamp: number): boolean;
	enqueueSignal(signal: Signal): void;
	refreshJobs(): void;
	finalizeJobWorktree(job: JobRecord | undefined): WorktreeDecision | undefined;
	foregroundOutcome(job: JobRecord, log: string): ForegroundResult;
	readLog(file: string): string;
	rosterFor(selfName: string | undefined): {roster?: string; rosterNames: string[]};
	runProjectHooks(
		event: 'SubagentStart' | 'SubagentStop',
		cwd: string,
		agent: string | undefined,
		input: Record<string, unknown>,
	): void;
	appendJobEntry(entry: {customType: string; data: Record<string, unknown>}): void;
	answerPendingPermission(controlDir: string, answered: Set<string>): Promise<void>;
	/**
	 * The permission requests already put to the human, across every path that
	 * asks: the foreground wait and the delivery drain share this one set, so a
	 * request reaches the human once instead of once per poller.
	 */
	readonly askedPermissions: Set<string>;
	readonly backgrounded: {has(id: string): boolean};
}

// ACP is the accepted worker transport: `cmd --mod <file> acp` loads this mod in
// the worker so appendSystemPrompt can return the definition body, and the adapter
// speaks JSON-RPC over stdout/stdin. Shell jobs keep the /bin/sh argv.
export function buildArgs(spec: WorkerSpec, modPath: string): string[] {
	if (spec.kind === 'command') return ['-c', String(spec.command ?? '')];
	if (spec.kind === 'fork') {
		// ACP has no fork primitive; the host CLI fork copies the session and its
		// prompt cache. `--output-format json` is what makes the new session id
		// readable from the result line, so a follow-up can resume it.
		const prompt = String(spec.prompt ?? '');
		if (typeof spec.resumeSession === 'string' && spec.resumeSession.length > 0) {
			return ['--resume', spec.resumeSession, '-p', prompt, '--output-format', 'json'];
		}
		const name = String(spec.sessionName ?? `subagent-${spec.id}`);
		return [
			'--fork-session',
			'--resume',
			String(spec.mainSessionId ?? ''),
			'-n',
			name,
			'-p',
			prompt,
			'--output-format',
			'json',
		];
	}
	// maxTurns is a host cap. The flag is shared with -p mode; ACP may ignore it,
	// but the stop reason is what the lifecycle contract reads, and a host that
	// honors it stops with max_turn_requests, which the worker marks partial.
	const maxTurns = typeof spec.maxTurns === 'number' && spec.maxTurns > 0 ? String(spec.maxTurns) : '';
	return [...(maxTurns.length > 0 ? ['--max-turns', maxTurns] : []), '--mod', modPath, 'acp'];
}

export function buildSpec(input: WorkerSpecInput, modPath: string): WorkerSpec {
	const spec: WorkerSpec = {...input, args: []};
	spec.args = buildArgs(spec, modPath);
	return spec;
}

function positiveInt(raw: string | undefined, fallback: number): number {
	const value = Number(raw);
	return Number.isFinite(value) && value >= 1 ? Math.floor(value) : fallback;
}

export function resolveMaxConcurrent(env: LimitEnv): number {
	return positiveInt(env[MAX_CONCURRENT_ENV] || env[MAX_CONCURRENT_ENV_ALIAS], MAX_CONCURRENT_DEFAULT);
}

export function resolveMaxDepth(env: LimitEnv): number {
	return positiveInt(env[MAX_DEPTH_ENV] || env[MAX_DEPTH_ENV_ALIAS], MAX_DEPTH_DEFAULT);
}

// A session's nesting depth: 0 for the main conversation, the job's own depth for
// a worker (the spec is the worker's environment).
function depthOf(spec: WorkerSpec | undefined): number {
	const depth = Number(spec?.depth);
	return Number.isFinite(depth) && depth >= 0 ? Math.floor(depth) : 0;
}

// The environment for a worker: the caller's env minus every worker marker, then
// this job's spec and flag. A nested spawn therefore cannot inherit the parent
// spec or a stale worker flag.
export function workerEnv(
	parent: Record<string, string | undefined>,
	spec: WorkerSpec,
): Record<string, string | undefined> {
	const env = {...parent};
	delete env[WORKER_SPEC_ENV];
	delete env[WORKER_ENV];
	delete env.PUBSUB_WORKER;
	delete env.BETTER_SUBAGENTS_APPEND_SYSTEM_PROMPT;
	delete env.CLAUDE_CODE_APPEND_SUBAGENT_SYSTEM_PROMPT;
	delete env.BETTER_SUBAGENTS_APPEND_SYSTEM_PROMPT_FILE;
	delete env.CLAUDE_CODE_APPEND_SUBAGENT_SYSTEM_PROMPT_FILE;
	env[WORKER_SPEC_ENV] = JSON.stringify(spec);
	env[WORKER_ENV] = '1';
	if (typeof spec.appendSystemPrompt === 'string' && spec.appendSystemPrompt.length > 0) {
		env.BETTER_SUBAGENTS_APPEND_SYSTEM_PROMPT = spec.appendSystemPrompt;
	}
	return env;
}

export function failJob(job: JobRecord, reason: string, ctx: SpawnContext): void {
	const now = ctx.now();
	const signal = signalOf(job, {
		state: 'failed',
		finishedAt: now,
		durationMs: Math.max(0, now - Number(job.startedAt ?? now)),
		body: reason,
	});
	// The state table decides whether this job may still fail: a terminal job keeps
	// whatever result it already has, and emits nothing.
	if (!ctx.applyPatch(job.id, patchOf(signal), job, now)) return;
	ctx.enqueueSignal(signal);
	ctx.refreshJobs();
}

// How it runs: one detached worker, whose spec is built only by buildSpec.
function startWorker(record: JobRecord, spec: WorkerSpec, ctx: SpawnContext): Launch {
	const id = record.id;
	let worker: ChildHandle;
	try {
		worker = ctx.spawn(ctx.execPath, ['-e', ctx.workerSource], {
			cwd: spec.cwd,
			detached: true,
			stdio: 'ignore',
			env: workerEnv(ctx.env, spec),
		});
	} catch (error) {
		const reason = `spawn error: ${String((error as Error)?.message ?? error)}`;
		failJob(record, reason, ctx);
		return {ok: false, error: reason};
	}

	// A spawn failure is also an async 'error' event; without a listener it would be
	// an uncaught exception in the host, and the job would sit at `queued` forever.
	worker.on('error', error => {
		failJob(ctx.readJob(id) ?? record, `worker spawn error: ${String((error as Error)?.message ?? error)}`, ctx);
	});
	worker.unref();

	if (typeof worker.pid !== 'number') {
		const reason = 'the worker process did not start';
		failJob(ctx.readJob(id) ?? record, reason, ctx);
		return {ok: false, error: reason};
	}

	// Merge the pid onto the freshest record: the worker may already have written
	// `running`, and a blind overwrite would drag it back to `queued`.
	const latest = ctx.readJob(id) ?? record;
	latest.workerPid = worker.pid;
	latest.updatedAt = ctx.now();
	ctx.writeJob(latest);
	// SubagentStart is a project hook: it runs once the job actually started.
	ctx.runProjectHooks('SubagentStart', spec.cwd, spec.agentType, {
		id,
		label: record.description,
		kind: record.kind,
	});
	ctx.refreshJobs();
	return {ok: true, id, pid: worker.pid};
}

// The pool is per session: only this session's queued and running jobs occupy
// slots. Records are the source of truth, so a cancel in another process frees
// one, and a job waiting for input has no worker process to count.
function activeJobCount(ctx: SpawnContext): number {
	const owner = ctx.owner();
	let count = 0;
	for (const job of ctx.readRecentJobs(ACTIVE_JOB_SCAN_MAX)) {
		if (job.owner !== owner) continue;
		const state = normalizeState(job.state);
		if (state !== undefined && state !== 'queued' && state !== 'running') continue;
		count += 1;
	}
	return count;
}

export async function fire(intent: JobPlan, ctx: SpawnContext): Promise<Launch> {
	// Both limits are checked before a record exists, so a refused spawn leaves no
	// job behind and emits no signal.
	const depth = depthOf(ctx.workerSpec) + 1;
	if (intent.prompt !== undefined) {
		const maxDepth = resolveMaxDepth(ctx.env);
		if (depth > maxDepth) {
			return {
				ok: false,
				error: `Subagent nesting depth limit reached (${maxDepth}). This subagent cannot spawn children; do not retry.`,
			};
		}
	}
	const maxConcurrent = resolveMaxConcurrent(ctx.env);
	if (activeJobCount(ctx) >= maxConcurrent) {
		return {
			ok: false,
			error: `Concurrent subagent limit reached (${maxConcurrent}). Do not retry; wait for a running subagent to finish or cancel one first.`,
		};
	}
	const id = `${ctx.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
	const logPath = join(ctx.logsDir, `${id}.log`);
	const startedAt = ctx.now();
	// Worktree isolation is created before anything runs: the record, the worker
	// cwd and the guard all carry one path. A creation failure is one failed job.
	const isolation =
		intent.isolation === 'worktree'
			? createJobWorktree({git: ctx.git.git, fs: ctx.fs}, {cwd: intent.cwd, path: join(ctx.worktreesDir, id)})
			: undefined;
	const owner = intent.owner ?? ctx.owner();
	const record = ctx.recordFromPlan(
		{...intent, depth, ...(ctx.workerSpec?.id !== undefined ? {parentJobId: ctx.workerSpec.id} : {})},
		id,
		logPath,
		startedAt,
		owner,
	);
	if (isolation?.ok === true) {
		record.worktree = isolation.path;
		record.worktreeRepo = isolation.mainCheckouts[0];
		if (isolation.head !== undefined) record.worktreeHead = isolation.head;
		record.mainCheckouts = isolation.mainCheckouts;
		record.cwd = isolation.path;
	}
	ctx.writeJob(record);
	if (isolation !== undefined && isolation.ok === false) {
		failJob(record, isolation.error, ctx);
		return {ok: false, error: isolation.error};
	}
	try {
		ctx.appendJobEntry({
			customType: JOB_ENTRY_TYPE,
			data: {
				id,
				description: intent.description,
				kind: intent.kind,
				state: 'queued',
				owner,
				sessionId: ctx.sessionId(),
			},
		});
	} catch {
		/* audit trail is optional */
	}

	const isAgent = intent.prompt !== undefined;
	const live = isAgent && intent.live !== false && intent.oneShot !== true;
	// The roster is for an agent that can actually message a peer. Host definitions
	// name the tool SendMessage; the mod registers it as subagent_send. A fork
	// already has the conversation (and its roster) and gets no fresh bundle.
	const canSend =
		intent.tools === undefined || intent.tools.includes('SendMessage') || intent.tools.includes('subagent_send');
	const roster = isAgent && intent.fork !== true && canSend ? ctx.rosterFor(intent.name) : {rosterNames: []};
	const idleMs = Number(ctx.env.BETTER_SUBAGENTS_LIVE_IDLE_MS);
	if (live) {
		try {
			ctx.fs.mkdir(join(ctx.controlRoot, id));
		} catch {
			/* the first send retries the mkdir */
		}
	}
	// The spec is the launch plus the transport envelope. Every launch field is
	// carried by the spread, so a new one cannot be dropped on the way to the
	// child. The six below are parent-only: the worktree is already created, the
	// id and depth are already settled, and the remaining three never cross the
	// process boundary. `live` and `restrictedTools` are re-derived below, so the
	// raw plan values must not leak through.
	const {
		isolation: _isolation,
		oneShot: _oneShot,
		parentJobId: _parentJobId,
		color: _color,
		resumedFrom: _resumedFrom,
		live: _live,
		...launch
	} = intent;
	const spec = buildSpec(
		{
			...launch,
			id,
			owner,
			cwd: isolation?.ok === true ? isolation.path : intent.cwd,
			logPath,
			inboxDir: ctx.inboxDir,
			jobsDir: ctx.jobsDir,
			startedAt,
			tailMax: LOG_TAIL_BYTES_MAX,
			controlPollMs: CONTROL_POLL_MS,
			permissionTimeoutMs: intent.background === false ? FOREGROUND_PERMISSION_TIMEOUT_MS : PERMISSION_TIMEOUT_MS,
			bin: isAgent ? (ctx.env.COMMANDCODE_BIN ?? 'cmd') : '/bin/sh',
			sessionId: ctx.sessionId(),
			depth,
			background: intent.background !== false,
			restrictedTools: intent.restrictedTools === true ? true : undefined,
			timeoutMs: intent.timeoutMs !== undefined && intent.timeoutMs > 0 ? intent.timeoutMs : undefined,
			// A foreground caller answers permission prompts inline; a background run
			// settles with the request instead of holding a worker that nobody watches.
			...(intent.background === false
				? {interactive: true, controlDir: join(ctx.jobsDir, '..', 'control', id)}
				: {}),
			...(record.agentId !== undefined ? {agentId: record.agentId} : {}),
			...(record.transcriptPath !== undefined ? {transcriptPath: record.transcriptPath} : {}),
			...(intent.fork === true
				? {promptCache: 'shared', sessionName: ctx.sessionNameOf(id)}
				: isAgent
					? {promptCache: 'own'}
					: {}),
			...(roster.roster !== undefined ? {roster: roster.roster} : {}),
			...(roster.rosterNames.length > 0 ? {rosterNames: roster.rosterNames} : {}),
			...(live ? {live: true, controlDir: join(ctx.controlRoot, id), ...(Number.isFinite(idleMs) && idleMs > 0 ? {idleMs} : {})} : {}),
			...(isAgent ? {promptMarkerPath: join(ctx.logsDir, `${id}.mod`)} : {}),
			...(isolation?.ok === true ? {worktree: isolation.path, mainCheckouts: isolation.mainCheckouts} : {}),
		},
		ctx.modPath,
	);
	const launched = startWorker(record, spec, ctx);
	if (launched.ok === false || intent.background !== false) return launched;
	return settleForeground(launched, ctx);
}

// Foreground is a bounded wait on the same durable record a background job
// leaves behind. Completion is the record, never the process: a worker that died
// after writing its result must still return it.
async function settleForeground(
	launched: {
		readonly ok: true;
		readonly id: string;
		readonly pid: number;
	},
	ctx: SpawnContext,
): Promise<Launch> {
	const rounds = Math.ceil(FOREGROUND_WAIT_MAX_MS / FOREGROUND_POLL_MS) + 1;
	let record = ctx.readJob(launched.id);
	// The shared set: the delivery drain polls the same control dir, and one
	// request must reach the human once.
	const answered = ctx.askedPermissions;
	const permissionDir = join(ctx.controlRoot, launched.id);
	for (let round = 0; round < rounds && !isTerminalState(record?.state); round += 1) {
		if (ctx.backgrounded.has(launched.id)) {
			// Ctrl+B: stop waiting, but leave the worker alone. Its result now arrives
			// as a normal signal, exactly like a spawn that asked for the background.
			ctx.refreshJobs();
			return {
				ok: true,
				id: launched.id,
				pid: launched.pid,
				result: {
					ok: true,
					partial: true,
					text: `${String(record?.description ?? launched.id)} continues in the background; its result will arrive as a signal.`,
				},
			};
		}
		await ctx.answerPendingPermission(permissionDir, answered);
		await ctx.sleep(FOREGROUND_POLL_MS);
		record = ctx.readJob(launched.id);
	}
	if (record === undefined || !isTerminalState(record.state)) {
		// Past the bound the run keeps going. Demote it so its result is delivered as
		// a signal instead of being lost with the tool call that gave up on it.
		if (record !== undefined) ctx.writeJob({...record, background: true, updatedAt: ctx.now()});
		ctx.refreshJobs();
		return {
			ok: false,
			error:
				`foreground subagent ${launched.id} did not finish within ${Math.round(FOREGROUND_WAIT_MAX_MS / 1000)}s; ` +
				'it continues in the background, and its result will arrive as a signal',
		};
	}
	const log = ctx.readLog(String(record.logPath ?? join(ctx.logsDir, `${launched.id}.log`)));
	// A foreground run never passes through the drain, so its worktree decision is
	// made here: a changed worktree is kept, reported, and never left unfinalized.
	const decision = ctx.finalizeJobWorktree(record);
	const outcome = ctx.foregroundOutcome(record, log);
	if (decision?.kept !== true) return {...launched, result: outcome};
	const note = `\nworktree kept at ${decision.path}${decision.error === undefined ? '' : ` (${decision.error})`}`;
	const result: ForegroundResult =
		outcome.ok === true
			? {ok: true, text: `${outcome.text}${note}`, partial: outcome.partial}
			: {ok: false, error: `${outcome.error}${note}`};
	return {...launched, result};
}
