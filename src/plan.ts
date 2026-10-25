// ---------------------------------------------------------------------------
// Planning a job
//
// One resolution for a new job and one reconstruction from a finished record:
// every field the record, the worker spec, and the transport need is decided
// here, once. The module closes over no session state: the entry passes a
// PlanContext with the spool, the environment, the session id, the config
// snapshot, and the resolvers that still live in the composition root.
// ---------------------------------------------------------------------------

import {join} from 'node:path';
import {FORK_AGENT_TYPE} from './constants.ts';
import {backgroundDisabled} from './config.ts';
import {stringField} from './definitions.ts';
import type {JobRecord} from './jobs.ts';
import type {SpoolFs} from './types.ts';

// Shared field-coercion helpers for the plan conversions. These are the
// mechanical parts only: string/number coercion with the same rules the
// conversions already applied inline.
export function numberField(value: unknown): number | undefined {
	const numeric = Number(value);
	return Number.isFinite(numeric) && numeric > 0 ? Math.round(numeric) : undefined;
}

export function nonEmptyString(value: unknown): string | undefined {
	return typeof value === 'string' && value.trim().length > 0 ? value : undefined;
}

export type PlanMemoryScope = 'user' | 'project' | 'local';

/** The subset of a definition the plan reads; a full definition is assignable. */
export interface PlanAgent {
	readonly name: string;
	readonly prompt: string;
	readonly scope?: string;
	readonly sourcePath?: string;
	readonly fork?: boolean;
	readonly oneShot?: boolean;
	readonly background?: boolean;
	readonly tools?: string[];
	readonly disallowedTools?: string[];
	readonly permissionMode?: string;
	readonly mcpServers?: NonNullable<JobRecord['mcpServers']>;
	readonly model?: string;
	readonly effort?: string;
	readonly maxTurns?: number;
	readonly memory?: PlanMemoryScope;
	readonly isolation?: string;
	readonly color?: string;
	readonly cacheTtl?: JobRecord['cacheTtl'];
	readonly hooks?: PlanHooks;
	readonly autoCompactWindow?: number;
}

export interface PlanHookRule {
	matcher?: string;
	command: string;
	timeoutMs?: number;
}

export interface PlanHooks {
	PreToolUse?: PlanHookRule[];
	PostToolUse?: PlanHookRule[];
	Stop?: PlanHookRule[];
}

export type PlanAgentResolution =
	| {ok: true; definition: PlanAgent}
	| {ok: false; error: string};

export interface PlanTeamInput {
	readonly teammate: boolean;
	readonly definitionBackground?: boolean;
	readonly requested?: boolean;
	readonly forkMode: 'on' | 'off';
}

export type PlanTeamPolicy =
	| {readonly ok: true; readonly background: false}
	| {readonly ok: false; readonly error: string};

export type PlanPermissionResolution =
	| {readonly ok: true; readonly mode: string}
	| {readonly ok: false; readonly error: string};

export interface PlanExecutionInput {
	readonly requested?: boolean;
	readonly definitionBackground?: boolean;
	readonly env: Record<string, string | undefined>;
}

export interface PlanLaunchInput {
	requestedModel?: string;
	definitionModel?: string;
	requestedEffort?: string;
	definitionEffort?: string;
	env?: Record<string, string | undefined>;
	tools?: string[];
	disallowedTools?: string[];
	permissionMode?: string;
	background?: boolean;
	team?: boolean;
	mcpServers?: string[];
}

export type PlanLaunch =
	| {
			ok: true;
			model: string | undefined;
			effort: string | undefined;
			tools: string[];
	  }
	| {ok: false; error: string};

export interface PlanMcpInput {
	defined: NonNullable<JobRecord['mcpServers']>;
	configured: Record<string, unknown>;
	definition?: PlanAgent;
	env: Record<string, string | undefined>;
	trusted: boolean;
}

export interface PlanMcpResolution {
	servers: NonNullable<JobRecord['mcpServers']>;
}

export interface PlanTrustRoots {
	readonly cwd: string;
	readonly home: string;
	readonly managedDir?: string;
	readonly addDirs?: readonly string[];
}

/** The activate state and levers the plan reads, passed explicitly. */
export interface PlanContext {
	readonly fs: SpoolFs;
	readonly env: Record<string, string | undefined>;
	/** The main session's cwd, the fallback for a plan that names none. */
	readonly cwd: string;
	/** The workspace root: MCP discovery and trust start here. */
	readonly runtimeCwd: string;
	readonly home: string;
	readonly transcriptsDir: string;
	readonly appendPromptText: string;
	readonly forkMode: 'on' | 'off';
	readonly forkModeEnabled: boolean;
	readonly managedDir: string;
	readonly addDirs: readonly string[];
	sessionId(): string | undefined;
	mainPermissionMode(): string | undefined;
	resolveAgentDefinition(name?: string): PlanAgentResolution;
	systemPromptFor(base: string | undefined): string | undefined;
	startupFor(
		definition: PlanAgent | undefined,
		cwd: string,
		prompt: string | undefined,
		model: string | undefined,
	): string | undefined;
	canonicalPermissionMode(value: unknown): string | undefined;
	resolveTeamPolicy(input: PlanTeamInput): PlanTeamPolicy;
	resolvePermissionMode(definitionMode: unknown, mainMode: unknown): PlanPermissionResolution;
	resolveExecutionPolicy(input: PlanExecutionInput): 'foreground' | 'background';
	mcpServerConfigs(fs: SpoolFs, dirs: string[]): Record<string, unknown>;
	resolveAgentMcpServers(input: PlanMcpInput): PlanMcpResolution;
	agentFolderTrusted(definition: PlanAgent | undefined, roots: PlanTrustRoots): boolean;
	resolveLaunch(input: PlanLaunchInput): PlanLaunch;
	withMemoryTools(tools: string[] | undefined, memory: PlanMemoryScope | undefined): string[] | undefined;
	contextWindowForModel(model: string | undefined, env: Record<string, string | undefined>): number;
	resolveAutoCompactWindow(inherited: number, requested: number | undefined, isMain: boolean): number | undefined;
	listField(value: unknown): string[] | undefined;
}

export interface JobPlan {
	readonly description: string;
	readonly kind: 'command' | 'agent' | 'fork';
	readonly command?: string;
	readonly prompt?: string;
	readonly cwd: string;
	/** the resolved definition name: the canonical `subagent_type` */
	readonly agentType?: string;
	/** Explore and Plan are one-shot: no agent id, no resume. */
	readonly oneShot?: boolean;
	/** A fork: inherit the main conversation instead of building a fresh one. */
	readonly fork?: boolean;
	/** The main conversation the fork copies. */
	readonly mainSessionId?: string;
	/** The team this teammate belongs to. */
	readonly teamName?: string;
	/** The resolved append text, carried to nested agents. */
	readonly appendSystemPrompt?: string;
	/** A tool restriction that only the worker hook can enforce. */
	readonly restrictedTools?: boolean;
	/** This job's nesting level, and the job that spawned it. */
	readonly depth?: number;
	readonly parentJobId?: string;
	/** The definition body, delivered as the worker's system prompt */
	readonly systemPrompt?: string;
	readonly hooks?: PlanHooks;
	/** The rendered startup bundle: instructions, git status, skills, memory */
	readonly startup?: string;
	readonly model?: string;
	readonly maxTurns?: number;
	readonly effort?: string;
	/** The resolved tool capabilities for an agent job. */
	readonly tools?: string[];
	readonly disallowedTools?: string[];
	/** The MCP servers scoped to this agent. */
	readonly mcpServers?: NonNullable<JobRecord['mcpServers']>;
	/** The resolved auto-compact window; only ever lowers the inherited one. */
	readonly autoCompactWindow?: number;
	readonly cacheTtl?: JobRecord['cacheTtl'];
	readonly color?: string;
	readonly permissionMode?: string;
	readonly isolation?: string;
	/** The resolved execution policy: false waits for the result in the tool call. */
	readonly background?: boolean;
	/** SIGTERM then SIGKILL after this long; 0 or absent means no timeout */
	readonly timeoutMs?: number;
	/** resume this ACP session id first */
	readonly resumeSession?: string;
	readonly resumedFrom?: string;
	/** addressable name; a named agent can be messaged after it finishes */
	readonly name?: string;
	/** override the owner (a worker resuming a session it did not create) */
	readonly owner?: string;
	/** keep the ACP session open for live messages after the first turn */
	readonly live?: boolean;
}

// One resolution for a new job: every field the record, the worker spec, and the
// transport need is decided here, once. `fire` adds identity (id, paths, owner,
// depth) and runs it; resume and send rebuild the same shape with planFromRecord.
export function planFromInput(
	input: Record<string, unknown>,
	ctx: PlanContext,
): {ok: true; plan: JobPlan} | {ok: false; error: string} {
	const label = String(input.description ?? input.label ?? '').trim();
	if (label.length === 0) return {ok: false, error: 'description is required and must not be empty'};
	const command = nonEmptyString(input.command);
	const prompt = nonEmptyString(input.prompt);
	if (command === undefined && prompt === undefined) {
		return {ok: false, error: 'pass either prompt (a headless agent) or command (a shell command)'};
	}
	if (command !== undefined && prompt !== undefined) return {ok: false, error: 'pass prompt or command, not both'};
	// permission_mode is resolved below instead of being rejected, and tools are
	// resolved by resolveLaunch. Isolation is chosen by a definition's frontmatter.
	if (input.isolation !== undefined) {
		return {
			ok: false,
			error: "isolation is chosen by a definition's frontmatter (`isolation: worktree`), not per call; omit it",
		};
	}
	const requestedMode = stringField(input.permission_mode);
	if (requestedMode !== undefined && ctx.canonicalPermissionMode(requestedMode) === undefined) {
		return {ok: false, error: `unknown permission_mode "${requestedMode}"`};
	}
	if (requestedMode !== undefined && prompt === undefined) {
		return {ok: false, error: 'permission_mode applies to agent jobs; a shell command has no permission mode'};
	}
	const model = stringField(input.model);
	const name = stringField(input.name);
	const teamName = stringField(input.team_name);
	// `subagent_type` is canonical; `agent_type` is the 1.x alias and must behave
	// identically. Passing both with different values is ambiguous, not a merge.
	const subagentType = stringField(input.subagent_type);
	const aliasType = stringField(input.agent_type);
	if (subagentType !== undefined && aliasType !== undefined && subagentType !== aliasType) {
		return {ok: false, error: 'pass subagent_type or its deprecated alias agent_type, not both'};
	}

	let definition: PlanAgent | undefined;
	if (prompt !== undefined) {
		// Omitted type resolves to general-purpose, or fails with the documented
		// error when no general-purpose agent exists.
		const resolved = ctx.resolveAgentDefinition(subagentType ?? aliasType);
		if (resolved.ok === false) return {ok: false, error: resolved.error};
		definition = resolved.definition;
	}
	const forkRequested = definition?.fork === true;
	const teammate = prompt !== undefined && teamName !== undefined;
	const requestedBackground = typeof input.run_in_background === 'boolean' ? input.run_in_background : undefined;
	// A teammate's definition may not pin it to the background, and a caller may
	// not ask for it while fork mode still exposes the parameter.
	const teamPolicy = ctx.resolveTeamPolicy({
		teammate,
		definitionBackground: definition?.background,
		...(requestedBackground !== undefined ? {requested: requestedBackground} : {}),
		forkMode: ctx.forkMode,
	});
	if (teamPolicy.ok === false) return {ok: false, error: teamPolicy.error};
	const requestedTools = prompt === undefined || forkRequested ? undefined : (ctx.listField(input.tools) ?? definition?.tools);
	const requestedDisallowed =
		prompt === undefined || forkRequested ? undefined : (ctx.listField(input.disallowed_tools) ?? definition?.disallowedTools);
	// The definition's mode and the main session's mode are combined here, once.
	// A definition asking for bypass in a default session is refused before the
	// record exists, so no job is left queued behind a mode that cannot run. A
	// fork skips this: it inherits the main session's permissions.
	let permissionMode: string | undefined;
	if (prompt !== undefined) {
		if (forkRequested) {
			permissionMode = ctx.mainPermissionMode() ?? 'default';
		} else {
			const modeResolution = ctx.resolvePermissionMode(requestedMode ?? definition?.permissionMode, ctx.mainPermissionMode());
			if (modeResolution.ok === false) return {ok: false, error: modeResolution.error};
			permissionMode = modeResolution.mode;
		}
	}
	// Three inputs, one precedence table: the disable env, the definition's pinned
	// background, then the caller's request. Fork mode overrides the definition and
	// the caller for a model-spawned agent (the parameter is gone from the
	// schema) and a teammate is always foreground.
	//
	// The disable env beats fork mode. Fork mode is on by default for a human in
	// a terminal, and it removes run_in_background from the schema, so a run under
	// the disable env would be background with no way to ask for the foreground.
	// The env is the user's explicit "run this in the foreground" switch.
	const background = teammate
		? false
		: ctx.forkModeEnabled && !backgroundDisabled(ctx.env)
			? true
			: ctx.resolveExecutionPolicy({
					...(requestedBackground !== undefined ? {requested: requestedBackground} : {}),
					definitionBackground: definition?.background,
					env: ctx.env,
				}) === 'background';
	// MCP servers are resolved once, for this agent: a reference reads the host's
	// config, an inline entry needs a trusted folder, and bare/strict/allow/deny
	// still apply. The result scopes `session/new`; the tool pool is widened by
	// the same names so a definition can list `mcp__server__*`. A fork inherits
	// the main session's servers and skips this.
	const configuredMcp = ctx.mcpServerConfigs(ctx.fs, [ctx.home, ctx.runtimeCwd]);
	const mcp = ctx.resolveAgentMcpServers({
		defined: definition?.mcpServers ?? [],
		configured: configuredMcp,
		definition,
		env: ctx.env,
		trusted:
			definition === undefined ||
			ctx.agentFolderTrusted(definition, {
				cwd: ctx.runtimeCwd,
				home: ctx.home,
				managedDir: ctx.managedDir,
				addDirs: ctx.addDirs,
			}),
	});
	const mcpNames = Object.keys(configuredMcp);
	for (const server of mcp.servers) if (!mcpNames.includes(server.name)) mcpNames.push(server.name);
	const launch = forkRequested
		? {ok: true as const, model: undefined, modelSource: 'inherit' as const, effort: undefined, effortSource: 'none' as const, tools: [], removed: []}
		: ctx.resolveLaunch({
				...(model !== undefined ? {requestedModel: model} : {}),
				...(definition?.model !== undefined ? {definitionModel: definition.model} : {}),
				...(typeof input.effort === 'string' ? {requestedEffort: input.effort} : {}),
				...(definition?.effort !== undefined ? {definitionEffort: definition.effort} : {}),
				env: ctx.env,
				// The mod API exposes no main-model getter: an absent model means the ACP
				// child inherits the user's configured model, which is the main one.
				...(requestedTools !== undefined ? {tools: requestedTools} : {}),
				...(requestedDisallowed !== undefined ? {disallowedTools: requestedDisallowed} : {}),
				...(permissionMode !== undefined ? {permissionMode} : {}),
				background,
				...(teammate ? {team: true} : {}),
				mcpServers: mcpNames,
			});
	if (launch.ok === false) return {ok: false, error: launch.error};
	const sessionId = ctx.sessionId();
	if (forkRequested && (sessionId === undefined || sessionId.length === 0)) {
		return {ok: false, error: 'a fork needs the main session id; the session has not bound yet'};
	}
	// Memory definitions auto-enable Read, Write, and Edit. A fork keeps the main
	// tool pool untouched, so it gets no filter and no memory additions.
	const launchTools = prompt === undefined || forkRequested ? undefined : ctx.withMemoryTools(launch.tools, definition?.memory);
	const maxTurns =
		prompt === undefined || forkRequested ? undefined : (numberField(input.max_turns) ?? definition?.maxTurns);
	const runCwd = typeof input.cwd === 'string' && input.cwd.length > 0 ? input.cwd : ctx.cwd;
	// The auto-compact window only lowers what this agent's own model gives it.
	const compactWindow = ctx.resolveAutoCompactWindow(
		ctx.contextWindowForModel(launch.model, ctx.env),
		definition?.autoCompactWindow,
		false,
	);
	return {
		ok: true,
		plan: {
			description: label,
			kind: forkRequested ? 'fork' : prompt !== undefined ? 'agent' : 'command',
			command,
			prompt,
			background,
			cwd: runCwd,
			...(name !== undefined ? {name} : {}),
			...(teamName !== undefined ? {teamName} : {}),
			agentType: definition?.name,
			oneShot: definition?.oneShot,
			...(forkRequested
				? {fork: true, ...(sessionId !== undefined ? {mainSessionId: sessionId} : {})}
				: {}),
			systemPrompt: forkRequested ? undefined : ctx.systemPromptFor(definition?.prompt),
			appendSystemPrompt: ctx.appendPromptText,
			// The flag says the worker's tool set must be enforced, and the worker
			// refuses to run when the mod did not load in the ACP child. It is true
			// whenever the tool set is a restriction: the caller's `tools`, the
			// definition's `tools`, or either `disallowedTools` list. The caller's
			// list was missing, so a run narrowed by the caller alone was not
			// flagged, and the worker ran with an allowlist it never enforced.
			restrictedTools:
				requestedTools !== undefined ||
				definition?.tools !== undefined ||
				definition?.disallowedTools !== undefined ||
				requestedDisallowed !== undefined,
			hooks: forkRequested ? undefined : definition?.hooks,
			startup: forkRequested ? undefined : ctx.startupFor(definition, runCwd, prompt, launch.model),
			model: prompt === undefined || forkRequested ? undefined : launch.model,
			tools: launchTools,
			...(requestedDisallowed !== undefined ? {disallowedTools: requestedDisallowed} : {}),
			...(definition?.color !== undefined ? {color: definition.color} : {}),
			...(prompt !== undefined && !forkRequested ? {mcpServers: mcp.servers} : {}),
			...(prompt !== undefined && !forkRequested && compactWindow !== undefined ? {autoCompactWindow: compactWindow} : {}),
			...(prompt !== undefined && !forkRequested && definition?.cacheTtl !== undefined ? {cacheTtl: definition.cacheTtl} : {}),
			maxTurns,
			effort: prompt === undefined || forkRequested ? undefined : launch.effort,
			permissionMode,
			isolation: forkRequested ? undefined : definition?.isolation,
			timeoutMs: numberField(input.timeout_ms),
		},
	};
}

// Rebuild the same resolved shape from a finished record. `mode` keeps the two
// current reconstruction paths honest: resume re-reads the definition and the
// MCP scope; send uses what the record stored. Folding them into one function
// removes the duplicated 40-field fire calls without changing either behavior.
export function planFromRecord(
	record: JobRecord,
	instruction: string,
	mode: 'resume' | 'message',
	ctx: PlanContext,
): {ok: true; plan: JobPlan} | {ok: false; error: string} {
	const label = String(record.name ?? record.id);
	const jobId = record.id;
	const suffix = mode === 'resume' ? 'resume' : 'message';
	const resumeCwd = String(record.cwd ?? ctx.cwd);
	if (record.fork === true) {
		if (record.forkSessionId === undefined) {
			return {
				ok: false,
				error:
					mode === 'resume'
						? `subagent: job ${jobId} is a fork with no recorded session; spawn it again`
						: `${label} has finished and its fork session was never recorded; spawn it again`,
			};
		}
		return {
			ok: true,
			plan: {
				description: `${String(record.description ?? jobId)} (${suffix})`,
				kind: 'fork',
				prompt: instruction,
				background: record.teamName !== undefined ? false : true,
				cwd: resumeCwd,
				agentType: FORK_AGENT_TYPE,
				fork: true,
				...(record.mainSessionId !== undefined ? {mainSessionId: record.mainSessionId} : {}),
				resumeSession: record.forkSessionId,
				resumedFrom: jobId,
				...(record.permissionMode !== undefined ? {permissionMode: record.permissionMode} : {}),
				...(record.name !== undefined ? {name: record.name} : {}),
				...(record.teamName !== undefined ? {teamName: record.teamName} : {}),
				...(record.owner !== undefined ? {owner: record.owner} : {}),
			},
		};
	}
	if (record.acpSessionId === undefined && mode === 'message') {
		return {ok: false, error: `${label} has finished and has no resumable session; spawn it again`};
	}
	const definition = record.agentType === undefined ? undefined : ctx.resolveAgentDefinition(record.agentType);
	const resumeDefinition = definition?.ok === true ? definition.definition : undefined;
	// A resume is a fresh job: it follows the default (background) and the
	// definition's pinned background, not the mode of the run it resumes. A
	// message follows the same rule the spawn's record had; a teammate is
	// always foreground.
	const background =
		mode === 'message'
			? record.teamName !== undefined
				? false
				: true
			: record.teamName !== undefined
				? false
				: ctx.resolveExecutionPolicy({
						definitionBackground: resumeDefinition?.background,
						env: ctx.env,
					}) === 'background';
	// The MCP scope of a follow-up. A MESSAGE continues the same ACP session, so
	// its scope is the one the session was created with: the recorded list. A
	// RESUME re-reads the definition, so a policy or trust change between the
	// two runs is honored.
	const resumedMcp =
		resumeDefinition === undefined
			? (record.mcpServers ?? [])
			: ctx.resolveAgentMcpServers({
					defined: resumeDefinition.mcpServers ?? [],
					configured: ctx.mcpServerConfigs(ctx.fs, [ctx.home, ctx.runtimeCwd]),
					definition: resumeDefinition,
					env: ctx.env,
					trusted: ctx.agentFolderTrusted(resumeDefinition, {
						cwd: ctx.runtimeCwd,
						home: ctx.home,
						managedDir: ctx.managedDir,
						addDirs: ctx.addDirs,
					}),
				}).servers;
	const resumeCompact =
		mode === 'message'
			? record.autoCompactWindow
			: (record.autoCompactWindow ??
				ctx.resolveAutoCompactWindow(
					ctx.contextWindowForModel(record.model, ctx.env),
					resumeDefinition?.autoCompactWindow,
					false,
				));
	const resumeCacheTtl = mode === 'message' ? record.cacheTtl : (record.cacheTtl ?? resumeDefinition?.cacheTtl);
	return {
		ok: true,
		plan: {
			description: `${String(record.description ?? jobId)} (${suffix})`,
			kind: 'agent',
			prompt: instruction,
			background,
			cwd: resumeCwd,
			agentType: record.agentType,
			systemPrompt: mode === 'message' ? resumeDefinition?.prompt : ctx.systemPromptFor(resumeDefinition?.prompt),
			appendSystemPrompt: ctx.appendPromptText,
			// The flag says the worker's tool set must be enforced, and the worker
			// refuses to run when the mod did not load in the ACP child. The record
			// knows whether a restriction was applied - a plain agent has a resolved
			// tool list too, so the list alone cannot say "restricted". A record
			// from before the flag was saved falls back to the definition's lists.
			restrictedTools:
				record.restrictedTools === true ||
				(record.restrictedTools === undefined &&
					(resumeDefinition?.tools !== undefined || resumeDefinition?.disallowedTools !== undefined)),
			hooks: resumeDefinition?.hooks,
			startup: ctx.startupFor(resumeDefinition, resumeCwd, instruction, record.model),
			model: record.model,
			...(record.tools !== undefined ? {tools: record.tools} : {}),
			// The deny list is the one the RUN resolved, not the definition's. A
			// definition can deny Bash while the spawn overrode that with
			// `disallowed_tools: Write`; the first run could use Bash, and the
			// saved tool list includes it. Restoring the definition's list here
			// would block Bash on the resume and the work could not continue.
			...(record.disallowedTools !== undefined
				? {disallowedTools: record.disallowedTools}
				: resumeDefinition?.disallowedTools !== undefined
					? {disallowedTools: resumeDefinition.disallowedTools}
					: {}),
			// A message continues the same ACP session, so its scope is the one the
			// session was created with. A resume re-reads the definition.
			...(mode === 'message'
				? record.mcpServers !== undefined
					? {mcpServers: record.mcpServers}
					: {}
				: resumedMcp.length > 0
					? {mcpServers: resumedMcp}
					: {}),
			...(resumeCompact !== undefined ? {autoCompactWindow: resumeCompact} : {}),
			...(resumeCacheTtl !== undefined ? {cacheTtl: resumeCacheTtl} : {}),
			...(record.color !== undefined ? {color: record.color} : {}),
			maxTurns: record.maxTurns,
			// The per-invocation model and effort are what make a resume the same run
			// continued, not a fresh one with the definition's defaults.
			effort: record.effort,
			permissionMode: record.permissionMode,
			timeoutMs: record.timeoutMs,
			resumeSession: record.acpSessionId,
			resumedFrom: jobId,
			...(record.name !== undefined ? {name: record.name} : {}),
			...(record.teamName !== undefined ? {teamName: record.teamName} : {}),
			...(record.owner !== undefined ? {owner: record.owner} : {}),
		},
	};
}

// The worker session name, deterministic from the job id. It stays on the record
// for display and for records written before ACP; a resume targets acpSessionId.
export function sessionNameOf(id: string): string {
	return `subagent-${id}`;
}

// What the job is: one record, before anything is spawned.
export function recordFromPlan(
	intent: JobPlan,
	id: string,
	logPath: string,
	startedAt: number,
	owner: string | undefined,
	ctx: Pick<PlanContext, 'sessionId' | 'transcriptsDir'>,
): JobRecord {
	const isAgent = intent.prompt !== undefined;
	return {
		id,
		owner,
		description: intent.description,
		kind: intent.kind,
		...(intent.name !== undefined ? {name: intent.name} : {}),
		state: 'queued',
		// The resolved policy, not the raw `run_in_background`: env and definition
		// overrides are already applied by the caller.
		background: intent.background !== false,
		...(isAgent && intent.live !== false && intent.oneShot !== true ? {live: true} : {}),
		cwd: intent.cwd,
		logPath,
		startedAt,
		sessionId: ctx.sessionId(),
		updatedAt: startedAt,
		...(intent.depth !== undefined ? {depth: intent.depth} : {}),
		...(intent.parentJobId !== undefined ? {parentJobId: intent.parentJobId} : {}),
		...(isAgent
			? {
					// The durable resume handle. A one-shot agent has none, so nothing can
					// invite a resume that the lifecycle forbids.
					...(intent.oneShot === true ? {} : {agentId: id}),
					agentType: intent.agentType ?? 'general-purpose',
					sessionName: sessionNameOf(id),
					transcriptPath: join(ctx.transcriptsDir, `${id}.jsonl`),
					...(intent.resumeSession !== undefined ? {resumedFrom: intent.resumedFrom} : {}),
				}
			: {}),
		...(intent.fork === true
			? {fork: true, promptCache: 'shared', ...(intent.mainSessionId !== undefined ? {mainSessionId: intent.mainSessionId} : {})}
			: isAgent
				? {promptCache: 'own'}
				: {}),
		...(intent.teamName !== undefined ? {teamName: intent.teamName} : {}),
		...(intent.model !== undefined ? {model: intent.model} : {}),
		...(intent.tools !== undefined ? {tools: intent.tools} : {}),
		// The deny list the run resolved, so a resume restores the same one. The
		// definition's list is not the same thing: a spawn may override it.
		...(intent.disallowedTools !== undefined ? {disallowedTools: intent.disallowedTools} : {}),
		// Whether a restriction was applied. A plain agent has a resolved tool list
		// too, so the list alone cannot say "restricted" on a follow-up.
		...(intent.restrictedTools === true ? {restrictedTools: true} : {}),
		...(intent.mcpServers !== undefined ? {mcpServers: intent.mcpServers} : {}),
		...(intent.autoCompactWindow !== undefined ? {autoCompactWindow: intent.autoCompactWindow} : {}),
		...(intent.cacheTtl !== undefined ? {cacheTtl: intent.cacheTtl} : {}),
		...(intent.color !== undefined ? {color: intent.color} : {}),
		...(intent.maxTurns !== undefined ? {maxTurns: intent.maxTurns} : {}),
		...(intent.effort !== undefined ? {effort: intent.effort} : {}),
		...(intent.permissionMode !== undefined ? {permissionMode: intent.permissionMode} : {}),
		...(intent.isolation !== undefined ? {isolation: intent.isolation} : {}),
		...(intent.timeoutMs !== undefined && intent.timeoutMs > 0 ? {timeoutMs: intent.timeoutMs} : {}),
		...(intent.prompt !== undefined ? {prompt: intent.prompt} : {command: intent.command}),
	};
}
