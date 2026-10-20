// The tool surface of the mod: the subagent_* tools and their handlers.
//
// The module owns the tool surface and nothing else. The entry passes in
// the session state it needs as a context object, and takes back the one
// reader the entry's own panels and commands also use.

import {join} from 'node:path';

import {
	ACTIVE_JOB_SCAN_MAX,
	FORK_AGENT_TYPE,
	HOST_RESERVED_AGENT_NAMES,
	OUTPUT_MAX_CHARS_CAP,
	OUTPUT_MAX_CHARS_DEFAULT,
	ROSTER_MAIN,
	WAIT_DEFAULT_MS,
	WAIT_MAX_MS,
	WAIT_POLL_MS,
} from './constants.ts';
import {crossSessionEnabled} from './config.ts';
import {AGENT_SCOPES} from './definitions.ts';
import type {AgentRegistry, AgentScope} from './definitions.ts';
import type {JobRecord} from './jobs.ts';
import {planFromInput, planFromRecord} from './plan.ts';
import type {PlanContext} from './plan.ts';
import {signalOf} from './signals.ts';
import {fire} from './spawn.ts';
import type {ForegroundResult, SpawnContext, WorkerSpec} from './spawn.ts';
import {canonicalState, isTerminal, isTerminalState, normalizeState, stateMarker} from './state.ts';
import type {Ledger} from './ledger.ts';
import type {Runtime, Signal, SpoolFs} from './types.ts';
import {isInside} from './worktree.ts';

export type ToolResult = {ok: boolean; error?: string; content?: {type: string; text: string}[]};

/** The one tool result that is fine. */
export const okText = (value: string): ToolResult => ({ok: true, content: [{type: 'text', text: value}]});
/** The one tool result that is a refusal. */
export const badInput = (error: string): ToolResult => ({ok: false, error});

export interface ToolsContext {
	/** The host API: the tools are registered on it. */
	readonly cmd: {addTool(tool: never): void};
	readonly planCtx: PlanContext;
	readonly spawnCtx: SpawnContext;
	readonly runtime: Runtime;
	readonly spool: SpoolFs;
	readonly logsDir: string;
	readonly transcriptsDir: string;
	readonly readJob: (id: string) => JobRecord | undefined;
	readonly writeJob: (record: JobRecord) => void;
	readonly readRecentJobs: (limit: number) => JobRecord[];
	readonly refreshJobs: () => void;
	readonly enqueueSignal: (signal: Signal) => void;
	readonly safeDrain: () => void;
	readonly cancelJob: (id: string) => {ok: boolean; message: string};
	readonly registry: AgentRegistry;
	readonly ledger: Ledger;
	readonly currentOwner: () => string | undefined;
	readonly ownerId: () => string | undefined;
	readonly rosterNames: () => string[];
	readonly namedJob: (name: string) => JobRecord | undefined;
	readonly liveState: (record: JobRecord) => {alive: boolean; busy: boolean};
	readonly appendLive: (id: string, from: string, message: string) => boolean;
	readonly workerMode: boolean;
	/** The worker spec is bound late, so it is read through a function. */
	readonly workerSpec: () => WorkerSpec | undefined;
	/** The session id is bound late, so it is read through a function. */
	readonly sessionId: () => string | undefined;
	readonly forkModeEnabled: boolean;
	readonly listText: (filter?: {state?: string; limit?: number}) => string;
	/** The scanner: one rule table, shared with the worker. */
	readonly scanOutput: (text: string) => {text: string; matches: string[]; marked: boolean};
	/** The parent report: the worker's own progress lines are not part of it. */
	readonly reportText: (text: string, kind: string) => string;
	/** The agents a foreground conversation can address. */
	readonly listAgentsVisible: (options: {background?: boolean; crossSessionMessaging?: boolean}) => boolean;
}

export interface Tools {
	/** Register the whole tool surface. */
	register(): void;
	/** Read one job: its state, exit code, duration and owner, plus a slice of its log. */
	outputText(id: string, input: Record<string, unknown>): ToolResult;
	/** Deliver a message to a named agent: a live worker queues it, an idle one starts a turn. */
	sendToRecord(record: JobRecord, message: string, from: string): Promise<ToolResult>;
}

export function createTools(ctx: ToolsContext): Tools {
	const {
		cmd,
		planCtx,
		spawnCtx,
		runtime,
		spool,
		logsDir,
		transcriptsDir,
		readJob,
		writeJob,
		readRecentJobs,
		refreshJobs,
		enqueueSignal,
		safeDrain,
		cancelJob,
		registry,
		ledger,
		currentOwner,
		ownerId,
		rosterNames,
		namedJob,
		liveState,
		appendLive,
		workerMode,
		workerSpec,
		sessionId,
		forkModeEnabled,
		listText,
		scanOutput,
		reportText,
		listAgentsVisible,
	} = ctx;

	// `subagent_*` is the surface. `pubsub_*` is registered from the same run function
	// for one release: an installed prompt, a script, or a habit that still calls the
	// old name keeps working, and cannot drift from the new one because there is only
	// one implementation.
	function addSurface(
		name: string,
		description: string,
		inputSchema: Record<string, unknown>,
		run: (args: {input: Record<string, unknown>}) => Promise<ToolResult>,
		options: {readOnly?: boolean; aliases?: string[]} = {},
	): void {
		const readOnly = options.readOnly === true;
		cmd.addTool({schema: {name, description, input_schema: inputSchema}, ...(readOnly ? {readOnly} : {}), run} as never);
		for (const alias of options.aliases ?? []) {
			cmd.addTool({
				schema: {name: alias, description: `[deprecated: use ${name}] ${description}`, input_schema: inputSchema},
				...(readOnly ? {readOnly} : {}),
				run,
			} as never);
		}
	}

	// The fire-and-forget tool. Returns the moment the worker is detached; the result
	// arrives later as a signal, never as a tool result.

			const spawnRun = async ({input}: {input: Record<string, unknown>}): Promise<ToolResult> => {
				const planned = planFromInput(input, planCtx);
				if (planned.ok === false) return badInput(planned.error);
				const plan = planned.plan;
				const launched = await fire(plan, spawnCtx);
				if (launched.ok === false) return badInput(launched.error);
				// A one-shot (Explore/Plan) has no agent id: the record is the only handle.
				const spawnedAgentId = readJob(launched.id)?.agentId;
				// A foreground run returns its result here; a background run returns the moment
				// the worker is detached.
				if (launched.result !== undefined) {
					if (launched.result.ok === false) return badInput(launched.result.error);
					return okText(
						spawnedAgentId !== undefined ? `agent id ${spawnedAgentId}\n${launched.result.text}` : launched.result.text,
					);
				}
				const kind =
					plan.kind === 'command'
						? 'shell (/bin/sh -c)'
						: `${plan.fork === true ? 'fork' : 'agent'} (${String(plan.agentType ?? 'agent')})${plan.teamName !== undefined ? ` on team ${plan.teamName}` : ''}`;
				return okText(
					`spawned ${launched.id} "${plan.description}" as ${kind}, pid ${launched.pid}.\n` +
						(spawnedAgentId !== undefined ? `agent id ${spawnedAgentId}\n` : '') +
						`log ${join(logsDir, `${launched.id}.log`)}\n` +
						`The result arrives here as a signal when it exits - do not wait for it. ` +
						`subagent_output ${launched.id} reads the live log; subagent_cancel ${launched.id} stops it.`,
				);
			};

			const listRun = async ({input}: {input: Record<string, unknown>}): Promise<ToolResult> => {
				refreshJobs();
				const limit = Number(input.limit);
				return okText(
					listText({
						state: typeof input.state === 'string' && input.state.length > 0 ? input.state : undefined,
						limit: Number.isFinite(limit) && limit > 0 ? Math.round(limit) : undefined,
					}),
				);
			};

			function readLog(file: string): string {
				try {
					return spool.readFile(file);
				} catch {
					return '';
				}
			}

			// The persisted conversation, rendered from the worker's JSONL stream. A
			// malformed line is skipped; it never hides the rest of the transcript.
			function transcriptText(id: string, record: JobRecord): string {
				const file = String(record.transcriptPath ?? join(transcriptsDir, `${id}.jsonl`));
				let raw = '';
				try {
					raw = spool.readFile(file);
				} catch {
					return '';
				}
				const parts: string[] = [];
				for (const line of raw.split('\n')) {
					if (!line.trim()) continue;
					try {
						const entry = JSON.parse(line) as {
							type?: unknown;
							line?: unknown;
							update?: {sessionUpdate?: unknown; content?: {text?: unknown}; title?: unknown; kind?: unknown};
						};
						const update = entry.update;
						if (update?.sessionUpdate === 'agent_message_chunk' && typeof update.content?.text === 'string') {
							parts.push(update.content.text);
						} else if (update?.sessionUpdate === 'tool_call') {
							parts.push(`[tool] ${String(update.title ?? update.kind ?? 'call')}\n`);
						} else if (typeof entry.line === 'string') {
							parts.push(`${entry.line}\n`);
						}
					} catch {
						/* one malformed line does not hide the transcript */
					}
				}
				return parts.join('');
			}

			// The transcript of one job: the record's facts, the log and transcript paths,
			// and a bounded slice of the conversation. A finished job falls back to its
			// transcript when the log is gone; `offset` is a character offset from the start.
			function outputText(id: string, input: Record<string, unknown>): ToolResult {
				const record = readJob(id);
				if (record === undefined) return badInput(`subagent: no job ${id}`);
				const state = canonicalState(record.state);
				const file = String(record.logPath ?? join(logsDir, `${id}.log`));
				const transcriptPath = String(record.transcriptPath ?? join(transcriptsDir, `${id}.jsonl`));
				const transcript = record.kind === 'agent' || record.kind === 'fork' ? transcriptText(id, record) : '';
				const logText = readLog(file);
				const log = logText.length > 0 ? logText : transcript;
				const requested = Number(input.max_chars);
				const maxChars = Math.min(
					OUTPUT_MAX_CHARS_CAP,
					Number.isFinite(requested) && requested > 0 ? Math.max(200, Math.round(requested)) : OUTPUT_MAX_CHARS_DEFAULT,
				);
				const offset = Number(input.offset);
				let body: string;
				let note = '';
				if (input.offset !== undefined && Number.isFinite(offset) && offset >= 0) {
					body = log.slice(offset, offset + maxChars);
					note = `\n[chars ${offset}-${Math.min(log.length, offset + maxChars)} of ${log.length}]`;
				} else if (log.length > maxChars) {
					body = log.slice(-maxChars);
					note = `\n[last ${maxChars} of ${log.length} chars]`;
				} else {
					body = log;
				}
				const facts = [
					state,
					record.partial === true ? 'partial' : '',
					typeof record.agentId === 'string' ? `agent ${record.agentId}` : '',
					typeof record.exitCode === 'number' ? `exit ${record.exitCode}` : '',
					typeof record.durationMs === 'number' ? `${(record.durationMs / 1000).toFixed(1)}s` : '',
					typeof record.model === 'string' && record.model.length > 0 ? record.model : '',
					typeof record.agentType === 'string' ? record.agentType : '',
					typeof record.owner === 'string' && record.owner.length > 0 ? `owner ${record.owner.slice(-6)}` : '',
				].filter(Boolean).join(' · ');
				const timing =
					state === 'queued' || state === 'running'
						? `\n(running ${Math.max(0, Math.round((runtime.now() - Number(record.startedAt ?? runtime.now())) / 1000))}s; the result will arrive as a signal)`
						: '';
				// Reading the log is still worker output reaching the parent: scan it here too,
				// and hide the worker's own tool-call progress.
				const visible = reportText(body, String(record.kind ?? 'command'));
				return okText(
					`${stateMarker(record.state)} ${id} "${String(record.description ?? '(unlabelled)')}" · ${facts}\nlog ${file}` +
						(record.transcriptPath !== undefined ? `\ntranscript ${transcriptPath}` : '') +
						`${timing}\n\n` +
						(visible.length > 0 ? visible : '(no output yet)') +
						note,
				);
			}

			const outputRun = async ({input}: {input: Record<string, unknown>}): Promise<ToolResult> => {
				const id = String(input.id ?? '').trim();
				if (id.length === 0) return badInput('id is required');
				if (readJob(id) === undefined) return badInput(`subagent: no job ${id}`);
				const waitMs = Number(input.wait_ms);
				const wantsWait = input.wait === true || (Number.isFinite(waitMs) && waitMs > 0);
				if (wantsWait) {
					// Bounded on purpose: a tool that waits is a turn held open, so the default is
					// short and the cap is hard. The loop is counted, not clock-only, so a frozen
					// clock cannot spin it forever.
					const budget = Math.min(WAIT_MAX_MS, Number.isFinite(waitMs) && waitMs > 0 ? waitMs : WAIT_DEFAULT_MS);
					const rounds = Math.ceil(budget / WAIT_POLL_MS) + 1;
					for (let round = 0; round < rounds; round += 1) {
						const record = readJob(id);
						if (record === undefined || isTerminalState(record.state)) break;
						await runtime.sleep(WAIT_POLL_MS);
					}
				}
				return outputText(id, input);
			};

			const cancelRun = async ({input}: {input: Record<string, unknown>}): Promise<ToolResult> => {
				const id = String(input.id ?? '').trim();
				if (id.length === 0) return badInput('id is required');
				const result = cancelJob(id);
				return result.ok ? okText(result.message) : badInput(result.message);
			};

			const resumeRun = async ({input}: {input: Record<string, unknown>}): Promise<ToolResult> => {
				const id = String(input.id ?? '').trim();
				if (id.length === 0) return badInput('id is required');
				const instruction = String(input.prompt ?? '').trim();
				if (instruction.length === 0) return badInput('prompt is required: it is the instruction for the resumed run');
				// The agent id is the durable handle; the job id also resolves, so a caller
				// holding either from the result can resume.
				const record =
					readJob(id) ??
					readRecentJobs(ACTIVE_JOB_SCAN_MAX).find(job => typeof job.agentId === 'string' && job.agentId === id);
				if (record === undefined) return badInput(`subagent: no job or agent ${id}`);
				const jobId = record.id;
				const state = normalizeState(record.state);
				if (state !== undefined && !isTerminal(state)) return badInput(`subagent: job ${jobId} is still ${state}`);
				if (record.prompt === undefined) {
					return badInput(`subagent: job ${jobId} is a shell job; there is no agent session to resume`);
				}
				// Explore and Plan are one-shot: the lifecycle gives them no agent id, so a
				// resume would pretend to continue a context that was never addressable. An
				// older record written before agent ids is refused for the same reason.
				if (record.fork !== true && record.agentId === undefined) {
					return badInput(
						`subagent: job ${jobId} has no agent id (${String(record.agentType ?? 'explore or plan')} is one-shot, or the record predates agent ids); it cannot be resumed`,
					);
				}
				// A live worker already owns this session. Starting a second worker would race
				// the first over it, so the live path is the message path.
				if (liveState(record).alive) {
					return badInput(
						`subagent: job ${jobId} is still running; send it a live message with subagent_send` +
							(record.name === undefined ? ' once it finishes, or wait for its result' : `({to: "${record.name}", message}) instead`),
					);
				}
				// Resume the ACP session the first run created, so the new instruction continues
				// with the context the worker already has. An older record without one starts a
				// fresh session (the definition body still applies). A fork resumes its fork
				// session instead; forking again would lose the context it already has.
				const planned = planFromRecord(record, instruction, 'resume', planCtx);
				if (planned.ok === false) return badInput(planned.error);
				const launched = await fire(planned.plan, spawnCtx);
				if (launched.ok === false) return badInput(launched.error);
				const resumedAgentId = readJob(launched.id)?.agentId;
				return launchedToolResult(
					launched,
					resumedAgentId,
					`resumed ${jobId} as ${launched.id} "${String(record.description ?? jobId)} (resume)", pid ${launched.pid}.\n` +
						(resumedAgentId !== undefined ? `agent id ${resumedAgentId}\n` : '') +
						`log ${join(logsDir, `${launched.id}.log`)}\n` +
						(record.fork === true
							? 'The resumed fork reports back here as its own signal.'
							: 'The resumed run reports back here as its own signal.'),
				);
			};

			// The read side of the team: definitions, addressable agents, and the agent's
			// explicit final report. `ListAgents` is registered only where it belongs.
			function agentsToolText(scope?: string): string {
				registry.refresh();
				const wanted = String(scope ?? '').trim().toLowerCase();
				const entries = registry.entries().filter(entry => wanted.length === 0 || entry.scope === wanted);
				if (entries.length === 0) return `no${wanted.length > 0 ? ` ${wanted}` : ''} agent definitions`;
				const lines = entries.map(entry => {
					const state = entry.valid ? 'valid' : `invalid: ${entry.reason ?? 'unknown'}`;
					const model = entry.definition?.model !== undefined ? ` model=${entry.definition.model}` : '';
					const tools = entry.definition?.tools !== undefined ? ` tools=${entry.definition.tools.join(',')}` : '';
					const source = entry.sourcePath.length > 0 ? ` · ${entry.sourcePath}` : '';
					return `  ${entry.name.length > 0 ? entry.name : '(unnamed)'} · ${entry.scope} · ${state}${model}${tools}${source}`;
				});
				return `${entries.length} definition(s)${wanted.length > 0 ? ` in ${wanted}` : ''}:\n${lines.join('\n')}`;
			}

			function listAgentsText(): string {
				const lines = [`${ROSTER_MAIN} — this conversation`];
				for (const name of rosterNames()) {
					const record = namedJob(name);
					if (record === undefined) continue;
					lines.push(`${name} — ${canonicalState(record.state)} (job ${record.id})`);
				}
				return lines.join('\n');
			}

			const noteRun = async ({input}: {input: Record<string, unknown>}): Promise<ToolResult> => {
				const title = typeof input.title === 'string' ? input.title : '';
				if (title.length === 0) return badInput('title is required');
				const body = typeof input.body === 'string' ? input.body : '';
				const owner = currentOwner();
				const id = `direct-${runtime.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
				enqueueSignal({
					id,
					owner,
					description: title,
					kind: 'note',
					state: 'note',
					exitCode: null,
					title,
					body,
					sessionId: sessionId(),
				});
				safeDrain();
				refreshJobs();
				return okText(ledger.has(id) ? 'delivered' : 'queued for this session');
			};

			// One reading of a fire() result, shared by resume and send. A foreground run
			// already returned its result inside the call; returning the "it reports back
			// as a signal" text instead would discard it.
			function launchedToolResult(
				launched: {readonly id: string; readonly result?: ForegroundResult},
				agentId: string | undefined,
				asyncText: string,
			): ToolResult {
				if (launched.result !== undefined) {
					if (launched.result.ok === false) return badInput(launched.result.error);
					return okText(agentId !== undefined ? `agent id ${agentId}\n${launched.result.text}` : launched.result.text);
				}
				return okText(asyncText);
			}

			// Live messaging. The target is a name, never a pid: the record decides whether
			// the message is queued to a live worker or starts a new turn in its finished
			// session, so a resume keeps the session the agent already has.
			async function sendToRecord(record: JobRecord, message: string, from: string): Promise<ToolResult> {
				const label = String(record.name ?? record.id);
				const {alive, busy} = liveState(record);
				if (record.fork !== true && record.agentId === undefined && record.live !== true) {
					return badInput(`subagent: ${label} is one-shot (no agent id); it cannot be messaged`);
				}
				if (alive) {
					if (!appendLive(record.id, from, message)) return badInput(`cannot reach ${label}: its control queue is not writable`);
					return okText(
						`${busy ? `queued for ${label}'s next safe point` : `sent to idle ${label}; it starts a turn`} (job ${record.id}).\n` +
							`The reply arrives here as its own message signal.`,
					);
				}
				const planned = planFromRecord(record, message, 'message', planCtx);
				if (planned.ok === false) return badInput(planned.error);
				const launched = await fire(planned.plan, spawnCtx);
				if (launched.ok === false) return badInput(launched.error);
				return launchedToolResult(
					launched,
					readJob(launched.id)?.agentId,
					`resumed ${label} as ${launched.id} "${String(record.description ?? record.id)} (message)", pid ${launched.pid}.\n` +
						`The reply arrives here as its own signal.`,
				);
			}

			const sendLiveRun = async ({input}: {input: Record<string, unknown>}): Promise<ToolResult> => {
				const to = String(input.to ?? '').trim();
				const message = typeof input.message === 'string' ? input.message : '';
				if (to.length === 0) return badInput('to is required: a named agent or main');
				if (message.trim().length === 0) return badInput('message is required and must not be empty');
				const from = workerMode ? String(workerSpec()?.name ?? 'worker') : ROSTER_MAIN;
				if (to === ROSTER_MAIN) {
					const id = `live-${runtime.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
					const scan = scanOutput(message);
					enqueueSignal({
						id,
						owner: workerMode ? workerSpec()?.owner : ownerId(),
						description: `message from ${from}`,
						kind: 'message',
						state: 'note',
						exitCode: null,
						title: `message from ${from}`,
						body: scan.text,
						sessionId: workerMode ? workerSpec()?.sessionId : sessionId(),
						...(workerMode ? {scanned: true, scanMatches: scan.matches} : {}),
					});
					if (!workerMode) {
						safeDrain();
						refreshJobs();
					}
					return okText(`sent a message to ${ROSTER_MAIN} from ${from}`);
				}
				const record = namedJob(to);
				if (record === undefined) {
					const names = rosterNames();
					return badInput(
						`no agent named "${to}"${names.length > 0 ? `; addressable: ${names.join(', ')}` : '; no named agents in this session'}`,
					);
				}
				return sendToRecord(record, message, from);
			};


		// The registration order is the surface order. The aliases register next to
		// the tool they stand in for, so both share one implementation.
		function register(): void {
					addSurface(
						'subagent_spawn',
						'Start a subagent. With `run_in_background: false` the call waits and returns the result; otherwise it returns immediately and the result arrives later as a separate message on its own - do not poll for it, and do not wait. Pass `prompt` for a headless Command Code agent, or `command` for a shell command. A definition with `background: true` or CLAUDE_CODE_DISABLE_BACKGROUND_TASKS can override the requested mode. Pass `team_name` to join an agent team as a foreground teammate. Pass `subagent_type: fork` to inherit the main conversation.' +
							(forkModeEnabled ? ' Fork mode is on: agent jobs run in the background and `run_in_background` is not accepted.' : ''),
						{
							type: 'object',
							properties: (() => {
								const properties: Record<string, unknown> = {
									description: {type: 'string', description: 'Short name for this job, used in the signal title.'},
									label: {type: 'string', description: 'Alias for description.'},
									name: {type: 'string', description: 'Address this agent later with subagent_send. A named agent appears in its siblings\' roster.'},
									team_name: {type: 'string', description: 'Join this agent team. A teammate runs in the foreground and keeps the task and cron tools.'},
									prompt: {type: 'string', description: 'Prompt for a detached headless Command Code agent.'},
									command: {type: 'string', description: 'Shell command to run detached under /bin/sh -c.'},
									agent_type: {type: 'string', description: "Deprecated alias for subagent_type. Use subagent_type."},
									subagent_type: {
										type: 'string',
										description:
											"Agent definition to run, by name. Omit for 'general-purpose'. A project or user definition in .commandcode/agents/ overrides the built-ins.",
									},
									model: {type: 'string', description: "Model for an agent job. Overrides the definition's model; 'inherit' means the session model."},
									effort: {type: 'string', description: "Reasoning effort for an agent job: low, medium, high, xhigh, or max. Overrides the definition's effort."},
									tools: {type: 'string', description: 'Comma-separated tool list (or an array). Only these tools remain; overrides the definition.'},
									disallowed_tools: {type: 'string', description: 'Comma-separated tool list (or an array) to remove. Wins over tools.'},
									max_turns: {type: 'number', description: 'Cap the agent conversation turns.'},
									permission_mode: {
										type: 'string',
										description:
											"Permission mode for an agent job: default, acceptEdits, auto, dontAsk, bypassPermissions, plan, or manual. The main session's mode wins when it is acceptEdits, auto, or bypassPermissions; bypassPermissions otherwise requires the main session to be in it.",
									},
									timeout_ms: {type: 'number', description: 'Kill the job after this many milliseconds and report a timeout.'},
									run_in_background: {
										type: 'boolean',
										description:
											'false waits for the run and returns its result in this call; true (or omitted) returns immediately and delivers the result as a signal. A definition pinned to background, or CLAUDE_CODE_DISABLE_BACKGROUND_TASKS, can override this.',
									},
									cwd: {type: 'string', description: 'Working directory. Defaults to the mod workspace root.'},
									};
								// Where fork mode is on, the model cannot ask for the foreground: the
								// parameter is absent, not merely ignored.
								if (forkModeEnabled) delete properties.run_in_background;
								return properties;
							})(),
							required: [],
						},
						spawnRun,
						{aliases: ['pubsub_fire']},
					);

					addSurface(
						'subagent_list',
						'List spawned jobs with their lifecycle states, subagent_type, agent name, model, elapsed time and owner, and count the signals still waiting for this session.',
						{
							type: 'object',
							properties: {
								state: {type: 'string', description: 'Only jobs in this state (queued, running, waiting-input, done, failed, canceled).'},
								limit: {type: 'number', description: 'Show at most this many jobs.'},
							},
							required: [],
						},
						listRun,
						{readOnly: true, aliases: ['pubsub_list']},
					);

					addSurface(
						'subagent_output',
						'Read one job: its state, exit code, duration and owner, plus a bounded slice of its log. By default the tail; `offset` reads from a character offset, `max_chars` bounds the slice.',
						{
							type: 'object',
							properties: {
								id: {type: 'string', description: 'Job id from subagent_spawn or subagent_list.'},
								wait: {type: 'boolean', description: 'Block briefly until the job finishes (bounded, 10s by default).'},
								wait_ms: {type: 'number', description: 'How long to wait, capped at 60s.'},
								offset: {type: 'number', description: 'Character offset into the log.'},
								max_chars: {type: 'number', description: 'Maximum characters to return.'},
							},
							required: ['id'],
						},
						outputRun,
						{readOnly: true},
					);

					addSurface(
						'subagent_cancel',
						'Cancel a queued or running subagent job by id (SIGTERM, then SIGKILL after 1s). The firing session receives one canceled signal.',
						{
							type: 'object',
							properties: {id: {type: 'string', description: 'Job id from subagent_list or subagent_spawn.'}},
							required: ['id'],
						},
						cancelRun,
						{aliases: ['pubsub_cancel']},
					);

					addSurface(
						'subagent_resume',
						"Resume a finished agent job in its own worker session and continue it with a new instruction. The resumed run is a new job with its own signal.",
						{
							type: 'object',
							properties: {
								id: {type: 'string', description: 'Job id of the finished agent job.'},
								prompt: {type: 'string', description: 'The instruction for the resumed run.'},
							},
							required: ['id', 'prompt'],
						},
						resumeRun,
					);

					addSurface(
						'subagent_agents',
						'List the available agent definitions: scope, model, tools, validity, and the file that provides each one. Pass `scope` to filter to project, user, or builtin.',
						{
							type: 'object',
							properties: {
								scope: {type: 'string', description: 'Only definitions from this scope (project, user, builtin).'},
							},
							required: [],
						},
						async ({input}: {input: Record<string, unknown>}) =>
							okText(agentsToolText(typeof input.scope === 'string' ? input.scope : undefined)),
						{readOnly: true},
					);

					const canMessageAcrossSessions = crossSessionEnabled(runtime.env);
					if (listAgentsVisible({background: workerMode ? workerSpec()?.background === true : false, crossSessionMessaging: canMessageAcrossSessions})) {
						addSurface(
							'ListAgents',
							'List the agents this conversation can message with subagent_send: main plus every named agent, with its current state and job id.',
							{type: 'object', properties: {}, required: []},
							async () => okText(listAgentsText()),
							{readOnly: true},
						);
					}

					if (workerSpec() !== undefined) {
						addSurface(
							'SubagentHandback',
							'Hand your final report back to the conversation that receives this run\'s result. The report is delivered as its own message; end the turn afterwards. The wrapper suppresses the duplicate end-of-turn result.',
							{
								type: 'object',
								properties: {
									report: {type: 'string', description: 'The final report for the parent conversation.'},
								},
								required: ['report'],
							},
							async ({input}: {input: Record<string, unknown>}): Promise<ToolResult> => {
								const report = String(input.report ?? '').trim();
								if (report.length === 0) return badInput('report is required and must not be empty');
								const id = String(workerSpec()?.id ?? '');
								const record = id.length === 0 ? undefined : readJob(id);
								if (record === undefined) return badInput(`subagent: no job ${id || '(worker)'}`);
								const now = runtime.now();
								const scan = scanOutput(report);
								record.handbackAt = now;
								record.updatedAt = now;
								writeJob(record);
								enqueueSignal(
									signalOf({...record, id: `${id}-handback-${now.toString(36)}`}, {
										state: 'done',
										finishedAt: now,
										durationMs: Math.max(0, now - Number(record.startedAt ?? now)),
										body: scan.text,
										scanned: true,
										scanMatches: scan.matches,
										title: `${String(record.description ?? id)} handback`,
									}),
								);
								refreshJobs();
								return okText(`handed the report back (job ${id}); it reaches the parent as one message`);
							},
						);
					}

					addSurface(
						'subagent_send',
						'Send a live message to a named agent (or "main"). A running agent queues it and answers at its next safe point; an idle agent starts a turn; a finished agent resumes its session with full context. The reply arrives later as its own message signal, so do not wait for it here.',
						{
							type: 'object',
							properties: {
								to: {type: 'string', description: 'A named agent, or "main" for this conversation.'},
								message: {type: 'string', description: 'The message to deliver. The target acts on it in its own session.'},
							},
							required: ['to', 'message'],
						},
						sendLiveRun,
					);

					// The 0.1.0 note channel. It is NOT an alias of live messaging: it publishes a
					// note into this session, while subagent_send delivers to another agent. It is
					// removed one release after the rename.
					addSurface(
						'pubsub_send',
						'[deprecated: live messaging moved to subagent_send] Publish a note into the agent loop of THIS session. It is delivered as a visible user turn. This is not live messaging to another agent.',
						{
							type: 'object',
							properties: {
								title: {type: 'string', description: 'One line, shown in the signal header.'},
								body: {type: 'string', description: 'The detail.'},
							},
							required: ['title'],
						},
						noteRun,
					);

		}

		return {register, outputText, sendToRecord};
}