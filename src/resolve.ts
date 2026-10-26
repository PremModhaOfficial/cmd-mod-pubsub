// Spawn-time resolution: which model and effort a worker runs with, and the
// single launch resolution that combines them with tool filtering. Pure
// functions only, so every precedence rule is testable without spawning a
// worker.

import {effortLevel, effortLevelFromEnv, subagentModel, subagentModelForced} from './config.ts';
import {EFFORT_LEVELS, filterTools, nonEmpty} from './tool-filter.ts';

export type ModelSource = 'parameter' | 'definition' | 'environment' | 'main' | 'inherit' | 'forced';

export interface ModelResolutionInput {
	/** The per-invocation `model` parameter. */
	requested?: string;
	/** The definition's `model` frontmatter. */
	definition?: string;
	env?: Record<string, string | undefined>;
	/** The main conversation's model; absent means the child inherits its default. */
	main?: string;
}

export interface ModelResolution {
	model: string | undefined;
	source: ModelSource;
}

// Parameter, frontmatter, environment, main. `inherit` (either place) selects the
// main model outright, so it outranks the environment default. FORCE makes the
// environment value win over the parameter and the frontmatter.
export function resolveModel(input: ModelResolutionInput = {}): ModelResolution {
	const env = input.env ?? {};
	const envModel = subagentModel(env);
	const forced = subagentModelForced(env);
	const main = nonEmpty(input.main);
	if (forced && envModel !== undefined) return {model: envModel, source: 'forced'};

	const requested = nonEmpty(input.requested);
	if (requested === 'inherit') return {model: main, source: 'inherit'};
	if (requested !== undefined) return {model: requested, source: 'parameter'};

	const definition = nonEmpty(input.definition);
	if (definition === 'inherit') return {model: main, source: 'inherit'};
	if (definition !== undefined) return {model: definition, source: 'definition'};

	if (envModel !== undefined) return {model: envModel, source: 'environment'};
	return {model: main, source: 'main'};
}

export type EffortSource = 'parameter' | 'definition' | 'environment' | 'none';

export interface EffortResolutionInput {
	/** The per-invocation `effort` parameter. */
	requested?: string;
	/** The definition's `effort` frontmatter. */
	definition?: string;
	env?: Record<string, string | undefined>;
}

export interface EffortResolution {
	effort: string | undefined;
	source: EffortSource;
}

// Per-invocation, frontmatter, environment. There is no effort inheritance from the
// main conversation: the runtime default applies when nothing is set.
export function resolveEffort(input: EffortResolutionInput = {}): EffortResolution {
	const env = input.env ?? {};
	const requested = effortLevel(input.requested, EFFORT_LEVELS);
	if (requested !== undefined) return {effort: requested, source: 'parameter'};
	const definition = effortLevel(input.definition, EFFORT_LEVELS);
	if (definition !== undefined) return {effort: definition, source: 'definition'};
	const fromEnv = effortLevelFromEnv(env, EFFORT_LEVELS);
	if (fromEnv !== undefined) return {effort: fromEnv, source: 'environment'};
	return {effort: undefined, source: 'none'};
}

export interface LaunchResolutionInput {
	requestedModel?: string;
	definitionModel?: string;
	requestedEffort?: string;
	definitionEffort?: string;
	env?: Record<string, string | undefined>;
	mainModel?: string;
	tools?: string[];
	disallowedTools?: string[];
	permissionMode?: string;
	background?: boolean;
	availableTools?: string[];
	mcpServers?: string[];
	team?: boolean;
}

export type LaunchResolution =
	| {
			ok: true;
			model: string | undefined;
			modelSource: ModelSource;
			effort: string | undefined;
			effortSource: EffortSource;
			tools: string[];
			removed: string[];
	  }
	| {ok: false; error: string};

// The one spawn-time resolution: tools, model, and effort from the same inputs, so
// the spec and the record cannot disagree about what was resolved.
export function resolveLaunch(input: LaunchResolutionInput = {}): LaunchResolution {
	const tools = filterTools({
		...(input.availableTools !== undefined ? {available: input.availableTools} : {}),
		...(input.mcpServers !== undefined ? {mcpServers: input.mcpServers} : {}),
		...(input.tools !== undefined ? {tools: input.tools} : {}),
		...(input.disallowedTools !== undefined ? {disallowedTools: input.disallowedTools} : {}),
		...(input.permissionMode !== undefined ? {permissionMode: input.permissionMode} : {}),
		...(input.background !== undefined ? {background: input.background} : {}),
		...(input.team !== undefined ? {team: input.team} : {}),
	});
	if (tools.ok === false) return tools;
	const model = resolveModel({
		...(input.requestedModel !== undefined ? {requested: input.requestedModel} : {}),
		...(input.definitionModel !== undefined ? {definition: input.definitionModel} : {}),
		...(input.env !== undefined ? {env: input.env} : {}),
		...(input.mainModel !== undefined ? {main: input.mainModel} : {}),
	});
	const effort = resolveEffort({
		...(input.requestedEffort !== undefined ? {requested: input.requestedEffort} : {}),
		...(input.definitionEffort !== undefined ? {definition: input.definitionEffort} : {}),
		...(input.env !== undefined ? {env: input.env} : {}),
	});
	return {
		ok: true,
		model: model.model,
		modelSource: model.source,
		effort: effort.effort,
		effortSource: effort.source,
		tools: tools.tools,
		removed: tools.removed,
	};
}