// ---------------------------------------------------------------------------
// Configuration: the environment and the host options
//
// One place knows how the mod is configured. Every setting is read under the
// same two rules:
//
//   - The mod's own `BETTER_SUBAGENTS_*` name wins over its documented
//     `CLAUDE_CODE_*` alias. The alias answers only when the mod's name is
//     unset - except for the settings that mean "either name says yes" (the
//     model force, the background disable, the SDK flag), which is spelled out
//     at the reader that owns it.
//   - A host option is a string, and an option that was never set reads as the
//     empty string. A deprecated option name answers only while the current
//     name still sits on its own default, so an upgraded machine keeps working
//     for one release.
//
// Each reader names one documented setting, so the setting is the contract and
// the environment variable appears in this file only. Every reader takes what
// it reads as a parameter - the env record, the option source, the home
// directory - so this module touches no session state and reads no file.
//
// The module is a leaf: only `./constants.ts` and node builtins.
// ---------------------------------------------------------------------------

import {join} from 'node:path';
import {
	BACKGROUND_DISABLE_ENV,
	BACKGROUND_DISABLE_ENV_ALIAS,
	FORK_MODE_ENV,
	FORK_MODE_ENV_ALIAS,
	MANAGED_SETTINGS_DEFAULT,
	SDK_ENV,
	SDK_ENV_ALIAS,
	WORKER_ENV,
} from './constants.ts';

/** The environment the mod reads. A name that is not set reads as undefined. */
export type Env = Readonly<Record<string, string | undefined>>;

/** What reads a host option: the host that owns the flags the mod registered. */
export interface OptionSource {
	getFlag(name: string): unknown;
}

/** The MCP policy the environment asks for. */
export interface McpPolicy {
	/** Bare mode: the session has no MCP server at all. */
	readonly bare: boolean;
	/** Strict mode: an inline server is ignored. */
	readonly strict: boolean;
	/** The servers that may be used; `*` allows every one of them. */
	readonly allow: readonly string[];
	/** The servers that may not be used; `*` denies every one of them. */
	readonly deny: readonly string[];
}

/** Where the managed and the user policy files live. */
export interface PolicyFiles {
	/** The organization-wide policy; the managed settings directory's file. */
	readonly managed: string;
	/** The user's own policy file. */
	readonly user: string;
}

// The settings this module owns, each with its documented alias.
const SUBAGENT_MODEL_ENV = 'BETTER_SUBAGENTS_SUBAGENT_MODEL';
const SUBAGENT_MODEL_ENV_ALIAS = 'CLAUDE_CODE_SUBAGENT_MODEL';
const SUBAGENT_MODEL_FORCE_ENV = 'BETTER_SUBAGENTS_SUBAGENT_MODEL_FORCE';
const SUBAGENT_MODEL_FORCE_ENV_ALIAS = 'CLAUDE_CODE_SUBAGENT_MODEL_FORCE';
const EFFORT_LEVEL_ENV = 'BETTER_SUBAGENTS_EFFORT_LEVEL';
const EFFORT_LEVEL_ENV_ALIAS = 'CLAUDE_CODE_EFFORT_LEVEL';
const CONTEXT_WINDOW_ENV = 'BETTER_SUBAGENTS_CONTEXT_WINDOW';
const CONTEXT_WINDOW_ENV_ALIAS = 'CLAUDE_CODE_CONTEXT_WINDOW';
const BARE_ENV = 'BETTER_SUBAGENTS_BARE';
const BARE_ENV_ALIAS = 'CLAUDE_CODE_BARE';
const STRICT_MCP_CONFIG_ENV = 'BETTER_SUBAGENTS_STRICT_MCP_CONFIG';
const STRICT_MCP_CONFIG_ENV_ALIAS = 'CLAUDE_CODE_STRICT_MCP_CONFIG';
const MCP_ALLOW_ENV = 'BETTER_SUBAGENTS_MCP_ALLOW';
const MCP_ALLOW_ENV_ALIAS = 'CLAUDE_CODE_MCP_ALLOW';
const MCP_DENY_ENV = 'BETTER_SUBAGENTS_MCP_DENY';
const MCP_DENY_ENV_ALIAS = 'CLAUDE_CODE_MCP_DENY';
const MANAGED_CLAUDE_MD_ENV = 'BETTER_SUBAGENTS_MANAGED_CLAUDE_MD';
const USER_CLAUDE_MD_ENV = 'BETTER_SUBAGENTS_USER_CLAUDE_MD';
const AGENT_ENV = 'BETTER_SUBAGENTS_AGENT';
const AGENT_ENV_ALIAS = 'CLAUDE_CODE_AGENT';
const CROSS_SESSION_ENV = 'BETTER_SUBAGENTS_CROSS_SESSION';
const MAIN_PERMISSION_MODE_ENV = 'BETTER_SUBAGENTS_MAIN_PERMISSION_MODE';
const PUB_SUB_WORKER_ENV = 'PUBSUB_WORKER';
const COMMANDCODE_SDK_ENV = 'COMMANDCODE_SDK';

// The mod option that points at the managed settings directory.
const MANAGED_DIR_OPTION = 'managed_dir';

// ---------------------------------------------------------------------------
// The shared rules
// ---------------------------------------------------------------------------

// The shared mechanical core for a setting with a mod name and a documented
// alias: read the mod name first, and let the alias answer only when the mod
// name's reader yields nothing. `read` is the setting's own parser, so a blank
// or invalid mod value can decline to answer and fall through to the alias.
function aliased<T>(
	env: Env,
	primary: string,
	alias: string,
	read: (value: string | undefined) => T | undefined,
): T | undefined {
	const primaryValue = read(env[primary]);
	if (primaryValue !== undefined) return primaryValue;
	return read(env[alias]);
}

// The value of `name`, or of its alias when the mod's name is unset. A name
// that is set to the empty string is set: the alias does not answer for it.
function aliasValue(env: Env, name: string, alias: string): string | undefined {
	return aliased(env, name, alias, value => value);
}

// A comma-separated list; blank entries are dropped.
function commaList(value: string | undefined): string[] {
	return String(value ?? '')
		.split(',')
		.map(item => item.trim())
		.filter(Boolean);
}

// The two spellings of "on" the host and the workers use.
function isOn(value: string | undefined): boolean {
	return value === '1' || value === 'true';
}

// ---------------------------------------------------------------------------
// The environment settings
// ---------------------------------------------------------------------------

/**
 * The model the environment asks for. The first of the two names that carries
 * text wins; a blank setting is no setting, so the alias answers instead.
 */
export function subagentModel(env: Env): string | undefined {
	return aliased(env, SUBAGENT_MODEL_ENV, SUBAGENT_MODEL_ENV_ALIAS, value => {
		const trimmed = value?.trim();
		return trimmed !== undefined && trimmed.length > 0 ? trimmed : undefined;
	});
}

/**
 * Whether the environment's model wins over the parameter and the frontmatter.
 * Either name says yes here: a force on the alias is still a force.
 */
export function subagentModelForced(env: Env): boolean {
	return [SUBAGENT_MODEL_FORCE_ENV, SUBAGENT_MODEL_FORCE_ENV_ALIAS].some(name => isOn(env[name]));
}

/** A normalized effort level, or undefined when the value names no level. */
export function effortLevel(value: unknown, levels: readonly string[]): string | undefined {
	const level = typeof value === 'string' ? value.trim().toLowerCase() : '';
	return level.length > 0 && levels.includes(level) ? level : undefined;
}

/**
 * The effort level the environment asks for. The first of the two names that
 * names a level the caller accepts wins; an unknown level is not an answer, so
 * the alias is tried.
 */
export function effortLevelFromEnv(env: Env, levels: readonly string[]): string | undefined {
	return aliased(env, EFFORT_LEVEL_ENV, EFFORT_LEVEL_ENV_ALIAS, value => effortLevel(value, levels));
}

/** The context window the environment asks for, if it asks for one at all. */
export function contextWindowOverride(env: Env): number | undefined {
	const value = Number(aliasValue(env, CONTEXT_WINDOW_ENV, CONTEXT_WINDOW_ENV_ALIAS));
	return Number.isFinite(value) ? value : undefined;
}

/** The MCP policy the environment asks for. */
export function mcpPolicy(env: Env): McpPolicy {
	return {
		bare: isOn(aliasValue(env, BARE_ENV, BARE_ENV_ALIAS)),
		strict: isOn(aliasValue(env, STRICT_MCP_CONFIG_ENV, STRICT_MCP_CONFIG_ENV_ALIAS)),
		allow: commaList(aliasValue(env, MCP_ALLOW_ENV, MCP_ALLOW_ENV_ALIAS)),
		deny: commaList(aliasValue(env, MCP_DENY_ENV, MCP_DENY_ENV_ALIAS)),
	};
}

/**
 * Whether the environment asks for foreground runs only. Either name says yes
 * here, and "off" is spelled `0`, `false`, `no` or nothing at all.
 */
export function backgroundDisabled(env: Env): boolean {
	// A switch is off when it is unset or spells "off"; anything else is on.
	const isOnSwitch = (value: string | undefined): boolean => {
		if (value === undefined) return false;
		const normalized = value.trim().toLowerCase();
		return normalized.length > 0 && normalized !== '0' && normalized !== 'false' && normalized !== 'no';
	};
	return [BACKGROUND_DISABLE_ENV, BACKGROUND_DISABLE_ENV_ALIAS].some(name => isOnSwitch(env[name]));
}

/**
 * An on/off toggle: `on`, `true`, `1` and `yes` are on; `off`, `false`, `0`
 * and `no` are off; anything else names no toggle at all.
 */
export function toggle(value: unknown): 'on' | 'off' | undefined {
	if (typeof value !== 'string') return undefined;
	const normalized = value.trim().toLowerCase();
	if (['on', 'true', '1', 'yes'].includes(normalized)) return 'on';
	if (['off', 'false', '0', 'no'].includes(normalized)) return 'off';
	return undefined;
}

/** The fork mode the environment asks for, if it asks for one at all. */
export function forkModeFromEnv(env: Env): 'on' | 'off' | undefined {
	return aliased(env, FORK_MODE_ENV, FORK_MODE_ENV_ALIAS, toggle);
}

/** Whether the Agent SDK is driving this session. */
export function agentSdk(env: Env): boolean {
	return [SDK_ENV, SDK_ENV_ALIAS, COMMANDCODE_SDK_ENV].some(name => toggle(env[name]) === 'on');
}

/**
 * The agent the environment asks the session to run as. The mod's name wins
 * when it is set to anything at all - even whitespace, which names no agent.
 */
export function agentNameFromEnv(env: Env): string {
	const resolved = aliased(env, AGENT_ENV, AGENT_ENV_ALIAS, value =>
		value !== undefined && value.length > 0 ? value.trim() : undefined,
	);
	return resolved ?? '';
}

/** Whether the session may message another session's agent. */
export function crossSessionEnabled(env: Env): boolean {
	return env[CROSS_SESSION_ENV] !== '0';
}

/** The main conversation's permission mode, as the environment states it. */
export function mainPermissionModeFromEnv(env: Env): string | undefined {
	return env[MAIN_PERMISSION_MODE_ENV];
}

/** Whether this process is a worker rather than a session. */
export function workerModeFromEnv(env: Env): boolean {
	return [WORKER_ENV, PUB_SUB_WORKER_ENV].some(name => env[name] === '1');
}

/** Where the managed and the user policy files live. */
export function policyFiles(env: Env, home: string): PolicyFiles {
	return {
		managed: env[MANAGED_CLAUDE_MD_ENV] ?? join(home, '.commandcode', 'managed', 'CLAUDE.md'),
		user: env[USER_CLAUDE_MD_ENV] ?? join(home, '.commandcode', 'CLAUDE.md'),
	};
}

// ---------------------------------------------------------------------------
// The host options
// ---------------------------------------------------------------------------

/**
 * A mod option. An option that was never set reads as the empty string, so
 * every caller can treat "unset" and "empty" the same way.
 */
export function optionValue(source: OptionSource, name: string): string {
	return String(source.getFlag(name) ?? '');
}

/**
 * The value of `primary`, or of the deprecated `legacy` name while the primary
 * still sits on its own default. A flag on its default is not an override, so
 * an upgraded machine keeps working for one release.
 */
export function pickOption(
	source: OptionSource,
	primary: string,
	primaryDefault: string,
	legacy: string,
	legacyDefault: string,
): string {
	const value = optionValue(source, primary);
	if (value.length > 0 && value !== primaryDefault) return value;
	const old = optionValue(source, legacy);
	if (old.length > 0 && old !== legacyDefault) return old;
	return value.length > 0 ? value : legacyDefault;
}

/** A comma-separated option: `--add-dir a,b` is two directories. */
export function optionList(source: OptionSource, name: string): string[] {
	return commaList(optionValue(source, name));
}

/** The managed settings directory: the `managed_dir` option, else the host's. */
export function managedSettingsDir(source: OptionSource): string {
	return optionValue(source, MANAGED_DIR_OPTION) || MANAGED_SETTINGS_DEFAULT;
}

/**
 * The managed settings directory under a given home. The host's own default
 * lives under the machine's home; a session that overrides the home (a test, a
 * worker) gets its managed settings there too.
 */
export function managedSettingsHome(home: string): string {
	return join(home, '.commandcode', 'managed');
}