// Permission modes and execution policy: one place for how a definition's mode
// combines with the main session's, whether a run is foreground or background,
// and whether fork mode and agent teams are allowed. Pure functions only, so
// every precedence rule is testable without spawning a worker.

import {agentSdk, backgroundDisabled, forkModeFromEnv, toggle} from './config.ts';

// ---------------------------------------------------------------------------
// Permission modes
//
// One table drives both transports: `acp` is the `session/set_mode` id the ACP
// adapter sends, `cli` is the `cmd --permission-mode` value for the CLI paths.
// The definition's mode and the main session's mode are combined in exactly one
// place, so the inheritance rules cannot drift between callers.
// ---------------------------------------------------------------------------

export const PERMISSION_MODE_LIST = [
	'default',
	'acceptEdits',
	'auto',
	'dontAsk',
	'bypassPermissions',
	'plan',
] as const;

export type PermissionMode = (typeof PERMISSION_MODE_LIST)[number] | 'manual';
export type CanonicalPermissionMode = (typeof PERMISSION_MODE_LIST)[number];

export interface PermissionModeRow {
	/** the ACP `session/set_mode` id; absent when ACP has no equivalent */
	readonly acp?: string;
	/** the `cmd --permission-mode` value; absent when the CLI cannot express it */
	readonly cli?: string;
	/** read-only exploration: writes are refused */
	readonly readOnly?: boolean;
	/** approval prompts are auto-denied; explicit allows still work */
	readonly autoDeny?: boolean;
	/** this mode wins over the definition's permissionMode when the main session is in it */
	readonly inherits?: boolean;
	/** this mode skips prompts and may only be used when the main session is in it */
	readonly bypass?: boolean;
}

const PERMISSION_MODE_TABLE: Readonly<Record<CanonicalPermissionMode, PermissionModeRow>> = {
	default: {acp: 'default', cli: 'standard'},
	acceptEdits: {acp: 'auto-accept', cli: 'accept-edits', inherits: true},
	auto: {inherits: true},
	dontAsk: {acp: 'dont-ask', autoDeny: true},
	bypassPermissions: {acp: 'bypass', cli: 'yolo', inherits: true, bypass: true},
	plan: {acp: 'plan', cli: 'plan', readOnly: true},
};

// Every spelling the host accepts: the frontmatter names, the CLI names, and the
// ACP ids. `manual` is the documented alias for `default`.
const PERMISSION_MODE_ALIASES: Readonly<Record<string, CanonicalPermissionMode>> = {
	manual: 'default',
	standard: 'default',
	'accept-edits': 'acceptEdits',
	'auto-accept': 'acceptEdits',
	'dont-ask': 'dontAsk',
	yolo: 'bypassPermissions',
	bypass: 'bypassPermissions',
};

export function canonicalPermissionMode(value: unknown): CanonicalPermissionMode | undefined {
	if (typeof value !== 'string' || value.length === 0) return undefined;
	const alias = PERMISSION_MODE_ALIASES[value];
	if (alias !== undefined) return alias;
	return Object.prototype.hasOwnProperty.call(PERMISSION_MODE_TABLE, value)
		? (value as CanonicalPermissionMode)
		: undefined;
}

export type PermissionResolution =
	| {readonly ok: true; readonly mode: CanonicalPermissionMode}
	| {readonly ok: false; readonly error: string};

// The inheritance table in SPEC "Permission modes": a main session in
// bypassPermissions, acceptEdits, or auto wins; otherwise the definition's mode
// applies, except that bypassPermissions still requires the main session to be
// in it. An unknown definition mode is ignored (default), never a launch error.
export function resolvePermissionMode(definitionMode: unknown, mainMode: unknown): PermissionResolution {
	const main = canonicalPermissionMode(mainMode) ?? 'default';
	const definition = canonicalPermissionMode(definitionMode) ?? 'default';
	if (PERMISSION_MODE_TABLE[main].inherits === true) return {ok: true, mode: main};
	if (PERMISSION_MODE_TABLE[definition].bypass === true) {
		return {
			ok: false,
			error: 'permissionMode "bypassPermissions" requires the main session to be in bypassPermissions',
		};
	}
	return {ok: true, mode: definition};
}

export const permissionModes = {
	list: PERMISSION_MODE_LIST,
	table: PERMISSION_MODE_TABLE,
	canonical: canonicalPermissionMode,
	resolve: resolvePermissionMode,
};

// ---------------------------------------------------------------------------
// Execution policy
//
// One rule set for "does this run block the conversation or report back later".
// Pure, so the precedence table is asserted without spawning a worker: the disable
// env wins over everything, a definition's `background: true` wins over the caller,
// and the caller's `run_in_background` wins over the default. The default is
// background, because a background run never holds the turn open.
// ---------------------------------------------------------------------------

export type ExecutionMode = 'foreground' | 'background';

export interface ExecutionPolicyInput {
	/** The caller's `run_in_background`; undefined means "no preference". */
	readonly requested?: boolean;
	/** The resolved definition's frontmatter `background`. */
	readonly definitionBackground?: boolean;
	readonly env: Record<string, string | undefined>;
}

export function resolveExecutionPolicy(input: ExecutionPolicyInput): ExecutionMode {
	if (backgroundDisabled(input.env)) return 'foreground';
	if (input.definitionBackground === true) return 'background';
	if (input.requested === false) return 'foreground';
	return 'background';
}

// ---------------------------------------------------------------------------
// Fork mode and agent teams
//
// Fork mode is the setting that decides whether the agents the model spawns run
// in the background. The default is the session shape: a human driving gets it,
// a non-interactive run and the Agent SDK do not. The flag and the environment
// override the default in both directions, so an SDK caller can opt in.
// ---------------------------------------------------------------------------

export type ForkMode = 'on' | 'off';

export interface ForkModeInput {
	readonly interactive?: boolean;
	/** The Agent SDK: off by default even when a TTY is attached. */
	readonly sdk?: boolean;
	readonly env?: Record<string, string | undefined>;
	/** `--mod-option fork_mode=on|off|auto`. An explicit on/off wins. */
	readonly flag?: string;
}

export function resolveForkMode(input: ForkModeInput = {}): ForkMode {
	const flag = toggle(input.flag);
	if (flag !== undefined) return flag;
	const env = input.env ?? {};
	const fromEnv = forkModeFromEnv(env);
	if (fromEnv !== undefined) return fromEnv;
	const inSdk = input.sdk === true || agentSdk(env);
	if (inSdk) return 'off';
	return input.interactive === true ? 'on' : 'off';
}

export interface TeamPolicyInput {
	readonly teammate: boolean;
	readonly definitionBackground?: boolean;
	readonly requested?: boolean;
	readonly forkMode: ForkMode;
}

export type TeamPolicy = {readonly ok: true; readonly background: false} | {readonly ok: false; readonly error: string};

// A teammate is always foreground. Its definition cannot pin it to the background,
// and a caller may only ask for the background when fork mode is off (where
// `run_in_background` still exists); with fork mode on the parameter is gone.
export function resolveTeamPolicy(input: TeamPolicyInput): TeamPolicy {
	if (input.teammate !== true) return {ok: true, background: false};
	if (input.definitionBackground === true) {
		return {
			ok: false,
			error:
				`a teammate cannot run in the background: its definition sets background: true; ` +
				'remove that field to join a team',
		};
	}
	if (input.requested === true && input.forkMode === 'off') {
		return {
			ok: false,
			error:
				'a teammate cannot run in the background: remove run_in_background: true (teammates run in the foreground)',
		};
	}
	return {ok: true, background: false};
}