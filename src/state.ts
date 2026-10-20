// ---------------------------------------------------------------------------
// Job states
//
// The six states and their edges in one table. Every writer asks this table
// instead of spelling a state string, an edge that is not in the table is
// rejected, and a state the table does not know renders as a warning - never as
// a silent dot.
// ---------------------------------------------------------------------------

export const JOB_STATES = ['queued', 'running', 'waiting-input', 'done', 'failed', 'canceled'] as const;
export type JobState = (typeof JOB_STATES)[number];

// A signal may also carry a note (subagent_send), which is not a job state.
export type SignalState = JobState | 'note';

// Legacy aliases, normalized in exactly one place.
export const STATE_ALIASES: Readonly<Record<string, JobState>> = {
	completed: 'done',
	'agent-done': 'done',
	'command-done': 'done',
};

export const STATE_TRANSITIONS: Readonly<Record<JobState, readonly JobState[]>> = {
	queued: ['running', 'failed', 'canceled'],
	running: ['waiting-input', 'done', 'failed', 'canceled'],
	'waiting-input': [],
	done: [],
	failed: [],
	canceled: [],
};

export const STATE_MARKERS: Readonly<Record<SignalState, string>> = {
	queued: '◌',
	running: '●',
	'waiting-input': '?',
	done: '✓',
	failed: '✗',
	canceled: '⊘',
	note: '◈',
};

export const STATE_COLORS: Readonly<Record<SignalState, string>> = {
	queued: 'gray',
	running: 'cyan',
	'waiting-input': 'yellow',
	done: 'green',
	failed: 'red',
	canceled: 'gray',
	note: 'gray',
};

export function isState(value: unknown): value is JobState {
	return typeof value === 'string' && (JOB_STATES as readonly string[]).includes(value);
}

// Alias in, state out. One normalization point for every reader and writer.
export function normalizeState(value: unknown): JobState | undefined {
	if (typeof value !== 'string' || value.length === 0) return undefined;
	const alias = STATE_ALIASES[value];
	if (alias !== undefined) return alias;
	return isState(value) ? value : undefined;
}

export function isSignalState(value: unknown): value is SignalState {
	return value === 'note' || isState(value);
}

export function canTransition(from: JobState, to: JobState): boolean {
	// Writing the state a job already has is not a transition; every other edge must
	// be in the table.
	if (from === to) return true;
	return STATE_TRANSITIONS[from].includes(to);
}

export function isTerminal(state: JobState): boolean {
	return STATE_TRANSITIONS[state].length === 0;
}

// Terminal for a value that may be a legacy alias or an unknown string.
export function isTerminalState(value: unknown): boolean {
	const state = normalizeState(value);
	return state !== undefined && isTerminal(state);
}

// Display: a legacy alias shows as its state, an unknown value shows as itself so
// it cannot hide.
export function canonicalState(value: unknown): string {
	return normalizeState(value) ?? String(value ?? 'unknown');
}

export function stateMarker(state: unknown): string {
	return isSignalState(state) ? STATE_MARKERS[state] : '⚠';
}

export function stateColor(state: unknown): string {
	return isSignalState(state) ? STATE_COLORS[state] : 'red';
}

// Exported for the tests, which assert edges rather than inferring them from
// English. The harness never reads it.
export const jobStates = {
	list: JOB_STATES,
	isState,
	normalize: normalizeState,
	canTransition,
	isTerminal,
	marker: stateMarker,
	color: stateColor,
};
