// Roster visibility: the sibling snapshot a named agent starts with, and the
// predicate that decides whether a foreground agent keeps the ListAgents tool.
// Pure functions only, so the peer rules are testable without spawning.

import {ROSTER_MAIN} from './constants.ts';

// ---------------------------------------------------------------------------
// Live messaging: the roster
//
// A named spawn is addressable. Every agent that has the send tool starts with a
// snapshot of its siblings, so it can message them without a discovery call. The
// snapshot lists main and the OTHER named agents only: an unnamed job has no
// address, and an agent is never its own peer. The newest record for a name wins,
// so a resumed agent replaces the job it continued.
// ---------------------------------------------------------------------------

export interface RosterEntry {
	name?: string;
	state?: string;
	jobId?: string;
}

export function buildRoster(entries: readonly RosterEntry[], selfName?: string): string {
	const named = new Map<string, RosterEntry>();
	for (const entry of entries) {
		const name = typeof entry.name === 'string' ? entry.name.trim() : '';
		if (name.length === 0 || name === selfName) continue;
		if (!named.has(name)) named.set(name, entry);
	}
	// `main` is always addressable, so the roster is never empty: a lone named
	// agent still learns how to reach the conversation that started it.
	const lines = [
		'SIBLING ROSTER (snapshot taken at start; address a peer with subagent_send({to, message}))',
		`${ROSTER_MAIN} — the conversation that started you`,
	];
	for (const [name, entry] of named) {
		const state = typeof entry.state === 'string' && entry.state.length > 0 ? ` (${entry.state})` : '';
		const job = typeof entry.jobId === 'string' && entry.jobId.length > 0 ? ` job ${entry.jobId}` : '';
		lines.push(`${name}${state}${job}`);
	}
	return lines.join('\n');
}

// ListAgents is a foreground tool: a foreground agent inherits it when cross-session
// messaging is on; a background agent does not keep it. One pure predicate, so the
// tool registration and the tests cannot disagree about visibility.
export function listAgentsVisible(options: {background?: boolean; crossSessionMessaging?: boolean}): boolean {
	if (options.crossSessionMessaging === false) return false;
	return options.background !== true;
}