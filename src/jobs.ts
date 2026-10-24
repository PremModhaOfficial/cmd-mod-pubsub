// ---------------------------------------------------------------------------
// Job records
//
// The durable record behind a job is one JSON file per job under <root>/jobs.
// This module owns the path convention and the three operations that touch it:
// read one, write one atomically, and scan the newest ones for the list pane
// and the restart reconcile. The fs seam and the directory arrive explicitly,
// so the store names no ambient spool and no clock.
// ---------------------------------------------------------------------------

import type {CompactBoundary, JobIdentity, PermissionRequest, SpoolFs} from './types.ts';
import {join} from 'node:path';

// The durable record behind a job: the shared identity plus its bookkeeping.
export interface JobRecord extends JobIdentity {
	command?: string;
	prompt?: string;
	cwd?: string;
	/** The persisted conversation for this agent; retention sweeps it with the record. */
	transcriptPath?: string;
	/** The maxTurns or max_tokens stop that produced a partial result. */
	partial?: boolean;
	/** The last compaction observed for this agent's own context window. */
	compactBoundary?: CompactBoundary;
	/** This job's nesting level, and the job that spawned it. */
	depth?: number;
	parentJobId?: string;
	agentType?: string;
	/** The addressable name from the spawn; absent for an anonymous agent. */
	name?: string;
	/** A fork: the main conversation's history, prompt, tools, model, cache. */
	fork?: boolean;
	/** The main conversation a fork copied. */
	mainSessionId?: string;
	/** The host's id for the fork session; follow-ups resume it. */
	forkSessionId?: string;
	/** `shared` for a fork, `own` for every other agent. */
	promptCache?: string;
	/** The team this teammate belongs to. */
	teamName?: string;
	/** Ctrl+B moved a foreground run to the background. */
	backgroundedAt?: number;
	/** Set by SubagentHandback: the report was already emitted, so the wrapper's
	 *  own end-of-turn signal is suppressed. */
	handbackAt?: number;
	/** The resolved execution policy: false is a foreground run. */
	background?: boolean;
	model?: string;
	tools?: string[];
	/** The deny list this run resolved, not the definition's. A spawn may override
	 *  the definition's `disallowedTools`, and a resume must restore what the run
	 *  actually had, or the follow-up loses a tool the first run was allowed. */
	disallowedTools?: string[];
	/** A tool restriction was applied to this run, so the worker hook must enforce
	 *  it. A plain agent has a resolved tool list too, so the list alone does not
	 *  say "restricted"; a follow-up needs this flag to know. */
	restrictedTools?: boolean;
	/** The MCP servers scoped to this agent; a resume keeps the same scope. The shape
	 *  is the one the definition layer parses, restated so the record names no registry. */
	mcpServers?: {name: string; command?: string; args?: string[]; env?: Record<string, string>; url?: string}[];
	autoCompactWindow?: number;
	cacheTtl?: '5m' | '1h';
	color?: string;
	maxTurns?: number;
	timeoutMs?: number;
	sessionName?: string;
	resumedFrom?: string;
	/** The ACP session the worker created; a resume targets this id. */
	acpSessionId?: string;
	effort?: string;
	permissionMode?: string;
	isolation?: string;
	/** The worktree this job ran in, when isolation created one. */
	worktree?: string;
	/** The repository root, used to remove the worktree. */
	worktreeRepo?: string;
	/** Checkout roots the worker must never enter. */
	mainCheckouts?: string[];
	/** Set once the project SubagentStop hook ran for this terminal job. */
	subagentStopHook?: boolean;
	/** Set once cleanup decided: the worktree was removed, or kept. */
	worktreeFinalized?: boolean;
	worktreeKept?: boolean;
	/** The worktree's starting commit; a new HEAD means committed work. */
	worktreeHead?: string;
	/** A live agent may still accept a follow-up; its checkout must survive. */
	live?: boolean;
	exitCode?: number;
	pid?: number;
	workerPid?: number;
	deliveredAt?: number;
	updatedAt?: number;
	failureReason?: string;
	canceledAt?: number;
	awaitingInput?: boolean;
	question?: string;
	permissionRequest?: PermissionRequest;
	scanned?: boolean;
	scanMatches?: string[];
}

export interface JobStoreDeps {
	/** The spool filesystem every record read and write goes through. */
	readonly fs: SpoolFs;
	/** The jobs directory: one `<id>.json` per job. */
	readonly jobsDir: string;
	/** Discriminates a concurrent writer's temp file. */
	readonly pid: number;
	/** The clock stamped into the temp file name. */
	readonly now: () => number;
}

export interface JobStore {
	readJob(id: string): JobRecord | undefined;
	writeJob(record: JobRecord): void;
	readRecentJobs(limit: number): JobRecord[];
}

export function createJobStore(deps: JobStoreDeps): JobStore {
	function readJob(id: string): JobRecord | undefined {
		try {
			const parsed = JSON.parse(deps.fs.readFile(join(deps.jobsDir, `${id}.json`))) as JobRecord;
			return parsed && typeof parsed.id === 'string' ? parsed : undefined;
		} catch {
			return undefined;
		}
	}

	// Write-then-rename, so a reader never sees a half-written record and a crash
	// leaves the previous one intact.
	function writeJob(record: JobRecord): void {
		try {
			const file = join(deps.jobsDir, `${record.id}.json`);
			const tmp = `${file}.mod-${deps.pid}-${deps.now()}`;
			deps.fs.writeFile(tmp, JSON.stringify(record, null, 2));
			deps.fs.rename(tmp, file);
		} catch {
			/* the in-memory list still shows it */
		}
	}

	// The newest `limit` records, newest first by file name. Used by the list pane
	// and by the restart reconcile, so both read one ordering.
	function readRecentJobs(limit: number): JobRecord[] {
		let names: string[];
		try {
			names = deps.fs.readdir(deps.jobsDir).filter(name => name.endsWith('.json') && !name.includes('.tmp-'));
		} catch {
			return [];
		}
		names.sort();
		const jobs: JobRecord[] = [];
		for (const name of names.slice(-limit).reverse()) {
			try {
				jobs.push(JSON.parse(deps.fs.readFile(join(deps.jobsDir, name))) as JobRecord);
			} catch {
				/* skip a torn record */
			}
		}
		return jobs;
	}

	return {readJob, writeJob, readRecentJobs};
}
