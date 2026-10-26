// ---------------------------------------------------------------------------
// Runtime seam
//
// Every OS call the mod makes goes through one interface: the clock, spawning and
// killing processes, and the spool filesystem. Production wires the OS adapter
// below; a test passes fakes, so spawn failure, kill escalation and clock-driven
// recovery are drivable without a real process, a real wait or a real crash.
// ---------------------------------------------------------------------------

export interface ChildHandle {
	readonly pid?: number;
	on(event: 'error', handler: (error: unknown) => void): void;
	on(event: 'exit', handler: (code: number | null, signal: string | null) => void): void;
	unref(): void;
	kill(signal?: string): void;
}

export interface SpawnOptions {
	cwd?: string;
	detached?: boolean;
	stdio?: 'ignore' | Array<'ignore' | number>;
	env?: Record<string, string | undefined>;
}

export interface SpoolFs {
	mkdir(dir: string): void;
	readdir(dir: string): string[];
	readFile(file: string): string;
	writeFile(file: string, data: string): void;
	appendFile(file: string, data: string): void;
	rename(from: string, to: string): void;
	remove(file: string): void;
	stat(file: string): {size: number; mtimeMs: number};
	exists(file: string): boolean;
}

/** One bounded command's result: a value, never a throw. */
export interface CommandResult {
	ok: boolean;
	stdout: string;
	code?: number;
	stderr?: string;
}

export interface Runtime {
	now(): number;
	spawn(command: string, args: string[], options: SpawnOptions): ChildHandle;
	kill(pid: number, signal: string): void;
	alive(pid: unknown): boolean;
	sleep(ms: number): Promise<void>;
	/**
	 * A one-shot timer. The host clock is preferred; this is the seam a test can
	 * drive, so no parent code names the OS clock.
	 */
	later(ms: number, fn: () => void): void;
	readonly pid: number;
	readonly execPath: string;
	readonly env: Record<string, string | undefined>;
	/** The workspace root: project discovery starts here. */
	readonly cwd: string;
	/** The user home: `~/.commandcode/agents` lives under it. */
	readonly home: string;
	/** Whether a human is driving. A CLI agent file is refused when one is. */
	readonly interactive: boolean;
	readonly fs: SpoolFs;
	/**
	 * One bounded, synchronous command. Optional so a test can leave the OS out; a
	 * failure is a value, never a throw.
	 */
	run?(command: string, args: string[], options: {cwd?: string; timeoutMs?: number; env?: Record<string, string | undefined>}): CommandResult;
}

export type RuntimeOverrides = Partial<Omit<Runtime, 'fs'>> & {fs?: Partial<SpoolFs>};

// ---------------------------------------------------------------------------
// The signal contract
//
// The identity a job record and its signal share, plus the two shapes built on
// it: the signal that reaches the session and the record write that signal
// implies. Declared once, so the record, the signal, and the worker's inlined
// envelope cannot drift.
// ---------------------------------------------------------------------------

// An ACP `session/request_permission` as the parent sees it: enough to show the
// prompt, name the tool call, and return the chosen option.
export interface PermissionRequestOption {
	readonly optionId: string;
	readonly kind?: string;
	readonly name?: string;
}

export interface PermissionRequest {
	readonly requestId?: number | string;
	readonly title: string;
	readonly toolCall?: {readonly title?: string; readonly kind?: string};
	readonly options: PermissionRequestOption[];
}

// ---------------------------------------------------------------------------
// The launch values
//
// The leaf shapes the one launch type and the durable record both name, declared
// here so neither has to borrow a field type from the other.
// ---------------------------------------------------------------------------

/** One MCP server scoped to a launch. */
export interface LaunchMcpServer {
	name: string;
	command?: string;
	args?: string[];
	env?: Record<string, string>;
	url?: string;
}

/** The prompt-cache lifetime a launch may ask for. */
export type LaunchCacheTtl = '5m' | '1h';

export interface JobIdentity {
	id: string;
	/** The durable resume handle. Absent for a one-shot agent (Explore, Plan). */
	agentId?: string;
	owner?: string;
	description?: string;
	kind?: string;
	state?: string;
	startedAt?: number;
	finishedAt?: number;
	durationMs?: number;
	logPath?: string;
	sessionId?: string;
}

/** One compaction observed in a subagent's own context window. */
export interface CompactBoundary {
	readonly trigger: 'auto' | 'manual';
	/** The token count before compaction ran. */
	readonly preTokens: number;
	readonly at: number;
}

export interface Signal extends JobIdentity {
	readonly exitCode?: number | null;
	readonly title?: string;
	readonly body?: string;
	/** The report was scanned before it could reach the parent. */
	readonly scanned?: boolean;
	readonly scanMatches?: string[];
	readonly question?: string;
	readonly permissionRequest?: PermissionRequest;
	/** A maxTurns or max_tokens stop: the output is partial and the job is resumable. */
	readonly partial?: boolean;
	readonly compactBoundary?: CompactBoundary;
}

// The record write a signal implies.
export interface JobPatch {
	state?: string;
	exitCode?: number;
	finishedAt?: number;
	durationMs?: number;
	failureReason?: string;
	canceledAt?: number;
	awaitingInput?: boolean;
	question?: string;
	permissionRequest?: PermissionRequest;
	scanned?: boolean;
	scanMatches?: string[];
	partial?: boolean;
	compactBoundary?: CompactBoundary;
}
