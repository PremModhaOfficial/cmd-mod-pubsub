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
