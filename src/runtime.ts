// The OS runtime adapter: the production `Runtime` and its spool filesystem.
// This is the only module allowed to name real `node:fs`, `node:child_process`
// or `process`; every other module takes those effects as an injected
// `Runtime`, so tests can drive the same code without touching the machine.

import type {ChildHandle, Runtime, RuntimeOverrides, SpoolFs} from './types.ts';
import {spawn, spawnSync} from 'node:child_process';
import {
	appendFileSync,
	mkdirSync,
	readdirSync,
	readFileSync,
	renameSync,
	rmSync,
	statSync,
	writeFileSync,
} from 'node:fs';
import {homedir} from 'node:os';

function osSpoolFs(): SpoolFs {
	return {
		mkdir: dir => {
			mkdirSync(dir, {recursive: true});
		},
		readdir: dir => readdirSync(dir),
		readFile: file => readFileSync(file, 'utf8'),
		writeFile: (file, data) => writeFileSync(file, data),
		appendFile: (file, data) => appendFileSync(file, data),
		rename: (from, to) => renameSync(from, to),
		remove: file => {
			rmSync(file, {force: true, recursive: true});
		},
		stat: file => {
			const stats = statSync(file);
			return {size: stats.size, mtimeMs: stats.mtimeMs};
		},
		exists: file => {
			try {
				return statSync(file).isDirectory();
			} catch {
				return false;
			}
		},
	};
}

// The production adapter. Only this object is allowed to name node:fs,
// node:child_process or process.
const OS_RUNTIME: Runtime = {
	now: () => Date.now(),
	spawn: (command, args, options) => spawn(command, args, options as never) as unknown as ChildHandle,
	kill: (pid, signal) => {
		process.kill(pid, signal as never);
	},
	alive: pid => {
		if (typeof pid !== 'number' || !Number.isInteger(pid) || pid <= 0) return false;
		try {
			process.kill(pid, 0);
			return true;
		} catch (error) {
			return (error as {code?: string}).code === 'EPERM';
		}
	},
	// The only wait in the mod, used by subagent_output's opt-in blocking read.
	sleep: ms => new Promise<void>(resolve => setTimeout(resolve, ms)),
	// The fallback for a host without `cmd.clock.after`, and the seam the tests drive.
	later: (ms, fn) => {
		const timer = setTimeout(fn, ms);
		if (typeof timer === 'object' && timer !== null && 'unref' in timer && typeof timer.unref === 'function') timer.unref();
	},
	pid: process.pid,
	execPath: process.execPath,
	env: process.env,
	cwd: process.cwd(),
	home: homedir(),
	interactive: process.stdin.isTTY === true,
	fs: osSpoolFs(),
	// One bounded command. Everything it returns is plain data, so a failed git (not a
	// repository, no ref) is a value, never a throw here.
	run: (command, args, options) => {
		const result = spawnSync(command, args, {
			cwd: options.cwd,
			encoding: 'utf8',
			timeout: options.timeoutMs ?? 2000,
			maxBuffer: 64 * 1024,
			...(options.env !== undefined ? {env: {...process.env, ...options.env}} : {}),
		});
		return {
			ok: result.status === 0,
			code: typeof result.status === 'number' ? result.status : 1,
			stdout: String(result.stdout ?? ''),
			stderr: String(result.stderr ?? result.error?.message ?? ''),
		};
	},
};

// The test adapter is the same interface with functions swapped out.
export function osRuntime(overrides: RuntimeOverrides = {}): Runtime {
	return {
		...OS_RUNTIME,
		...overrides,
		fs: {...OS_RUNTIME.fs, ...overrides.fs},
	};
}