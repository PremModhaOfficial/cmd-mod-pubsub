// The fake ModApi + fake Runtime harness.
//
// Both suites drive the real `better-sub-agents.ts` source with Node's native
// TypeScript type stripping - no build step, no dependencies. Everything the mod
// touches (spool dirs, session transcript, UI, clock, processes) is a fake here,
// except in `test/e2e.mjs`, which uses the real OS on purpose.
//
// The mod takes its runtime as the second factory argument, so a spawn failure, a
// dead pid, a failed rename or a frozen clock is a function call - not a process, a
// crash or a wait.

import assert from 'node:assert/strict';
import * as modExports from '../better-sub-agents.ts';
import {
	appendFileSync,
	mkdirSync,
	mkdtempSync,
	readdirSync,
	readFileSync,
	renameSync,
	rmSync,
	statSync,
	writeFileSync,
} from 'node:fs';
import * as nodeFs from 'node:fs';
import {tmpdir} from 'node:os';
import * as nodePath from 'node:path';
import {join} from 'node:path';

// ---------------------------------------------------------------- assertions

export const results = {checks: 0};

export function ok(label) {
	results.checks += 1;
	process.stdout.write(`ok ${results.checks} - ${label}\n`);
}

export function fail(error) {
	process.stdout.write(`\nFAIL: ${String(error?.stack ?? error?.message ?? error)}\n`);
	process.exitCode = 1;
}

// ------------------------------------------------------------------- fakes

// A session fake. `appendCustomEntry` / `getCustomEntries` are real in-memory
// behaviour: the mod mints an owner token once and reads it back on resume.
export function makeSession() {
	const entries = [];
	return {
		entries,
		appendCustomEntry(entry) {
			entries.push({customType: entry.customType, data: entry.data});
			return entry;
		},
		getCustomEntries(query) {
			const type = query?.customType;
			return entries.filter((entry) => type === undefined || entry.customType === type);
		},
	};
}

// A session whose transcript refuses the first appends (read-only transcript, a
// racing writer). The mod must still know its own identity.
export function makeFlakySession(failures = 1) {
	const inner = makeSession();
	let attempts = 0;
	return {
		entries: inner.entries,
		appendCustomEntry(entry) {
			attempts += 1;
			if (attempts <= failures) throw new Error('EACCES');
			return inner.appendCustomEntry(entry);
		},
		getCustomEntries(query) {
			return inner.getCustomEntries(query);
		},
	};
}

export function makeChild({pid = 424242} = {}) {
	const handlers = new Map();
	return {
		pid,
		on(event, handler) {
			const list = handlers.get(event) ?? [];
			list.push(handler);
			handlers.set(event, list);
		},
		unref() {},
		kill() {},
		// test-only: drive the handle's listeners
		emit(event, value) {
			for (const handler of handlers.get(event) ?? []) handler(value);
		},
	};
}

// The OS seam: spawn, kill, the clock, liveness and the wait. `sleep` resolves
// immediately, so an opt-in wait in a test cannot spend real time.
export function makeRuntime(overrides = {}) {
	const spawnCalls = [];
	const kills = [];
	const runtime = {
		home: EMPTY_HOME,
		spawn(command, args, options) {
			const child = makeChild();
			spawnCalls.push({command, args, options, child});
			return child;
		},
		kill(pid, signal) {
			kills.push({pid, signal});
		},
		alive() {
			return false;
		},
		run() {
			return {ok: false, stdout: ''};
		},
		async sleep() {},
		...overrides,
	};
	return {runtime, spawnCalls, kills};
}

// A ModApi fake with exactly the surface the mod uses. No speculative members:
// if the mod starts reaching for something else, the suite fails loudly.
export function makeApi({spoolDir, session, runtime, dirFlag = 'subagent_dir'}) {
	const toolDefs = [];
	const commandDefs = [];
	const rendererDefs = [];
	const shortcutDefs = [];
	const flagDefs = new Map();
	const flagOverrides = new Map();
	if (spoolDir !== undefined) flagOverrides.set(dirFlag, spoolDir);
	const registeredHooks = {};
	const listeners = new Map();
	const everyTicks = [];
	const afterTicks = [];
	const paneDefs = [];
	const openPaneCalls = [];
	const statusCalls = [];
	const shownEntries = [];
	const userMessages = [];
	const notifyCalls = [];
	const stateValues = new Map();

	const api = {
		// ---- registration
		addFlag(name, def) {
			flagDefs.set(name, def);
		},
		getFlag(name) {
			if (flagOverrides.has(name)) return flagOverrides.get(name);
			return flagDefs.get(name)?.default;
		},
		setFlag(name, value) {
			flagOverrides.set(name, value);
		},
		addRenderer(id, render) {
			rendererDefs.push({id, render});
		},
		addTool(def) {
			toolDefs.push(def);
		},
		addCommand(def) {
			commandDefs.push(def);
		},
		addShortcut(keys, def) {
			shortcutDefs.push({keys, ...def});
			return {dispose() {}};
		},
		hooks(h) {
			Object.assign(registeredHooks, h);
		},
		on(name, handler) {
			const list = listeners.get(name) ?? [];
			list.push(handler);
			listeners.set(name, list);
			return () => {
				listeners.set(name, (listeners.get(name) ?? []).filter((h) => h !== handler));
			};
		},

		// ---- session surface
		session,
		showEntry(type, data) {
			shownEntries.push({type, data});
		},
		sendUserMessage(text, options) {
			userMessages.push({text, options});
			return true;
		},

		// ---- clock
		clock: {
			every(ms, fn) {
				everyTicks.push({ms, fn});
				return () => {};
			},
			after(ms, fn) {
				afterTicks.push({ms, fn});
				return () => {};
			},
		},

		// ---- state
		state: {
			get(key) {
				return stateValues.get(key);
			},
			set(key, value) {
				stateValues.set(key, value);
			},
		},

		// ---- ui
		ui: {
			pane(def) {
				paneDefs.push(def);
				return {close() {}};
			},
			openPane(id) {
				openPaneCalls.push(id);
				return true;
			},
			notify(message, level) {
				notifyCalls.push({message, level});
			},
			refreshWidgets() {},
			setStatus(text) {
				statusCalls.push(text);
			},
			capabilities: {pane: true, status: true, widgets: true, richText: true},
		},

		// ---- process surface
		cwd: process.cwd(),
	};

	return {
		api,
		runtime,
		toolDefs,
		commandDefs,
		rendererDefs,
		shortcutDefs,
		flagDefs,
		registeredHooks,
		listeners,
		everyTicks,
		afterTicks,
		paneDefs,
		openPaneCalls,
		statusCalls,
		shownEntries,
		userMessages,
		notifyCalls,
	};
}

export const toolNames = (bundle) => bundle.toolDefs.map((def) => def.schema?.name);
export const commandNames = (bundle) => bundle.commandDefs.map((def) => def.name);
export const toolOf = (bundle, name) => bundle.toolDefs.find((def) => def.schema?.name === name);

export function ownerEntries(session, customType = 'subagent/owner') {
	return session
		.getCustomEntries({customType})
		.map((entry) => entry?.data?.token)
		.filter((token) => typeof token === 'string' && token.length > 0);
}

// The detached worker is a string in the mod. Exporting the built string means a
// syntax error surfaces here, and the envelope injection is already resolved. The
// literal-extraction fallback keeps the suite runnable against the pre-T1 source
// (used to demonstrate the new checks failing before the change).
export function workerSource() {
	if (typeof modExports.WORKER_SOURCE === 'string') return modExports.WORKER_SOURCE;
	const source = readFileSync(new URL('../better-sub-agents.ts', import.meta.url), 'utf8');
	const start = source.indexOf('const WORKER_SOURCE = `');
	const end = source.indexOf('\n`;', start);
	assert.ok(start > 0 && end > start, 'WORKER_SOURCE must be findable in better-sub-agents.ts');
	const literal = source.slice(start + 'const WORKER_SOURCE = '.length, end + 2);
	return Function(`"use strict"; return ${literal};`)();
}

export function modSource() {
	return readFileSync(new URL('../better-sub-agents.ts', import.meta.url), 'utf8');
}

// A real filesystem adapter, for a fake spool that must behave like a disk (the
// ledger tests, and the restore-failure tests).
export const realFs = {
	mkdir: (dir) => mkdirSync(dir, {recursive: true}),
	readdir: (dir) => readdirSync(dir),
	readFile: (file) => readFileSync(file, 'utf8'),
	writeFile: (file, data) => writeFileSync(file, data),
	appendFile: (file, data) => appendFileSync(file, data),
	rename: (from, to) => renameSync(from, to),
	remove: (file) => rmSync(file, {force: true, recursive: true}),
	stat: (file) => {
		const stats = statSync(file);
		return {size: stats.size, mtimeMs: stats.mtimeMs};
	},
	exists: (file) => {
		try {
			return statSync(file).isDirectory();
		} catch {
			return false;
		}
	},
};

// Every fake runtime gets an empty home, so a developer's real
// ~/.commandcode/agents can never change what the suite asserts. A test that
// exercises discovery overrides it.
const EMPTY_HOME = mkdtempSync(join(tmpdir(), 'subagent-empty-home-'));

export const tempDir = (prefix) => mkdtempSync(join(tmpdir(), prefix));

// Drive the detached worker's `finish` path in this process. The worker source is
// evaluated with a fake child and a fake `process`, and the test fires the `exit`
// listener itself: no real process, no wait. The spool files are real, so the
// record and the signal are the same bytes a detached worker would write.
//
// Command jobs only: an agent job needs a JSON-RPC child on stdout.
export function makeWorkerDriver(spec) {
	const handlers = new Map();
	const spawns = [];
	const exitCodes = [];
	const child = {
		pid: 515151,
		stdin: {end() {}},
		on(event, handler) {
			const list = handlers.get(event) ?? [];
			list.push(handler);
			handlers.set(event, list);
		},
		unref() {},
		kill() {},
	};
	const fakeProcess = {
		pid: 616161,
		env: {SUBAGENT_SPEC: JSON.stringify(spec)},
		on() {},
		exit(code) {
			exitCodes.push(code);
		},
	};
	Function('require', 'process', 'setTimeout', 'clearTimeout', workerSource())(
		(name) => {
			if (name === 'node:child_process') {
				return {
					spawn(command, args, options) {
						spawns.push({command, args, options});
						return child;
					},
				};
			}
			if (name === 'node:fs') return nodeFs;
			if (name === 'node:path') return nodePath;
			throw new Error(`worker require(${name}) is not stubbed`);
		},
		fakeProcess,
		// The driver is synchronous: a worker timer must not keep the suite alive.
		() => 0,
		() => {},
	);
	return {
		spawns,
		exitCodes,
		emit(event, ...args) {
			for (const handler of handlers.get(event) ?? []) handler(...args);
		},
	};
}
