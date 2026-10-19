// Smoke test for the pubsub mod.
//
// Runs against the real `pubsub.ts` source with Node's native TypeScript type
// stripping, so no build step and no dependencies. Everything the mod touches
// (spool dirs, session transcript, UI) is a fake.
//
// What this proves:
//   1. the file is loadable as an ES module and default-exports a factory
//   2. the factory registers the tools, command and flags the README documents
//   3. the session's owner token is persisted as a `pubsub/owner` custom entry
//   4. two sessions get DIFFERENT owner tokens, and a drain in session B never
//      delivers a signal fired by session A  <- the cross-session isolation invariant
//   5. untagged legacy signals are quarantined to `orphaned/`, never delivered

import assert from 'node:assert/strict';
import {existsSync, mkdtempSync, readdirSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';

import mod from '../pubsub.ts';

// ---------------------------------------------------------------- assertions

let checks = 0;
const ok = (label) => {
	checks += 1;
	process.stdout.write(`ok ${checks} - ${label}\n`);
};

function fail(error) {
	process.stdout.write(`\nFAIL: ${String(error?.message ?? error)}\n`);
	process.exitCode = 1;
}

// ------------------------------------------------------------------- fakes

// A session fake. `appendCustomEntry` / `getCustomEntries` are real in-memory
// behaviour: the mod mints an owner token once and reads it back on resume.
function makeSession() {
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

// A ModApi fake. Every member the harness exposes to a mod is present, even the
// ones pubsub does not use, so a future version cannot silently start crashing.
function makeApi({spoolDir, session}) {
	const toolDefs = [];
	const commandDefs = [];
	const rendererDefs = [];
	const shortcutDefs = [];
	const flagDefs = new Map();
	const flagOverrides = new Map([['pubsub_dir', spoolDir]]);
	const registeredHooks = {};
	const listeners = new Map();
	const everyTicks = [];
	const afterTicks = [];
	const paneDefs = [];
	const openPaneCalls = [];
	const closePaneCalls = [];
	const refreshWidgetCalls = [];
	const statusCalls = [];
	const shownEntries = [];
	const userMessages = [];
	const stateValues = new Map();
	const stateSubscribers = [];
	const storeValues = new Map();
	const eventHandlers = new Map();

	const api = {
		// ---- registration
		addFlag(name, def) {
			flagDefs.set(name, def);
		},
		getFlag(name) {
			if (flagOverrides.has(name)) return flagOverrides.get(name);
			return flagDefs.get(name)?.default;
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
		addShortcut(def) {
			shortcutDefs.push(def);
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
		isIdle() {
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
			subscribe(fn) {
				stateSubscribers.push(fn);
				return () => {};
			},
		},
		store: {
			get(key) {
				return storeValues.get(key);
			},
			set(key, value) {
				storeValues.set(key, value);
			},
			delete(key) {
				storeValues.delete(key);
			},
			clear() {
				storeValues.clear();
			},
		},
		events: {
			on(name, handler) {
				eventHandlers.set(name, handler);
				return () => {};
			},
			off(name) {
				eventHandlers.delete(name);
			},
			emit() {
				return undefined;
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
			closePane(id) {
				closePaneCalls.push(id);
				return true;
			},
			refreshWidgets() {
				refreshWidgetCalls.push(Date.now());
			},
			setStatus(text) {
				statusCalls.push(text);
			},
			capabilities: {pane: true, status: true, widgets: true, richText: true},
		},

		// ---- process surface
		cwd: process.cwd(),
		exec() {
			return Promise.resolve({code: 0, stdout: '', stderr: ''});
		},
		process: {
			spawn() {
				return {pid: 424242, on() {}, unref() {}, kill() {}};
			},
			pid: process.pid,
		},
	};

	return {
		api,
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
	};
}

const toolNames = (bundle) => bundle.toolDefs.map((def) => def.schema?.name);
const commandNames = (bundle) => bundle.commandDefs.map((def) => def.name);

function ownerEntries(session) {
	return session
		.getCustomEntries({customType: 'pubsub/owner'})
		.map((entry) => entry?.data?.token)
		.filter((token) => typeof token === 'string' && token.length > 0);
}

// --------------------------------------------------------------------- run

try {
	// 1. module shape -------------------------------------------------------
	assert.equal(typeof mod, 'function', 'default export must be the mod factory');
	ok('default export is a function');

	// Both sessions share ONE spool dir on purpose: that is how the real machine
	// looks. Isolation must come from the owner token, not from the directory.
	const spoolDir = mkdtempSync(join(tmpdir(), 'pubsub-smoke-'));
	const sessionA = makeSession();
	const sessionB = makeSession();
	const a = makeApi({spoolDir, session: sessionA});
	const b = makeApi({spoolDir, session: sessionB});

	mod(a.api);
	mod(b.api);
	ok('factory ran twice (two sessions, one shared spool)');

	// 2. registration -------------------------------------------------------
	for (const bundle of [a, b]) {
		const names = toolNames(bundle);
		for (const expected of ['pubsub_fire', 'pubsub_send', 'pubsub_list', 'pubsub_cancel']) {
			assert.ok(names.includes(expected), `tool ${expected} must be registered, got ${names.join(', ')}`);
		}
		for (const def of bundle.toolDefs) {
			assert.equal(typeof def.run, 'function', `tool ${def.schema?.name} must have a run function`);
			assert.equal(def.schema?.input_schema?.type, 'object', `tool ${def.schema?.name} needs an object schema`);
		}
		assert.ok(commandNames(bundle).includes('pubsub'), `command pubsub must be registered, got ${commandNames(bundle).join(', ')}`);
		assert.ok(bundle.flagDefs.has('pubsub_dir'), 'flag pubsub_dir must be registered');
		assert.ok(bundle.flagDefs.has('pubsub_deliver'), 'flag pubsub_deliver must be registered');
		assert.equal(bundle.flagDefs.get('pubsub_deliver').default, 'steer', 'pubsub_deliver defaults to steer');
		assert.ok(bundle.rendererDefs.some((r) => r.id === 'pubsub'), 'renderer pubsub must be registered');
		assert.ok(bundle.paneDefs.some((p) => p.id === 'jobs'), 'jobs pane must be registered');
		assert.equal(typeof bundle.registeredHooks.onSessionStart, 'function', 'onSessionStart hook must be registered');
		assert.equal(typeof bundle.registeredHooks.onStop, 'function', 'onStop hook must be registered');
		assert.ok(bundle.listeners.get('run_start')?.length > 0, 'run_start listener must be registered');
		assert.ok(bundle.everyTicks.length >= 2, 'drain + refresh timers must be scheduled');
	}
	ok('tools, command, flags, renderer, pane, hooks and timers all registered');

	// 3. owner token --------------------------------------------------------
	const tokensA = ownerEntries(sessionA);
	const tokensB = ownerEntries(sessionB);
	assert.equal(tokensA.length, 1, `first factory must append exactly one pubsub/owner entry, got ${tokensA.length}`);
	assert.equal(tokensB.length, 1, `second factory must append exactly one pubsub/owner entry, got ${tokensB.length}`);
	assert.notEqual(tokensA[0], tokensB[0], 'the two sessions must have different owner tokens');
	ok('each session persisted a pubsub/owner entry with a distinct token');

	// 4. cross-session isolation -------------------------------------------
	// Session A fires a signal. A's drain delivers it; B's drain must not.
	const send = a.toolDefs.find((def) => def.schema.name === 'pubsub_send');
	const sendResult = await send.run({input: {title: 'smoke signal', body: 'hello from A'}});
	assert.equal(sendResult.ok, true, 'pubsub_send must succeed');
	assert.equal(a.userMessages.length, 1, 'A must receive its own signal');
	assert.match(a.userMessages[0].text, /\[pubsub\] smoke signal/, 'signal text must be addressed');
	assert.equal(a.userMessages[0].options?.deliverAs, 'steer', 'default delivery is steer');
	assert.equal(b.userMessages.length, 0, 'B must NOT receive a signal owned by A');
	assert.ok(a.shownEntries.some((entry) => entry.type === 'pubsub'), 'A must show the signal as a feed entry');
	assert.ok(b.shownEntries.length === 0, 'B must not show A\'s signal');
	ok('a signal fired by session A is delivered to A only, never to B');

	// 5. legacy untagged signals are quarantined ---------------------------
	const inbox = join(spoolDir, 'inbox');
	const orphaned = join(spoolDir, 'orphaned');
	const inboxBefore = readdirSync(inbox).length;
	writeFileSync(
		join(inbox, `${Date.now()}-legacy-unowned.json`),
		JSON.stringify({id: 'legacy-unowned', label: 'legacy', kind: 'command', state: 'done', title: 'legacy', body: 'x'}),
	);
	assert.equal(readdirSync(inbox).length, inboxBefore + 1, 'legacy signal must be staged in inbox');

	// A run_start tick drives B's drain over the shared spool.
	b.listeners.get('run_start')[0]({sessionId: 'session-b'});
	assert.ok(readdirSync(orphaned).length >= 1, 'untagged signal must be quarantined to orphaned/');
	assert.ok(
		!readdirSync(inbox).some((name) => name.includes('legacy-unowned')),
		'untagged signal must leave the inbox',
	);
	assert.equal(b.userMessages.length, 0, 'untagged signal must never be delivered');
	assert.equal(a.userMessages.length, 1, 'untagged signal must never be delivered to A either');
	ok('untagged legacy signal quarantined to orphaned/, delivered to nobody');

	// 6. spool layout -------------------------------------------------------
	for (const dir of ['inbox', 'claimed', 'orphaned', 'logs', 'jobs']) {
		assert.ok(existsSync(join(spoolDir, dir)), `spool dir ${dir}/ must exist`);
	}
	assert.ok(existsSync(join(spoolDir, 'delivered.jsonl')), 'delivery ledger must be written');
	ok('spool layout: inbox/ claimed/ orphaned/ logs/ jobs/ delivered.jsonl');

	// 7. command handlers ---------------------------------------------------
	const command = a.commandDefs.find((def) => def.name === 'pubsub');
	const listed = command.handler({args: 'list'});
	assert.equal(typeof listed?.message, 'string', '/pubsub list must return a message');
	assert.match(listed.message, /recent job/, '/pubsub list must summarise jobs');
	const canceled = command.handler({args: 'cancel does-not-exist'});
	assert.match(canceled.message, /no job/, '/pubsub cancel <unknown> must report a missing job');
	const opened = command.handler({args: 'jobs'});
	assert.equal(typeof opened?.message, 'string', '/pubsub jobs must return a message');
	assert.ok(a.openPaneCalls.includes('jobs'), '/pubsub jobs must open the jobs pane');
	ok('/pubsub list | cancel <id> | jobs behave');

	// 8. tools that must not spawn anything -------------------------------
	const list = a.toolDefs.find((def) => def.schema.name === 'pubsub_list');
	const listResult = await list.run({input: {}});
	assert.equal(listResult.ok, true, 'pubsub_list must succeed');
	assert.equal(typeof listResult.content[0].text, 'string', 'pubsub_list must return text');

	const cancel = a.toolDefs.find((def) => def.schema.name === 'pubsub_cancel');
	const cancelMissing = await cancel.run({input: {id: 'nope'}});
	assert.equal(cancelMissing.ok, false, 'pubsub_cancel on an unknown id must report an error');
	const cancelNoId = await cancel.run({input: {}});
	assert.equal(cancelNoId.ok, false, 'pubsub_cancel without an id must report an error');

	const fire = a.toolDefs.find((def) => def.schema.name === 'pubsub_fire');
	assert.equal((await fire.run({input: {label: 'x'}})).ok, false, 'pubsub_fire needs command or prompt');
	assert.equal(
		(await fire.run({input: {label: 'x', command: 'true', prompt: 'hi'}})).ok,
		false,
		'pubsub_fire rejects command + prompt together',
	);
	assert.equal((await fire.run({input: {command: 'true'}})).ok, false, 'pubsub_fire requires a label');
	ok('tool input validation and list/cancel paths behave');

	process.stdout.write('\nPASS\n');
} catch (error) {
	fail(error);
}
