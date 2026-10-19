// Smoke test for the pubsub mod.
//
// Runs against the real `pubsub.ts` source with Node's native TypeScript type
// stripping, so no build step and no dependencies. Everything the mod touches
// (spool dirs, session transcript, UI, OS) is a fake.
//
// What this proves:
//   1. the file is loadable as an ES module and default-exports a factory
//   2. the factory registers the tools, command and flags the README documents
//   3. the session's owner token is persisted as a `pubsub/owner` custom entry
//   4. two sessions get DIFFERENT owner tokens, and a drain in session B never
//      delivers a signal fired by session A  <- the cross-session isolation invariant
//   5. untagged legacy signals are quarantined to `orphaned/`, never delivered
//   6. the runtime seam is real: spawn, kill and the clock come from the second
//      factory argument, so the OS never runs during the suite

import assert from 'node:assert/strict';
import {
	appendFileSync,
	existsSync,
	mkdirSync,
	mkdtempSync,
	readdirSync,
	readFileSync,
	renameSync,
	rmSync,
	statSync,
	writeFileSync,
} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {Script} from 'node:vm';

import mod, {jobStates, ledgerFactory} from '../pubsub.ts';

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

// A session whose transcript refuses the first append (read-only transcript, a racing
// writer). The mod must still know its own identity.
function makeFlakySession(failures = 1) {
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

// The OS seam. The mod takes a runtime object as the second factory argument:
// spawn, kill, the clock and the spool filesystem. Production fills it from the
// OS; here it is a fake, so a spawn failure or a cancel escalation is a function
// call instead of a process.
function makeChild({pid = 424242} = {}) {
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

function makeRuntime(overrides = {}) {
	const spawnCalls = [];
	const kills = [];
	const runtime = {
		spawn(command, args, options) {
			const child = makeChild();
			spawnCalls.push({command, args, options, child});
			return child;
		},
		kill(pid, signal) {
			kills.push({pid, signal});
		},
		...overrides,
	};
	return {runtime, spawnCalls, kills};
}

// A ModApi fake with exactly the surface the mod uses. No speculative members:
// if the mod starts reaching for something else, the suite fails loudly.
function makeApi({spoolDir, session, runtime}) {
	const toolDefs = [];
	const commandDefs = [];
	const rendererDefs = [];
	const flagDefs = new Map();
	const flagOverrides = new Map([['pubsub_dir', spoolDir]]);
	const registeredHooks = {};
	const listeners = new Map();
	const everyTicks = [];
	const afterTicks = [];
	const paneDefs = [];
	const openPaneCalls = [];
	const statusCalls = [];
	const shownEntries = [];
	const userMessages = [];
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
		addRenderer(id, render) {
			rendererDefs.push({id, render});
		},
		addTool(def) {
			toolDefs.push(def);
		},
		addCommand(def) {
			commandDefs.push(def);
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

// The detached worker is a string in the mod, so a syntax error in it would only
// surface inside a process nobody is watching. Compile it here instead.
function workerSource() {
	const source = readFileSync(new URL('../pubsub.ts', import.meta.url), 'utf8');
	const start = source.indexOf('const WORKER_SOURCE = `');
	const end = source.indexOf('\n`;', start);
	assert.ok(start > 0 && end > start, 'WORKER_SOURCE must be findable in pubsub.ts');
	const literal = source.slice(start + 'const WORKER_SOURCE = '.length, end + 2);
	return Function(`"use strict"; return ${literal};`)();
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
	const a = makeApi({spoolDir, session: sessionA, runtime: makeRuntime().runtime});
	const b = makeApi({spoolDir, session: sessionB, runtime: makeRuntime().runtime});

	mod(a.api, a.runtime);
	mod(b.api, b.runtime);
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

	// 9. runtime seam -------------------------------------------------------
	// A fresh spool and a fake clock, so these assertions cannot be perturbed by
	// the shared-spool sections above.
	const seamDir = mkdtempSync(join(tmpdir(), 'pubsub-seam-'));
	const fixedNow = 1_700_000_000_000;
	const seamRuntime = makeRuntime({now: () => fixedNow});
	const seamSession = makeSession();
	const c = makeApi({spoolDir: seamDir, session: seamSession, runtime: seamRuntime.runtime});
	mod(c.api, c.runtime);

	const seamFire = c.toolDefs.find((def) => def.schema.name === 'pubsub_fire');
	const fired = await seamFire.run({input: {label: 'seam', command: 'printf hi'}});
	assert.equal(fired.ok, true, `pubsub_fire must succeed through the fake runtime: ${JSON.stringify(fired)}`);
	assert.equal(seamRuntime.spawnCalls.length, 1, 'exactly one spawn must come out of the injected runtime');
	const spawnCall = seamRuntime.spawnCalls[0];
	assert.equal(spawnCall.command, process.execPath, 'the detached worker runs on the current node');
	assert.equal(spawnCall.args[0], '-e', 'the worker source is passed with -e');
	assert.equal(spawnCall.options.detached, true, 'the worker is detached');
	assert.equal(spawnCall.options.env.PUBSUB_WORKER, '1', 'the worker is marked as a worker');
	const spec = JSON.parse(spawnCall.options.env.PUBSUB_SPEC);
	assert.equal(spec.label, 'seam', 'the job spec carries the label');
	assert.equal(spec.command, 'printf hi', 'the job spec carries the command');
	const firedId = spec.id;

	const recordPath = join(seamDir, 'jobs', `${firedId}.json`);
	const record = JSON.parse(readFileSync(recordPath, 'utf8'));
	assert.equal(record.startedAt, fixedNow, 'the job record uses the injected clock');
	assert.equal(record.workerPid, 424242, 'the injected worker pid lands on the record');

	seamRuntime.kills.length = 0;
	const seamCancel = c.toolDefs.find((def) => def.schema.name === 'pubsub_cancel');
	const canceledJob = await seamCancel.run({input: {id: firedId}});
	assert.equal(canceledJob.ok, true, `pubsub_cancel must cancel the fired job: ${JSON.stringify(canceledJob)}`);
	assert.deepEqual(
		seamRuntime.kills[0],
		{pid: 424242, signal: 'SIGTERM'},
		'the first kill goes through the injected runtime',
	);
	const escalate = c.afterTicks.at(-1);
	assert.equal(typeof escalate?.fn, 'function', 'cancel schedules a SIGKILL escalation');
	escalate.fn();
	assert.deepEqual(seamRuntime.kills.at(-1), {pid: 424242, signal: 'SIGKILL'}, 'escalation also uses the runtime');
	ok('spawn, kill and the clock all cross the runtime seam');

	// 10. envelope ---------------------------------------------------------
	// signalOf/patchOf are the only constructors of a job result, so the record
	// and the signal for one state change cannot carry different facts.
	const canceledRecord = JSON.parse(readFileSync(recordPath, 'utf8'));
	assert.equal(canceledRecord.state, 'canceled', 'the canceled record is written from the signal');
	assert.equal(canceledRecord.failureReason, 'canceled by request', 'the record carries the signal reason');
	assert.ok(
		c.userMessages.some((message) => message.text.includes('canceled by request')),
		'the canceled signal carries the same reason as the record',
	);
	assert.equal(canceledRecord.durationMs, 0, 'duration comes from the envelope, not from a hand-written literal');

	const worker = workerSource();
	new Script(worker, {filename: 'PUBSUB_WORKER_SOURCE'});
	assert.match(worker, /PUBSUB_SPEC/, 'the worker reads its spec from the environment');
	ok('one envelope for record and signal; the inline worker source compiles');

	// 11. spool claim / settle ---------------------------------------------
	// A signal this session does not own belongs to another session. onStop used to
	// delete one when the restore rename failed; settle('held') must never destroy
	// it, and the owner must still collect it exactly once.
	const raceDir = mkdtempSync(join(tmpdir(), 'pubsub-race-'));
	const raceSessionA = makeSession();
	const raceSessionB = makeSession();
	const restoreFails = {
		rename(from, to) {
			if (from.startsWith(join(raceDir, 'claimed')) && to.startsWith(join(raceDir, 'inbox'))) {
				throw new Error('EXDEV'); // the restore path the old code deleted on
			}
			return renameSync(from, to);
		},
	};
	const raceA = makeApi({spoolDir: raceDir, session: raceSessionA, runtime: makeRuntime().runtime});
	const raceB = makeApi({spoolDir: raceDir, session: raceSessionB, runtime: makeRuntime({fs: restoreFails}).runtime});
	mod(raceA.api, raceA.runtime);
	mod(raceB.api, raceB.runtime);
	const raceOwner = ownerEntries(raceSessionA)[0];
	assert.notEqual(ownerEntries(raceSessionB)[0], raceOwner, 'the two racing sessions are distinct owners');

	writeFileSync(
		join(raceDir, 'inbox', `${Date.now()}-race-1.json`),
		JSON.stringify({id: 'race-1', owner: raceOwner, label: 'race', kind: 'command', state: 'done', title: 'race done', body: 'ok'}),
	);
	assert.equal(raceB.registeredHooks.onStop(), undefined, 'onStop hands nothing foreign to the model');
	assert.equal(raceB.userMessages.length, 0, "B must not receive A's signal");
	assert.equal(raceB.shownEntries.length, 0, "B must not show A's signal");
	const raceFiles = readdirSync(join(raceDir, 'inbox')).filter((name) => name.includes('race-1'));
	assert.equal(raceFiles.length, 1, "a foreign signal survives a failed restore: it is put back, never deleted");
	assert.equal(
		JSON.parse(readFileSync(join(raceDir, 'inbox', raceFiles[0]), 'utf8')).id,
		'race-1',
		'the held signal comes back intact',
	);
	ok("onStop never deletes a foreign session's signal");

	// The pending count reads the same spool, so it cannot disagree with delivery.
	const raceCommandA = raceA.commandDefs.find((def) => def.name === 'pubsub');
	const raceCommandB = raceB.commandDefs.find((def) => def.name === 'pubsub');
	assert.match(
		raceCommandA.handler({args: 'list'}).message,
		/1 signal\(s\) waiting for this session/,
		'A sees its one pending signal',
	);
	assert.match(
		raceCommandB.handler({args: 'list'}).message,
		/0 signal\(s\) waiting for this session/,
		'B sees none of A\'s signals',
	);
	raceA.listeners.get('run_start')[0]({sessionId: 'race-a'});
	assert.equal(raceA.userMessages.length, 1, 'the owner collects the held signal once');
	assert.match(raceA.userMessages[0].text, /race done/, 'the held signal arrives intact');
	raceA.listeners.get('run_start')[0]({sessionId: 'race-a'});
	assert.equal(raceA.userMessages.length, 1, 'and only once');
	ok('a held signal is delivered to its owner exactly once');

	// 12. no session bound is a state, not an exception ---------------------
	const unboundDir = mkdtempSync(join(tmpdir(), 'pubsub-unbound-'));
	const unboundSession = makeSession();
	const u = makeApi({spoolDir: unboundDir, session: unboundSession, runtime: makeRuntime().runtime});
	mod(u.api, u.runtime);
	const unboundOwner = ownerEntries(unboundSession)[0];
	writeFileSync(
		join(unboundDir, 'inbox', `${Date.now()}-unbound-1.json`),
		JSON.stringify({id: 'unbound-1', owner: unboundOwner, label: 'unbound', kind: 'command', state: 'done', title: 'unbound', body: 'x'}),
	);
	u.api.session = undefined; // the session unbinds (hot reload teardown) before the drain
	const drainTick = u.everyTicks.find((tick) => tick.ms === 400);
	assert.equal(typeof drainTick?.fn, 'function', 'a drain tick is scheduled');
	drainTick.fn();
	assert.ok(
		readdirSync(join(unboundDir, 'inbox')).some((name) => name.includes('unbound-1')),
		'no bound session: the signal is held in inbox/, not dropped',
	);
	assert.equal(u.userMessages.length, 0, 'nothing is delivered without a session');
	ok('no session bound holds the signal instead of throwing');


	// 13. job state machine -------------------------------------------------
	assert.deepEqual(
		[...jobStates.list],
		['queued', 'running', 'waiting-input', 'done', 'failed', 'canceled'],
		'the six states live in one table',
	);
	assert.equal(jobStates.isState('running'), true, 'a state in the table is a state');
	assert.equal(jobStates.isState('teleported'), false, 'a state outside the table is not a state');
	assert.equal(jobStates.normalize('completed'), 'done', 'legacy aliases normalize in one place');
	assert.equal(jobStates.normalize('agent-done'), 'done');
	assert.equal(jobStates.normalize('command-done'), 'done');
	assert.equal(jobStates.normalize('done'), 'done', 'a canonical state normalizes to itself');
	assert.equal(jobStates.normalize(undefined), undefined, 'a missing state is not invented');
	assert.equal(jobStates.canTransition('queued', 'running'), true);
	assert.equal(jobStates.canTransition('running', 'done'), true);
	assert.equal(jobStates.canTransition('done', 'running'), false, 'a terminal state has no outgoing edge');
	assert.equal(jobStates.canTransition('queued', 'done'), false, 'running cannot be skipped');
	assert.equal(jobStates.canTransition('done', 'done'), true, 'writing the same state is not a transition');
	assert.equal(jobStates.isTerminal('canceled'), true);
	assert.equal(jobStates.isTerminal('queued'), false);
	assert.equal(jobStates.marker('done'), '✓');
	assert.equal(jobStates.marker('teleported'), '⚠', 'an unknown state is never a silent dot');
	ok('state table: six states, one alias map, one edge rule');

	// 14. an invalid transition is rejected ---------------------------------
	const stateDir = mkdtempSync(join(tmpdir(), 'pubsub-state-'));
	const stateSession = makeSession();
	const stateRuntime = makeRuntime({now: () => fixedNow});
	const s = makeApi({spoolDir: stateDir, session: stateSession, runtime: stateRuntime.runtime});
	mod(s.api, s.runtime);
	const stateFire = s.toolDefs.find((def) => def.schema.name === 'pubsub_fire');
	const stateFired = await stateFire.run({input: {label: 'terminal', command: 'true'}});
	assert.equal(stateFired.ok, true, 'the job is fired');
	const stateSpec = JSON.parse(stateRuntime.spawnCalls[0].options.env.PUBSUB_SPEC);
	const stateRecordPath = join(stateDir, 'jobs', `${stateSpec.id}.json`);

	// The worker already wrote a terminal record ...
	writeFileSync(
		stateRecordPath,
		JSON.stringify({
			...JSON.parse(readFileSync(stateRecordPath, 'utf8')),
			state: 'done',
			exitCode: 0,
			finishedAt: fixedNow,
			durationMs: 3,
		}),
	);
	const inboxBeforeError = readdirSync(join(stateDir, 'inbox')).length;
	// ... and a late spawn error arrives afterwards.
	stateRuntime.spawnCalls[0].child.emit('error', new Error('late spawn error'));
	const afterError = JSON.parse(readFileSync(stateRecordPath, 'utf8'));
	assert.equal(afterError.state, 'done', 'a terminal job is not dragged back to failed');
	assert.equal(
		readdirSync(join(stateDir, 'inbox')).length,
		inboxBeforeError,
		'a rejected transition emits no phantom signal',
	);

	// A state the table does not know is loud in the pane, not a dot.
	writeFileSync(
		join(stateDir, 'jobs', 'zz-teleported.json'),
		JSON.stringify({id: 'zz-teleported', label: 'teleported', state: 'teleported', startedAt: fixedNow}),
	);
	s.everyTicks.find((tick) => tick.ms === 1000).fn();
	const pane = s.paneDefs.find((def) => def.id === 'jobs');
	const rendered = JSON.stringify(pane.render());
	assert.ok(rendered.includes('⚠'), 'an unknown state renders as a warning marker');
	assert.ok(rendered.includes('teleported'), 'and its raw state stays visible');
	ok('an invalid transition is rejected and an unknown state is loud');


	// 15. one spawn-failure choke point -------------------------------------
	// Three different ways a spawn can fail, one contract: a failed record, exactly
	// one failed signal, exactly one delivery.
	const failureModes = [
		{
			name: 'sync throw',
			make() {
				const bundle = makeRuntime({
					spawn() {
						throw new Error('EAGAIN');
					},
				});
				return {...bundle, trigger() {}};
			},
			toolFails: true,
		},
		{
			name: 'async error event',
			make() {
				const child = makeChild();
				const bundle = makeRuntime({spawn: () => child});
				return {...bundle, trigger: () => child.emit('error', new Error('EPIPE'))};
			},
			toolFails: false,
		},
		{
			name: 'missing pid',
			make() {
				const bundle = makeRuntime({spawn: () => makeChild({pid: null})});
				return {...bundle, trigger() {}};
			},
			toolFails: true,
		},
	];

	for (const mode of failureModes) {
		const dir = mkdtempSync(join(tmpdir(), 'pubsub-fail-'));
		const session = makeSession();
		const bundle = mode.make();
		const api = makeApi({spoolDir: dir, session, runtime: bundle.runtime});
		mod(api.api, api.runtime);
		const fireTool = api.toolDefs.find((def) => def.schema.name === 'pubsub_fire');
		const launched = await fireTool.run({input: {label: 'boom', command: 'true'}});
		assert.equal(launched.ok, !mode.toolFails, `${mode.name}: the tool reports the failure state`);
		bundle.trigger();

		const signals = readdirSync(join(dir, 'inbox')).filter((name) => name.endsWith('.json'));
		assert.equal(signals.length, 1, `${mode.name}: exactly one failed signal`);
		const signal = JSON.parse(readFileSync(join(dir, 'inbox', signals[0]), 'utf8'));
		assert.equal(signal.state, 'failed', `${mode.name}: the signal is a failure`);
		assert.equal(typeof signal.body, 'string', `${mode.name}: the signal carries the reason`);

		const records = readdirSync(join(dir, 'jobs')).filter((name) => name.endsWith('.json'));
		assert.equal(records.length, 1, `${mode.name}: one job record`);
		const record = JSON.parse(readFileSync(join(dir, 'jobs', records[0]), 'utf8'));
		assert.equal(record.state, 'failed', `${mode.name}: the record is failed too`);
		assert.equal(record.failureReason, signal.body, `${mode.name}: record and signal carry the same reason`);
		assert.equal(record.id, signal.id, `${mode.name}: the signal belongs to the record`);

		api.listeners.get('run_start')[0]({sessionId: 'failure'});
		const delivered = api.userMessages.filter((message) => message.text.includes('boom failed'));
		assert.equal(delivered.length, 1, `${mode.name}: exactly one failed signal is delivered`);
	}
	ok('every spawn failure goes through one choke point and emits one signal');


	// 16. owner identity ----------------------------------------------------
	// A session that cannot persist its token on the first try still owns its jobs. The
	// old process-<pid> fallback stamped a token the session could never match, so its
	// own result became foreign and was never delivered.
	const ownerDir = mkdtempSync(join(tmpdir(), 'pubsub-owner-'));
	// The transcript refuses every append made before the job is fired, and recovers
	// afterwards - the exact window the old code filled with process-<pid>.
	const flaky = makeFlakySession(2);
	const ownerRuntime = makeRuntime({now: () => fixedNow});
	const o = makeApi({spoolDir: ownerDir, session: flaky, runtime: ownerRuntime.runtime});
	mod(o.api, o.runtime);
	assert.equal(ownerEntries(flaky).length, 0, 'the transcript refuses appends for now');
	const ownerFire = o.toolDefs.find((def) => def.schema.name === 'pubsub_fire');
	assert.equal((await ownerFire.run({input: {label: 'orphan-proof', command: 'true'}})).ok, true);
	const ownerSpec = JSON.parse(ownerRuntime.spawnCalls[0].options.env.PUBSUB_SPEC);
	assert.ok(ownerSpec.owner, 'the job carries an owner even while the transcript refuses it');
	assert.ok(!ownerSpec.owner.startsWith('process-'), 'there is no process-<pid> fallback identity');

	// The transcript recovers. The persisted identity must be the one the job carries.
	o.registeredHooks.onSessionStart({source: 'resume', sessionId: 'owner'});
	assert.deepEqual(ownerEntries(flaky), [ownerSpec.owner], 'the retried token is the token the job carries');

	writeFileSync(
		join(ownerDir, 'inbox', `${Date.now()}-owner-proof.json`),
		JSON.stringify({
			id: 'owner-proof',
			owner: ownerSpec.owner,
			label: 'owner proof',
			kind: 'command',
			state: 'done',
			title: 'owner proof',
			body: 'ok',
		}),
	);
	o.listeners.get('run_start')[0]({sessionId: 'owner'});
	assert.equal(
		o.userMessages.filter((message) => message.text.includes('owner proof')).length,
		1,
		'a session collects a signal stamped with its own retried token',
	);
	ok('owner identity: persisted, retried, never a process-pid fallback');

	// An unresolved identity is a state: the signal waits, it is not guessed at.
	const pendingDir = mkdtempSync(join(tmpdir(), 'pubsub-pending-'));
	const pendingApi = makeApi({spoolDir: pendingDir, session: undefined, runtime: makeRuntime().runtime});
	mod(pendingApi.api, pendingApi.runtime);
	writeFileSync(
		join(pendingDir, 'inbox', `${Date.now()}-pending-1.json`),
		JSON.stringify({
			id: 'pending-1',
			owner: 'owner-somebody-else',
			label: 'pending',
			kind: 'command',
			state: 'done',
			title: 'pending',
			body: 'x',
		}),
	);
	pendingApi.everyTicks.find((tick) => tick.ms === 400).fn();
	assert.ok(
		readdirSync(join(pendingDir, 'inbox')).some((name) => name.includes('pending-1')),
		'pending ownership holds the signal in inbox/',
	);
	assert.equal(pendingApi.userMessages.length, 0, 'nothing is delivered on an unknown identity');
	ok('an unresolved owner is pending, not foreign and not a delivery guess');


	// 17. ledger ------------------------------------------------------------
	// The replay guard and retention are one module: retention drops the oldest facts
	// in memory AND on disk, so a full ledger can never empty the guard.
	const ledgerDir = mkdtempSync(join(tmpdir(), 'pubsub-ledger-'));
	const ledgerPath = join(ledgerDir, 'delivered.jsonl');
	const realFs = {
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
	};
	let ledgerClock = fixedNow;
	const ledgerOptions = {
		fs: realFs,
		path: ledgerPath,
		now: () => (ledgerClock += 1),
		max: 5,
		keep: 3,
		compactBytes: 1024 * 1024,
	};
	const ledger = ledgerFactory(ledgerOptions);
	for (const id of ['one', 'two', 'three', 'four', 'five']) ledger.accept(id);
	assert.equal(ledger.size(), 5, 'five facts fit under the cap');
	assert.equal(ledger.has('one'), true);
	ledger.accept('six');
	assert.equal(ledger.has('six'), true, 'the newest fact is always kept');
	assert.ok(ledger.size() <= 3, 'retention bounds the ledger instead of clearing it');
	assert.equal(ledger.has('one'), false, 'the oldest fact is the one dropped');

	const diskIds = () =>
		readFileSync(ledgerPath, 'utf8')
			.split('\n')
			.filter(Boolean)
			.map((line) => JSON.parse(line).id);
	assert.equal(diskIds().length, ledger.size(), 'memory and disk hold the same number of facts');
	assert.ok(diskIds().includes('six'), 'the kept facts are on disk');
	assert.ok(!diskIds().includes('one'), 'the dropped facts are off disk too');
	ledger.retain(1);
	assert.equal(ledger.size(), 1, 'retain(n) is the only compaction');
	assert.deepEqual(diskIds(), ['six'], 'and it rewrites the file to match');

	const restarted = ledgerFactory({...ledgerOptions, now: () => (ledgerClock += 1)});
	assert.equal(restarted.has('six'), true, 'a restart cannot replay a delivered signal');
	assert.equal(restarted.size(), ledger.size(), 'the restart agrees with the disk');
	for (let index = 0; index < 50; index += 1) restarted.accept(`bulk-${index}`);
	assert.ok(restarted.size() > 0, 'a full ledger never clears its guard');
	assert.equal(restarted.has('bulk-49'), true, 'the newest fact survives repeated retention');
	ok('ledger: has/accept/retain keep memory and disk consistent and never clear');

	// 18. no replay after a restart -----------------------------------------
	const replayDir = mkdtempSync(join(tmpdir(), 'pubsub-replay-'));
	const replaySession = makeSession();
	const replayOne = makeApi({spoolDir: replayDir, session: replaySession, runtime: makeRuntime().runtime});
	mod(replayOne.api, replayOne.runtime);
	await replayOne.toolDefs
		.find((def) => def.schema.name === 'pubsub_send')
		.run({input: {title: 'once only', body: 'first'}});
	assert.equal(replayOne.userMessages.length, 1, 'the first factory delivers once');
	const deliveredId = replayOne.userMessages[0].text.match(/job (\S+)/)[1];
	const replayOwner = ownerEntries(replaySession)[0];

	// A second factory over the same spool and the same session: the durable ledger
	// already knows that id, so a replayed copy must be cleared, not delivered.
	const replayTwo = makeApi({spoolDir: replayDir, session: replaySession, runtime: makeRuntime().runtime});
	mod(replayTwo.api, replayTwo.runtime);
	writeFileSync(
		join(replayDir, 'inbox', `${Date.now()}-replay.json`),
		JSON.stringify({
			id: deliveredId,
			owner: replayOwner,
			label: 'once only',
			kind: 'note',
			state: 'note',
			title: 'once only',
			body: 'first',
		}),
	);
	replayTwo.listeners.get('run_start')[0]({sessionId: 'replay'});
	assert.equal(replayTwo.userMessages.length, 0, 'a second factory does not replay a delivered signal');
	assert.equal(
		readdirSync(join(replayDir, 'inbox')).filter((name) => name.includes(deliveredId)).length,
		0,
		'the replayed copy is cleared, not delivered',
	);
	ok('no replay after a second factory over the same spool');

	process.stdout.write('\nPASS\n');
} catch (error) {
	fail(error);
}
