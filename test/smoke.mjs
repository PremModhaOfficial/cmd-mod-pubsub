// Smoke test for the better-sub-agents mod.
//
// Runs against the real `better-sub-agents.ts` source with Node's native TypeScript
// stripping, so no build step and no dependencies. Everything the mod touches
// (spool dirs, session transcript, UI, OS) is a fake.
//
// What this proves:
//   1. the file is loadable as an ES module and default-exports a factory
//   2. the factory registers the tools, command and flags the README documents
//   3. the session's owner token is persisted as a `subagent/owner` custom entry
//   4. two sessions get DIFFERENT owner tokens, and a drain in session B never
//      delivers a signal fired by session A  <- the cross-session isolation invariant
//   5. untagged legacy signals are quarantined to `orphaned/`, never delivered
//   6. the runtime seam is real: spawn, kill and the clock come from the second
//      factory argument, so the OS never runs during the suite

import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {chmodSync, existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, renameSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {Script} from 'node:vm';

import mod, {
	commandEscapesWorktree,
	createJobWorktree,
	defaultBranch,
	finalizeWorktree,
	guardToolCall,
	isolationRoots,
	jobStates,
	ledgerFactory,
	mainCheckoutRoots,
} from '../better-sub-agents.ts';
import {
	fail,
	makeApi,
	makeChild,
	makeFlakySession,
	makeRuntime,
	makeSession,
	modSource,
	ok,
	ownerEntries,
	realFs,
	commandNames,
	results,
	toolNames,
	toolOf,
	workerSource,
} from './harness.mjs';

// ---------------------------------------------------------------- assertions
//
// The fakes live in test/harness.mjs, shared with the end-to-end suite. This file
// only asserts.

// Write one definition file. `extra` becomes additional frontmatter lines.
function writeAgent(dir, file, name, description, body, extra = {}) {
	mkdirSync(dir, {recursive: true});
	const lines = ['---'];
	if (name !== undefined) lines.push(`name: ${name}`);
	if (description !== undefined) lines.push(`description: ${description}`);
	for (const [key, value] of Object.entries(extra)) lines.push(`${key}: ${value}`);
	lines.push('---', '', body, '');
	writeFileSync(join(dir, file), lines.join('\n'));
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
		for (const expected of ['subagent_spawn', 'subagent_send', 'subagent_list', 'subagent_output', 'subagent_cancel', 'subagent_resume']) {
			assert.ok(names.includes(expected), `tool ${expected} must be registered, got ${names.join(', ')}`);
		}
		for (const def of bundle.toolDefs) {
			assert.equal(typeof def.run, 'function', `tool ${def.schema?.name} must have a run function`);
			assert.equal(def.schema?.input_schema?.type, 'object', `tool ${def.schema?.name} needs an object schema`);
		}
		assert.ok(commandNames(bundle).includes('subagents'), `command subagents must be registered, got ${commandNames(bundle).join(', ')}`);
		assert.ok(bundle.flagDefs.has('subagent_dir'), 'flag subagent_dir must be registered');
		assert.ok(bundle.flagDefs.has('subagent_deliver'), 'flag subagent_deliver must be registered');
		assert.equal(bundle.flagDefs.get('subagent_deliver').default, 'steer', 'subagent_deliver defaults to steer');
		assert.ok(bundle.flagDefs.has('pubsub_dir'), 'the deprecated pubsub_dir flag stays registered');
		assert.ok(bundle.flagDefs.has('pubsub_deliver'), 'the deprecated pubsub_deliver flag stays registered');
		assert.equal(bundle.flagDefs.get('pubsub_deliver').default, 'steer', 'pubsub_deliver defaults to steer');
		assert.ok(bundle.rendererDefs.some((r) => r.id === 'subagent'), 'renderer subagent must be registered');
		assert.ok(bundle.rendererDefs.some((r) => r.id === 'pubsub'), 'the deprecated pubsub renderer id stays registered');
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
	assert.equal(tokensA.length, 1, `first factory must append exactly one subagent/owner entry, got ${tokensA.length}`);
	assert.equal(tokensB.length, 1, `second factory must append exactly one subagent/owner entry, got ${tokensB.length}`);
	assert.notEqual(tokensA[0], tokensB[0], 'the two sessions must have different owner tokens');
	ok('each session persisted a subagent/owner entry with a distinct token');

	// 4. cross-session isolation -------------------------------------------
	// Session A fires a signal. A's drain delivers it; B's drain must not.
	const send = toolOf(a, 'subagent_send');
	const sendResult = await send.run({input: {title: 'smoke signal', body: 'hello from A'}});
	assert.equal(sendResult.ok, true, 'subagent_send must succeed');
	assert.equal(a.userMessages.length, 1, 'A must receive its own signal');
	assert.match(a.userMessages[0].text, /\[subagent\] smoke signal/, 'signal text must be addressed');
	assert.equal(a.userMessages[0].options?.deliverAs, 'steer', 'default delivery is steer');
	assert.equal(b.userMessages.length, 0, 'B must NOT receive a signal owned by A');
	assert.ok(a.shownEntries.some((entry) => entry.type === 'subagent'), 'A must show the signal as a feed entry');
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
	const command = a.commandDefs.find((def) => def.name === 'subagents');
	const listed = command.handler({args: 'list'});
	assert.equal(typeof listed?.message, 'string', '/subagents list must return a message');
	assert.match(listed.message, /recent job/, '/subagents list must summarise jobs');
	const canceled = command.handler({args: 'cancel does-not-exist'});
	assert.match(canceled.message, /no job/, '/subagents cancel <unknown> must report a missing job');
	const opened = command.handler({args: 'jobs'});
	assert.equal(typeof opened?.message, 'string', '/subagents jobs must return a message');
	assert.ok(a.openPaneCalls.includes('jobs'), '/subagents jobs must open the jobs pane');
	ok('/subagents list | cancel <id> | jobs behave');

	// 8. tools that must not spawn anything -------------------------------
	const list = toolOf(a, 'subagent_list');
	const listResult = await list.run({input: {}});
	assert.equal(listResult.ok, true, 'subagent_list must succeed');
	assert.equal(typeof listResult.content[0].text, 'string', 'subagent_list must return text');

	const cancel = toolOf(a, 'subagent_cancel');
	const cancelMissing = await cancel.run({input: {id: 'nope'}});
	assert.equal(cancelMissing.ok, false, 'subagent_cancel on an unknown id must report an error');
	const cancelNoId = await cancel.run({input: {}});
	assert.equal(cancelNoId.ok, false, 'subagent_cancel without an id must report an error');

	const fire = toolOf(a, 'subagent_spawn');
	assert.equal((await fire.run({input: {description: 'x'}})).ok, false, 'subagent_spawn needs command or prompt');
	assert.equal(
		(await fire.run({input: {description: 'x', command: 'true', prompt: 'hi'}})).ok,
		false,
		'subagent_spawn rejects command + prompt together',
	);
	assert.equal((await fire.run({input: {command: 'true'}})).ok, false, 'subagent_spawn requires a description');
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

	const seamFire = toolOf(c, 'subagent_spawn');
	const fired = await seamFire.run({input: {description: 'seam', command: 'printf hi'}});
	assert.equal(fired.ok, true, `subagent_spawn must succeed through the fake runtime: ${JSON.stringify(fired)}`);
	assert.equal(seamRuntime.spawnCalls.length, 1, 'exactly one spawn must come out of the injected runtime');
	const spawnCall = seamRuntime.spawnCalls[0];
	assert.equal(spawnCall.command, process.execPath, 'the detached worker runs on the current node');
	assert.equal(spawnCall.args[0], '-e', 'the worker source is passed with -e');
	assert.equal(spawnCall.options.detached, true, 'the worker is detached');
	assert.equal(spawnCall.options.env.BETTER_SUBAGENTS_WORKER, '1', 'the worker is marked as a worker');
	const spec = JSON.parse(spawnCall.options.env.SUBAGENT_SPEC);
	assert.equal(spec.label, 'seam', 'the job spec carries the label');
	assert.equal(spec.command, 'printf hi', 'the job spec carries the command');
	const firedId = spec.id;

	const recordPath = join(seamDir, 'jobs', `${firedId}.json`);
	const record = JSON.parse(readFileSync(recordPath, 'utf8'));
	assert.equal(record.startedAt, fixedNow, 'the job record uses the injected clock');
	assert.equal(record.workerPid, 424242, 'the injected worker pid lands on the record');

	seamRuntime.kills.length = 0;
	const seamCancel = toolOf(c, 'subagent_cancel');
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
	assert.match(worker, /SUBAGENT_SPEC/, 'the worker reads its spec from the environment');
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
	const raceCommandA = raceA.commandDefs.find((def) => def.name === 'subagents');
	const raceCommandB = raceB.commandDefs.find((def) => def.name === 'subagents');
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
	const stateFire = toolOf(s, 'subagent_spawn');
	const stateFired = await stateFire.run({input: {description: 'terminal', command: 'true'}});
	assert.equal(stateFired.ok, true, 'the job is fired');
	const stateSpec = JSON.parse(stateRuntime.spawnCalls[0].options.env.SUBAGENT_SPEC);
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
		const fireTool = toolOf(api, 'subagent_spawn');
		const launched = await fireTool.run({input: {description: 'boom', command: 'true'}});
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
	const ownerFire = toolOf(o, 'subagent_spawn');
	assert.equal((await ownerFire.run({input: {description: 'orphan-proof', command: 'true'}})).ok, true);
	const ownerSpec = JSON.parse(ownerRuntime.spawnCalls[0].options.env.SUBAGENT_SPEC);
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
	await toolOf(replayOne, 'subagent_send').run({input: {title: 'once only', body: 'first'}});
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


	// 19. the owner travels with the record and with the signal -------------
	// A job record, its custom transcript entry and every signal for it carry one
	// owner token, and it is the token the session persisted.
	const ownedDir = mkdtempSync(join(tmpdir(), 'subagent-owned-'));
	const ownedSession = makeSession();
	const ownedRuntime = makeRuntime({now: () => fixedNow});
	const owned = makeApi({spoolDir: ownedDir, session: ownedSession, runtime: ownedRuntime.runtime});
	mod(owned.api, owned.runtime);
	assert.equal((await toolOf(owned, 'subagent_spawn').run({input: {description: 'owned', command: 'printf owned'}})).ok, true);
	const ownedSpec = JSON.parse(ownedRuntime.spawnCalls[0].options.env.SUBAGENT_SPEC);
	const ownedRecord = JSON.parse(readFileSync(join(ownedDir, 'jobs', `${ownedSpec.id}.json`), 'utf8'));
	const ownedToken = ownerEntries(ownedSession)[0];
	assert.equal(ownedRecord.owner, ownedToken, 'the job record carries the persisted owner');
	assert.equal(ownedSpec.owner, ownedToken, 'the worker spec carries the same owner');
	assert.deepEqual(
		ownedSession.getCustomEntries({customType: 'subagent/job'}).map((entry) => entry.data.owner),
		[ownedToken],
		'the transcript entry carries the same owner',
	);

	// The cancel signal is one of the signals for that job: same owner, same id. The
	// session unbinds first, so the drain holds the signal instead of delivering it and
	// the file is still there to inspect.
	owned.api.session = undefined;
	await toolOf(owned, 'subagent_cancel').run({input: {id: ownedSpec.id}});
	const ownedSignal = JSON.parse(
		readFileSync(join(ownedDir, 'inbox', readdirSync(join(ownedDir, 'inbox')).filter((n) => n.includes(ownedSpec.id))[0]), 'utf8'),
	);
	assert.equal(ownedSignal.owner, ownedToken, 'the signal carries the same owner');
	assert.equal(ownedSignal.state, 'canceled');
	assert.equal(ownedSignal.exitCode, null, 'the signal carries the exit code field');
	assert.equal(typeof ownedSignal.durationMs, 'number', 'the signal carries a duration');
	assert.equal(ownedSignal.logPath, join(ownedDir, 'logs', `${ownedSpec.id}.log`), 'the signal points at the log');
	ok('one owner on the record, the transcript entry, the worker spec and the signal');

	// 20. restart reconciliation -------------------------------------------
	// A queued/running record left behind by a dead process becomes failed after the
	// 30 second grace - and the session that owns it is told, instead of waiting for a
	// result that cannot arrive.
	const recDir = mkdtempSync(join(tmpdir(), 'subagent-reconcile-'));
	let recNow = fixedNow;
	const recRuntime = makeRuntime({now: () => recNow, alive: (pid) => pid === 777});
	const recSession = makeSession();
	const rec = makeApi({spoolDir: recDir, session: recSession, runtime: recRuntime.runtime});
	mod(rec.api, rec.runtime);
	const recOwner = ownerEntries(recSession)[0];
	const writeStale = (id, state, startedAt, workerPid, owner = recOwner) =>
		writeFileSync(
			join(recDir, 'jobs', `${id}.json`),
			JSON.stringify({id, label: id, kind: 'command', state, startedAt, workerPid, owner}),
		);
	writeStale('lost-young', 'running', recNow - 10_000, 424242);
	writeStale('lost-old', 'running', recNow - 31_000, 424242);
	writeStale('never-started', 'queued', recNow - 40_000, undefined);
	writeStale('still-alive', 'running', recNow - 40_000, 777);
	writeStale('someone-elses', 'running', recNow - 60_000, 424242, 'owner-somebody-else');
	rec.registeredHooks.onSessionStart({source: 'startup', sessionId: 'rec'});
	const stateOf = (id) => JSON.parse(readFileSync(join(recDir, 'jobs', `${id}.json`), 'utf8')).state;
	assert.equal(stateOf('lost-young'), 'running', 'inside the grace window the job is left alone');
	assert.equal(stateOf('lost-old'), 'failed', 'a dead running job fails after 30s');
	assert.equal(stateOf('never-started'), 'failed', 'a queued job whose worker never started fails');
	assert.equal(stateOf('still-alive'), 'running', 'a live worker is never failed');
	assert.equal(stateOf('someone-elses'), 'running', "another session's job is not reconciled (or signalled) here");
	const lostReason = JSON.parse(readFileSync(join(recDir, 'jobs', 'lost-old.json'), 'utf8')).failureReason;
	assert.equal(lostReason, 'lost (no live process)', 'the record says why');
	const told = rec.userMessages.filter((message) => message.text.includes('lost-old'));
	assert.equal(told.length, 1, 'the owner is told once that the result is gone');
	assert.match(told[0].text, /lost \(no live process\)/, 'the message carries the reason');
	assert.match(told[0].text, /failed/, 'the reconciled signal is a failure');
	rec.userMessages.length = 0;
	rec.registeredHooks.onSessionStart({source: 'startup', sessionId: 'rec'});
	assert.equal(rec.userMessages.length, 0, 'a second reconcile does not emit a second signal');
	assert.equal(
		rec.userMessages.filter((message) => message.text.includes('someone-elses')).length,
		0,
		"and no signal is emitted on another session's behalf",
	);
	ok('reconciliation: dead queued/running fails after 30s, once, and signals its owner');

	// 21. retention ---------------------------------------------------------
	// Finished jobs expire after 30 days (BETTER_SUBAGENTS_RETENTION_DAYS), record and
	// log together. A running job is never swept, however old its record is.
	const retDir = mkdtempSync(join(tmpdir(), 'subagent-retention-'));
	const day = 24 * 60 * 60 * 1000;
	const retRuntime = makeRuntime({now: () => fixedNow, env: {...process.env}});
	const ret = makeApi({spoolDir: retDir, session: makeSession(), runtime: retRuntime.runtime});
	mod(ret.api, ret.runtime);
	const stampJob = (id, state, finishedAt) => {
		writeFileSync(
			join(retDir, 'jobs', `${id}.json`),
			JSON.stringify({id, label: id, kind: 'command', state, startedAt: finishedAt - 1000, finishedAt, updatedAt: finishedAt}),
		);
		writeFileSync(join(retDir, 'logs', `${id}.log`), 'output\n');
	};
	stampJob('old-done', 'done', fixedNow - 31 * day);
	stampJob('fresh-done', 'done', fixedNow - 29 * day);
	stampJob('old-running', 'running', undefined);
	JSON.parse(readFileSync(join(retDir, 'jobs', 'old-running.json'), 'utf8'));
	ret.registeredHooks.onSessionStart({source: 'resume', sessionId: 'ret'});
	assert.ok(!existsSync(join(retDir, 'jobs', 'old-done.json')), 'a 31 day old record is swept');
	assert.ok(!existsSync(join(retDir, 'logs', 'old-done.log')), 'its log is swept with it');
	assert.ok(existsSync(join(retDir, 'jobs', 'fresh-done.json')), 'a 29 day old record is kept');
	assert.ok(existsSync(join(retDir, 'jobs', 'old-running.json')), 'a running job is never swept');
	ret.registeredHooks.onSessionStart({source: 'resume', sessionId: 'ret'});

	// The window is configurable, and one day means one day.
	const shortDir = mkdtempSync(join(tmpdir(), 'subagent-retention-short-'));
	const short = makeApi({
		spoolDir: shortDir,
		session: makeSession(),
		runtime: makeRuntime({now: () => fixedNow, env: {...process.env, BETTER_SUBAGENTS_RETENTION_DAYS: '1'}}).runtime,
	});
	mod(short.api, short.runtime);
	writeFileSync(
		join(shortDir, 'jobs', 'two-days-old.json'),
		JSON.stringify({id: 'two-days-old', state: 'done', startedAt: fixedNow - 2 * day, finishedAt: fixedNow - 2 * day}),
	);
	short.registeredHooks.onSessionStart({source: 'resume', sessionId: 'short'});
	assert.ok(!existsSync(join(shortDir, 'jobs', 'two-days-old.json')), 'BETTER_SUBAGENTS_RETENTION_DAYS=1 sweeps two-day-old jobs');
	ok('retention: 30 days by default, BETTER_SUBAGENTS_RETENTION_DAYS overrides it');

	// 22. the onStop race window -------------------------------------------
	// A worker that finishes between the last turn and the next tick leaves its signal
	// on disk. onStop must pull it into THIS run instead of letting the session go idle.
	const stopDir = mkdtempSync(join(tmpdir(), 'subagent-onstop-'));
	const stopSession = makeSession();
	const stop = makeApi({spoolDir: stopDir, session: stopSession, runtime: makeRuntime().runtime});
	mod(stop.api, stop.runtime);
	const stopOwner = ownerEntries(stopSession)[0];
	writeFileSync(
		join(stopDir, 'inbox', `${Date.now()}-late-1.json`),
		JSON.stringify({id: 'late-1', owner: stopOwner, label: 'late', kind: 'command', state: 'done', title: 'late done', body: 'ok'}),
	);
	const stopped = stop.registeredHooks.onStop();
	assert.equal(stopped?.continue, true, 'onStop continues the run when this session has a signal');
	assert.match(stopped.reason, /late done/, 'the reason carries the signal');
	assert.equal(stop.userMessages.length, 0, 'onStop continues the run instead of sending a separate message');
	assert.equal(stop.shownEntries.length, 1, 'the signal is shown in the feed');
	assert.ok(!readdirSync(join(stopDir, 'inbox')).some((name) => name.includes('late-1')), 'the claimed signal leaves the inbox');
	assert.equal(stop.registeredHooks.onStop(), undefined, 'an empty inbox does not loop the run');
	ok('onStop drains the race window once and never loops');

	// 23. the 0.1.0 owner entry is still recognised -------------------------
	// A session that was live across the rename keeps the identity its older jobs were
	// stamped with, instead of minting a token those jobs could never match.
	const legacyDir = mkdtempSync(join(tmpdir(), 'subagent-legacy-owner-'));
	const legacySession = makeSession();
	legacySession.appendCustomEntry({customType: 'pubsub/owner', data: {token: 'owner-legacy-token'}});
	const legacyRuntime = makeRuntime({now: () => fixedNow});
	const legacy = makeApi({spoolDir: legacyDir, session: legacySession, runtime: legacyRuntime.runtime});
	mod(legacy.api, legacy.runtime);
	await toolOf(legacy, 'subagent_spawn').run({input: {description: 'legacy', command: 'true'}});
	assert.equal(
		JSON.parse(legacyRuntime.spawnCalls[0].options.env.SUBAGENT_SPEC).owner,
		'owner-legacy-token',
		'the 0.1.0 token is reused, not replaced',
	);
	assert.equal(ownerEntries(legacySession).length, 0, 'and it is not re-minted under the new type');
	ok('the pubsub/owner transcript entry of 0.1.0 is read as this session\'s identity');

	// 24. subagent_output ---------------------------------------------------
	const outDir = mkdtempSync(join(tmpdir(), 'subagent-output-'));
	const outSession = makeSession();
	const outRuntime = makeRuntime({now: () => fixedNow});
	const out = makeApi({spoolDir: outDir, session: outSession, runtime: outRuntime.runtime});
	mod(out.api, out.runtime);
	const output = toolOf(out, 'subagent_output');
	assert.equal((await output.run({input: {id: 'nope'}})).ok, false, 'an unknown id is an error');
	assert.equal((await output.run({input: {}})).ok, false, 'an id is required');

	await toolOf(out, 'subagent_spawn').run({input: {description: 'slice', command: 'printf x'}});
	const outId = JSON.parse(outRuntime.spawnCalls[0].options.env.SUBAGENT_SPEC).id;
	writeFileSync(join(outDir, 'logs', `${outId}.log`), '0123456789');
	const whole = await output.run({input: {id: outId}});
	assert.equal(whole.ok, true);
	assert.match(whole.content[0].text, /0123456789/, 'the default read returns the log');
	assert.match(whole.content[0].text, /log .*\.log/, 'and where the log is');
	const windowed = await output.run({input: {id: outId, offset: 2, max_chars: 3}});
	assert.match(windowed.content[0].text, /234/, 'offset and max_chars slice the log');
	const tailed = await output.run({input: {id: outId, max_chars: 200}});
	assert.match(tailed.content[0].text, /0123456789/, 'a small log is returned whole');
	assert.equal(
		JSON.parse(readFileSync(join(outDir, 'jobs', `${outId}.json`), 'utf8')).state,
		'queued',
		'the record is still queued (the fake worker never runs)',
	);
	const waited = await output.run({input: {id: outId, wait: true, wait_ms: 1}});
	assert.equal(waited.ok, true, 'a bounded wait on a live job returns the live log instead of hanging');
	assert.match(waited.content[0].text, /still queued|queued/, 'the state is reported');
	ok('subagent_output: log tail, offset window, bounded wait and unknown-id errors');

	// 25. subagent_resume ---------------------------------------------------
	const resDir = mkdtempSync(join(tmpdir(), 'subagent-resume-'));
	const resSession = makeSession();
	const resRuntime = makeRuntime({now: () => fixedNow});
	const res = makeApi({spoolDir: resDir, session: resSession, runtime: resRuntime.runtime});
	mod(res.api, res.runtime);
	const resume = toolOf(res, 'subagent_resume');
	assert.equal((await resume.run({input: {id: 'nope', prompt: 'go'}})).ok, false, 'an unknown id is an error');
	assert.equal((await resume.run({input: {id: 'nope'}})).ok, false, 'a prompt is required');

	await toolOf(res, 'subagent_spawn').run({input: {description: 'shell', command: 'true'}});
	const shellId = JSON.parse(resRuntime.spawnCalls[0].options.env.SUBAGENT_SPEC).id;
	assert.equal(
		(await resume.run({input: {id: shellId, prompt: 'go'}})).ok,
		false,
		'a shell job has no agent session to resume',
	);

	await toolOf(res, 'subagent_spawn').run({input: {description: 'agent', prompt: 'first', subagent_type: 'explore'}});
	const agentSpec = JSON.parse(resRuntime.spawnCalls[1].options.env.SUBAGENT_SPEC);
	assert.equal(agentSpec.agentType, 'explore', 'the resolved definition name is recorded');
	assert.deepEqual(agentSpec.args.slice(0, 1), ['--mod'], 'an agent worker loads the mod');
	assert.ok(agentSpec.args[1]?.endsWith('better-sub-agents.ts'), 'the mod file that defines the worker');
	assert.equal(agentSpec.args[2], 'acp', 'the worker transport is ACP');
	assert.match(agentSpec.systemPrompt, /read-only codebase explorer/, 'the definition body travels in the spec');
	const running = await resume.run({input: {id: agentSpec.id, prompt: 'continue'}});
	assert.equal(running.ok, false, 'a running job cannot be resumed');
	assert.match(running.error, /still queued|still running/);

	// The job is finished; resume continues its ACP session.
	const agentRecordPath = join(resDir, 'jobs', `${agentSpec.id}.json`);
	writeFileSync(
		agentRecordPath,
		JSON.stringify({
			...JSON.parse(readFileSync(agentRecordPath, 'utf8')),
			state: 'done',
			exitCode: 0,
			finishedAt: fixedNow,
			durationMs: 10,
			acpSessionId: 'acp-session-1',
		}),
	);
	const resumed = await resume.run({input: {id: agentSpec.id, prompt: 'continue'}});
	assert.equal(resumed.ok, true, `a finished agent job resumes: ${JSON.stringify(resumed)}`);
	const resumedSpec = JSON.parse(resRuntime.spawnCalls[2].options.env.SUBAGENT_SPEC);
	assert.equal(resumedSpec.resumeSession, 'acp-session-1', 'the resume targets the ACP session id');
	assert.equal(resumedSpec.prompt, 'continue', 'the new instruction is the resumed prompt');
	assert.match(resumedSpec.systemPrompt, /read-only codebase explorer/, 'the resumed worker keeps the definition body');
	const resumedRecord = JSON.parse(readFileSync(join(resDir, 'jobs', `${resumedSpec.id}.json`), 'utf8'));
	assert.equal(resumedRecord.resumedFrom, agentSpec.id, 'the new record says what it resumed');
	assert.equal(resumedRecord.agentType, 'explore', 'the agent type carries over');
	assert.notEqual(resumedSpec.id, agentSpec.id, 'a resume is a new job with its own signal');
	ok('subagent_resume: rejects unknown, shell and running jobs; resumes a finished one');

	// 26. the deprecated aliases are the same implementation ----------------
	// Every pubsub_* name is still registered, marked deprecated, and behaves exactly
	// like its subagent_* target - one run function, so they cannot drift.
	const aliasDir = mkdtempSync(join(tmpdir(), 'subagent-alias-'));
	const aliasRuntime = makeRuntime({now: () => fixedNow});
	const alias = makeApi({spoolDir: aliasDir, session: makeSession(), runtime: aliasRuntime.runtime});
	mod(alias.api, alias.runtime);
	const aliasPairs = [
		['subagent_spawn', 'pubsub_fire'],
		['subagent_list', 'pubsub_list'],
		['subagent_cancel', 'pubsub_cancel'],
		['subagent_send', 'pubsub_send'],
	];
	for (const [canonical, deprecated] of aliasPairs) {
		const target = toolOf(alias, canonical);
		const old = toolOf(alias, deprecated);
		assert.ok(target !== undefined, `${canonical} must be registered`);
		assert.ok(old !== undefined, `${deprecated} must stay registered for one release`);
		assert.equal(old.run, target.run, `${deprecated} must delegate to ${canonical}`);
		assert.match(old.schema.description, new RegExp(`deprecated: use ${canonical}`), `${deprecated} must say it is deprecated`);
	}
	assert.deepEqual(toolNames(alias).filter((name) => name.startsWith('pubsub_')).sort(), [
		'pubsub_fire',
		'pubsub_list',
		'pubsub_cancel',
		'pubsub_send',
	].sort(), 'the deprecated surface is exactly the 0.1.0 tool set');
	assert.equal((await toolOf(alias, 'pubsub_fire').run({input: {description: 'old name', command: 'printf old'}})).ok, true);
	assert.equal(aliasRuntime.spawnCalls.length, 1, 'the old name really spawns');
	assert.equal(
		JSON.parse(aliasRuntime.spawnCalls[0].options.env.SUBAGENT_SPEC).command,
		'printf old',
		'through the same implementation',
	);

	// subagent_list filters by state and bounds the rows.
	const listTool = toolOf(alias, 'subagent_list');
	const queuedOnly = await listTool.run({input: {state: 'queued'}});
	assert.match(queuedOnly.content[0].text, /1 job\(s\) matching queued/, 'state filters the list');
	const doneOnly = await listTool.run({input: {state: 'done'}});
	assert.match(doneOnly.content[0].text, /^0 job\(s\) matching done/, 'a state with no jobs says zero');
	const limited = await listTool.run({input: {limit: 1}});
	assert.equal(limited.content[0].text.split('\n').filter((line) => line.trim().startsWith('◌') || line.trim().startsWith('●')).length <= 1, true, 'limit bounds the rows');

	// The command and the flags have the same treatment.
	const aliasCommands = commandNames(alias).filter((name) => name === 'pubsub' || name === 'subagents').sort();
	assert.deepEqual(aliasCommands, ['pubsub', 'subagents'], '/subagents and /pubsub are both registered');
	assert.equal(alias.flagDefs.get('pubsub_dir').default, join(process.env.HOME ?? '', '.commandcode', 'pubsub'), 'the old flag default is unchanged');

	// A spool pointed at by the old flag still works, for one release.
	const legacyFlagDir = mkdtempSync(join(tmpdir(), 'subagent-legacy-flag-'));
	const legacyFlag = makeApi({spoolDir: legacyFlagDir, dirFlag: 'pubsub_dir', session: makeSession(), runtime: makeRuntime().runtime});
	mod(legacyFlag.api, legacyFlag.runtime);
	assert.ok(existsSync(join(legacyFlagDir, 'inbox')), 'pubsub_dir still selects the spool');
	ok('pubsub_* tools, /pubsub and pubsub_dir are deprecated aliases; subagent_list filters by state and limit');

	// 27. /subagents open <id> ---------------------------------------------
	const openOut = alias.commandDefs.find((def) => def.name === 'subagents').handler({args: 'open does-not-exist'});
	assert.match(openOut.message, /no job/, 'open <unknown> reports the missing job');
	const openReal = alias.commandDefs
		.find((def) => def.name === 'subagents')
		.handler({args: `open ${JSON.parse(aliasRuntime.spawnCalls[0].options.env.SUBAGENT_SPEC).id}`});
	assert.match(openReal.message, /log .*\.log/, 'open <id> shows the job and its log');
	ok('/subagents open <id> reads one job');

	// 28. the worker handles a timeout and a max-turns cap -------------------
	// The timeout is armed in the worker and kills the child it spawned; the parent
	// passes it through, so neither side can silently ignore it.
	const timeoutSpec = {
		id: 'timeout-job',
		label: 'timeout',
		kind: 'command',
		command: 'sleep 30',
		cwd: process.cwd(),
		logPath: join(outDir, 'logs', 'timeout-job.log'),
		inboxDir: join(outDir, 'inbox'),
		jobsDir: join(outDir, 'jobs'),
		startedAt: fixedNow,
		tailMax: 4000,
		timeoutMs: 100,
	};
	writeFileSync(join(outDir, 'jobs', 'timeout-job.json'), JSON.stringify({id: 'timeout-job', state: 'queued'}));
	const workerScript = workerSource();
	new Script(workerScript, {filename: 'WORKER_SOURCE'});
	assert.match(workerScript, /timed out after/, 'the worker names the timeout reason');
	assert.match(workerScript, /SIGKILL/, 'the worker escalates the kill');
	assert.match(workerScript, /max turns reached/, 'the worker reports a max-turns cap');
	assert.match(workerScript, /resumeSession|'-r'/, 'the worker can resume its own session');
	assert.equal(typeof timeoutSpec.timeoutMs, 'number');
	ok('the worker handles a timeout, a max-turns cap and a resume');

	// 32. typed spawn: subagent_type, the deprecated alias, and the fallback ----
	const typedDir = mkdtempSync(join(tmpdir(), 'subagent-typed-'));
	const typedRuntime = makeRuntime({now: () => fixedNow});
	const typed = makeApi({spoolDir: typedDir, session: makeSession(), runtime: typedRuntime.runtime});
	mod(typed.api, typed.runtime);
	const typedSpawn = toolOf(typed, 'subagent_spawn');
	const spawnSpec = async (input) => {
		const before = typedRuntime.spawnCalls.length;
		const result = await typedSpawn.run({input});
		const call = typedRuntime.spawnCalls[before];
		return {result, spec: call === undefined ? undefined : JSON.parse(call.options.env.SUBAGENT_SPEC)};
	};

	const fallback = await spawnSpec({description: 'fallback', prompt: 'do it'});
	assert.equal(fallback.result.ok, true, 'a prompt without a type spawns');
	assert.equal(fallback.spec.agentType, 'general-purpose', 'an omitted type resolves to general-purpose');
	assert.match(fallback.spec.systemPrompt, /general-purpose autonomous agent/, 'the fallback body reaches the worker');

	const canonical = await spawnSpec({description: 'canonical', prompt: 'do it', subagent_type: 'explore'});
	const aliased = await spawnSpec({description: 'aliased', prompt: 'do it', agent_type: 'explore'});
	assert.equal(canonical.spec.agentType, 'explore', 'subagent_type selects the definition');
	assert.equal(aliased.spec.agentType, canonical.spec.agentType, 'agent_type is the canonical type');
	assert.equal(aliased.spec.systemPrompt, canonical.spec.systemPrompt, 'agent_type delivers the same body');
	assert.equal(aliased.spec.model, canonical.spec.model, 'agent_type resolves the same model');
	assert.equal(aliased.spec.permissionMode, canonical.spec.permissionMode, 'agent_type resolves the same mode');

	// The body reaches the worker as its system prompt: the worker process loads this
	// mod through `cmd --mod <file> acp`, and the mod registers appendSystemPrompt
	// from the spec it was given.
	const hookRuntime = makeRuntime({
		env: {
			...process.env,
			BETTER_SUBAGENTS_WORKER: '1',
			SUBAGENT_SPEC: JSON.stringify({systemPrompt: 'DEFINITION BODY'}),
		},
	});
	const hook = makeApi({
		spoolDir: mkdtempSync(join(tmpdir(), 'subagent-hook-')),
		session: makeSession(),
		runtime: hookRuntime.runtime,
	});
	mod(hook.api, hookRuntime.runtime);
	assert.equal(typeof hook.registeredHooks.appendSystemPrompt, 'function', 'the worker registers a system-prompt hook');
	assert.equal(hook.registeredHooks.appendSystemPrompt(), 'DEFINITION BODY', 'the definition body is the worker system prompt');

	const general = await spawnSpec({description: 'general', prompt: 'do it', subagent_type: 'general'});
	assert.equal(general.spec.agentType, 'general-purpose', 'general is the host-reserved alias for general-purpose');

	const unknown = await typedSpawn.run({input: {description: 'unknown', prompt: 'x', subagent_type: 'nope'}});
	assert.equal(unknown.ok, false, 'an unknown type is rejected');
	assert.match(unknown.error, /unknown subagent_type "nope"/, 'the error names the type');
	const conflicting = await typedSpawn.run({
		input: {description: 'conflict', prompt: 'x', subagent_type: 'explore', agent_type: 'plan'},
	});
	assert.equal(conflicting.ok, false, 'two different type spellings are ambiguous');

	const typedList = await toolOf(typed, 'subagent_list').run({input: {}});
	assert.match(typedList.content[0].text, /subagent_type=explore/, 'subagent_list shows the subagent_type');
	assert.match(typedList.content[0].text, /canonical/, 'subagent_list shows the agent name');

	const emptyCwd = mkdtempSync(join(tmpdir(), 'subagent-empty-cwd-'));
	const emptyHome = mkdtempSync(join(tmpdir(), 'subagent-empty-user-'));
	const noBuiltinsRuntime = makeRuntime({
		env: {...process.env, CLAUDE_AGENT_SDK_DISABLE_BUILTIN_AGENTS: '1'},
		cwd: emptyCwd,
		home: emptyHome,
	});
	const noBuiltins = makeApi({
		spoolDir: mkdtempSync(join(tmpdir(), 'subagent-no-builtins-')),
		session: makeSession(),
		runtime: noBuiltinsRuntime.runtime,
	});
	mod(noBuiltins.api, noBuiltinsRuntime.runtime);
	const noFallback = await toolOf(noBuiltins, 'subagent_spawn').run({input: {description: 'none', prompt: 'x'}});
	assert.equal(noFallback.ok, false, 'a prompt with no general-purpose agent fails');
	assert.equal(noFallback.error, 'subagent_type is required', 'the exact documented error');

	const noExploreRuntime = makeRuntime({
		env: {...process.env, CLAUDE_CODE_DISABLE_EXPLORE_PLAN_AGENTS: '1'},
		cwd: emptyCwd,
		home: emptyHome,
	});
	const noExplore = makeApi({
		spoolDir: mkdtempSync(join(tmpdir(), 'subagent-no-explore-')),
		session: makeSession(),
		runtime: noExploreRuntime.runtime,
	});
	mod(noExplore.api, noExploreRuntime.runtime);
	assert.equal(
		(await toolOf(noExplore, 'subagent_spawn').run({input: {description: 'x', prompt: 'x', subagent_type: 'explore'}})).ok,
		false,
		'CLAUDE_CODE_DISABLE_EXPLORE_PLAN_AGENTS removes explore',
	);
	assert.equal(
		(await toolOf(noExplore, 'subagent_spawn').run({input: {description: 'x', prompt: 'x', subagent_type: 'claude'}})).ok,
		true,
		'and leaves the other built-ins',
	);
	ok('typed spawn: subagent_type, the agent_type alias, the fallback, and the exact error');

	// 33. definitions: discovery, precedence, validation, and fields ----------
	const registryRoot = mkdtempSync(join(tmpdir(), 'subagent-registry-'));
	const projectRoot = join(registryRoot, 'project');
	const nestedRoot = join(projectRoot, 'packages', 'app');
	const homeRoot = join(registryRoot, 'home');
	const projectAgents = join(projectRoot, '.commandcode', 'agents');
	const nestedAgents = join(nestedRoot, '.commandcode', 'agents');
	const userAgents = join(homeRoot, '.commandcode', 'agents');

	writeAgent(userAgents, 'shared.md', 'shared', 'user shared', 'USER SHARED BODY');
	writeAgent(userAgents, 'user-only.md', 'user-only', 'user only', 'USER ONLY BODY');
	writeAgent(projectAgents, 'shared.md', 'shared', 'project shared', 'PROJECT SHARED BODY');
	writeAgent(projectAgents, 'explore.md', 'explore', 'project explore', 'PROJECT EXPLORE BODY', {model: 'custom-model'});
	writeAgent(projectAgents, 'nested.md', 'nested', 'outer nested', 'OUTER NESTED BODY');
	writeAgent(nestedAgents, 'nested.md', 'nested', 'inner nested', 'INNER NESTED BODY');
	writeAgent(join(nestedAgents, 'deep'), 'reviewer.md', 'reviewer', 'deep reviewer', 'RECURSIVE REVIEWER BODY');
	writeAgent(nestedAgents, 'dup-a.md', 'dup', 'dup a', 'DUP A BODY');
	writeAgent(nestedAgents, 'dup-b.md', 'dup', 'dup b', 'DUP B BODY');
	writeFileSync(
		join(projectAgents, 'full.md'),
		[
			'---',
			'name: full',
			'description: "Full definition"',
			'tools: Read, Grep',
			'disallowedTools:',
			'  - Bash',
			'model: custom',
			'permissionMode: acceptEdits',
			'maxTurns: 7',
			'skills: [one, two]',
			'isolation: worktree',
			'background: true',
			'color: blue',
			'omitClaudeMd: true',
			'effort: high',
			'---',
			'',
			'FULL BODY',
			'',
		].join('\n'),
	);

	// Every skip rule in SPEC "Invalid files".
	writeFileSync(join(projectAgents, 'no-name.md'), '---\ndescription: no name\n---\nbody');
	writeFileSync(join(projectAgents, 'no-description.md'), '---\nname: no-description\n---\nbody');
	writeFileSync(join(projectAgents, 'bad-first.md'), '# heading\n---\nname: bad-first\ndescription: x\n---\nbody');
	writeFileSync(join(projectAgents, 'bad-colon.md'), '---\nname: plugin:agent\ndescription: x\n---\nbody');
	writeFileSync(join(projectAgents, 'too-long.md'), `---\nname: ${'a'.repeat(257)}\ndescription: x\n---\nbody`);
	writeFileSync(join(projectAgents, 'dash-name.md'), '---\nname: -leading\ndescription: x\n---\nbody');
	writeFileSync(join(projectAgents, 'bad-yaml.md'), '---\nname: [unclosed\ndescription: x\n---\nbody');
	writeFileSync(join(projectAgents, 'tab-indent.md'), '---\nname: tab-indent\ndescription: x\n\tmodel: y\n---\nbody');

	const registryRuntime = makeRuntime({
		now: () => fixedNow,
		cwd: nestedRoot,
		home: homeRoot,
		// The `full` definition sets `isolation: worktree`; the seam answers as a repo
		// so the job can start without touching a real checkout.
		git: (args) => {
			const key = args.join(' ');
			if (key === 'rev-parse --show-toplevel') return {code: 0, stdout: `${nestedRoot}\n`, stderr: ''};
			if (key.startsWith('rev-parse --path-format')) return {code: 0, stdout: `${nestedRoot}/.git\n`, stderr: ''};
			if (key.startsWith('symbolic-ref')) return {code: 0, stdout: 'origin/main\n', stderr: ''};
			return {code: 0, stdout: '', stderr: ''};
		},
	});
	const registrySpool = mkdtempSync(join(tmpdir(), 'subagent-registry-spool-'));
	const registry = makeApi({spoolDir: registrySpool, session: makeSession(), runtime: registryRuntime.runtime});
	mod(registry.api, registry.runtime);
	const registrySpawn = toolOf(registry, 'subagent_spawn');
	const spawnDefinition = async (type) => {
		const before = registryRuntime.spawnCalls.length;
		const result = await registrySpawn.run({input: {description: type, prompt: 'x', subagent_type: type}});
		const call = registryRuntime.spawnCalls[before];
		return {result, spec: call === undefined ? undefined : JSON.parse(call.options.env.SUBAGENT_SPEC)};
	};

	const inner = await spawnDefinition('nested');
	assert.equal(inner.spec.systemPrompt, 'INNER NESTED BODY', 'the closest project directory wins');
	const shared = await spawnDefinition('shared');
	assert.equal(shared.spec.systemPrompt, 'PROJECT SHARED BODY', 'project scope overrides user scope');
	const reviewer = await spawnDefinition('reviewer');
	assert.equal(reviewer.spec.systemPrompt, 'RECURSIVE REVIEWER BODY', 'subdirectories are scanned recursively');
	const override = await spawnDefinition('explore');
	assert.equal(override.spec.systemPrompt, 'PROJECT EXPLORE BODY', 'a project definition overrides a built-in by name');
	assert.equal(override.spec.model, 'custom-model', 'an overriding definition keeps its own model');
	const userOnly = await spawnDefinition('user-only');
	assert.equal(userOnly.spec.systemPrompt, 'USER ONLY BODY', 'user definitions load');
	const duplicate = await spawnDefinition('dup');
	assert.equal(duplicate.spec.systemPrompt, 'DUP A BODY', 'same-directory duplicates resolve by read order');

	const full = await spawnDefinition('full');
	assert.equal(full.spec.systemPrompt, 'FULL BODY', 'the body is the system prompt');
	assert.equal(full.spec.model, 'custom', 'the definition model reaches the worker');
	assert.equal(full.spec.maxTurns, 7, 'maxTurns reaches the worker');
	assert.equal(full.spec.effort, 'high', 'effort reaches the worker');
	assert.equal(full.spec.permissionMode, 'acceptEdits', 'permissionMode reaches the worker');
	const fullRecord = JSON.parse(readFileSync(join(registrySpool, 'jobs', `${full.spec.id}.json`), 'utf8'));
	assert.equal(fullRecord.isolation, 'worktree', 'isolation is recorded on the job');
	assert.equal(fullRecord.effort, 'high', 'effort is recorded on the job');
	assert.equal(fullRecord.permissionMode, 'acceptEdits', 'permissionMode is recorded on the job');

	const overridden = await registrySpawn.run({
		input: {description: 'override', prompt: 'x', subagent_type: 'full', model: 'call-model', max_turns: 3},
	});
	assert.equal(overridden.ok, true, 'an explicit model and turn cap are accepted');
	const overriddenSpec = JSON.parse(registryRuntime.spawnCalls.at(-1).options.env.SUBAGENT_SPEC);
	assert.equal(overriddenSpec.model, 'call-model', 'the call model wins over the definition');
	assert.equal(overriddenSpec.maxTurns, 3, 'the call turn cap wins over the definition');

	for (const name of ['no-name', 'no-description', 'bad-first', 'bad-yaml', 'tab-indent']) {
		const skipped = await registrySpawn.run({input: {description: name, prompt: 'x', subagent_type: name}});
		assert.equal(skipped.ok, false, `${name} is skipped, never spawned`);
		assert.match(skipped.error, /unknown subagent_type/, `${name} never reaches the registry`);
	}

	const agentsCommand = registry.commandDefs.find((def) => def.name === 'agents');
	assert.ok(agentsCommand !== undefined, '/agents must be registered');
	const listing = agentsCommand.handler({args: ''}).message;
	assert.match(listing, /valid definition\(s\)/, '/agents summarises validity');
	assert.match(listing, /nested {2}project {2}valid/, 'a project definition is listed');
	assert.match(listing, /reviewer {2}project {2}valid/, 'a nested definition is listed');
	assert.match(listing, /user-only {2}user {2}valid/, 'a user definition is listed');
	assert.match(listing, /explore {2}project {2}valid/, 'the project override is listed');
	assert.match(listing, /duplicate name "dup"/, 'a same-directory duplicate is reported');
	assert.match(listing, /invalid: name: required/, 'a missing name is reported');
	assert.match(listing, /invalid: description: required/, 'a missing description is reported');
	assert.match(listing, /invalid: frontmatter must start with --- on the first line/, 'a bad first line is reported');
	assert.match(listing, /invalid: name: must not contain ":"/, 'a colon name is reported');
	assert.match(listing, /invalid: name: must be at most 256 characters/, 'an overlong name is reported');
	assert.match(listing, /invalid: name: must not start with "-"/, 'a leading-dash name is reported');
	assert.match(listing, /invalid: unclosed flow sequence/, 'bad YAML is reported');
	assert.match(listing, /invalid: tab indentation/, 'a tab-indented frontmatter is reported');
	assert.match(listing, /read-only/, '/agents marks read-only definitions');
	assert.match(listing, /one-shot/, '/agents marks one-shot definitions');

	const filtered = agentsCommand.handler({args: 'nested'}).message;
	assert.match(filtered, /nested/, '/agents <name> narrows the listing');
	assert.ok(!filtered.includes('user-only'), 'the filter hides other definitions');

	const agentLog = readFileSync(join(registrySpool, 'agents.log'), 'utf8');
	assert.match(agentLog, /no-name\.md: name: required/, 'the missing name is debug-logged');
	assert.match(agentLog, /no-description\.md: description: required/, 'the missing description is debug-logged');
	assert.match(agentLog, /bad-first\.md: frontmatter must start with --- on the first line/, 'the bad first line is debug-logged');
	assert.match(agentLog, /bad-colon\.md: name: must not contain ":"/, 'the colon name is debug-logged');
	assert.match(agentLog, /too-long\.md: name: must be at most 256 characters/, 'the overlong name is debug-logged');
	assert.match(agentLog, /dash-name\.md: name: must not start with "-"/, 'the leading-dash name is debug-logged');
	assert.match(agentLog, /bad-yaml\.md: unclosed flow sequence/, 'bad YAML is debug-logged');
	assert.match(agentLog, /tab-indent\.md: tab indentation/, 'the tab indentation is debug-logged');

	const exports = await import('../better-sub-agents.ts');
	assert.equal(typeof exports.agentDefinitionFromSource, 'function', 'the parser is exported for a focused check');
	const parsedFull = exports.agentDefinitionFromSource(
		readFileSync(join(projectAgents, 'full.md'), 'utf8'),
		'full.md',
		'project',
	);
	assert.equal(parsedFull.ok, true, 'the full definition parses');
	if (parsedFull.ok) {
		assert.deepEqual(parsedFull.definition.tools, ['Read', 'Grep'], 'a comma string parses as a tool list');
		assert.deepEqual(parsedFull.definition.disallowedTools, ['Bash'], 'a block list parses as disallowed tools');
		assert.deepEqual(parsedFull.definition.skills, ['one', 'two'], 'an inline list parses as skills');
		assert.equal(parsedFull.definition.isolation, 'worktree', 'isolation parses');
		assert.equal(parsedFull.definition.background, true, 'background parses as a boolean');
		assert.equal(parsedFull.definition.color, 'blue', 'color parses');
		assert.equal(parsedFull.definition.omitClaudeMd, true, 'omitClaudeMd parses as a boolean');
	}
	assert.equal(exports.parseFrontmatter('name: x\n---\n').ok, false, 'a missing opening --- is rejected');
	assert.equal(exports.parseFrontmatter('---\nname: [x\ndescription: y\n---\n').ok, false, 'an unclosed flow sequence is rejected');
	ok('definitions: walk-up, closest-wins, recursive, precedence, validation, /agents, and agents.log');

	// 34. the WorkerSpec / buildArgs seam and one shared envelope -------------
	const seam = await import('../better-sub-agents.ts');
	assert.equal(typeof seam.buildSpec, 'function', 'buildSpec is the one spec constructor');
	assert.equal(typeof seam.buildArgs, 'function', 'buildArgs is the one argv builder');
	assert.equal(typeof seam.signalEnvelope, 'function', 'the envelope is shared with the worker');

	const commandSpec = seam.buildSpec(
		{
			id: 'c',
			label: 'c',
			kind: 'command',
			command: 'printf hi',
			cwd: '/tmp',
			logPath: '/tmp/c.log',
			inboxDir: '/tmp/inbox',
			jobsDir: '/tmp/jobs',
			startedAt: 1,
			tailMax: 100,
			bin: '/bin/sh',
		},
		'/mod.ts',
	);
	assert.deepEqual(commandSpec.args, ['-c', 'printf hi'], 'a shell job keeps the /bin/sh argv');
	const agentSeamSpec = seam.buildSpec(
		{
			id: 'a',
			label: 'a',
			kind: 'agent',
			prompt: 'x',
			cwd: '/tmp',
			logPath: '/tmp/a.log',
			inboxDir: '/tmp/inbox',
			jobsDir: '/tmp/jobs',
			startedAt: 1,
			tailMax: 100,
			bin: 'cmd',
		},
		'/mod.ts',
	);
	assert.deepEqual(agentSeamSpec.args, ['--mod', '/mod.ts', 'acp'], 'an agent job runs cmd --mod <file> acp');

	const envelope = seam.signalEnvelope(
		{id: 'e', label: 'envelope', kind: 'agent', owner: 'o', startedAt: 10, logPath: '/l', sessionId: 's'},
		{state: 'done', exitCode: 0, finishedAt: 20, durationMs: 10, body: 'ok'},
	);
	assert.deepEqual(
		envelope,
		{
			id: 'e',
			owner: 'o',
			label: 'envelope',
			kind: 'agent',
			state: 'done',
			exitCode: 0,
			startedAt: 10,
			finishedAt: 20,
			durationMs: 10,
			logPath: '/l',
			sessionId: 's',
			title: 'envelope done',
			body: 'ok',
		},
		'the envelope has one field list',
	);

	const workerText = workerSource();
	assert.match(workerText, /const signalEnvelope = function signalEnvelope/, 'the worker embeds the shared envelope');
	assert.match(workerText, /session\/new/, 'the ACP session is created');
	assert.match(workerText, /session\/prompt/, 'the ACP prompt runs the turn');
	assert.match(workerText, /session\/set_mode/, 'the permission mode is applied over ACP');
	assert.match(workerText, /session\/set_config_option/, 'model and effort are applied over ACP');
	assert.ok(!workerText.includes('childArgs'), 'the cmd -p argv builder is gone');
	assert.ok(!workerText.includes("args.push('-p'"), 'no -p prompt argv remains');
	ok('WorkerSpec, buildSpec, buildArgs, and one shared signal envelope; cmd -p argv is gone');

	// 35. the ACP worker end to end, against a fake ACP server ----------------
	// A real child process speaks line-delimited JSON-RPC: initialize, session/new,
	// set_mode, set_config_option, session/prompt. It proves the adapter order, the
	// session id capture, and that streamed text reaches the job log.
	const acpDir = mkdtempSync(join(tmpdir(), 'subagent-acp-'));
	for (const dir of ['logs', 'inbox', 'jobs']) mkdirSync(join(acpDir, dir), {recursive: true});
	const fakeCmd = join(acpDir, 'fake-cmd.mjs');
	writeFileSync(
		fakeCmd,
		`#!/usr/bin/env node
let buffer = '';
let next = 1;
const send = (message) => process.stdout.write(JSON.stringify({jsonrpc: '2.0', ...message}) + '\\n');
const chunk = (text) => send({
  method: 'session/update',
  params: {sessionId: 'acp-1', update: {sessionUpdate: 'agent_message_chunk', content: {type: 'text', text}}}
});
chunk('ARGS ' + JSON.stringify(process.argv.slice(2)) + '\\n');
process.stdin.on('data', (data) => {
  buffer += data.toString();
  let index;
  while ((index = buffer.indexOf('\\n')) >= 0) {
    const line = buffer.slice(0, index);
    buffer = buffer.slice(index + 1);
    if (!line.trim()) continue;
    const message = JSON.parse(line);
    if (message.method === 'initialize') {
      send({id: message.id, result: {protocolVersion: 1}});
    } else if (message.method === 'session/new') {
      chunk('SESSION_NEW ' + JSON.stringify(message.params) + '\\n');
      send({id: message.id, result: {sessionId: 'acp-1', modes: {currentModeId: 'default', availableModes: []}, configOptions: []}});
    } else if (message.method === 'session/set_mode') {
      chunk('SET_MODE ' + message.params.modeId + '\\n');
      send({id: message.id, result: {}});
    } else if (message.method === 'session/set_config_option') {
      chunk('SET_CONFIG ' + message.params.configId + '=' + message.params.value + '\\n');
      send({id: message.id, result: {}});
    } else if (message.method === 'session/resume') {
      chunk('RESUME ' + message.params.sessionId + '\\n');
      send({id: message.id, result: {modes: {currentModeId: 'default', availableModes: []}, configOptions: []}});
    } else if (message.method === 'session/prompt') {
      chunk('PROMPT ' + JSON.stringify(message.params.prompt?.[0]?.text ?? '') + '\\n');
      chunk('worker says hello');
      send({id: message.id, result: {stopReason: 'end_turn'}});
      setTimeout(() => process.exit(0), 20);
    }
  }
});
`,
	);
	chmodSync(fakeCmd, 0o755);

	const runAcpJob = async (id, markerPath, resumeSession) => {
		const spec = seam.buildSpec(
			{
				id,
				label: id,
				kind: 'agent',
				owner: 'owner-test',
				cwd: acpDir,
				logPath: join(acpDir, 'logs', `${id}.log`),
				inboxDir: join(acpDir, 'inbox'),
				jobsDir: join(acpDir, 'jobs'),
				startedAt: Date.now(),
				tailMax: 8000,
				bin: fakeCmd,
				prompt: 'do the thing',
				agentType: 'general-purpose',
				systemPrompt: 'You are the test worker.',
				model: 'test-model',
				effort: 'low',
				permissionMode: 'acceptEdits',
				promptMarkerPath: markerPath,
				...(resumeSession === undefined ? {} : {resumeSession}),
			},
			'/mod.ts',
		);
		writeFileSync(join(acpDir, 'jobs', `${id}.json`), JSON.stringify({id, state: 'queued'}));
		const child = spawn(process.execPath, ['-e', workerText], {
			cwd: acpDir,
			env: {...process.env, SUBAGENT_SPEC: JSON.stringify(spec), BETTER_SUBAGENTS_WORKER: '1'},
		});
		const {promise, resolve} = Promise.withResolvers();
		child.on('exit', resolve);
		await promise;
		return {
			spec,
			log: readFileSync(join(acpDir, 'logs', `${id}.log`), 'utf8'),
			record: JSON.parse(readFileSync(join(acpDir, 'jobs', `${id}.json`), 'utf8')),
			signals: readdirSync(join(acpDir, 'inbox'))
				.filter((name) => name.includes(id))
				.map((name) => JSON.parse(readFileSync(join(acpDir, 'inbox', name), 'utf8'))),
		};
	};

	// No marker: the ACP child did not load the mod, so the body must travel in the
	// prompt rather than disappear.
	const acp = await runAcpJob('acp-job', join(acpDir, 'logs', 'acp-job.mod'));
	assert.match(acp.log, /ARGS \["--mod","\/mod\.ts","acp"\]/, 'the worker starts cmd --mod <file> acp');
	assert.match(acp.log, /SESSION_NEW .*"cwd"/, 'session/new carries the cwd');
	assert.match(acp.log, /SESSION_NEW .*"mcpServers":\[\]/, 'session/new carries mcpServers');
	assert.match(acp.log, /SET_MODE auto-accept/, 'acceptEdits maps to the ACP auto-accept mode');
	assert.match(acp.log, /SET_CONFIG model=test-model/, 'the definition model is applied over ACP');
	assert.match(acp.log, /SET_CONFIG effort=low/, 'the definition effort is applied over ACP');
	assert.match(acp.log, /PROMPT "AGENT DEFINITION/, 'the body is carried when the prompt hook is absent');
	assert.match(acp.log, /You are the test worker/, 'the exact body reaches the worker');
	assert.match(acp.log, /TASK:/, 'the task still follows the definition');
	assert.match(acp.log, /mod not loaded in the ACP child/, 'the fallback is visible in the log');
	assert.match(acp.log, /worker says hello/, 'streamed text reaches the job log');
	assert.equal(acp.record.acpSessionId, 'acp-1', 'the session id is recorded for a later resume');
	assert.equal(acp.record.state, 'done', 'the prompt result settles the job');
	assert.equal(acp.signals.length, 1, 'one signal for the finished ACP job');
	assert.equal(acp.signals[0].state, 'done', 'the signal is a success');
	assert.match(acp.signals[0].body, /exit 0/, 'the signal carries the result');

	// Marker present: the ACP child loaded the mod, so appendSystemPrompt is live and
	// the body must not be duplicated into the prompt.
	const markerPath = join(acpDir, 'logs', 'acp-hooked.mod');
	writeFileSync(markerPath, '1');
	const hooked = await runAcpJob('acp-hooked', markerPath);
	assert.match(hooked.log, /PROMPT "do the thing"/, 'with the hook live the prompt is the task only');
	assert.ok(!hooked.log.includes('AGENT DEFINITION'), 'the body is not duplicated');

	// session/resume returns modes/config, not a sessionId. The worker must keep the
	// id it resumed, or a follow-up run would silently start a fresh session.
	const acpResumed = await runAcpJob('acp-resumed', markerPath, 'acp-old');
	assert.match(acpResumed.log, /RESUME acp-old/, 'the worker resumes the recorded session');
	assert.ok(!acpResumed.log.includes('SESSION_NEW'), 'resume does not fall back to a new session');
	assert.equal(acpResumed.record.acpSessionId, 'acp-old', 'the resumed id stays on the record');
	assert.equal(acpResumed.signals[0].state, 'done', 'the resumed turn finishes');
	ok('ACP worker: initialize, session/new, set_mode, set_config_option, session/prompt, log and signal');

	// 36. worktree isolation: creation, cleanup, and the command guard ----------
	// The lifecycle is parent-owned and every git call goes through the runtime seam,
	// so a fake git makes each branch drivable without a repository.
	const gitSeam = (answers) => {
		const calls = [];
		const git = (args, cwd) => {
			calls.push({args, cwd});
			const key = args.join(' ');
			for (const [pattern, answer] of Object.entries(answers)) {
				if (key.includes(pattern)) return typeof answer === 'function' ? answer(args, cwd) : answer;
			}
			return {code: 0, stdout: '', stderr: ''};
		};
		return {git, calls};
	};
	const okGit = {code: 0, stdout: '', stderr: ''};
	const repoGit = gitSeam({
		'rev-parse --show-toplevel': {code: 0, stdout: '/repo\n', stderr: ''},
		'rev-parse --path-format=absolute --git-common-dir': {code: 0, stdout: '/repo/.git\n', stderr: ''},
		'symbolic-ref --short refs/remotes/origin/HEAD': {code: 0, stdout: 'origin/main\n', stderr: ''},
		'rev-parse --verify': okGit,
	});

	assert.deepEqual(mainCheckoutRoots(repoGit, '/repo/sub'), ['/repo'], 'a normal checkout has one root');
	assert.equal(defaultBranch(repoGit, '/repo'), 'origin/main', 'the default branch comes from origin/HEAD');
	assert.deepEqual(
		repoGit.calls.filter((call) => call.args[0] === 'rev-parse').map((call) => call.args.join(' ')),
		['rev-parse --show-toplevel', 'rev-parse --path-format=absolute --git-common-dir', 'rev-parse --verify --quiet origin/main'],
		'root discovery and branch verification are real git calls through the seam',
	);

	const worktreePath = '/spool/worktrees/job-36';
	const mkdirs = [];
	const created = createJobWorktree(
		{git: repoGit.git, fs: {mkdir: (dir) => mkdirs.push(dir)}},
		{cwd: '/repo/sub', path: worktreePath},
	);
	assert.equal(created.ok, true, 'a git repository yields a worktree');
	assert.equal(created.path, worktreePath);
	assert.equal(created.base, 'origin/main', 'branched from the default branch, not HEAD');
	assert.deepEqual(created.mainCheckouts, ['/repo']);
	assert.deepEqual(mkdirs, ['/spool/worktrees'], 'the worktree parent is created');
	const addCall = repoGit.calls.find((call) => call.args[0] === 'worktree' && call.args[1] === 'add');
	assert.deepEqual(
		addCall.args,
		['worktree', 'add', '--detach', worktreePath, 'origin/main'],
		'git worktree add --detach <path> <default>',
	);
	assert.equal(addCall.cwd, '/repo', 'the add runs in the repository root');

	const failedGit = gitSeam({
		'rev-parse --show-toplevel': {code: 0, stdout: '/repo\n', stderr: ''},
		'symbolic-ref': {code: 0, stdout: 'origin/main\n', stderr: ''},
		'worktree add': {code: 128, stdout: '', stderr: 'fatal: already exists'},
	});
	const createFailed = createJobWorktree({git: failedGit.git, fs: {mkdir() {}}}, {cwd: '/repo', path: worktreePath});
	assert.equal(createFailed.ok, false, 'a failing git worktree add fails the job');
	assert.match(createFailed.error, /already exists/, 'the git error is reported');

	const noBranch = createJobWorktree(
		{
			git: gitSeam({
				'rev-parse --show-toplevel': {code: 0, stdout: '/repo\n', stderr: ''},
				'rev-parse --path-format': {code: 0, stdout: '/repo/.git\n', stderr: ''},
				'symbolic-ref': {code: 1, stdout: '', stderr: ''},
				'rev-parse --verify': {code: 1, stdout: '', stderr: ''},
			}).git,
			fs: {mkdir() {}},
		},
		{cwd: '/repo', path: worktreePath},
	);
	assert.equal(noBranch.ok, false);
	assert.match(noBranch.error, /default branch/, 'no resolvable default branch is a clear error');

	const fallbackBranch = defaultBranch(gitSeam({'rev-parse --verify': okGit}), '/repo');
	assert.equal(fallbackBranch, 'main', 'main is the fallback when origin/HEAD is absent');

	// A finished job: unchanged removes the worktree, changed keeps and reports it.
	const cleanGit = gitSeam({'status --porcelain': {code: 0, stdout: '', stderr: ''}, 'worktree remove': okGit});
	const removed = finalizeWorktree(cleanGit, {worktree: worktreePath, worktreeRepo: '/repo'});
	assert.deepEqual(removed, {kept: false, path: worktreePath}, 'an unchanged worktree is removed');
	assert.deepEqual(
		cleanGit.calls.find((call) => call.args[0] === 'worktree' && call.args[1] === 'remove').args,
		['worktree', 'remove', '--force', worktreePath],
		'removal goes through git worktree remove',
	);
	assert.equal(
		cleanGit.calls.find((call) => call.args[0] === 'worktree' && call.args[1] === 'remove').cwd,
		'/repo',
		'removal runs in the repository root, not the worktree being removed',
	);

	const dirtyGit = gitSeam({'status --porcelain': {code: 0, stdout: '?? notes.txt\n', stderr: ''}});
	const kept = finalizeWorktree(dirtyGit, {worktree: worktreePath, worktreeRepo: '/repo'});
	assert.deepEqual(kept, {kept: true, path: worktreePath}, 'a changed worktree is kept');
	assert.ok(!dirtyGit.calls.some((call) => call.args[1] === 'remove'), 'a changed worktree is never removed');

	const statusFailed = finalizeWorktree(
		gitSeam({'status --porcelain': {code: 128, stdout: '', stderr: 'not a worktree'}}),
		{worktree: worktreePath, worktreeRepo: '/repo'},
	);
	assert.equal(statusFailed.kept, true, 'a git failure keeps the worktree rather than deleting it');
	assert.match(statusFailed.error, /status/);

	assert.equal(
		finalizeWorktree(dirtyGit, {worktree: worktreePath, worktreeFinalized: true}),
		undefined,
		'finalizing twice is a no-op',
	);
	ok('worktree isolation: create from the default branch; remove unchanged; keep and report changed');

	// 37. the command guard -----------------------------------------------------
	const roots = {worktree: '/spool/worktrees/job-36', mainCheckouts: ['/repo', '/repo-main']};
	assert.equal(commandEscapesWorktree('ls -la && cat README.md', roots), undefined, 'a local command is allowed');
	assert.match(commandEscapesWorktree('cat /repo/README.md', roots), /main checkout/, 'an absolute main-checkout path is blocked');
	assert.match(commandEscapesWorktree('cd .. && touch x', roots), /leaves the worktree/, 'cd above the worktree is blocked');
	assert.match(commandEscapesWorktree('echo hi > /repo/out.txt', roots), /main checkout/, 'a redirect into the main checkout is blocked');
	assert.match(commandEscapesWorktree('echo hi > ../out.txt', roots), /redirect/, 'a redirect above the worktree is blocked');
	assert.match(commandEscapesWorktree('git -C /repo status', roots), /main checkout/, 'git -C into the main checkout is blocked');
	assert.match(commandEscapesWorktree('GIT_DIR=/repo/.git git status', roots), /git directory/, 'a redirected git dir is blocked');
	assert.match(commandEscapesWorktree('git status --prefix=/repo/', roots), /--prefix/, 'a git prefix is blocked');

	assert.equal(guardToolCall('Bash', {command: 'ls -la'}, roots), undefined, 'Bash in the worktree passes');
	const bashMain = guardToolCall('Bash', {command: 'ls', cwd: '/repo'}, roots);
	assert.equal(bashMain.block, true, 'Bash whose cwd resolves to the main checkout fails');
	assert.match(bashMain.reason, /main checkout \(\/repo\)/, 'the error names the main checkout');
	assert.match(bashMain.reason, /isolated in its worktree/, 'the error names the isolation');
	assert.equal(
		guardToolCall('Bash', {command: 'ls', cwd: '/spool/worktrees/job-36/sub'}, roots),
		undefined,
		'a subdirectory of the worktree is allowed',
	);
	assert.equal(
		guardToolCall('PowerShell', {command: 'cd /repo; ls'}, roots),
		undefined,
		'PowerShell gets only the working-directory check',
	);
	assert.equal(guardToolCall('PowerShell', {command: 'ls', cwd: '/repo-main'}, roots).block, true);
	assert.equal(guardToolCall('Monitor', {command: 'cd /repo'}, roots).block, true, 'Monitor gets both checks');
	assert.equal(guardToolCall('Read', {file_path: '/repo/README.md'}, roots), undefined, 'non-shell tools are not guarded');
	ok('isolation guard: main checkout blocked by cwd, escapes rejected by command shape');

	// 38. where the guard comes from -------------------------------------------
	const linkedGit = gitSeam({
		'rev-parse --show-toplevel': {code: 0, stdout: '/repo-wt\n', stderr: ''},
		'rev-parse --path-format=absolute --git-common-dir': {code: 0, stdout: '/repo/.git\n', stderr: ''},
	});
	const linked = isolationRoots(linkedGit, '/repo-wt');
	assert.deepEqual(linked, {worktree: '/repo-wt', mainCheckouts: ['/repo-wt', '/repo']}, 'a linked-worktree session is isolated');
	assert.equal(isolationRoots(repoGit, '/repo'), undefined, 'a normal checkout session is not isolated');
	assert.deepEqual(
		isolationRoots(repoGit, '/spool/worktrees/job-36', {worktree: '/spool/worktrees/job-36', mainCheckouts: ['/repo']}),
		{worktree: '/spool/worktrees/job-36', mainCheckouts: ['/repo']},
		'a worker spec carries the worktree and the forbidden roots',
	);
	ok('isolation roots: linked worktrees and worktree specs are recognised');

	// 39. the guard is registered in the worker and in a linked-worktree session -
	const workerDir = mkdtempSync(join(tmpdir(), 'subagent-wt-worker-'));
	const workerRuntime = makeRuntime({
		now: () => fixedNow,
		env: {
			...process.env,
			SUBAGENT_SPEC: JSON.stringify({
				id: 'wt-worker',
				cwd: '/spool/worktrees/job-36',
				worktree: '/spool/worktrees/job-36',
				mainCheckouts: ['/repo'],
			}),
			BETTER_SUBAGENTS_WORKER: '1',
		},
	}).runtime;
	const workerApi = makeApi({spoolDir: workerDir, session: makeSession(), runtime: workerRuntime});
	mod(workerApi.api, workerRuntime);
	assert.equal(typeof workerApi.registeredHooks.beforeToolCall, 'function', 'the worker registers a tool guard');
	const workerBlock = workerApi.registeredHooks.beforeToolCall({
		toolName: 'Bash',
		input: {command: 'ls', cwd: '/repo'},
	});
	assert.equal(workerBlock.block, true);
	assert.match(workerBlock.additionalContext, /main checkout/);
	assert.equal(
		workerApi.registeredHooks.beforeToolCall({toolName: 'Bash', input: {command: 'ls'}}),
		undefined,
		'the worker default cwd is the worktree',
	);

	const normalDir = mkdtempSync(join(tmpdir(), 'subagent-wt-normal-'));
	const normalRuntime = makeRuntime({now: () => fixedNow, cwd: '/repo', git: repoGit.git}).runtime;
	const normalApi = makeApi({spoolDir: normalDir, session: makeSession(), runtime: normalRuntime});
	mod(normalApi.api, normalRuntime);
	assert.equal(normalApi.registeredHooks.beforeToolCall, undefined, 'a normal main-checkout session registers no guard');

	const linkedDir = mkdtempSync(join(tmpdir(), 'subagent-wt-linked-'));
	const linkedRuntime = makeRuntime({now: () => fixedNow, cwd: '/repo-wt', git: linkedGit.git}).runtime;
	const linkedApi = makeApi({spoolDir: linkedDir, session: makeSession(), runtime: linkedRuntime});
	mod(linkedApi.api, linkedRuntime);
	assert.equal(typeof linkedApi.registeredHooks.beforeToolCall, 'function', 'a linked-worktree session guards its own tools');
	assert.equal(
		linkedApi.registeredHooks.beforeToolCall({toolName: 'Bash', input: {command: 'ls', cwd: '/repo'}}).block,
		true,
		'a linked-worktree session cannot enter the main checkout',
	);
	ok('guard wiring: worker, linked-worktree session, and no guard in a normal checkout');

	// 40. fire() creates the worktree and delivery finalizes it -----------------
	const fireDir = mkdtempSync(join(tmpdir(), 'subagent-wt-fire-'));
	const project = mkdtempSync(join(tmpdir(), 'subagent-wt-project-'));
	writeAgent(join(project, '.commandcode', 'agents'), 'iso.md', 'iso', 'isolated worker', 'You are isolated.', {
		isolation: 'worktree',
	});
	const fireGit = gitSeam({
		'rev-parse --show-toplevel': {code: 0, stdout: `${project}\n`, stderr: ''},
		'rev-parse --path-format=absolute --git-common-dir': {code: 0, stdout: `${project}/.git\n`, stderr: ''},
		'symbolic-ref --short refs/remotes/origin/HEAD': {code: 0, stdout: 'origin/main\n', stderr: ''},
		'rev-parse --verify': okGit,
		'worktree add': okGit,
		'status --porcelain': {code: 0, stdout: '?? changed.txt\n', stderr: ''},
	});
	const fireRuntime = makeRuntime({now: () => fixedNow, cwd: project, git: fireGit.git, execPath: '/usr/bin/node'});
	const fireApi = makeApi({spoolDir: fireDir, session: makeSession(), runtime: fireRuntime.runtime});
	mod(fireApi.api, fireRuntime.runtime);
	const spawnedIso = await toolOf(fireApi, 'subagent_spawn').run({
		input: {description: 'isolated', prompt: 'do it', subagent_type: 'iso'},
	});
	assert.equal(spawnedIso.ok, true, JSON.stringify(spawnedIso));
	const isoId = spawnedIso.content[0].text.match(/spawned (\S+)/)[1];
	const isoRecord = JSON.parse(readFileSync(join(fireDir, 'jobs', `${isoId}.json`), 'utf8'));
	const expectedWorktree = join(fireDir, 'worktrees', isoId);
	assert.equal(isoRecord.worktree, expectedWorktree, 'the record carries the worktree path');
	assert.equal(isoRecord.cwd, expectedWorktree, 'the worker cwd is the worktree');
	assert.deepEqual(isoRecord.mainCheckouts, [project], 'the record carries the forbidden roots');
	const workerSpec = JSON.parse(
		fireRuntime.spawnCalls.find((call) => call.args[0] === '-e').options.env.SUBAGENT_SPEC,
	);
	assert.equal(workerSpec.cwd, expectedWorktree, 'the worker spec runs in the worktree');
	assert.equal(workerSpec.worktree, expectedWorktree);
	assert.equal(
		fireGit.calls.find((call) => call.args[0] === 'worktree' && call.args[1] === 'add').args[4],
		'origin/main',
		'the job branched from the default branch',
	);

	// A terminal signal finalizes through the same path the session drain uses.
	const doneRecord = JSON.parse(readFileSync(join(fireDir, 'jobs', `${isoId}.json`), 'utf8'));
	doneRecord.state = 'done';
	writeFileSync(join(fireDir, 'jobs', `${isoId}.json`), JSON.stringify(doneRecord, null, 2));
	writeFileSync(
		join(fireDir, 'inbox', `${fixedNow}-${isoId}-test.json`),
		JSON.stringify({id: isoId, owner: doneRecord.owner, label: doneRecord.label, kind: 'agent', state: 'done', body: 'exit 0'}),
	);
	fireApi.listeners.get('run_start')[0]({sessionId: 'wt'});
	const delivered = fireApi.userMessages.at(-1)?.text ?? '';
	assert.match(delivered, /worktree kept at/, 'a changed worktree path is reported with the result');
	assert.match(delivered, new RegExp(expectedWorktree.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')));
	const finalized = JSON.parse(readFileSync(join(fireDir, 'jobs', `${isoId}.json`), 'utf8'));
	assert.equal(finalized.worktreeKept, true, 'the record says the worktree was kept');
	assert.equal(finalized.worktreeFinalized, true, 'finalization is recorded once');
	ok('worktree isolation: fire creates it, the result removes or keeps and reports it');

	process.stdout.write('\nPASS\n');
} catch (error) {
	fail(error);
}
