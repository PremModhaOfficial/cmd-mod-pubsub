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
import {existsSync, mkdtempSync, readdirSync, readFileSync, renameSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {Script} from 'node:vm';

import mod, {jobStates, ledgerFactory} from '../better-sub-agents.ts';
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

	await toolOf(res, 'subagent_spawn').run({input: {description: 'agent', prompt: 'first', agent_type: 'explore'}});
	const agentSpec = JSON.parse(resRuntime.spawnCalls[1].options.env.SUBAGENT_SPEC);
	assert.equal(agentSpec.sessionName, `subagent-${agentSpec.id}`, 'an agent job pins a deterministic session name');
	assert.deepEqual(
		agentSpec.sessionName === undefined ? [] : [agentSpec.sessionName],
		[`subagent-${agentSpec.id}`],
		'the worker spec carries the session name',
	);
	const running = await resume.run({input: {id: agentSpec.id, prompt: 'continue'}});
	assert.equal(running.ok, false, 'a running job cannot be resumed');
	assert.match(running.error, /still queued|still running/);

	// The job is finished; resume continues its worker session.
	const agentRecordPath = join(resDir, 'jobs', `${agentSpec.id}.json`);
	writeFileSync(
		agentRecordPath,
		JSON.stringify({...JSON.parse(readFileSync(agentRecordPath, 'utf8')), state: 'done', exitCode: 0, finishedAt: fixedNow, durationMs: 10}),
	);
	const resumed = await resume.run({input: {id: agentSpec.id, prompt: 'continue'}});
	assert.equal(resumed.ok, true, `a finished agent job resumes: ${JSON.stringify(resumed)}`);
	const resumedSpec = JSON.parse(resRuntime.spawnCalls[2].options.env.SUBAGENT_SPEC);
	assert.equal(resumedSpec.resumeSession, `subagent-${agentSpec.id}`, 'the resume targets the worker session by name');
	assert.equal(resumedSpec.prompt, 'continue', 'the new instruction is the resumed prompt');
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

	process.stdout.write('\nPASS\n');
} catch (error) {
	fail(error);
}
