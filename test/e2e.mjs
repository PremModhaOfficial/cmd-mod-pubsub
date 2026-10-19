// End-to-end: the real OS, the real detached worker, a real ACP session (`cmd acp`).
//
// The smoke test fakes the runtime, so it cannot prove that a worker process
// actually launches, writes `logs/<id>.log`, and drops one signal in `inbox/`.
// This suite does, with no fakes except the ModApi host:
//
//   1. a real `/bin/sh -c` job (`printf`) runs to completion and signals
//   2. a real ACP agent job runs to completion and signals
//   3. a job that overruns its timeout is killed and reports a timeout
//   4. a finished agent job is resumed in its own worker session
//
// Requires: `cmd` on PATH (or COMMANDCODE_BIN), a signed-in Command Code, and
// network. Not run in CI for that reason. `node test/e2e.mjs`.

import assert from 'node:assert/strict';
import {existsSync, mkdtempSync, readdirSync, readFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';

import mod from '../better-sub-agents.ts';
import {makeApi, makeSession, ok, toolOf} from './harness.mjs';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function waitFor(check, label, timeout = 60_000) {
	const deadline = Date.now() + timeout;
	while (Date.now() < deadline) {
		const value = check();
		if (value) return value;
		await sleep(100);
	}
	throw new Error(`timed out after ${timeout}ms waiting for ${label}`);
}

const recordOf = (dir, id) => {
	try {
		return JSON.parse(readFileSync(join(dir, 'jobs', `${id}.json`), 'utf8'));
	} catch {
		return undefined;
	}
};

const signalsFor = (dir, id) => {
	try {
		return readdirSync(join(dir, 'inbox'))
			.filter((name) => name.includes(id))
			.map((name) => JSON.parse(readFileSync(join(dir, 'inbox', name), 'utf8')));
	} catch {
		return [];
	}
};

const isTerminal = (record) =>
	record !== undefined && ['done', 'failed', 'waiting-input', 'canceled'].includes(record.state);

try {
	// Keep the idle live session short so a later explicit resume is exercised on a
	// worker that has already exited, not one still waiting for messages.
	process.env.BETTER_SUBAGENTS_LIVE_IDLE_MS ??= '750';
	const dir = mkdtempSync(join(tmpdir(), 'subagent-e2e-'));
	const api = makeApi({spoolDir: dir, session: makeSession()});
	mod(api.api, api.runtime);
	ok(`real spool at ${dir}`);

	// 1. a real shell job ---------------------------------------------------
	const spawned = await toolOf(api, 'subagent_spawn').run({
		input: {description: 'printf job', command: 'printf hello-e2e'},
	});
	assert.equal(spawned.ok, true, `spawn must succeed: ${JSON.stringify(spawned)}`);
	const shellId = spawned.content[0].text.match(/spawned (\S+)/)[1];
	const shellRecord = await waitFor(() => {
		const record = recordOf(dir, shellId);
		return isTerminal(record) ? record : undefined;
	}, 'the printf job to finish', 30_000);
	assert.equal(shellRecord.state, 'done', `printf job state: ${JSON.stringify(shellRecord)}`);
	assert.equal(shellRecord.exitCode, 0, 'exit code 0');
	const logPath = join(dir, 'logs', `${shellId}.log`);
	assert.ok(existsSync(logPath), `the log exists at ${logPath}`);
	assert.match(readFileSync(logPath, 'utf8'), /hello-e2e/, 'the log holds the real stdout');
	const [shellSignal] = await waitFor(() => {
		const signals = signalsFor(dir, shellId);
		return signals.length > 0 ? signals : undefined;
	}, 'the printf signal', 10_000);
	assert.equal(shellSignal.owner, shellRecord.owner, 'the signal carries the job owner');
	assert.equal(shellSignal.state, 'done');
	assert.equal(shellSignal.exitCode, 0);
	assert.equal(typeof shellSignal.durationMs, 'number', 'the signal carries a duration');
	assert.match(shellSignal.body, /hello-e2e/, 'the signal carries the log tail');
	ok(`printf job completed: jobs/${shellId}.json, logs/${shellId}.log, inbox/ signal`);

	// The drain hands it to the session as one message.
	api.listeners.get('run_start')[0]({sessionId: 'e2e'});
	assert.equal(api.userMessages.length, 1, 'the signal reaches the session');
	assert.match(api.userMessages[0].text, new RegExp(`\\[subagent\\] printf job done`), 'addressed as a signal');
	assert.match(api.userMessages[0].text, /hello-e2e/, 'with the log tail');
	ok('the signal is delivered to the session that fired it');

	// 2. a real ACP agent job ------------------------------------------------
	const agentSpawned = await toolOf(api, 'subagent_spawn').run({
		input: {
			description: 'acp job',
			prompt: 'Reply with exactly the word pong and nothing else.',
			subagent_type: 'general-purpose',
			name: 'ponger',
		},
	});
	assert.equal(agentSpawned.ok, true, `agent spawn must succeed: ${JSON.stringify(agentSpawned)}`);
	const agentId = agentSpawned.content[0].text.match(/spawned (\S+)/)[1];
	const agentRecord = await waitFor(() => {
		const record = recordOf(dir, agentId);
		return isTerminal(record) ? record : undefined;
	}, 'the ACP job to finish', 240_000);
	const agentLog = readFileSync(join(dir, 'logs', `${agentId}.log`), 'utf8');
	assert.equal(agentRecord.state, 'done', `ACP state ${agentRecord.state}: ${agentLog.slice(-800)}`);
	assert.match(agentLog, /pong/i, 'the real agent answered in its log');
	const [agentSignal] = await waitFor(() => {
		const signals = signalsFor(dir, agentId);
		return signals.length > 0 ? signals : undefined;
	}, 'the ACP signal', 10_000);
	assert.equal(agentSignal.kind, 'agent');
	assert.equal(agentSignal.state, 'done');
	assert.equal(agentSignal.owner, agentRecord.owner);
	ok(`ACP job completed: jobs/${agentId}.json, logs/${agentId}.log, inbox/ signal`);

	// 2b. a real live message ------------------------------------------------
	// The agent is running, idle, or already finished by now; each path is a legal
	// live send. The answer must land in the same session (the original log or the
	// log of the resumed job), which proves the control queue or the resume path.
	const liveSend = await toolOf(api, 'subagent_send').run({
		input: {to: 'ponger', message: 'Reply with exactly the word banana and nothing else.'},
	});
	assert.equal(liveSend.ok, true, `live send must start: ${JSON.stringify(liveSend)}`);
	const liveText = await waitFor(() => {
		const own = readFileSync(join(dir, 'logs', `${agentId}.log`), 'utf8');
		if (/banana/i.test(own)) return own;
		for (const name of readdirSync(join(dir, 'jobs')).filter((file) => file.endsWith('.json'))) {
			let job;
			try {
				job = JSON.parse(readFileSync(join(dir, 'jobs', name), 'utf8'));
			} catch {
				continue;
			}
			if (job.name !== 'ponger' || job.id === agentId) continue;
			try {
				const text = readFileSync(join(dir, 'logs', `${job.id}.log`), 'utf8');
				if (/banana/i.test(text)) return text;
			} catch {
				/* the resumed job has not opened its log yet */
			}
		}
		return undefined;
	}, 'the live message to be answered', 180_000);
	assert.match(liveText, /banana/i, 'the live message was answered in the agent session');
	ok('live message: the named agent answered the message in its own session');

	// 3. a real timeout -----------------------------------------------------
	const slow = await toolOf(api, 'subagent_spawn').run({
		input: {description: 'timeout job', command: 'sleep 30', timeout_ms: 400},
	});
	assert.equal(slow.ok, true);
	const slowId = slow.content[0].text.match(/spawned (\S+)/)[1];
	const slowRecord = await waitFor(() => {
		const record = recordOf(dir, slowId);
		return isTerminal(record) ? record : undefined;
	}, 'the timeout job', 15_000);
	assert.equal(slowRecord.state, 'failed', 'a job past its timeout fails');
	assert.match(String(slowRecord.failureReason), /timed out after/, `reason: ${slowRecord.failureReason}`);
	ok(`timeout job killed and reported: jobs/${slowId}.json (${slowRecord.failureReason})`);

	// 3b. a rejected model is reported, not a hang --------------------------
	// ACP applies the model with session/set_config_option. A rejection must be
	// logged and must not leave the job queued forever.
	const apiFail = await toolOf(api, 'subagent_spawn').run({
		input: {description: 'api failure', prompt: 'hi', model: 'definitely-not-a-model-xyz'},
	});
	assert.equal(apiFail.ok, true, 'the spawn itself succeeds; the failure arrives as a signal');
	const failId = apiFail.content[0].text.match(/spawned (\S+)/)[1];
	const failRecord = await waitFor(() => {
		const record = recordOf(dir, failId);
		return isTerminal(record) ? record : undefined;
	}, 'the API failure', 60_000);
	assert.ok(['done', 'failed'].includes(failRecord.state), 'the bad model does not hang the job');
	assert.match(
		readFileSync(join(dir, 'logs', `${failId}.log`), 'utf8'),
		/model not set|unknown model|not in the allowed catalog/i,
		'the model rejection or documented fallback is in the log',
	);
	ok(`bad model reported and the job finished: jobs/${failId}.json (${failRecord.state})`);

	// 4. a real resume ------------------------------------------------------
	// The live worker (original or resumed by the message) owns the session until its
	// idle window passes; wait it out, then resume explicitly.
	await sleep(1500);
	const resumed = await toolOf(api, 'subagent_resume').run({
		input: {id: agentId, prompt: 'Reply with exactly the word ping and nothing else.'},
	});
	assert.equal(resumed.ok, true, `resume must start: ${JSON.stringify(resumed)}`);
	const resumedId = resumed.content[0].text.match(/as (\S+)/)[1];
	const resumedRecord = await waitFor(() => {
		const record = recordOf(dir, resumedId);
		return isTerminal(record) ? record : undefined;
	}, 'the resumed job', 240_000);
	const resumedLog = readFileSync(join(dir, 'logs', `${resumedId}.log`), 'utf8');
	assert.equal(resumedRecord.state, 'done', `resumed state ${resumedRecord.state}: ${resumedLog.slice(-800)}`);
	assert.equal(resumedRecord.resumedFrom, agentId, 'the resume records what it continued');
	assert.match(resumedLog, /ping/i, 'the resumed session answered the new instruction');
	ok(`resumed job completed: jobs/${resumedId}.json, logs/${resumedId}.log`);

	process.stdout.write(`\nPASS (artifacts under ${dir})\n`);
} catch (error) {
	process.stdout.write(`\nFAIL: ${String(error?.stack ?? error?.message ?? error)}\n`);
	process.exitCode = 1;
}
