// End-to-end: the real OS, the real detached worker, a real `cmd -p`.
//
// The smoke test fakes the runtime, so it cannot prove that a worker process
// actually launches, writes `logs/<id>.log`, and drops one signal in `inbox/`.
// This suite does, with no fakes except the ModApi host:
//
//   1. a real `/bin/sh -c` job (`printf`) runs to completion and signals
//   2. a real `cmd -p` agent job runs to completion and signals
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

	// 2. a real cmd -p agent job --------------------------------------------
	const agentSpawned = await toolOf(api, 'subagent_spawn').run({
		input: {
			description: 'cmd -p job',
			prompt: 'Reply with exactly the word pong and nothing else.',
			agent_type: 'general-purpose',
		},
	});
	assert.equal(agentSpawned.ok, true, `agent spawn must succeed: ${JSON.stringify(agentSpawned)}`);
	const agentId = agentSpawned.content[0].text.match(/spawned (\S+)/)[1];
	const agentRecord = await waitFor(() => {
		const record = recordOf(dir, agentId);
		return isTerminal(record) ? record : undefined;
	}, 'the cmd -p job to finish', 240_000);
	const agentLog = readFileSync(join(dir, 'logs', `${agentId}.log`), 'utf8');
	assert.equal(agentRecord.state, 'done', `cmd -p state ${agentRecord.state}: ${agentLog.slice(-800)}`);
	assert.match(agentLog, /pong/i, 'the real agent answered in its log');
	const [agentSignal] = await waitFor(() => {
		const signals = signalsFor(dir, agentId);
		return signals.length > 0 ? signals : undefined;
	}, 'the cmd -p signal', 10_000);
	assert.equal(agentSignal.kind, 'agent');
	assert.equal(agentSignal.state, 'done');
	assert.equal(agentSignal.owner, agentRecord.owner);
	ok(`cmd -p job completed: jobs/${agentId}.json, logs/${agentId}.log, inbox/ signal`);

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

	// 3b. an API/CLI failure is a failure, not a hang -----------------------
	// An unknown model is rejected by the CLI on exit 1. The worker must turn that into
	// one failed signal with the CLI's own message in the tail, not leave the job
	// queued forever and not swallow the reason.
	const apiFail = await toolOf(api, 'subagent_spawn').run({
		input: {description: 'api failure', prompt: 'hi', model: 'definitely-not-a-model-xyz'},
	});
	assert.equal(apiFail.ok, true, 'the spawn itself succeeds; the failure arrives as a signal');
	const failId = apiFail.content[0].text.match(/spawned (\S+)/)[1];
	const failRecord = await waitFor(() => {
		const record = recordOf(dir, failId);
		return isTerminal(record) ? record : undefined;
	}, 'the API failure', 60_000);
	assert.equal(failRecord.state, 'failed', 'an API failure is a failed job');
	assert.equal(failRecord.exitCode, 1, 'the CLI exit code is kept');
	assert.match(readFileSync(join(dir, 'logs', `${failId}.log`), 'utf8'), /unknown model/, 'the CLI error is in the log');
	ok(`API failure reported as one failed signal: jobs/${failId}.json (${failRecord.failureReason})`);

	// 4. a real resume ------------------------------------------------------
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
