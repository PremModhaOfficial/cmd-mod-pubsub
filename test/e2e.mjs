// End-to-end: the real OS, the real detached worker, a real ACP session (`cmd acp`).
//
// The smoke test fakes the runtime, so it cannot prove that a worker process
// actually launches, writes `logs/<id>.log`, and drops one signal in `inbox/`.
// This suite does, with no fakes except the ModApi host:
//
//   1. a real `/bin/sh -c` job (`printf`) runs to completion and signals
//   1a. two real sessions over one spool: no cross-delivery
//   2. a real ACP agent job runs to completion and signals
//   3. a job that overruns its timeout is killed and reports a timeout
//   4. a finished agent job is resumed in its own worker session
//   5. a real git worktree is created from the default branch, removed when
//      unchanged, and kept when changed
//
// Requires: `cmd` on PATH (or COMMANDCODE_BIN), a signed-in Command Code, and
// network. Not run in CI for that reason. `node test/e2e.mjs`.

import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';

import mod, {createJobWorktree, finalizeWorktree} from '../better-sub-agents.ts';
import {
	makeApi,
	makeSession,
	ok,
	toolOf,
	sleep,
	waitFor,
	recordOf,
	signalsFor,
	isTerminal,
	isAlive,
	waitForWorkerExit,
	answerText,
} from './helpers.mjs';

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

	// 1a. two real sessions over one spool ------------------------------------
	// The invariant, with two mod instances, two owner tokens, and a real detached
	// worker: a session that does not own a signal may look at it, but never gets it.
	const isoDir = mkdtempSync(join(tmpdir(), 'subagent-e2e-iso-'));
	const isoA = makeApi({spoolDir: isoDir, session: makeSession()});
	const isoB = makeApi({spoolDir: isoDir, session: makeSession()});
	mod(isoA.api, isoA.runtime);
	mod(isoB.api, isoB.runtime);
	const isoSpawn = await toolOf(isoA, 'subagent_spawn').run({
		input: {description: 'owner a', command: 'printf owner-a'},
	});
	assert.equal(isoSpawn.ok, true, `the isolation spawn must succeed: ${JSON.stringify(isoSpawn)}`);
	const isoId = isoSpawn.content[0].text.match(/spawned (\S+)/)[1];
	const isoRecord = await waitFor(() => {
		const record = recordOf(isoDir, isoId);
		return isTerminal(record) ? record : undefined;
	}, 'the two-session job', 30_000);
	assert.equal(isoRecord.state, 'done', `the two-session job: ${JSON.stringify(isoRecord)}`);
	await waitFor(() => signalsFor(isoDir, isoId).length > 0, 'the two-session signal', 10_000);
	isoB.listeners.get('run_start')[0]({sessionId: 'iso-b'});
	assert.equal(isoB.userMessages.length, 0, 'session B never receives a signal owned by A');
	assert.equal(signalsFor(isoDir, isoId).length, 1, 'the foreign drain leaves the signal in inbox/');
	isoA.listeners.get('run_start')[0]({sessionId: 'iso-a'});
	assert.equal(isoA.userMessages.length, 1, 'session A receives its own signal');
	assert.match(isoA.userMessages[0].text, /owner-a/, 'the delivered signal carries the job output');
	assert.equal(signalsFor(isoDir, isoId).length, 0, 'the owner drained it');
	ok('two real sessions over one spool: B never receives A\'s signal, A receives it once');

	// 1b. a real worktree ------------------------------------------------------
	// The smoke suite fakes git. This proves the real lifecycle: a worktree from
	// the default branch (not the parent's HEAD), removed when unchanged, kept when
	// changed.
	const repo = mkdtempSync(join(tmpdir(), 'subagent-e2e-repo-'));
	const realGit = (args, cwd) => {
		const result = spawnSync('git', args, {cwd, encoding: 'utf8'});
		return {code: typeof result.status === 'number' ? result.status : 1, stdout: String(result.stdout ?? ''), stderr: String(result.stderr ?? '')};
	};
	const git = (args, cwd) => {
		const result = realGit(['-c', 'user.email=e2e@example.com', '-c', 'user.name=E2E', ...args], cwd);
		assert.equal(result.code, 0, `git ${args.join(' ')}: ${result.stderr}`);
		return result;
	};
	git(['init', '-b', 'main', repo], tmpdir());
	writeFileSync(join(repo, 'version.txt'), 'main\n');
	git(['add', '-A'], repo);
	git(['commit', '-m', 'base'], repo);
	git(['checkout', '-b', 'parent-head'], repo);
	writeFileSync(join(repo, 'version.txt'), 'parent\n');
	git(['add', '-A'], repo);
	git(['commit', '-m', 'parent'], repo);

	const seam = {git: realGit, fs: {mkdir: (target) => mkdirSync(target, {recursive: true})}};
	const fresh = createJobWorktree(seam, {cwd: repo, path: join(dir, 'worktrees', 'unchanged')});
	assert.equal(fresh.ok, true, JSON.stringify(fresh));
	assert.equal(fresh.base, 'main', 'a repository without origin/HEAD resolves main');
	assert.equal(
		readFileSync(join(fresh.path, 'version.txt'), 'utf8'),
		'main\n',
		'the worktree is the default branch, not the parent HEAD',
	);
	const removedReal = finalizeWorktree(seam, {worktree: fresh.path, worktreeRepo: repo});
	assert.deepEqual(removedReal, {kept: false, path: fresh.path}, 'a real unchanged worktree is removed');
	assert.equal(existsSync(fresh.path), false, 'the unchanged worktree directory is gone');

	const changedReal = createJobWorktree(seam, {cwd: repo, path: join(dir, 'worktrees', 'changed')});
	assert.equal(changedReal.ok, true, JSON.stringify(changedReal));
	writeFileSync(join(changedReal.path, 'changed.txt'), 'kept\n');
	const keptReal = finalizeWorktree(seam, {worktree: changedReal.path, worktreeRepo: repo});
	assert.equal(keptReal.kept, true, 'a real changed worktree is kept');
	assert.equal(existsSync(join(changedReal.path, 'changed.txt')), true, 'the change survives');
	rmSync(keptReal.path, {recursive: true, force: true});
	rmSync(repo, {recursive: true, force: true});
	ok(`real worktree: branched from ${fresh.base}, removed unchanged, kept changed at ${keptReal.path}`);

	// 2. a real ACP agent job ------------------------------------------------
	// The first run is given two codes that only it knows. A later resume asks
	// for one back, so the answer proves the session's history survived the
	// resume rather than a fresh conversation being started.
	//
	// Two codes, so each resume can ask for a DIFFERENT one. The second resume
	// then cannot pass by echoing what the first resume said: the code it asks
	// for was never in any prompt but the first run's.
	//
	// The codes are LABELLED, and each resume asks for one BY ITS LABEL. An
	// unlabelled pair is ambiguous: "the secret code" names neither of them, so
	// a correctly resumed agent can answer with either one, or with both. The
	// history survived in that case, and the test would still fail - a failure
	// that says nothing about the thing under test. With labels, one code has
	// exactly one name, so the answer is unambiguous.
	const secretCode = 'zephyr-4417';
	const secondCode = 'quartz-8823';
	const secretLabel = 'ALPHA';
	const secondLabel = 'BRAVO';
	const agentSpawned = await toolOf(api, 'subagent_spawn').run({
		input: {
			description: 'acp job',
			prompt:
				`Remember these two codes exactly, each with its label: ` +
				`${secretLabel} is ${secretCode}, and ${secondLabel} is ${secondCode}. ` +
				'Then reply with exactly the word pong and nothing else.',
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
	assert.equal(agentRecord.agentId, agentId, 'the real job carries its agent id');
	assert.equal(agentSignal.agentId, agentId, 'the real signal carries the agent id');
	const agentTranscript = join(dir, 'transcripts', `${agentId}.jsonl`);
	assert.ok(existsSync(agentTranscript), `the transcript exists at ${agentTranscript}`);
	assert.match(readFileSync(agentTranscript, 'utf8'), /pong/i, 'the transcript holds the real answer');
	ok(`ACP job completed: jobs/${agentId}.json, logs/${agentId}.log, transcripts/${agentId}.jsonl, inbox/ signal`);

	// 2b. a real live message ------------------------------------------------
	// The agent is running, idle, or already finished by now; each path is a legal
	// live send. The answer must land in the same session (the original log or the
	// log of the resumed job), which proves the control queue or the resume path.
	const liveSend = await toolOf(api, 'subagent_send').run({
		input: {to: 'ponger', message: 'Reply with exactly the word banana and nothing else.'},
	});
	assert.equal(liveSend.ok, true, `live send must start: ${JSON.stringify(liveSend)}`);
	const pongerIds = () =>
		readdirSync(join(dir, 'jobs'))
			.filter((file) => file.endsWith('.json'))
			.map((file) => file.slice(0, -'.json'.length))
			.filter((id) => {
				try {
					return JSON.parse(readFileSync(join(dir, 'jobs', `${id}.json`), 'utf8')).name === 'ponger';
				} catch {
					return false;
				}
			});
	const liveText = await waitFor(() => {
		for (const id of pongerIds()) {
			const text = answerText(dir, id);
			if (/banana/i.test(text)) return text;
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
	}, 'the API failure', 120_000);
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
	//
	// The resume asks for a fact the FIRST run was told and this prompt does not
	// carry. A fresh conversation would not know it, so the answer can only come
	// from the resumed session's own history. Asking for a word that is in the
	// prompt would pass even if the resume silently started a new session.
	//
	// The question names the code by its LABEL, so exactly one code answers it.
	// The answer must be that one code and not the other: an answer carrying
	// both codes would be a correct recall of the wrong thing, and the second
	// resume would then have nothing left that only the first run knew.
	//
	// The wait is on the worker's PROCESS, not on a fixed sleep: the record says
	// `done` while the live worker still holds the session, and a resume of a job
	// whose worker is alive is refused in favour of a live message.
	await waitForWorkerExit(dir, agentId, 'the first agent worker');
	const resumed = await toolOf(api, 'subagent_resume').run({
		input: {
			id: agentId,
			prompt: `What is the code labelled ${secretLabel}? Reply with only that code.`,
		},
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
	assert.equal(resumedRecord.agentId, resumedId, 'the resumed job has its own agent id');
	assert.ok(existsSync(join(dir, 'transcripts', `${resumedId}.jsonl`)), 'the resumed job persists its transcript');
	assert.match(
		resumedLog,
		new RegExp(secretCode, 'i'),
		`the resumed session remembered the code it was given earlier: ${resumedLog.slice(-800)}`,
	);
	// The answer is the code that was ASKED for. Answering with both is a
	// correct recall of the wrong thing: the second resume asks for the other
	// code, and it can only answer from the first run's history if this one
	// left it there.
	const resumedAnswer = answerText(dir, resumedId);
	assert.doesNotMatch(
		resumedAnswer,
		new RegExp(secondCode, 'i'),
		`the resume answered with the code it asked for, not both: ${resumedAnswer}`,
	);
	ok(`resumed job completed: jobs/${resumedId}.json, logs/${resumedId}.log`);

	// 4b. a SECOND resume ----------------------------------------------------
	// The first resume proved the context survived one hop. This one resumes the
	// RESUMED job, so the chain is two hops deep, and it asks for the OTHER
	// code: the second code was in the FIRST run's prompt and in no prompt
	// since. A resume that started a fresh conversation - or one that answered
	// from the previous resume's text - could not produce it.
	//
	// It asks for the other code BY ITS LABEL, so the answer is one code and
	// not both: the first resume already proved it can name one code, and this
	// one proves the OTHER one survived the hop as well.
	//
	// The session NAME changes with every hop: the first run is
	// `subagent-<id>`, the first resume is `subagent-<resumedId>`, and this one
	// is `subagent-<twiceId>`. The name is derived from the job id, so a second
	// resume that reused the first one's name would mean it resumed the same
	// job twice rather than continuing the chain.
	await waitForWorkerExit(dir, resumedId, 'the first resumed worker');
	const twice = await toolOf(api, 'subagent_resume').run({
		input: {
			id: resumedId,
			prompt: `What is the code labelled ${secondLabel}? Reply with only that code.`,
		},
	});
	assert.equal(twice.ok, true, `the second resume must start: ${JSON.stringify(twice)}`);
	const twiceId = twice.content[0].text.match(/as (\S+)/)[1];
	assert.notEqual(twiceId, resumedId, 'the second resume is a new job with a new id');
	const twiceRecord = await waitFor(() => {
		const record = recordOf(dir, twiceId);
		return isTerminal(record) ? record : undefined;
	}, 'the second resumed job', 240_000);
	const twiceLog = readFileSync(join(dir, 'logs', `${twiceId}.log`), 'utf8');
	assert.equal(twiceRecord.state, 'done', `second resumed state ${twiceRecord.state}: ${twiceLog.slice(-800)}`);
	assert.equal(twiceRecord.resumedFrom, resumedId, 'the second resume continues the FIRST resume');
	assert.equal(twiceRecord.agentId, twiceId, 'the second resumed job has its own agent id');
	assert.ok(existsSync(join(dir, 'transcripts', `${twiceId}.jsonl`)), 'the second resumed job persists its transcript');
	assert.match(
		twiceLog,
		new RegExp(secondCode, 'i'),
		`the second resumed session remembered the other code: ${twiceLog.slice(-800)}`,
	);
	// The answer is the code that was ASKED for, and only that one.
	const twiceAnswer = answerText(dir, twiceId);
	assert.doesNotMatch(
		twiceAnswer,
		new RegExp(secretCode, 'i'),
		`the second resume answered with the code it asked for, not both: ${twiceAnswer}`,
	);
	// The session name follows the job id, so every hop names its own session.
	assert.equal(
		twiceRecord.sessionName,
		`subagent-${twiceId}`,
		`the second resume names its own session: ${String(twiceRecord.sessionName)}`,
	);
	assert.equal(
		resumedRecord.sessionName,
		`subagent-${resumedId}`,
		`the first resume named its own session: ${String(resumedRecord.sessionName)}`,
	);
	assert.equal(
		agentRecord.sessionName,
		`subagent-${agentId}`,
		`the first run named its own session: ${String(agentRecord.sessionName)}`,
	);
	// The ACP session is the same one through the chain: the history the second
	// resume answered from is the first run's session, resumed twice.
	assert.equal(
		twiceRecord.acpSessionId,
		agentRecord.acpSessionId,
		'the whole chain continued one ACP session',
	);
	ok(`second resume completed: jobs/${twiceId}.json, logs/${twiceId}.log`);

	process.stdout.write(`\nPASS (artifacts under ${dir})\n`);
} catch (error) {
	process.stdout.write(`\nFAIL: ${String(error?.stack ?? error?.message ?? error)}\n`);
	process.exitCode = 1;
}
