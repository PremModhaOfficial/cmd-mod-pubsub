// Shared test helpers for the better-sub-agents suites.
//
// Re-exports the fake ModApi / Runtime / session harness from `./harness.mjs`
// (so callers only need one import) and adds the small utilities that both
// `smoke.mjs` and `e2e.mjs` use: polling, record/signal reads, liveness checks.

export * from './harness.mjs';

import {existsSync, readFileSync, readdirSync} from 'node:fs';
import {join} from 'node:path';

export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export async function waitFor(check, label, timeout = 60_000) {
	const deadline = Date.now() + timeout;
	while (Date.now() < deadline) {
		const value = check();
		if (value) return value;
		await sleep(100);
	}
	throw new Error(`timed out after ${timeout}ms waiting for ${label}`);
}

export const recordOf = (dir, id) => {
	try {
		return JSON.parse(readFileSync(join(dir, 'jobs', `${id}.json`), 'utf8'));
	} catch {
		return undefined;
	}
};

export const signalsFor = (dir, id) => {
	try {
		return readdirSync(join(dir, 'inbox'))
			.filter((name) => name.includes(id))
			.map((name) => JSON.parse(readFileSync(join(dir, 'inbox', name), 'utf8')));
	} catch {
		return [];
	}
};

export const isTerminal = (record) =>
	record !== undefined && ['done', 'failed', 'waiting-input', 'canceled'].includes(record.state);

export const isAlive = (pid) => {
	if (typeof pid !== 'number' || !Number.isInteger(pid) || pid <= 0) return false;
	try {
		process.kill(pid, 0);
		return true;
	} catch (error) {
		return error?.code === 'EPERM';
	}
};

export async function waitForWorkerExit(dir, id, label, timeout = 60_000) {
	const deadline = Date.now() + timeout;
	while (Date.now() < deadline) {
		const record = recordOf(dir, id);
		if (record === undefined) return;
		if (!isAlive(record.workerPid) && !isAlive(record.pid)) return;
		await sleep(100);
	}
	throw new Error(`timed out after ${timeout}ms waiting for ${label} to exit`);
}

export const answerText = (dir, id) => {
	try {
		return readFileSync(join(dir, 'transcripts', `${id}.jsonl`), 'utf8')
			.trim()
			.split('\n')
			.map((line) => {
				try {
					return JSON.parse(line);
				} catch {
					return undefined;
				}
			})
			.filter((entry) => entry && entry.update && entry.update.sessionUpdate === 'agent_message_chunk')
			.map((entry) => String((entry.update.content && entry.update.content.text) || ''))
			.join('');
	} catch {
		return '';
	}
};

export const waitForFile = async (file, timeoutMs = 4000) => {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (existsSync(file)) return true;
		await sleep(20);
	}
	return false;
};
