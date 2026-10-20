#!/usr/bin/env node
// Fails when the committed bundle does not match the bundle in the worktree.
// Run this after `npm run build`: a clean build must reproduce HEAD byte for byte.
import {execFileSync} from 'node:child_process';
import {readFileSync} from 'node:fs';

const BUNDLE = 'better-sub-agents.ts';

function stale(reason) {
	console.error(`STALE BUNDLE: ${BUNDLE} ${reason}`);
	console.error(`The committed ${BUNDLE} must match a fresh build of src/.`);
	console.error('Run `npm run build` and commit the regenerated bundle.');
	process.exit(1);
}

function gitFailure(error) {
	if (error && typeof error === 'object') {
		const stderr = error.stderr?.toString().trim();
		if (stderr) return stderr;
		if (typeof error.status === 'number') return `git exited with status ${error.status}`;
		if (error.code) return String(error.code);
	}
	return String(error);
}

let head;
try {
	head = execFileSync('git', ['show', `HEAD:${BUNDLE}`], {
		maxBuffer: 16 * 1024 * 1024,
	});
} catch (error) {
	stale(`is missing from HEAD (${gitFailure(error)}).`);
}

let worktree;
try {
	worktree = readFileSync(BUNDLE);
} catch (error) {
	stale(`is missing from the worktree (${error?.code ?? error}).`);
}

if (!head.equals(worktree)) {
	stale(
		`has changed since HEAD: HEAD is ${head.length} bytes, the worktree is ${worktree.length} bytes.`,
	);
}

console.log(`PASS: ${BUNDLE} matches HEAD (${worktree.length} bytes).`);
