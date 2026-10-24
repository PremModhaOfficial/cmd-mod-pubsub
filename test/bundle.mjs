// Bundle contract test for the committed single-file mod artifact.
//
// The source of truth lives in `src/`, but `cmd --mod <file>` loads exactly one
// file, the worker re-execs `node -e <WORKER_SOURCE>`, and the test suites import
// the root path directly. This test pins the properties of the bundle those
// consumers rely on, so a broken or stale build fails before the smoke suite.
//
// What this proves:
//   1. `better-sub-agents.ts` loads as an ES module on the Node version CI uses
//   2. the default export is the factory `(api, runtime?) => void`
//   3. every named export the smoke and e2e suites import is present
//   4. `WORKER_SOURCE` is still the embeddable CJS worker string, with the
//      markers the harness slices and asserts on
//   5. the published `files` list still ships the artifact

import assert from 'node:assert';
import {readFileSync} from 'node:fs';

import mod, * as bundle from '../better-sub-agents.ts';

const REQUIRED_NAMED_EXPORTS = [
	'WORKER_SOURCE',
	'buildSpec',
	'buildArgs',
	'signalEnvelope',
	'filterTools',
	'resolveModel',
	'resolveEffort',
	'createJobWorktree',
	'finalizeWorktree',
];

const REQUIRED_WORKER_MARKERS = [
	'SUBAGENT_SPEC',
	'const signalEnvelope = function signalEnvelope',
	'session/new',
	'timed out after',
	'SIGKILL',
];

// The default export is called as `mod(api)` and as `mod(api, runtime)`, so it
// must be a function that accepts both.
assert.strictEqual(typeof mod, 'function', 'default export must be the mod factory');
assert.ok(mod.length >= 1 && mod.length <= 2, `mod factory arity must be 1 or 2, got ${mod.length}`);

for (const name of REQUIRED_NAMED_EXPORTS) {
	assert.ok(name in bundle, `missing named export: ${name}`);
}

assert.strictEqual(typeof bundle.WORKER_SOURCE, 'string', 'WORKER_SOURCE must be a string');
assert.ok(bundle.WORKER_SOURCE.length > 0, 'WORKER_SOURCE must not be empty');

for (const marker of REQUIRED_WORKER_MARKERS) {
	assert.ok(
		bundle.WORKER_SOURCE.includes(marker),
		`WORKER_SOURCE must contain ${JSON.stringify(marker)}`,
	);
}

// Every exported helper the suites call is a function (WORKER_SOURCE aside).
for (const name of REQUIRED_NAMED_EXPORTS) {
	if (name === 'WORKER_SOURCE') continue;
	assert.strictEqual(typeof bundle[name], 'function', `${name} must be a function`);
}

const pkg = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8'));
assert.ok(
	Array.isArray(pkg.files) && pkg.files.includes('better-sub-agents.ts'),
	'package.json files must include the single-file artifact',
);

console.log('PASS bundle');
