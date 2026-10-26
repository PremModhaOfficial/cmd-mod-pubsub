// Test for scripts/extract-region.mjs: the extraction tool must never write a
// range that leaves either file syntactically incomplete.
//
// Two cases, both real: a range that includes the function's closing brace
// succeeds and writes both files; a range that stops one line short is
// refused and changes nothing on disk. The second case is the bug this guard
// exists for: it used to leave a stray `}` behind and only fail later in tsc.

import assert from 'node:assert/strict';
import {mkdtempSync, readFileSync, rmSync, writeFileSync, existsSync} from 'node:fs';
import {spawnSync} from 'node:child_process';
import {tmpdir} from 'node:os';
import {join} from 'node:path';

const script = new URL('../scripts/extract-region.mjs', import.meta.url).pathname;
const root = mkdtempSync(join(tmpdir(), 'extract-region-'));
const source = join(root, 'source.ts');
const out = join(root, 'out.ts');

const body = [
	'export const keep = 1;',
	'',
	'export function moved(): number {',
	'\treturn keep + 1;',
	'}',
	'',
	'export const after = 2;',
	'',
].join('\n');

const run = (range) => spawnSync(process.execPath, [script, source, out, range], {encoding: 'utf8'});

try {
	// Case 1: the range includes the closing brace. Both files are written.
	writeFileSync(source, body);
	const first = run('3-5');
	assert.equal(first.status, 0, `a complete range must succeed: ${first.stderr}`);
	assert.ok(existsSync(out), 'the new module must be written');
	assert.match(readFileSync(out, 'utf8'), /export function moved/, 'the moved declaration must be in the new module');
	const rest = readFileSync(source, 'utf8');
	assert.doesNotMatch(rest, /function moved/, 'the moved declaration must leave the source');
	assert.match(rest, /export const keep = 1;/, 'the surrounding declarations must stay');
	assert.match(rest, /export const after = 2;/, 'the surrounding declarations must stay');
	console.log('ok - a complete range is written');

	// Case 2: the range stops before the closing brace. Nothing is written.
	rmSync(out, {force: true});
	writeFileSync(source, body);
	const second = run('3-4');
	assert.equal(second.status, 2, 'an incomplete range must be refused');
	assert.match(second.stderr, /refusing to write/, 'the refusal must say why');
	assert.ok(!existsSync(out), 'a refused range must not create the new module');
	assert.equal(readFileSync(source, 'utf8'), body, 'a refused range must leave the source unchanged');
	console.log('ok - an incomplete range is refused and changes nothing');

	console.log('PASS');
} finally {
	rmSync(root, {recursive: true, force: true});
}
