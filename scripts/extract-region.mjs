#!/usr/bin/env node
// Copy one or more line ranges out of a source file into a new module, and
// remove those ranges from the source, in one command.
//
//   node scripts/extract-region.mjs <source> <out> <range> [<range>...]
//
// A range is <start>-<end>, 1-based and inclusive. The ranges are removed from
// the source and concatenated, in source order, into the new module. The out
// file must not already exist, so a mistyped range cannot clobber work.
//
// After extracting, run `npx tsc --noEmit`: every "Cannot find name 'x'" in
// the new module is a name the module still needs (an import or a context
// field). That is the whole free-identifier question, answered by the type
// checker instead of by re-reading the region.
//
// The script refuses to write a range that does not leave both files
// syntactically complete. A range that stops one line before a function's
// closing brace is the common mistake; it used to be written out and then
// surfaced later as a stray `}` in the source. Parsing happens before any
// write, so a bad range changes nothing.

import {readFileSync, writeFileSync, existsSync} from 'node:fs';
import {resolve} from 'node:path';
import ts from 'typescript';

const [sourceArg, outArg, ...rangeArgs] = process.argv.slice(2);
if (!sourceArg || !outArg || rangeArgs.length === 0) {
	console.error('usage: node scripts/extract-region.mjs <source> <out> <start-end> [...]');
	process.exit(2);
}

const sourcePath = resolve(sourceArg);
const outPath = resolve(outArg);

if (existsSync(outPath)) {
	console.error(`refusing to overwrite ${outPath}`);
	process.exit(2);
}

const lines = readFileSync(sourcePath, 'utf8').split('\n');
const ranges = [];
for (const arg of rangeArgs) {
	const match = /^(\d+)-(\d+)$/.exec(arg);
	if (!match) {
		console.error(`bad range: ${arg} (expected <start>-<end>)`);
		process.exit(2);
	}
	const start = Number(match[1]);
	const end = Number(match[2]);
	if (start < 1 || end < start) {
		console.error(`bad range: ${arg}`);
		process.exit(2);
	}
	if (end > lines.length) {
		console.error(`range ${arg} is past the end of ${sourceArg} (${lines.length} lines)`);
		process.exit(2);
	}
	ranges.push({start, end});
}

ranges.sort((a, b) => a.start - b.start);
for (let i = 1; i < ranges.length; i += 1) {
	if (ranges[i].start <= ranges[i - 1].end) {
		console.error(`overlapping ranges: ${ranges[i - 1].start}-${ranges[i - 1].end} and ${ranges[i].start}-${ranges[i].end}`);
		process.exit(2);
	}
}

const bodies = ranges.map(({start, end}) => lines.slice(start - 1, end));
const header = [
	`// Extracted from ${sourceArg}: ${ranges.map(r => `${r.start}-${r.end}`).join(', ')}.`,
	'// The identifiers this module still needs are the ones `npx tsc --noEmit`',
	'// reports as "Cannot find name"; add each as an import or a context field.',
	'',
	'',
];
const newModule = header.concat(bodies.flat()).join('\n');

// Remove from the last range first so the earlier line numbers stay valid.
const kept = [...lines];
for (const {start, end} of [...ranges].reverse()) {
	kept.splice(start - 1, end - start + 1);
}
const remainder = kept.join('\n');

// Parse both results before touching disk. Missing imports are not parse
// errors, so a correct range always passes here; a range that cuts through a
// declaration does not.
const parseErrors = (name, text) =>
	ts.createSourceFile(name, text, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS).parseDiagnostics;
const problems = [
	...parseErrors(outArg, newModule).map(d => ({where: outArg, diagnostic: d})),
	...parseErrors(sourceArg, remainder).map(d => ({where: sourceArg, diagnostic: d})),
];
if (problems.length > 0) {
	for (const {where, diagnostic} of problems) {
		const {line, character} = diagnostic.file.getLineAndCharacterOfPosition(diagnostic.start ?? 0);
		const message = ts.flattenDiagnosticMessageText(diagnostic.messageText, '\n');
		console.error(`${where}:${line + 1}:${character + 1}: ${message}`);
	}
	console.error('');
	console.error('refusing to write: the range does not leave both files syntactically complete.');
	console.error('a range that stops one line before a closing brace is the usual cause; extend it and retry.');
	process.exit(2);
}

writeFileSync(outPath, newModule);
writeFileSync(sourcePath, remainder);

console.log(`wrote ${outArg} (${bodies.reduce((n, b) => n + b.length, 0)} lines)`);
console.log(`removed ${ranges.map(r => `${r.start}-${r.end}`).join(', ')} from ${sourceArg}`);
console.log('next: npx tsc --noEmit');
