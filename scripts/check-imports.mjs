#!/usr/bin/env node
// Enforces the module rules the bundle split relies on:
//   - only the entry module may import `./index.ts`
//   - runtime imports must not form a cycle
//   - `./types.ts` is type-only
//   - every file is a module, not a script
//
// The last rule matters more than it looks. A TypeScript file with no top-level
// `import` or `export` is a SCRIPT: its declarations land in the global scope,
// and every other file can then use them without importing. That silently
// hides errors - a name the entry uses but never imports resolves against the
// leaked global, so the typecheck passes on code that would not run.
import {readdirSync, readFileSync} from 'node:fs';
import {join, posix} from 'node:path';

const DIR = 'src';
const ENTRY = 'index.ts';
const files = readdirSync(DIR).filter((f) => f.endsWith('.ts') && !f.endsWith('.d.ts'));
const errors = [];
const graph = new Map(files.map((f) => [f, []]));

const statement = /(?:^|\n)\s*(?:import|export)\s+(type\s+)?([\s\S]*?)from\s+['"](\.\.?\/[^'"]+)['"]/g;
const sideEffect = /(?:^|\n)\s*import\s+['"](\.\.?\/[^'"]+)['"]/g;

function isTypeOnly(keyword, body) {
	if (keyword) return true;
	const braces = body.match(/\{([\s\S]*)\}/);
	if (!braces) return false;
	const specifiers = braces[1].split(',').map((s) => s.trim()).filter(Boolean);
	return specifiers.length > 0 && specifiers.every((s) => /^type\s+/.test(s));
}

for (const file of files) {
	const source = readFileSync(join(DIR, file), 'utf8');
	const edges = [];

	const visit = (specifier, typeOnly) => {
		const target = posix.normalize(specifier).replace(/^\.\//, '');
		if (target === ENTRY && file !== ENTRY) errors.push(`${file}: imports the entry module ${specifier}`);
		if (target === 'types.ts' && !typeOnly) errors.push(`${file}: ${specifier} must be imported with \`import type\``);
		if (!typeOnly && files.includes(target) && !edges.includes(target)) edges.push(target);
	};

	for (const match of source.matchAll(statement)) visit(match[3], isTypeOnly(match[1], match[2]));
	for (const match of source.matchAll(sideEffect)) visit(match[1], false);
	graph.set(file, edges);

	// A file with no top-level import or export is a script, not a module: its
	// declarations become globals and leak into every other file.
	const hasModuleSyntax = /^\s*(?:import|export)\b/m.test(source);
	if (!hasModuleSyntax) {
		errors.push(`${file}: has no top-level import or export, so it is a script and its declarations leak as globals`);
	}
}

const state = new Map();
const stack = [];
function walk(file) {
	if (state.get(file) === 'done') return;
	if (state.get(file) === 'open') {
		errors.push(`runtime import cycle: ${[...stack.slice(stack.indexOf(file)), file].join(' -> ')}`);
		return;
	}
	state.set(file, 'open');
	stack.push(file);
	for (const next of graph.get(file)) walk(next);
	stack.pop();
	state.set(file, 'done');
}
for (const file of files) walk(file);

if (errors.length > 0) {
	for (const error of errors) console.error(`FAIL ${error}`);
	process.exit(1);
}
console.log(`PASS ${files.length} modules: no entry imports, runtime cycles, or value imports of types.ts`);
