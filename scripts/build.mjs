#!/usr/bin/env node
// Bundles the modular source back into the single committed mod artifact.
import {build} from 'esbuild';

try {
	await build({
		entryPoints: ['src/index.ts'],
		bundle: true,
		platform: 'node',
		format: 'esm',
		target: 'node22',
		minify: false,
		keepNames: true,
		legalComments: 'none',
		outfile: 'better-sub-agents.ts',
		external: ['@commandcode/harness'],
	});
} catch (error) {
	console.error('build failed: better-sub-agents.ts was not written');
	console.error(error instanceof Error ? error.message : error);
	process.exitCode = 1;
}
