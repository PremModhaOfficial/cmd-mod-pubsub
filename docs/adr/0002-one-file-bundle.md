# ADR 0001: One committed single-file mod bundle

Status: accepted
Date: 2026-10-20
Owner: Prem Modha
Related: `SPEC.md`, `scripts/build.mjs`, `.github/workflows/ci.yml`

## Context

A Command Code mod is loaded by path: `cmd --mod <file> acp` reads exactly one file. The mod also derives its own path from `import.meta.url` (`MOD_PATH`) and passes it back to the child process, and the worker re-execs itself as a string: `runtime.spawn(runtime.execPath, ['-e', WORKER_SOURCE], …)`. `test/smoke.mjs`, `test/e2e.mjs`, and `test/harness.mjs` all import `../better-sub-agents.ts` directly, and `test/harness.mjs` also reads that file as text to slice out `WORKER_SOURCE`.

Keeping all 8,000+ lines in that one root file makes review, ownership, and navigation expensive. Splitting into ordinary modules, however, would break every consumer above: the host would load `src/index.ts`, `MOD_PATH` would point at the wrong file, and the suites would need a loader.

## Decision

- Author the mod in `src/`, with `src/index.ts` as the entry point.
- Bundle it with esbuild (`scripts/build.mjs`) to the repo root as `better-sub-agents.ts`: ESM, `platform: node`, `minify: false`, `keepNames: true`, `@commandcode/harness` external (it is `import type` only). Unminified output preserves the `import.meta.url` reference, the `const WORKER_SOURCE = \`` literal, and the textual markers the harness slices on.
- Commit the bundle. The root artifact is the only loadable, published file; `package.json` `files` ships it and nothing from `src/`.
- Make CI prove the artifact is current: `npm run gate` runs `npm run check` (`build` then `git diff --exit-code -- better-sub-agents.ts`), `node test/bundle.mjs`, and the smoke suite. A source edit whose bundle was not committed fails the diff.

## Consequences

- One command still loads everything, and existing paths keep working: `node --mod better-sub-agents.ts`, `MOD_PATH`, `../better-sub-agents.ts` imports in tests, and the `node -e` worker string.
- Editing requires running `npm run build` and committing both `src/` and the regenerated root file. Forgetting the rebuild fails CI, not production.
- The root bundle is generated code and should not be hand-edited; review happens in `src/`, where a diff is meaningful.
- A small devDependency (esbuild) and a lockfile are now required for `npm ci`. The repo previously had no build step, so CI gains one install and one build; the end-to-end suite still runs only locally because it needs a signed-in `cmd` and network.
- Duplicate code exists in the tree during review (source plus bundle). That is the price of a loader that accepts exactly one file.
