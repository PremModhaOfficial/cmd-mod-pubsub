# ADR 0003: Source modules under `src/`, still published as one file

Status: accepted
Date: 2026-10-20
Owner: Prem Modha
Related: ADR 0001 (worker and control topology), ADR 0002 (one committed single-file
bundle), `SPEC.md`, `scripts/build.mjs`, `scripts/check-imports.mjs`,
`scripts/check-stale.mjs`, `docs/split-map.md`

## Context

The mod is installed by copying one file. A user runs `cp better-sub-agents.ts
~/.commandcode/mods/`, or `cmd --mod ./better-sub-agents.ts`; the host loads that
path and nothing else. There is no install step for the user, no `node_modules`
for the mod, and no loader that could resolve a second file. ADR 0002 already
decided the ship model: author in `src/`, bundle to the repo root with esbuild,
commit the bundle, ship only the bundle.

ADR 0002 left the *source* shape open, and the source stayed a single
8,448-line `src/index.ts`. That file held one composition root and roughly
thirty-five top-level groups: the runtime adapter, the state table, the envelope,
the job store, the ledger, the spool claim/settle, the definition frontmatter
parser, the tool filter, the worktree helpers, the plan, the spawn, and
`activate` itself. Reviewing it meant reading a diff that mixed unrelated
concerns, and every owner of one area had to hold the whole file in their head.
It was also one file to navigate: no way to answer "where does the permission
bridge live?" without reading top to bottom.

The source split is therefore not a change to the ship model. It is a change to
how the source is written, reviewed, and checked.

## Decision

Author the mod as ES modules under `src/`, with `src/index.ts` as the single
entry point, and keep publishing exactly one file.

- `src/index.ts` is the entry and the only composition root. It owns the two
  bundle-literal symbols — `WORKER_SOURCE` (the detached worker's source as one
  template string) and `MOD_PATH` (the mod's own path, derived from
  `import.meta.url`) — because the bundler has to emit both into the shipped
  file and no leaf may need either at module scope. `MOD_PATH` is defined in
  `src/constants.ts`, but the entry is its only consumer, so it is
  entry-owned; a leaf that needs the mod path takes it as a parameter.
- Every other file under `src/` is a leaf: it takes its OS effects as
  parameters (`fs`, `run`, `now`, `env`, `cwd`) or as an explicit deps object,
  and names no session state.
- Imports flow downward. No module imports `./index.ts`. `src/types.ts` is
  type-only and is imported with `import type`. The graph is a DAG.
- `scripts/build.mjs` bundles `src/index.ts` to the repo root as
  `better-sub-agents.ts` (esbuild, ESM, `platform: node`, `minify: false`,
  `keepNames: true`, `@commandcode/harness` external).
- The bundle is committed, and CI proves it is current.

### Why one file is still published

The one-file install is the requirement that survives every alternative: the
host loads one path, and the documented install is a file copy. Splitting the
*source* costs the user nothing, because the source is never installed. The
source split buys reviewability at review time; the bundle keeps the install
path unchanged.

## Consequences

- **The bundle is committed and is generated code.** `better-sub-agents.ts` in
  the repo root is output. It is never hand-edited; the reviewable diff is the
  `src/` diff. The committed copy is the thing users install, so it must exist
  in the repository rather than being produced at install time.
- **A stale bundle is a build failure, not a review judgement.** `npm run check`
  runs the build and then `scripts/check-stale.mjs`, which compares the
  worktree bundle against `HEAD`'s byte for byte and exits non-zero on any
  difference. Editing `src/` without regenerating the bundle fails the gate
  before anyone reads the change.
- **The typecheck is a separate gate.** `npm run typecheck` (`tsc --noEmit`)
  covers `src/`, which no longer exists as one file, so the boundaries between
  modules are now type-checked rather than merely reviewed. A module that
  reaches for a symbol it does not own now fails the typecheck.
- **The import seams are enforced mechanically.**
  `scripts/check-imports.mjs` fails the gate when a module imports
  `./index.ts`, when the runtime imports form a cycle, or when `types.ts` is
  imported as a value.
- **The tree carries a second copy of the code.** Source plus bundle is
  duplicate code in the repository. That is the price of a loader that accepts
  exactly one file, and the stale-bundle gate is what keeps the copy honest.
- **A module move is a review unit.** One module per commit, with the
  regenerated bundle in the same commit, so the diff shows one move and its
  mechanical consequence rather than a rewrite.
- **`docs/split-map.md` is the ownership map.** It lists every file under
  `src/` with its line count and its role, and it is the reference when
  deciding where a new symbol belongs: the lowest module that needs it.

## Alternatives rejected

### A multi-file mod (publish the modules as they are)

Rejected. The host loads exactly one file, so a multi-file mod would need an
installer, a loader, a bundler at the user's machine, or a path rewrite — and
the `MOD_PATH` derivation from `import.meta.url` would point at the wrong file
once the code lives beside the entry. It also breaks the documented install
(`cp better-sub-agents.ts ~/.commandcode/mods/`) and the one-file requirement
that the whole user-facing design rests on: no install step, no dependencies.
The source split captures the review benefit with none of that cost.

### A build-at-publish-only artifact (bundle produced at release time)

Rejected. The repository would no longer contain the shipped file, so:

- review would happen against a tree that cannot be loaded or tested as
  installed, and the "does the published artifact match the source?" question
  would have no local answer;
- the mod would need a release pipeline and a package publish step, and the
  install path in the README would change from a file copy to an install
  command — which is the change this ADR is explicitly avoiding;
- a drift between source and artifact would surface only at release, where it
  is expensive to detect.

Committing the bundle and gating it with the stale check keeps the artifact
available for the existing install path and turns drift into a local, immediate
failure. ADR 0002 already chose the committed bundle; this ADR extends the
same reasoning to the source tree.

## Verification

- `npm run gate` — build, stale-bundle check, typecheck, import check, bundle
  contract, smoke suite.
- `node test/e2e.mjs` — the real host, a signed-in `cmd`, and the network; it
  stays a local run.
- `docs/split-map.md` — the current module inventory, with the seam rules that
  `scripts/check-imports.mjs` enforces.
