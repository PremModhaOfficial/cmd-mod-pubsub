# Environment

As of `PremModhaOfficial/cmd-mod-pubsub@e1ed3b8` on 2026-10-20:

- `better-sub-agents.ts` is 1,957 lines. It has no runtime dependencies. `node test/smoke.mjs` is the CI suite; `node test/e2e.mjs` needs credentials and network.
- The main worktree has untracked `.agents/`, `.claude/`, `.commandcode/`, and `skills-lock.json`. They are not part of the mod and must not be committed.
- GitHub labels present: `ready-for-agent`, `bug`, `enhancement`, `wontfix`. State labels missing: `needs-triage`, `needs-info`, `ready-for-human`. The sub-issues carry no category label. Run `/setup-matt-pocock-skills` to install the state mapping.

## 2026-10-20 — after the source split

Correction to the line count above: the 8,448-line file was the pre-split
artifact at `14e3743`. At `e7bba0c` the root `better-sub-agents.ts` is 6,246
lines, generated from `src/` by `npm run build` (esbuild via
`scripts/build.mjs`). The root file is output; edit `src/` and commit the
regenerated artifact.

- `npm run gate` is `npm run check` (build, then `scripts/check-stale.mjs`),
  `npm run typecheck` (`tsc --noEmit`), `npm run check:imports`
  (`scripts/check-imports.mjs`: no non-entry `./index.ts` import, no runtime
  import cycle, `types.ts` type-only), `node test/bundle.mjs` (bundle
  contract), and `npm test` (`node test/smoke.mjs`). CI runs it on push and
  pull request and additionally `node --check test/e2e.mjs`.
- `node test/e2e.mjs` still needs a signed-in `cmd` and network, so it stays a
  local run.
- Start from `AGENTS.md` (commands and the generated-file rule), `CODING_STANDARDS.md`
  (review rules), and `docs/split-map.md` (module ownership and seams). Behavior
  stays in `SPEC.md`; vocabulary stays in `GLOSSARY.md`.
