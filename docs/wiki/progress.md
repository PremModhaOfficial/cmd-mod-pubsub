# Progress log

- 2026-10-20: T1 (#4) landed on `t1-definitions` as `ae7ce2b`; merged into `better-sub-agents` at `83431ae`; `node test/smoke.mjs` 35/35. The TDD red run used the new tests against `e1ed3b8`; green on the branch.
- 2026-10-20: the frontier #5, #6, #7, #8, #9, #10, #11, #12, and #14 was dispatched to nine background implementers. Each has a worktree under `/home/prm/project/cmd-mod-better-sub-agents-wt/`.
- 2026-10-20: the chief-of-staff wiki lives at `docs/wiki/`; first commit `2eba6b9`.
- 2026-10-20: T6 (#9) output scanning: `scanOutput` is embedded in the worker like `signalEnvelope`; signals and job records carry `scanned`/`scanMatches`, failed workers return their tail, and `subagent_output` scans the log before the parent reads it. `node test/smoke.mjs` 40/40.
- 2026-10-20: T14 (#17) release reconciliation on `t14-parity`: the parity ledger has a check on every row, all six documented built-ins exist (`statusline-setup` and `claude-code-guide` added), the 15,000-token description limit warns at startup, and README/CHECKLIST/package version are 2.0.0. `node test/smoke.mjs` 88/88; `node test/e2e.mjs` 10/10. Repository rename, tag, and PR merge remain human-gated.
