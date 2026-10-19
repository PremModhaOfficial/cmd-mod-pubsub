# CHECKLIST: cmd-mod-better-sub-agents

One spec. One checklist. The phases below are tickets, not separate specs.
ADRs are added only for a decision that is hard to reverse, surprising, and a real trade-off.
A glossary is added only if a new term is settled.

Verification command: `node test/smoke.mjs`

## Phase 0: Baseline

- [x] Record current `cmd-mod-pubsub` test results.
  - Baseline on `main` @ `977e74b` (`node test/smoke.mjs`): the 0.1.0 release script has no suite.
  - Baseline on `better-sub-agents` @ `2a3d58d` before this pass: **21 ok / PASS** (Node 26.12.0).
- [x] Create branch `better-sub-agents`.
- [x] Copy `pubsub.ts` to `better-sub-agents.ts` (`git mv`, so the history follows the file).
- [ ] Rename the repository to `cmd-mod-better-sub-agents`. (Deferred to Phase 10 by instruction: the
  GitHub URL in `package.json` stays `cmd-mod-pubsub` until the repository itself is renamed.)
- [x] Update `package.json` name, version, description, repository, homepage, bugs, and files.
- [x] Update README title and install path.
- [x] Update CI paths. (`ci.yml` runs `node test/smoke.mjs`, which is path-free; the import inside the
  suite moved to `../better-sub-agents.ts`. A grep for `pubsub.ts` finds only the deprecated aliases.)
- Gate: the existing smoke test passes unchanged. — **21 ok / PASS after the rename.**

## Phase 1: Spec and harness

- [ ] Commit `SPEC.md` and `CHECKLIST.md`.
- [ ] Build the fake ModApi harness in `test/`.
- [ ] Add the two-session owner isolation test first.
- [ ] Add a resume and hot reload owner test.
- [ ] Run the suite and confirm the two new tests fail for the right reason.
- Gate: the isolation tests fail before the owner changes.

## Phase 2: Owner isolation

- [ ] Change the owner custom entry type to `subagent/owner`.
- [ ] Mint the token once per session.
- [ ] Persist it with `appendCustomEntry`.
- [ ] Restore it with `getCustomEntries`.
- [ ] Put the owner in every job record.
- [ ] Put the owner in every signal.
- [ ] Drain `mine` signals with `sendUserMessage`.
- [ ] Leave `foreign` signals in `inbox/`.
- [ ] Move untagged legacy signals to `orphaned/`.
- [ ] Keep workers from stealing or quarantining.
- [ ] Add the `onStop` drain for the race window.
- Gate: the two-session test passes. Resume keeps the same token.

## Phase 3: Job model

- [ ] Add the six states: `queued`, `running`, `waiting-input`, `done`, `failed`, `canceled`.
- [ ] Add the allowed transitions and reject the rest.
- [ ] Add a job record per job under `jobs/`.
- [ ] Add the durable `delivered.jsonl` ledger.
- [ ] Add restart reconciliation. A dead `queued` or `running` job becomes `failed` after 30 seconds.
- [ ] Add retention cleanup, default 30 days.
- Gate: state machine, reconciliation, and ledger replay tests pass.

## Phase 4: Worker execution

- [ ] Run a detached worker for `cmd -p` agent jobs.
- [ ] Run a detached worker for `/bin/sh -c` shell jobs.
- [ ] Write stdout and stderr to `logs/<id>.log`.
- [ ] Write one signal with owner, job id, state, exit code, duration, and log tail.
- [ ] Handle spawn error, timeout, and API failure.
- [ ] Set `BETTER_SUBAGENTS_WORKER=1` in the worker.
- Gate: a real `printf` job and a real `cmd -p` job both complete.

## Phase 5: Tools and command

- [ ] Add `subagent_spawn`.
- [ ] Add `subagent_list`.
- [ ] Add `subagent_output`.
- [ ] Add `subagent_cancel`.
- [ ] Add `subagent_resume`.
- [ ] Add `/subagents`.
- [ ] Keep `pubsub_*` as deprecated aliases for one release.
- Gate: every surface has a behavioral test.

## Phase 6: Agent registry

- [ ] Read `.commandcode/agents/*.md`.
- [ ] Read `~/.commandcode/agents/*.md`.
- [ ] Parse frontmatter: `name`, `description`, `tools`, `disallowedTools`, `model`, `permissionMode`, `maxTurns`, `skills`, `isolation`, `background`, `color`, `omitClaudeMd`, `effort`.
- [ ] Register built-ins: `explore`, `plan`, `general-purpose`, `claude`.
- [ ] Mark `explore` and `plan` read-only and one-shot.
- [ ] Skip invalid files with a warning.
- [ ] Resolve duplicate names with project scope first.
- Gate: precedence, invalid frontmatter, and built-in tests pass.

## Phase 7: Limits and isolation

- [ ] Add the concurrency pool. Default 20.
- [ ] Add `BETTER_SUBAGENTS_MAX_CONCURRENT`.
- [ ] Fail the 21st spawn with `Concurrent subagent limit reached`.
- [ ] Add the nesting depth limit. Default 3.
- [ ] Add `BETTER_SUBAGENTS_MAX_DEPTH`.
- [ ] Add `isolation: worktree` with `git worktree`.
- [ ] Remove an unchanged worktree.
- [ ] Keep and report a changed worktree.
- [ ] Add `BETTER_SUBAGENTS_DISABLE_BACKGROUND=1`.
- Gate: the concurrency, nesting, and worktree tests pass.

## Phase 8: Output and permissions

- [ ] Scan worker output for instruction-shaped text.
- [ ] Insert the backslash into imitated system tags.
- [ ] Add the `[harness: subagent output matched instruction-shaped pattern(s): ...]` marker.
- [ ] Map `permissionMode` to the worker CLI flags.
- [ ] Put a worker that needs approval into `waiting-input`.
- [ ] Signal the parent with the question or the approval request.
- [ ] Do not hang the worker.
- Gate: scanning and `waiting-input` tests pass.

## Phase 9: UI and persistence

- [ ] Add the jobs pane.
- [ ] Add state markers for the six states.
- [ ] Add refresh and cancel controls.
- [ ] Add the footer running count.
- [ ] Add `/subagents list`, `jobs`, `cancel`, and `open`.
- [ ] Add transcript open from the pane.
- [ ] Add hot reload recovery.
- Gate: manual smoke in two sessions.

## Phase 10: Release

- [ ] All tests pass.
- [ ] Two-session isolation test passes.
- [ ] No foreign signal is delivered.
- [ ] No untagged signal remains in `inbox/`.
- [ ] No duplicate delivery after restart.
- [ ] README and parity matrix updated.
- [ ] `package.json` version is 2.0.0.
- [ ] Repository is renamed.
- [ ] Tag `v2.0.0` only after every box is checked.
- Gate: release tag after all boxes and all tests pass.
