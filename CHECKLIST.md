# CHECKLIST: cmd-mod-better-sub-agents

One spec. One checklist. The phases below are tickets, not separate specs.
ADRs are added only for a decision that is hard to reverse, surprising, and a real trade-off.
A glossary is added only if a new term is settled.

Verification commands: `node test/smoke.mjs` (CI) and `node test/e2e.mjs` (needs credentials/network).

Parity note (2026-10-19): `SPEC.md` was expanded to the full Claude Code parity contract. Phases 0-10 are the original build plan; phases 11-16 close the gaps found in the 2026-10-19 validation. A phase is not done until its SPEC.md acceptance tests pass.

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

- [x] Commit `SPEC.md` and `CHECKLIST.md`. (`58b750f`)
- [x] Build the fake ModApi harness in `test/`. (`test/harness.mjs`: one fake ModApi, one fake
  `Runtime`, one fake session, shared by the smoke and end-to-end suites.)
- [x] Add the two-session owner isolation test first. (smoke checks 5, 12, 13)
- [x] Add a resume and hot reload owner test. (smoke check 18: the persisted token survives a resume and
  is the token the job carries; check 26: the 0.1.0 transcript entry is reused instead of re-minted)
- [ ] Run the suite and confirm the two new tests fail for the right reason. (Done by the pre-owner
  baseline in Stage A; it cannot be re-run now without reverting the owner changes.)
- Gate: the isolation tests fail before the owner changes. (Historical; see above.)

## Phase 2: Owner isolation

- [x] Change the owner custom entry type to `subagent/owner`. (with a read-only fallback to the 0.1.0
  `pubsub/owner` entry, so a session that was live across the rename keeps its identity — smoke 26)
- [x] Mint the token once per session. (smoke 4)
- [x] Persist it with `appendCustomEntry`. (smoke 4, 18)
- [x] Restore it with `getCustomEntries`. (smoke 18, 26)
- [x] Put the owner in every job record. (smoke 22)
- [x] Put the owner in every signal. (smoke 22: cancel signal; smoke 17: every failed signal)
- [x] Drain `mine` signals with `sendUserMessage`. (smoke 5, 13)
- [x] Leave `foreign` signals in `inbox/`. (smoke 12: a foreign signal survives a failed restore)
- [x] Move untagged legacy signals to `orphaned/`. (smoke 6)
- [x] Keep workers from stealing or quarantining. (`BETTER_SUBAGENTS_WORKER=1` → `foreign` for untagged)
- [x] Add the `onStop` drain for the race window. (smoke 25: continues the run once, never loops)
- Gate: the two-session test passes. Resume keeps the same token. (smoke 5, 13, 18, 25, 26)

## Phase 3: Job model

- [x] Add the six states: `queued`, `running`, `waiting-input`, `done`, `failed`, `canceled`. (smoke 15)
- [x] Add the allowed transitions and reject the rest. (smoke 15, 16: a late failure cannot drag a
  terminal job back, and emits no phantom signal)
- [x] Add a job record per job under `jobs/`. (smoke 10, 17, 22)
- [x] Add the durable `delivered.jsonl` ledger. (smoke 20, 21: retention keeps memory and disk in step,
  and a second factory cannot replay a delivered signal)
- [x] Add restart reconciliation. A dead `queued` or `running` job becomes `failed` after 30 seconds.
  (smoke 23: inside the grace is untouched, a live pid is never failed, a dead one fails after 30s and
  emits exactly one signal through the same `failJob` contract)
- [x] Add retention cleanup, default 30 days. (`BETTER_SUBAGENTS_RETENTION_DAYS` overrides; record + log
  are swept together, a running job is never swept — smoke 24)
- Gate: state machine, reconciliation, and ledger replay tests pass. (smoke 15, 16, 20, 21, 23, 24)

## Phase 4: Worker execution

- [x] Run a detached worker for `cmd -p` agent jobs. (`spawn(process.execPath, ['-e', WORKER_SOURCE],
  {detached: true})`; e2e check 4 ran a real `cmd -p` to completion)
- [x] Run a detached worker for `/bin/sh -c` shell jobs. (e2e check 2 ran a real `printf`)
- [x] Write stdout and stderr to `logs/<id>.log`. (both stdio streams share the fd; e2e asserts the log
  holds the real stdout)
- [x] Write one signal with owner, job id, state, exit code, duration, and log tail. (e2e 2, 4; the
  parent-side copy of the contract is smoke 17, 22)
- [x] Handle spawn error, timeout, and API failure. (spawn error: smoke 17 covers a sync throw, an async
  `error` event and a missing pid; timeout: e2e check 5, `timed out after 400ms` after SIGTERM/SIGKILL;
  API failure: e2e check 6, a rejected model exits 1 and lands as one failed signal with the CLI error in
  the tail, and exit 8 is labelled `max turns reached; result is partial`)
- [x] Set `BETTER_SUBAGENTS_WORKER=1` in the worker. (smoke 10)
- Gate: a real `printf` job and a real `cmd -p` job both complete. (`node test/e2e.mjs`: 7 ok / PASS;
  artifacts under the printed spool path)

## Phase 5: Tools and command

- [x] Add `subagent_spawn`. (smoke 9: validation and rejections; 10: the spawn itself)
- [x] Add `subagent_list`. (smoke 8: state, model, elapsed, owner, pending count; smoke 29: the `state`
  and `limit` filters)
- [x] Add `subagent_output`. (smoke 27: tail, `offset` + `max_chars` window, bounded `wait`, unknown id)
- [x] Add `subagent_cancel`. (smoke 9, 10, 22)
- [x] Add `subagent_resume`. (smoke 28: unknown, shell and running jobs refused; a finished agent job
  resumes its own `subagent-<id>` session and records `resumedFrom`; e2e check 7 did a real resume)
- [x] Add `/subagents`. (smoke 8: list / jobs / cancel; smoke 30: open <id>)
- [x] Keep `pubsub_*` as deprecated aliases for one release. (smoke 29: `pubsub_fire`, `pubsub_list`,
  `pubsub_cancel`, `pubsub_send` are registered from the same `run` function and marked deprecated;
  `/pubsub` and the `pubsub_dir` / `pubsub_deliver` flags are aliases too)
- Gate: every surface has a behavioral test. (smoke 8, 9, 10, 22, 27, 28, 29, 30)

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
- [x] Add `isolation: worktree` with `git worktree`.
- [x] Remove an unchanged worktree.
- [x] Keep and report a changed worktree.
- [ ] Add `BETTER_SUBAGENTS_DISABLE_BACKGROUND=1`.
- Gate: the concurrency, nesting, and worktree tests pass.

## Phase 8: Output and permissions

- [x] Scan worker output for instruction-shaped text. (T6: `scanOutput`, embedded in the worker;
  smoke 37)
- [x] Insert the backslash into imitated system tags. (smoke 37, 38: `<system-reminder>`,
  `Human:`, `Assistant:`)
- [x] Add the `[harness: subagent output matched instruction-shaped pattern(s): ...]` marker.
  (smoke 37-39; permission mentions are marked and left as written; failed signals carry the tail)
- [x] Map `permissionMode` to the worker CLI flags. (T5: `permissionModes` table; smoke 53-54)
- [x] Put a worker that needs approval into `waiting-input`. (smoke 55)
- [x] Signal the parent with the question or the approval request. (smoke 55, 58-59)
- [x] Do not hang the worker. (bounded inline wait, then `waiting-input`; smoke 56)
- Gate: scanning and `waiting-input` tests pass.

## Phase 9: UI and persistence

- [x] Add the jobs pane. (T10: task rows, selection, open/stop/dismiss.)
- [x] Add state markers for the six states.
- [x] Add refresh and cancel controls. (T10 adds prev/next/enter/x/esc; Ctrl+B backgrounds a foreground run.)
- [x] Add the footer running count. (T10 adds the 30-second `/tasks` hint.)
- [x] Add `/subagents list`, `jobs`, `cancel`, and `open`.
- [x] Add transcript open from the pane. (T10: pane Enter renders the transcript; `/tasks enter` also sends a follow-up.)
- [ ] Add hot reload recovery.
- Gate: two sessions stay isolated. (smoke 4-5 and 12-13, plus the real two-session e2e step 1a. No interactive TUI pass is needed for the invariant.)

## Phase 10: Release

- [x] All tests pass. (`node test/smoke.mjs` 102/102; `node test/e2e.mjs` 10/10 on a signed-in host.)
- [x] Two-session isolation test passes. (smoke 4-5 and 12-13, and real e2e 1a: two sessions, one shared spool, distinct owner tokens, no cross-delivery.)
- [x] No foreign signal is delivered. (smoke 5, 12, 13.)
- [x] No untagged signal remains in `inbox/`. (smoke 6: it moves to `orphaned/`.)
- [x] No duplicate delivery after restart. (smoke 21: the durable ledger suppresses replay.)
- [x] README and parity matrix updated. (README title/version/install/surfaces/built-ins/description limit; `SPEC.md` parity ledger has a check on every row.)
- [x] `package.json` version is 2.0.0.
- [ ] Repository is renamed. (Human gate: `cmd-mod-pubsub` -> `cmd-mod-better-sub-agents`; package URLs intentionally stay on `cmd-mod-pubsub` until then.)
- [ ] Tag `v2.0.0` only after every box is checked. (Human gate: no tag is created or pushed by this ticket.)
- Gate: release tag after all boxes and all tests pass.

## Phase 11: Parity ledger and naming

- [x] Adopt the `SPEC.md` parity ledger as the completion checklist; no silent omissions. (Every row is `Covered` or `Covered (deviation)` with a check; ADR 0001 names every deviation.)
- [x] Rename the spawn type argument to `subagent_type`; accept `agent_type` as a deprecated alias. (smoke 32: canonical, alias, conflicting pair, and the fallback.)
- [x] Make `subagent_send` live messaging. Keep `pubsub_send` as the legacy note tool only. (smoke 66-68; the note channel is smoke 4/29.)
- [x] Add `subagent_agents` and `/agents`. (smoke 3, 42, 69.)
- [x] Accept the Claude Code environment aliases alongside `BETTER_SUBAGENTS_*`. (smoke 36, 47, 73; the aliases share one resolver.)
- [x] Write the ADR for the live-messaging, fork, and permission-prompt transport before Phase 15. (`docs/adr/0001-worker-and-control.md`, accepted 2026-10-20: ACP for non-fork jobs, CLI fork plus one-shot `cmd --resume -p` follow-ups for forks.)
- Gate: the naming table matches `SPEC.md`; every old alias has a deprecation test. (smoke 29, 32.)

## Phase 12: Scopes, discovery, and built-ins

- [x] Managed, CLI, project, user, and plugin scopes with the documented precedence. (smoke 42: one name
  defined in all five scopes resolves to the managed definition; removing scopes one at a time resolves to
  CLI, nested project, outer project, then user.)
- [x] `--agents` JSON and JSON-file form; interactive refuses a file path. (The host has no `--agents`, so
  the CLI scope arrives as `--mod-option agents=<inline JSON|path>`; smoke 42: inline and file both load,
  interactive mode records a refusal with a reason, and a second registry without the flag has no CLI
  definition.)
- [x] Project walk-up, nested closest wins, `--add-dir`, recursive scanning. (smoke 42: the walk stops at
  `.git`, a nested `.commandcode/agents` wins, an `add_dir` directory and its subfolder both load.)
- [x] Plugin scoped names; plugin ignores `hooks`, `mcpServers`, `permissionMode`. (smoke 42:
  `my-plugin:review:security` spawns and lists; `security` does not resolve; all three fields are absent.)
- [x] Name rules; same-directory duplicates by read order; `/doctor` reports them. (smoke 42: the first
  file wins, the duplicate entry names the kept path and proposes rename-or-remove; `/doctor` prints both.)
- [x] Invalid-file skip rules and debug log; a plugin file with no name loads under its filename. (smoke 33
  and 42: the skip reasons reach `agents.log`; a nameless and an unparseable plugin file load with a
  warning that `/doctor` and `/doctor plugins` report.)
- [x] Built-ins: Explore, Plan, general-purpose, claude, statusline-setup, claude-code-guide. (smoke 87: all six exist; statusline-setup is Sonnet with `Read`/`Edit`; claude-code-guide is Haiku with read/web tools; `fork` is the seventh registry entry.)
- [x] Explore and Plan are read-only, one-shot, no agent id; Explore thoroughness; both skip CLAUDE.md and git status. (smoke 45 skips the hierarchy and snapshot; smoke 81 returns no id and refuses resume. Thoroughness is carried in the Explore body because the host exposes no per-spawn option; ADR 0001.)
- [x] Disable controls: `permissions.deny`, `CLAUDE_CODE_DISABLE_EXPLORE_PLAN_AGENTS`, `CLAUDE_AGENT_SDK_DISABLE_BUILTIN_AGENTS`. (smoke 32 and 87 cover both environment switches. `permissions.deny` for a built-in type is host policy; ADR 0001.)
- [x] The 15,000-token description-limit warning. (smoke 88: the total is 15001 in the fixture, the warning names the threshold, and built-ins are excluded.)
- Gate: precedence, invalid-file, built-in, and description-limit tests pass.

## Phase 13: Frontmatter and resolution

- [x] `tools` / `disallowedTools` inherit, list, deny, precedence, MCP wildcards, unresolved-list failure. (smoke 47: every branch of `filterTools`; smoke 48: the resolved set reaches the worker spec and survives resume.)
- [x] Tool availability filters: universal removals; background built-in allowlist plus every MCP tool; forks skip both; `ListAgents` visibility. (smoke 47: universal removals, the background allowlist with every MCP tool, and foreground-only `ListAgents`.)
- [x] Model aliases, resolution order, `inherit`, force, allowlist substitution, thinking inheritance. (smoke 47: every `resolveModel` branch including `inherit` and FORCE; smoke 48: the resolved model reaches the spec and the job record. Worker-side catalog substitution and thinking inheritance are asserted in smoke 48.)
- [x] Permission-mode inheritance: bypass, auto classifier, dontAsk, plan, manual. (smoke 53: the ACP/CLI table and the manual alias; smoke 54: bypass requires the main session and the main mode wins; smoke 57: dontAsk/plan deny, bypass allows.)
- [x] `skills` full-content preload; unlisted skills still invocable. (smoke 43: the named skill's
  `SKILL.md` is in the bundle; discovery order is project walk-up, then user, for `.commandcode/skills`
  and `.claude/skills`. An unlisted skill is simply not preloaded and stays with the host's Skill tool.)
- [x] `mcpServers` inline and referenced; trust; plugin ignored. (smoke 70: both forms parse; smoke 71:
  a reference reads config, an inline server needs a trusted folder, strict/bare/allow/deny apply, a plugin
  gets none, and the resolved set is on the worker spec, which `session/new` receives.)
- [x] `hooks` PreToolUse/PostToolUse/Stop plus project SubagentStart/SubagentStop; plugin ignored.
  (smoke 70: every event parses and a plugin gets none; smoke 72: matcher forms, exit-2 block/context/
  continue, the worker registers all three hooks, and `.commandcode/hooks.json` fires start/stop once.)
- [x] `memory` scopes, `MEMORY.md` 200 lines or 25KB. (smoke 44: `memoryDirFor` for user, project and
  local, the first 200 lines or 25KB of `MEMORY.md`, and the directory is created on spawn. The field is
  parsed and recorded. `withMemoryTools` adds Read, Write and Edit (smoke 44) and spawn applies it to the
  resolved tool set before the worker spec is built.)
- [x] `initialPrompt`, `autoCompactWindow`, `experimental.cacheTtl`, `omitClaudeMd`, `effort`, `color`.
  (smoke 45 covers `omitClaudeMd`; smoke 47-48 cover `effort`/`color`; smoke 70 parses the new fields and
  rejects an invalid TTL; smoke 73 submits `initialPrompt` only as the session agent and carries the
  lowered window and cache TTL on the spec. The adapter logs both as ACP deviations.)
- [x] Unknown fields ignored without error. (smoke 70: an unknown scalar and an unknown nested map load
  without error and are not carried on the definition.)
- [x] `--agent` session-wide mode and `--append-subagent-system-prompt` plus its file form. (smoke 73:
  the `agent` flag supplies the main prompt and submits `initialPrompt`; the append flag, file form and
  environment form all reach the worker spec, and the environment value is inherited by a nested worker.)
- Gate: every field and resolution rule has a behavioral test.

## Phase 14: Context and persistence

- [x] Startup loads: system prompt, task message, CLAUDE.md hierarchy, git status, skills, roster, output style, no parent auto memory, model-sized context window. (smoke 43: `buildStartupBundle` exposes each component and the rendered text. The roster field is rendered when supplied; the live roster is T9.)
- [x] Explore/Plan skip CLAUDE.md and git status; `omitClaudeMd`; subdirectory CLAUDE.md still loads. (smoke 45: Explore/Plan drop the hierarchy, AGENTS.md and the snapshot; `omitClaudeMd` keeps managed policy only; a path-scoped CLAUDE.md still loads in both cases.)
- [x] Resume by agent id; Explore/Plan return no id; `maxTurns` partial; per-invocation model and effort survive resume. (smoke 81: an agent id on the record/signal/spawn result and no id for Explore; smoke 82: resume by agent id keeps model, effort, `cacheTtl`, window and `maxTurns`; smoke 85-86: a `maxTurn_requests` stop is `partial` and the job resumes its ACP session. e2e step 4 resumes a real job.)
- [x] Main compaction leaves subagent transcripts untouched; transcripts persist; retention sweeps records, logs, transcripts; running jobs are never swept. (smoke 35: the worker writes `transcripts/<id>.jsonl`, a new session reads it after the fake main transcript is cleared; smoke 83: an expired record, log and transcript go together while a fresh one stays; a non-terminal job is skipped by the transcript sweep as well as the record sweep.)
- [x] Auto-compaction and the `compact_boundary` event. (smoke 85: the host's `usage_update` crossing the resolved window records and logs `compactBoundary {trigger: 'auto', preTokens}` on the job and the signal, and appends it to the transcript. ACP has no compaction event of its own; the deviation is recorded in the ADR.)
- [x] Prompt-cache split for fork and non-fork; `cacheTtl`. (smoke 81: a fork records/passes `promptCache: 'shared'`, a non-fork `'own'`; smoke 82: `cacheTtl` survives resume; smoke 35: the adapter logs the cache selection and the TTL as an ACP deviation.)
- Gate: startup-context, resume, compaction, retention, and cache tests pass. (smoke 81-86 and 35; 102 checks green.)

## Phase 15: Live messaging, teams, forks, permissions

- [x] Named agents, sibling roster at startup, `subagent_send` queue/start/resume.
- [x] `ListAgents` foreground/background visibility; `SubagentHandback`.
- [x] Agent teams: teammates keep task and cron tools; teammate runs foreground; refuse `background: true` and `run_in_background: true`. (T10: smoke 78; `filterTools({team: true})`, `resolveTeamPolicy`.)
- [x] Forks: full history, same prompt/tools/model/permissions/prompt cache; `fork` type; `/subtask`. (T10: smoke 74-77; CLI `--fork-session`, one-shot `--resume` follow-ups, `promptCache: shared`.)
- [x] Observe and steer: move rows, Enter opens the transcript and sends follow-ups, `x` stops or dismisses, Esc returns. (T10: pane + `/tasks` next/prev/enter/send/x/stop/esc; smoke 76, 79.)
- [x] Confirmation for `/compact`, `/clear`, `/rewind`; `/model` and `/fast` do not act on the fork. (Host-owned commands with no interception seam; the mod never acts on a fork from them, and `/model` and `/fast` cannot mutate it. Recorded as a host deviation in ADR 0001 and the parity ledger.)
- [x] Fork-mode defaults and controls; `run_in_background` removed where fork mode is on. (T10: `resolveForkMode`; smoke 74-75.)
- [x] Panel task rows: 30-second success/failure lifecycle, Ctrl+B, `/tasks`, `@mention` candidates. (T10: smoke 79.)
- [x] Permission prompts: foreground inline; background surfaced in the main session naming the agent; Esc denies one call without stopping it. (smoke 56 inline prompt and Esc denial; smoke 58 surfaces the prompt, names the agent, and keeps the job resumable; smoke 55 the approval request reaches the parent.)
- [x] `waiting-input` carries the question or approval request and does not hang. (smoke 55 bounds the wait; smoke 59 carries a shell question.)
- [x] The transport ADR is accepted. (`docs/adr/0001-worker-and-control.md`.)
- Gate: live-message, fork, team, and permission tests pass; ADR accepted. (Fork/team/steer/panel tests: smoke 74-80; permission prompts: smoke 55-59.)

## Phase 16: Surfaces, parity, and release

- [x] `subagent_spawn` accepts `subagent_type`, `description`, `prompt`, `name`, `team_name`, `model`, `effort`, `run_in_background`, `max_turns`, `tools`, `disallowed_tools`, `permission_mode`. (`isolation`, `mcpServers`, and `skills` are definition fields; resume is `subagent_resume`; a per-call `isolation` is refused with the frontmatter pointer. smoke 32, 47, 48, 54; schema at `subagent_spawn`.)
- [x] `/subagents`, `/agents`, `/tasks`, `/doctor`, and plugin validation. (smoke 8, 30, 42, 79.)
- [x] Pane, footer count, 30-second task-row lifecycle, transcript open, @mention typeahead. (smoke 79.)
- [x] Claude Code environment aliases accepted. (smoke 36, 47, 73.)
- [x] README and the parity matrix updated. (README title/version/install/surfaces/built-ins/description limit; `SPEC.md` ledger has a check on every row.)
- [x] Every `SPEC.md` acceptance test passes. (102 smoke checks; real-host checks pass in e2e.)
- [x] Two-session isolation, no foreign delivery, no untagged inbox, no duplicate after restart. (smoke 4-6, 12-13, 21; real e2e 1a.)
- [x] Version 2.0.0.
- [ ] Repository rename and tag after the human release gate. (Human gate: `cmd-mod-pubsub` -> `cmd-mod-better-sub-agents`, then tag `v2.0.0`. This ticket does neither.)
- Gate: the parity ledger is green; release only after human approval.
