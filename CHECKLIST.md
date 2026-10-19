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
- [ ] Add `isolation: worktree` with `git worktree`.
- [ ] Remove an unchanged worktree.
- [ ] Keep and report a changed worktree.
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

## Phase 11: Parity ledger and naming

- [ ] Adopt the `SPEC.md` parity ledger as the completion checklist; no silent omissions.
- [ ] Rename the spawn type argument to `subagent_type`; accept `agent_type` as a deprecated alias.
- [ ] Make `subagent_send` live messaging. Keep `pubsub_send` as the legacy note tool only.
- [ ] Add `subagent_agents` and `/agents`.
- [ ] Accept the Claude Code environment aliases alongside `BETTER_SUBAGENTS_*`.
- [x] Write the ADR for the live-messaging, fork, and permission-prompt transport before Phase 15. (`docs/adr/0001-worker-and-control.md`, accepted 2026-10-20: ACP for non-fork jobs, CLI fork plus one-shot `cmd --resume -p` follow-ups for forks.)
- Gate: the naming table matches `SPEC.md`; every old alias has a deprecation test.

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
- [ ] Built-ins: Explore, Plan, general-purpose, claude, statusline-setup, claude-code-guide.
- [ ] Explore and Plan are read-only, one-shot, no agent id; Explore thoroughness; both skip CLAUDE.md and git status.
- [ ] Disable controls: `permissions.deny`, `CLAUDE_CODE_DISABLE_EXPLORE_PLAN_AGENTS`, `CLAUDE_AGENT_SDK_DISABLE_BUILTIN_AGENTS`.
- [ ] The 15,000-token description-budget warning.
- Gate: precedence, invalid-file, built-in, and description-budget tests pass.

## Phase 13: Frontmatter and resolution

- [x] `tools` / `disallowedTools` inherit, list, deny, precedence, MCP wildcards, unresolved-list failure. (smoke 47: every branch of `filterTools`; smoke 48: the resolved set reaches the worker spec and survives resume.)
- [x] Tool availability filters: universal removals; background built-in allowlist plus every MCP tool; forks skip both; `ListAgents` visibility. (smoke 47: universal removals, the background allowlist with every MCP tool, and foreground-only `ListAgents`.)
- [x] Model aliases, resolution order, `inherit`, force, allowlist substitution, thinking inheritance. (smoke 47: every `resolveModel` branch including `inherit` and FORCE; smoke 48: the resolved model reaches the spec and the job record. Worker-side catalog substitution and thinking inheritance are asserted in smoke 48.)
- [ ] Permission-mode inheritance: bypass, auto classifier, dontAsk, plan, manual.
- [x] `skills` full-content preload; unlisted skills still invocable. (smoke 43: the named skill's
  `SKILL.md` is in the bundle; discovery order is project walk-up, then user, for `.commandcode/skills`
  and `.claude/skills`. An unlisted skill is simply not preloaded and stays with the host's Skill tool.)
- [ ] `mcpServers` inline and referenced; trust; plugin ignored.
- [ ] `hooks` PreToolUse/PostToolUse/Stop plus project SubagentStart/SubagentStop; plugin ignored.
- [x] `memory` scopes, `MEMORY.md` 200 lines or 25KB. (smoke 44: `memoryDirFor` for user, project and
  local, the first 200 lines or 25KB of `MEMORY.md`, and the directory is created on spawn. The field is
  parsed and recorded. `withMemoryTools` adds Read, Write and Edit (smoke 44) and spawn applies it to the
  resolved tool set before the worker spec is built.)
- [ ] `initialPrompt`, `autoCompactWindow`, `experimental.cacheTtl`, `omitClaudeMd`, `effort`, `color`.
- [ ] Unknown fields ignored without error.
- [ ] `--agent` session-wide mode and `--append-subagent-system-prompt` plus its file form.
- Gate: every field and resolution rule has a behavioral test.

## Phase 14: Context and persistence

- [x] Startup loads: system prompt, task message, CLAUDE.md hierarchy, git status, skills, roster, output style, no parent auto memory, model-sized context window. (smoke 43: `buildStartupBundle` exposes each component and the rendered text. The roster field is rendered when supplied; the live roster is T9.)
- [x] Explore/Plan skip CLAUDE.md and git status; `omitClaudeMd`; subdirectory CLAUDE.md still loads. (smoke 45: Explore/Plan drop the hierarchy, AGENTS.md and the snapshot; `omitClaudeMd` keeps managed policy only; a path-scoped CLAUDE.md still loads in both cases.)
- [ ] Resume by agent id; Explore/Plan return no id; `maxTurns` partial; per-invocation model and effort survive resume.
- [ ] Main compaction leaves subagent transcripts untouched; transcripts persist; retention sweeps records, logs, transcripts; running jobs are never swept.
- [ ] Auto-compaction and the `compact_boundary` event.
- [ ] Prompt-cache split for fork and non-fork; `cacheTtl`.
- Gate: startup-context, resume, compaction, retention, and cache tests pass.

## Phase 15: Live messaging, teams, forks, permissions

- [x] Named agents, sibling roster at startup, `subagent_send` queue/start/resume.
- [x] `ListAgents` foreground/background visibility; `SubagentHandback`.
- [ ] Agent teams: teammates keep task and cron tools; teammate runs foreground; refuse `background: true` and `run_in_background: true`.
- [ ] Forks: full history, same prompt/tools/model/permissions/prompt cache; `fork` type; `/subtask`.
- [ ] Observe and steer: Enter, `x`, Esc; confirmation for `/compact`, `/clear`, `/rewind`; `/model` and `/fast` do not act on the fork.
- [ ] Fork-mode defaults and controls; `run_in_background` removed where fork mode is on.
- [ ] Permission prompts: foreground inline; background surfaced in the main session naming the agent; Esc denies one call without stopping it.
- [ ] `waiting-input` carries the question or approval request and does not hang.
- [ ] The transport ADR is accepted.
- Gate: live-message, fork, team, and permission tests pass; ADR accepted.

## Phase 16: Surfaces, parity, and release

- [ ] `subagent_spawn` accepts `subagent_type`, `description`, `prompt`, `name`, `team_name`, `mode`, `model`, `effort`, `run_in_background`, `resume`, `isolation`, `max_turns`, `tools`, `disallowed_tools`, `permission_mode`, `mcp_servers`, `skills`.
- [ ] `/subagents`, `/agents`, `/tasks`, `/doctor`, and plugin validation.
- [ ] Pane, footer count, 30-second task-row lifecycle, transcript open, @mention typeahead.
- [ ] Claude Code environment aliases accepted.
- [ ] README and the parity matrix updated.
- [ ] Every `SPEC.md` acceptance test passes.
- [ ] Two-session isolation, no foreign delivery, no untagged inbox, no duplicate after restart.
- [ ] Version 2.0.0.
- [ ] Repository rename and tag after the human release gate.
- Gate: the parity ledger is green; release only after human approval.
