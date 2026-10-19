# SPEC: cmd-mod-better-sub-agents

Status: draft
Owner: Prem Modha
Date: 2026-10-19
Supersedes: cmd-mod-pubsub 0.1.0
Parity target: Claude Code subagent and tools documentation, retrieved 2026-10-19
Normative language: MUST, MUST NOT, SHOULD, MAY

## Document map

- `SPEC.md` is the single decision record for this change.
- `CHECKLIST.md` is the execution plan. Its phases are tickets, not separate specs.
- `GLOSSARY.md` is created only if a new term is settled.
- `docs/adr/NNNN-*.md` is created only when a decision is hard to reverse, surprising, and a real trade-off.
- The parity ledger at the end is the completeness check against Claude Code.

## Problem

Claude Code's async subagents let the main conversation continue while a subagent works in its own context and returns a result later. Command Code exposes an `agent` tool, but the mod-level async surface is fire-and-forget only. It has a global spool, no agent definitions, no background or foreground policy, no resume, no worktree isolation, and no concurrency control.

`cmd-mod-pubsub` proved the transport. This mod turns that transport into a subagent runtime with the same observable behavior as Claude Code's documented subagents: definitions, scopes, invocation, foreground and background execution, permission handling, lifecycle, resume, context, isolation, limits, live messaging, and forks.

## Users

- A developer running long research, audit, or code tasks who wants to keep working.
- A developer who wants named, reusable subagents with their own prompts, tools, models, and permissions.
- A developer who wants to observe, steer, and message a subagent while it runs.
- A maintainer who needs per-session isolation so one session never receives another session's result.

## Goals

1. Follow Claude Code's documented subagent behavior inside the Command Code mod API.
2. Use one transport, one state machine, and one owner model.
3. Prove per-session owner isolation with a two-session test.
4. Run in the background by default, with an explicit foreground mode when the caller needs the result.
5. Provide spawn, list, output, cancel, resume, send, and agent-listing surfaces.
6. Load agent definitions from every Claude Code scope, with Claude Code precedence.
7. Support live messaging, forks, permission-prompt passthrough, and the full frontmatter contract.
8. Preserve the single-file, no-build delivery model unless an ADR replaces it.

## Parity contract

The Claude Code documentation is the source of truth for behavior. The mod MUST implement each behavior in the parity ledger with the same observable result, adapting names only where the host already owns a name.

- No behavior is silently omitted.
- A behavior that the host cannot express MUST be recorded in an ADR as a deviation, with the closest observable behavior named.
- A field or mode that the host cannot express MUST be ignored with a warning, matching Claude Code's unknown-field behavior.
- The parity ledger is exhaustive: adding a Claude Code subagent behavior requires adding a ledger row and an acceptance test.

## Non-goals

- Reimplementing Claude Code's model catalog, provider routing, billing, or usage accounting.
- Shipping a plugin marketplace or an organization's managed-settings distribution.
- Native UI outside the host's pane, status, command, and transcript APIs.
- Host features that do not exist. Those are deviations recorded in an ADR, not silent gaps.
- Frontmatter fields with no Command Code equivalent. They are ignored with a warning, as Claude Code ignores unknown fields.

## Scopes and precedence

A definition's location determines its scope. Higher priority wins when names collide.

| Priority | Scope | Location | Lifetime |
| --- | --- | --- | --- |
| 1 | Managed | Host managed settings directory, `agents/` | Organization-wide |
| 2 | CLI | `--agents` JSON, or a JSON file in non-interactive mode | Current session only |
| 3 | Project | `.commandcode/agents/**/*.md` | The project |
| 4 | User | `~/.commandcode/agents/**/*.md` | All projects |
| 5 | Plugin | A plugin's `agents/` directory | Where the plugin is enabled |

- Project discovery MUST walk up from the working directory to the repository root and scan every `.commandcode/agents/` on the way.
- When nested project directories define the same name, the definition closest to the working directory MUST win.
- `--add-dir` and `/add-dir` MUST also load `.commandcode/agents/` from the added directory.
- Project, user, and plugin directories MUST be scanned recursively. Subdirectories do not change a project or user identity.
- A plugin subdirectory DOES become part of the scoped identifier: `agents/review/security.md` in plugin `my-plugin` registers as `my-plugin:review:security`.
- Duplicate names in one directory MUST load one definition, chosen by read order. `/doctor` MUST report the duplicate and propose a rename or removal.
- CLI definitions MUST be session-only. In non-interactive mode, `--agents` MAY be a JSON file path. Interactive mode MUST refuse a file path.
- A CLI definition accepts the same fields as frontmatter except `color` and `experimental`, which are ignored there.
- Managed definitions MUST take precedence over project and user definitions.
- Plugin definitions MUST ignore `hooks`, `mcpServers`, and `permissionMode`.

## Agent definitions

### File format

A definition is a Markdown file with YAML frontmatter. The body is the subagent's system prompt; it is not the host's system prompt. Only `name` and `description` are required. Multi-word field names use camelCase.

```markdown
---
name: code-reviewer
description: Reviews code for quality, security, and maintainability
tools: Read, Grep, Glob, Bash
model: sonnet
permissionMode: default
---

You are a senior code reviewer. Report findings, not edits.
```

### Name rules

- `name` MUST be at most 256 characters.
- `name` MUST NOT contain `:`, which is reserved for plugin-scoped identifiers.
- `name` MUST NOT start with `-` in CLI definitions.
- `name` MUST be unique across the loaded tree. The filename need not match.
- Identity comes only from `name`; a subdirectory does not change a project or user identity.

### Frontmatter reference

| Field | Required | Behavior |
| --- | --- | --- |
| `name` | Yes | Unique identifier; hooks receive it as `agent_type`. |
| `description` | Yes | Drives automatic delegation. Descriptions count against a 15,000-token warning budget. |
| `tools` | No | Comma-separated string or YAML list. Omitted means inherit every tool available to subagents. Only listed tools remain. |
| `disallowedTools` | No | Removed from the inherited or specified set. A specifier such as `Bash(git push *)` removes the whole tool. When both are set, `disallowedTools` wins. |
| `model` | No | `sonnet`, `opus`, `haiku`, `fable`, a full model id, or `inherit`. Resolution order is below. |
| `permissionMode` | No | `default`, `acceptEdits`, `auto`, `dontAsk`, `bypassPermissions`, `plan`, or `manual` (alias for `default`). Inheritance rules are below. |
| `maxTurns` | No | Stops the agent after this many agentic turns. The result is marked partial and resumable. |
| `autoCompactWindow` | No | Lowers the auto-compact window to 100,000–1,000,000 tokens. It cannot raise the inherited window. No effect when the agent is the main session. |
| `skills` | No | Skills whose full content is preloaded at startup. Unlisted skills remain invocable through the Skill tool. |
| `mcpServers` | No | Inline server definitions or references to already-configured servers. Inline definitions require the same trust as the agent file's folder; strict MCP config, bare mode, and managed allow/deny policies still apply. Scoped to this agent. Ignored for plugin agents. |
| `hooks` | No | `PreToolUse`, `PostToolUse`, and `Stop` hooks that run only while this agent is active; `Stop` is converted to `SubagentStop` at runtime. Ignored for plugin agents. |
| `memory` | No | `user`, `project`, or `local`. Enables `~/.commandcode/agent-memory/<name>/`, `.commandcode/agent-memory/<name>/`, or `.commandcode/agent-memory-local/<name>/`, auto-enables Read, Write, and Edit, and injects the first 200 lines or 25KB of `MEMORY.md`, whichever comes first. |
| `background` | No | `true` keeps the agent in the background even when the caller asks for the foreground. |
| `omitClaudeMd` | No | Starts without the user, project, and local CLAUDE.md files. Managed policy still loads, except for managed agents. Ignored when the agent runs as the main session. |
| `effort` | No | `low`, `medium`, `high`, `xhigh`, `max`. Overrides session effort; `CLAUDE_CODE_EFFORT_LEVEL` still wins. A per-invocation effort overrides the field and stays in effect on resume. |
| `isolation` | No | `worktree` runs the agent in a temporary git worktree. |
| `color` | No | `red`, `blue`, `green`, `yellow`, `purple`, `orange`, `pink`, or `cyan`. Shown in the task list and transcript. |
| `initialPrompt` | No | Auto-submitted as the first user turn when this agent runs as the main session agent. Commands and skills are processed; the value is prepended to any user-provided prompt. Ignored for plugin agents. |
| `experimental` | No | Map. `cacheTtl` is `5m` or `1h`; other keys are ignored. |

Unknown fields MUST be ignored without error.

### Tools semantics

- Both `tools` and `disallowedTools` omitted: inherit every tool available to subagents.
- `tools` only: the agent gets only the listed tools.
- `disallowedTools` only: the agent gets every inherited tool except the listed ones.
- Both set: `disallowedTools` takes precedence; a tool in both is removed.
- `mcp__server__*` selects a server's tools; `mcp__server` or `mcp__server__*` in `disallowedTools` withholds them; `mcp__*` withholds every MCP tool.
- If a `tools` list resolves to no tool, the launch MUST fail with an error naming the unresolved entries.

### Tool availability filters

Every subagent is filtered twice. A fork skips both filters and receives the main conversation's exact tool pool.

The first filter removes these tools from every subagent, even when listed in `tools`:

- `Agent` at the depth limit (it remains listed and returns an error).
- `AskUserQuestion`.
- `EndConversation` (it can end only the main conversation).
- `EnterPlanMode`.
- `ExitPlanMode`, unless the agent's `permissionMode` is `plan`.
- `ScheduleWakeup`.
- `WaitForMcpServers`.
- `Workflow`.

The second filter applies to background subagents. Apart from `Agent` and `ExitPlanMode`, which follow the first filter wherever the agent runs, a background subagent keeps every MCP tool but only these built-in tools:

`Read`, `Grep`, `Glob`, `LSP`, `Bash`, `PowerShell`, `Edit`, `Write`, `NotebookEdit`, `WebFetch`, `WebSearch`, `TodoWrite`, `Skill`, `ToolSearch`, `EnterWorktree`, `ExitWorktree`, `Monitor`, `TaskStop`, `SendMessage`, `Artifact`, and `SubagentHandback` for an agent that reports through it.

Every other built-in tool is removed from a background agent, whether inherited or listed. The removal reports no error unless it leaves `tools` resolving to nothing. Teammates in agent teams additionally keep the task and cron tools. `ListAgents` is visible to a foreground agent when cross-session messaging is enabled and is not kept by a background agent.

### Model resolution

1. The per-invocation `model` parameter.
2. The definition's `model` frontmatter. `inherit` selects the main conversation's model.
3. `CLAUDE_CODE_SUBAGENT_MODEL`, when set.
4. The main conversation's model.

- `CLAUDE_CODE_SUBAGENT_MODEL_FORCE=1` makes the environment model win over the parameter and frontmatter.
- Family aliases resolve to the newest allowed version. If an organization allowlist blocks a value, the runtime substitutes the allowed version or the inherited model and warns.
- Extended thinking is inherited from the main conversation. There is no per-subagent thinking setting.
- A subagent's context window is sized by its own model.

### Permission modes

| Mode | Behavior |
| --- | --- |
| `default` | Prompts for permission. |
| `acceptEdits` | Auto-accepts file edits and common filesystem commands in the working directory or additional directories. |
| `auto` | A background classifier reviews commands, protected-directory writes, and the final report. |
| `dontAsk` | Auto-denies prompts. Explicitly allowed tools still work. |
| `bypassPermissions` | Skips prompts. A subagent runs in this mode only when the main conversation is in it. |
| `plan` | Read-only exploration. |
| `manual` | Alias for `default`. |

- When the main conversation is in `bypassPermissions`, `acceptEdits`, or auto mode, the subagent runs in that mode and the definition's `permissionMode` is ignored.
- When the main conversation is in `default`, `dontAsk`, or `plan`, the subagent runs in the definition's mode, except that `bypassPermissions` still requires the main conversation to be in it.
- Auto mode's classifier MUST evaluate the subagent's tool calls and review its final report before delivery.
- `dontAsk` MUST still allow explicit allow rules and tools marked as requiring user interaction.
- Permission prompts follow the foreground/background rules below.

### Invalid files

A definition in a project, user, managed, or added directory MUST be skipped without a session error, and a debug-log entry MUST be written, when:

- `name` is missing.
- The opening `---` is not the first line.
- `name` starts with `-`, contains `:`, or exceeds 256 characters.
- `description` is missing.
- YAML does not parse.

A plugin definition with no `name` or unparseable frontmatter MUST still load under its filename. `/doctor` and a plugin-validation command MUST report unparseable files.

### Built-in agents

| Agent | Model | Tools | Notes |
| --- | --- | --- | --- |
| `Explore` | Main model, with the host's documented alias behavior | Read-only; Write and Edit denied | Skipped CLAUDE.md and git status. One-shot, no agent ID. Accepts a `quick`, `medium`, or `very thorough` thoroughness level. |
| `Plan` | Main model | Read-only; Write and Edit denied | Skipped CLAUDE.md and git status. One-shot, no agent ID. |
| `general-purpose` | `CLAUDE_CODE_SUBAGENT_MODEL` or the main model | Every tool available to subagents | The fallback when `subagent_type` is omitted. |
| `claude` | Main model | Every tool available to subagents | Catch-all when no specialized agent fits. |
| `statusline-setup` | Sonnet | Restricted | Used by `/statusline`. |
| `claude-code-guide` | Haiku | Restricted | Answers questions about the host's features. |

- A project or user definition MAY override a built-in by name and keeps its own `model`.
- Built-ins inherit the parent conversation's permission rules.
- `CLAUDE_CODE_DISABLE_EXPLORE_PLAN_AGENTS=1` removes Explore and Plan.
- `CLAUDE_AGENT_SDK_DISABLE_BUILTIN_AGENTS=1` removes every built-in.
- `permissions.deny` MAY block a specific built-in type, including the `Agent` tool itself.

## Invocation

### Automatic delegation

- The model MUST use each definition's `description` to decide when to delegate.
- When the combined descriptions of non-built-in agents exceed 15,000 tokens, the runtime MUST warn at startup with the total token count.
- Descriptions SHOULD stay short; detail belongs in the system prompt, which loads only when the agent runs.

### Explicit invocation

- Naming the agent in natural language SHOULD cause delegation when the model judges it appropriate.
- An `@mention` MUST guarantee the agent runs for that task. The mention typeahead MUST list custom, built-in, and plugin-scoped names.
- A direct tool call MUST start the named agent.
- If `subagent_type` is omitted and no `general-purpose` agent exists, the call MUST fail with `subagent_type is required`.

### Session-wide agent

- `--agent <name>` or the `agent` setting MUST run the whole session as that agent.
- `--append-subagent-system-prompt` and its file variant MUST append text to every subagent's system prompt, nested subagents included. A fork is exempt because it reuses the conversation's prompt.
- `initialPrompt` applies only when the agent runs as the main session agent.

## Foreground and background

- Foreground agents block the main conversation until complete. Permission prompts appear inline as each tool call happens.
- Background agents run concurrently. The main conversation continues.
- When a background agent reaches a tool call that needs permission, the prompt MUST surface in the main session and name the agent. Approving continues; Esc denies that one call without stopping the agent.
- A definition with `background: true` MUST stay in the background even when the caller asks for the foreground.
- `CLAUDE_CODE_DISABLE_BACKGROUND_TASKS=1` (alias: `BETTER_SUBAGENTS_DISABLE_BACKGROUND=1`) MUST force every run into the foreground.
- Fork mode is on by default in an interactive session and off in non-interactive mode and the Agent SDK unless it is turned on.
- Where fork mode is on, the runtime MUST run the subagents the model spawns in the background and MUST remove the `run_in_background` parameter, so the model cannot ask for the foreground.
- Where fork mode is off, the default is background; the model may request the foreground when it needs the result before continuing.
- Fork mode MUST be controllable on and off, including in non-interactive mode and the Agent SDK.
- The user MUST be able to background a running foreground task with Ctrl+B.
- On success, the task row disappears immediately and a `tasks` hint SHOULD remain in the footer for 30 seconds. On failure or stop, the row MUST remain for 30 seconds; `x` clears it.
- `/tasks` MUST open a subagent's transcript and support follow-up messages while it runs.

## Lifecycle

```
queued -> running -> waiting-input | done | failed | canceled
```

- `queued`: the job record exists and the worker is being started.
- `running`: the worker has written the child pid.
- `waiting-input`: the worker ended with a question or a permission request. The parent receives the question or request and the job stays resumable.
- `done` or `failed`: the child exited. The signal carries the exit code, duration, log tail, and partial flag.
- `canceled`: terminal. Cancellation kills the child and the worker, then emits exactly one canceled signal.
- A completed agent job MUST keep an agent ID. Resume uses that ID.
- Explore and Plan are one-shot and MUST NOT return an agent ID.
- Reaching `maxTurns` MUST mark the result partial and resumable.
- A restart MUST reconcile a dead `queued` or `running` job to `failed` after a 30-second grace.
- A transition outside the allowed set MUST be rejected, not written.
- Cancel MUST be terminal: a late result MUST NOT move the job out of `canceled` and MUST NOT emit a second signal.

## API errors

- Foreground: a rate limit, overload, or server error that cuts off an agent which already produced text MUST return the partial output with a cut-off note. If it produced nothing, or only tool calls, the call MUST fail with `Agent terminated early due to an API error` plus the error detail.
- Background: the job MUST be marked `failed`, and the delivered message MUST name the API error and include the agent's last output.
- Partial work MUST NOT be discarded.

## Concurrency and nesting

- Default concurrent subagents per session: 20.
- `CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS` (alias: `BETTER_SUBAGENTS_MAX_CONCURRENT`) changes the limit.
- The 21st spawn MUST fail with `Concurrent subagent limit reached`, and the error MUST tell the model not to retry.
- Spawning succeeds again when the running count drops below the limit.
- The limit applies to model-spawned subagents. Other features that occupy the same slots follow their own limits.
- Default nesting depth: 3. `CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH` (alias: `BETTER_SUBAGENTS_MAX_DEPTH`) changes it.
- A subagent MAY spawn its own subagents up to the configured depth.
- At the depth limit, the `Agent` tool MUST remain listed but MUST return an error instead of spawning. A fork MUST return an error instead of spawning.

## Isolation

- A worker starts in the caller's working directory. `cd` does not persist between tool calls and does not affect the main conversation.
- `isolation: worktree` creates a temporary git worktree branched from the default branch, not the parent session's HEAD.
- Bash and PowerShell commands inside the subagent MUST run in that worktree.
- A command whose working directory resolves to the main checkout MUST fail. This covers the whole repository containing the launch directory and, when the session runs in a linked worktree, the main checkout it links from.
- Bash commands MUST also be checked for redirect vectors and command-shape rules that escape the worktree. PowerShell gets the working-directory check. Monitor commands get both checks.
- A worktree with no changes MUST be removed automatically. A worktree with changes MUST be kept and its path reported.
- When the main conversation itself runs isolated in a worktree, the same checks apply to the session and to every subagent it spawns, including subagents without `isolation: worktree`.

## Context and persistence

### What loads at startup

- The agent's own system prompt plus host environment details, not the host's full system prompt.
- The delegation task message.
- The CLAUDE.md hierarchy: user, project, local, managed policy, and AGENTS.md loaded as project instructions.
- A git status snapshot, unless it is turned off or the agent is Explore or Plan.
- The full content of every skill named in `skills`.
- A sibling roster of `main` and every other named agent, when the agent has `SendMessage` and at least one other agent has a name. The roster is a snapshot taken at start.
- No parent conversation history, except for a fork.
- No parent auto memory. `memory` gives the agent its own.
- The agent's own output style; a fork uses the conversation's.
- A context window sized by the agent's model.

Explore and Plan MUST skip CLAUDE.md and the git status snapshot. `omitClaudeMd` MUST skip the user, project, and local CLAUDE.md files; managed policy still loads, except for managed agents. A subdirectory's CLAUDE.md and path-scoped rules still load when the agent works with files there.

### Resume

- A completed agent MUST return an agent ID. The parent MUST be able to resume it with full context.
- A `maxTurns` stop MUST return partial output and remain resumable.
- A resume MUST keep the per-invocation model and effort from the original run.
- Main conversation compaction MUST NOT affect subagent transcripts.
- Subagent transcripts MUST persist within the session and survive a restart when the same session is resumed.
- Transcripts MUST be deleted after `cleanupPeriodDays`, default 30 days (alias: `BETTER_SUBAGENTS_RETENTION_DAYS`). Retention MUST sweep records, logs, and transcripts together and MUST NOT remove a running job.
- Explore and Plan MUST NOT be resumable.

### Auto-compaction

- Each subagent has its own context window and compacts independently.
- `autoCompactWindow` lowers the window to a value between 100,000 and 1,000,000 tokens. It cannot raise the inherited window and has no effect when the agent is the main session.
- Compaction MUST emit a `compact_boundary` event with the trigger and pre-compaction token count.

### Prompt cache

- A fork shares the main conversation's prompt cache.
- A non-fork subagent has its own cache.
- `experimental.cacheTtl` selects `5m` or `1h` for the subagent's requests.

## Output and scanning

- Worker output MUST be scanned before it reaches the parent.
- The scan MUST insert a backslash into text that imitates the host's own output, including a `<system-reminder>` tag and a line starting with `Human:` or `Assistant:`.
- The scan MUST prepend `[harness: subagent output matched instruction-shaped pattern(s): ...]` when the report imitates a tag or mentions permission settings such as `bypassPermissions` or `--dangerously-skip-permissions`.
- A permission-setting mention gets the marker; the text itself stays as written.
- A failed worker MUST return its last output. Partial work MUST NOT be discarded.
- The parent MUST NOT see intermediate tool calls.

## Agent teams, forks, and live messaging

### Named agents and the roster

- A spawn MAY include a `name`. Named agents are addressable peers.
- A subagent that has `SendMessage` MUST receive a roster of `main` and every other named agent in the session. The roster is a snapshot taken at start.
- `subagent_send` MUST deliver a message to a named agent by `to`, including `main`.
- If the target is running, the message MUST be queued and processed at the next safe point without restarting the turn.
- If the target is idle, the message MUST start a turn.
- If the target has finished, the message MUST resume its session with full context.
- `ListAgents` MUST expose the addressable agents. A foreground agent inherits it when cross-session messaging is enabled; a background agent does not keep it.
- Agent team teammates MUST keep the task and cron tools.
- A teammate spawned by an in-process agent team MUST run in the foreground. The runtime MUST refuse a teammate subagent whose definition sets `background: true`, and MUST refuse `run_in_background: true` when fork mode is off.

### Forks

- A fork MUST inherit the full conversation history, the main session's system prompt and tools, the main session's model, the main session's permissions, and the main session's prompt cache.
- A non-fork subagent starts with a fresh context, its own system prompt and filtered tools, its own model, its own permissions, and its own cache.
- The model MAY request a fork with the `fork` subagent type. When it does not request a type, it gets `general-purpose` if that type exists.
- `/subtask` MUST start a forked subagent when agent view is off. `/fork` MUST behave as documented by the host.
- Running forks MUST be observable and steerable: move between rows, Enter opens the transcript and sends follow-ups, `x` stops or dismisses, Esc returns focus.
- `/compact`, `/clear`, and `/rewind` MUST NOT act on a fork without confirmation. `/model` and `/fast` MUST NOT change a fork's model from that view.
- Where fork mode is on, subagents the model spawns run in the background and `run_in_background` is removed.
- Fork mode MUST be controllable on and off, including in non-interactive mode and the Agent SDK.

## Surfaces

### Tools

| Tool | Arguments | Behavior |
| --- | --- | --- |
| `subagent_spawn` | `subagent_type`, `description`, `prompt`, `name`, `team_name`, `mode`, `model`, `effort`, `run_in_background`, `resume`, `isolation`, `max_turns`, `tools`, `disallowed_tools`, `permission_mode`, `mcp_servers`, `skills` | Starts an agent job. Returns the job id, agent id, and pid. Omitted `subagent_type` falls back to `general-purpose` or fails with `subagent_type is required`. |
| `subagent_list` | `state`, `limit` | Lists jobs with state, model, effort, elapsed time, owner, agent id, and name. |
| `subagent_output` | `id`, `wait`, `offset`, `max_chars` | Returns the log tail, the final signal, or a bounded wait. |
| `subagent_cancel` | `id` | Sends SIGTERM, then SIGKILL after 1 second. Emits exactly one canceled signal. |
| `subagent_resume` | `id`, `prompt` | Resumes the agent session with full context. Refuses Explore, Plan, shell jobs, and unknown ids. |
| `subagent_send` | `to`, `message` | Live message to a named agent or `main`. Queues, starts, or resumes as above. |
| `subagent_agents` | `scope` | Lists available agent definitions with scope, model, tools, and source. |

Compatibility:

- `pubsub_fire`, `pubsub_list`, and `pubsub_cancel` remain as deprecated aliases for one release and delegate to `subagent_spawn`, `subagent_list`, and `subagent_cancel`.
- `pubsub_send` is the legacy note-signal tool. It is not an alias of `subagent_send`, which is live messaging. It is removed after one release.
- The canonical Claude Code names are documented for parity. Where the host already owns a name, the mod uses the `subagent_` prefix.

### Commands

- `/subagents [list|jobs|cancel <id>|open <id>]`.
- `/agents` to inspect and manage definitions and report invalid files.
- `/tasks` to inspect running subagents, open transcripts, send follow-ups, and stop a task.
- `/doctor` reports duplicate names and invalid definitions.

### UI

- Jobs pane with six state markers, refresh, cancel, and transcript open.
- Footer count of running subagents, plus the 30-second completion hint.
- Task rows that follow the success/failure lifecycle above.
- Named agents appear in the `@mention` typeahead, including plugin-scoped names.

## Data contracts

```ts
type AgentDefinition = {
  name: string;
  description: string;
  prompt: string;
  scope: 'managed' | 'cli' | 'project' | 'user' | 'plugin';
  sourcePath?: string;
  tools?: string[];
  disallowedTools?: string[];
  model?: string;
  permissionMode?: 'default' | 'acceptEdits' | 'auto' | 'dontAsk' | 'bypassPermissions' | 'plan' | 'manual';
  maxTurns?: number;
  autoCompactWindow?: number;
  skills?: string[];
  mcpServers?: Array<string | Record<string, unknown>>;
  hooks?: Record<string, unknown>;
  memory?: 'user' | 'project' | 'local';
  background?: boolean;
  omitClaudeMd?: boolean;
  effort?: 'low' | 'medium' | 'high' | 'xhigh' | 'max';
  isolation?: 'worktree';
  color?: 'red' | 'blue' | 'green' | 'yellow' | 'purple' | 'orange' | 'pink' | 'cyan';
  initialPrompt?: string;
  experimental?: { cacheTtl?: '5m' | '1h' };
};

type Job = {
  id: string;
  agentId?: string;
  name?: string;
  teamName?: string;
  owner: string;
  sessionId: string;
  agentType: string;
  description: string;
  prompt?: string;
  command?: string;
  state: 'queued' | 'running' | 'waiting-input' | 'done' | 'failed' | 'canceled';
  pid?: number;
  workerPid?: number;
  workerSessionId?: string;
  cwd: string;
  worktree?: string;
  depth: number;
  parentJobId?: string;
  model?: string;
  effort?: string;
  permissionMode?: string;
  startedAt: number;
  endedAt?: number;
  exitCode?: number;
  logPath: string;
  signalPath?: string;
  partial?: boolean;
};

type Signal = {
  kind: 'subagent';
  jobId: string;
  agentId?: string;
  name?: string;
  owner: string;
  state: Job['state'];
  exitCode?: number;
  durationMs?: number;
  logTail?: string;
  error?: string;
  question?: string;
  partial?: boolean;
  scanned?: boolean;
  scanMatches?: string[];
};
```

## Ownership and delivery

The spool is shared by every session on the machine.

- The owner token is minted once per session and persisted with `cmd.session.appendCustomEntry({customType: 'subagent/owner', data: {token}})`.
- The token is read with `cmd.session.getCustomEntries({customType: 'subagent/owner'})`, so it survives resume and hot reload.
- Every job and every signal carries the owner token.
- A drain delivers a signal only when its owner matches.
- A foreign signal stays in `inbox/` for its owner.
- An untagged legacy signal moves to `orphaned/`.
- A worker runs with `BETTER_SUBAGENTS_WORKER=1` so its own mod instance never claims an unaddressed signal.
- Delivery is exactly once per job id in the normal path. Claim by atomic rename into `claimed/`. Seed the in-memory set from a durable `delivered.jsonl` ledger.
- A crash between delivery and the ledger write can duplicate a signal. The runtime MUST prefer no-loss plus job-id dedupe, and MUST document the at-least-once edge.
- `onStop` drains the race window.

## Reference design (non-normative)

Each job is one detached worker process. The worker launches either `cmd -p <prompt>` for an agent job or `/bin/sh -c <command>` for a shell job. The worker writes one signal envelope and exits. The parent drains the spool and delivers only its own signals.

Live messaging, permission-prompt passthrough, and forks cannot be expressed by a one-shot detached `cmd -p` process alone. The reference design adds a duplex control channel (a control directory, socket, or host-provided session handle) and a worker loop that can accept a message, a permission decision, or a follow-up turn without losing the session. The behavior is normative; the transport is an ADR before the live-messaging work starts.

Storage under `subagent_dir`, default `~/.commandcode/subagents`:

```
inbox/          workers drop <timestamp>-<id>.json
claimed/        atomic rename target during delivery
orphaned/       untagged legacy signals
logs/           one log per job
jobs/           one JSON record per job
delivered.jsonl durable delivery ledger
worktrees/      isolated checkouts
control/        live messages and permission decisions
memory/         persistent agent memory, when enabled
```

The mod SHOULD remain copyable as one file with no build step. Any split that breaks that model requires an ADR.

## Environment variables

| Canonical | Claude Code alias | Behavior |
| --- | --- | --- |
| `BETTER_SUBAGENTS_MAX_CONCURRENT` | `CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS` | Concurrency limit, default 20. |
| `BETTER_SUBAGENTS_MAX_DEPTH` | `CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH` | Nesting depth, default 3. |
| `BETTER_SUBAGENTS_DISABLE_BACKGROUND` | `CLAUDE_CODE_DISABLE_BACKGROUND_TASKS` | Force foreground. |
| `BETTER_SUBAGENTS_RETENTION_DAYS` | `cleanupPeriodDays` | Transcript retention, default 30 days. |
| `BETTER_SUBAGENTS_SUBAGENT_MODEL` | `CLAUDE_CODE_SUBAGENT_MODEL` | Default model for subagents. |
| `BETTER_SUBAGENTS_SUBAGENT_MODEL_FORCE` | `CLAUDE_CODE_SUBAGENT_MODEL_FORCE` | Force the default model over parameter and frontmatter. |
| `BETTER_SUBAGENTS_EFFORT_LEVEL` | `CLAUDE_CODE_EFFORT_LEVEL` | Effort level that wins over the definition. |
| `BETTER_SUBAGENTS_DISABLE_EXPLORE_PLAN` | `CLAUDE_CODE_DISABLE_EXPLORE_PLAN_AGENTS` | Remove Explore and Plan. |
| `BETTER_SUBAGENTS_DISABLE_BUILTINS` | `CLAUDE_AGENT_SDK_DISABLE_BUILTIN_AGENTS` | Remove every built-in. |

Claude Code aliases SHOULD be accepted so a user can move between the two tools without rewriting their environment.

## Acceptance tests

Scope and definitions:

- [ ] Managed, CLI, project, user, and plugin scopes load with the documented precedence.
- [ ] Nested project directories resolve to the definition closest to the working directory.
- [ ] `--add-dir` loads the added directory's agents.
- [ ] Recursive scanning finds subfolder definitions; plugin subfolders become scoped names.
- [ ] Same-directory duplicates load one definition and `/doctor` reports them.
- [ ] Missing `name`, missing `description`, bad name, bad first line, and bad YAML are skipped and debug-logged.
- [ ] A plugin definition with no `name` loads under its filename.
- [ ] All built-ins exist, and Explore/Plan are read-only, one-shot, and skip CLAUDE.md and git status.
- [ ] Description-driven delegation works, and the 15,000-token budget warns.
- [ ] `@mention` guarantees the named agent runs; a missing `subagent_type` with no `general-purpose` fails with the documented error.

Frontmatter and resolution:

- [ ] `tools` and `disallowedTools` follow the inherit/list/deny/precedence rules, including MCP wildcards.
- [ ] A `tools` list that resolves to nothing fails the launch naming the entries.
- [ ] Model resolution follows parameter, frontmatter, environment, main; `inherit` and force work.
- [ ] Permission modes follow the inheritance table; `bypassPermissions` requires the main session.
- [ ] `skills` preloads full content; unlisted skills remain invocable.
- [ ] `mcpServers` scopes an inline server and a referenced server.
- [ ] Frontmatter hooks fire; project `SubagentStart` and `SubagentStop` fire.
- [ ] `memory` creates the right directory and loads `MEMORY.md` within the limit.
- [ ] `autoCompactWindow` lowers the window and has no effect as main.
- [ ] `experimental.cacheTtl` selects the cache lifetime.
- [ ] `initialPrompt` applies only as the main session agent.
- [ ] `omitClaudeMd` behaves as specified.
- [ ] `effort` resolves with the environment precedence.
- [ ] `color` renders in the task list.
- [ ] Unknown fields are ignored without error.

Execution and lifecycle:

- [ ] Two sessions get different owner tokens; a signal from A is never delivered in B.
- [ ] Resume and hot reload restore the same owner token.
- [ ] A foreign signal stays in `inbox/` and is delivered when its owner drains.
- [ ] An untagged signal moves to `orphaned/`.
- [x] A background spawn returns immediately; a foreground spawn waits and returns the result. (smoke 36, 37)
- [ ] A background permission prompt surfaces in the main session, names the agent, and Esc denies one call without stopping it.
- [ ] Ctrl+B backgrounds a running foreground task.
- [ ] Task rows follow the success/failure lifecycle and the 30-second hint.
- [ ] The 21st concurrent spawn fails with the exact message; spawning resumes below the limit.
- [ ] Nesting stops at the configured depth; the `Agent` tool errors at the limit.
- [ ] Cancel produces exactly one canceled signal and no later result.
- [ ] Restart reconciliation marks a dead job failed after 30 seconds.
- [x] A foreground API error returns partial output with a cut-off note; no output fails with the documented error. (smoke 38, 39)
- [x] A background API error marks the job failed and delivers the last output. (smoke 38)
- [ ] The delivered ledger prevents replay after restart.
- [ ] A worktree with no changes is removed; a changed worktree is kept and reported.
- [ ] A command resolving to the main checkout fails inside a worktree.
- [ ] Output scanning inserts the backslash for `<system-reminder>`, `Human:`, and `Assistant:` imitations, and adds the marker for tags and permission-setting mentions.
- [ ] A real shell job and a real `cmd -p` job both complete.
- [ ] Resume continues the same agent with full context; Explore and Plan refuse resume.
- [ ] `maxTurns` returns partial output and resumes.
- [ ] Main compaction leaves subagent transcripts untouched.

Live messaging, forks, and teams:

- [ ] A named agent is addressable by `subagent_send`; a running agent queues the message without restarting the turn.
- [ ] An idle agent starts a turn; a finished agent resumes.
- [ ] The sibling roster lists `main` and named agents at startup.
- [ ] `ListAgents` is visible in the foreground and absent in the background.
- [ ] A fork inherits history, prompt, tools, model, permissions, and prompt cache.
- [ ] A running fork can be opened, sent a follow-up, and stopped from the task view.
- [ ] `/subtask` starts a fork when agent view is off.
- [ ] Fork mode runs model-spawned agents in the background and removes `run_in_background`.
- [ ] A teammate subagent runs foreground and refuses `background: true`.

## Parity ledger

| Claude Code behavior | Spec section | Status |
| --- | --- | --- |
| Custom definitions, Markdown + YAML, body is system prompt | Agent definitions | Required |
| Managed, CLI, project, user, plugin scopes and precedence | Scopes and precedence | Required |
| Recursive discovery, nested precedence, `--add-dir` | Scopes and precedence | Required |
| Name rules, duplicates, `/doctor` | Scopes and precedence | Required |
| Invalid-file skip rules and debug log | Invalid files | Required |
| Full frontmatter contract, including `mcpServers`, `hooks`, `memory`, `initialPrompt`, `autoCompactWindow`, `experimental` | Frontmatter reference | Required |
| `tools`/`disallowedTools` inheritance and precedence | Tools semantics | Required |
| Model aliases, resolution order, allowlist substitution, thinking inheritance | Model resolution | Required |
| Permission modes and inheritance | Permission modes | Required |
| Built-ins, overrides, disable controls | Built-in agents | Required |
| Automatic delegation and description budget | Invocation | Required |
| `@mention`, natural language, direct call, `subagent_type` requirement | Invocation | Required |
| Session-wide `--agent`, appended system prompt | Invocation | Required |
| Foreground/background, inline and surfaced permission prompts | Foreground and background | Required |
| `background: true`, disable-background env, fork-mode defaults | Foreground and background | Required |
| Ctrl+B, task rows, 30-second hint | Foreground and background | Required |
| Lifecycle, agent ID, partial `maxTurns`, restart reconcile | Lifecycle | Required |
| API error behavior foreground and background | API errors | Required |
| Concurrency 20, env, exact error, no-retry | Concurrency and nesting | Required |
| Nesting depth 3, env, spawn-own-subagents, depth-limit error | Concurrency and nesting | Required |
| Worktree isolation and enforcement | Isolation | Required |
| Startup context, CLAUDE.md, git status, skills, roster | Context and persistence | Required |
| Resume, transcript persistence, retention | Context and persistence | Required |
| Per-subagent auto-compaction | Context and persistence | Required |
| Prompt cache split and `cacheTtl` | Context and persistence | Required |
| Output scanning and markers | Output and scanning | Required |
| Named agents, roster, `SendMessage`, `ListAgents`, handback | Agent teams, forks, and live messaging | Required |
| Forks, `/subtask`, observe and steer | Agent teams, forks, and live messaging | Required |
| Agent teams and teammate rules | Agent teams, forks, and live messaging | Required |
| Tool surfaces and commands | Surfaces | Required |
| UI pane, footer, task rows, transcript | Surfaces | Required |
| Plugin-scoped names and plugin field restrictions | Scopes and precedence | Required |
| `/doctor` and plugin validation | Surfaces | Required |

## Open decisions

- Repository rename from `cmd-mod-pubsub` to `cmd-mod-better-sub-agents`. Recommended yes. GitHub keeps a redirect.
- Package version 2.0.0 because the tool namespace changes.
- Transport for live messaging, permission-prompt passthrough, and forks. The behavior is normative; the transport MUST be chosen in an ADR before that work starts. A one-shot detached worker cannot provide it.
- Whether the canonical environment names are `BETTER_SUBAGENTS_*` or the `CLAUDE_CODE_*` aliases. Both SHOULD be accepted; one MUST be canonical.

## References

- https://code.claude.com/docs/en/sub-agents
- https://code.claude.com/docs/en/tools-reference
