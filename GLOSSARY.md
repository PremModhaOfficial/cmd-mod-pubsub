# Subagents

The vocabulary Claude Code uses for delegated AI work: definitions, invocation, execution, permissions, context, teams, and messaging. Terms follow the Claude Code documentation as retrieved on 2026-10-19; Command Code-specific implementation names are deliberately excluded.

## Language

### Agents and definitions

**Agent**:
A Claude Code participant that runs a task in its own context. A subagent, a teammate, and a fork are all agents.
_Avoid_: bot, assistant, worker

**Subagent**:
A specialized AI assistant that handles a side task in its own context window with its own system prompt, tool access, model, and permissions, and returns only its result to the main conversation.
_Avoid_: child agent, helper, task agent

**Main conversation**:
The top-level Claude Code conversation that delegates work and receives a subagent's result.
_Avoid_: main session, root conversation

**Parent conversation**:
The conversation that spawned an agent. For a fork, it is the conversation whose history and prompt the fork inherits.
_Avoid_: caller, owner

**Session**:
A Claude Code conversation with its own context, history, and lifetime.
_Avoid_: instance, run

**Subagent definition**:
The Markdown file, with YAML frontmatter and a system-prompt body, that configures a reusable subagent.
_Avoid_: agent file, agent config, custom agent

**Frontmatter**:
The YAML block at the top of a subagent definition that sets its metadata and configuration.
_Avoid_: header, metadata block

**System prompt**:
The Markdown body of a subagent definition that guides the subagent's behavior.
_Avoid_: instructions, prompt body

**Description**:
The frontmatter field that tells Claude Code when to delegate to a subagent.
_Avoid_: summary, purpose

**Subagent type**:
The name that identifies a subagent definition and is passed when spawning it.
_Avoid_: agent type, agent_type, agent name

**Agent ID**:
The identifier Claude Code returns when it spawns a subagent. It is used to resume the subagent or address it with SendMessage.
_Avoid_: task ID, session ID

**Agent name**:
The optional name a spawn gives an agent so it can be addressed later with SendMessage. With agent teams enabled, a named spawn becomes a teammate.
_Avoid_: label, alias

**Named agent**:
An agent that has an agent name and can therefore receive messages by name.
_Avoid_: labeled agent, addressable agent

**Spawn**:
The act of starting a subagent, teammate, or fork from the main conversation or another agent.
_Avoid_: launch, create

### Scopes

**Scope**:
The location class that determines who can use a subagent definition and which definition wins when names collide.
_Avoid_: level, tier

**Managed subagent**:
A subagent deployed by an organization administrator. It takes precedence over project and user subagents.
_Avoid_: admin agent, org agent

**CLI-defined subagent**:
A subagent passed as JSON at launch. It exists only for that session.
_Avoid_: ephemeral agent, flag agent

**Project subagent**:
A subagent defined for a codebase and shared through version control. Discovery walks up from the working directory.
_Avoid_: repo agent, local agent

**User subagent**:
A personal subagent available across all of the user's projects.
_Avoid_: global agent, personal agent

**Plugin subagent**:
A subagent distributed by an installed plugin. A subdirectory in the plugin becomes part of its scoped name.
_Avoid_: extension agent, marketplace agent

### Built-in subagents

**Built-in subagent**:
A subagent that Claude Code registers by default rather than loading from a definition.
_Avoid_: system agent, default agent

**Explore**:
The read-only built-in subagent used for fast codebase research. It is one-shot and returns no agent ID.
_Avoid_: search agent, research agent

**Plan**:
The read-only built-in subagent used to design an approach. It is one-shot and returns no agent ID, and it is not the same as plan mode.
_Avoid_: planner, planning agent

**General-purpose**:
The built-in fallback subagent used when no subagent type is given.
_Avoid_: default subagent, generic agent

**Claude**:
The built-in catch-all subagent used when no specialized subagent fits.
_Avoid_: main agent, default agent

### Invocation

**Delegation**:
The act of handing a task to a subagent instead of doing it in the main conversation.
_Avoid_: dispatch, offload

**Automatic delegation**:
Claude Code choosing a subagent from its description and the task context without an explicit mention.
_Avoid_: auto-spawn, routing

**@-mention**:
An explicit reference to a subagent by name that guarantees that subagent runs.
_Avoid_: tag, call

**Description limit**:
The 15,000-token combined limit for non-built-in subagent descriptions, above which Claude Code warns at startup.
_Avoid_: description budget, prompt budget

### Forks and panels

**Fork**:
A subagent that inherits the full parent conversation, system prompt, tools, model, permissions, and prompt cache instead of starting fresh.
_Avoid_: clone, branch

**Fork mode**:
The setting that controls whether Claude may launch a fork when it spawns a subagent.
_Avoid_: background mode, clone mode

**Subtask**:
A fork started by the user with `/subtask` rather than by the model.
_Avoid_: fork command, manual fork

**Agent panel**:
The panel below the prompt input that lists teammates and subagents and lets the user open, message, or stop one.
_Avoid_: sidebar, task view, agent view

**Subagent panel**:
The tree of nested subagents shown in the agent panel, with rows marked when they still have descendants.
_Avoid_: task view, fork panel

**Sibling**:
Another subagent that shares the same parent; a subagent's siblings and direct children are shown when its row is opened.
_Avoid_: peer, cousin

### Execution

**Foreground subagent**:
A subagent that blocks the main conversation and shows permission prompts inline.
_Avoid_: blocking agent, sync agent

**Background subagent**:
A subagent that runs concurrently while the main conversation continues. Its permission prompts surface in the main session.
_Avoid_: async agent, detached agent

**Background task**:
A task Claude Code runs in the background, including a background subagent, with a task ID and an output file.
_Avoid_: job, worker, process

**Task ID**:
The identifier for a background task, used by TaskOutput and TaskStop.
_Avoid_: job ID, pid

**Task row**:
The status row for a running background task in the task list.
_Avoid_: job row, status line

**TaskOutput**:
The tool that reads a background task's output.
_Avoid_: output tool, log reader

**TaskStop**:
The tool that stops a running background task by task ID or an agent by agent ID or name.
_Avoid_: cancel tool, kill tool

### Lifecycle and limits

**Agent turn**:
One agentic turn of a subagent. Max turns caps how many it may take.
_Avoid_: iteration, step

**Max turns**:
The cap on agent turns, after which the result is partial and the subagent can be resumed.
_Avoid_: turn limit, budget

**Partial output**:
The result a subagent returns when it stops at max turns or is cut off by an API error. It remains resumable.
_Avoid_: incomplete result, truncated output

**Resume**:
Continuing a subagent from its transcript, preserving its context and its per-invocation model and effort.
_Avoid_: restart, continue

**Transcript**:
The persisted record of a subagent's conversation, addressed by agent ID.
_Avoid_: log, history

**Transcript retention**:
The period after which subagent transcripts are deleted. It is 30 days by default.
_Avoid_: cleanup, TTL

**Concurrency limit**:
The maximum number of subagents that may run at once in a session.
_Avoid_: pool size, parallel limit

**Nesting depth**:
How many levels of subagents may spawn their own subagents.
_Avoid_: recursion depth, spawn depth

### Permissions and isolation

**Permission mode**:
The setting that determines how a subagent asks for approval: default, acceptEdits, auto, dontAsk, bypassPermissions, plan, or manual.
_Avoid_: approval mode, sandbox mode

**Permission prompt**:
The approval request shown when a subagent's tool call needs permission. It is inline in the foreground and surfaced in the main session in the background.
_Avoid_: permission dialog, ask

**Permission rule**:
An allow, ask, or deny rule that controls a tool call.
_Avoid_: ACL, policy

**Auto mode**:
The permission mode in which a background classifier reviews commands, protected-directory writes, and the final report.
_Avoid_: classifier mode, automatic mode

**Plan mode**:
The permission mode in which the agent explores read-only and cannot edit. It is not the same as the Plan subagent.
_Avoid_: planning mode, read-only mode

**Tool access**:
The set of tools a subagent may use, determined by its definition, the main conversation, and the two tool filters.
_Avoid_: permissions, capabilities

**Tool filter**:
One of the two narrowings of a subagent's tool set: a universal removal list and a background allowlist.
_Avoid_: tool policy, tool restriction

**Worktree isolation**:
The `isolation: worktree` mode in which a subagent runs in a temporary git worktree branched from the default branch.
_Avoid_: sandbox, isolated copy

### Context and memory

**Context window**:
The token budget a subagent has for its own conversation, sized by its own model.
_Avoid_: context, window

**Compaction**:
The automatic summarization of a subagent's context when its window fills.
_Avoid_: summarization, truncation

**Compact boundary**:
The event that records a compaction, with its trigger and pre-compaction token count.
_Avoid_: checkpoint, snapshot

**Auto-compact window**:
The token threshold at which auto-compaction runs. The `autoCompactWindow` setting can lower it but not raise it.
_Avoid_: compaction threshold, context limit

**Prompt cache**:
The cached prompt prefix that lets a resumed or forked agent avoid reprocessing history. A fork shares the main conversation's cache.
_Avoid_: KV cache, prefix cache

**Cache TTL**:
The lifetime of a subagent's prompt cache: 5 minutes or 1 hour.
_Avoid_: cache expiry, cache duration

**Extended thinking**:
The main conversation's thinking setting, which every subagent inherits. There is no per-subagent setting.
_Avoid_: reasoning mode, thinking budget

**Effort**:
The reasoning effort level a subagent runs at: low, medium, high, xhigh, or max.
_Avoid_: reasoning level, intensity

**Model alias**:
A short model name such as sonnet, opus, haiku, or fable, or inherit, resolved to a concrete model.
_Avoid_: model name, model family

**CLAUDE.md**:
The user, project, and local instruction files loaded into a subagent unless the omit-CLAUDE.md setting is used.
_Avoid_: memory file, config

**Agent memory**:
A persistent memory directory scoped to user, project, or local, with its own MEMORY.md.
_Avoid_: context, notes

**Auto memory**:
The main conversation's automatically maintained memory. A subagent does not inherit it and uses agent memory instead.
_Avoid_: memory, notes

**Skill**:
A reusable prompt-based workflow that a subagent can preload by name. An unlisted skill remains invocable through the Skill tool.
_Avoid_: plugin, command

**Hook**:
A command that runs at a lifecycle event, such as before or after a tool call, or when a subagent stops.
_Avoid_: trigger, callback

**Output scanning**:
The check that neutralizes instruction-shaped text in a subagent's report before it reaches the main conversation.
_Avoid_: sanitization, filtering

### Agent teams and messaging

**Agent team**:
A group of Claude Code instances coordinated by a lead session, with shared tasks and direct messaging.
_Avoid_: swarm, cluster

**Team lead**:
The main session that coordinates an agent team for its lifetime.
_Avoid_: manager, orchestrator

**Teammate**:
A member of an agent team that works in its own context window and communicates with the lead and other teammates.
_Avoid_: team member, worker

**Mailbox**:
The per-agent inbox through which team messages are delivered.
_Avoid_: inbox, queue

**In-process teammate**:
A teammate that runs inside the lead's session and follows the lead's session behavior.
_Avoid_: local teammate, embedded teammate

**Split-pane teammate**:
A teammate that runs as a separate Claude Code process in its own split pane, so its own model decides.
_Avoid_: external teammate, separate teammate

**Cross-session messaging**:
The ability for Claude to message other Claude Code sessions, on the same machine or beyond it.
_Avoid_: inter-session chat, remote messaging

**SendMessage**:
The tool that sends a message to another agent by agent ID or name, or to another Claude Code session.
_Avoid_: message tool, chat

**ListAgents**:
The tool that lists the agents Claude can message with SendMessage.
_Avoid_: roster tool, directory tool

**SubagentHandback**:
The tool that delivers a subagent's final report to the conversation that receives its result in auto mode.
_Avoid_: handoff, callback

**Task list**:
The shared list of tasks a team coordinates through, backed by the Task tools.
_Avoid_: backlog, queue

**Task tools**:
The shared task-list tools that teammates keep: TaskCreate, TaskGet, TaskList, and TaskUpdate.
_Avoid_: todo tools, backlog

**Cron tools**:
The scheduling tools that teammates keep: CronCreate, CronDelete, and CronList.
_Avoid_: scheduler, timers

**Plan approval**:
The team exception in which the lead session approves a teammate's plan without a separate user prompt.
_Avoid_: auto-approve, plan sign-off

## Source

- Claude Code, Create custom subagents: https://code.claude.com/docs/en/sub-agents
- Claude Code, Tools reference: https://code.claude.com/docs/en/tools-reference
- Claude Code, Agent teams: https://code.claude.com/docs/en/agent-teams

All definitions follow those pages as retrieved on 2026-10-19.
