import {homedir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';

// Bounds. Every loop and buffer below is capped on purpose.
export const DRAIN_INTERVAL_MS = 400;
export const REFRESH_INTERVAL_MS = 1000;
export const FILES_PER_DRAIN_MAX = 50;
export const LOG_TAIL_BYTES_MAX = 4000;
// A claimed file is normally removed in the same tick. One abandoned by a crashed
// drain (or left behind when the restore rename failed) is swept after this long.
export const CLAIMED_TTL_MS = 60 * 60 * 1000;
export const ORPHANED_TTL_MS = 7 * 24 * 60 * 60 * 1000;
// A finished job keeps its record and its log for this long, then is swept. Running
// jobs are never swept, however old their record is.
export const RETENTION_DAYS_DEFAULT = 30;
export const RETENTION_SWEEP_INTERVAL_MS = 60 * 60 * 1000;
// A queued/running record with no live pid is only declared lost after this grace,
// so a worker that has been spawned but has not written its pid yet is not failed.
export const LOST_GRACE_MS = 30 * 1000;
// Reconcile and list only the newest records; the spool is long-lived.
export const RECONCILE_SCAN_MAX = 200;
export const LIST_SCAN_MAX = 50;
export const DELIVERED_LEDGER_MAX = 5000;
export const DELIVERED_LEDGER_KEEP = 2000;
export const LEDGER_COMPACT_BYTES = 512 * 1024;
export const PANE_ROWS_MAX = 12;
export const PENDING_SCAN_MAX = 500;
// subagent_output's optional blocking read. Bounded, because a tool that waits is a
// turn held open - the whole point of the mod is that you never have to.
export const WAIT_DEFAULT_MS = 10 * 1000;
export const WAIT_MAX_MS = 60 * 1000;
export const WAIT_POLL_MS = 200;
export const OUTPUT_MAX_CHARS_DEFAULT = 4000;
export const OUTPUT_MAX_CHARS_CAP = 100 * 1000;
// The adapter's own cadence, set on every spec: how often it polls the control
// channel, and how long a foreground permission prompt waits for an answer.
export const CONTROL_POLL_MS = 100;
export const PERMISSION_TIMEOUT_MS = 30 * 1000;
// A foreground prompt is answered by a person, so it gets a person's bound. The
// 30s value only bounds the wait a background run never performs.
export const FOREGROUND_PERMISSION_TIMEOUT_MS = 10 * 60 * 1000;
export const ROSTER_MAIN = 'main';

// Task rows. A success disappears immediately; a failure or stop stays visible for
// this window so the user can read it, and `x` clears it early.
export const TASK_ROW_GRACE_MS = 30 * 1000;
// Fork mode. On by default for a human, off for a non-interactive/SDK run. The
// flag wins over the environment, which wins over the default.
export const FORK_MODE_FLAG = 'fork_mode';
export const FORK_MODE_ENV = 'BETTER_SUBAGENTS_FORK_MODE';
export const FORK_MODE_ENV_ALIAS = 'CLAUDE_CODE_FORK_MODE';
export const SDK_ENV = 'CLAUDE_AGENT_SDK';
export const SDK_ENV_ALIAS = 'BETTER_SUBAGENTS_SDK';
export const FORK_AGENT_TYPE = 'fork';
// A teammate keeps the shared task list and the cron scheduler even when its
// definition's tools list or disallowedTools would remove them.
export const TEAM_TOOL_NAMES: string[] = [
	'TaskCreate',
	'TaskGet',
	'TaskList',
	'TaskUpdate',
	'CronCreate',
	'CronDelete',
	'CronList',
];

// Agent definitions. A definition is a Markdown file with YAML frontmatter; its
// body is the worker's system prompt. Discovery follows the scope table: managed,
// CLI, project (closest directory upward, then --add-dir), user, plugin, built-in.
export const AGENT_NAME_MAX = 256;
export const AGENT_SCAN_MAX = 1000;
export const AGENT_SCAN_DEPTH_MAX = 8;
export const AGENT_LOG_FILE = 'agents.log';
// The host reserves these names for its own agents. We never register *our*
// built-ins under them; `general` resolves to our `general-purpose` instead.
export const HOST_RESERVED_AGENT_NAMES: Record<string, true> = {
	explore: true,
	plan: true,
	review: true,
	general: true,
	'statusline-setup': true,
	'claude-code-guide': true,
};
// The mod file the worker passes back to `cmd --mod <file> acp`. ACP is the
// accepted transport (docs/adr/0001-worker-and-control.md). The host loads a mod
// through jiti under a virtual filename (`<real>.load-<n>.<ext>`), so the shim is
// stripped to recover a path that exists on disk.
export const MOD_PATH = fileURLToPath(import.meta.url).replace(/\.load-\d+(?=\.[^./]+$)/, '');

export const OWNER_TYPE = 'subagent/owner';
// The 0.1.0 transcript entry. Read once so an installed session keeps its identity
// across the rename; never written again.
export const OWNER_TYPE_LEGACY = 'pubsub/owner';
export const JOB_ENTRY_TYPE = 'subagent/job';
export const WORKER_ENV = 'BETTER_SUBAGENTS_WORKER';
export const WORKER_SPEC_ENV = 'SUBAGENT_SPEC';
export const RETENTION_ENV = 'BETTER_SUBAGENTS_RETENTION_DAYS';
// `cleanupPeriodDays` is the Claude Code name for the same window. The host has no
// such setting, so the alias arrives through the environment, and a project or user
// `.commandcode/settings.json` is read as the documented settings surface.
export const RETENTION_ENV_ALIAS = 'CLAUDE_CODE_CLEANUP_PERIOD_DAYS';
export const CLEANUP_PERIOD_SETTING = 'cleanupPeriodDays';
// Limits. The concurrency limit is per session; the depth limit caps nesting.
export const MAX_CONCURRENT_DEFAULT = 20;
export const MAX_CONCURRENT_ENV = 'BETTER_SUBAGENTS_MAX_CONCURRENT';
export const MAX_CONCURRENT_ENV_ALIAS = 'CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS';
export const MAX_DEPTH_DEFAULT = 3;
export const MAX_DEPTH_ENV = 'BETTER_SUBAGENTS_MAX_DEPTH';
export const MAX_DEPTH_ENV_ALIAS = 'CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH';
// The pool scans recent records; the spool is long-lived and a session's active
// jobs are always among the newest.
export const ACTIVE_JOB_SCAN_MAX = 500;
export const SUBAGENT_DIR_DEFAULT = join(homedir(), '.commandcode', 'subagents');
export const PUBSUB_DIR_DEFAULT = join(homedir(), '.commandcode', 'pubsub');
// Definition scopes. The host has no `--agents`; the CLI scope arrives as
// `--mod-option agents=<json|path>`. The managed settings directory is the host's
// organization-wide config root; `agents/` under it beats every other scope.
export const MANAGED_SETTINGS_DEFAULT = join(homedir(), '.commandcode', 'managed');
export const CLI_AGENTS_OPTION = 'agents';
export const ADD_DIRS_OPTION = 'add_dir';
export const PLUGIN_DIRS_OPTION = 'plugin_dir';

// Execution policy. A run is either foreground (the tool call waits and returns
// the result) or background (the tool returns and the result arrives as a signal).
// The disable envs force the foreground, so they win over everything; a definition's
// `background: true` wins over the caller but never over the env.
export const BACKGROUND_DISABLE_ENV = 'CLAUDE_CODE_DISABLE_BACKGROUND_TASKS';
export const BACKGROUND_DISABLE_ENV_ALIAS = 'BETTER_SUBAGENTS_DISABLE_BACKGROUND';
// Foreground is bounded: a tool call must not hold the turn open forever. Past the
// bound the run is demoted to background, so its result still reaches the session.
export const FOREGROUND_WAIT_MAX_MS = 30 * 60 * 1000;
export const FOREGROUND_POLL_MS = 200;
// The worker settles an ACP turn cut off by a rate limit, an overload, or a server
// error with this exact lead. The parent uses the lead to tell a partial result
// apart from any other failure. Keep the text verbatim: it is the documented error.
export const AGENT_API_ERROR = 'Agent terminated early due to an API error';
