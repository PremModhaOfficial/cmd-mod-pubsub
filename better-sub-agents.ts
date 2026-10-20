var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });

// src/index.ts
import { spawn, spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import {
  appendFileSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  renameSync,
  rmSync,
  statSync,
  writeFileSync
} from "node:fs";
import { homedir as homedir2 } from "node:os";
import { dirname as dirname4, isAbsolute as isAbsolute2, join as join12, resolve as resolve2 } from "node:path";

// src/constants.ts
import { homedir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
var DRAIN_INTERVAL_MS = 400;
var REFRESH_INTERVAL_MS = 1e3;
var FILES_PER_DRAIN_MAX = 50;
var LOG_TAIL_BYTES_MAX = 4e3;
var CLAIMED_TTL_MS = 60 * 60 * 1e3;
var ORPHANED_TTL_MS = 7 * 24 * 60 * 60 * 1e3;
var RETENTION_DAYS_DEFAULT = 30;
var RETENTION_SWEEP_INTERVAL_MS = 60 * 60 * 1e3;
var LOST_GRACE_MS = 30 * 1e3;
var RECONCILE_SCAN_MAX = 200;
var LIST_SCAN_MAX = 50;
var DELIVERED_LEDGER_MAX = 5e3;
var DELIVERED_LEDGER_KEEP = 2e3;
var LEDGER_COMPACT_BYTES = 512 * 1024;
var PANE_ROWS_MAX = 12;
var PENDING_SCAN_MAX = 500;
var WAIT_DEFAULT_MS = 10 * 1e3;
var WAIT_MAX_MS = 60 * 1e3;
var WAIT_POLL_MS = 200;
var OUTPUT_MAX_CHARS_DEFAULT = 4e3;
var OUTPUT_MAX_CHARS_CAP = 100 * 1e3;
var LIVE_IDLE_MS_DEFAULT = 30 * 1e3;
var CONTROL_POLL_MS = 100;
var PERMISSION_TIMEOUT_MS = 30 * 1e3;
var FOREGROUND_PERMISSION_TIMEOUT_MS = 10 * 60 * 1e3;
var ROSTER_MAIN = "main";
var TASK_ROW_GRACE_MS = 30 * 1e3;
var FORK_MODE_FLAG = "fork_mode";
var FORK_MODE_ENV = "BETTER_SUBAGENTS_FORK_MODE";
var FORK_MODE_ENV_ALIAS = "CLAUDE_CODE_FORK_MODE";
var SDK_ENV = "CLAUDE_AGENT_SDK";
var SDK_ENV_ALIAS = "BETTER_SUBAGENTS_SDK";
var FORK_AGENT_TYPE = "fork";
var TEAM_TOOL_NAMES = [
  "TaskCreate",
  "TaskGet",
  "TaskList",
  "TaskUpdate",
  "CronCreate",
  "CronDelete",
  "CronList"
];
var AGENT_NAME_MAX = 256;
var AGENT_SCAN_MAX = 1e3;
var AGENT_SCAN_DEPTH_MAX = 8;
var AGENT_LOG_FILE = "agents.log";
var HOST_RESERVED_AGENT_NAMES = {
  explore: true,
  plan: true,
  review: true,
  general: true,
  "statusline-setup": true,
  "claude-code-guide": true
};
var MOD_PATH = fileURLToPath(import.meta.url).replace(/\.load-\d+(?=\.[^./]+$)/, "");
var OWNER_TYPE = "subagent/owner";
var OWNER_TYPE_LEGACY = "pubsub/owner";
var JOB_ENTRY_TYPE = "subagent/job";
var WORKER_ENV = "BETTER_SUBAGENTS_WORKER";
var WORKER_SPEC_ENV = "SUBAGENT_SPEC";
var RETENTION_ENV = "BETTER_SUBAGENTS_RETENTION_DAYS";
var RETENTION_ENV_ALIAS = "CLAUDE_CODE_CLEANUP_PERIOD_DAYS";
var CLEANUP_PERIOD_SETTING = "cleanupPeriodDays";
var MAX_CONCURRENT_DEFAULT = 20;
var MAX_CONCURRENT_ENV = "BETTER_SUBAGENTS_MAX_CONCURRENT";
var MAX_CONCURRENT_ENV_ALIAS = "CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS";
var MAX_DEPTH_DEFAULT = 3;
var MAX_DEPTH_ENV = "BETTER_SUBAGENTS_MAX_DEPTH";
var MAX_DEPTH_ENV_ALIAS = "CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH";
var ACTIVE_JOB_SCAN_MAX = 500;
var SUBAGENT_DIR_DEFAULT = join(homedir(), ".commandcode", "subagents");
var PUBSUB_DIR_DEFAULT = join(homedir(), ".commandcode", "pubsub");
var MANAGED_SETTINGS_DEFAULT = join(homedir(), ".commandcode", "managed");
var CLI_AGENTS_OPTION = "agents";
var ADD_DIRS_OPTION = "add_dir";
var PLUGIN_DIRS_OPTION = "plugin_dir";
var BACKGROUND_DISABLE_ENV = "CLAUDE_CODE_DISABLE_BACKGROUND_TASKS";
var BACKGROUND_DISABLE_ENV_ALIAS = "BETTER_SUBAGENTS_DISABLE_BACKGROUND";
var FOREGROUND_WAIT_MAX_MS = 30 * 60 * 1e3;
var FOREGROUND_POLL_MS = 200;
var AGENT_API_ERROR = "Agent terminated early due to an API error";

// src/definitions.ts
import { dirname, join as join2 } from "node:path";
var AGENT_SCOPES = ["managed", "cli", "project", "user", "plugin", "builtin"];
var MEMORY_SCOPES = { user: "user", project: "project", local: "local" };
function parseYamlScalar(raw) {
  const text = raw.trim();
  if (text.length === 0) return { ok: true, value: void 0 };
  if (text.startsWith("[")) {
    if (!text.endsWith("]")) return { ok: false, reason: "unclosed flow sequence" };
    const inner = text.slice(1, -1).trim();
    if (inner.length === 0) return { ok: true, value: [] };
    const items = [];
    let current = "";
    let quote = "";
    for (const char of inner) {
      if (quote.length > 0) {
        current += char;
        if (char === quote) quote = "";
        continue;
      }
      if (char === '"' || char === "'") {
        quote = char;
        current += char;
        continue;
      }
      if (char === ",") {
        items.push(current.trim());
        current = "";
        continue;
      }
      current += char;
    }
    if (quote.length > 0) return { ok: false, reason: "unterminated quoted scalar" };
    items.push(current.trim());
    const values = [];
    for (const item of items) {
      if (item.length === 0) return { ok: false, reason: "empty flow sequence item" };
      const parsed = parseYamlScalar(item);
      if (parsed.ok === false) return parsed;
      values.push(parsed.value);
    }
    return { ok: true, value: values };
  }
  if (text.startsWith("{")) {
    if (!text.endsWith("}")) return { ok: false, reason: "unclosed flow mapping" };
    return { ok: true, value: null };
  }
  if (text.startsWith('"')) {
    if (text.length < 2 || !text.endsWith('"')) return { ok: false, reason: "unterminated double-quoted scalar" };
    try {
      return { ok: true, value: JSON.parse(text) };
    } catch {
      return { ok: false, reason: "bad escape in double-quoted scalar" };
    }
  }
  if (text.startsWith("'")) {
    if (text.length < 2 || !text.endsWith("'")) return { ok: false, reason: "unterminated single-quoted scalar" };
    return { ok: true, value: text.slice(1, -1).replaceAll("''", "'") };
  }
  const comment = text.search(/\s#/);
  const bare = (comment >= 0 ? text.slice(0, comment) : text).trim();
  if (bare === "true") return { ok: true, value: true };
  if (bare === "false") return { ok: true, value: false };
  if (bare === "null" || bare === "~") return { ok: true, value: void 0 };
  return { ok: true, value: bare };
}
__name(parseYamlScalar, "parseYamlScalar");
function normalizeNestedLines(raw) {
  const lines = raw.filter((line) => line.trim().length > 0 && !line.trim().startsWith("#")).map((line) => {
    const indent = line.length - line.trimStart().length;
    return { indent, text: line.slice(indent).replace(/\s+$/, "") };
  });
  if (lines.length === 0) return lines;
  const margin = Math.min(...lines.map((line) => line.indent));
  return lines.map((line) => ({ indent: line.indent - margin, text: line.text }));
}
__name(normalizeNestedLines, "normalizeNestedLines");
var isSequenceLine = /* @__PURE__ */ __name((text) => text === "-" || text.startsWith("- "), "isSequenceLine");
var MAP_LINE = /^([A-Za-z0-9_.-]+):(?:[ \t]+(.*))?$/;
function parseYamlNode(lines, start, indent) {
  const first = lines[start];
  if (first === void 0) return { ok: true, value: void 0, next: start };
  return isSequenceLine(first.text) ? parseYamlSequence(lines, start, indent) : parseYamlMapping(lines, start, indent);
}
__name(parseYamlNode, "parseYamlNode");
function parseYamlSequence(lines, start, indent) {
  const values = [];
  let index = start;
  while (index < lines.length) {
    const line = lines[index];
    if (line.indent < indent) break;
    if (line.indent > indent) return { ok: false, reason: `unexpected indentation` };
    if (!isSequenceLine(line.text)) break;
    const rest = line.text === "-" ? "" : line.text.slice(2);
    const trimmed = rest.trim();
    if (trimmed.length === 0) {
      const next = lines[index + 1];
      if (next !== void 0 && next.indent > indent) {
        const nested = parseYamlNode(lines, index + 1, next.indent);
        if (!nested.ok) return nested;
        values.push(nested.value);
        index = nested.next;
      } else {
        values.push(null);
        index += 1;
      }
      continue;
    }
    const map = MAP_LINE.exec(trimmed);
    if (map !== null) {
      const base = indent + (line.text.length - trimmed.length);
      const itemLines = [{ indent: base, text: trimmed }];
      let cursor = index + 1;
      while (cursor < lines.length && lines[cursor].indent > indent) {
        itemLines.push(lines[cursor]);
        cursor += 1;
      }
      const nested = parseYamlNode(itemLines, 0, base);
      if (!nested.ok) return nested;
      values.push(nested.value);
      index = cursor;
      continue;
    }
    const parsed = parseYamlScalar(trimmed);
    if (parsed.ok === false) return parsed;
    values.push(parsed.value);
    index += 1;
  }
  return { ok: true, value: values, next: index };
}
__name(parseYamlSequence, "parseYamlSequence");
function parseYamlMapping(lines, start, indent) {
  const value = {};
  let index = start;
  while (index < lines.length) {
    const line = lines[index];
    if (line.indent < indent) break;
    if (line.indent > indent) return { ok: false, reason: "unexpected indentation" };
    const match = MAP_LINE.exec(line.text);
    if (match === null) return { ok: false, reason: `expected key: value, got "${line.text.slice(0, 40)}"` };
    const key = match[1];
    const inline = (match[2] ?? "").trim();
    if (Object.hasOwn(value, key)) return { ok: false, reason: `duplicate key "${key}"` };
    const next = lines[index + 1];
    if (inline.length > 0) {
      if (next !== void 0 && next.indent > indent) {
        return { ok: false, reason: `unexpected indentation under "${key}"` };
      }
      const parsed = parseYamlScalar(inline);
      if (parsed.ok === false) return parsed;
      value[key] = parsed.value;
      index += 1;
      continue;
    }
    if (next !== void 0 && next.indent > indent) {
      const nested = parseYamlNode(lines, index + 1, next.indent);
      if (!nested.ok) return nested;
      value[key] = nested.value;
      index = nested.next;
      continue;
    }
    value[key] = void 0;
    index += 1;
  }
  return { ok: true, value, next: index };
}
__name(parseYamlMapping, "parseYamlMapping");
function parseFrontmatter(source) {
  const text = source.startsWith("\uFEFF") ? source.slice(1) : source;
  const lines = text.split("\n");
  if (lines[0]?.trim() !== "---") return { ok: false, reason: "frontmatter must start with --- on the first line" };
  let close = -1;
  for (let index = 1; index < lines.length; index += 1) {
    if (lines[index].trim() === "---") {
      close = index;
      break;
    }
  }
  if (close < 0) return { ok: false, reason: "frontmatter is not closed" };
  const blocks = [];
  for (let index = 1; index < close; index += 1) {
    const line = lines[index];
    const trimmed = line.trim();
    if (trimmed.length === 0 || trimmed.startsWith("#")) {
      if (blocks.length > 0) blocks[blocks.length - 1].lines.push("");
      continue;
    }
    if (line.startsWith("	")) return { ok: false, reason: "tab indentation is not valid YAML" };
    const indent = line.length - line.trimStart().length;
    if (indent > 0) {
      if (blocks.length === 0) return { ok: false, reason: "unexpected indentation" };
      blocks[blocks.length - 1].lines.push(line);
      continue;
    }
    const match = /^([A-Za-z0-9_.-]+):(.*)$/.exec(line);
    if (match === null) return { ok: false, reason: `expected key: value, got "${line.slice(0, 40)}"` };
    blocks.push({ key: match[1], inline: match[2].trim(), lines: [] });
  }
  const fields = {};
  for (const block of blocks) {
    if (Object.hasOwn(fields, block.key)) return { ok: false, reason: `duplicate key "${block.key}"` };
    const blockScalar = block.inline === "|" || block.inline === "|-" || block.inline === ">" || block.inline === ">-";
    if (blockScalar) {
      const filled = block.lines.filter((line) => line.trim().length > 0);
      const margin = filled.length === 0 ? 0 : Math.min(...filled.map((line) => line.length - line.trimStart().length));
      const body = block.lines.map((line) => line.trim().length === 0 ? "" : line.slice(margin).replace(/\s+$/, ""));
      fields[block.key] = (block.inline.startsWith(">") ? body.join(" ") : body.join("\n")).replace(/^\n+|\n+$/g, "");
      continue;
    }
    const hasNested = block.lines.some((line) => line.trim().length > 0);
    if (block.inline.length === 0 && hasNested) {
      const parsed2 = parseYamlNode(normalizeNestedLines(block.lines), 0, 0);
      if (parsed2.ok === false) return parsed2;
      fields[block.key] = parsed2.value;
      continue;
    }
    if (hasNested) {
      return { ok: false, reason: `unexpected indentation under "${block.key}"` };
    }
    if (block.inline.length === 0) {
      fields[block.key] = void 0;
      continue;
    }
    const parsed = parseYamlScalar(block.inline);
    if (parsed.ok === false) return parsed;
    fields[block.key] = parsed.value;
  }
  return { ok: true, fields, body: lines.slice(close + 1).join("\n").replace(/^\r?\n/, "").trimEnd() };
}
__name(parseFrontmatter, "parseFrontmatter");
function listField(value) {
  if (value === void 0 || value === null) return void 0;
  if (Array.isArray(value)) {
    const items = value.filter((item) => typeof item === "string").map((item) => item.trim()).filter(Boolean);
    return items.length > 0 ? items : void 0;
  }
  if (typeof value !== "string") return void 0;
  const text = value.trim();
  if (text.length === 0) return void 0;
  if (text === "*") return ["*"];
  return text.split(",").map((item) => item.trim()).filter(Boolean);
}
__name(listField, "listField");
function stringField(value) {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : void 0;
}
__name(stringField, "stringField");
function booleanField(value) {
  if (typeof value === "boolean") return value;
  if (value === "true") return true;
  if (value === "false") return false;
  return void 0;
}
__name(booleanField, "booleanField");
function numberField(value) {
  const numeric = typeof value === "number" ? value : typeof value === "string" ? Number(value) : Number.NaN;
  return Number.isFinite(numeric) && numeric > 0 ? Math.round(numeric) : void 0;
}
__name(numberField, "numberField");
function stringMapField(value) {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return void 0;
  const out = {};
  for (const [key, raw] of Object.entries(value)) {
    if (typeof raw !== "string") continue;
    out[key] = raw;
  }
  return Object.keys(out).length > 0 ? out : void 0;
}
__name(stringMapField, "stringMapField");
function serverFields(value) {
  const command = stringField(value.command);
  const url = stringField(value.url);
  const args = listField(value.args);
  const env = stringMapField(value.env);
  if (command === void 0 && url === void 0) return void 0;
  return {
    ...command !== void 0 ? { command } : {},
    ...args !== void 0 ? { args } : {},
    ...env !== void 0 ? { env } : {},
    ...url !== void 0 ? { url } : {}
  };
}
__name(serverFields, "serverFields");
function parseMcpServers(value) {
  const out = [];
  if (typeof value === "string") {
    const name = stringField(value);
    return name === void 0 ? void 0 : [{ name }];
  }
  if (Array.isArray(value)) {
    for (const item of value) {
      if (typeof item === "string") {
        const name2 = stringField(item);
        if (name2 !== void 0) out.push({ name: name2 });
        continue;
      }
      if (item === null || typeof item !== "object") continue;
      const record = item;
      const name = stringField(record.name) ?? stringField(record.server);
      if (name === void 0) continue;
      out.push({ name, ...serverFields(record) });
    }
  } else if (value !== null && typeof value === "object") {
    for (const [name, config] of Object.entries(value)) {
      const clean = name.trim();
      if (clean.length === 0) continue;
      if (config === null || config === void 0 || config === true) {
        out.push({ name: clean });
        continue;
      }
      if (typeof config === "string") {
        out.push({ name: clean, command: config });
        continue;
      }
      if (typeof config === "object" && !Array.isArray(config)) {
        out.push({ name: clean, ...serverFields(config) });
      }
    }
  }
  return out.length > 0 ? out : void 0;
}
__name(parseMcpServers, "parseMcpServers");
function hookRule(value, inheritedMatcher) {
  if (typeof value === "string") {
    const command2 = value.trim();
    return command2.length === 0 ? [] : [{ command: command2, ...inheritedMatcher !== void 0 ? { matcher: inheritedMatcher } : {} }];
  }
  if (value === null || typeof value !== "object" || Array.isArray(value)) return [];
  const record = value;
  const matcher = stringField(record.matcher) ?? inheritedMatcher;
  const nested = record.hooks;
  if (nested !== void 0) {
    return Array.isArray(nested) ? nested.flatMap((item) => hookRule(item, matcher)) : hookRule(nested, matcher);
  }
  const command = stringField(record.command);
  if (command === void 0) return [];
  const seconds = numberField(record.timeout);
  return [
    {
      command,
      ...matcher !== void 0 ? { matcher } : {},
      ...seconds !== void 0 ? { timeoutMs: seconds * 1e3 } : {}
    }
  ];
}
__name(hookRule, "hookRule");
function hookList(value) {
  const rules = (Array.isArray(value) ? value : [value]).flatMap((item) => hookRule(item));
  return rules.length > 0 ? rules : void 0;
}
__name(hookList, "hookList");
function parseHooks(value) {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return void 0;
  const byEvent = {};
  for (const [event, raw] of Object.entries(value)) {
    const normalized = event.replace(/[^a-z]/gi, "").toLowerCase();
    const key = normalized === "pretooluse" ? "PreToolUse" : normalized === "posttooluse" ? "PostToolUse" : normalized === "stop" || normalized === "subagentstop" ? "Stop" : void 0;
    if (key === void 0) continue;
    const rules = hookList(raw);
    if (rules !== void 0) byEvent[key] = [...byEvent[key] ?? [], ...rules];
  }
  return Object.keys(byEvent).length > 0 ? byEvent : void 0;
}
__name(parseHooks, "parseHooks");
function parseCacheTtl(value) {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return void 0;
  const ttl = stringField(value.cacheTtl);
  return ttl === "5m" || ttl === "1h" ? ttl : void 0;
}
__name(parseCacheTtl, "parseCacheTtl");
function validateAgentName(name) {
  if ([...name].length > AGENT_NAME_MAX) return `name: must be at most ${AGENT_NAME_MAX} characters`;
  if (name.includes(":")) return 'name: must not contain ":" (plugin-scoped identifiers reserve it)';
  if (name.startsWith("-")) return 'name: must not start with "-"';
  return void 0;
}
__name(validateAgentName, "validateAgentName");
function fallbackBody(source) {
  const text = source.startsWith("\uFEFF") ? source.slice(1) : source;
  const lines = text.split("\n");
  if (lines[0]?.trim() !== "---") return text.trim();
  for (let index = 1; index < lines.length; index += 1) {
    if (lines[index].trim() === "---") return lines.slice(index + 1).join("\n").trim();
  }
  return text.trim();
}
__name(fallbackBody, "fallbackBody");
function fallbackDescription(body, name) {
  const line = body.split("\n").map((part) => part.trim()).find((part) => part.length > 0);
  return line !== void 0 ? line.slice(0, 200) : `Plugin agent ${name}`;
}
__name(fallbackDescription, "fallbackDescription");
function pluginFallbackDefinition(body, path, options) {
  const raw = options.fallbackName !== void 0 && options.fallbackName.length > 0 ? options.fallbackName : "agent";
  const name = `${options.scopedPrefix ?? ""}${raw}`;
  const definition = {
    name,
    description: fallbackDescription(body, name),
    prompt: body,
    scope: "plugin",
    sourcePath: path
  };
  if (options.pluginName !== void 0) definition.plugin = options.pluginName;
  return definition;
}
__name(pluginFallbackDefinition, "pluginFallbackDefinition");
function agentDefinitionFromSource(source, path, scope, options = {}) {
  const frontmatter = parseFrontmatter(source);
  if (frontmatter.ok === false) {
    if (options.plugin !== true) return frontmatter;
    return {
      ok: true,
      definition: pluginFallbackDefinition(fallbackBody(source), path, options),
      warning: `frontmatter: ${frontmatter.reason}; loaded under its filename`
    };
  }
  const fields = frontmatter.fields;
  const fallbackDefinition = /* @__PURE__ */ __name((warning) => {
    const definition2 = pluginFallbackDefinition(frontmatter.body, path, options);
    const parsedDescription = stringField(fields.description);
    if (parsedDescription !== void 0) definition2.description = parsedDescription;
    return { ok: true, definition: definition2, warning };
  }, "fallbackDefinition");
  const rawName = stringField(fields.name) ?? "";
  if (rawName.length === 0) {
    if (options.plugin !== true) return { ok: false, reason: "name: required" };
    return fallbackDefinition("name: required; loaded under its filename");
  }
  const nameError = validateAgentName(rawName);
  if (nameError !== void 0) {
    if (options.plugin !== true) return { ok: false, reason: nameError };
    return fallbackDefinition(`${nameError}; loaded under its filename`);
  }
  const name = options.scopedPrefix !== void 0 ? `${options.scopedPrefix}${rawName}` : rawName;
  const description = stringField(fields.description);
  if (description === void 0) return { ok: false, reason: "description: required" };
  const definition = {
    name,
    description,
    prompt: frontmatter.body,
    scope,
    sourcePath: path
  };
  if (options.pluginName !== void 0) definition.plugin = options.pluginName;
  const tools = listField(fields.tools);
  const disallowed = listField(fields.disallowedTools);
  const skills = listField(fields.skills);
  if (tools !== void 0) definition.tools = tools;
  if (disallowed !== void 0) definition.disallowedTools = disallowed;
  const model = stringField(fields.model);
  const permissionMode = stringField(fields.permissionMode);
  const maxTurns = numberField(fields.maxTurns);
  const isolation = stringField(fields.isolation);
  const color = stringField(fields.color);
  const effort = stringField(fields.effort);
  if (model !== void 0) definition.model = model;
  if (permissionMode !== void 0 && options.plugin !== true) definition.permissionMode = permissionMode;
  if (maxTurns !== void 0) definition.maxTurns = maxTurns;
  if (skills !== void 0) definition.skills = skills;
  if (isolation !== void 0) definition.isolation = isolation;
  if (color !== void 0) definition.color = color;
  if (effort !== void 0) definition.effort = effort;
  const background = booleanField(fields.background);
  const omitClaudeMd = booleanField(fields.omitClaudeMd);
  const memory = typeof fields.memory === "string" ? MEMORY_SCOPES[fields.memory] : void 0;
  if (background !== void 0) definition.background = background;
  if (omitClaudeMd !== void 0) definition.omitClaudeMd = omitClaudeMd;
  if (memory !== void 0) definition.memory = memory;
  if (options.plugin !== true) {
    const mcpServers = parseMcpServers(fields.mcpServers);
    const hooks = parseHooks(fields.hooks);
    const initialPrompt = stringField(fields.initialPrompt);
    if (mcpServers !== void 0) definition.mcpServers = mcpServers;
    if (hooks !== void 0) definition.hooks = hooks;
    if (initialPrompt !== void 0) definition.initialPrompt = initialPrompt;
  }
  const autoCompactWindow = numberField(fields.autoCompactWindow);
  const cacheTtl = parseCacheTtl(fields.experimental);
  if (autoCompactWindow !== void 0) definition.autoCompactWindow = autoCompactWindow;
  if (cacheTtl !== void 0) definition.cacheTtl = cacheTtl;
  return { ok: true, definition };
}
__name(agentDefinitionFromSource, "agentDefinitionFromSource");
var BUILTIN_AGENT_DEFINITIONS = [
  {
    name: "explore",
    description: "Read-only codebase exploration: find files, symbols, and call paths.",
    prompt: "You are a read-only codebase explorer. Find the files, symbols, and call paths that answer the question. Do not edit anything. Report concrete paths with line numbers and a short summary of the call flow. Be quick by default; go deeper only when the task needs it.",
    scope: "builtin",
    sourcePath: "",
    tools: ["Read", "Grep", "Glob"],
    model: "inherit",
    permissionMode: "plan",
    readOnly: true,
    oneShot: true
  },
  {
    name: "plan",
    description: "Read-only design: turn findings into an implementation plan.",
    prompt: "You are a planning agent. Explore the codebase read-only, identify the constraints, then return a concrete implementation plan: the files to change, the ordered steps, the risks, and the tests. Do not implement.",
    scope: "builtin",
    sourcePath: "",
    tools: ["Read", "Grep", "Glob"],
    model: "inherit",
    permissionMode: "plan",
    readOnly: true,
    oneShot: true
  },
  {
    name: "general-purpose",
    description: "General-purpose agent for multi-step tasks. The fallback when no subagent_type is given.",
    prompt: "You are a general-purpose autonomous agent. You receive one self-contained task, complete it fully, and report back once at the end. Work autonomously: you cannot ask follow-up questions, so make reasonable assumptions and state them in the final report. Use the minimum set of tool calls that gets the job done correctly.",
    scope: "builtin",
    sourcePath: ""
  },
  {
    name: "claude",
    description: "Catch-all coding agent for a task that fits no specialized definition.",
    prompt: "You are a general-purpose coding agent. Complete the assigned task end to end, then report what changed and how it was verified.",
    scope: "builtin",
    sourcePath: ""
  },
  {
    name: "statusline-setup",
    description: "Configure the status line. Used by /statusline; restricted to settings files.",
    prompt: "You configure the Command Code status line. Read the current settings, change only the statusLine entry, and report the file you edited. Do not run shell commands.",
    scope: "builtin",
    sourcePath: "",
    model: "sonnet",
    tools: ["Read", "Edit"]
  },
  {
    name: "claude-code-guide",
    description: "Answer questions about the host's features, commands, and configuration.",
    prompt: "You answer questions about Command Code: agents, tools, hooks, permissions, MCP, and settings. Read the local documentation first, then the official pages when the local copy is silent. Answer with the exact flag or field name.",
    scope: "builtin",
    sourcePath: "",
    model: "haiku",
    tools: ["Read", "Grep", "Glob", "WebFetch", "WebSearch"]
  },
  {
    name: FORK_AGENT_TYPE,
    description: "Fork the current conversation: full history, the main system prompt, tools, model, permissions, and prompt cache. Use it when the task needs the context already in this session.",
    prompt: "",
    scope: "builtin",
    sourcePath: "",
    fork: true
  }
];
function builtinAgentDefinitions(env) {
  if (env.CLAUDE_AGENT_SDK_DISABLE_BUILTIN_AGENTS === "1" || env.BETTER_SUBAGENTS_DISABLE_BUILTIN_AGENTS === "1" || env.BETTER_SUBAGENTS_DISABLE_BUILTINS === "1") {
    return [];
  }
  if (env.CLAUDE_CODE_DISABLE_EXPLORE_PLAN_AGENTS === "1" || env.BETTER_SUBAGENTS_DISABLE_EXPLORE_PLAN === "1") {
    return BUILTIN_AGENT_DEFINITIONS.filter((definition) => definition.name !== "explore" && definition.name !== "plan");
  }
  return [...BUILTIN_AGENT_DEFINITIONS];
}
__name(builtinAgentDefinitions, "builtinAgentDefinitions");
var DESCRIPTION_LIMIT_TOKENS = 15e3;
function descriptionLimit(definitions, maxTokens = DESCRIPTION_LIMIT_TOKENS) {
  let characters = 0;
  for (const definition of definitions) {
    if (definition.scope === "builtin") continue;
    characters += String(definition.description ?? "").length;
  }
  const tokens = Math.ceil(characters / 4);
  return { tokens, maxTokens, over: tokens > maxTokens };
}
__name(descriptionLimit, "descriptionLimit");
function cliAgentDefinition(rawName, spec, sourcePath) {
  const name = rawName.length > 0 ? rawName : stringField(spec.name) ?? "";
  if (name.length === 0) return { ok: false, reason: "name: required" };
  const nameError = validateAgentName(name);
  if (nameError !== void 0) return { ok: false, reason: nameError };
  const description = stringField(spec.description);
  if (description === void 0) return { ok: false, reason: "description: required" };
  const prompt = typeof spec.prompt === "string" ? spec.prompt : typeof spec.systemPrompt === "string" ? spec.systemPrompt : "";
  const definition = { name, description, prompt, scope: "cli", sourcePath };
  const tools = listField(spec.tools);
  const disallowed = listField(spec.disallowedTools);
  const skills = listField(spec.skills);
  if (tools !== void 0) definition.tools = tools;
  if (disallowed !== void 0) definition.disallowedTools = disallowed;
  if (skills !== void 0) definition.skills = skills;
  const model = stringField(spec.model);
  const permissionMode = stringField(spec.permissionMode);
  const maxTurns = numberField(spec.maxTurns);
  const isolation = stringField(spec.isolation);
  const effort = stringField(spec.effort);
  if (model !== void 0) definition.model = model;
  if (permissionMode !== void 0) definition.permissionMode = permissionMode;
  if (maxTurns !== void 0) definition.maxTurns = maxTurns;
  if (isolation !== void 0) definition.isolation = isolation;
  if (effort !== void 0) definition.effort = effort;
  const background = booleanField(spec.background);
  const omitClaudeMd = booleanField(spec.omitClaudeMd);
  const memory = typeof spec.memory === "string" ? MEMORY_SCOPES[spec.memory] : void 0;
  if (background !== void 0) definition.background = background;
  if (omitClaudeMd !== void 0) definition.omitClaudeMd = omitClaudeMd;
  if (memory !== void 0) definition.memory = memory;
  const mcpServers = parseMcpServers(spec.mcpServers);
  const hooks = parseHooks(spec.hooks);
  const initialPrompt = stringField(spec.initialPrompt);
  const autoCompactWindow = numberField(spec.autoCompactWindow);
  if (mcpServers !== void 0) definition.mcpServers = mcpServers;
  if (hooks !== void 0) definition.hooks = hooks;
  if (initialPrompt !== void 0) definition.initialPrompt = initialPrompt;
  if (autoCompactWindow !== void 0) definition.autoCompactWindow = autoCompactWindow;
  return { ok: true, definition };
}
__name(cliAgentDefinition, "cliAgentDefinition");
function hasGitMarker(fs, dir) {
  try {
    fs.stat(join2(dir, ".git"));
    return true;
  } catch {
    return false;
  }
}
__name(hasGitMarker, "hasGitMarker");
function createAgentRegistry(deps) {
  let entries = [];
  const loggedSkips = /* @__PURE__ */ new Set();
  const definitionWarnings = /* @__PURE__ */ new WeakMap();
  const managedDir = /* @__PURE__ */ __name(() => deps.managedDir ?? join2(deps.home, ".commandcode", "managed"), "managedDir");
  function pushEntry(entry) {
    entries.push(entry);
    const problem = entry.warning ?? entry.reason;
    if (problem === void 0) return;
    const key = `${entry.sourcePath}\0${problem}`;
    if (loggedSkips.has(key)) return;
    loggedSkips.add(key);
    deps.log(`${entry.sourcePath.length > 0 ? entry.sourcePath : entry.name}: ${problem}`);
  }
  __name(pushEntry, "pushEntry");
  function scanFile(path, scope, options = {}) {
    let source = "";
    try {
      source = deps.fs.readFile(path);
    } catch (error) {
      pushEntry({
        name: "",
        scope,
        sourcePath: path,
        valid: false,
        kind: "invalid",
        reason: `unreadable: ${String(error?.message ?? error)}`
      });
      return void 0;
    }
    const parsed = agentDefinitionFromSource(source, path, scope, options);
    if (parsed.ok === false) {
      pushEntry({ name: "", scope, sourcePath: path, valid: false, kind: "invalid", reason: parsed.reason });
      return void 0;
    }
    if (parsed.warning !== void 0) definitionWarnings.set(parsed.definition, parsed.warning);
    return parsed.definition;
  }
  __name(scanFile, "scanFile");
  function pushDuplicate(name, scope, path, kept) {
    pushEntry({
      name,
      scope,
      sourcePath: path,
      valid: false,
      kind: "duplicate",
      duplicateOf: kept,
      reason: `duplicate name "${name}": ${kept} was read first; rename or remove this file`
    });
  }
  __name(pushDuplicate, "pushDuplicate");
  function scanDir(dir, scope, depth, found, localNames) {
    if (depth > AGENT_SCAN_DEPTH_MAX || entries.length >= AGENT_SCAN_MAX || !deps.fs.exists(dir)) return;
    let names = [];
    try {
      names = deps.fs.readdir(dir).sort();
    } catch {
      return;
    }
    for (const name of names) {
      if (entries.length >= AGENT_SCAN_MAX) return;
      const full = join2(dir, name);
      if (name.toLowerCase().endsWith(".md")) {
        const definition = scanFile(full, scope);
        if (definition === void 0) continue;
        const kept = localNames.get(definition.name);
        if (kept !== void 0) {
          pushDuplicate(definition.name, scope, full, kept);
          continue;
        }
        localNames.set(definition.name, full);
        found.push(definition);
        continue;
      }
      if (deps.fs.exists(full)) scanDir(full, scope, depth + 1, found, localNames);
    }
  }
  __name(scanDir, "scanDir");
  function scanPluginDir(dir, pluginName, trail, depth, found, localNames) {
    if (depth > AGENT_SCAN_DEPTH_MAX || entries.length >= AGENT_SCAN_MAX || !deps.fs.exists(dir)) return;
    let children = [];
    try {
      children = deps.fs.readdir(dir).sort();
    } catch {
      return;
    }
    for (const child of children) {
      if (entries.length >= AGENT_SCAN_MAX) return;
      const full = join2(dir, child);
      if (child.toLowerCase().endsWith(".md")) {
        const definition = scanFile(full, "plugin", {
          plugin: true,
          pluginName,
          scopedPrefix: `${[pluginName, ...trail].join(":")}:`,
          fallbackName: child.replace(/\.md$/i, "")
        });
        if (definition === void 0) continue;
        const kept = localNames.get(definition.name);
        if (kept !== void 0) {
          pushDuplicate(definition.name, "plugin", full, kept);
          continue;
        }
        localNames.set(definition.name, full);
        found.push(definition);
        continue;
      }
      scanPluginDir(full, pluginName, [...trail, child], depth + 1, found, localNames);
    }
  }
  __name(scanPluginDir, "scanPluginDir");
  function scanPlugins(root, found) {
    if (!deps.fs.exists(root)) return;
    let pluginNames = [];
    try {
      pluginNames = deps.fs.readdir(root).sort();
    } catch {
      return;
    }
    for (const pluginName of pluginNames) {
      const agentsDir = join2(root, pluginName, "agents");
      if (deps.fs.exists(agentsDir)) scanPluginDir(agentsDir, pluginName, [], 0, found, /* @__PURE__ */ new Map());
    }
  }
  __name(scanPlugins, "scanPlugins");
  function projectBases() {
    const bases = [];
    let current = deps.cwd;
    for (let depth = 0; depth <= 64; depth += 1) {
      bases.push(current);
      if (hasGitMarker(deps.fs, current)) break;
      const parent = dirname(current);
      if (parent === current) break;
      current = parent;
    }
    return bases;
  }
  __name(projectBases, "projectBases");
  function pluginRoots() {
    const roots = [...deps.pluginRoots ?? []];
    roots.push(join2(managedDir(), "plugins"));
    for (const base of projectBases()) roots.push(join2(base, ".commandcode", "plugins"));
    roots.push(join2(deps.home, ".commandcode", "plugins"));
    return roots;
  }
  __name(pluginRoots, "pluginRoots");
  function scanCli(found) {
    const value = (deps.cli?.value ?? "").trim();
    if (value.length === 0) return;
    let text = value;
    let sourcePath = "cli";
    if (!value.startsWith("{") && !value.startsWith("[")) {
      const file = value.startsWith("@") ? value.slice(1) : value;
      if (deps.cli?.interactive === true) {
        pushEntry({
          name: "",
          scope: "cli",
          sourcePath: file,
          valid: false,
          kind: "invalid",
          reason: "an interactive session cannot load --mod-option agents from a file; pass inline JSON"
        });
        return;
      }
      try {
        text = deps.fs.readFile(file);
      } catch (error) {
        pushEntry({
          name: "",
          scope: "cli",
          sourcePath: file,
          valid: false,
          kind: "invalid",
          reason: `unreadable: ${String(error?.message ?? error)}`
        });
        return;
      }
      sourcePath = file;
    }
    let parsed;
    try {
      parsed = JSON.parse(text);
    } catch (error) {
      pushEntry({
        name: "",
        scope: "cli",
        sourcePath,
        valid: false,
        kind: "invalid",
        reason: `agents JSON: ${String(error?.message ?? error)}`
      });
      return;
    }
    const specs = [];
    if (Array.isArray(parsed)) {
      for (const item of parsed) {
        if (item === null || typeof item !== "object" || Array.isArray(item)) {
          pushEntry({
            name: "",
            scope: "cli",
            sourcePath,
            valid: false,
            kind: "invalid",
            reason: "agents JSON: every array item must be an object"
          });
          continue;
        }
        const record = item;
        specs.push({ name: typeof record.name === "string" ? record.name : "", spec: record });
      }
    } else if (parsed !== null && typeof parsed === "object") {
      for (const [name, item] of Object.entries(parsed)) {
        if (item === null || typeof item !== "object" || Array.isArray(item)) {
          pushEntry({
            name,
            scope: "cli",
            sourcePath,
            valid: false,
            kind: "invalid",
            reason: "agents JSON: every definition must be an object"
          });
          continue;
        }
        specs.push({ name, spec: item });
      }
    } else {
      pushEntry({
        name: "",
        scope: "cli",
        sourcePath,
        valid: false,
        kind: "invalid",
        reason: "agents JSON: expected an object of definitions or an array"
      });
      return;
    }
    const localNames = /* @__PURE__ */ new Map();
    for (const { name, spec } of specs) {
      const parsedDefinition = cliAgentDefinition(name, spec, sourcePath);
      if (parsedDefinition.ok === false) {
        pushEntry({
          name,
          scope: "cli",
          sourcePath,
          valid: false,
          kind: "invalid",
          reason: parsedDefinition.reason
        });
        continue;
      }
      const kept = localNames.get(parsedDefinition.definition.name);
      if (kept !== void 0) {
        pushDuplicate(parsedDefinition.definition.name, "cli", sourcePath, kept);
        continue;
      }
      localNames.set(parsedDefinition.definition.name, sourcePath);
      found.push(parsedDefinition.definition);
    }
  }
  __name(scanCli, "scanCli");
  function refresh() {
    entries = [];
    const found = [];
    scanDir(join2(managedDir(), "agents"), "managed", 0, found, /* @__PURE__ */ new Map());
    scanCli(found);
    for (const base of projectBases()) {
      scanDir(join2(base, ".commandcode", "agents"), "project", 0, found, /* @__PURE__ */ new Map());
    }
    for (const dir of deps.addDirs ?? []) {
      scanDir(join2(dir, ".commandcode", "agents"), "project", 0, found, /* @__PURE__ */ new Map());
    }
    scanDir(join2(deps.home, ".commandcode", "agents"), "user", 0, found, /* @__PURE__ */ new Map());
    for (const root of pluginRoots()) scanPlugins(root, found);
    found.push(...deps.builtins);
    const seen = /* @__PURE__ */ new Set();
    for (const definition of found) {
      if (seen.has(definition.name)) continue;
      seen.add(definition.name);
      const warning = definitionWarnings.get(definition);
      entries.push({
        name: definition.name,
        scope: definition.scope,
        sourcePath: definition.sourcePath ?? "",
        valid: true,
        kind: "valid",
        ...warning !== void 0 ? { warning } : {},
        definition
      });
    }
  }
  __name(refresh, "refresh");
  refresh();
  return {
    entries: /* @__PURE__ */ __name(() => [...entries], "entries"),
    list: /* @__PURE__ */ __name(() => entries.flatMap((entry) => entry.valid && entry.definition !== void 0 ? [entry.definition] : []), "list"),
    get: /* @__PURE__ */ __name((name) => entries.find((entry) => entry.valid && entry.definition?.name === name)?.definition, "get"),
    refresh
  };
}
__name(createAgentRegistry, "createAgentRegistry");

// src/state.ts
var JOB_STATES = ["queued", "running", "waiting-input", "done", "failed", "canceled"];
var STATE_ALIASES = {
  completed: "done",
  "agent-done": "done",
  "command-done": "done"
};
var STATE_TRANSITIONS = {
  queued: ["running", "failed", "canceled"],
  running: ["waiting-input", "done", "failed", "canceled"],
  "waiting-input": [],
  done: [],
  failed: [],
  canceled: []
};
var STATE_MARKERS = {
  queued: "\u25CC",
  running: "\u25CF",
  "waiting-input": "?",
  done: "\u2713",
  failed: "\u2717",
  canceled: "\u2298",
  note: "\u25C8"
};
var STATE_COLORS = {
  queued: "gray",
  running: "cyan",
  "waiting-input": "yellow",
  done: "green",
  failed: "red",
  canceled: "gray",
  note: "gray"
};
function isState(value) {
  return typeof value === "string" && JOB_STATES.includes(value);
}
__name(isState, "isState");
function normalizeState(value) {
  if (typeof value !== "string" || value.length === 0) return void 0;
  const alias = STATE_ALIASES[value];
  if (alias !== void 0) return alias;
  return isState(value) ? value : void 0;
}
__name(normalizeState, "normalizeState");
function isSignalState(value) {
  return value === "note" || isState(value);
}
__name(isSignalState, "isSignalState");
function canTransition(from, to) {
  if (from === to) return true;
  return STATE_TRANSITIONS[from].includes(to);
}
__name(canTransition, "canTransition");
function isTerminal(state) {
  return STATE_TRANSITIONS[state].length === 0;
}
__name(isTerminal, "isTerminal");
function isTerminalState(value) {
  const state = normalizeState(value);
  return state !== void 0 && isTerminal(state);
}
__name(isTerminalState, "isTerminalState");
function canonicalState(value) {
  return normalizeState(value) ?? String(value ?? "unknown");
}
__name(canonicalState, "canonicalState");
function stateMarker(state) {
  return isSignalState(state) ? STATE_MARKERS[state] : "\u26A0";
}
__name(stateMarker, "stateMarker");
function stateColor(state) {
  return isSignalState(state) ? STATE_COLORS[state] : "red";
}
__name(stateColor, "stateColor");
var jobStates = {
  list: JOB_STATES,
  isState,
  normalize: normalizeState,
  canTransition,
  isTerminal,
  marker: stateMarker,
  color: stateColor
};

// src/append.ts
import { isAbsolute, join as join3 } from "node:path";
var AUTO_COMPACT_MIN = 1e5;
var AUTO_COMPACT_MAX = 1e6;
function resolveAutoCompactWindow(inherited, requested, isMain) {
  if (isMain || requested === void 0 || !Number.isFinite(requested)) return void 0;
  const bounded = Math.min(AUTO_COMPACT_MAX, Math.max(AUTO_COMPACT_MIN, Math.round(requested)));
  return Math.min(inherited, bounded);
}
__name(resolveAutoCompactWindow, "resolveAutoCompactWindow");
function resolveAppendSystemPrompt(input) {
  const env = input.env ?? {};
  const file = (input.file || env.BETTER_SUBAGENTS_APPEND_SYSTEM_PROMPT_FILE || env.CLAUDE_CODE_APPEND_SUBAGENT_SYSTEM_PROMPT_FILE || "").trim();
  const value = (input.flag || env.BETTER_SUBAGENTS_APPEND_SYSTEM_PROMPT || env.CLAUDE_CODE_APPEND_SUBAGENT_SYSTEM_PROMPT || "").trim();
  const read = /* @__PURE__ */ __name((path) => {
    try {
      return input.fs.readFile(isAbsolute(path) ? path : join3(input.cwd, path)).trim();
    } catch {
      return "";
    }
  }, "read");
  const parts = [];
  if (file.length > 0) {
    const text = read(file);
    if (text.length > 0) parts.push(text);
  }
  if (value.startsWith("@")) {
    const text = read(value.slice(1).trim());
    if (text.length > 0) parts.push(text);
  } else if (value.length > 0) {
    parts.push(value);
  }
  return parts.join("\n\n");
}
__name(resolveAppendSystemPrompt, "resolveAppendSystemPrompt");

// src/jobs.ts
import { join as join4 } from "node:path";
function createJobStore(deps) {
  function readJob(id) {
    try {
      const parsed = JSON.parse(deps.fs.readFile(join4(deps.jobsDir, `${id}.json`)));
      return parsed && typeof parsed.id === "string" ? parsed : void 0;
    } catch {
      return void 0;
    }
  }
  __name(readJob, "readJob");
  function writeJob(record) {
    try {
      const file = join4(deps.jobsDir, `${record.id}.json`);
      const tmp = `${file}.mod-${deps.pid}-${deps.now()}`;
      deps.fs.writeFile(tmp, JSON.stringify(record, null, 2));
      deps.fs.rename(tmp, file);
    } catch {
    }
  }
  __name(writeJob, "writeJob");
  function readRecentJobs(limit) {
    let names;
    try {
      names = deps.fs.readdir(deps.jobsDir).filter((name) => name.endsWith(".json") && !name.includes(".tmp-"));
    } catch {
      return [];
    }
    names.sort();
    const jobs = [];
    for (const name of names.slice(-limit).reverse()) {
      try {
        jobs.push(JSON.parse(deps.fs.readFile(join4(deps.jobsDir, name))));
      } catch {
      }
    }
    return jobs;
  }
  __name(readRecentJobs, "readRecentJobs");
  return { readJob, writeJob, readRecentJobs };
}
__name(createJobStore, "createJobStore");

// src/ledger.ts
function createLedger(deps) {
  const ids = /* @__PURE__ */ new Map();
  const ordered = /* @__PURE__ */ __name(() => [...ids.entries()].sort((left, right) => left[1] - right[1]), "ordered");
  function write() {
    const body = ordered().map(([id, at]) => JSON.stringify({ id, at })).join("\n");
    try {
      deps.fs.writeFile(deps.path, body.length > 0 ? `${body}
` : "");
    } catch {
    }
  }
  __name(write, "write");
  function retain(limit) {
    const keep = ordered().slice(-Math.max(0, limit));
    ids.clear();
    for (const [id, at] of keep) ids.set(id, at);
    write();
  }
  __name(retain, "retain");
  try {
    for (const line of deps.fs.readFile(deps.path).split("\n").filter(Boolean).slice(-deps.max)) {
      try {
        const entry = JSON.parse(line);
        if (typeof entry.id === "string") ids.set(entry.id, typeof entry.at === "number" ? entry.at : 0);
      } catch {
      }
    }
  } catch {
  }
  try {
    if (deps.fs.stat(deps.path).size > deps.compactBytes) retain(deps.keep);
  } catch {
  }
  return {
    has: /* @__PURE__ */ __name((id) => ids.has(id), "has"),
    accept: /* @__PURE__ */ __name((id) => {
      if (ids.has(id)) return;
      const at = deps.now();
      ids.set(id, at);
      try {
        deps.fs.appendFile(deps.path, JSON.stringify({ id, at }) + "\n");
      } catch {
      }
      if (ids.size > deps.max) retain(deps.keep);
    }, "accept"),
    retain,
    size: /* @__PURE__ */ __name(() => ids.size, "size")
  };
}
__name(createLedger, "createLedger");
var ledgerFactory = createLedger;

// src/delivery.ts
import { join as join5 } from "node:path";
function createDelivery(deps) {
  const answeredPermissions = /* @__PURE__ */ new Set();
  let draining = false;
  let lastJobsSignature = "";
  function markDelivered(id) {
    const job = deps.readJob(id);
    if (job === void 0) return;
    job.deliveredAt = deps.now();
    job.updatedAt = job.deliveredAt;
    deps.writeJob(job);
  }
  __name(markDelivered, "markDelivered");
  function formatSignalMessage(message) {
    const state = canonicalState(message.state ?? message.kind ?? "signal");
    const worktree = deps.finalizeWorktree(deps.readJob(String(message.id ?? "")));
    const kept = worktree?.kept === true ? `
worktree kept at ${worktree.path}${worktree.error === void 0 ? "" : ` (${worktree.error})`}` : "";
    const facts = [
      `job ${String(message.id ?? "unknown")}`,
      typeof message.agentId === "string" ? `agent ${message.agentId}` : "",
      String(message.kind ?? "signal"),
      state,
      message.partial === true ? "partial" : "",
      message.exitCode === null || message.exitCode === void 0 ? "" : `exit ${message.exitCode}`,
      typeof message.durationMs === "number" ? `${(message.durationMs / 1e3).toFixed(1)}s` : ""
    ].filter(Boolean).join(" \xB7 ");
    const log = typeof message.logPath === "string" ? `
log ${message.logPath}` : "";
    const boundary = message.compactBoundary === void 0 ? "" : `
compact_boundary: ${message.compactBoundary.trigger} at ${message.compactBoundary.preTokens} tokens`;
    const body = typeof message.body === "string" ? message.body.trim() : "";
    const title = String(message.title ?? message.description ?? message.id ?? "(signal)");
    const request = message.permissionRequest;
    const options = request === void 0 ? "" : request.options.map((option) => option.name ?? option.kind ?? option.optionId).filter(Boolean).join(", ");
    const ask = state === "waiting-input" ? request !== void 0 ? `

Approval required: ${request.title}` + (options.length > 0 ? `
options: ${options}` : "") : typeof message.question === "string" && message.question.length > 0 ? `

${message.question}` : "" : "";
    const next2 = state === "waiting-input" ? typeof message.agentId === "string" ? `The job is resumable: subagent_resume {id: "${message.agentId}", prompt} continues it. Nothing else arrives until then.` : "The job is not resumable (no agent id); fire a follow-up job. Nothing else arrives until then." : state === "canceled" ? "The job was canceled; nothing else will arrive for it." : message.partial === true && typeof message.agentId === "string" ? `The result is partial: resume it with subagent_resume ${message.agentId}.` : "This result arrived automatically. Read it now and use it in your current work; do not poll for it and do not wait for it.";
    return `[subagent] ${title}
${facts}${log}${boundary}${kept}` + (body.length > 0 ? `

${body}` : "") + ask + `

${next2}`;
  }
  __name(formatSignalMessage, "formatSignalMessage");
  function deliver(message) {
    deps.sendUserMessage(formatSignalMessage(message), { deliverAs: deps.deliverAs() });
    deps.showEntry("subagent", message);
  }
  __name(deliver, "deliver");
  function inboxNames() {
    try {
      return deps.fs.readdir(deps.inboxDir).filter((name) => name.endsWith(".json"));
    } catch {
      return [];
    }
  }
  __name(inboxNames, "inboxNames");
  function next(seen) {
    for (const name of inboxNames()) {
      if (seen.has(name)) continue;
      seen.add(name);
      const live = join5(deps.inboxDir, name);
      const claimed = join5(deps.claimedDir, name);
      try {
        deps.fs.rename(live, claimed);
      } catch {
        continue;
      }
      let signal;
      try {
        signal = JSON.parse(deps.fs.readFile(claimed));
      } catch {
        signal = void 0;
      }
      if (signal === void 0 || typeof signal.id !== "string" || signal.id.length === 0) {
        deps.fs.remove(claimed);
        continue;
      }
      const claim = { name, signal, live, claimed };
      if (deps.ledger.has(signal.id)) {
        settle(claim, "accepted");
        continue;
      }
      const owner = deps.ownerVerdict(signal.owner);
      if (owner === "orphan") {
        settle(claim, "quarantined");
        continue;
      }
      if (owner !== "mine") {
        settle(claim, "held");
        continue;
      }
      const settledJob = deps.readJob(signal.id);
      if (settledJob !== void 0 && settledJob.background === false) {
        markDelivered(signal.id);
        settle(claim, "accepted");
        continue;
      }
      return claim;
    }
    return void 0;
  }
  __name(next, "next");
  function settle(claim, outcome) {
    if (outcome === "accepted") {
      deps.ledger.accept(claim.signal.id);
      deps.fs.remove(claim.claimed);
      return;
    }
    if (outcome === "quarantined") {
      try {
        deps.fs.rename(
          claim.claimed,
          join5(deps.orphanedDir, `${deps.now()}-${Math.random().toString(36).slice(2, 6)}-${claim.name}`)
        );
      } catch {
      }
      return;
    }
    try {
      deps.fs.rename(claim.claimed, claim.live);
    } catch {
      try {
        deps.fs.writeFile(claim.live, JSON.stringify(claim.signal, null, 2));
        deps.fs.remove(claim.claimed);
      } catch {
      }
    }
  }
  __name(settle, "settle");
  function answerLivePermissions() {
    for (const job of deps.jobs()) {
      if (job.live !== true) continue;
      void deps.answerPendingPermission(join5(deps.controlRoot, job.id), answeredPermissions);
    }
  }
  __name(answerLivePermissions, "answerLivePermissions");
  function drain() {
    if (draining) return;
    draining = true;
    const seen = /* @__PURE__ */ new Set();
    try {
      answerLivePermissions();
      for (let handled = 0; handled < FILES_PER_DRAIN_MAX; handled += 1) {
        const claim = next(seen);
        if (claim === void 0) return;
        if (!deps.hasSession()) {
          settle(claim, "held");
          continue;
        }
        try {
          deliver(claim.signal);
        } catch {
          settle(claim, "held");
          continue;
        }
        markDelivered(claim.signal.id);
        settle(claim, "accepted");
      }
    } finally {
      draining = false;
    }
  }
  __name(drain, "drain");
  const safeDrain = /* @__PURE__ */ __name(() => {
    try {
      drain();
    } catch {
    }
  }, "safeDrain");
  function claimPending(limit) {
    const seen = /* @__PURE__ */ new Set();
    const claims = [];
    while (claims.length < limit) {
      const claim = next(seen);
      if (claim === void 0) break;
      claims.push(claim);
    }
    return claims;
  }
  __name(claimPending, "claimPending");
  function countPending() {
    const seen = /* @__PURE__ */ new Set();
    let count = 0;
    while (count < PENDING_SCAN_MAX) {
      const claim = next(seen);
      if (claim === void 0) break;
      count += 1;
      settle(claim, "held");
    }
    return count;
  }
  __name(countPending, "countPending");
  function countOrphaned() {
    try {
      return deps.fs.readdir(deps.orphanedDir).length;
    } catch {
      return 0;
    }
  }
  __name(countOrphaned, "countOrphaned");
  function refreshJobs() {
    const jobs = deps.readRecentJobs(LIST_SCAN_MAX);
    deps.setJobs(jobs);
    deps.sweepSubagentStopHooks();
    deps.updateStatus();
    const signature = jobs.map((job) => `${job.id}:${canonicalState(job.state)}`).join("|");
    if (signature !== lastJobsSignature) {
      lastJobsSignature = signature;
      deps.markJobsTick();
    }
  }
  __name(refreshJobs, "refreshJobs");
  function sweepClaimed() {
    deps.sweep(deps.claimedDir, CLAIMED_TTL_MS);
  }
  __name(sweepClaimed, "sweepClaimed");
  function sweepOrphaned() {
    if (deps.workerMode) return;
    deps.sweep(deps.orphanedDir, ORPHANED_TTL_MS);
  }
  __name(sweepOrphaned, "sweepOrphaned");
  return {
    drain,
    safeDrain,
    claimPending,
    settle,
    markDelivered,
    formatSignalMessage,
    countPending,
    countOrphaned,
    refreshJobs,
    sweepClaimed,
    sweepOrphaned
  };
}
__name(createDelivery, "createDelivery");

// src/signals.ts
function signalOf(record, result) {
  return signalEnvelope(record, { ...result, state: canonicalState(result.state) });
}
__name(signalOf, "signalOf");
function signalEnvelope(record, result) {
  const description = String(record.description ?? record.id);
  const state = String(result.state);
  return {
    id: record.id,
    ...record.agentId !== void 0 ? { agentId: record.agentId } : {},
    owner: record.owner,
    description,
    kind: String(record.kind ?? "command"),
    state,
    exitCode: typeof result.exitCode === "number" ? result.exitCode : null,
    startedAt: record.startedAt,
    finishedAt: result.finishedAt,
    durationMs: result.durationMs,
    logPath: record.logPath,
    sessionId: record.sessionId,
    title: result.title ?? `${description} ${state}`,
    body: result.body,
    ...typeof result.question === "string" ? { question: result.question } : {},
    ...result.permissionRequest !== void 0 ? { permissionRequest: result.permissionRequest } : {},
    // Omitted when the writer did not scan, so a legacy signal keeps its shape.
    ...result.scanned === void 0 ? {} : { scanned: result.scanned === true, scanMatches: result.scanMatches ?? [] },
    ...result.partial === true ? { partial: true } : {},
    ...result.compactBoundary !== void 0 ? { compactBoundary: result.compactBoundary } : {}
  };
}
__name(signalEnvelope, "signalEnvelope");
function patchOf(signal) {
  const patch = {
    exitCode: typeof signal.exitCode === "number" ? signal.exitCode : void 0,
    finishedAt: signal.finishedAt,
    durationMs: signal.durationMs
  };
  if (signal.state !== void 0) {
    const state = canonicalState(signal.state);
    patch.state = state;
    if (state === "failed" || state === "canceled") patch.failureReason = signal.body;
    if (state === "waiting-input") patch.awaitingInput = true;
  }
  if (typeof signal.question === "string") patch.question = signal.question;
  if (signal.permissionRequest !== void 0) patch.permissionRequest = signal.permissionRequest;
  if (signal.scanned !== void 0) {
    patch.scanned = signal.scanned;
    patch.scanMatches = signal.scanMatches ?? [];
  }
  if (signal.partial === true) patch.partial = true;
  if (signal.compactBoundary !== void 0) patch.compactBoundary = signal.compactBoundary;
  return patch;
}
__name(patchOf, "patchOf");

// src/tool-filter.ts
var SUBAGENT_TOOL_UNIVERSE = [
  "Agent",
  "Artifact",
  "AskUserQuestion",
  "Bash",
  "Edit",
  "EndConversation",
  "EnterPlanMode",
  "EnterWorktree",
  "ExitPlanMode",
  "ExitWorktree",
  "Glob",
  "Grep",
  "LSP",
  "ListAgents",
  "Monitor",
  "NotebookEdit",
  "PowerShell",
  "Read",
  "ScheduleWakeup",
  "SendMessage",
  "Skill",
  "SubagentHandback",
  "TaskStop",
  "TodoWrite",
  "ToolSearch",
  "WaitForMcpServers",
  "WebFetch",
  "WebSearch",
  "Workflow",
  "Write"
];
var UNIVERSAL_TOOL_REMOVALS = [
  "AskUserQuestion",
  "EndConversation",
  "EnterPlanMode",
  "ScheduleWakeup",
  "WaitForMcpServers",
  "Workflow"
];
var BACKGROUND_BUILTIN_TOOLS = [
  "Read",
  "Grep",
  "Glob",
  "LSP",
  "Bash",
  "PowerShell",
  "Edit",
  "Write",
  "NotebookEdit",
  "WebFetch",
  "WebSearch",
  "TodoWrite",
  "Skill",
  "ToolSearch",
  "EnterWorktree",
  "ExitWorktree",
  "Monitor",
  "TaskStop",
  "SendMessage",
  "Artifact",
  "SubagentHandback"
];
var EFFORT_LEVELS = ["low", "medium", "high", "xhigh", "max"];
var MCP_PREFIX = "mcp__";
function unique(values) {
  const out = [];
  for (const value of values) if (!out.includes(value)) out.push(value);
  return out;
}
__name(unique, "unique");
var isMcpTool = /* @__PURE__ */ __name((name) => name.startsWith(MCP_PREFIX), "isMcpTool");
function toolNameOfSpecifier(specifier) {
  const text = String(specifier ?? "").trim();
  const open = text.indexOf("(");
  return (open >= 0 ? text.slice(0, open) : text).trim();
}
__name(toolNameOfSpecifier, "toolNameOfSpecifier");
function mcpCapability(server) {
  return `${MCP_PREFIX}${server}__*`;
}
__name(mcpCapability, "mcpCapability");
function mcpParts(name) {
  const rest = name.startsWith(MCP_PREFIX) ? name.slice(MCP_PREFIX.length) : name;
  const separator = rest.indexOf("__");
  if (separator < 0) return { server: rest };
  return { server: rest.slice(0, separator), tool: rest.slice(separator + 2) };
}
__name(mcpParts, "mcpParts");
function expandSpecifier(specifier, available, mcpServers) {
  const name = toolNameOfSpecifier(specifier);
  if (name.length === 0) return [];
  const capabilities = mcpServers.map(mcpCapability);
  if (name === "*") return [...available, ...capabilities];
  if (name === `${MCP_PREFIX}*`) return capabilities;
  if (isMcpTool(name)) {
    const { server, tool } = mcpParts(name);
    if (!mcpServers.includes(server)) return [];
    if (tool === void 0 || tool === "*") return [mcpCapability(server)];
    return [`${MCP_PREFIX}${server}__${tool}`];
  }
  return available.includes(name) ? [name] : [];
}
__name(expandSpecifier, "expandSpecifier");
function withheldBy(specifier, capability) {
  const name = toolNameOfSpecifier(specifier);
  if (name.length === 0) return false;
  if (name === "*") return true;
  if (name === `${MCP_PREFIX}*`) return isMcpTool(capability);
  if (isMcpTool(name)) {
    if (!isMcpTool(capability)) return false;
    const wanted = mcpParts(name);
    const actual = mcpParts(capability);
    if (wanted.server !== actual.server) return false;
    if (wanted.tool === void 0 || wanted.tool === "*") return true;
    return actual.tool === wanted.tool;
  }
  return capability === name;
}
__name(withheldBy, "withheldBy");
var WORKER_TOOL_ALIASES = {
  subagent_spawn: "Agent",
  pubsub_fire: "Agent",
  subagent_send: "SendMessage"
};
function workerToolAllowed(toolName, tools, disallowedTools = [], team = false) {
  for (const specifier of disallowedTools) {
    if (team && TEAM_TOOL_NAMES.includes(toolName)) continue;
    if (withheldBy(specifier, toolName)) return false;
  }
  if (tools.includes(toolName)) return true;
  const hostName = WORKER_TOOL_ALIASES[toolName];
  if (hostName !== void 0 && tools.includes(hostName)) return true;
  if (!isMcpTool(toolName)) return false;
  if (tools.includes(`${MCP_PREFIX}*`)) return true;
  return tools.includes(mcpCapability(mcpParts(toolName).server));
}
__name(workerToolAllowed, "workerToolAllowed");
function filterTools(input = {}) {
  const available = unique([
    ...input.available ?? SUBAGENT_TOOL_UNIVERSE,
    ...input.team === true ? TEAM_TOOL_NAMES : []
  ]);
  const mcpServers = unique(input.mcpServers ?? []);
  const requested = input.tools === void 0 || input.tools.length === 0 ? void 0 : input.tools;
  let selected;
  if (requested === void 0) {
    selected = [...available, ...mcpServers.map(mcpCapability)];
  } else {
    selected = [];
    for (const specifier of requested) selected.push(...expandSpecifier(specifier, available, mcpServers));
    selected = unique(selected);
  }
  for (const specifier of input.disallowedTools ?? []) {
    selected = selected.filter((capability) => !withheldBy(specifier, capability));
  }
  const removed = [];
  const kept = selected.filter((capability) => {
    if (isMcpTool(capability)) return true;
    if (UNIVERSAL_TOOL_REMOVALS.includes(capability)) {
      removed.push(capability);
      return false;
    }
    if (capability === "ExitPlanMode" && input.permissionMode !== "plan") {
      removed.push(capability);
      return false;
    }
    if (input.background === true && capability !== "Agent" && capability !== "ExitPlanMode" && !BACKGROUND_BUILTIN_TOOLS.includes(capability)) {
      removed.push(capability);
      return false;
    }
    return true;
  });
  if (input.team === true) {
    for (const tool of TEAM_TOOL_NAMES) if (!kept.includes(tool)) kept.push(tool);
  }
  if (requested !== void 0 && kept.length === 0) {
    return { ok: false, error: `tools list resolved to no available tool: ${requested.join(", ")}` };
  }
  return { ok: true, tools: kept, removed: removed.filter((tool) => !kept.includes(tool)) };
}
__name(filterTools, "filterTools");
function nonEmpty(value) {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : void 0;
}
__name(nonEmpty, "nonEmpty");

// src/config.ts
import { join as join6 } from "node:path";
var SUBAGENT_MODEL_ENV = "BETTER_SUBAGENTS_SUBAGENT_MODEL";
var SUBAGENT_MODEL_ENV_ALIAS = "CLAUDE_CODE_SUBAGENT_MODEL";
var SUBAGENT_MODEL_FORCE_ENV = "BETTER_SUBAGENTS_SUBAGENT_MODEL_FORCE";
var SUBAGENT_MODEL_FORCE_ENV_ALIAS = "CLAUDE_CODE_SUBAGENT_MODEL_FORCE";
var EFFORT_LEVEL_ENV = "BETTER_SUBAGENTS_EFFORT_LEVEL";
var EFFORT_LEVEL_ENV_ALIAS = "CLAUDE_CODE_EFFORT_LEVEL";
var CONTEXT_WINDOW_ENV = "BETTER_SUBAGENTS_CONTEXT_WINDOW";
var CONTEXT_WINDOW_ENV_ALIAS = "CLAUDE_CODE_CONTEXT_WINDOW";
var BARE_ENV = "BETTER_SUBAGENTS_BARE";
var BARE_ENV_ALIAS = "CLAUDE_CODE_BARE";
var STRICT_MCP_CONFIG_ENV = "BETTER_SUBAGENTS_STRICT_MCP_CONFIG";
var STRICT_MCP_CONFIG_ENV_ALIAS = "CLAUDE_CODE_STRICT_MCP_CONFIG";
var MCP_ALLOW_ENV = "BETTER_SUBAGENTS_MCP_ALLOW";
var MCP_ALLOW_ENV_ALIAS = "CLAUDE_CODE_MCP_ALLOW";
var MCP_DENY_ENV = "BETTER_SUBAGENTS_MCP_DENY";
var MCP_DENY_ENV_ALIAS = "CLAUDE_CODE_MCP_DENY";
var MANAGED_CLAUDE_MD_ENV = "BETTER_SUBAGENTS_MANAGED_CLAUDE_MD";
var USER_CLAUDE_MD_ENV = "BETTER_SUBAGENTS_USER_CLAUDE_MD";
var AGENT_ENV = "BETTER_SUBAGENTS_AGENT";
var AGENT_ENV_ALIAS = "CLAUDE_CODE_AGENT";
var CROSS_SESSION_ENV = "BETTER_SUBAGENTS_CROSS_SESSION";
var MAIN_PERMISSION_MODE_ENV = "BETTER_SUBAGENTS_MAIN_PERMISSION_MODE";
var PUB_SUB_WORKER_ENV = "PUBSUB_WORKER";
var COMMANDCODE_SDK_ENV = "COMMANDCODE_SDK";
var MANAGED_DIR_OPTION = "managed_dir";
function aliasValue(env, name, alias) {
  const value = env[name];
  return value !== void 0 ? value : env[alias];
}
__name(aliasValue, "aliasValue");
function commaList(value) {
  return String(value ?? "").split(",").map((item) => item.trim()).filter(Boolean);
}
__name(commaList, "commaList");
function isOn(value) {
  return value === "1" || value === "true";
}
__name(isOn, "isOn");
function subagentModel(env) {
  const primary = env[SUBAGENT_MODEL_ENV]?.trim();
  if (primary !== void 0 && primary.length > 0) return primary;
  const alias = env[SUBAGENT_MODEL_ENV_ALIAS]?.trim();
  return alias !== void 0 && alias.length > 0 ? alias : void 0;
}
__name(subagentModel, "subagentModel");
function subagentModelForced(env) {
  return isOn(env[SUBAGENT_MODEL_FORCE_ENV]) || isOn(env[SUBAGENT_MODEL_FORCE_ENV_ALIAS]);
}
__name(subagentModelForced, "subagentModelForced");
function effortLevel(value, levels) {
  const level = typeof value === "string" ? value.trim().toLowerCase() : "";
  return level.length > 0 && levels.includes(level) ? level : void 0;
}
__name(effortLevel, "effortLevel");
function effortLevelFromEnv(env, levels) {
  return effortLevel(env[EFFORT_LEVEL_ENV], levels) ?? effortLevel(env[EFFORT_LEVEL_ENV_ALIAS], levels);
}
__name(effortLevelFromEnv, "effortLevelFromEnv");
function contextWindowOverride(env) {
  const value = Number(aliasValue(env, CONTEXT_WINDOW_ENV, CONTEXT_WINDOW_ENV_ALIAS));
  return Number.isFinite(value) ? value : void 0;
}
__name(contextWindowOverride, "contextWindowOverride");
function mcpPolicy(env) {
  return {
    bare: isOn(aliasValue(env, BARE_ENV, BARE_ENV_ALIAS)),
    strict: isOn(aliasValue(env, STRICT_MCP_CONFIG_ENV, STRICT_MCP_CONFIG_ENV_ALIAS)),
    allow: commaList(aliasValue(env, MCP_ALLOW_ENV, MCP_ALLOW_ENV_ALIAS)),
    deny: commaList(aliasValue(env, MCP_DENY_ENV, MCP_DENY_ENV_ALIAS))
  };
}
__name(mcpPolicy, "mcpPolicy");
function backgroundDisabled(env) {
  const isOnSwitch = /* @__PURE__ */ __name((value) => {
    if (value === void 0) return false;
    const normalized = value.trim().toLowerCase();
    return normalized.length > 0 && normalized !== "0" && normalized !== "false" && normalized !== "no";
  }, "isOnSwitch");
  return isOnSwitch(env[BACKGROUND_DISABLE_ENV]) || isOnSwitch(env[BACKGROUND_DISABLE_ENV_ALIAS]);
}
__name(backgroundDisabled, "backgroundDisabled");
function toggle(value) {
  if (typeof value !== "string") return void 0;
  const normalized = value.trim().toLowerCase();
  if (["on", "true", "1", "yes"].includes(normalized)) return "on";
  if (["off", "false", "0", "no"].includes(normalized)) return "off";
  return void 0;
}
__name(toggle, "toggle");
function forkModeFromEnv(env) {
  return toggle(env[FORK_MODE_ENV]) ?? toggle(env[FORK_MODE_ENV_ALIAS]);
}
__name(forkModeFromEnv, "forkModeFromEnv");
function agentSdk(env) {
  return toggle(env[SDK_ENV]) === "on" || toggle(env[SDK_ENV_ALIAS]) === "on" || toggle(env[COMMANDCODE_SDK_ENV]) === "on";
}
__name(agentSdk, "agentSdk");
function agentNameFromEnv(env) {
  const primary = env[AGENT_ENV];
  if (primary !== void 0 && primary.length > 0) return primary.trim();
  const alias = env[AGENT_ENV_ALIAS];
  return alias !== void 0 && alias.length > 0 ? alias.trim() : "";
}
__name(agentNameFromEnv, "agentNameFromEnv");
function crossSessionEnabled(env) {
  return env[CROSS_SESSION_ENV] !== "0";
}
__name(crossSessionEnabled, "crossSessionEnabled");
function mainPermissionModeFromEnv(env) {
  return env[MAIN_PERMISSION_MODE_ENV];
}
__name(mainPermissionModeFromEnv, "mainPermissionModeFromEnv");
function workerModeFromEnv(env) {
  return env[WORKER_ENV] === "1" || env[PUB_SUB_WORKER_ENV] === "1";
}
__name(workerModeFromEnv, "workerModeFromEnv");
function policyFiles(env, home) {
  return {
    managed: env[MANAGED_CLAUDE_MD_ENV] ?? join6(home, ".commandcode", "managed", "CLAUDE.md"),
    user: env[USER_CLAUDE_MD_ENV] ?? join6(home, ".commandcode", "CLAUDE.md")
  };
}
__name(policyFiles, "policyFiles");
function optionValue(source, name) {
  return String(source.getFlag(name) ?? "");
}
__name(optionValue, "optionValue");
function pickOption(source, primary, primaryDefault, legacy, legacyDefault) {
  const value = optionValue(source, primary);
  if (value.length > 0 && value !== primaryDefault) return value;
  const old = optionValue(source, legacy);
  if (old.length > 0 && old !== legacyDefault) return old;
  return value.length > 0 ? value : legacyDefault;
}
__name(pickOption, "pickOption");
function optionList(source, name) {
  return commaList(optionValue(source, name));
}
__name(optionList, "optionList");
function managedSettingsDir(source) {
  return optionValue(source, MANAGED_DIR_OPTION) || MANAGED_SETTINGS_DEFAULT;
}
__name(managedSettingsDir, "managedSettingsDir");
function managedSettingsHome(home) {
  return join6(home, ".commandcode", "managed");
}
__name(managedSettingsHome, "managedSettingsHome");

// src/worktree.ts
import { dirname as dirname2, resolve, sep } from "node:path";
function gitSeam(runtime) {
  return {
    git(args, cwd) {
      const reply = runtime.run?.("git", args, { cwd, timeoutMs: 3e4 });
      if (reply === void 0) return { code: 1, stdout: "", stderr: "git is not available" };
      return {
        code: typeof reply.code === "number" ? reply.code : reply.ok ? 0 : 1,
        stdout: String(reply.stdout ?? ""),
        stderr: String(reply.stderr ?? "")
      };
    }
  };
}
__name(gitSeam, "gitSeam");
function isInside(parent, child) {
  const root = resolve(parent);
  const target = resolve(child);
  return target === root || target.startsWith(root.endsWith(sep) ? root : root + sep);
}
__name(isInside, "isInside");
function firstLine(value) {
  return value.trim().split("\n")[0].trim();
}
__name(firstLine, "firstLine");
function mainCheckoutRoots(seam, cwd) {
  const top = seam.git(["rev-parse", "--show-toplevel"], cwd);
  if (top.code !== 0) return [];
  const topLevel = firstLine(top.stdout);
  if (topLevel.length === 0) return [];
  const roots = [topLevel];
  const common = seam.git(["rev-parse", "--path-format=absolute", "--git-common-dir"], topLevel);
  let commonDir = "";
  if (common.code === 0) {
    commonDir = firstLine(common.stdout);
  } else {
    const plain = seam.git(["rev-parse", "--git-common-dir"], topLevel);
    if (plain.code === 0) commonDir = resolve(topLevel, firstLine(plain.stdout));
  }
  if (commonDir.length > 0 && !isInside(topLevel, commonDir)) roots.push(dirname2(commonDir));
  return roots;
}
__name(mainCheckoutRoots, "mainCheckoutRoots");
function defaultBranch(seam, cwd) {
  const candidates = [];
  const head = seam.git(["symbolic-ref", "--short", "refs/remotes/origin/HEAD"], cwd);
  if (head.code === 0) {
    const name = firstLine(head.stdout);
    if (name.length > 0) candidates.push(name);
  }
  candidates.push("main", "master");
  for (const candidate of candidates) {
    if (seam.git(["rev-parse", "--verify", "--quiet", candidate], cwd).code === 0) return candidate;
  }
  return void 0;
}
__name(defaultBranch, "defaultBranch");
function createJobWorktree(seam, opts) {
  const roots = mainCheckoutRoots(seam, opts.cwd);
  if (roots.length === 0) {
    return { ok: false, error: "isolation: worktree needs a git repository (git rev-parse --show-toplevel failed)" };
  }
  const base = defaultBranch(seam, opts.cwd);
  if (base === void 0) {
    return {
      ok: false,
      error: "isolation: worktree cannot resolve the default branch (no origin/HEAD, main, or master)"
    };
  }
  try {
    seam.fs.mkdir(dirname2(opts.path));
  } catch (error) {
    const message = String(error?.message ?? error);
    return { ok: false, error: `isolation: worktree cannot create ${dirname2(opts.path)}: ${message}` };
  }
  const added = seam.git(["worktree", "add", "--detach", opts.path, base], roots[0]);
  if (added.code !== 0) {
    const reason = firstLine(added.stderr) || firstLine(added.stdout) || `exit ${added.code}`;
    return { ok: false, error: `isolation: worktree add failed: ${reason}` };
  }
  const head = seam.git(["rev-parse", "HEAD"], opts.path);
  const start = head.code === 0 ? firstLine(head.stdout) : "";
  return { ok: true, path: opts.path, base, ...start.length > 0 ? { head: start } : {}, mainCheckouts: roots };
}
__name(createJobWorktree, "createJobWorktree");
function worktreeChanged(seam, path, head) {
  const status = seam.git(["status", "--porcelain"], path);
  if (status.code !== 0) return void 0;
  if (status.stdout.trim().length > 0) return true;
  if (head === void 0) return false;
  const current = seam.git(["rev-parse", "HEAD"], path);
  if (current.code !== 0) return void 0;
  return firstLine(current.stdout) !== head;
}
__name(worktreeChanged, "worktreeChanged");
function finalizeWorktree(seam, record) {
  if (typeof record.worktree !== "string" || record.worktree.length === 0 || record.worktreeFinalized === true) {
    return void 0;
  }
  const path = record.worktree;
  if (record.live === true) return { kept: true, path };
  const changed = worktreeChanged(seam, path, record.worktreeHead);
  if (changed === void 0) return { kept: true, path, error: "git status failed; kept the worktree" };
  if (changed) return { kept: true, path };
  const removed = seam.git(["worktree", "remove", "--force", path], record.worktreeRepo ?? dirname2(path));
  if (removed.code !== 0) {
    return { kept: true, path, error: `git worktree remove failed: ${firstLine(removed.stderr) || `exit ${removed.code}`}` };
  }
  return { kept: false, path };
}
__name(finalizeWorktree, "finalizeWorktree");
function isolationRoots(seam, cwd, spec) {
  const supplied = (spec?.mainCheckouts ?? []).filter((root) => typeof root === "string" && root.length > 0);
  const mainCheckouts = supplied.length > 0 ? supplied : mainCheckoutRoots(seam, cwd);
  const worktree = typeof spec?.worktree === "string" && spec.worktree.length > 0 ? spec.worktree : void 0;
  if (worktree !== void 0) {
    return { worktree, mainCheckouts };
  }
  if (mainCheckouts.length > 1 && isInside(mainCheckouts[0], cwd)) {
    return { worktree: mainCheckouts[0], mainCheckouts };
  }
  return void 0;
}
__name(isolationRoots, "isolationRoots");
var SHELL_TOOL_NAMES = {
  bash: true,
  shell: true,
  sh: true,
  monitor: true,
  powershell: true,
  pwsh: true
};
var NULL_DEVICES = {
  "/dev/null": true,
  "/dev/stdout": true,
  "/dev/stderr": true,
  "/dev/tty": true
};
function namedCwd(input) {
  for (const key of ["cwd", "workdir", "working_directory", "workingDirectory"]) {
    const value = input[key];
    if (typeof value === "string" && value.trim().length > 0) return value.trim();
  }
  return void 0;
}
__name(namedCwd, "namedCwd");
function commandEscapesWorktree(command, roots) {
  if (/\b(?:GIT_DIR|GIT_WORK_TREE|GIT_COMMON_DIR)\s*=/.test(command)) {
    return "the command overrides the git directory or work tree";
  }
  if (/\bgit\b[^\n;&|]*\s--prefix[=\s]/.test(command)) return "the command overrides git --prefix";
  const outsideWorktree = command.split(roots.worktree).join("");
  for (const main of roots.mainCheckouts) {
    if (outsideWorktree.includes(main)) return `the command names the main checkout (${main})`;
  }
  const targets = [];
  for (const match of command.matchAll(/(?:^|[;&|]\s*|\s)(?:cd|pushd|set-location|sl)\s+(['"]?)([^\s'";|&]+)\1/gi)) {
    targets.push({ kind: "cd", target: match[2] });
  }
  for (const match of command.matchAll(/(?:\d?|&)>>?\|?\s*(['"]?)([^\s'";|&<>]+)\1/g)) {
    targets.push({ kind: "redirect", target: match[2] });
  }
  for (const match of command.matchAll(/\bgit\b[^\n;&|]*(?:-C\s+|--work-tree[=\s]+|--git-dir[=\s]+)(['"]?)([^\s'";|&]+)\1/g)) {
    targets.push({ kind: "git path", target: match[2] });
  }
  for (const { kind, target } of targets) {
    if (kind === "cd" && (target === "-" || target.startsWith("~"))) {
      return `a cd to ${target} leaves the worktree`;
    }
    if (target.startsWith("$")) continue;
    const resolved = resolve(roots.worktree, target);
    if (kind === "cd" && !isInside(roots.worktree, resolved)) {
      return `a cd to ${target} leaves the worktree (${resolved})`;
    }
    if (kind === "redirect" && NULL_DEVICES[resolved] === true) continue;
    if (kind !== "cd" && !isInside(roots.worktree, resolved)) {
      return `a ${kind} to ${target} escapes the worktree (${resolved})`;
    }
  }
  return void 0;
}
__name(commandEscapesWorktree, "commandEscapesWorktree");
function guardToolCall(toolName, input, roots, toolCwd = roots.worktree) {
  const name = toolName.toLowerCase();
  if (SHELL_TOOL_NAMES[name] !== true) return void 0;
  const named = namedCwd(input);
  const cwd = named === void 0 ? toolCwd : resolve(toolCwd, named);
  const insideWorktree = isInside(roots.worktree, cwd);
  if (!insideWorktree) {
    for (const main of roots.mainCheckouts) {
      if (!isInside(main, cwd)) continue;
      return {
        block: true,
        reason: `Blocked: this subagent is isolated in its worktree (${roots.worktree}); a command in the main checkout (${cwd}) is not allowed. Run it inside the worktree.`
      };
    }
  }
  if (name === "powershell" || name === "pwsh") return void 0;
  const command = [input.command, input.script, input.cmd].find((value) => typeof value === "string");
  if (command === void 0) return void 0;
  const escape = commandEscapesWorktree(command, roots);
  if (escape === void 0) return void 0;
  return { block: true, reason: `Blocked: ${escape}. This subagent is isolated in ${roots.worktree}.` };
}
__name(guardToolCall, "guardToolCall");

// src/worktree-lifecycle.ts
import { dirname as dirname3, join as join7 } from "node:path";
function sweepExpiredEntries(io, dir, now, options = {}) {
  let names;
  try {
    names = io.readdir(dir);
  } catch {
    return [];
  }
  const removed = [];
  for (const name of names) {
    const file = join7(dir, name);
    try {
      if (options.skip?.(name) === true) continue;
      const expired = options.expired?.(name) === true;
      const old = options.ttlMs !== void 0 && now - io.stat(file).mtimeMs > options.ttlMs;
      if (!expired && !old) continue;
      io.remove(file);
      removed.push(name);
    } catch {
    }
  }
  return removed;
}
__name(sweepExpiredEntries, "sweepExpiredEntries");
function createWorktreeLifecycle(ctx) {
  const spool = ctx.fs;
  const git = ctx.git;
  const now = ctx.now;
  function cleanupPeriodDays() {
    for (const base of [ctx.cwd, ctx.home]) {
      if (typeof base !== "string" || base.length === 0) continue;
      try {
        const parsed = JSON.parse(spool.readFile(join7(base, ".commandcode", "settings.json")));
        const value = Number(parsed?.[CLEANUP_PERIOD_SETTING]);
        if (Number.isFinite(value) && value > 0) return value;
      } catch {
      }
    }
    return void 0;
  }
  __name(cleanupPeriodDays, "cleanupPeriodDays");
  function retentionMs() {
    const raw = ctx.env[RETENTION_ENV] ?? ctx.env[RETENTION_ENV_ALIAS];
    const days = Number(raw);
    const configured = Number.isFinite(days) && days > 0 ? days : cleanupPeriodDays();
    const value = configured !== void 0 && Number.isFinite(configured) && configured > 0 ? configured : RETENTION_DAYS_DEFAULT;
    return value * 24 * 60 * 60 * 1e3;
  }
  __name(retentionMs, "retentionMs");
  function discardCleanWorktree(job, path) {
    if (job?.worktreeKept === true) return;
    if (worktreeChanged(git, path) !== false) return;
    const roots = mainCheckoutRoots(git, path);
    const repo = job?.worktreeRepo ?? roots[1] ?? roots[0] ?? dirname3(path);
    git.git(["worktree", "remove", "--force", path], repo);
  }
  __name(discardCleanWorktree, "discardCleanWorktree");
  function finalizeJobWorktree(job) {
    if (job === void 0) return void 0;
    const state = normalizeState(job.state);
    if (state === void 0 || !isTerminal(state)) return void 0;
    if (typeof job.worktree !== "string" || job.worktree.length === 0) return void 0;
    if (job.worktreeFinalized === true) return { kept: job.worktreeKept === true, path: job.worktree };
    const live = job.live === true && liveState(job).alive;
    const decision = finalizeWorktree(git, { ...job, live });
    if (decision === void 0) return void 0;
    job.worktreeFinalized = true;
    job.worktreeKept = decision.kept;
    job.updatedAt = now();
    ctx.writeJob(job);
    return decision;
  }
  __name(finalizeJobWorktree, "finalizeJobWorktree");
  function liveState(record) {
    const alive = ctx.alive(record.workerPid) || ctx.alive(record.pid);
    let busy = true;
    try {
      const state = JSON.parse(spool.readFile(join7(ctx.controlRoot, record.id, "state.json")));
      busy = state.busy !== false;
    } catch {
    }
    return { alive, busy };
  }
  __name(liveState, "liveState");
  function sweepExpired() {
    if (ctx.workerMode) return;
    const stamp = now();
    const ttlMs = retentionMs();
    const cutoff = stamp - ttlMs;
    const expired = [];
    sweepExpiredEntries(spool, ctx.jobsDir, stamp, {
      expired: /* @__PURE__ */ __name((name) => {
        if (!name.endsWith(".json") || name.includes(".tmp-")) return false;
        try {
          const job = JSON.parse(spool.readFile(join7(ctx.jobsDir, name)));
          const state = normalizeState(job.state);
          if (state === void 0 || !isTerminal(state)) return false;
          const settledAt = Number(job.finishedAt ?? job.updatedAt ?? job.startedAt ?? 0) || 0;
          if (settledAt === 0 || settledAt > cutoff) return false;
          if (typeof job.id === "string" && job.id.length > 0) expired.push(job);
          return true;
        } catch {
          return false;
        }
      }, "expired")
    });
    for (const job of expired) {
      const id = job.id;
      try {
        spool.remove(join7(ctx.logsDir, `${id}.log`));
      } catch {
      }
      try {
        spool.remove(
          typeof job.transcriptPath === "string" && job.transcriptPath.length > 0 ? job.transcriptPath : join7(ctx.transcriptsDir, `${id}.jsonl`)
        );
      } catch {
      }
      discardCleanWorktree(
        job,
        typeof job.worktree === "string" && job.worktree.length > 0 ? job.worktree : join7(ctx.worktreesDir, id)
      );
    }
    sweepExpiredEntries(spool, ctx.logsDir, stamp, {
      ttlMs,
      skip: /* @__PURE__ */ __name((name) => {
        const job = ctx.readJob(name.replace(/\.log$/, ""));
        if (job === void 0) return false;
        const state = normalizeState(job.state);
        return state === void 0 || !isTerminal(state);
      }, "skip")
    });
    sweepExpiredEntries(spool, ctx.transcriptsDir, stamp, {
      ttlMs,
      // A transcript belongs to a job, so a running job's transcript is never
      // swept even when the file itself is old.
      skip: /* @__PURE__ */ __name((name) => {
        const job = ctx.readJob(name.replace(/\.jsonl$/, ""));
        if (job === void 0) return false;
        const state = normalizeState(job.state);
        return state === void 0 || !isTerminal(state);
      }, "skip")
    });
    let worktreeNames = [];
    try {
      worktreeNames = spool.readdir(ctx.worktreesDir);
    } catch {
      worktreeNames = [];
    }
    for (const name of worktreeNames) {
      const job = ctx.readJob(name);
      if (job !== void 0) {
        const state = normalizeState(job.state);
        if (state === void 0 || !isTerminal(state)) continue;
      }
      const path = join7(ctx.worktreesDir, name);
      let old = false;
      try {
        old = stamp - spool.stat(path).mtimeMs > ttlMs;
      } catch {
        old = false;
      }
      if (old) discardCleanWorktree(job, path);
    }
  }
  __name(sweepExpired, "sweepExpired");
  return { finalizeJobWorktree, sweepExpired, liveState };
}
__name(createWorktreeLifecycle, "createWorktreeLifecycle");

// src/plan.ts
import { join as join8 } from "node:path";
function planFromInput(input, ctx) {
  const label = String(input.description ?? input.label ?? "").trim();
  if (label.length === 0) return { ok: false, error: "description is required and must not be empty" };
  const command = typeof input.command === "string" && input.command.trim().length > 0 ? input.command : void 0;
  const prompt = typeof input.prompt === "string" && input.prompt.trim().length > 0 ? input.prompt : void 0;
  if (command === void 0 && prompt === void 0) {
    return { ok: false, error: "pass either prompt (a headless agent) or command (a shell command)" };
  }
  if (command !== void 0 && prompt !== void 0) return { ok: false, error: "pass prompt or command, not both" };
  if (input.isolation !== void 0) {
    return {
      ok: false,
      error: "isolation is chosen by a definition's frontmatter (`isolation: worktree`), not per call; omit it"
    };
  }
  const requestedMode = typeof input.permission_mode === "string" && input.permission_mode.trim().length > 0 ? input.permission_mode.trim() : void 0;
  if (requestedMode !== void 0 && ctx.canonicalPermissionMode(requestedMode) === void 0) {
    return { ok: false, error: `unknown permission_mode "${requestedMode}"` };
  }
  if (requestedMode !== void 0 && prompt === void 0) {
    return { ok: false, error: "permission_mode applies to agent jobs; a shell command has no permission mode" };
  }
  const model = typeof input.model === "string" && input.model.trim().length > 0 ? input.model.trim() : void 0;
  const name = typeof input.name === "string" && input.name.trim().length > 0 ? input.name.trim() : void 0;
  const teamName = typeof input.team_name === "string" && input.team_name.trim().length > 0 ? input.team_name.trim() : void 0;
  const requestedMaxTurns = Number(input.max_turns);
  const timeoutMs = Number(input.timeout_ms);
  const subagentType = typeof input.subagent_type === "string" && input.subagent_type.trim().length > 0 ? input.subagent_type.trim() : void 0;
  const aliasType = typeof input.agent_type === "string" && input.agent_type.trim().length > 0 ? input.agent_type.trim() : void 0;
  if (subagentType !== void 0 && aliasType !== void 0 && subagentType !== aliasType) {
    return { ok: false, error: "pass subagent_type or its deprecated alias agent_type, not both" };
  }
  let definition;
  if (prompt !== void 0) {
    const resolved = ctx.resolveAgentDefinition(subagentType ?? aliasType);
    if (resolved.ok === false) return { ok: false, error: resolved.error };
    definition = resolved.definition;
  }
  const forkRequested = definition?.fork === true;
  const teammate = prompt !== void 0 && teamName !== void 0;
  const requestedBackground = typeof input.run_in_background === "boolean" ? input.run_in_background : void 0;
  const teamPolicy = ctx.resolveTeamPolicy({
    teammate,
    definitionBackground: definition?.background,
    ...requestedBackground !== void 0 ? { requested: requestedBackground } : {},
    forkMode: ctx.forkMode
  });
  if (teamPolicy.ok === false) return { ok: false, error: teamPolicy.error };
  const requestedTools = prompt === void 0 || forkRequested ? void 0 : ctx.listField(input.tools) ?? definition?.tools;
  const requestedDisallowed = prompt === void 0 || forkRequested ? void 0 : ctx.listField(input.disallowed_tools) ?? definition?.disallowedTools;
  let permissionMode;
  if (prompt !== void 0) {
    if (forkRequested) {
      permissionMode = ctx.mainPermissionMode() ?? "default";
    } else {
      const modeResolution = ctx.resolvePermissionMode(requestedMode ?? definition?.permissionMode, ctx.mainPermissionMode());
      if (modeResolution.ok === false) return { ok: false, error: modeResolution.error };
      permissionMode = modeResolution.mode;
    }
  }
  const background = teammate ? false : ctx.forkModeEnabled ? true : ctx.resolveExecutionPolicy({
    ...requestedBackground !== void 0 ? { requested: requestedBackground } : {},
    definitionBackground: definition?.background,
    env: ctx.env
  }) === "background";
  const configuredMcp = ctx.mcpServerConfigs(ctx.fs, [ctx.home, ctx.runtimeCwd]);
  const mcp = ctx.resolveAgentMcpServers({
    defined: definition?.mcpServers ?? [],
    configured: configuredMcp,
    definition,
    env: ctx.env,
    trusted: definition === void 0 || ctx.agentFolderTrusted(definition, {
      cwd: ctx.runtimeCwd,
      home: ctx.home,
      managedDir: ctx.managedDir,
      addDirs: ctx.addDirs
    })
  });
  const mcpNames = Object.keys(configuredMcp);
  for (const server of mcp.servers) if (!mcpNames.includes(server.name)) mcpNames.push(server.name);
  const launch = forkRequested ? { ok: true, model: void 0, modelSource: "inherit", effort: void 0, effortSource: "none", tools: [], removed: [] } : ctx.resolveLaunch({
    ...model !== void 0 ? { requestedModel: model } : {},
    ...definition?.model !== void 0 ? { definitionModel: definition.model } : {},
    ...typeof input.effort === "string" ? { requestedEffort: input.effort } : {},
    ...definition?.effort !== void 0 ? { definitionEffort: definition.effort } : {},
    env: ctx.env,
    // The mod API exposes no main-model getter: an absent model means the ACP
    // child inherits the user's configured model, which is the main one.
    ...requestedTools !== void 0 ? { tools: requestedTools } : {},
    ...requestedDisallowed !== void 0 ? { disallowedTools: requestedDisallowed } : {},
    ...permissionMode !== void 0 ? { permissionMode } : {},
    background,
    ...teammate ? { team: true } : {},
    mcpServers: mcpNames
  });
  if (launch.ok === false) return { ok: false, error: launch.error };
  const sessionId = ctx.sessionId();
  if (forkRequested && (sessionId === void 0 || sessionId.length === 0)) {
    return { ok: false, error: "a fork needs the main session id; the session has not bound yet" };
  }
  const launchTools = prompt === void 0 || forkRequested ? void 0 : ctx.withMemoryTools(launch.tools, definition?.memory);
  const maxTurns = prompt === void 0 || forkRequested ? void 0 : Number.isFinite(requestedMaxTurns) && requestedMaxTurns > 0 ? Math.round(requestedMaxTurns) : definition?.maxTurns;
  const runCwd = typeof input.cwd === "string" && input.cwd.length > 0 ? input.cwd : ctx.cwd;
  const compactWindow = ctx.resolveAutoCompactWindow(
    ctx.contextWindowForModel(launch.model, ctx.env),
    definition?.autoCompactWindow,
    false
  );
  return {
    ok: true,
    plan: {
      description: label,
      kind: forkRequested ? "fork" : prompt !== void 0 ? "agent" : "command",
      command,
      prompt,
      background,
      cwd: runCwd,
      ...name !== void 0 ? { name } : {},
      ...teamName !== void 0 ? { teamName } : {},
      agentType: definition?.name,
      oneShot: definition?.oneShot,
      ...forkRequested ? { fork: true, ...sessionId !== void 0 ? { mainSessionId: sessionId } : {} } : {},
      systemPrompt: forkRequested ? void 0 : ctx.systemPromptFor(definition?.prompt),
      appendSystemPrompt: ctx.appendPromptText,
      restrictedTools: definition?.tools !== void 0 || definition?.disallowedTools !== void 0 || requestedDisallowed !== void 0,
      hooks: forkRequested ? void 0 : definition?.hooks,
      startup: forkRequested ? void 0 : ctx.startupFor(definition, runCwd, prompt, launch.model),
      model: prompt === void 0 || forkRequested ? void 0 : launch.model,
      tools: launchTools,
      ...requestedDisallowed !== void 0 ? { disallowedTools: requestedDisallowed } : {},
      ...definition?.color !== void 0 ? { color: definition.color } : {},
      ...prompt !== void 0 && !forkRequested ? { mcpServers: mcp.servers } : {},
      ...prompt !== void 0 && !forkRequested && compactWindow !== void 0 ? { autoCompactWindow: compactWindow } : {},
      ...prompt !== void 0 && !forkRequested && definition?.cacheTtl !== void 0 ? { cacheTtl: definition.cacheTtl } : {},
      maxTurns,
      effort: prompt === void 0 || forkRequested ? void 0 : launch.effort,
      permissionMode,
      isolation: forkRequested ? void 0 : definition?.isolation,
      timeoutMs: Number.isFinite(timeoutMs) && timeoutMs > 0 ? Math.round(timeoutMs) : void 0
    }
  };
}
__name(planFromInput, "planFromInput");
function planFromRecord(record, instruction, mode, ctx) {
  const label = String(record.name ?? record.id);
  const jobId = record.id;
  const suffix = mode === "resume" ? "resume" : "message";
  const resumeCwd = String(record.cwd ?? ctx.cwd);
  if (record.fork === true) {
    if (record.forkSessionId === void 0) {
      return {
        ok: false,
        error: mode === "resume" ? `subagent: job ${jobId} is a fork with no recorded session; spawn it again` : `${label} has finished and its fork session was never recorded; spawn it again`
      };
    }
    return {
      ok: true,
      plan: {
        description: `${String(record.description ?? jobId)} (${suffix})`,
        kind: "fork",
        prompt: instruction,
        background: record.teamName !== void 0 ? false : true,
        cwd: resumeCwd,
        agentType: FORK_AGENT_TYPE,
        fork: true,
        ...record.mainSessionId !== void 0 ? { mainSessionId: record.mainSessionId } : {},
        resumeSession: record.forkSessionId,
        resumedFrom: jobId,
        ...record.permissionMode !== void 0 ? { permissionMode: record.permissionMode } : {},
        ...record.name !== void 0 ? { name: record.name } : {},
        ...record.teamName !== void 0 ? { teamName: record.teamName } : {},
        ...record.owner !== void 0 ? { owner: record.owner } : {}
      }
    };
  }
  if (record.acpSessionId === void 0 && mode === "message") {
    return { ok: false, error: `${label} has finished and has no resumable session; spawn it again` };
  }
  const definition = record.agentType === void 0 ? void 0 : ctx.resolveAgentDefinition(record.agentType);
  const resumeDefinition = definition?.ok === true ? definition.definition : void 0;
  const background = mode === "message" ? record.teamName !== void 0 ? false : true : record.teamName !== void 0 ? false : ctx.resolveExecutionPolicy({
    definitionBackground: resumeDefinition?.background,
    env: ctx.env
  }) === "background";
  const resumedMcp = mode === "message" ? [] : resumeDefinition === void 0 ? record.mcpServers ?? [] : ctx.resolveAgentMcpServers({
    defined: resumeDefinition.mcpServers ?? [],
    configured: ctx.mcpServerConfigs(ctx.fs, [ctx.home, ctx.runtimeCwd]),
    definition: resumeDefinition,
    env: ctx.env,
    trusted: ctx.agentFolderTrusted(resumeDefinition, {
      cwd: ctx.runtimeCwd,
      home: ctx.home,
      managedDir: ctx.managedDir,
      addDirs: ctx.addDirs
    })
  }).servers;
  const resumeCompact = mode === "message" ? record.autoCompactWindow : record.autoCompactWindow ?? ctx.resolveAutoCompactWindow(
    ctx.contextWindowForModel(record.model, ctx.env),
    resumeDefinition?.autoCompactWindow,
    false
  );
  const resumeCacheTtl = mode === "message" ? record.cacheTtl : record.cacheTtl ?? resumeDefinition?.cacheTtl;
  return {
    ok: true,
    plan: {
      description: `${String(record.description ?? jobId)} (${suffix})`,
      kind: "agent",
      prompt: instruction,
      background,
      cwd: resumeCwd,
      agentType: record.agentType,
      systemPrompt: mode === "message" ? resumeDefinition?.prompt : ctx.systemPromptFor(resumeDefinition?.prompt),
      appendSystemPrompt: ctx.appendPromptText,
      restrictedTools: resumeDefinition?.tools !== void 0 || resumeDefinition?.disallowedTools !== void 0,
      hooks: resumeDefinition?.hooks,
      startup: ctx.startupFor(resumeDefinition, resumeCwd, instruction, record.model),
      model: record.model,
      ...record.tools !== void 0 ? { tools: record.tools } : {},
      ...mode === "message" ? record.mcpServers !== void 0 ? { mcpServers: record.mcpServers } : {} : resumedMcp.length > 0 ? { mcpServers: resumedMcp } : {},
      ...resumeCompact !== void 0 ? { autoCompactWindow: resumeCompact } : {},
      ...resumeCacheTtl !== void 0 ? { cacheTtl: resumeCacheTtl } : {},
      ...record.color !== void 0 ? { color: record.color } : {},
      maxTurns: record.maxTurns,
      // The per-invocation model and effort are what make a resume the same run
      // continued, not a fresh one with the definition's defaults.
      effort: record.effort,
      permissionMode: record.permissionMode,
      timeoutMs: record.timeoutMs,
      resumeSession: record.acpSessionId,
      resumedFrom: jobId,
      ...record.name !== void 0 ? { name: record.name } : {},
      ...record.teamName !== void 0 ? { teamName: record.teamName } : {},
      ...record.owner !== void 0 ? { owner: record.owner } : {}
    }
  };
}
__name(planFromRecord, "planFromRecord");
function sessionNameOf(id) {
  return `subagent-${id}`;
}
__name(sessionNameOf, "sessionNameOf");
function recordFromPlan(intent, id, logPath, startedAt, owner, ctx) {
  const isAgent = intent.prompt !== void 0;
  return {
    id,
    owner,
    description: intent.description,
    kind: intent.kind,
    ...intent.name !== void 0 ? { name: intent.name } : {},
    state: "queued",
    // The resolved policy, not the raw `run_in_background`: env and definition
    // overrides are already applied by the caller.
    background: intent.background !== false,
    ...isAgent && intent.live !== false && intent.oneShot !== true ? { live: true } : {},
    cwd: intent.cwd,
    logPath,
    startedAt,
    sessionId: ctx.sessionId(),
    updatedAt: startedAt,
    ...intent.depth !== void 0 ? { depth: intent.depth } : {},
    ...intent.parentJobId !== void 0 ? { parentJobId: intent.parentJobId } : {},
    ...isAgent ? {
      // The durable resume handle. A one-shot agent has none, so nothing can
      // invite a resume that the lifecycle forbids.
      ...intent.oneShot === true ? {} : { agentId: id },
      agentType: intent.agentType ?? "general-purpose",
      sessionName: sessionNameOf(id),
      transcriptPath: join8(ctx.transcriptsDir, `${id}.jsonl`),
      ...intent.resumeSession !== void 0 ? { resumedFrom: intent.resumedFrom } : {}
    } : {},
    ...intent.fork === true ? { fork: true, promptCache: "shared", ...intent.mainSessionId !== void 0 ? { mainSessionId: intent.mainSessionId } : {} } : isAgent ? { promptCache: "own" } : {},
    ...intent.teamName !== void 0 ? { teamName: intent.teamName } : {},
    ...intent.model !== void 0 ? { model: intent.model } : {},
    ...intent.tools !== void 0 ? { tools: intent.tools } : {},
    ...intent.mcpServers !== void 0 ? { mcpServers: intent.mcpServers } : {},
    ...intent.autoCompactWindow !== void 0 ? { autoCompactWindow: intent.autoCompactWindow } : {},
    ...intent.cacheTtl !== void 0 ? { cacheTtl: intent.cacheTtl } : {},
    ...intent.color !== void 0 ? { color: intent.color } : {},
    ...intent.maxTurns !== void 0 ? { maxTurns: intent.maxTurns } : {},
    ...intent.effort !== void 0 ? { effort: intent.effort } : {},
    ...intent.permissionMode !== void 0 ? { permissionMode: intent.permissionMode } : {},
    ...intent.isolation !== void 0 ? { isolation: intent.isolation } : {},
    ...intent.timeoutMs !== void 0 && intent.timeoutMs > 0 ? { timeoutMs: intent.timeoutMs } : {},
    ...intent.prompt !== void 0 ? { prompt: intent.prompt } : { command: intent.command }
  };
}
__name(recordFromPlan, "recordFromPlan");

// src/tools.ts
import { join as join11 } from "node:path";

// src/spawn.ts
import { join as join10 } from "node:path";

// src/permission.ts
import { join as join9 } from "node:path";
async function answerPendingPermission(controlDir, answered, ctx) {
  let request;
  try {
    request = JSON.parse(ctx.fs.readFile(join9(controlDir, "permission.json")));
  } catch {
    return;
  }
  const key = String(request.requestId ?? "");
  if (key.length === 0 || answered.has(key)) return;
  const options = request.options ?? [];
  const choices = options.map((option2, index) => ({
    label: option2.name || option2.kind || option2.optionId || `option ${index + 1}`,
    index
  }));
  const ui = ctx.ui;
  if (typeof ui.select !== "function") return;
  let chosen;
  try {
    chosen = await ui.select({
      title: `Approve "${String(request.title)}"?`,
      options: choices.map((choice) => ({ label: choice.label }))
    });
  } catch {
    return;
  }
  if (typeof chosen !== "string") return;
  answered.add(key);
  const picked = choices.find((choice) => choice.label === chosen);
  const option = picked === void 0 ? void 0 : options[picked.index];
  const response = option === void 0 ? { requestId: request.requestId, outcome: "cancelled" } : { requestId: request.requestId, optionId: option.optionId };
  const file = join9(controlDir, "permission-response.json");
  const tmp = `${file}.mod-${ctx.pid}-${ctx.now()}`;
  ctx.fs.writeFile(tmp, JSON.stringify(response));
  ctx.fs.rename(tmp, file);
}
__name(answerPendingPermission, "answerPendingPermission");

// src/spawn.ts
function buildArgs(spec, modPath) {
  if (spec.kind === "command") return ["-c", String(spec.command ?? "")];
  if (spec.kind === "fork") {
    const prompt = String(spec.prompt ?? "");
    if (typeof spec.resumeSession === "string" && spec.resumeSession.length > 0) {
      return ["--resume", spec.resumeSession, "-p", prompt, "--output-format", "json"];
    }
    const name = String(spec.sessionName ?? `subagent-${spec.id}`);
    return [
      "--fork-session",
      "--resume",
      String(spec.mainSessionId ?? ""),
      "-n",
      name,
      "-p",
      prompt,
      "--output-format",
      "json"
    ];
  }
  const maxTurns = typeof spec.maxTurns === "number" && spec.maxTurns > 0 ? String(spec.maxTurns) : "";
  return [...maxTurns.length > 0 ? ["--max-turns", maxTurns] : [], "--mod", modPath, "acp"];
}
__name(buildArgs, "buildArgs");
function buildSpec(input, modPath) {
  const spec = { ...input, args: [] };
  spec.args = buildArgs(spec, modPath);
  return spec;
}
__name(buildSpec, "buildSpec");
function positiveInt(raw, fallback) {
  const value = Number(raw);
  return Number.isFinite(value) && value >= 1 ? Math.floor(value) : fallback;
}
__name(positiveInt, "positiveInt");
function resolveMaxConcurrent(env) {
  return positiveInt(env[MAX_CONCURRENT_ENV] || env[MAX_CONCURRENT_ENV_ALIAS], MAX_CONCURRENT_DEFAULT);
}
__name(resolveMaxConcurrent, "resolveMaxConcurrent");
function resolveMaxDepth(env) {
  return positiveInt(env[MAX_DEPTH_ENV] || env[MAX_DEPTH_ENV_ALIAS], MAX_DEPTH_DEFAULT);
}
__name(resolveMaxDepth, "resolveMaxDepth");
function depthOf(spec) {
  const depth = Number(spec?.depth);
  return Number.isFinite(depth) && depth >= 0 ? Math.floor(depth) : 0;
}
__name(depthOf, "depthOf");
function workerEnv(parent, spec) {
  const env = { ...parent };
  delete env[WORKER_SPEC_ENV];
  delete env[WORKER_ENV];
  delete env.PUBSUB_WORKER;
  delete env.BETTER_SUBAGENTS_APPEND_SYSTEM_PROMPT;
  delete env.CLAUDE_CODE_APPEND_SUBAGENT_SYSTEM_PROMPT;
  delete env.BETTER_SUBAGENTS_APPEND_SYSTEM_PROMPT_FILE;
  delete env.CLAUDE_CODE_APPEND_SUBAGENT_SYSTEM_PROMPT_FILE;
  env[WORKER_SPEC_ENV] = JSON.stringify(spec);
  env[WORKER_ENV] = "1";
  if (typeof spec.appendSystemPrompt === "string" && spec.appendSystemPrompt.length > 0) {
    env.BETTER_SUBAGENTS_APPEND_SYSTEM_PROMPT = spec.appendSystemPrompt;
  }
  return env;
}
__name(workerEnv, "workerEnv");
function failJob(job, reason, ctx) {
  const now = ctx.now();
  const signal = signalOf(job, {
    state: "failed",
    finishedAt: now,
    durationMs: Math.max(0, now - Number(job.startedAt ?? now)),
    body: reason
  });
  if (!ctx.applyPatch(job.id, patchOf(signal), job, now)) return;
  ctx.enqueueSignal(signal);
  ctx.refreshJobs();
}
__name(failJob, "failJob");
function startWorker(record, spec, ctx) {
  const id = record.id;
  let worker;
  try {
    worker = ctx.spawn(ctx.execPath, ["-e", ctx.workerSource], {
      cwd: spec.cwd,
      detached: true,
      stdio: "ignore",
      env: workerEnv(ctx.env, spec)
    });
  } catch (error) {
    const reason = `spawn error: ${String(error?.message ?? error)}`;
    failJob(record, reason, ctx);
    return { ok: false, error: reason };
  }
  worker.on("error", (error) => {
    failJob(ctx.readJob(id) ?? record, `worker spawn error: ${String(error?.message ?? error)}`, ctx);
  });
  worker.unref();
  if (typeof worker.pid !== "number") {
    const reason = "the worker process did not start";
    failJob(ctx.readJob(id) ?? record, reason, ctx);
    return { ok: false, error: reason };
  }
  const latest = ctx.readJob(id) ?? record;
  latest.workerPid = worker.pid;
  latest.updatedAt = ctx.now();
  ctx.writeJob(latest);
  ctx.runProjectHooks("SubagentStart", spec.cwd, spec.agentType, {
    id,
    label: record.description,
    kind: record.kind
  });
  ctx.refreshJobs();
  return { ok: true, id, pid: worker.pid };
}
__name(startWorker, "startWorker");
function activeJobCount(ctx) {
  const owner = ctx.owner();
  let count = 0;
  for (const job of ctx.readRecentJobs(ACTIVE_JOB_SCAN_MAX)) {
    if (job.owner !== owner) continue;
    const state = normalizeState(job.state);
    if (state !== void 0 && state !== "queued" && state !== "running") continue;
    count += 1;
  }
  return count;
}
__name(activeJobCount, "activeJobCount");
async function fire(intent, ctx) {
  const depth = depthOf(ctx.workerSpec) + 1;
  if (intent.prompt !== void 0) {
    const maxDepth = resolveMaxDepth(ctx.env);
    if (depth > maxDepth) {
      return {
        ok: false,
        error: `Subagent nesting depth limit reached (${maxDepth}). This subagent cannot spawn children; do not retry.`
      };
    }
  }
  const maxConcurrent = resolveMaxConcurrent(ctx.env);
  if (activeJobCount(ctx) >= maxConcurrent) {
    return {
      ok: false,
      error: `Concurrent subagent limit reached (${maxConcurrent}). Do not retry; wait for a running subagent to finish or cancel one first.`
    };
  }
  const id = `${ctx.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
  const logPath = join10(ctx.logsDir, `${id}.log`);
  const startedAt = ctx.now();
  const isolation = intent.isolation === "worktree" ? createJobWorktree({ git: ctx.git.git, fs: ctx.fs }, { cwd: intent.cwd, path: join10(ctx.worktreesDir, id) }) : void 0;
  const owner = intent.owner ?? ctx.owner();
  const record = ctx.recordFromPlan(
    { ...intent, depth, ...ctx.workerSpec?.id !== void 0 ? { parentJobId: ctx.workerSpec.id } : {} },
    id,
    logPath,
    startedAt,
    owner
  );
  if (isolation?.ok === true) {
    record.worktree = isolation.path;
    record.worktreeRepo = isolation.mainCheckouts[0];
    if (isolation.head !== void 0) record.worktreeHead = isolation.head;
    record.mainCheckouts = isolation.mainCheckouts;
    record.cwd = isolation.path;
  }
  ctx.writeJob(record);
  if (isolation !== void 0 && isolation.ok === false) {
    failJob(record, isolation.error, ctx);
    return { ok: false, error: isolation.error };
  }
  try {
    ctx.appendJobEntry({
      customType: JOB_ENTRY_TYPE,
      data: {
        id,
        description: intent.description,
        kind: intent.kind,
        state: "queued",
        owner,
        sessionId: ctx.sessionId()
      }
    });
  } catch {
  }
  const isAgent = intent.prompt !== void 0;
  const live = isAgent && intent.live !== false && intent.oneShot !== true;
  const canSend = intent.tools === void 0 || intent.tools.includes("SendMessage") || intent.tools.includes("subagent_send");
  const roster = isAgent && intent.fork !== true && canSend ? ctx.rosterFor(intent.name) : { rosterNames: [] };
  const idleMs = Number(ctx.env.BETTER_SUBAGENTS_LIVE_IDLE_MS);
  if (live) {
    try {
      ctx.fs.mkdir(join10(ctx.controlRoot, id));
    } catch {
    }
  }
  const spec = buildSpec(
    {
      id,
      description: intent.description,
      kind: intent.kind,
      owner,
      cwd: isolation?.ok === true ? isolation.path : intent.cwd,
      logPath,
      inboxDir: ctx.inboxDir,
      jobsDir: ctx.jobsDir,
      startedAt,
      tailMax: LOG_TAIL_BYTES_MAX,
      controlPollMs: CONTROL_POLL_MS,
      permissionTimeoutMs: intent.background === false ? FOREGROUND_PERMISSION_TIMEOUT_MS : PERMISSION_TIMEOUT_MS,
      bin: isAgent ? ctx.env.COMMANDCODE_BIN ?? "cmd" : "/bin/sh",
      sessionId: ctx.sessionId(),
      ...intent.name !== void 0 ? { name: intent.name } : {},
      background: intent.background !== false,
      // A foreground caller answers permission prompts inline; a background run
      // settles with the request instead of holding a worker that nobody watches.
      ...intent.background === false ? { interactive: true, controlDir: join10(ctx.jobsDir, "..", "control", id) } : {},
      depth,
      ...intent.agentType !== void 0 ? { agentType: intent.agentType } : {},
      ...record.agentId !== void 0 ? { agentId: record.agentId } : {},
      ...record.transcriptPath !== void 0 ? { transcriptPath: record.transcriptPath } : {},
      ...intent.fork === true ? {
        fork: true,
        promptCache: "shared",
        sessionName: ctx.sessionNameOf(id),
        ...intent.mainSessionId !== void 0 ? { mainSessionId: intent.mainSessionId } : {}
      } : isAgent ? { promptCache: "own" } : {},
      ...intent.teamName !== void 0 ? { teamName: intent.teamName } : {},
      ...intent.systemPrompt !== void 0 ? { systemPrompt: intent.systemPrompt } : {},
      ...intent.appendSystemPrompt !== void 0 ? { appendSystemPrompt: intent.appendSystemPrompt } : {},
      ...intent.restrictedTools === true ? { restrictedTools: true } : {},
      ...intent.hooks !== void 0 ? { hooks: intent.hooks } : {},
      ...intent.startup !== void 0 ? { startup: intent.startup } : {},
      ...intent.model !== void 0 ? { model: intent.model } : {},
      ...intent.tools !== void 0 ? { tools: intent.tools } : {},
      ...intent.disallowedTools !== void 0 ? { disallowedTools: intent.disallowedTools } : {},
      ...intent.mcpServers !== void 0 ? { mcpServers: intent.mcpServers } : {},
      ...intent.autoCompactWindow !== void 0 ? { autoCompactWindow: intent.autoCompactWindow } : {},
      ...intent.cacheTtl !== void 0 ? { cacheTtl: intent.cacheTtl } : {},
      ...intent.maxTurns !== void 0 ? { maxTurns: intent.maxTurns } : {},
      ...intent.effort !== void 0 ? { effort: intent.effort } : {},
      ...intent.permissionMode !== void 0 ? { permissionMode: intent.permissionMode } : {},
      ...intent.timeoutMs !== void 0 && intent.timeoutMs > 0 ? { timeoutMs: intent.timeoutMs } : {},
      ...intent.resumeSession !== void 0 ? { resumeSession: intent.resumeSession } : {},
      ...intent.prompt !== void 0 ? { prompt: intent.prompt } : { command: intent.command },
      ...roster.roster !== void 0 ? { roster: roster.roster } : {},
      ...roster.rosterNames.length > 0 ? { rosterNames: roster.rosterNames } : {},
      ...intent.background !== void 0 ? { background: intent.background } : {},
      ...live ? { live: true, controlDir: join10(ctx.controlRoot, id), ...Number.isFinite(idleMs) && idleMs > 0 ? { idleMs } : {} } : {},
      ...isAgent ? { promptMarkerPath: join10(ctx.logsDir, `${id}.mod`) } : {},
      ...isolation?.ok === true ? { worktree: isolation.path, mainCheckouts: isolation.mainCheckouts } : {}
    },
    ctx.modPath
  );
  const launched = startWorker(record, spec, ctx);
  if (launched.ok === false || intent.background !== false) return launched;
  return settleForeground(launched, ctx);
}
__name(fire, "fire");
async function settleForeground(launched, ctx) {
  const rounds = Math.ceil(FOREGROUND_WAIT_MAX_MS / FOREGROUND_POLL_MS) + 1;
  let record = ctx.readJob(launched.id);
  const answered = /* @__PURE__ */ new Set();
  const permissionDir = join10(ctx.controlRoot, launched.id);
  for (let round = 0; round < rounds && !isTerminalState(record?.state); round += 1) {
    if (ctx.backgrounded.has(launched.id)) {
      ctx.refreshJobs();
      return {
        ok: true,
        id: launched.id,
        pid: launched.pid,
        result: {
          ok: true,
          partial: true,
          text: `${String(record?.description ?? launched.id)} continues in the background; its result will arrive as a signal.`
        }
      };
    }
    await ctx.answerPendingPermission(permissionDir, answered);
    await ctx.sleep(FOREGROUND_POLL_MS);
    record = ctx.readJob(launched.id);
  }
  if (record === void 0 || !isTerminalState(record.state)) {
    if (record !== void 0) ctx.writeJob({ ...record, background: true, updatedAt: ctx.now() });
    ctx.refreshJobs();
    return {
      ok: false,
      error: `foreground subagent ${launched.id} did not finish within ${Math.round(FOREGROUND_WAIT_MAX_MS / 1e3)}s; it continues in the background, and its result will arrive as a signal`
    };
  }
  const log = ctx.readLog(String(record.logPath ?? join10(ctx.logsDir, `${launched.id}.log`)));
  const decision = ctx.finalizeJobWorktree(record);
  const outcome = ctx.foregroundOutcome(record, log);
  if (decision?.kept !== true) return { ...launched, result: outcome };
  const note = `
worktree kept at ${decision.path}${decision.error === void 0 ? "" : ` (${decision.error})`}`;
  const result = outcome.ok === true ? { ok: true, text: `${outcome.text}${note}`, partial: outcome.partial } : { ok: false, error: `${outcome.error}${note}` };
  return { ...launched, result };
}
__name(settleForeground, "settleForeground");

// src/tools.ts
var okText = /* @__PURE__ */ __name((value) => ({ ok: true, content: [{ type: "text", text: value }] }), "okText");
var badInput = /* @__PURE__ */ __name((error) => ({ ok: false, error }), "badInput");
function createTools(ctx) {
  const {
    cmd,
    planCtx,
    spawnCtx,
    runtime,
    spool,
    logsDir,
    transcriptsDir,
    readJob,
    writeJob,
    readRecentJobs,
    refreshJobs,
    enqueueSignal,
    safeDrain,
    cancelJob,
    registry,
    ledger,
    currentOwner,
    ownerId,
    rosterNames,
    namedJob,
    liveState,
    appendLive,
    workerMode,
    workerSpec,
    sessionId,
    forkModeEnabled,
    listText,
    scanOutput: scanOutput2,
    reportText: reportText2,
    listAgentsVisible: listAgentsVisible2
  } = ctx;
  function addSurface(name, description, inputSchema, run, options = {}) {
    const readOnly = options.readOnly === true;
    cmd.addTool({ schema: { name, description, input_schema: inputSchema }, ...readOnly ? { readOnly } : {}, run });
    for (const alias of options.aliases ?? []) {
      cmd.addTool({
        schema: { name: alias, description: `[deprecated: use ${name}] ${description}`, input_schema: inputSchema },
        ...readOnly ? { readOnly } : {},
        run
      });
    }
  }
  __name(addSurface, "addSurface");
  const spawnRun = /* @__PURE__ */ __name(async ({ input }) => {
    const planned = planFromInput(input, planCtx);
    if (planned.ok === false) return badInput(planned.error);
    const plan = planned.plan;
    const launched = await fire(plan, spawnCtx);
    if (launched.ok === false) return badInput(launched.error);
    const spawnedAgentId = readJob(launched.id)?.agentId;
    if (launched.result !== void 0) {
      if (launched.result.ok === false) return badInput(launched.result.error);
      return okText(
        spawnedAgentId !== void 0 ? `agent id ${spawnedAgentId}
${launched.result.text}` : launched.result.text
      );
    }
    const kind = plan.kind === "command" ? "shell (/bin/sh -c)" : `${plan.fork === true ? "fork" : "agent"} (${String(plan.agentType ?? "agent")})${plan.teamName !== void 0 ? ` on team ${plan.teamName}` : ""}`;
    return okText(
      `spawned ${launched.id} "${plan.description}" as ${kind}, pid ${launched.pid}.
` + (spawnedAgentId !== void 0 ? `agent id ${spawnedAgentId}
` : "") + `log ${join11(logsDir, `${launched.id}.log`)}
The result arrives here as a signal when it exits - do not wait for it. subagent_output ${launched.id} reads the live log; subagent_cancel ${launched.id} stops it.`
    );
  }, "spawnRun");
  const listRun = /* @__PURE__ */ __name(async ({ input }) => {
    refreshJobs();
    const limit = Number(input.limit);
    return okText(
      listText({
        state: typeof input.state === "string" && input.state.length > 0 ? input.state : void 0,
        limit: Number.isFinite(limit) && limit > 0 ? Math.round(limit) : void 0
      })
    );
  }, "listRun");
  function readLog(file) {
    try {
      return spool.readFile(file);
    } catch {
      return "";
    }
  }
  __name(readLog, "readLog");
  function transcriptText(id, record) {
    const file = String(record.transcriptPath ?? join11(transcriptsDir, `${id}.jsonl`));
    let raw = "";
    try {
      raw = spool.readFile(file);
    } catch {
      return "";
    }
    const parts = [];
    for (const line of raw.split("\n")) {
      if (!line.trim()) continue;
      try {
        const entry = JSON.parse(line);
        const update = entry.update;
        if (update?.sessionUpdate === "agent_message_chunk" && typeof update.content?.text === "string") {
          parts.push(update.content.text);
        } else if (update?.sessionUpdate === "tool_call") {
          parts.push(`[tool] ${String(update.title ?? update.kind ?? "call")}
`);
        } else if (typeof entry.line === "string") {
          parts.push(`${entry.line}
`);
        }
      } catch {
      }
    }
    return parts.join("");
  }
  __name(transcriptText, "transcriptText");
  function outputText(id, input) {
    const record = readJob(id);
    if (record === void 0) return badInput(`subagent: no job ${id}`);
    const state = canonicalState(record.state);
    const file = String(record.logPath ?? join11(logsDir, `${id}.log`));
    const transcriptPath = String(record.transcriptPath ?? join11(transcriptsDir, `${id}.jsonl`));
    const transcript = record.kind === "agent" || record.kind === "fork" ? transcriptText(id, record) : "";
    const logText = readLog(file);
    const log = logText.length > 0 ? logText : transcript;
    const requested = Number(input.max_chars);
    const maxChars = Math.min(
      OUTPUT_MAX_CHARS_CAP,
      Number.isFinite(requested) && requested > 0 ? Math.max(200, Math.round(requested)) : OUTPUT_MAX_CHARS_DEFAULT
    );
    const offset = Number(input.offset);
    let body;
    let note = "";
    if (input.offset !== void 0 && Number.isFinite(offset) && offset >= 0) {
      body = log.slice(offset, offset + maxChars);
      note = `
[chars ${offset}-${Math.min(log.length, offset + maxChars)} of ${log.length}]`;
    } else if (log.length > maxChars) {
      body = log.slice(-maxChars);
      note = `
[last ${maxChars} of ${log.length} chars]`;
    } else {
      body = log;
    }
    const facts = [
      state,
      record.partial === true ? "partial" : "",
      typeof record.agentId === "string" ? `agent ${record.agentId}` : "",
      typeof record.exitCode === "number" ? `exit ${record.exitCode}` : "",
      typeof record.durationMs === "number" ? `${(record.durationMs / 1e3).toFixed(1)}s` : "",
      typeof record.model === "string" && record.model.length > 0 ? record.model : "",
      typeof record.agentType === "string" ? record.agentType : "",
      typeof record.owner === "string" && record.owner.length > 0 ? `owner ${record.owner.slice(-6)}` : ""
    ].filter(Boolean).join(" \xB7 ");
    const timing = state === "queued" || state === "running" ? `
(running ${Math.max(0, Math.round((runtime.now() - Number(record.startedAt ?? runtime.now())) / 1e3))}s; the result will arrive as a signal)` : "";
    const visible = reportText2(body, String(record.kind ?? "command"));
    return okText(
      `${stateMarker(record.state)} ${id} "${String(record.description ?? "(unlabelled)")}" \xB7 ${facts}
log ${file}` + (record.transcriptPath !== void 0 ? `
transcript ${transcriptPath}` : "") + `${timing}

` + (visible.length > 0 ? visible : "(no output yet)") + note
    );
  }
  __name(outputText, "outputText");
  const outputRun = /* @__PURE__ */ __name(async ({ input }) => {
    const id = String(input.id ?? "").trim();
    if (id.length === 0) return badInput("id is required");
    if (readJob(id) === void 0) return badInput(`subagent: no job ${id}`);
    const waitMs = Number(input.wait_ms);
    const wantsWait = input.wait === true || Number.isFinite(waitMs) && waitMs > 0;
    if (wantsWait) {
      const budget = Math.min(WAIT_MAX_MS, Number.isFinite(waitMs) && waitMs > 0 ? waitMs : WAIT_DEFAULT_MS);
      const rounds = Math.ceil(budget / WAIT_POLL_MS) + 1;
      for (let round = 0; round < rounds; round += 1) {
        const record = readJob(id);
        if (record === void 0 || isTerminalState(record.state)) break;
        await runtime.sleep(WAIT_POLL_MS);
      }
    }
    return outputText(id, input);
  }, "outputRun");
  const cancelRun = /* @__PURE__ */ __name(async ({ input }) => {
    const id = String(input.id ?? "").trim();
    if (id.length === 0) return badInput("id is required");
    const result = cancelJob(id);
    return result.ok ? okText(result.message) : badInput(result.message);
  }, "cancelRun");
  const resumeRun = /* @__PURE__ */ __name(async ({ input }) => {
    const id = String(input.id ?? "").trim();
    if (id.length === 0) return badInput("id is required");
    const instruction = String(input.prompt ?? "").trim();
    if (instruction.length === 0) return badInput("prompt is required: it is the instruction for the resumed run");
    const record = readJob(id) ?? readRecentJobs(ACTIVE_JOB_SCAN_MAX).find((job) => typeof job.agentId === "string" && job.agentId === id);
    if (record === void 0) return badInput(`subagent: no job or agent ${id}`);
    const jobId = record.id;
    const state = normalizeState(record.state);
    if (state !== void 0 && !isTerminal(state)) return badInput(`subagent: job ${jobId} is still ${state}`);
    if (record.prompt === void 0) {
      return badInput(`subagent: job ${jobId} is a shell job; there is no agent session to resume`);
    }
    if (record.fork !== true && record.agentId === void 0) {
      return badInput(
        `subagent: job ${jobId} has no agent id (${String(record.agentType ?? "explore or plan")} is one-shot, or the record predates agent ids); it cannot be resumed`
      );
    }
    if (liveState(record).alive) {
      return badInput(
        `subagent: job ${jobId} is still running; send it a live message with subagent_send` + (record.name === void 0 ? " once it finishes, or wait for its result" : `({to: "${record.name}", message}) instead`)
      );
    }
    const planned = planFromRecord(record, instruction, "resume", planCtx);
    if (planned.ok === false) return badInput(planned.error);
    const launched = await fire(planned.plan, spawnCtx);
    if (launched.ok === false) return badInput(launched.error);
    const resumedAgentId = readJob(launched.id)?.agentId;
    return launchedToolResult(
      launched,
      resumedAgentId,
      `resumed ${jobId} as ${launched.id} "${String(record.description ?? jobId)} (resume)", pid ${launched.pid}.
` + (resumedAgentId !== void 0 ? `agent id ${resumedAgentId}
` : "") + `log ${join11(logsDir, `${launched.id}.log`)}
` + (record.fork === true ? "The resumed fork reports back here as its own signal." : "The resumed run reports back here as its own signal.")
    );
  }, "resumeRun");
  function agentsToolText(scope) {
    registry.refresh();
    const wanted = String(scope ?? "").trim().toLowerCase();
    const entries = registry.entries().filter((entry) => wanted.length === 0 || entry.scope === wanted);
    if (entries.length === 0) return `no${wanted.length > 0 ? ` ${wanted}` : ""} agent definitions`;
    const lines = entries.map((entry) => {
      const state = entry.valid ? "valid" : `invalid: ${entry.reason ?? "unknown"}`;
      const model = entry.definition?.model !== void 0 ? ` model=${entry.definition.model}` : "";
      const tools = entry.definition?.tools !== void 0 ? ` tools=${entry.definition.tools.join(",")}` : "";
      const source = entry.sourcePath.length > 0 ? ` \xB7 ${entry.sourcePath}` : "";
      return `  ${entry.name.length > 0 ? entry.name : "(unnamed)"} \xB7 ${entry.scope} \xB7 ${state}${model}${tools}${source}`;
    });
    return `${entries.length} definition(s)${wanted.length > 0 ? ` in ${wanted}` : ""}:
${lines.join("\n")}`;
  }
  __name(agentsToolText, "agentsToolText");
  function listAgentsText() {
    const lines = [`${ROSTER_MAIN} \u2014 this conversation`];
    for (const name of rosterNames()) {
      const record = namedJob(name);
      if (record === void 0) continue;
      lines.push(`${name} \u2014 ${canonicalState(record.state)} (job ${record.id})`);
    }
    return lines.join("\n");
  }
  __name(listAgentsText, "listAgentsText");
  const noteRun = /* @__PURE__ */ __name(async ({ input }) => {
    const title = typeof input.title === "string" ? input.title : "";
    if (title.length === 0) return badInput("title is required");
    const body = typeof input.body === "string" ? input.body : "";
    const owner = currentOwner();
    const id = `direct-${runtime.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
    enqueueSignal({
      id,
      owner,
      description: title,
      kind: "note",
      state: "note",
      exitCode: null,
      title,
      body,
      sessionId: sessionId()
    });
    safeDrain();
    refreshJobs();
    return okText(ledger.has(id) ? "delivered" : "queued for this session");
  }, "noteRun");
  function launchedToolResult(launched, agentId, asyncText) {
    if (launched.result !== void 0) {
      if (launched.result.ok === false) return badInput(launched.result.error);
      return okText(agentId !== void 0 ? `agent id ${agentId}
${launched.result.text}` : launched.result.text);
    }
    return okText(asyncText);
  }
  __name(launchedToolResult, "launchedToolResult");
  async function sendToRecord(record, message, from) {
    const label = String(record.name ?? record.id);
    const { alive, busy } = liveState(record);
    if (record.fork !== true && record.agentId === void 0 && record.live !== true) {
      return badInput(`subagent: ${label} is one-shot (no agent id); it cannot be messaged`);
    }
    if (alive) {
      if (!appendLive(record.id, from, message)) return badInput(`cannot reach ${label}: its control queue is not writable`);
      return okText(
        `${busy ? `queued for ${label}'s next safe point` : `sent to idle ${label}; it starts a turn`} (job ${record.id}).
The reply arrives here as its own message signal.`
      );
    }
    const planned = planFromRecord(record, message, "message", planCtx);
    if (planned.ok === false) return badInput(planned.error);
    const launched = await fire(planned.plan, spawnCtx);
    if (launched.ok === false) return badInput(launched.error);
    return launchedToolResult(
      launched,
      readJob(launched.id)?.agentId,
      `resumed ${label} as ${launched.id} "${String(record.description ?? record.id)} (message)", pid ${launched.pid}.
The reply arrives here as its own signal.`
    );
  }
  __name(sendToRecord, "sendToRecord");
  const sendLiveRun = /* @__PURE__ */ __name(async ({ input }) => {
    const to = String(input.to ?? "").trim();
    const message = typeof input.message === "string" ? input.message : "";
    if (to.length === 0) return badInput("to is required: a named agent or main");
    if (message.trim().length === 0) return badInput("message is required and must not be empty");
    const from = workerMode ? String(workerSpec()?.name ?? "worker") : ROSTER_MAIN;
    if (to === ROSTER_MAIN) {
      const id = `live-${runtime.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
      const scan = scanOutput2(message);
      enqueueSignal({
        id,
        owner: workerMode ? workerSpec()?.owner : ownerId(),
        description: `message from ${from}`,
        kind: "message",
        state: "note",
        exitCode: null,
        title: `message from ${from}`,
        body: scan.text,
        sessionId: workerMode ? workerSpec()?.sessionId : sessionId(),
        ...workerMode ? { scanned: true, scanMatches: scan.matches } : {}
      });
      if (!workerMode) {
        safeDrain();
        refreshJobs();
      }
      return okText(`sent a message to ${ROSTER_MAIN} from ${from}`);
    }
    const record = namedJob(to);
    if (record === void 0) {
      const names = rosterNames();
      return badInput(
        `no agent named "${to}"${names.length > 0 ? `; addressable: ${names.join(", ")}` : "; no named agents in this session"}`
      );
    }
    return sendToRecord(record, message, from);
  }, "sendLiveRun");
  function register() {
    addSurface(
      "subagent_spawn",
      "Start a subagent. With `run_in_background: false` the call waits and returns the result; otherwise it returns immediately and the result arrives later as a separate message on its own - do not poll for it, and do not wait. Pass `prompt` for a headless Command Code agent, or `command` for a shell command. A definition with `background: true` or CLAUDE_CODE_DISABLE_BACKGROUND_TASKS can override the requested mode. Pass `team_name` to join an agent team as a foreground teammate. Pass `subagent_type: fork` to inherit the main conversation." + (forkModeEnabled ? " Fork mode is on: agent jobs run in the background and `run_in_background` is not accepted." : ""),
      {
        type: "object",
        properties: (() => {
          const properties = {
            description: { type: "string", description: "Short name for this job, used in the signal title." },
            label: { type: "string", description: "Alias for description." },
            name: { type: "string", description: "Address this agent later with subagent_send. A named agent appears in its siblings' roster." },
            team_name: { type: "string", description: "Join this agent team. A teammate runs in the foreground and keeps the task and cron tools." },
            prompt: { type: "string", description: "Prompt for a detached headless Command Code agent." },
            command: { type: "string", description: "Shell command to run detached under /bin/sh -c." },
            agent_type: { type: "string", description: "Deprecated alias for subagent_type. Use subagent_type." },
            subagent_type: {
              type: "string",
              description: "Agent definition to run, by name. Omit for 'general-purpose'. A project or user definition in .commandcode/agents/ overrides the built-ins."
            },
            model: { type: "string", description: "Model for an agent job. Overrides the definition's model; 'inherit' means the session model." },
            effort: { type: "string", description: "Reasoning effort for an agent job: low, medium, high, xhigh, or max. Overrides the definition's effort." },
            tools: { type: "string", description: "Comma-separated tool list (or an array). Only these tools remain; overrides the definition." },
            disallowed_tools: { type: "string", description: "Comma-separated tool list (or an array) to remove. Wins over tools." },
            max_turns: { type: "number", description: "Cap the agent conversation turns." },
            permission_mode: {
              type: "string",
              description: "Permission mode for an agent job: default, acceptEdits, auto, dontAsk, bypassPermissions, plan, or manual. The main session's mode wins when it is acceptEdits, auto, or bypassPermissions; bypassPermissions otherwise requires the main session to be in it."
            },
            timeout_ms: { type: "number", description: "Kill the job after this many milliseconds and report a timeout." },
            run_in_background: {
              type: "boolean",
              description: "false waits for the run and returns its result in this call; true (or omitted) returns immediately and delivers the result as a signal. A definition pinned to background, or CLAUDE_CODE_DISABLE_BACKGROUND_TASKS, can override this."
            },
            cwd: { type: "string", description: "Working directory. Defaults to the mod workspace root." }
          };
          if (forkModeEnabled) delete properties.run_in_background;
          return properties;
        })(),
        required: []
      },
      spawnRun,
      { aliases: ["pubsub_fire"] }
    );
    addSurface(
      "subagent_list",
      "List spawned jobs with their lifecycle states, subagent_type, agent name, model, elapsed time and owner, and count the signals still waiting for this session.",
      {
        type: "object",
        properties: {
          state: { type: "string", description: "Only jobs in this state (queued, running, waiting-input, done, failed, canceled)." },
          limit: { type: "number", description: "Show at most this many jobs." }
        },
        required: []
      },
      listRun,
      { readOnly: true, aliases: ["pubsub_list"] }
    );
    addSurface(
      "subagent_output",
      "Read one job: its state, exit code, duration and owner, plus a bounded slice of its log. By default the tail; `offset` reads from a character offset, `max_chars` bounds the slice.",
      {
        type: "object",
        properties: {
          id: { type: "string", description: "Job id from subagent_spawn or subagent_list." },
          wait: { type: "boolean", description: "Block briefly until the job finishes (bounded, 10s by default)." },
          wait_ms: { type: "number", description: "How long to wait, capped at 60s." },
          offset: { type: "number", description: "Character offset into the log." },
          max_chars: { type: "number", description: "Maximum characters to return." }
        },
        required: ["id"]
      },
      outputRun,
      { readOnly: true }
    );
    addSurface(
      "subagent_cancel",
      "Cancel a queued or running subagent job by id (SIGTERM, then SIGKILL after 1s). The firing session receives one canceled signal.",
      {
        type: "object",
        properties: { id: { type: "string", description: "Job id from subagent_list or subagent_spawn." } },
        required: ["id"]
      },
      cancelRun,
      { aliases: ["pubsub_cancel"] }
    );
    addSurface(
      "subagent_resume",
      "Resume a finished agent job in its own worker session and continue it with a new instruction. The resumed run is a new job with its own signal.",
      {
        type: "object",
        properties: {
          id: { type: "string", description: "Job id of the finished agent job." },
          prompt: { type: "string", description: "The instruction for the resumed run." }
        },
        required: ["id", "prompt"]
      },
      resumeRun
    );
    addSurface(
      "subagent_agents",
      "List the available agent definitions: scope, model, tools, validity, and the file that provides each one. Pass `scope` to filter to project, user, or builtin.",
      {
        type: "object",
        properties: {
          scope: { type: "string", description: "Only definitions from this scope (project, user, builtin)." }
        },
        required: []
      },
      async ({ input }) => okText(agentsToolText(typeof input.scope === "string" ? input.scope : void 0)),
      { readOnly: true }
    );
    const canMessageAcrossSessions = crossSessionEnabled(runtime.env);
    if (listAgentsVisible2({ background: workerMode ? workerSpec()?.background === true : false, crossSessionMessaging: canMessageAcrossSessions })) {
      addSurface(
        "ListAgents",
        "List the agents this conversation can message with subagent_send: main plus every named agent, with its current state and job id.",
        { type: "object", properties: {}, required: [] },
        async () => okText(listAgentsText()),
        { readOnly: true }
      );
    }
    if (workerSpec() !== void 0) {
      addSurface(
        "SubagentHandback",
        "Hand your final report back to the conversation that receives this run's result. The report is delivered as its own message; end the turn afterwards. The wrapper suppresses the duplicate end-of-turn result.",
        {
          type: "object",
          properties: {
            report: { type: "string", description: "The final report for the parent conversation." }
          },
          required: ["report"]
        },
        async ({ input }) => {
          const report = String(input.report ?? "").trim();
          if (report.length === 0) return badInput("report is required and must not be empty");
          const id = String(workerSpec()?.id ?? "");
          const record = id.length === 0 ? void 0 : readJob(id);
          if (record === void 0) return badInput(`subagent: no job ${id || "(worker)"}`);
          const now = runtime.now();
          const scan = scanOutput2(report);
          record.handbackAt = now;
          record.updatedAt = now;
          writeJob(record);
          enqueueSignal(
            signalOf({ ...record, id: `${id}-handback-${now.toString(36)}` }, {
              state: "done",
              finishedAt: now,
              durationMs: Math.max(0, now - Number(record.startedAt ?? now)),
              body: scan.text,
              scanned: true,
              scanMatches: scan.matches,
              title: `${String(record.description ?? id)} handback`
            })
          );
          refreshJobs();
          return okText(`handed the report back (job ${id}); it reaches the parent as one message`);
        }
      );
    }
    addSurface(
      "subagent_send",
      'Send a live message to a named agent (or "main"). A running agent queues it and answers at its next safe point; an idle agent starts a turn; a finished agent resumes its session with full context. The reply arrives later as its own message signal, so do not wait for it here.',
      {
        type: "object",
        properties: {
          to: { type: "string", description: 'A named agent, or "main" for this conversation.' },
          message: { type: "string", description: "The message to deliver. The target acts on it in its own session." }
        },
        required: ["to", "message"]
      },
      sendLiveRun
    );
    addSurface(
      "pubsub_send",
      "[deprecated: live messaging moved to subagent_send] Publish a note into the agent loop of THIS session. It is delivered as a visible user turn. This is not live messaging to another agent.",
      {
        type: "object",
        properties: {
          title: { type: "string", description: "One line, shown in the signal header." },
          body: { type: "string", description: "The detail." }
        },
        required: ["title"]
      },
      noteRun
    );
  }
  __name(register, "register");
  return { register, outputText, sendToRecord };
}
__name(createTools, "createTools");

// src/index.ts
function scanOutput(text) {
  const marker = "[harness: subagent output matched instruction-shaped pattern(s): ";
  const settings = [
    ["bypassPermissions", "bypassPermissions permission setting"],
    ["--dangerously-skip-permissions", "--dangerously-skip-permissions permission setting"]
  ];
  const matches = [];
  let marked = false;
  let out = String(text ?? "");
  out = out.replace(/<\/?system-reminder\b[^>]*>/gi, (match) => {
    if (!matches.includes("system-reminder tag")) matches.push("system-reminder tag");
    marked = true;
    return "\\" + match;
  });
  out = out.replace(/^([ \t]*)(Human|Assistant):/gm, (_match, space, label) => {
    const name = label + ": role label";
    if (!matches.includes(name)) matches.push(name);
    return space + "\\" + label + ":";
  });
  for (const [needle, name] of settings) {
    if (out.includes(needle) && !matches.includes(name)) {
      matches.push(name);
      marked = true;
    }
  }
  if (marked) out = marker + matches.join(", ") + "]\n" + out;
  return { text: out, matches, marked };
}
__name(scanOutput, "scanOutput");
function reportText(text, kind) {
  const visible = kind === "agent" ? text.split("\n").filter((line) => !/^\[tool[ \]]/.test(line)).join("\n") : text;
  return scanOutput(visible).text;
}
__name(reportText, "reportText");
var WORKER_SOURCE = `
const {spawn} = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');
const spec = JSON.parse(process.env.SUBAGENT_SPEC);
const recordPath = path.join(spec.jobsDir, spec.id + '.json');

// The adapter's own cadence. The parent sets both on every spec; the fallbacks
// are for a spec written by an older build.
const CONTROL_POLL_MS = typeof spec.controlPollMs === 'number' && spec.controlPollMs > 0 ? spec.controlPollMs : 100;
const PERMISSION_TIMEOUT_MS = typeof spec.permissionTimeoutMs === 'number' && spec.permissionTimeoutMs > 0 ? spec.permissionTimeoutMs : 30000;

const readRecord = () => {
  try { return JSON.parse(fs.readFileSync(recordPath, 'utf8')); } catch (e) { return {}; }
};

// The persisted conversation for this agent. Append-only JSONL: the ACP update
// stream in order, plus a result line and any compact boundary. It lives outside
// the main session, so the main transcript compacting can never touch it.
const transcriptPath = typeof spec.transcriptPath === 'string' ? spec.transcriptPath : '';
let transcriptFd;
const transcriptWrite = entry => {
  if (transcriptPath.length === 0) return;
  try {
    if (transcriptFd === undefined) {
      fs.mkdirSync(path.dirname(transcriptPath), {recursive: true});
      transcriptFd = fs.openSync(transcriptPath, 'a');
    }
    fs.writeSync(transcriptFd, JSON.stringify(entry) + '\\n');
  } catch (e) {}
};

// Canceled is terminal. Re-read inside this call (Node is single-threaded, so the
// read+write pair cannot interleave with the cancel tool) and force the state back
// if a cancellation landed while the child was exiting.
const writeRecord = patch => {
  try {
    const record = readRecord();
    const next = Object.assign({}, record, patch);
    if (record.state === 'canceled') next.state = 'canceled';
    next.updatedAt = Date.now();
    const tmp = recordPath + '.worker-' + process.pid;
    fs.writeFileSync(tmp, JSON.stringify(next, null, 2));
    fs.renameSync(tmp, recordPath);
  } catch (e) {}
};

const emit = message => {
  try {
    const tmp = path.join(spec.inboxDir, '.tmp-' + spec.id + '-' + process.pid + '-' + Date.now());
    fs.writeFileSync(tmp, JSON.stringify(message, null, 2));
    fs.renameSync(tmp, path.join(spec.inboxDir, Date.now() + '-' + spec.id + '-' + process.pid + '.json'));
  } catch (e) {}
};

const readTail = () => {
  try { return fs.readFileSync(spec.logPath, 'utf8').slice(-spec.tailMax); } catch (e) { return ''; }
};

// The envelope is the parent's signalEnvelope, injected here as source. Both
// sides therefore construct a signal from the same field list.
const signalEnvelope = ${signalEnvelope.toString()};

// The scanner is the parent's scanOutput, injected the same way: one rule table.
const scanOutput = ${scanOutput.toString()};

// The report tail drops the worker's own tool-call progress. Those lines stay in
// the log for the task view; the parent is never asked to act on a tool call.
// Shell output is passed through as written.
const readReportTail = () => {
  try {
    const tail = readTail();
    if (spec.kind !== 'agent') return tail;
    return tail.split('\\n').filter(line => !/^\\[tool[ \\]]/.test(line)).join('\\n');
  } catch (e) { return ''; }
};

let settled = false;

// One constructor for "this job reached this state": the record patch and the
// signal are derived from the same facts, so they cannot disagree. The optional
// extra argument carries the waiting-input payload (question, permissionRequest).
const settle = (state, detail, code, tail, extra) => {
  if (settled) return;
  settled = true;
  const finishedAt = Date.now();
  const durationMs = Math.max(0, finishedAt - spec.startedAt);
  const failed = state === 'failed';
  // SubagentHandback already sent the report to the parent. Record the terminal
  // state, but do not emit a second signal for the same run.
  if (readRecord().handbackAt) {
    writeRecord({
      state: state,
      exitCode: typeof code === 'number' ? code : undefined,
      finishedAt: finishedAt,
      durationMs: durationMs,
      failureReason: failed ? detail : undefined,
      awaitingInput: state === 'waiting-input' ? true : undefined
    });
    return;
  }
  const payload = extra || {};
  // Scan is the last step before the text can reach the parent; the record and
  // the signal carry the same facts.
  const scan = scanOutput(detail + (tail ? '\\n\\n' + tail : ''));
  writeRecord({
    state: state,
    exitCode: typeof code === 'number' ? code : undefined,
    finishedAt: finishedAt,
    durationMs: durationMs,
    failureReason: failed ? detail : undefined,
    awaitingInput: state === 'waiting-input' ? true : undefined,
    question: typeof payload.question === 'string' ? payload.question : undefined,
    permissionRequest: payload.permissionRequest,
    partial: payload.partial === true ? true : undefined,
    compactBoundary: payload.compactBoundary,
    scanned: true,
    scanMatches: scan.matches
  });
  transcriptWrite({
    at: finishedAt,
    type: 'result',
    state: state,
    exitCode: typeof code === 'number' ? code : null,
    partial: payload.partial === true,
    compactBoundary: payload.compactBoundary
  });
  emit(signalEnvelope({
    id: spec.id, description: spec.description, kind: spec.kind, owner: spec.owner,
    startedAt: spec.startedAt, logPath: spec.logPath, sessionId: spec.sessionId,
    ...(typeof spec.agentId === 'string' ? {agentId: spec.agentId} : {})
  }, Object.assign({
    state: state,
    exitCode: typeof code === 'number' ? code : null,
    finishedAt: finishedAt,
    durationMs: durationMs,
    body: scan.text,
    scanned: true,
    scanMatches: scan.matches
  }, payload)));
};

// A failed worker still returns its last output: partial work is never discarded.
const fail = (reason, code) => settle('failed', reason, code, readReportTail());
// A cut-off is not a plain failure: the last output is part of the result, so the
// foreground can return it as partial work and the background can deliver it.
const failApi = detail => settle('failed', ${JSON.stringify(AGENT_API_ERROR)} + (detail ? ': ' + detail : ''), null, readReportTail());

let out;
try {
  out = fs.openSync(spec.logPath, 'w');
} catch (e) {
  fail('cannot open log: ' + String(e && e.message || e), null);
  process.exit(0);
}
const logWrite = text => { try { fs.writeSync(out, text); } catch (e) {} };

// The fork path: the host CLI runs one print turn. stdout is the NDJSON event
// stream, so it is parsed instead of logged; the result line names the fork
// session, which is what a follow-up resumes.
const isFork = spec.kind === 'fork';
let forkStdout = '';
let forkSessionId = typeof spec.forkSessionId === 'string' ? spec.forkSessionId : undefined;

const child = isFork
  ? spawn(spec.bin, spec.args || [], {cwd: spec.cwd, stdio: ['ignore', 'pipe', 'pipe']})
  : spec.kind === 'agent'
    ? spawn(spec.bin, spec.args || [], {cwd: spec.cwd, stdio: ['pipe', 'pipe', 'pipe']})
    : spawn(spec.bin, spec.args || [], {cwd: spec.cwd, stdio: ['ignore', out, out]});

if (isFork && child.stdout && child.stderr) {
  child.stdout.on('data', chunk => { forkStdout += chunk.toString(); });
  child.stderr.on('data', chunk => logWrite(chunk.toString()));
}

let timedOut = false;
let killTimer;
let hardTimer;

// A timeout is still one result: SIGTERM, SIGKILL a second later, and a failed
// signal that says why. Nothing is left running and nothing is left silent.
if (typeof spec.timeoutMs === 'number' && spec.timeoutMs > 0) {
  killTimer = setTimeout(() => {
    timedOut = true;
    try { child.kill('SIGTERM'); } catch (e) {}
    hardTimer = setTimeout(() => { try { child.kill('SIGKILL'); } catch (e) {} }, 1000);
    if (hardTimer.unref) hardTimer.unref();
  }, spec.timeoutMs);
  if (killTimer.unref) killTimer.unref();
}

const clearTimers = () => {
  clearTimeout(killTimer);
  clearTimeout(hardTimer);
};

// The ACP child is long-lived only for the turn. After the signal is written,
// close its stdin, then make sure it cannot outlive the worker.
const shutdown = () => {
  try { child.stdin.end(); } catch (e) {}
  try { child.kill('SIGTERM'); } catch (e) {}
  setTimeout(() => {
    try { child.kill('SIGKILL'); } catch (e) {}
    process.exit(0);
  }, 1000);
};

const finish = (code, spawnError, signal) => {
  if (settled) return;
  clearTimers();
  const record = readRecord();
  // The cancel tool has already written the record and emitted its own signal.
  if (record.state === 'canceled') {
    settled = true;
    shutdown();
    return;
  }
  if (spec.kind === 'fork') {
    finishFork(code, spawnError, signal);
    return;
  }

  const tail = readTail();
  const timeoutLabel = spec.timeoutMs >= 1000 ? Math.round(spec.timeoutMs / 1000) + 's' : spec.timeoutMs + 'ms';
  if (spec.kind === 'agent') {
    const detail = timedOut
      ? 'timed out after ' + timeoutLabel
      : spawnError !== undefined
        ? 'spawn error: ' + String(spawnError && spawnError.message || spawnError)
        : 'ACP exited before the prompt completed (exit ' + String(code) + ')';
    if (timedOut || spawnError !== undefined) fail(detail, code); else failApi(detail);
    shutdown();
    return;
  }

  const lastLine = tail.trimEnd().split('\\n').filter(line => line.trim()).pop() || '';
  const failed = timedOut || spawnError !== undefined || code !== 0 || (typeof signal === 'string' && signal.length > 0);
  const wantsInput = !failed && /\\?\\s*$/.test(lastLine);
  const detail = timedOut
    ? 'timed out after ' + timeoutLabel
    : spawnError !== undefined
      ? 'spawn error: ' + String(spawnError && spawnError.message || spawnError)
      : typeof code === 'number' && code !== 0
        ? 'exit ' + code
        : signal
          ? 'terminated by ' + signal
          : 'exit 0';
  settle(failed ? 'failed' : wantsInput ? 'waiting-input' : 'done', detail, code, readReportTail(), wantsInput ? {question: lastLine} : undefined);
};

// One NDJSON parser for the fork path. Event lines become tool progress in the
// transcript; the result line carries finalText, the stop reason, and the new
// session id. Anything else is ignored, never guessed at.
const parseForkLines = text => {
  let result;
  const toolLines = [];
  for (const line of String(text).split('\\n')) {
    if (!line.trim()) continue;
    let parsed;
    try { parsed = JSON.parse(line); } catch (e) { continue; }
    if (parsed && parsed.type === 'event' && parsed.event) {
      const event = parsed.event;
      const name = String(event.name || event.toolName || event.tool || 'call');
      if (event.type === 'tool_running') toolLines.push('[tool] ' + name);
      else if (event.type === 'tool_completed') toolLines.push('[tool completed] ' + name);
    } else if (parsed && parsed.type === 'result') {
      result = parsed;
    }
  }
  return {result: result, toolLines: toolLines};
};



// The fork keeps the process alive after the first turn, exactly like a live ACP
// worker. A message cannot preempt the turn that is running - the host CLI has no
// mid-turn steering - so it becomes the next --resume <fork> -p turn.
const forkLiveLoop = async () => {
  const controlDir = typeof spec.controlDir === 'string' ? spec.controlDir : '';
  if (controlDir.length === 0) return;
  const sendPath = path.join(controlDir, 'send.jsonl');
  const cancelPath = path.join(controlDir, 'cancel');
  const statePath = path.join(controlDir, 'state.json');
  let offset = 0;
  let partial = '';
  let canceled = false;
  let turnChild;
  let messageCount = 0;
  const queue = [];
  const writeState = busy => {
    try {
      fs.mkdirSync(controlDir, {recursive: true});
      const tmp = statePath + '.tmp-' + process.pid;
      fs.writeFileSync(tmp, JSON.stringify({busy: !!busy, turns: messageCount, updatedAt: Date.now()}));
      fs.renameSync(tmp, statePath);
    } catch (e) {}
  };
  const readSend = () => {
    let size;
    try { size = fs.statSync(sendPath).size; } catch (e) { return; }
    if (size <= offset) return;
    let chunk = '';
    try {
      const fd = fs.openSync(sendPath, 'r');
      const length = size - offset;
      const buffer = Buffer.alloc(length);
      fs.readSync(fd, buffer, 0, length, offset);
      fs.closeSync(fd);
      offset = size;
      chunk = buffer.toString('utf8');
    } catch (e) { return; }
    const lines = (partial + chunk).split('\\n');
    partial = lines.pop() || '';
    for (const line of lines) {
      if (!line.trim()) continue;
      try {
        const entry = JSON.parse(line);
        if (entry && typeof entry.message === 'string' && entry.message.length > 0) queue.push(entry);
      } catch (e) {}
    }
  };
  const timer = setInterval(() => {
    readSend();
    if (!canceled) {
      try {
        if (fs.existsSync(cancelPath)) {
          canceled = true;
          logWrite('[live] cancel requested\\n');
          try { if (turnChild) turnChild.kill('SIGTERM'); } catch (e) {}
        }
      } catch (e) {}
    }
  }, CONTROL_POLL_MS);
  if (timer.unref) timer.unref();
  writeState(false);
  const idleMs = typeof spec.idleMs === 'number' && spec.idleMs >= 0 ? spec.idleMs : 30000;
  while (!canceled) {
    if (queue.length === 0) {
      const deadline = Date.now() + idleMs;
      while (queue.length === 0 && !canceled && Date.now() < deadline) {
        const {promise, resolve} = Promise.withResolvers();
        setTimeout(resolve, Math.min(50, CONTROL_POLL_MS));
        await promise;
      }
      if (queue.length === 0) {
        if (!canceled) logWrite('[live] idle window elapsed\\n');
        break;
      }
    }
    if (canceled) break;
    const entry = queue.shift();
    const from = typeof entry.from === 'string' ? entry.from : 'main';
    const text = String(entry.message);
    logWrite('\\n[live] message from ' + from + ': ' + text + '\\n');
    writeState(true);
    const startedAt = Date.now();
    let turnStdout = '';
    let turnCode = null;
    let turnError;
    try {
      turnChild = spawn(spec.bin, ['--resume', String(forkSessionId || ''), '-p', text, '--output-format', 'json'], {cwd: spec.cwd, stdio: ['ignore', 'pipe', 'pipe']});
      if (turnChild.stdout) turnChild.stdout.on('data', chunk => { turnStdout += chunk.toString(); });
      if (turnChild.stderr) turnChild.stderr.on('data', chunk => logWrite(chunk.toString()));
      const {promise, resolve} = Promise.withResolvers();
      turnChild.on('close', code => { turnCode = code; resolve(); });
      turnChild.on('error', error => { turnError = error; resolve(); });
      await promise;
    } catch (error) {
      turnError = error;
    }
    turnChild = undefined;
    if (canceled) break;
    const parsed = parseForkLines(turnStdout);
    if (parsed.result && typeof parsed.result.sessionId === 'string' && parsed.result.sessionId.length > 0) {
      forkSessionId = parsed.result.sessionId;
      writeRecord({forkSessionId: forkSessionId});
    }
    for (const line of parsed.toolLines) logWrite(line + '\\n');
    const reply = parsed.result && typeof parsed.result.finalText === 'string' ? parsed.result.finalText.trim() : '';
    if (reply.length > 0) {
      logWrite(reply + '\\n');
      transcriptWrite({at: Date.now(), update: {sessionUpdate: 'agent_message_chunk', content: {type: 'text', text: reply + '\\n'}}});
    }
    const failure = turnError !== undefined
      ? 'live turn failed: ' + String(turnError && turnError.message || turnError)
      : !parsed.result
        ? 'live turn failed: fork exited before a result (exit ' + String(turnCode) + ')'
        : parsed.result.subtype === 'error' || (typeof parsed.result.error === 'string' && parsed.result.error.length > 0)
          ? 'live turn failed: ' + String(parsed.result.error || 'API error')
          : undefined;
    messageCount += 1;
    const scan = scanOutput((failure === undefined ? 'reply to ' + from + ':' : failure + '\\nreply to ' + from + ':') + '\\n' + reply);
    emit(signalEnvelope({
      id: spec.id + '-msg-' + messageCount,
      description: spec.description + ' (message)',
      kind: 'message',
      owner: spec.owner,
      startedAt: startedAt,
      logPath: spec.logPath,
      sessionId: spec.sessionId,
      ...(typeof spec.agentId === 'string' ? {agentId: spec.agentId} : {})
    }, {
      state: failure === undefined ? 'done' : 'failed',
      exitCode: failure === undefined ? 0 : null,
      finishedAt: Date.now(),
      durationMs: 0,
      title: (spec.name || spec.description) + (failure === undefined ? ' replied' : ' failed a live turn'),
      body: scan.text,
      scanned: true,
      scanMatches: scan.matches
    }));
    writeState(false);
  }
  clearInterval(timer);
  try { if (turnChild) turnChild.kill('SIGTERM'); } catch (e) {}
};

// The first fork turn. A result line is success; an event stream with no result,
// a timeout, or an error subtype is a failure that still carries the last text.
const finishFork = (code, spawnError, signal) => {
  if (settled) return;
  clearTimers();
  const record = readRecord();
  if (record.state === 'canceled') {
    settled = true;
    shutdown();
    return;
  }
  const parsed = parseForkLines(forkStdout);
  const result = parsed.result;
  if (result && typeof result.sessionId === 'string' && result.sessionId.length > 0) {
    forkSessionId = result.sessionId;
    writeRecord({forkSessionId: forkSessionId});
  }
  for (const line of parsed.toolLines) logWrite(line + '\\n');
  const text = result && typeof result.finalText === 'string' ? result.finalText.trim() : '';
  if (text.length > 0) {
    logWrite(text.charAt(text.length - 1) === '\\n' ? text : text + '\\n');
    transcriptWrite({at: Date.now(), update: {sessionUpdate: 'agent_message_chunk', content: {type: 'text', text: text + '\\n'}}});
  }
  const timeoutLabel = spec.timeoutMs >= 1000 ? Math.round(spec.timeoutMs / 1000) + 's' : spec.timeoutMs + 'ms';
  const errorText = result && typeof result.error === 'string' ? result.error : '';
  const stopReason = result && typeof result.stopReason === 'string' ? result.stopReason : '';
  const detail = timedOut
    ? 'timed out after ' + timeoutLabel
    : spawnError !== undefined
      ? 'spawn error: ' + String(spawnError && spawnError.message || spawnError)
      : !result
        ? 'fork exited before a result (exit ' + String(code) + ')'
        : errorText.length > 0
          ? errorText
          : 'API error';
  if (timedOut || spawnError !== undefined || !result) {
    fail(detail, code);
    shutdown();
    return;
  }
  if (result.subtype === 'error' || errorText.length > 0) {
    failApi(detail);
    shutdown();
    return;
  }
  if (stopReason === 'max_turn_requests' || stopReason === 'max_tokens' || stopReason === 'max_turns') {
    settle('failed', 'max turns reached; result is partial', code, text, {partial: true});
  } else {
    settle('done', 'exit 0', code === null ? 0 : code, text);
  }
  if (spec.live === true) {
    forkLiveLoop().then(shutdown).catch(() => shutdown());
    return;
  }
  shutdown();
};

if (typeof child.pid === 'number') {
  writeRecord({state: 'running', pid: child.pid, workerPid: process.pid});
}

child.on('exit', (code, signal) => { if (!isFork) finish(code, undefined, signal); });
if (isFork) child.on('close', (code, signal) => finishFork(code, undefined, signal));
child.on('error', err => finish(null, err));

// The ACP client. One child, one session, one prompt. session/new creates the
// session and returns its id, modes, and model/effort config options; the order
// is initialize, session/new (or session/resume), set_mode, set_config_option,
// session/prompt. Updates stream to the job log.
if (spec.kind === 'agent' && child.stdout && child.stderr) {
  let nextId = 1;
  const pending = new Map();
  const sendMessage = message => {
    try { child.stdin.write(JSON.stringify(Object.assign({jsonrpc: '2.0'}, message)) + '\\n'); } catch (e) {}
  };
  const request = (method, params) => {
    const {promise, resolve, reject} = Promise.withResolvers();
    const id = nextId++;
    pending.set(id, {resolve: resolve, reject: reject});
    sendMessage({id: id, method: method, params: params});
    return promise;
  };
  const reply = (id, result) => sendMessage({id: id, result: result});
  const replyError = (id, code, message) => sendMessage({id: id, error: {code: code, message: message}});

  // Permission prompts. ACP asks; the user decides. A background run cannot ask,
  // so it settles waiting-input with the request on the signal and exits (the job
  // stays resumable). A foreground run writes the request to its control dir and
  // waits, bounded, for the parent's answer; Esc answers cancelled, which denies
  // one call and lets the turn continue. dontAsk and plan deny without asking;
  // bypass allows, because the main session already did.
  const permissionFile = spec.controlDir ? path.join(spec.controlDir, 'permission.json') : '';
  const permissionResponseFile = spec.controlDir ? path.join(spec.controlDir, 'permission-response.json') : '';

  const writeJsonAtomic = (file, value) => {
    try {
      fs.mkdirSync(path.dirname(file), {recursive: true});
      const tmp = file + '.tmp-' + process.pid;
      fs.writeFileSync(tmp, JSON.stringify(value, null, 2));
      fs.renameSync(tmp, file);
      return true;
    } catch (e) { return false; }
  };

  const readPermissionResponse = () => {
    if (permissionResponseFile.length === 0) return undefined;
    try {
      const response = JSON.parse(fs.readFileSync(permissionResponseFile, 'utf8'));
      try { fs.rmSync(permissionResponseFile, {force: true}); } catch (e) {}
      try { fs.rmSync(permissionFile, {force: true}); } catch (e) {}
      return response;
    } catch (e) { return undefined; }
  };

  const waitForPermissionResponse = () => {
    const timeout = PERMISSION_TIMEOUT_MS;


    const deadline = Date.now() + timeout;
    const {promise, resolve} = Promise.withResolvers();
    const tick = () => {
      const response = readPermissionResponse();
      if (response !== undefined) return resolve(response);
      if (Date.now() >= deadline) return resolve(undefined);
      const timer = setTimeout(tick, 25);
      if (timer.unref) timer.unref();
    };
    tick();
    return promise;
  };

  const answerPermission = (message, decision, options) => {
    if (decision && decision.outcome === 'cancelled') {
      reply(message.id, {outcome: {outcome: 'cancelled'}});
      return 'denied';
    }
    const wanted = decision && typeof decision.optionId === 'string' ? decision.optionId : undefined;
    const selected = wanted !== undefined
      ? options.find(option => option.optionId === wanted)
      : options.find(option => option.kind === 'allow_once')
        || options.find(option => option.kind === 'allow_always')
        || options.find(option => option.kind.indexOf('allow') === 0);
    if (selected && selected.optionId.length > 0) {
      reply(message.id, {outcome: {outcome: 'selected', optionId: selected.optionId}});
      return 'allowed';
    }
    reply(message.id, {outcome: {outcome: 'cancelled'}});
    return 'denied';
  };

  const handlePermission = async message => {
    const rawOptions = message.params && Array.isArray(message.params.options) ? message.params.options : [];
    const options = rawOptions.map(option => ({
      optionId: String(option.optionId || ''),
      kind: String(option.kind || ''),
      name: String(option.name || '')
    }));
    const toolCall = message.params && message.params.toolCall;
    const rawTitle = toolCall ? String(toolCall.title || toolCall.kind || '') : '';
    const title = rawTitle.length > 0 ? rawTitle : 'a tool call';
    const mode = spec.permissionMode;
    logWrite('\\n[permission] ' + title + '\\n');
    if (mode === 'bypassPermissions') {
      logWrite('[permission] bypassPermissions -> ' + answerPermission(message, undefined, options) + '\\n');
      return;
    }
    if (mode === 'dontAsk' || mode === 'plan') {
      logWrite('[permission] ' + mode + ' -> ' + answerPermission(message, {outcome: 'cancelled'}, options) + '\\n');
      return;
    }
    if (spec.interactive === true && permissionFile.length > 0) {
      const request = {
        requestId: message.id,
        title: title,
        toolCall: toolCall ? {title: title, kind: String(toolCall.kind || '')} : undefined,
        options: options
      };
      if (writeJsonAtomic(permissionFile, request)) {
        // A response left over from an earlier prompt must not answer this one.
        try { fs.rmSync(permissionResponseFile, {force: true}); } catch (e) {}
        logWrite('[permission] waiting for the main session\\n');
        const response = await waitForPermissionResponse();
        if (response !== undefined) {
          logWrite('[permission] ' + answerPermission(message, response, options) + ' by the main session\\n');
          return;
        }
        logWrite('[permission] no answer; waiting-input\\n');
      }
    }
    const names = options.map(option => option.name).filter(name => name.length > 0);
    settle('waiting-input', 'approval required: ' + title + (names.length > 0 ? ' (' + names.join(', ') + ')' : ''), null, '', {
      question: 'Approve "' + title + '"?' + (names.length > 0 ? ' Options: ' + names.join(', ') + '.' : ''),
      permissionRequest: {requestId: message.id, title: title, options: options}
    });
    shutdown();
  };

  // The host publishes usage after each turn. Auto-compaction itself is host-side;
  // the observable event is the crossing of this agent's window with the token
  // count from before it. A new high-water mark per turn keeps one crossing from
  // emitting duplicates. Each subagent has its own session, so windows never mix.
  let compaction;
  const maybeCompact = update => {
    if (!update || update.sessionUpdate !== 'usage_update') return;
    const used = Number(update.used);
    if (!Number.isFinite(used) || used <= 0) return;
    const window = typeof spec.autoCompactWindow === 'number' ? spec.autoCompactWindow : undefined;
    if (window === undefined || used < window) return;
    if (compaction !== undefined && used <= compaction.preTokens) return;
    compaction = {trigger: 'auto', preTokens: Math.round(used), at: Date.now()};
    writeRecord({compactBoundary: compaction});
    transcriptWrite({at: compaction.at, type: 'compact_boundary', trigger: 'auto', preTokens: compaction.preTokens});
    logWrite('\\n[harness: compact_boundary ' + JSON.stringify(compaction) + ']\\n');
  };

  const handleLine = line => {
    let message;
    try { message = JSON.parse(line); } catch (e) { return; }
    if (message && typeof message.method === 'string') {
      if (message.method === 'session/update') {
        const update = message.params && message.params.update;
        if (!update) return;
        // The transcript is append-only and independent of the log, so a restart can
        // read the conversation even if the log was rotated or the session compacted.
        transcriptWrite({at: Date.now(), update: update});
        maybeCompact(update);
        if (update.sessionUpdate === 'agent_message_chunk' && update.content && typeof update.content.text === 'string') {
          logWrite(update.content.text);
        } else if (update.sessionUpdate === 'tool_call') {
          logWrite('\\n[tool] ' + String(update.title || update.kind || 'call') + '\\n');
        } else if (update.sessionUpdate === 'tool_call_update' && update.status) {
          logWrite('[tool ' + String(update.status) + '] ' + String(update.title || '') + '\\n');
        }
        return;
      }
      if (message.method === 'session/request_permission') {
        handlePermission(message).catch(error => {
          logWrite('[permission] error: ' + String(error && error.message || error) + '\\n');
          settle('waiting-input', 'approval required', null, '', {
            question: 'Approve a tool call?',
            permissionRequest: {requestId: message.id, title: 'a tool call', options: []}
          });
          shutdown();
        });
        return;
      }
      if (message.method === 'fs/read_text_file') {
        try { reply(message.id, {content: fs.readFileSync(message.params.path, 'utf8')}); }
        catch (e) { replyError(message.id, -32000, String(e && e.message || e)); }
        return;
      }
      if (message.method === 'fs/write_text_file') {
        try {
          fs.mkdirSync(path.dirname(message.params.path), {recursive: true});
          fs.writeFileSync(message.params.path, String(message.params.content || ''));
          reply(message.id, null);
        } catch (e) { replyError(message.id, -32000, String(e && e.message || e)); }
        return;
      }
      if (message.id !== undefined) replyError(message.id, -32601, 'method not found: ' + message.method);
      return;
    }
    if (message && message.id !== undefined && pending.has(message.id)) {
      const entry = pending.get(message.id);
      pending.delete(message.id);
      if (message.error) entry.reject(new Error(String(message.error.message || 'ACP error')));
      else entry.resolve(message.result);
    }
  };

  let lineBuffer = '';
  child.stdout.on('data', chunk => {
    lineBuffer += chunk.toString();
    let index;
    while ((index = lineBuffer.indexOf('\\n')) >= 0) {
      const line = lineBuffer.slice(0, index).trim();
      lineBuffer = lineBuffer.slice(index + 1);
      if (line.length > 0) handleLine(line);
    }
  });
  child.stderr.on('data', chunk => logWrite(chunk.toString().replace(/\\s+$/, '') + '\\n'));

  // Spec permissionMode -> ACP mode id. auto is deliberately absent: it is the
  // host classifier, which has no ACP equivalent, so the adapter leaves ACP in its
  // own mode and logs it. Every other mode maps to a real ACP mode.
  const modeIds = {
    default: 'default',
    acceptEdits: 'auto-accept',
    dontAsk: 'dont-ask',
    plan: 'plan',
    manual: 'default',
    bypassPermissions: 'bypass',
    // No ACP classifier exists; prompting is the closest observable behavior.
    auto: 'default'
  };

  // ---- live messaging ----
  // One control directory per job: send.jsonl is appended by the sender, cancel is
  // a marker, and state.json says whether a turn is active. Polling (not watch)
  // keeps this portable; the send file is append-only and a byte offset means each
  // line is read exactly once, even while a turn is running.
  const controlDir = typeof spec.controlDir === 'string' ? spec.controlDir : '';
  const sendPath = controlDir.length > 0 ? path.join(controlDir, 'send.jsonl') : '';
  const cancelPath = controlDir.length > 0 ? path.join(controlDir, 'cancel') : '';
  const statePath = controlDir.length > 0 ? path.join(controlDir, 'state.json') : '';
  const queue = [];
  let sendOffset = 0;
  let sendPartial = '';
  let canceled = false;
  let cancelSent = false;
  let activeSessionId;
  let turnCount = 0;
  let waiting = null;
  let childGone = false;

  const wake = () => {
    const resolve = waiting;
    waiting = null;
    if (resolve) resolve();
  };

  // The parent reads this to tell "queued for the next safe point" from "starts a
  // turn now". Atomic rename, so a reader never sees a half-written file.
  const writeState = busy => {
    if (statePath.length === 0) return;
    try {
      fs.mkdirSync(controlDir, {recursive: true});
      const tmp = statePath + '.tmp-' + process.pid;
      fs.writeFileSync(tmp, JSON.stringify({busy: !!busy, turns: turnCount, updatedAt: Date.now()}));
      fs.renameSync(tmp, statePath);
    } catch (e) {}
  };

  const readSend = () => {
    if (sendPath.length === 0) return;
    let size;
    try { size = fs.statSync(sendPath).size; } catch (e) { return; }
    if (size <= sendOffset) return;
    let chunk = '';
    try {
      const fd = fs.openSync(sendPath, 'r');
      const length = size - sendOffset;
      const buffer = Buffer.alloc(length);
      fs.readSync(fd, buffer, 0, length, sendOffset);
      fs.closeSync(fd);
      sendOffset = size;
      chunk = buffer.toString('utf8');
    } catch (e) { return; }
    const lines = (sendPartial + chunk).split('\\n');
    sendPartial = lines.pop() || '';
    let added = false;
    for (const line of lines) {
      if (!line.trim()) continue;
      try {
        const entry = JSON.parse(line);
        if (entry && typeof entry.message === 'string' && entry.message.length > 0) {
          queue.push(entry);
          added = true;
        }
      } catch (e) {}
    }
    if (added) wake();
  };

  const pollControl = () => {
    if (cancelPath.length > 0 && !canceled) {
      try {
        if (fs.existsSync(cancelPath)) {
          canceled = true;
          logWrite('[live] cancel requested\\n');
          wake();
        }
      } catch (e) {}
    }
    readSend();
    if (canceled && !cancelSent && typeof activeSessionId === 'string') {
      cancelSent = true;
      sendMessage({id: nextId++, method: 'session/cancel', params: {sessionId: activeSessionId}});
    }
    if (child.exitCode !== null || child.signalCode !== null) {
      childGone = true;
      wake();
    }
  };

  const controlTimer = setInterval(pollControl, CONTROL_POLL_MS);
  if (controlTimer.unref) controlTimer.unref();
  pollControl();

  // A turn's terminal reason, or undefined for a completed turn. The first turn
  // settles the job (existing behavior); a live turn emits a message signal and
  // leaves the session open.
  const turnOutcome = result => {
    clearTimers();
    const stopReason = result && result.stopReason;
    if (stopReason === 'end_turn') return undefined;
    if (stopReason === 'cancelled') return 'cancelled';
    if (stopReason === 'max_tokens') return 'max tokens reached; result is partial';
    if (stopReason === 'max_turn_requests') return 'max turns reached; result is partial';
    return 'ACP turn ended: ' + String(stopReason || 'unknown');
  };

  const emitTurn = (from, start, failure) => {
    turnCount += 1;
    let tail = '';
    try { tail = fs.readFileSync(spec.logPath, 'utf8').slice(start); } catch (e) { tail = ''; }
    const prefix = failure === undefined
      ? 'reply to ' + String(from || 'main') + ':'
      : failure + '\\nreply to ' + String(from || 'main') + ':';
    const scan = scanOutput(prefix + '\\n' + tail);
    const partial = typeof failure === 'string' && /result is partial/.test(failure);
    emit(signalEnvelope({
      id: spec.id + '-msg-' + turnCount,
      description: spec.description + ' (message)',
      kind: 'message',
      owner: spec.owner,
      startedAt: Date.now(),
      logPath: spec.logPath,
      sessionId: spec.sessionId,
      ...(typeof spec.agentId === 'string' ? {agentId: spec.agentId} : {})
    }, Object.assign({
      state: failure === undefined ? 'done' : 'failed',
      exitCode: failure === undefined ? 0 : null,
      finishedAt: Date.now(),
      durationMs: 0,
      title: (spec.name || spec.description) + (failure === undefined ? ' replied' : ' failed a live turn'),
      body: scan.text,
      scanned: true,
      scanMatches: scan.matches
    }, partial ? {partial: true} : {})));
  };

  // After the first turn: wait for a message, run it as its own prompt, and only
  // exit once the idle window passes. A message that arrives mid-turn is queued by
  // the poller and picked up here, so the running turn is never restarted.
  const liveLoop = async () => {
    if (spec.live !== true) return;
    const idleMs = typeof spec.idleMs === 'number' && spec.idleMs >= 0 ? spec.idleMs : 30000;
    writeState(false);
    while (!canceled && !childGone) {
      if (queue.length === 0) {
        const {promise, resolve} = Promise.withResolvers();
        waiting = resolve;
        if (queue.length > 0 || canceled || childGone) {
          waiting = null;
          resolve();
        }
        const deadline = setTimeout(resolve, idleMs);
        if (deadline.unref) deadline.unref();
        await promise;
        clearTimeout(deadline);
        waiting = null;
        if (queue.length === 0) {
          if (!canceled && !childGone) logWrite('[live] idle window elapsed\\n');
          return;
        }
      }
      if (canceled || childGone) return;
      const entry = queue.shift();
      const from = typeof entry.from === 'string' ? entry.from : 'main';
      const text = String(entry.message);
      logWrite('\\n[live] message from ' + from + ': ' + text + '\\n');
      writeState(true);
      let start = 0;
      try { start = fs.statSync(spec.logPath).size; } catch (e) { start = 0; }
      let result;
      try {
        result = await request('session/prompt', {sessionId: activeSessionId, prompt: [{type: 'text', text: text}]});
      } catch (error) {
        logWrite('[live] turn failed: ' + String(error && error.message || error) + '\\n');
        emitTurn(from, start, 'live turn failed: ' + String(error && error.message || error));
        writeState(false);
        continue;
      }
      const outcome = turnOutcome(result);
      if (canceled && (outcome === 'cancelled' || outcome === undefined)) return;
      emitTurn(from, start, outcome);
      writeState(false);
      if (outcome !== undefined) return;
    }
  };

  const runPrompt = async () => {
    await request('initialize', {
      protocolVersion: 1,
      clientCapabilities: {fs: {readTextFile: true, writeTextFile: true}, terminal: false}
    });
    // session/resume returns the mode/config state, not a sessionId: the resumed id
    // is the one the worker sent. session/new is the only place an id is discovered.
    let sessionId;
    let configOptions = [];
    // The MCP servers are resolved in the parent and scoped to this agent;
    // session/new is where ACP attaches them to this job's session.
    const mcpServers = Array.isArray(spec.mcpServers) ? spec.mcpServers : [];
    if (mcpServers.length > 0) logWrite('[acp] mcp servers ' + mcpServers.map(server => server.name).join(', ') + '\\n');
    // ACP exposes none of these as a per-session option. Record the resolved values
    // so the deviation is visible in the job log instead of silent; the host default
    // still governs.
    if (typeof spec.promptCache === 'string') {
      logWrite('[acp] prompt cache ' + spec.promptCache + (spec.promptCache === 'shared' ? ' (the main conversation)' : ' (this agent)') + '\\n');
    }
    if (typeof spec.autoCompactWindow === 'number') {
      logWrite('[acp] auto-compact window ' + spec.autoCompactWindow + ' requested; ACP has no option (host default applies)\\n');
    }
    if (typeof spec.cacheTtl === 'string') {
      logWrite('[acp] cache TTL ' + spec.cacheTtl + ' requested; ACP has no option (host default applies)\\n');
    }
    if (typeof spec.resumeSession === 'string' && spec.resumeSession.length > 0) {
      try {
        const state = await request('session/resume', {sessionId: spec.resumeSession, cwd: spec.cwd, mcpServers: mcpServers});
        sessionId = spec.resumeSession;
        configOptions = state && Array.isArray(state.configOptions) ? state.configOptions : [];
        logWrite('[acp] resumed ' + sessionId + '\\n');
      } catch (error) {
        const detail = String(error && error.message || error);
        logWrite('[acp] resume failed: ' + detail + '\\n');
        throw new Error('session/resume failed: ' + detail);
      }
    }
    if (sessionId === undefined) {
      const session = await request('session/new', {cwd: spec.cwd, mcpServers: mcpServers});
      if (!session || typeof session.sessionId !== 'string') throw new Error('session/new returned no sessionId');
      sessionId = session.sessionId;
      configOptions = Array.isArray(session.configOptions) ? session.configOptions : [];
    }
    writeRecord({acpSessionId: sessionId});
    activeSessionId = sessionId;
    logWrite('[acp] session ' + sessionId + ' cwd ' + spec.cwd + '\\n');
    if (typeof spec.permissionMode === 'string') {
      if (modeIds[spec.permissionMode]) {
        if (spec.permissionMode === 'auto') {
          // The host's auto classifier has no ACP mode. The prompting mode is the
          // closest observable behavior: nothing is auto-allowed (ADR 0001).
          logWrite('[acp] permissionMode auto -> default (no ACP classifier; the prompting mode is the closest behavior)\\n');
        }
        try {
          await request('session/set_mode', {sessionId: sessionId, modeId: modeIds[spec.permissionMode]});
        } catch (error) {
          logWrite('[acp] mode not set: ' + String(error && error.message || error) + '\\n');
        }
      }
    }
    // The tool set is resolved in the parent and enforced by this mod's
    // beforeToolCall in the ACP child; the log records the same set.
    if (Array.isArray(spec.tools)) {
      logWrite('[acp] tools ' + spec.tools.length + (spec.tools.length > 0 ? ': ' + spec.tools.join(', ') : '') + '\\n');
    }
    // Family aliases resolve to the newest model the session offers, matching the
    // host's catalog order. A full id passes through unchanged. When the catalog is
    // known and blocks the value, the inherited (main) model is used and the
    // substitution is logged: resolution never silently picks another model.
    const modelOption = configOptions.find(option => option && option.id === 'model');
    const modelOptions = (modelOption && modelOption.options) || [];
    const inheritedModel = modelOption && typeof modelOption.currentValue === 'string' ? modelOption.currentValue : undefined;
    const modelAliases = {sonnet: /sonnet/i, opus: /opus/i, haiku: /haiku/i, fable: /fable/i};
    let modelValue = spec.model;
    if (typeof modelValue === 'string' && modelAliases[modelValue.toLowerCase()]) {
      const match = modelOptions.find(option => option && modelAliases[modelValue.toLowerCase()].test(String(option.name || '')));
      if (match && typeof match.value === 'string') {
        logWrite('[acp] model alias ' + modelValue + ' -> ' + match.value + '\\n');
        modelValue = match.value;
      } else if (modelOptions.length > 0) {
        logWrite('[acp] model alias ' + modelValue + ' is not in the allowed catalog; inheriting ' + String(inheritedModel || 'the session default') + '\\n');
        modelValue = inheritedModel;
      }
    } else if (typeof modelValue === 'string' && modelOptions.length > 0 && !modelOptions.some(option => option && String(option.value) === modelValue)) {
      logWrite('[acp] model ' + modelValue + ' is not in the allowed catalog; inheriting ' + String(inheritedModel || 'the session default') + '\\n');
      modelValue = inheritedModel;
    }
    for (const option of ['model', 'effort']) {
      const value = option === 'model' ? modelValue : spec[option];
      if (typeof value !== 'string' || value.length === 0 || value === 'inherit') continue;
      try {
        await request('session/set_config_option', {sessionId: sessionId, configId: option, value: value});
      } catch (error) {
        logWrite('[acp] ' + option + ' not set: ' + String(error && error.message || error) + '\\n');
      }
    }
    // The definition body belongs in the system prompt. That path depends on the mod
    // being loaded by the ACP child (installed), which writes promptMarkerPath from
    // its factory. When the marker is absent, the body is carried in the prompt so
    // it can never silently disappear.
    const body = typeof spec.systemPrompt === 'string' ? spec.systemPrompt : '';
    const rosterText = typeof spec.roster === 'string' ? spec.roster : '';
    const startupText = typeof spec.startup === 'string' ? spec.startup : '';
    let hookReady = false;
    if (typeof spec.promptMarkerPath === 'string' && spec.promptMarkerPath.length > 0) {
      try { hookReady = fs.existsSync(spec.promptMarkerPath); } catch (e) { hookReady = false; }
    }
    // The tool check lives in this mod's beforeToolCall. If the ACP child did not
    // load the mod, a restricted agent would keep every host tool; refuse it
    // rather than run with a false sense of enforcement.
    if (spec.restrictedTools === true && !hookReady) {
      settle('failed', 'restricted tools require the worker hook; the ACP child did not load the mod', null, '', {});
      shutdown();
      return;
    }
    let promptText = spec.prompt;
    const preamble = [];
    if (body.length > 0 && !hookReady) preamble.push('AGENT DEFINITION (your system prompt):\\n' + body);
    if (rosterText.length > 0 && !hookReady) preamble.push(rosterText);
    if (startupText.length > 0 && !hookReady) preamble.push(startupText);
    if (preamble.length > 0) {
      promptText = preamble.join('\\n\\n') + '\\n\\nTASK:\\n' + String(spec.prompt);
    }
    if (body.length > 0 && !hookReady) {
      logWrite('[acp] mod not loaded in the ACP child; the definition body is in the prompt\\n');
    }
    if (startupText.length > 0) {
      logWrite('[acp] startup context ' + startupText.length + ' chars (hook ' + (hookReady ? 'live' : 'absent') + ')\\n');
    }
    const result = await request('session/prompt', {sessionId: sessionId, prompt: [{type: 'text', text: promptText}]});
    const stopReason = result && typeof result.stopReason === 'string' ? result.stopReason : '';
    // maxTurns and max_tokens stop with what the agent already produced. The result
    // is partial, not lost: the record and signal say so and the job stays resumable.
    const partial = stopReason === 'max_turns' || stopReason === 'max_turn_requests' || stopReason === 'max_tokens';
    const outcome = turnOutcome(result);
    if (outcome !== undefined) {
      settle('failed', outcome, null, readReportTail(), Object.assign(
        partial ? {partial: true} : {},
        compaction === undefined ? {} : {compactBoundary: compaction}
      ));
      shutdown();
      return;
    }
    // The first turn is the job's result, as before. With live messaging the
    // session then stays open: the record is done, but the process (and its ACP
    // session) keeps accepting messages until the idle window passes.
    settle('done', 'exit 0', 0, readReportTail(), compaction === undefined ? undefined : {compactBoundary: compaction});
    await liveLoop();
    shutdown();
  };
  runPrompt().catch(error => {
    failApi('ACP session failed: ' + String(error && error.message || error));
    shutdown();
  });
}

process.on('uncaughtException', err => {
  try { fail('worker crash: ' + String(err && err.message || err), null); } catch (e) {}
  process.exit(0);
});
`;
function osSpoolFs() {
  return {
    mkdir: /* @__PURE__ */ __name((dir) => {
      mkdirSync(dir, { recursive: true });
    }, "mkdir"),
    readdir: /* @__PURE__ */ __name((dir) => readdirSync(dir), "readdir"),
    readFile: /* @__PURE__ */ __name((file) => readFileSync(file, "utf8"), "readFile"),
    writeFile: /* @__PURE__ */ __name((file, data) => writeFileSync(file, data), "writeFile"),
    appendFile: /* @__PURE__ */ __name((file, data) => appendFileSync(file, data), "appendFile"),
    rename: /* @__PURE__ */ __name((from, to) => renameSync(from, to), "rename"),
    remove: /* @__PURE__ */ __name((file) => {
      rmSync(file, { force: true, recursive: true });
    }, "remove"),
    stat: /* @__PURE__ */ __name((file) => {
      const stats = statSync(file);
      return { size: stats.size, mtimeMs: stats.mtimeMs };
    }, "stat"),
    exists: /* @__PURE__ */ __name((file) => {
      try {
        return statSync(file).isDirectory();
      } catch {
        return false;
      }
    }, "exists")
  };
}
__name(osSpoolFs, "osSpoolFs");
var OS_RUNTIME = {
  now: /* @__PURE__ */ __name(() => Date.now(), "now"),
  spawn: /* @__PURE__ */ __name((command, args, options) => spawn(command, args, options), "spawn"),
  kill: /* @__PURE__ */ __name((pid, signal) => {
    process.kill(pid, signal);
  }, "kill"),
  alive: /* @__PURE__ */ __name((pid) => {
    if (typeof pid !== "number" || !Number.isInteger(pid) || pid <= 0) return false;
    try {
      process.kill(pid, 0);
      return true;
    } catch (error) {
      return error.code === "EPERM";
    }
  }, "alive"),
  // The only wait in the mod, used by subagent_output's opt-in blocking read.
  sleep: /* @__PURE__ */ __name((ms) => new Promise((resolve3) => setTimeout(resolve3, ms)), "sleep"),
  // The fallback for a host without `cmd.clock.after`, and the seam the tests drive.
  later: /* @__PURE__ */ __name((ms, fn) => {
    const timer = setTimeout(fn, ms);
    if (typeof timer === "object" && timer !== null && "unref" in timer && typeof timer.unref === "function") timer.unref();
  }, "later"),
  pid: process.pid,
  execPath: process.execPath,
  env: process.env,
  cwd: process.cwd(),
  home: homedir2(),
  interactive: process.stdin.isTTY === true,
  fs: osSpoolFs(),
  // One bounded command. Everything it returns is plain data, so a failed git (not a
  // repository, no ref) is a value, never a throw here.
  run: /* @__PURE__ */ __name((command, args, options) => {
    const result = spawnSync(command, args, {
      cwd: options.cwd,
      encoding: "utf8",
      timeout: options.timeoutMs ?? 2e3,
      maxBuffer: 64 * 1024,
      ...options.env !== void 0 ? { env: { ...process.env, ...options.env } } : {}
    });
    return {
      ok: result.status === 0,
      code: typeof result.status === "number" ? result.status : 1,
      stdout: String(result.stdout ?? ""),
      stderr: String(result.stderr ?? result.error?.message ?? "")
    };
  }, "run")
};
function osRuntime(overrides = {}) {
  return {
    ...OS_RUNTIME,
    ...overrides,
    fs: { ...OS_RUNTIME.fs, ...overrides.fs }
  };
}
__name(osRuntime, "osRuntime");
function hookMatches(matcher, toolName) {
  const pattern = (matcher ?? "").trim();
  if (pattern.length === 0 || pattern === "*") return true;
  if (pattern.length > 2 && pattern.startsWith("/") && pattern.endsWith("/")) {
    try {
      return new RegExp(pattern.slice(1, -1), "i").test(toolName);
    } catch {
      return false;
    }
  }
  const glob = /* @__PURE__ */ __name((value) => {
    const escaped = value.replace(/[.+?^${}()|[\]\\]/g, "\\$&").replaceAll("*", ".*");
    return new RegExp(`^${escaped}$`, "i").test(toolName);
  }, "glob");
  return pattern.split("|").some((part) => glob(part.trim()));
}
__name(hookMatches, "hookMatches");
var HOOK_TIMEOUT_MS = 1e4;
function runHookRules(run, rules, detail) {
  const outcome = { ran: 0 };
  if (run === void 0 || rules === void 0 || rules.length === 0) return outcome;
  const contexts = [];
  for (const rule of rules) {
    if ((detail.event === "PreToolUse" || detail.event === "PostToolUse") && !hookMatches(rule.matcher, detail.toolName ?? "")) {
      continue;
    }
    const env = {
      BETTER_SUBAGENTS_HOOK_EVENT: detail.event,
      BETTER_SUBAGENTS_HOOK_AGENT: detail.agent ?? "",
      BETTER_SUBAGENTS_HOOK_TOOL_NAME: detail.toolName ?? "",
      BETTER_SUBAGENTS_HOOK_TOOL_INPUT: JSON.stringify(detail.input ?? {}),
      BETTER_SUBAGENTS_HOOK_RESULT: JSON.stringify(detail.result ?? null),
      BETTER_SUBAGENTS_HOOK_CWD: detail.cwd
    };
    let result;
    try {
      result = run("/bin/sh", ["-c", rule.command], {
        cwd: detail.cwd,
        timeoutMs: rule.timeoutMs ?? HOOK_TIMEOUT_MS,
        env
      });
    } catch {
      continue;
    }
    outcome.ran += 1;
    const code = typeof result.code === "number" ? result.code : result.ok ? 0 : 1;
    if (code !== 2) continue;
    const message = (result.stderr || result.stdout || "").trim();
    if (detail.event === "PreToolUse") {
      outcome.block = message.length > 0 ? message : "Blocked by a PreToolUse hook.";
      return outcome;
    }
    if (detail.event === "PostToolUse") {
      if (message.length > 0) contexts.push(message);
      continue;
    }
    if (detail.event === "SubagentStop") {
      outcome.continue = true;
      outcome.reason = message.length > 0 ? message : "SubagentStop hook requested another turn.";
      return outcome;
    }
  }
  if (contexts.length > 0) outcome.additionalContext = contexts.join("\n\n");
  return outcome;
}
__name(runHookRules, "runHookRules");
var MEMORY_INDEX_FILE = "MEMORY.md";
var MEMORY_INDEX_LINES_MAX = 200;
var MEMORY_INDEX_BYTES_MAX = 25 * 1024;
var GIT_STATUS_MAX_CHARS = 2e3;
var STARTUP_WALK_DEPTH_MAX = 8;
var CONTEXT_WINDOW_DEFAULT = 2e5;
var CONTEXT_WINDOW_MAX = 10 * 1e3 * 1e3;
var CONTEXT_WINDOW_FAMILIES = [
  [/\[1m\]|-1m\b/i, 1e6],
  [/opus|sonnet|haiku|fable/i, 2e5]
];
var EXPLORE_PLAN_NAMES = { explore: true, plan: true };
function readStartupFile(fs, path) {
  try {
    return fs.readFile(path);
  } catch {
    return void 0;
  }
}
__name(readStartupFile, "readStartupFile");
function startupFile(fs, path) {
  const content = readStartupFile(fs, path);
  return content === void 0 ? void 0 : { path, content };
}
__name(startupFile, "startupFile");
function projectDirsUpward(cwd, fs) {
  const dirs = [];
  let current = cwd;
  for (let depth = 0; depth < STARTUP_WALK_DEPTH_MAX; depth += 1) {
    dirs.push(current);
    if (fs !== void 0 && hasGitMarker(fs, current)) break;
    const parent = dirname4(current);
    if (parent === current) break;
    current = parent;
  }
  return dirs.reverse();
}
__name(projectDirsUpward, "projectDirsUpward");
function subdirectoryClaudeMd(fs, cwd, paths) {
  const seen = /* @__PURE__ */ new Set();
  const files = [];
  for (const path of paths) {
    const absolute = isAbsolute2(path) ? path : join12(cwd, path);
    let dir = dirname4(absolute);
    for (let depth = 0; depth < STARTUP_WALK_DEPTH_MAX; depth += 1) {
      if (dir === cwd || dir === dirname4(dir) || !dir.startsWith(cwd)) break;
      const filePath = join12(dir, "CLAUDE.md");
      if (seen.has(filePath)) break;
      seen.add(filePath);
      const file = startupFile(fs, filePath);
      if (file !== void 0) files.push(file);
      dir = dirname4(dir);
    }
  }
  return files;
}
__name(subdirectoryClaudeMd, "subdirectoryClaudeMd");
function claudeMdHierarchy(options) {
  const env = options.env ?? {};
  const fs = options.fs;
  const definition = options.definition;
  const agentType = (options.agentType ?? definition?.name ?? "").toLowerCase();
  const claudeMd = [];
  const agentsMd = [];
  const skipHierarchy = EXPLORE_PLAN_NAMES[agentType] === true;
  const policy = policyFiles(env, options.home);
  if (!skipHierarchy && definition?.scope !== "managed") {
    const managed = startupFile(fs, policy.managed);
    if (managed !== void 0) claudeMd.push(managed);
  }
  if (!skipHierarchy && definition?.omitClaudeMd !== true) {
    const user = startupFile(fs, policy.user);
    if (user !== void 0) claudeMd.push(user);
    for (const dir of projectDirsUpward(options.cwd, options.fs)) {
      const project = startupFile(fs, join12(dir, "CLAUDE.md"));
      if (project !== void 0) claudeMd.push(project);
      const local = startupFile(fs, join12(dir, "CLAUDE.local.md"));
      if (local !== void 0) claudeMd.push(local);
    }
    for (const dir of projectDirsUpward(options.cwd, options.fs)) {
      const agents = startupFile(fs, join12(dir, "AGENTS.md"));
      if (agents !== void 0) agentsMd.push(agents);
    }
  }
  claudeMd.push(...subdirectoryClaudeMd(fs, options.cwd, options.paths ?? []));
  return { claudeMd, agentsMd };
}
__name(claudeMdHierarchy, "claudeMdHierarchy");
function skillFileFor(name, options) {
  if (name.length === 0) return void 0;
  const candidates = [];
  for (const dir of projectDirsUpward(options.cwd, options.fs)) {
    candidates.push(join12(dir, ".commandcode", "skills", name, "SKILL.md"));
    candidates.push(join12(dir, ".claude", "skills", name, "SKILL.md"));
  }
  candidates.push(join12(options.home, ".commandcode", "skills", name, "SKILL.md"));
  candidates.push(join12(options.home, ".claude", "skills", name, "SKILL.md"));
  for (const candidate of candidates) {
    if (readStartupFile(options.fs, candidate) !== void 0) return candidate;
  }
  return void 0;
}
__name(skillFileFor, "skillFileFor");
function memoryDirFor(scope, name, options) {
  const root = scope === "user" ? join12(options.home, ".commandcode", "agent-memory") : scope === "project" ? join12(options.cwd, ".commandcode", "agent-memory") : join12(options.cwd, ".commandcode", "agent-memory-local");
  return join12(root, name);
}
__name(memoryDirFor, "memoryDirFor");
function readMemoryIndex(path, fs) {
  let source;
  try {
    source = fs.readFile(path);
  } catch {
    return { content: "", lines: 0, truncated: false };
  }
  const allLines = source.split("\n");
  if (allLines.length > 0 && allLines[allLines.length - 1] === "") allLines.pop();
  const lineBound = allLines.slice(0, MEMORY_INDEX_LINES_MAX).join("\n");
  let content = lineBound;
  let truncatedByBytes = false;
  if (Buffer.byteLength(lineBound, "utf8") > MEMORY_INDEX_BYTES_MAX) {
    const buffer = Buffer.from(lineBound, "utf8").subarray(0, MEMORY_INDEX_BYTES_MAX);
    content = buffer.toString("utf8").replace(/\uFFFD$/, "");
    truncatedByBytes = true;
  }
  const truncatedByLines = allLines.length > MEMORY_INDEX_LINES_MAX;
  return {
    content,
    lines: content.length === 0 ? 0 : content.split("\n").length,
    truncated: truncatedByLines || truncatedByBytes
  };
}
__name(readMemoryIndex, "readMemoryIndex");
function startupMemory(options) {
  const scope = options.definition?.memory;
  const name = options.definition?.name;
  if (scope === void 0 || name === void 0 || name.length === 0) return void 0;
  const dir = memoryDirFor(scope, name, { cwd: options.cwd, home: options.home });
  try {
    options.fs.mkdir(dir);
  } catch {
  }
  const path = join12(dir, MEMORY_INDEX_FILE);
  const index = readMemoryIndex(path, options.fs);
  return { scope, dir, path, content: index.content, lines: index.lines, truncated: index.truncated };
}
__name(startupMemory, "startupMemory");
function contextWindowForModel(model, env = {}) {
  const override = contextWindowOverride(env);
  if (override !== void 0 && override >= 1e3) return Math.min(CONTEXT_WINDOW_MAX, Math.round(override));
  const value = (model ?? "").toLowerCase();
  for (const [pattern, window] of CONTEXT_WINDOW_FAMILIES) {
    if (pattern.test(value)) return window;
  }
  return CONTEXT_WINDOW_DEFAULT;
}
__name(contextWindowForModel, "contextWindowForModel");
function withMemoryTools(tools, memory) {
  if (memory === void 0) return tools === void 0 ? void 0 : [...tools];
  const result = tools === void 0 ? [] : [...tools];
  for (const tool of ["Read", "Write", "Edit"]) if (!result.includes(tool)) result.push(tool);
  return result;
}
__name(withMemoryTools, "withMemoryTools");
function gitStatusSnapshot(runtime, cwd) {
  const run = runtime.run;
  if (typeof run !== "function") return void 0;
  try {
    const result = run("git", ["status", "--short", "--branch"], { cwd });
    return result.ok ? result.stdout : void 0;
  } catch {
    return void 0;
  }
}
__name(gitStatusSnapshot, "gitStatusSnapshot");
function trimGitStatus(status) {
  if (status === void 0) return void 0;
  const text = status.replace(/\s+$/, "");
  if (text.length === 0) return void 0;
  return text.length > GIT_STATUS_MAX_CHARS ? `${text.slice(0, GIT_STATUS_MAX_CHARS)}
(truncated)` : text;
}
__name(trimGitStatus, "trimGitStatus");
function renderStartup(bundle) {
  const sections = [bundle.environmentText];
  if (bundle.claudeMd.length > 0) {
    const files = bundle.claudeMd.map((file) => `## ${file.path}
${file.content}`);
    sections.push(["# Project instructions (CLAUDE.md)", ...files].join("\n\n"));
  }
  if (bundle.agentsMd.length > 0) {
    const files = bundle.agentsMd.map((file) => `## ${file.path}
${file.content}`);
    sections.push(["# AGENTS.md", ...files].join("\n\n"));
  }
  if (bundle.gitStatus !== void 0) sections.push(`# Git status
${bundle.gitStatus}`);
  if (bundle.skills.length > 0) {
    const skills = bundle.skills.map((skill) => `## ${skill.name} (${skill.path})
${skill.content}`);
    sections.push(["# Skills (full content)", ...skills].join("\n\n"));
  }
  if (bundle.roster !== void 0 && bundle.roster.length > 0) {
    sections.push(`# Sibling agents
${bundle.roster.map((name) => `- ${name}`).join("\n")}`);
  }
  if (bundle.outputStyle !== void 0) sections.push(`# Output style
${bundle.outputStyle}`);
  if (bundle.memory !== void 0) {
    const bound = bundle.memory.truncated ? " (truncated to the first 200 lines or 25KB)" : "";
    sections.push(`# Agent memory
${bundle.memory.path}${bound}
${bundle.memory.content}`);
  }
  return sections.join("\n\n");
}
__name(renderStartup, "renderStartup");
function buildStartupBundle(options) {
  const env = options.env ?? {};
  const definition = options.definition;
  const agentType = (options.agentType ?? definition?.name ?? "").toLowerCase();
  const contextWindow = contextWindowForModel(options.model, env);
  const { claudeMd, agentsMd } = claudeMdHierarchy(options);
  const gitStatus = EXPLORE_PLAN_NAMES[agentType] === true ? void 0 : trimGitStatus(options.gitStatus);
  const skills = [];
  for (const name of definition?.skills ?? []) {
    const path = skillFileFor(name, options);
    const content = path === void 0 ? void 0 : readStartupFile(options.fs, path);
    if (path !== void 0 && content !== void 0) skills.push({ name, path, content });
  }
  const roster = options.roster === void 0 ? void 0 : [...options.roster];
  const outputStyle = options.outputStyle ?? definition?.outputStyle;
  const memory = startupMemory(options);
  const environment = [
    "# Environment",
    `working directory: ${options.cwd}`,
    `platform: ${options.platform ?? "unknown"}`
  ];
  if (options.now !== void 0) environment.push(`date: ${new Date(options.now).toISOString().slice(0, 10)}`);
  if (options.model !== void 0) environment.push(`model: ${options.model}`);
  environment.push(`context window: ${contextWindow} tokens`);
  const base = {
    systemPrompt: definition?.prompt ?? "",
    environmentText: environment.join("\n"),
    task: options.prompt ?? "",
    claudeMd,
    agentsMd,
    ...gitStatus !== void 0 ? { gitStatus } : {},
    skills,
    ...roster !== void 0 ? { roster } : {},
    ...outputStyle !== void 0 ? { outputStyle } : {},
    ...memory !== void 0 ? { memory } : {},
    contextWindow,
    parentMemory: false
  };
  return { ...base, text: renderStartup(base) };
}
__name(buildStartupBundle, "buildStartupBundle");
function buildRoster(entries, selfName) {
  const named = /* @__PURE__ */ new Map();
  for (const entry of entries) {
    const name = typeof entry.name === "string" ? entry.name.trim() : "";
    if (name.length === 0 || name === selfName) continue;
    if (!named.has(name)) named.set(name, entry);
  }
  const lines = [
    "SIBLING ROSTER (snapshot taken at start; address a peer with subagent_send({to, message}))",
    `${ROSTER_MAIN} \u2014 the conversation that started you`
  ];
  for (const [name, entry] of named) {
    const state = typeof entry.state === "string" && entry.state.length > 0 ? ` (${entry.state})` : "";
    const job = typeof entry.jobId === "string" && entry.jobId.length > 0 ? ` job ${entry.jobId}` : "";
    lines.push(`${name}${state}${job}`);
  }
  return lines.join("\n");
}
__name(buildRoster, "buildRoster");
function listAgentsVisible(options) {
  if (options.crossSessionMessaging === false) return false;
  return options.background !== true;
}
__name(listAgentsVisible, "listAgentsVisible");
function resolveModel(input = {}) {
  const env = input.env ?? {};
  const envModel = subagentModel(env);
  const forced = subagentModelForced(env);
  const main = nonEmpty(input.main);
  if (forced && envModel !== void 0) return { model: envModel, source: "forced" };
  const requested = nonEmpty(input.requested);
  if (requested === "inherit") return { model: main, source: "inherit" };
  if (requested !== void 0) return { model: requested, source: "parameter" };
  const definition = nonEmpty(input.definition);
  if (definition === "inherit") return { model: main, source: "inherit" };
  if (definition !== void 0) return { model: definition, source: "definition" };
  if (envModel !== void 0) return { model: envModel, source: "environment" };
  return { model: main, source: "main" };
}
__name(resolveModel, "resolveModel");
function resolveEffort(input = {}) {
  const env = input.env ?? {};
  const requested = effortLevel(input.requested, EFFORT_LEVELS);
  if (requested !== void 0) return { effort: requested, source: "parameter" };
  const definition = effortLevel(input.definition, EFFORT_LEVELS);
  if (definition !== void 0) return { effort: definition, source: "definition" };
  const fromEnv = effortLevelFromEnv(env, EFFORT_LEVELS);
  if (fromEnv !== void 0) return { effort: fromEnv, source: "environment" };
  return { effort: void 0, source: "none" };
}
__name(resolveEffort, "resolveEffort");
function resolveLaunch(input = {}) {
  const tools = filterTools({
    ...input.availableTools !== void 0 ? { available: input.availableTools } : {},
    ...input.mcpServers !== void 0 ? { mcpServers: input.mcpServers } : {},
    ...input.tools !== void 0 ? { tools: input.tools } : {},
    ...input.disallowedTools !== void 0 ? { disallowedTools: input.disallowedTools } : {},
    ...input.permissionMode !== void 0 ? { permissionMode: input.permissionMode } : {},
    ...input.background !== void 0 ? { background: input.background } : {},
    ...input.team !== void 0 ? { team: input.team } : {}
  });
  if (tools.ok === false) return tools;
  const model = resolveModel({
    ...input.requestedModel !== void 0 ? { requested: input.requestedModel } : {},
    ...input.definitionModel !== void 0 ? { definition: input.definitionModel } : {},
    ...input.env !== void 0 ? { env: input.env } : {},
    ...input.mainModel !== void 0 ? { main: input.mainModel } : {}
  });
  const effort = resolveEffort({
    ...input.requestedEffort !== void 0 ? { requested: input.requestedEffort } : {},
    ...input.definitionEffort !== void 0 ? { definition: input.definitionEffort } : {},
    ...input.env !== void 0 ? { env: input.env } : {}
  });
  return {
    ok: true,
    model: model.model,
    modelSource: model.source,
    effort: effort.effort,
    effortSource: effort.source,
    tools: tools.tools,
    removed: tools.removed
  };
}
__name(resolveLaunch, "resolveLaunch");
function mcpServerConfigs(fs, dirs) {
  const configs = {};
  for (const dir of dirs.slice(0, 4)) {
    if (typeof dir !== "string" || dir.length === 0) continue;
    try {
      const parsed = JSON.parse(fs.readFile(join12(dir, ".commandcode", "mcp.json")));
      for (const [name, config] of Object.entries(parsed?.mcpServers ?? {})) {
        if (name.length === 0 || config?.enabled === false || Object.hasOwn(configs, name)) continue;
        if (config === null || typeof config !== "object") continue;
        const command = stringField(config.command);
        const url = stringField(config.url);
        const args = listField(config.args);
        const env = stringMapField(config.env);
        configs[name] = {
          ...command !== void 0 ? { command } : {},
          ...args !== void 0 ? { args } : {},
          ...env !== void 0 ? { env } : {},
          ...url !== void 0 ? { url } : {}
        };
      }
    } catch {
    }
  }
  return configs;
}
__name(mcpServerConfigs, "mcpServerConfigs");
function mcpServerNames(fs, dirs) {
  return Object.keys(mcpServerConfigs(fs, dirs));
}
__name(mcpServerNames, "mcpServerNames");
function agentFolderTrusted(definition, roots) {
  if (definition.scope === "plugin") return false;
  const path = definition.sourcePath;
  if (path === void 0 || path.length === 0) return true;
  const absolute = isAbsolute2(path) ? path : join12(roots.cwd, path);
  const trustedRoots = [
    roots.cwd,
    roots.home,
    roots.managedDir ?? managedSettingsHome(roots.home),
    ...roots.addDirs ?? []
  ];
  return trustedRoots.some((root) => root.length > 0 && isInside(root, absolute));
}
__name(agentFolderTrusted, "agentFolderTrusted");
function resolveAgentMcpServers(input = {}) {
  const env = input.env ?? {};
  const defined = input.defined ?? [];
  const dropped = [];
  if (input.definition?.scope === "plugin") {
    for (const spec of defined) dropped.push({ name: spec.name, reason: "plugin definitions ignore mcpServers" });
    return { servers: [], dropped };
  }
  const policy = mcpPolicy(env);
  if (policy.bare) {
    for (const spec of defined) dropped.push({ name: spec.name, reason: "bare mode disables MCP" });
    return { servers: [], dropped };
  }
  const strict = policy.strict;
  const allow = policy.allow;
  const deny = policy.deny;
  const trusted = input.trusted !== false;
  const servers = [];
  const seen = /* @__PURE__ */ new Set();
  for (const spec of defined) {
    const name = spec.name.trim();
    if (name.length === 0 || seen.has(name)) continue;
    const inline = spec.command !== void 0 || spec.url !== void 0;
    let resolved;
    if (inline) {
      if (!trusted) {
        dropped.push({ name, reason: "inline server outside a trusted folder" });
        continue;
      }
      if (strict) {
        dropped.push({ name, reason: "strict MCP config ignores inline servers" });
        continue;
      }
      resolved = { ...spec, name };
    } else {
      const config = input.configured?.[name];
      if (config === void 0) {
        dropped.push({ name, reason: "not configured" });
        continue;
      }
      resolved = { name, ...config };
    }
    if (deny.includes("*") || deny.includes(name)) {
      dropped.push({ name, reason: "denied by the MCP policy" });
      continue;
    }
    if (allow.length > 0 && !allow.includes("*") && !allow.includes(name)) {
      dropped.push({ name, reason: "not in the MCP allow policy" });
      continue;
    }
    seen.add(name);
    servers.push(resolved);
  }
  return { servers, dropped };
}
__name(resolveAgentMcpServers, "resolveAgentMcpServers");
var PERMISSION_MODE_LIST = [
  "default",
  "acceptEdits",
  "auto",
  "dontAsk",
  "bypassPermissions",
  "plan"
];
var PERMISSION_MODE_TABLE = {
  default: { acp: "default", cli: "standard" },
  acceptEdits: { acp: "auto-accept", cli: "accept-edits", inherits: true },
  auto: { inherits: true },
  dontAsk: { acp: "dont-ask", autoDeny: true },
  bypassPermissions: { acp: "bypass", cli: "yolo", inherits: true, bypass: true },
  plan: { acp: "plan", cli: "plan", readOnly: true }
};
var PERMISSION_MODE_ALIASES = {
  manual: "default",
  standard: "default",
  "accept-edits": "acceptEdits",
  "auto-accept": "acceptEdits",
  "dont-ask": "dontAsk",
  yolo: "bypassPermissions",
  bypass: "bypassPermissions"
};
function canonicalPermissionMode(value) {
  if (typeof value !== "string" || value.length === 0) return void 0;
  const alias = PERMISSION_MODE_ALIASES[value];
  if (alias !== void 0) return alias;
  return Object.prototype.hasOwnProperty.call(PERMISSION_MODE_TABLE, value) ? value : void 0;
}
__name(canonicalPermissionMode, "canonicalPermissionMode");
function resolvePermissionMode(definitionMode, mainMode) {
  const main = canonicalPermissionMode(mainMode) ?? "default";
  const definition = canonicalPermissionMode(definitionMode) ?? "default";
  if (PERMISSION_MODE_TABLE[main].inherits === true) return { ok: true, mode: main };
  if (PERMISSION_MODE_TABLE[definition].bypass === true) {
    return {
      ok: false,
      error: 'permissionMode "bypassPermissions" requires the main session to be in bypassPermissions'
    };
  }
  return { ok: true, mode: definition };
}
__name(resolvePermissionMode, "resolvePermissionMode");
var permissionModes = {
  list: PERMISSION_MODE_LIST,
  table: PERMISSION_MODE_TABLE,
  canonical: canonicalPermissionMode,
  resolve: resolvePermissionMode
};
function resolveExecutionPolicy(input) {
  if (backgroundDisabled(input.env)) return "foreground";
  if (input.definitionBackground === true) return "background";
  if (input.requested === false) return "foreground";
  return "background";
}
__name(resolveExecutionPolicy, "resolveExecutionPolicy");
function resolveForkMode(input = {}) {
  const flag = toggle(input.flag);
  if (flag !== void 0) return flag;
  const env = input.env ?? {};
  const fromEnv = forkModeFromEnv(env);
  if (fromEnv !== void 0) return fromEnv;
  const inSdk = input.sdk === true || agentSdk(env);
  if (inSdk) return "off";
  return input.interactive === true ? "on" : "off";
}
__name(resolveForkMode, "resolveForkMode");
function resolveTeamPolicy(input) {
  if (input.teammate !== true) return { ok: true, background: false };
  if (input.definitionBackground === true) {
    return {
      ok: false,
      error: `a teammate cannot run in the background: its definition sets background: true; remove that field to join a team`
    };
  }
  if (input.requested === true && input.forkMode === "off") {
    return {
      ok: false,
      error: "a teammate cannot run in the background: remove run_in_background: true (teammates run in the foreground)"
    };
  }
  return { ok: true, background: false };
}
__name(resolveTeamPolicy, "resolveTeamPolicy");
var PLUMBING_LINE = /^\s*\[(tool|permission|acp)\b/;
function hasAgentOutput(log) {
  for (const line of log.split("\n")) {
    if (line.trim().length === 0) continue;
    if (PLUMBING_LINE.test(line)) continue;
    return true;
  }
  return false;
}
__name(hasAgentOutput, "hasAgentOutput");
function foregroundOutcome(job, log) {
  const state = canonicalState(job.state);
  const label = String(job.description ?? job.id ?? "subagent");
  const full = reportText(log, String(job.kind ?? "agent")).trim();
  const text = full.length > OUTPUT_MAX_CHARS_DEFAULT ? `\u2026${full.slice(-OUTPUT_MAX_CHARS_DEFAULT)}` : full;
  if (state === "done") {
    return { ok: true, partial: false, text: text.length > 0 ? text : `${label} finished with no output.` };
  }
  if (state === "waiting-input") {
    const followUp = String(job.kind ?? "agent") === "command" ? `inspect it with subagent_output ${String(job.id)}` : `resume it with subagent_resume ${String(job.id)}`;
    return {
      ok: true,
      partial: true,
      text: `${text.length > 0 ? `${text}

` : ""}${label} stopped with a question; ${followUp}.`
    };
  }
  const reason = typeof job.failureReason === "string" && job.failureReason.length > 0 ? job.failureReason : `${label} ${state}`;
  if (state === "failed" && job.partial === true && hasAgentOutput(log)) {
    return { ok: true, partial: true, text: `${text}

[partial: ${reason}]` };
  }
  if (state === "failed" && reason.startsWith(AGENT_API_ERROR) && hasAgentOutput(log)) {
    return { ok: true, partial: true, text: `${text}

[cut off: ${reason}]` };
  }
  return { ok: false, error: reason };
}
__name(foregroundOutcome, "foregroundOutcome");
function taskRowVisible(job, options) {
  const id = String(job.id ?? "");
  if (id.length > 0 && options.dismissed?.has(id) === true) return false;
  const state = canonicalState(job.state);
  if (state === "done") return false;
  if (state === "failed" || state === "canceled") {
    const finishedAt = typeof job.finishedAt === "number" ? job.finishedAt : void 0;
    if (finishedAt === void 0) return true;
    return options.now - finishedAt <= TASK_ROW_GRACE_MS;
  }
  return true;
}
__name(taskRowVisible, "taskRowVisible");
function taskPanelRows(jobs, options) {
  return jobs.filter((job) => taskRowVisible(job, options));
}
__name(taskPanelRows, "taskPanelRows");
function moveTaskSelection(rows, selectedId, delta) {
  if (rows.length === 0) return void 0;
  const current = rows.findIndex((row) => String(row.id ?? "") === selectedId);
  if (current === -1) return String((delta < 0 ? rows[rows.length - 1] : rows[0])?.id ?? "");
  const next = Math.min(rows.length - 1, Math.max(0, current + (delta < 0 ? -1 : 1)));
  return String(rows[next]?.id ?? "");
}
__name(moveTaskSelection, "moveTaskSelection");
function mentionCandidates(definitions, namedAgents = []) {
  const names = /* @__PURE__ */ new Set();
  for (const definition of definitions) {
    const name = String(definition.name ?? "").trim();
    if (name.length > 0 && definition.valid !== false) names.add(name);
  }
  for (const agent of namedAgents) {
    const name = String(agent ?? "").trim();
    if (name.length > 0) names.add(name);
  }
  return [...names].sort();
}
__name(mentionCandidates, "mentionCandidates");
function tasksHintVisible(jobs, now) {
  return jobs.some((job) => {
    const state = canonicalState(job.state);
    const finishedAt = typeof job.finishedAt === "number" ? job.finishedAt : void 0;
    if (finishedAt === void 0) return false;
    if (!isTerminalState(state)) return false;
    return now - finishedAt <= TASK_ROW_GRACE_MS;
  });
}
__name(tasksHintVisible, "tasksHintVisible");
function index_default(cmd, overrides = {}) {
  const runtime = osRuntime(overrides);
  const spool = runtime.fs;
  const git = gitSeam(runtime);
  cmd.addFlag("subagent_dir", {
    type: "string",
    default: SUBAGENT_DIR_DEFAULT,
    description: "Where the spool, logs and job records live."
  });
  cmd.addFlag("subagent_deliver", {
    type: "string",
    default: "steer",
    description: "How a signal reaches the session: 'steer' (after the current tool batch) or 'follow-up' (only when the run would stop)."
  });
  cmd.addFlag("pubsub_dir", {
    type: "string",
    default: PUBSUB_DIR_DEFAULT,
    description: "Deprecated alias for subagent_dir."
  });
  cmd.addFlag("pubsub_deliver", {
    type: "string",
    default: "steer",
    description: "Deprecated alias for subagent_deliver."
  });
  cmd.addFlag("managed_dir", {
    type: "string",
    default: MANAGED_SETTINGS_DEFAULT,
    description: "Managed settings directory; agents/ under it wins every other scope."
  });
  cmd.addFlag(CLI_AGENTS_OPTION, {
    type: "string",
    default: "",
    description: "Session-only agent definitions as inline JSON, or a JSON file path in a non-interactive session (--mod-option agents=...)."
  });
  cmd.addFlag(ADD_DIRS_OPTION, {
    type: "string",
    default: "",
    description: "Comma-separated extra directories; .commandcode/agents/ under each is project scope (--add-dir)."
  });
  cmd.addFlag(PLUGIN_DIRS_OPTION, {
    type: "string",
    default: "",
    description: "Comma-separated plugin roots; every child directory with an agents/ dir is a plugin."
  });
  cmd.addFlag(FORK_MODE_FLAG, {
    type: "string",
    default: "auto",
    description: "Fork mode: auto (interactive on, non-interactive/SDK off), on, or off."
  });
  cmd.addFlag("agent", {
    type: "string",
    default: "",
    description: "Run this session as the named agent definition (--agent <name>)."
  });
  cmd.addFlag("append_subagent_system_prompt", {
    type: "string",
    default: "",
    description: "Text, or @path, appended to every subagent system prompt (--append-subagent-system-prompt)."
  });
  cmd.addFlag("append_subagent_system_prompt_file", {
    type: "string",
    default: "",
    description: "File whose text is appended to every subagent system prompt."
  });
  const addDirs = optionList(cmd, ADD_DIRS_OPTION);
  const workerMode = workerModeFromEnv(runtime.env);
  const workerRoot = (() => {
    if (!workerMode) return void 0;
    try {
      const spec = JSON.parse(String(runtime.env[WORKER_SPEC_ENV] ?? ""));
      return typeof spec.inboxDir === "string" ? dirname4(spec.inboxDir) : void 0;
    } catch {
      return void 0;
    }
  })();
  const root = workerRoot ?? resolve2(pickOption(cmd, "subagent_dir", SUBAGENT_DIR_DEFAULT, "pubsub_dir", PUBSUB_DIR_DEFAULT));
  const forkMode = resolveForkMode({
    interactive: runtime.interactive === true,
    env: runtime.env,
    flag: optionValue(cmd, FORK_MODE_FLAG)
  });
  const forkModeEnabled = forkMode === "on";
  const inboxDir = join12(root, "inbox");
  const claimedDir = join12(root, "claimed");
  const orphanedDir = join12(root, "orphaned");
  const logsDir = join12(root, "logs");
  const jobsDir = join12(root, "jobs");
  const transcriptsDir = join12(root, "transcripts");
  const worktreesDir = join12(root, "worktrees");
  const controlRoot = join12(root, "control");
  const ledgerPath = join12(root, "delivered.jsonl");
  for (const dir of [root, inboxDir, claimedDir, orphanedDir, logsDir, jobsDir, transcriptsDir, worktreesDir, controlRoot]) {
    spool.mkdir(dir);
  }
  let mainPermissionMode = canonicalPermissionMode(
    mainPermissionModeFromEnv(runtime.env)
  );
  function noteMainPermissionMode(value) {
    const mode = canonicalPermissionMode(value);
    if (mode !== void 0) mainPermissionMode = mode;
  }
  __name(noteMainPermissionMode, "noteMainPermissionMode");
  const registry = createAgentRegistry({
    fs: spool,
    cwd: runtime.cwd,
    home: runtime.home,
    builtins: builtinAgentDefinitions(runtime.env),
    managedDir: managedSettingsDir(cmd),
    addDirs,
    pluginRoots: optionList(cmd, PLUGIN_DIRS_OPTION),
    cli: { value: optionValue(cmd, CLI_AGENTS_OPTION), interactive: runtime.interactive === true },
    log: /* @__PURE__ */ __name((message) => {
      try {
        spool.appendFile(join12(root, AGENT_LOG_FILE), `[${new Date(runtime.now()).toISOString()}] ${message}
`);
      } catch {
      }
    }, "log")
  });
  let workerSpec;
  if (workerMode) {
    try {
      workerSpec = JSON.parse(String(runtime.env[WORKER_SPEC_ENV] ?? ""));
    } catch {
      workerSpec = void 0;
    }
  }
  if (workerMode && workerSpec?.systemPrompt !== void 0 && workerSpec.promptMarkerPath !== void 0) {
    try {
      spool.writeFile(workerSpec.promptMarkerPath, "1");
    } catch {
    }
  }
  function resolveAgentDefinition(name) {
    if (name === void 0) {
      const fallback = registry.get("general-purpose") ?? registry.get("general");
      if (fallback !== void 0) return { ok: true, definition: fallback };
      return { ok: false, error: "subagent_type is required" };
    }
    const exact = registry.get(name);
    if (exact !== void 0) return { ok: true, definition: exact };
    if (name === "general") {
      const general = registry.get("general-purpose");
      if (general !== void 0) return { ok: true, definition: general };
    }
    const available = registry.list().map((definition) => definition.name).sort().join(", ");
    return {
      ok: false,
      error: `unknown subagent_type "${name}"${available.length > 0 ? `; available: ${available}` : ""}`
    };
  }
  __name(resolveAgentDefinition, "resolveAgentDefinition");
  const sessionAgentName = /* @__PURE__ */ __name(() => {
    if (workerMode) return "";
    return (optionValue(cmd, "agent") || agentNameFromEnv(runtime.env)).trim();
  }, "sessionAgentName");
  function sessionAgent() {
    const name = sessionAgentName();
    return name.length === 0 ? void 0 : registry.get(name);
  }
  __name(sessionAgent, "sessionAgent");
  let initialPromptSubmitted = false;
  function submitInitialPrompt() {
    if (initialPromptSubmitted || workerMode) return;
    const text = sessionAgent()?.initialPrompt;
    if (text === void 0 || !("prompt" in cmd)) return;
    const promptApi = cmd.prompt;
    if (promptApi === null || typeof promptApi !== "object" || !("submit" in promptApi)) return;
    const submit = promptApi.submit;
    if (typeof submit !== "function") return;
    submit(text);
    initialPromptSubmitted = true;
  }
  __name(submitInitialPrompt, "submitInitialPrompt");
  const appendPromptText = workerMode && typeof workerSpec?.appendSystemPrompt === "string" ? workerSpec.appendSystemPrompt : resolveAppendSystemPrompt({
    flag: optionValue(cmd, "append_subagent_system_prompt"),
    file: optionValue(cmd, "append_subagent_system_prompt_file"),
    env: runtime.env,
    fs: spool,
    cwd: cmd.cwd
  });
  function subagentSystemPrompt(base) {
    const parts = [base ?? "", appendPromptText].filter((part) => part.length > 0);
    return parts.length > 0 ? parts.join("\n\n") : void 0;
  }
  __name(subagentSystemPrompt, "subagentSystemPrompt");
  function workerSystemPrompt() {
    const roster = workerSpec?.roster;
    const parts = [
      typeof workerSpec?.systemPrompt === "string" ? workerSpec.systemPrompt : "",
      typeof roster === "string" ? roster : "",
      typeof workerSpec?.startup === "string" ? workerSpec.startup : ""
    ].filter((part) => part.length > 0);
    return parts.join("\n\n");
  }
  __name(workerSystemPrompt, "workerSystemPrompt");
  function startupFor(definition, cwd, prompt, model) {
    if (definition === void 0 || prompt === void 0) return void 0;
    return buildStartupBundle({
      cwd,
      home: runtime.home,
      env: runtime.env,
      fs: runtime.fs,
      definition,
      agentType: definition.name,
      prompt,
      model,
      gitStatus: gitStatusSnapshot(runtime, cwd),
      now: runtime.now()
    }).text;
  }
  __name(startupFor, "startupFor");
  function projectHookRules(event) {
    const rules = [];
    let current = runtime.cwd;
    for (let depth = 0; depth < STARTUP_WALK_DEPTH_MAX; depth += 1) {
      try {
        const parsed = JSON.parse(spool.readFile(join12(current, ".commandcode", "hooks.json")));
        const found = hookList(parsed?.[event]);
        if (found !== void 0) rules.push(...found);
      } catch {
      }
      try {
        spool.stat(join12(current, ".git"));
        break;
      } catch {
      }
      const parent = dirname4(current);
      if (parent === current) break;
      current = parent;
    }
    return rules;
  }
  __name(projectHookRules, "projectHookRules");
  const deliverAs = /* @__PURE__ */ __name(() => pickOption(cmd, "subagent_deliver", "steer", "pubsub_deliver", "steer") === "follow-up" ? "follow-up" : "steer", "deliverAs");
  let sessionId;
  let ownerToken;
  let ownerSaved = false;
  function saveOwner(token) {
    if (ownerSaved || cmd.session === void 0) return;
    try {
      cmd.session.appendCustomEntry({ customType: OWNER_TYPE, data: { token } });
      ownerSaved = true;
    } catch {
    }
  }
  __name(saveOwner, "saveOwner");
  function ownerId() {
    if (ownerToken !== void 0) {
      saveOwner(ownerToken);
      return ownerToken;
    }
    if (cmd.session === void 0) return void 0;
    for (const customType of [OWNER_TYPE, OWNER_TYPE_LEGACY]) {
      try {
        const entries = cmd.session.getCustomEntries({ customType });
        for (let index = (entries?.length ?? 0) - 1; index >= 0; index -= 1) {
          const token = entries?.[index]?.data?.token;
          if (typeof token === "string" && token.length > 0) {
            ownerToken = token;
            ownerSaved = true;
            return ownerToken;
          }
        }
      } catch {
      }
    }
    ownerToken = `owner-${randomUUID()}`;
    saveOwner(ownerToken);
    return ownerToken;
  }
  __name(ownerId, "ownerId");
  function currentOwner() {
    if (workerMode && typeof workerSpec?.owner === "string" && workerSpec.owner.length > 0) return workerSpec.owner;
    return ownerId();
  }
  __name(currentOwner, "currentOwner");
  function ownerVerdict(signalOwner) {
    if (ownerToken === void 0) return "pending";
    if (typeof signalOwner === "string" && signalOwner.length > 0) {
      return signalOwner === ownerToken ? "mine" : "foreign";
    }
    return workerMode ? "foreign" : "orphan";
  }
  __name(ownerVerdict, "ownerVerdict");
  const renderSignalRow = /* @__PURE__ */ __name((data) => {
    const signal = data;
    const state = signal.state === void 0 ? "" : ` \xB7 ${signal.state}`;
    return [`${stateMarker(signal.state)} subagent \xB7 ${String(signal.kind ?? "signal")}${state} \xB7 ${String(signal.title ?? "(untitled)")}`];
  }, "renderSignalRow");
  cmd.addRenderer("subagent", renderSignalRow);
  cmd.addRenderer("pubsub", renderSignalRow);
  const ledger = createLedger({
    fs: spool,
    path: ledgerPath,
    now: runtime.now,
    max: DELIVERED_LEDGER_MAX,
    keep: DELIVERED_LEDGER_KEEP,
    compactBytes: LEDGER_COMPACT_BYTES
  });
  const { readJob, writeJob, readRecentJobs } = createJobStore({
    fs: spool,
    jobsDir,
    pid: runtime.pid,
    now: /* @__PURE__ */ __name(() => runtime.now(), "now")
  });
  const { finalizeJobWorktree, sweepExpired, liveState } = createWorktreeLifecycle({
    fs: spool,
    git,
    now: /* @__PURE__ */ __name(() => runtime.now(), "now"),
    alive: /* @__PURE__ */ __name((pid) => runtime.alive(pid), "alive"),
    env: runtime.env,
    cwd: runtime.cwd,
    home: runtime.home,
    jobsDir,
    logsDir,
    transcriptsDir,
    worktreesDir,
    controlRoot,
    workerMode,
    readJob,
    writeJob
  });
  const permissionCtx = {
    fs: spool,
    now: /* @__PURE__ */ __name(() => runtime.now(), "now"),
    pid: process.pid,
    get ui() {
      return cmd.ui;
    }
  };
  const {
    safeDrain,
    claimPending,
    settle,
    markDelivered,
    formatSignalMessage,
    countPending,
    countOrphaned,
    refreshJobs,
    sweepClaimed,
    sweepOrphaned
  } = createDelivery({
    fs: spool,
    inboxDir,
    claimedDir,
    orphanedDir,
    controlRoot,
    ledger,
    now: /* @__PURE__ */ __name(() => runtime.now(), "now"),
    readJob,
    writeJob,
    readRecentJobs,
    finalizeWorktree: finalizeJobWorktree,
    ownerVerdict,
    workerMode,
    hasSession: /* @__PURE__ */ __name(() => cmd.session !== void 0, "hasSession"),
    deliverAs,
    sendUserMessage: /* @__PURE__ */ __name((text, options) => cmd.sendUserMessage(text, options), "sendUserMessage"),
    showEntry: /* @__PURE__ */ __name((type, data) => cmd.showEntry(type, data), "showEntry"),
    answerPendingPermission: /* @__PURE__ */ __name((controlDir, answered) => answerPendingPermission(controlDir, answered, permissionCtx), "answerPendingPermission"),
    jobs: /* @__PURE__ */ __name(() => jobCache, "jobs"),
    setJobs: /* @__PURE__ */ __name((jobs) => {
      jobCache = jobs;
    }, "setJobs"),
    sweepSubagentStopHooks,
    updateStatus,
    markJobsTick: /* @__PURE__ */ __name(() => {
      try {
        cmd.state.set("jobsTick", runtime.now());
      } catch {
      }
    }, "markJobsTick"),
    sweep: /* @__PURE__ */ __name((dir, ttlMs) => sweepExpiredEntries(spool, dir, runtime.now(), { ttlMs }), "sweep")
  });
  function applyPatch(id, patch, fallback, stamp) {
    const current = readJob(id) ?? fallback;
    const from = normalizeState(current.state);
    const to = patch.state === void 0 ? from : normalizeState(patch.state);
    if (patch.state !== void 0 && to === void 0) return false;
    if (from !== void 0 && to !== void 0 && !canTransition(from, to)) return false;
    writeJob({ ...current, ...patch, ...to !== void 0 ? { state: to } : {}, updatedAt: stamp });
    return true;
  }
  __name(applyPatch, "applyPatch");
  function ownedNamedJobs() {
    const owner = workerMode ? void 0 : currentOwner();
    return readRecentJobs(AGENT_SCAN_MAX).filter((job) => {
      if (typeof job.name !== "string" || job.name.length === 0) return false;
      return owner === void 0 || job.owner === void 0 || job.owner === owner;
    });
  }
  __name(ownedNamedJobs, "ownedNamedJobs");
  function rosterNames() {
    if (workerMode) return Array.isArray(workerSpec?.rosterNames) ? workerSpec.rosterNames : [];
    const names = [];
    for (const job of ownedNamedJobs()) {
      const name = job.name;
      if (typeof name === "string" && !names.includes(name)) names.push(name);
    }
    return names;
  }
  __name(rosterNames, "rosterNames");
  function namedJob(name) {
    const wanted = name.trim();
    if (wanted.length === 0) return void 0;
    const allowed = workerMode ? rosterNames() : void 0;
    if (allowed !== void 0 && !allowed.includes(wanted)) return void 0;
    const matches = ownedNamedJobs().filter((job) => job.name === wanted);
    return matches.find((job) => runtime.alive(job.workerPid) || runtime.alive(job.pid)) ?? matches[0];
  }
  __name(namedJob, "namedJob");
  function appendLive(id, from, message) {
    const dir = join12(controlRoot, id);
    try {
      spool.mkdir(dir);
      spool.appendFile(join12(dir, "send.jsonl"), `${JSON.stringify({ from, message, at: runtime.now() })}
`);
      return true;
    } catch {
      return false;
    }
  }
  __name(appendLive, "appendLive");
  function rosterFor(selfName) {
    const entries = [];
    const names = [];
    for (const job of ownedNamedJobs()) {
      const name = job.name;
      if (typeof name !== "string" || name === selfName || names.includes(name)) continue;
      names.push(name);
      entries.push({ name, state: canonicalState(job.state), jobId: job.id });
    }
    if (workerMode) {
      const inherited = [String(workerSpec?.name ?? ""), ...Array.isArray(workerSpec?.rosterNames) ? workerSpec.rosterNames : []];
      for (const name of inherited) {
        if (name.length === 0 || name === selfName || names.includes(name)) continue;
        names.push(name);
        entries.push({ name });
      }
    }
    const roster = buildRoster(entries, selfName);
    return { ...roster.length > 0 ? { roster } : {}, rosterNames: names };
  }
  __name(rosterFor, "rosterFor");
  function enqueueSignal(message) {
    try {
      const stamp = `${runtime.now()}-${message.id}-${runtime.pid}-${Math.random().toString(36).slice(2, 6)}.json`;
      const tmp = join12(inboxDir, `.tmp-${stamp}`);
      spool.writeFile(tmp, JSON.stringify(message, null, 2));
      spool.rename(tmp, join12(inboxDir, stamp));
    } catch {
    }
  }
  __name(enqueueSignal, "enqueueSignal");
  function hasPendingSignal(id) {
    try {
      return spool.readdir(inboxDir).some((name) => name.includes(id)) || spool.readdir(claimedDir).some((name) => name.includes(id));
    } catch {
      return false;
    }
  }
  __name(hasPendingSignal, "hasPendingSignal");
  function reconcileJobs() {
    if (workerMode) return;
    const now = runtime.now();
    for (const job of readRecentJobs(RECONCILE_SCAN_MAX)) {
      if (typeof job.owner === "string" && job.owner.length > 0 && job.owner !== ownerToken) continue;
      const state = normalizeState(job.state);
      if (state !== void 0 && isTerminal(state)) finalizeJobWorktree(job);
      if (state === "queued" || state === "running") {
        if (runtime.alive(job.pid) || runtime.alive(job.workerPid)) continue;
        if (hasPendingSignal(job.id)) continue;
        const startedAt = Number(job.startedAt ?? 0) || 0;
        if (now - startedAt < LOST_GRACE_MS) continue;
        failJob(job, state === "queued" ? "worker never started" : "lost (no live process)", spawnCtx);
      } else if (state !== void 0 && state !== job.state) {
        applyPatch(job.id, { state }, job, now);
      }
    }
  }
  __name(reconcileJobs, "reconcileJobs");
  let jobCache = [];
  let lastStatus = "";
  function updateStatus() {
    try {
      const ui = cmd.ui;
      if (ui === void 0 || typeof ui.setStatus !== "function") return;
      if (ui.capabilities !== void 0 && ui.capabilities.status === false) return;
      const counts = {};
      for (const job of jobCache) {
        const state = canonicalState(job.state);
        counts[state] = (counts[state] ?? 0) + 1;
      }
      const pending = countPending();
      const parts = [];
      if (counts.running) parts.push(`${counts.running} running`);
      if (counts.queued) parts.push(`${counts.queued} queued`);
      if (counts["waiting-input"]) parts.push(`${counts["waiting-input"]} waiting`);
      if (pending) parts.push(`${pending} signal${pending === 1 ? "" : "s"}`);
      if (tasksHintVisible(jobCache, runtime.now())) parts.push("/tasks");
      const text = parts.length > 0 ? `\u25C8 ${parts.join(" \xB7 ")}` : "";
      if (text === lastStatus) return;
      lastStatus = text;
      ui.setStatus(text.length > 0 ? text : null);
    } catch {
    }
  }
  __name(updateStatus, "updateStatus");
  function sweepSubagentStopHooks() {
    if (workerMode) return;
    const owner = currentOwner();
    for (const job of jobCache) {
      const state = normalizeState(job.state);
      if (state === void 0 || !isTerminal(state) || job.subagentStopHook === true) continue;
      if (typeof job.owner === "string" && job.owner.length > 0 && job.owner !== owner) continue;
      runHookRules(runtime.run, projectHookRules("SubagentStop"), {
        event: "SubagentStop",
        cwd: String(job.cwd ?? runtime.cwd),
        agent: job.agentType,
        input: { id: job.id, state }
      });
      writeJob({ ...job, subagentStopHook: true, updatedAt: runtime.now() });
    }
  }
  __name(sweepSubagentStopHooks, "sweepSubagentStopHooks");
  function listText(filter = {}) {
    const all = readRecentJobs(LIST_SCAN_MAX);
    const wanted = filter.state === void 0 ? void 0 : canonicalState(filter.state);
    const matching = wanted === void 0 ? all : all.filter((job) => canonicalState(job.state) === wanted);
    const shown = matching.slice(0, filter.limit ?? LIST_SCAN_MAX);
    const counts = {};
    for (const job of all) {
      const state = canonicalState(job.state);
      counts[state] = (counts[state] ?? 0) + 1;
    }
    const summary = Object.entries(counts).map(([state, count]) => `${count} ${state}`).join(", ") || "none";
    const pending = countPending();
    const orphaned = countOrphaned();
    const lines = shown.map((job) => {
      const state = canonicalState(job.state);
      const exit = typeof job.exitCode === "number" ? ` exit ${job.exitCode}` : "";
      const elapsed = typeof job.durationMs === "number" ? `${(job.durationMs / 1e3).toFixed(1)}s` : typeof job.startedAt === "number" ? `${Math.max(0, Math.round((runtime.now() - job.startedAt) / 1e3))}s\u2026` : "";
      const duration = elapsed.length > 0 ? ` ${elapsed}` : "";
      const model = typeof job.model === "string" && job.model.length > 0 ? ` \xB7 ${job.model}` : "";
      const effort = typeof job.effort === "string" && job.effort.length > 0 ? ` \xB7 effort=${job.effort}` : "";
      const color = typeof job.color === "string" && job.color.length > 0 ? ` \xB7 color=${job.color}` : "";
      const owner = typeof job.owner === "string" && job.owner.length > 0 ? ` \xB7 ${job.owner.slice(-6)}` : "";
      const unread = isTerminalState(state) && job.deliveredAt === void 0 ? " \xB7 unread" : "";
      const agentType = typeof job.agentType === "string" && job.agentType.length > 0 ? ` \xB7 subagent_type=${job.agentType}` : job.kind === "agent" ? " \xB7 subagent_type=general-purpose" : "";
      const agentId = typeof job.agentId === "string" && job.agentId.length > 0 ? ` \xB7 agent id ${job.agentId}` : "";
      const name = typeof job.name === "string" && job.name.length > 0 ? ` \xB7 name=${job.name}` : "";
      return `  ${stateMarker(state)} ${job.id}  ${String(job.description ?? "(unlabelled)")}  [${state}${exit}${duration}]${agentType}${agentId}${name}${model}${effort}${color}${owner}${unread}`;
    });
    const orphanNote = orphaned > 0 ? ` ${orphaned} orphaned signal(s) quarantined.` : "";
    const filterNote = wanted === void 0 ? "" : ` matching ${wanted}`;
    return `${matching.length} job(s)${filterNote}, ${all.length} recent job(s): ${summary}. ${pending} signal(s) waiting for this session.${orphanNote}
` + (lines.length > 0 ? lines.join("\n") : "  (none)");
  }
  __name(listText, "listText");
  function killPid(pid, signal) {
    if (typeof pid !== "number" || !Number.isInteger(pid) || pid <= 0) return;
    try {
      runtime.kill(pid, signal);
    } catch {
    }
  }
  __name(killPid, "killPid");
  function cancelJob(id) {
    const job = readJob(id);
    if (job === void 0) return { ok: false, message: `subagent: no job ${id}` };
    const state = normalizeState(job.state);
    if (state !== void 0 && isTerminal(state)) {
      if (!liveState(job).alive) return { ok: false, message: `subagent: job ${id} is already ${state}` };
      try {
        spool.mkdir(join12(controlRoot, id));
        spool.writeFile(join12(controlRoot, id, "cancel"), "1");
      } catch {
      }
      killPid(job.pid, "SIGTERM");
      killPid(job.workerPid, "SIGTERM");
      return { ok: true, message: `subagent: stopped live agent ${id} (${String(job.description ?? "unlabelled")})` };
    }
    const now = runtime.now();
    const startedAt = Number(job.startedAt ?? now) || now;
    const signal = signalOf(
      {
        ...job,
        owner: typeof job.owner === "string" && job.owner.length > 0 ? job.owner : currentOwner(),
        startedAt
      },
      { state: "canceled", exitCode: null, finishedAt: now, durationMs: Math.max(0, now - startedAt), body: "canceled by request" }
    );
    if (!applyPatch(id, { ...patchOf(signal), canceledAt: now }, job, now)) {
      return { ok: false, message: `subagent: job ${id} is already ${canonicalState(job.state)}` };
    }
    killPid(job.pid, "SIGTERM");
    killPid(job.workerPid, "SIGTERM");
    const escalate = /* @__PURE__ */ __name(() => {
      killPid(job.pid, "SIGKILL");
      killPid(job.workerPid, "SIGKILL");
    }, "escalate");
    try {
      cmd.clock.after(1e3, escalate);
    } catch {
      runtime.later(1e3, escalate);
    }
    enqueueSignal(signal);
    safeDrain();
    refreshJobs();
    return { ok: true, message: `subagent: canceled ${id} (${String(job.description ?? "unlabelled")})` };
  }
  __name(cancelJob, "cancelJob");
  const backgroundedForegrounds = /* @__PURE__ */ new Set();
  const dismissedTasks = /* @__PURE__ */ new Set();
  let selectedTaskId;
  let agentViewOpen = false;
  let panelNotice;
  function visibleTaskRows() {
    return taskPanelRows(jobCache, { now: runtime.now(), dismissed: dismissedTasks });
  }
  __name(visibleTaskRows, "visibleTaskRows");
  function selectedTask() {
    if (selectedTaskId === void 0) return void 0;
    return visibleTaskRows().find((job) => job.id === selectedTaskId);
  }
  __name(selectedTask, "selectedTask");
  function selectTask(delta) {
    const rows = visibleTaskRows();
    selectedTaskId = moveTaskSelection(rows, selectedTaskId, delta);
    cmd.ui.refreshWidgets();
    return selectedTask();
  }
  __name(selectTask, "selectTask");
  function dismissOrStopTask(id) {
    const job = readJob(id);
    if (job === void 0) return { ok: false, message: `subagent: no job ${id}` };
    const state = canonicalState(job.state);
    if (state === "queued" || state === "running") return cancelJob(id);
    dismissedTasks.add(id);
    selectedTaskId = selectedTaskId === id ? void 0 : selectedTaskId;
    refreshJobs();
    cmd.ui.refreshWidgets();
    return { ok: true, message: `subagent: dismissed ${id} from the task view` };
  }
  __name(dismissOrStopTask, "dismissOrStopTask");
  function backgroundForegroundTask() {
    const job = jobCache.find((candidate) => {
      const state = canonicalState(candidate.state);
      return candidate.background === false && (state === "queued" || state === "running");
    });
    if (job === void 0) return void 0;
    writeJob({ ...job, background: true, backgroundedAt: runtime.now(), updatedAt: runtime.now() });
    backgroundedForegrounds.add(job.id);
    refreshJobs();
    return job;
  }
  __name(backgroundForegroundTask, "backgroundForegroundTask");
  cmd.ui.pane({
    id: "jobs",
    title: "Async subagents",
    render: /* @__PURE__ */ __name(() => {
      try {
        const rows = visibleTaskRows();
        const nodes = [
          {
            type: "row",
            children: [
              { type: "text", text: "\u25C8 subagents", bold: true },
              { type: "text", text: ` ${rows.length} task${rows.length === 1 ? "" : "s"} \xB7 ${countPending()} waiting`, dim: true }
            ]
          },
          { type: "divider" }
        ];
        if (rows.length === 0) {
          nodes.push({ type: "text", text: "No tasks running or recently failed.", dim: true });
        }
        for (const job of rows.slice(0, PANE_ROWS_MAX)) {
          const state = canonicalState(job.state);
          const duration = typeof job.durationMs === "number" ? ` ${(job.durationMs / 1e3).toFixed(1)}s` : "";
          const color = typeof job.color === "string" && job.color.length > 0 ? job.color : "";
          const selected = job.id === selectedTaskId ? "\u25B8 " : "  ";
          nodes.push({
            type: "text",
            text: `${selected}${stateMarker(state)} ${job.id}  ${String(job.description ?? "(unlabelled)").slice(0, 36)}  ${state}${duration}${color.length > 0 ? `  ${color}` : ""}`,
            // The definition color is the agent's identity; the state color is the
            // fallback, so an uncoloured job still reports its lifecycle.
            color: color.length > 0 ? color : stateColor(state)
          });
        }
        if (panelNotice !== void 0) {
          nodes.push({ type: "divider" }, { type: "text", text: panelNotice });
        }
        nodes.push({
          type: "row",
          children: [
            {
              type: "button",
              label: "Prev",
              hotkey: "\u2191",
              key: "subagent-prev",
              onPress: /* @__PURE__ */ __name(() => selectTask(-1), "onPress")
            },
            {
              type: "button",
              label: "Next",
              hotkey: "\u2193",
              key: "subagent-next",
              onPress: /* @__PURE__ */ __name(() => selectTask(1), "onPress")
            },
            {
              type: "button",
              label: "Enter",
              hotkey: "\u21B5",
              key: "subagent-open",
              onPress: /* @__PURE__ */ __name(() => {
                const job = selectedTask();
                if (job === void 0) return;
                agentViewOpen = true;
                const shown = outputText(job.id, {});
                panelNotice = shown.ok ? String(shown.content?.[0]?.text ?? "").slice(0, 4e3) : String(shown.error ?? "cannot read the task");
                cmd.ui.refreshWidgets();
              }, "onPress")
            },
            {
              type: "button",
              label: "x",
              hotkey: "x",
              key: "subagent-dismiss",
              onPress: /* @__PURE__ */ __name(() => {
                const job = selectedTask();
                if (job !== void 0) dismissOrStopTask(job.id);
              }, "onPress")
            },
            {
              type: "button",
              label: "Esc",
              hotkey: "esc",
              key: "subagent-back",
              onPress: /* @__PURE__ */ __name(() => {
                selectedTaskId = void 0;
                agentViewOpen = false;
                cmd.ui.refreshWidgets();
              }, "onPress")
            },
            {
              type: "button",
              label: "Refresh",
              hotkey: "r",
              key: "subagent-refresh",
              onPress: /* @__PURE__ */ __name(() => {
                refreshJobs();
                cmd.ui.refreshWidgets();
              }, "onPress")
            },
            {
              type: "button",
              label: "Cancel newest",
              hotkey: "c",
              key: "subagent-cancel",
              onPress: /* @__PURE__ */ __name(() => {
                const active = jobCache.find((job) => {
                  const state = canonicalState(job.state);
                  return state === "queued" || state === "running";
                });
                if (active !== void 0) cancelJob(active.id);
                refreshJobs();
                cmd.ui.refreshWidgets();
              }, "onPress")
            }
          ]
        });
        return nodes;
      } catch {
        return ["subagent: cannot render the jobs pane"];
      }
    }, "render")
  });
  function readLog(file) {
    try {
      return spool.readFile(file);
    } catch {
      return "";
    }
  }
  __name(readLog, "readLog");
  const planCtx = {
    fs: spool,
    env: runtime.env,
    cwd: cmd.cwd,
    runtimeCwd: runtime.cwd,
    home: runtime.home,
    transcriptsDir,
    appendPromptText,
    forkMode,
    forkModeEnabled,
    managedDir: managedSettingsDir(cmd),
    addDirs,
    sessionId: /* @__PURE__ */ __name(() => sessionId, "sessionId"),
    mainPermissionMode: /* @__PURE__ */ __name(() => mainPermissionMode, "mainPermissionMode"),
    resolveAgentDefinition,
    systemPromptFor: subagentSystemPrompt,
    startupFor: /* @__PURE__ */ __name((definition, cwd, prompt, model) => startupFor(definition, cwd, prompt, model), "startupFor"),
    canonicalPermissionMode,
    resolveTeamPolicy,
    resolvePermissionMode,
    resolveExecutionPolicy,
    mcpServerConfigs,
    resolveAgentMcpServers: /* @__PURE__ */ __name((input) => resolveAgentMcpServers({
      defined: input.defined,
      configured: input.configured,
      definition: input.definition,
      env: input.env,
      trusted: input.trusted
    }), "resolveAgentMcpServers"),
    agentFolderTrusted: /* @__PURE__ */ __name((definition, roots) => agentFolderTrusted(
      definition,
      roots
    ), "agentFolderTrusted"),
    resolveLaunch: /* @__PURE__ */ __name((input) => resolveLaunch(input), "resolveLaunch"),
    withMemoryTools,
    contextWindowForModel,
    resolveAutoCompactWindow,
    listField
  };
  const spawnCtx = {
    fs: spool,
    env: runtime.env,
    execPath: runtime.execPath,
    spawn: runtime.spawn,
    now: /* @__PURE__ */ __name(() => runtime.now(), "now"),
    sleep: runtime.sleep,
    workerSpec,
    workerSource: WORKER_SOURCE,
    modPath: MOD_PATH,
    inboxDir,
    jobsDir,
    logsDir,
    worktreesDir,
    controlRoot,
    git,
    sessionId: /* @__PURE__ */ __name(() => sessionId, "sessionId"),
    owner: currentOwner,
    recordFromPlan: /* @__PURE__ */ __name((intent, id, logPath, startedAt, owner) => recordFromPlan(intent, id, logPath, startedAt, owner, planCtx), "recordFromPlan"),
    sessionNameOf,
    readJob,
    writeJob,
    readRecentJobs,
    applyPatch,
    enqueueSignal,
    refreshJobs,
    finalizeJobWorktree,
    foregroundOutcome,
    readLog,
    rosterFor,
    runProjectHooks: /* @__PURE__ */ __name((event, cwd, agent, input) => runHookRules(runtime.run, projectHookRules(event), { event, cwd, agent, input }), "runProjectHooks"),
    appendJobEntry: /* @__PURE__ */ __name((entry) => {
      cmd.session?.appendCustomEntry(entry);
    }, "appendJobEntry"),
    answerPendingPermission: /* @__PURE__ */ __name((controlDir, answered) => answerPendingPermission(controlDir, answered, permissionCtx), "answerPendingPermission"),
    backgrounded: backgroundedForegrounds
  };
  const tools = createTools({
    cmd,
    planCtx,
    spawnCtx,
    runtime,
    spool,
    logsDir,
    transcriptsDir,
    readJob,
    writeJob,
    readRecentJobs,
    refreshJobs,
    enqueueSignal,
    safeDrain,
    cancelJob,
    registry,
    ledger,
    currentOwner,
    ownerId,
    rosterNames,
    namedJob,
    liveState,
    appendLive,
    workerMode,
    workerSpec: /* @__PURE__ */ __name(() => workerSpec, "workerSpec"),
    sessionId: /* @__PURE__ */ __name(() => sessionId, "sessionId"),
    forkModeEnabled,
    listText,
    scanOutput,
    reportText,
    listAgentsVisible
  });
  tools.register();
  const { outputText, sendToRecord } = tools;
  function agentsText(filter) {
    registry.refresh();
    const entries = registry.entries();
    const wanted = String(filter ?? "").trim().toLowerCase();
    const shown = wanted.length === 0 ? entries : entries.filter((entry) => entry.name.toLowerCase() === wanted || entry.sourcePath.toLowerCase().includes(wanted));
    const counts = {};
    let valid = 0;
    for (const entry of entries) {
      if (!entry.valid) continue;
      valid += 1;
      counts[entry.scope] = (counts[entry.scope] ?? 0) + 1;
    }
    const summary = AGENT_SCOPES.filter((scope) => (counts[scope] ?? 0) > 0).map((scope) => `${counts[scope]} ${scope === "builtin" ? "built-in" : scope}`).join(", ");
    const lines = shown.map((entry) => {
      const flags = [];
      if (entry.definition?.readOnly === true) flags.push("read-only");
      if (entry.definition?.oneShot === true) flags.push("one-shot");
      if (entry.definition?.plugin !== void 0) flags.push(`plugin ${entry.definition.plugin}`);
      if (entry.scope !== "builtin" && Object.hasOwn(HOST_RESERVED_AGENT_NAMES, entry.name)) {
        flags.push("shadows a host-reserved name");
      }
      if (entry.warning !== void 0) flags.push(`warning: ${entry.warning}`);
      const state = entry.kind === "duplicate" ? `duplicate: ${entry.reason ?? "unknown"}` : entry.valid ? "valid" : `invalid: ${entry.reason ?? "unknown"}`;
      const location = entry.sourcePath.length > 0 ? `  ${entry.sourcePath}` : "";
      const settings = [
        entry.definition?.model !== void 0 ? `model=${entry.definition.model}` : "",
        entry.definition?.effort !== void 0 ? `effort=${entry.definition.effort}` : "",
        entry.definition?.color !== void 0 ? `color=${entry.definition.color}` : "",
        entry.definition?.tools !== void 0 ? `tools=${entry.definition.tools.join("+")}` : ""
      ].filter(Boolean).join(" ");
      return `  ${entry.name.length > 0 ? entry.name : "(unnamed)"}  ${entry.scope}  ${state}${flags.length > 0 ? ` \xB7 ${flags.join(" \xB7 ")}` : ""}${settings.length > 0 ? ` \xB7 ${settings}` : ""}${location}`;
    });
    return `${valid} valid definition(s)${summary.length > 0 ? ` (${summary})` : ""}, ${entries.length - valid} invalid.
` + (lines.length > 0 ? lines.join("\n") : "  (none)");
  }
  __name(agentsText, "agentsText");
  function doctorText(filter) {
    registry.refresh();
    const entries = registry.entries();
    if (String(filter ?? "").trim().toLowerCase() === "plugins") {
      const pluginEntries = entries.filter((entry) => entry.scope === "plugin");
      const problems = pluginEntries.filter((entry) => !entry.valid || entry.warning !== void 0);
      const lines2 = pluginEntries.map((entry) => {
        const state = entry.kind === "valid" ? entry.warning !== void 0 ? `warning: ${entry.warning}` : "valid" : `${entry.kind}: ${entry.reason ?? "unknown"}`;
        return `  ${entry.name.length > 0 ? entry.name : "(unnamed)"}  ${state}  ${entry.sourcePath}`;
      });
      return `plugin validation: ${pluginEntries.length} file(s), ${problems.length} problem(s)
` + (lines2.length > 0 ? lines2.join("\n") : "  (no plugin agents)");
    }
    const duplicates = entries.filter((entry) => entry.kind === "duplicate");
    const invalid = entries.filter((entry) => entry.kind === "invalid");
    const warnings = entries.filter((entry) => entry.valid && entry.warning !== void 0);
    const pluginAgents = entries.filter((entry) => entry.scope === "plugin" && entry.valid && entry.definition !== void 0).map((entry) => entry.name);
    const lines = [];
    for (const entry of duplicates) {
      lines.push(`  duplicate ${entry.name}: kept ${entry.duplicateOf ?? "?"}, skipped ${entry.sourcePath} - rename or remove one`);
    }
    for (const entry of invalid) {
      lines.push(`  invalid ${entry.sourcePath.length > 0 ? entry.sourcePath : entry.name}: ${entry.reason ?? "unknown"}`);
    }
    for (const entry of warnings) lines.push(`  warning ${entry.sourcePath}: ${entry.warning}`);
    if (pluginAgents.length > 0) lines.push(`  plugin agents: ${pluginAgents.join(", ")}`);
    const summary = `${duplicates.length} duplicate(s), ${invalid.length} invalid, ${warnings.length} warning(s)`;
    return `subagents doctor: ${lines.length > 0 ? summary : "ok"}
` + (lines.length > 0 ? lines.join("\n") : "  no duplicate or invalid definitions");
  }
  __name(doctorText, "doctorText");
  function commandHandler({ args }) {
    const [sub, ...rest] = String(args ?? "").trim().split(/\s+/).filter(Boolean);
    if (sub === "list") return { message: listText() };
    if (sub === "cancel" && rest[0] !== void 0) return { message: cancelJob(rest[0]).message };
    if (sub === "open" && rest[0] !== void 0) {
      const shown = outputText(rest[0], {});
      return { message: shown.ok ? String(shown.content?.[0]?.text ?? "") : String(shown.error ?? "") };
    }
    agentViewOpen = true;
    try {
      cmd.ui.openPane("jobs");
    } catch {
    }
    return { message: "subagents: opened the async subagents pane (list | jobs | cancel <id> | open <id>)" };
  }
  __name(commandHandler, "commandHandler");
  function recordForTarget(target) {
    const wanted = String(target ?? "").trim();
    if (wanted.length === 0) return void 0;
    return readJob(wanted) ?? namedJob(wanted);
  }
  __name(recordForTarget, "recordForTarget");
  function mentionText(prefix) {
    const definitions = registry.entries().map((entry) => ({ name: entry.name, valid: entry.valid }));
    const typed = String(prefix ?? "").replace(/^@/, "").toLowerCase();
    const names = mentionCandidates(definitions, rosterNames()).filter((name) => name.toLowerCase().startsWith(typed));
    return names.length > 0 ? names.map((name) => `@${name}`).join("  ") : "no matching @mention";
  }
  __name(mentionText, "mentionText");
  async function tasksHandler({ args }) {
    const [sub, ...rest] = String(args ?? "").trim().split(/\s+/).filter(Boolean);
    if (sub === void 0) {
      agentViewOpen = true;
      try {
        cmd.ui.openPane("jobs");
      } catch {
      }
      return {
        message: "tasks: opened the task view (next | prev | enter [message] | open <id> | send <id> <message> | x <id> | stop <id> | esc | mention <prefix>)"
      };
    }
    if (sub === "next" || sub === "prev") {
      const row = selectTask(sub === "next" ? 1 : -1);
      return { message: row === void 0 ? "tasks: no visible rows" : `tasks: selected ${row.id} (${canonicalState(row.state)})` };
    }
    if (sub === "open" || sub === "enter") {
      const target = rest[0] ?? selectedTaskId;
      const record = recordForTarget(target);
      if (record === void 0) return { message: "tasks: no task selected" };
      selectedTaskId = record.id;
      const shown = outputText(record.id, {});
      if (sub === "enter" && rest.length > 1) {
        const sent = await sendToRecord(record, rest.slice(1).join(" "), ROSTER_MAIN);
        return { message: `${shown.ok ? String(shown.content?.[0]?.text ?? "") : String(shown.error ?? "")}

${sent.ok ? String(sent.content?.[0]?.text ?? "") : String(sent.error ?? "")}` };
      }
      return { message: shown.ok ? String(shown.content?.[0]?.text ?? "") : String(shown.error ?? "") };
    }
    if (sub === "send") {
      const record = recordForTarget(rest[0] ?? selectedTaskId);
      const message = rest.slice(1).join(" ");
      if (record === void 0) return { message: "tasks: no task selected" };
      if (message.trim().length === 0) return { message: "tasks: usage: send <id> <message>" };
      const sent = await sendToRecord(record, message, ROSTER_MAIN);
      return { message: sent.ok ? String(sent.content?.[0]?.text ?? "") : String(sent.error ?? "") };
    }
    if (sub === "stop" || sub === "cancel") {
      const record = recordForTarget(rest[0] ?? selectedTaskId);
      if (record === void 0) return { message: "tasks: no task selected" };
      return { message: cancelJob(record.id).message };
    }
    if (sub === "x" || sub === "dismiss") {
      const record = recordForTarget(rest[0] ?? selectedTaskId);
      if (record === void 0) return { message: "tasks: no task selected" };
      return { message: dismissOrStopTask(record.id).message };
    }
    if (sub === "esc" || sub === "back") {
      selectedTaskId = void 0;
      agentViewOpen = false;
      return { message: "tasks: returned to the prompt" };
    }
    if (sub === "mention") return { message: mentionText(rest.join(" ")) };
    return { message: "tasks: usage: next | prev | enter [message] | open <id> | send <id> <message> | x <id> | stop <id> | esc" };
  }
  __name(tasksHandler, "tasksHandler");
  async function subtaskHandler({ args }) {
    const prompt = String(args ?? "").trim();
    if (prompt.length === 0) return { message: "subtask: usage: /subtask <prompt>" };
    if (agentViewOpen) return { message: "subtask: the agent view is on; select a task row (or Esc) before starting another" };
    if (sessionId === void 0 || sessionId.length === 0) return { message: "subtask: the main session has not bound yet; try again" };
    const launched = await fire(
      {
        description: "subtask",
        kind: "fork",
        prompt,
        background: true,
        cwd: cmd.cwd,
        agentType: FORK_AGENT_TYPE,
        fork: true,
        mainSessionId: sessionId
      },
      spawnCtx
    );
    if (launched.ok === false) return { message: `subtask: ${launched.error}` };
    return {
      message: `subtask: started fork ${launched.id} from the main conversation, pid ${launched.pid}.
/tasks open ${launched.id} reads it; /tasks send ${launched.id} <message> steers it.`
    };
  }
  __name(subtaskHandler, "subtaskHandler");
  cmd.addCommand({
    name: "subagents",
    description: "Async subagents: list, jobs, cancel <id>, open <id>",
    argumentHint: "list|jobs|cancel <id>|open <id>",
    handler: commandHandler
  });
  cmd.addCommand({
    name: "pubsub",
    description: "[deprecated: use /subagents] Async subagents: list, jobs, cancel <id>, open <id>",
    argumentHint: "list|jobs|cancel <id>|open <id>",
    handler: commandHandler
  });
  cmd.addCommand({
    name: "tasks",
    description: "Agent panel: list running and recently failed tasks, open a transcript, send a follow-up, stop or dismiss a row",
    argumentHint: "next|prev|enter [message]|open <id>|send <id> <message>|x <id>|stop <id>|esc|mention <prefix>",
    handler: tasksHandler
  });
  cmd.addCommand({
    name: "subtask",
    description: "Start a fork of the main conversation when the agent view is off",
    argumentHint: "<prompt>",
    handler: subtaskHandler
  });
  cmd.addCommand({
    name: "agents",
    description: "List subagent definitions: scope, validity, and the file that provides each one",
    argumentHint: "[name|path]",
    handler: /* @__PURE__ */ __name(({ args }) => ({ message: agentsText(args) }), "handler")
  });
  cmd.addCommand({
    name: "add-dir",
    description: "Load subagent definitions from .commandcode/agents/ under an added directory",
    argumentHint: "<path>",
    handler: /* @__PURE__ */ __name(({ args }) => {
      const wanted = String(args ?? "").trim();
      if (wanted.length === 0) return { message: "add-dir: usage: /add-dir <path>" };
      const dir = isAbsolute2(wanted) ? wanted : join12(cmd.cwd, wanted);
      if (!addDirs.includes(dir)) addDirs.push(dir);
      registry.refresh();
      const loaded = registry.entries().filter((entry) => entry.scope === "project" && isInside(dir, entry.sourcePath));
      return { message: `add-dir: ${loaded.length} definition(s) loaded from ${dir}` };
    }, "handler")
  });
  cmd.addCommand({
    name: "doctor",
    description: "Report duplicate, invalid, and plugin agent definitions, with the fix for each one",
    argumentHint: "[plugins]",
    handler: /* @__PURE__ */ __name(({ args }) => ({ message: doctorText(args) }), "handler")
  });
  try {
    cmd.addShortcut("ctrl+b", {
      description: "Move the running foreground subagent task to the background",
      handler: /* @__PURE__ */ __name(() => {
        const job = backgroundForegroundTask();
        if (job === void 0) {
          cmd.ui.notify("no foreground subagent task to background", "info");
          return;
        }
        cmd.ui.notify(`${String(job.description ?? job.id)} continues in the background; /tasks shows it`, "info");
      }, "handler")
    });
  } catch {
  }
  const isolation = isolationRoots(git, String(workerSpec?.cwd ?? runtime.cwd), workerSpec);
  const workerHooks = workerMode ? workerSpec?.hooks : void 0;
  const workerTools = workerMode && Array.isArray(workerSpec?.tools) ? workerSpec.tools : void 0;
  const workerDisallowed = workerMode && Array.isArray(workerSpec?.disallowedTools) ? workerSpec.disallowedTools : void 0;
  const workerHookCwd = String(workerSpec?.cwd ?? runtime.cwd);
  const workerHookAgent = workerSpec?.agentType;
  cmd.hooks({
    // The worker process loads this mod with `cmd --mod <file> acp`. Its whole
    // purpose there is this hook: the definition body and the startup bundle are
    // the worker's system prompt. The parent already has both, so only worker mode
    // registers it, and only when there is something to append.
    // In the main session the same hook is a read-only tap: the host passes the
    // live permission mode, and an empty string appends nothing.
    ...workerSystemPrompt().length > 0 ? { appendSystemPrompt: /* @__PURE__ */ __name(() => workerSystemPrompt(), "appendSystemPrompt") } : !workerMode ? {
      // The session-wide agent's body is the main session's prompt; with no
      // `--agent` this is a read-only tap for the permission mode.
      appendSystemPrompt: /* @__PURE__ */ __name((context) => {
        noteMainPermissionMode(context?.permissionMode);
        return sessionAgent()?.prompt ?? "";
      }, "appendSystemPrompt")
    } : {},
    // initialPrompt is a main-session first turn. If the host exposes
    // `cmd.prompt.submit`, it is submitted there; otherwise the first typed
    // prompt is prefixed, so the value is never silently dropped.
    ...!workerMode && sessionAgent()?.initialPrompt !== void 0 ? {
      transformInput: /* @__PURE__ */ __name((input) => {
        const text = sessionAgent()?.initialPrompt;
        if (text === void 0 || initialPromptSubmitted) return void 0;
        initialPromptSubmitted = true;
        return { action: "transform", text: `${text}

${String(input?.text ?? "")}` };
      }, "transformInput")
    } : {},
    // A blocked tool returns the reason as its result, so the model learns where it
    // is allowed to work instead of silently retrying. The definition's PreToolUse
    // hooks run after the isolation guard: a hook cannot widen a worktree.
    ...isolation !== void 0 || workerHooks?.PreToolUse !== void 0 || workerTools !== void 0 || workerDisallowed !== void 0 ? {
      beforeToolCall: /* @__PURE__ */ __name((call) => {
        const toolName = String(call?.toolName ?? "");
        const denied = workerTools !== void 0 ? !workerToolAllowed(
          toolName,
          workerTools,
          workerDisallowed ?? [],
          workerSpec.teamName !== void 0
        ) : (workerDisallowed ?? []).some((specifier) => withheldBy(specifier, toolName));
        if (denied) {
          return {
            block: true,
            additionalContext: `subagent: "${toolName}" is not in this agent's resolved tool set` + (workerTools !== void 0 && workerTools.length > 0 ? ` (${workerTools.join(", ")})` : "") + ". Use only the listed tools."
          };
        }
        const verdict = isolation === void 0 ? void 0 : guardToolCall(String(call?.toolName ?? ""), call?.input ?? {}, isolation);
        if (verdict !== void 0) return { block: true, additionalContext: verdict.reason };
        const outcome = runHookRules(runtime.run, workerHooks?.PreToolUse, {
          event: "PreToolUse",
          cwd: workerHookCwd,
          agent: workerHookAgent,
          toolName: String(call?.toolName ?? ""),
          input: call?.input ?? {}
        });
        return outcome.block === void 0 ? void 0 : { block: true, additionalContext: outcome.block };
      }, "beforeToolCall")
    } : {},
    // PostToolUse's exit 2 message reaches the model as an extra text block.
    ...workerHooks?.PostToolUse !== void 0 ? {
      afterToolCall: /* @__PURE__ */ __name((call) => {
        const outcome = runHookRules(runtime.run, workerHooks.PostToolUse, {
          event: "PostToolUse",
          cwd: workerHookCwd,
          agent: workerHookAgent,
          toolName: String(call?.toolName ?? ""),
          input: call?.input ?? {},
          result: call?.result
        });
        return outcome.additionalContext === void 0 ? void 0 : { additionalContext: outcome.additionalContext };
      }, "afterToolCall")
    } : {},
    onSessionStart: /* @__PURE__ */ __name((info) => {
      if (typeof info?.sessionId === "string" && info.sessionId.length > 0) {
        sessionId = info.sessionId;
      }
      ownerId();
      registry.refresh();
      const limit = descriptionLimit(registry.list());
      if (limit.over) {
        try {
          cmd.ui.notify(
            `agent descriptions are about ${limit.tokens} tokens, over the ${limit.maxTokens}-token description limit; shorten them so the model keeps choosing the right agent`,
            "warn"
          );
        } catch {
        }
      }
      sweepClaimed();
      sweepOrphaned();
      sweepExpired();
      reconcileJobs();
      refreshJobs();
      safeDrain();
      if (info?.source !== "resume" && info?.source !== "reload") submitInitialPrompt();
    }, "onSessionStart"),
    // The race guard. A worker can finish in the window between the last turn and the
    // next drain tick, and the session would then fall idle with the result unread.
    // Pull whatever is still on disk into THIS run as the continuation reason (A2A's
    // onStop inbox drain). Anything already delivered is no longer in the inbox, so
    // this cannot double-report a signal.
    onStop: /* @__PURE__ */ __name(() => {
      if (workerMode) {
        const outcome = runHookRules(runtime.run, workerHooks?.Stop, {
          event: "SubagentStop",
          cwd: workerHookCwd,
          agent: workerHookAgent
        });
        return outcome.continue === true ? { continue: true, reason: outcome.reason ?? "SubagentStop hook requested another turn." } : void 0;
      }
      const pending = claimPending(5);
      if (pending.length === 0) return void 0;
      for (const claim of pending) {
        cmd.showEntry("subagent", claim.signal);
        markDelivered(claim.signal.id);
        settle(claim, "accepted");
      }
      refreshJobs();
      return { continue: true, reason: pending.map((claim) => formatSignalMessage(claim.signal)).join("\n\n") };
    }, "onStop")
  });
  cmd.on("run_start", (event) => {
    if (typeof event?.sessionId === "string" && event.sessionId.length > 0) {
      sessionId = event.sessionId;
      ownerId();
      registry.refresh();
      safeDrain();
    }
  });
  cmd.on("permission_mode_changed", (event) => {
    noteMainPermissionMode(event?.mode);
  });
  cmd.clock.every(DRAIN_INTERVAL_MS, safeDrain);
  cmd.clock.every(REFRESH_INTERVAL_MS, refreshJobs);
  cmd.clock.every(RETENTION_SWEEP_INTERVAL_MS, sweepExpired);
  ownerId();
  reconcileJobs();
  refreshJobs();
}
__name(index_default, "default");
export {
  AGENT_API_ERROR,
  AGENT_SCOPES,
  BACKGROUND_BUILTIN_TOOLS,
  BUILTIN_AGENT_DEFINITIONS,
  DESCRIPTION_LIMIT_TOKENS,
  EFFORT_LEVELS,
  PERMISSION_MODE_LIST,
  SUBAGENT_TOOL_UNIVERSE,
  TEAM_TOOL_NAMES,
  UNIVERSAL_TOOL_REMOVALS,
  WORKER_SOURCE,
  agentDefinitionFromSource,
  agentFolderTrusted,
  buildArgs,
  buildRoster,
  buildSpec,
  buildStartupBundle,
  builtinAgentDefinitions,
  canonicalPermissionMode,
  claudeMdHierarchy,
  commandEscapesWorktree,
  contextWindowForModel,
  createAgentRegistry,
  createJobWorktree,
  index_default as default,
  defaultBranch,
  descriptionLimit,
  filterTools,
  finalizeWorktree,
  foregroundOutcome,
  gitSeam,
  gitStatusSnapshot,
  guardToolCall,
  hasAgentOutput,
  hookMatches,
  isolationRoots,
  jobStates,
  ledgerFactory,
  listAgentsVisible,
  mainCheckoutRoots,
  mcpServerConfigs,
  mcpServerNames,
  memoryDirFor,
  mentionCandidates,
  moveTaskSelection,
  parseCacheTtl,
  parseFrontmatter,
  parseHooks,
  parseMcpServers,
  permissionModes,
  readMemoryIndex,
  reportText,
  resolveAgentMcpServers,
  resolveAppendSystemPrompt,
  resolveAutoCompactWindow,
  resolveEffort,
  resolveExecutionPolicy,
  resolveForkMode,
  resolveLaunch,
  resolveMaxConcurrent,
  resolveMaxDepth,
  resolveModel,
  resolvePermissionMode,
  resolveTeamPolicy,
  runHookRules,
  scanOutput,
  signalEnvelope,
  skillFileFor,
  sweepExpiredEntries,
  taskPanelRows,
  taskRowVisible,
  tasksHintVisible,
  toolNameOfSpecifier,
  withMemoryTools,
  workerEnv,
  workerToolAllowed,
  worktreeChanged
};
