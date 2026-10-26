// MCP configuration resolution: discovery of host MCP server configs, trust of
// the folder that declares an inline server, and the single resolution that
// turns a definition's `mcpServers` into the exact server list a session gets.

import {isAbsolute, join} from 'node:path';
import {listField, stringField, stringMapField} from './definitions.ts';
import type {AgentMcpServer} from './definitions.ts';
import {managedSettingsHome, mcpPolicy} from './config.ts';
import type {SpoolFs} from './types.ts';
import {isInside} from './worktree.ts';

export interface McpServerConfig {
	command?: string;
	args?: string[];
	env?: Record<string, string>;
	url?: string;
}

// MCP server configs come from the host's config files. Discovery is best-effort:
// a missing or malformed file means "no MCP server known", never a session error.
// The first directory wins, matching the host's config precedence.
export function mcpServerConfigs(
	fs: Pick<SpoolFs, 'readFile'>,
	dirs: string[],
): Record<string, McpServerConfig> {
	const configs: Record<string, McpServerConfig> = {};
	for (const dir of dirs.slice(0, 4)) {
		if (typeof dir !== 'string' || dir.length === 0) continue;
		try {
			const parsed = JSON.parse(fs.readFile(join(dir, '.commandcode', 'mcp.json'))) as {
				mcpServers?: Record<string, Record<string, unknown> | undefined>;
			};
			for (const [name, config] of Object.entries(parsed?.mcpServers ?? {})) {
				if (name.length === 0 || config?.enabled === false || Object.hasOwn(configs, name)) continue;
				if (config === null || typeof config !== 'object') continue;
				const command = stringField(config.command);
				const url = stringField(config.url);
				const args = listField(config.args);
				const env = stringMapField(config.env);
				configs[name] = {
					...(command !== undefined ? {command} : {}),
					...(args !== undefined ? {args} : {}),
					...(env !== undefined ? {env} : {}),
					...(url !== undefined ? {url} : {}),
				};
			}
		} catch {
			/* no MCP config here */
		}
	}
	return configs;
}

export function mcpServerNames(fs: Pick<SpoolFs, 'readFile'>, dirs: string[]): string[] {
	return Object.keys(mcpServerConfigs(fs, dirs));
}

export interface McpTrustRoots {
	readonly cwd: string;
	readonly home: string;
	readonly managedDir?: string;
	readonly addDirs?: readonly string[];
}

// An inline server runs a command, so the folder that declares it must be one the
// session already trusts: the workspace, home, the managed directory, or an
// --add-dir. A plugin is never trusted with an inline server.
export function agentFolderTrusted(
	definition: {readonly scope?: string; readonly sourcePath?: string},
	roots: McpTrustRoots,
): boolean {
	if (definition.scope === 'plugin') return false;
	const path = definition.sourcePath;
	if (path === undefined || path.length === 0) return true;
	const absolute = isAbsolute(path) ? path : join(roots.cwd, path);
	const trustedRoots = [
		roots.cwd,
		roots.home,
		roots.managedDir ?? managedSettingsHome(roots.home),
		...(roots.addDirs ?? []),
	];
	return trustedRoots.some(root => root.length > 0 && isInside(root, absolute));
}

export interface McpResolutionInput {
	/** The definition's authored `mcpServers`. */
	readonly defined?: readonly AgentMcpServer[];
	readonly configured?: Record<string, McpServerConfig>;
	readonly definition?: {readonly scope?: string; readonly path?: string};
	readonly env?: Record<string, string | undefined>;
	/** The caller's trust decision; `false` drops every inline server. */
	readonly trusted?: boolean;
}

export interface McpDrop {
	readonly name: string;
	readonly reason: string;
}

export interface McpResolution {
	readonly servers: AgentMcpServer[];
	readonly dropped: McpDrop[];
}

// The one resolution: a name resolves from configured servers, an inline entry
// needs trust, and strict/bare/allow/deny policies apply afterwards. The result
// is what `session/new` receives, so the scope is exactly this agent's.
export function resolveAgentMcpServers(input: McpResolutionInput = {}): McpResolution {
	const env = input.env ?? {};
	const defined = input.defined ?? [];
	const dropped: McpDrop[] = [];
	if (input.definition?.scope === 'plugin') {
		for (const spec of defined) dropped.push({name: spec.name, reason: 'plugin definitions ignore mcpServers'});
		return {servers: [], dropped};
	}
	const policy = mcpPolicy(env);
	if (policy.bare) {
		for (const spec of defined) dropped.push({name: spec.name, reason: 'bare mode disables MCP'});
		return {servers: [], dropped};
	}
	const strict = policy.strict;
	const allow = policy.allow;
	const deny = policy.deny;
	const trusted = input.trusted !== false;
	const servers: AgentMcpServer[] = [];
	const seen = new Set<string>();
	for (const spec of defined) {
		const name = spec.name.trim();
		if (name.length === 0 || seen.has(name)) continue;
		const inline = spec.command !== undefined || spec.url !== undefined;
		let resolved: AgentMcpServer;
		if (inline) {
			if (!trusted) {
				dropped.push({name, reason: 'inline server outside a trusted folder'});
				continue;
			}
			if (strict) {
				dropped.push({name, reason: 'strict MCP config ignores inline servers'});
				continue;
			}
			resolved = {...spec, name};
		} else {
			const config = input.configured?.[name];
			if (config === undefined) {
				dropped.push({name, reason: 'not configured'});
				continue;
			}
			resolved = {name, ...config};
		}
		if (deny.includes('*') || deny.includes(name)) {
			dropped.push({name, reason: 'denied by the MCP policy'});
			continue;
		}
		if (allow.length > 0 && !allow.includes('*') && !allow.includes(name)) {
			dropped.push({name, reason: 'not in the MCP allow policy'});
			continue;
		}
		seen.add(name);
		servers.push(resolved);
	}
	return {servers, dropped};
}
