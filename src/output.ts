// Output scanning: a subagent's report is data, never instructions.
//
// WORKER_SOURCE embeds `scanOutput.toString()`, so the function body must stay
// plain and reference-free; the rule table lives inside it on purpose, so the
// worker and the parent run one contract. Everything else here is a leaf.

export interface ScanResult {
	readonly text: string;
	readonly matches: string[];
	readonly marked: boolean;
}

export function scanOutput(text: string): ScanResult {
	// Inline the marker and the rule names: this body is embedded in WORKER_SOURCE,
	// so it cannot read module constants.
	const marker = '[harness: subagent output matched instruction-shaped pattern(s): ';
	const settings = [
		['bypassPermissions', 'bypassPermissions permission setting'],
		['--dangerously-skip-permissions', '--dangerously-skip-permissions permission setting'],
	];
	const matches: string[] = [];
	let marked = false;
	let out = String(text ?? '');
	// A tag imitation is escaped, never removed: the text stays readable and can no
	// longer be mistaken for host output.
	out = out.replace(/<\/?system-reminder\b[^>]*>/gi, match => {
		if (!matches.includes('system-reminder tag')) matches.push('system-reminder tag');
		marked = true;
		return '\\' + match;
	});
	// A role label only counts at the start of a line, after indentation.
	out = out.replace(/^([ \t]*)(Human|Assistant):/gm, (_match, space: string, label: string) => {
		const name = label + ': role label';
		if (!matches.includes(name)) matches.push(name);
		return space + '\\' + label + ':';
	});
	// A permission-setting mention is reported, not rewritten: rewriting a flag
	// would hide what the subagent actually said.
	for (const [needle, name] of settings) {
		if (out.includes(needle) && !matches.includes(name)) {
			matches.push(name);
			marked = true;
		}
	}
	if (marked) out = marker + matches.join(', ') + ']\n' + out;
	return {text: out, matches, marked};
}

// The report the parent reads: the worker's own progress lines (`[tool] ...`) are
// not part of it. They stay in the log for the task view; a tool call is never a
// result the parent is asked to act on. Shell output is returned as written.
export function reportText(text: string, kind: string): string {
	const visible =
		kind === 'agent'
			? text.split('\n').filter(line => !/^\[tool[ \]]/.test(line)).join('\n')
			: text;
	return scanOutput(visible).text;
}
