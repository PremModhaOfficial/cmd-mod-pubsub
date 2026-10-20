# AGENTS.md

Pointers, not a spec.

- `SPEC.md` — the behavior contract.
- `CHECKLIST.md` — parity ledger and review gates.
- `GLOSSARY.md` — domain terms.
- `docs/split-map.md` — which module owns which symbol, and the import seams.

Commands: `npm run gate` (build, stale-bundle/typecheck/import checks, bundle
contract, smoke) and `node test/e2e.mjs` (real `cmd`, needs credentials and
network; not in CI).

The root `better-sub-agents.ts` is generated from `src/`. Edit `src/` only, run
`npm run build`, and commit the regenerated root file with the source change.

## Moving a group of code into a module

1. `node scripts/extract-region.mjs src/index.ts <start> <end> src/<name>.ts`
   cuts the range out and writes the new module in one command.
2. `npx tsc --noEmit` lists every name the new module still needs. Each is
   either an import from a lower module or a field on the context object the
   entry passes in. Add them; the checker says which, so do not re-read the
   region to find them.
3. Wire the entry to call the new module, then `npm run gate`.

Do not hand-audit free identifiers with grep and scripts, and do not re-read
the whole region to plan the move: the type checker is the auditor. A move that
needs more than one context field is a signal the group is not one module —
split it instead of inventing a wide context.
