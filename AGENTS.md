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
