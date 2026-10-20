# Coding standards

Review rules for this repo. The map in `docs/split-map.md` has the specifics.

1. Tests assert behavior. Never assert the source text, line numbers, or ordering
   of the generated `better-sub-agents.ts`; assert observable results through the
   public surface. (Reading `WORKER_SOURCE` from the bundle is a legacy harness
   seam, not a precedent for new tests.)
2. Modules import downward only: `index.ts` → leaves, leaves → their own
   dependencies. Never import `./index.ts` from a module. The graph is a DAG.
3. A leaf takes OS effects as arguments (`fs`, `run`, `now`, `env`, `cwd`) or a
   deps object; only the entry composes the real runtime.
4. One module per commit, with the regenerated root artifact in the same commit.
   `npm run gate` must pass; a stale bundle is a review failure.
5. Edit `src/`, never the generated root file. Review the `src/` diff, not the
   artifact diff.
