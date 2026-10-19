# Pipeline

On 2026-10-20 the user asked for `implement-spec`, then `code-review`, then `pr`.

- `implement-spec` runs against the integration branch `better-sub-agents`. Each implementer gets its own worktree under `/home/prm/project/cmd-mod-better-sub-agents-wt/`, reads `skill://tdd`, implements one ticket, runs `node test/smoke.mjs`, commits, and merges the integration tip into its branch. A merger then merges the ticket branch into the integration branch.
- `code-review` runs two parallel reviews over `main...HEAD`: Standards and Spec. The findings are aggregated, then fixed in one implementer subagent.
- `pr` writes the body with the `/pr` template and marks PR #2 ready for review.
- The first worktree is `/home/prm/project/cmd-mod-better-sub-agents-wt/t1` on `t1-definitions`, based on `e1ed3b8`.
