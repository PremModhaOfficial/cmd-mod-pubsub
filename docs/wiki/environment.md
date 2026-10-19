# Environment

As of `PremModhaOfficial/cmd-mod-pubsub@e1ed3b8` on 2026-10-20:

- `better-sub-agents.ts` is 1,957 lines. It has no runtime dependencies. `node test/smoke.mjs` is the CI suite; `node test/e2e.mjs` needs credentials and network.
- The main worktree has untracked `.agents/`, `.claude/`, `.commandcode/`, and `skills-lock.json`. They are not part of the mod and must not be committed.
- GitHub labels present: `ready-for-agent`, `bug`, `enhancement`, `wontfix`. State labels missing: `needs-triage`, `needs-info`, `ready-for-human`. The sub-issues carry no category label. Run `/setup-matt-pocock-skills` to install the state mapping.
