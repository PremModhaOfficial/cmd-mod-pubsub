# Transport decision

On 2026-10-20, ADR 0001 accepted ACP for non-fork jobs. Fork follow-ups use one-shot `cmd --resume -p`. ACP `session/load` replays the fork history, but a follow-up prompt did not complete in 180 s or 300 s probes.

- Source: `docs/adr/0001-worker-and-control.md`.
- Correction commit: `PremModhaOfficial/cmd-mod-pubsub@2a8d1f8`.
- Checklist alignment: `PremModhaOfficial/cmd-mod-pubsub@e1ed3b8`.
