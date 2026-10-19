# Decisions

## Exit condition

On 2026-10-20 the run stops when all 14 tickets #4-#17 are merged into `better-sub-agents`, `node test/smoke.mjs` is green on the integration tip, the two-axis code review findings are fixed, and PR #2 carries the `/pr` body and is marked ready.

## Trail

| Date | Decision | Evidence |
| --- | --- | --- |
| 2026-10-20 | Continue the implement-spec pipeline autonomously after the user went AFK. | The user wrote "i am goung afk best of luck". |
| 2026-10-20 | Merge one ticket branch at a time through a merger subagent that runs smoke. | T1 `83431ae`, T7 `a79dc4d`, T6 `ccf55f2`, T2 `cc0d1d2`, T11 `c4ba344`, T3 `40c7932`, T4 `d6c69cb`, T5 `264dec6`, T8 `7bfcda4`, T9 `15d1579`; each merger reported PASS. |
| 2026-10-20 | Append each ticket's new smoke checks after the current tip and renumber only that ticket's comments. | T2 42, T11 43-46, T3 47-48, T4 49-52, T5 53-59, T8 60-64, T9 65-69. |
| 2026-10-20 | Run T12 and T10 in parallel; T13 waits for both. | Ticket graph: #16 blocked by #15 and #13. |
| 2026-10-20 | T12 and T10 landed. | T12 `b1662aa` (checks 70-73); T10 `fcf787e` (checks 74-80); smoke 80 PASS. |
| 2026-10-20 | T14 closes the release ledger without the human gates: all parity rows get a check, deviations stay as rows, and the repository rename/tag wait for the human. | `SPEC.md` ledger (33 rows), ADR 0001 deviations, smoke 88/88, e2e 10/10. |
