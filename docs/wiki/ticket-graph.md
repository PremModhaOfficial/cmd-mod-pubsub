# Ticket graph

As of 2026-10-20, from the issue relations on `PremModhaOfficial/cmd-mod-pubsub`.

- T0 #3 is closed.
- T1 #4 is blocked by #3. It blocks #5, #6, #7, #8, #9, #10, #11, #12, and #14.
- T2 #5 is blocked by #4. It blocks #17.
- T3 #6 is blocked by #4. It blocks #17.
- T4 #7 is blocked by #4. It blocks #17.
- T5 #8 is blocked by #4. It blocks #17.
- T6 #9 is blocked by #4 and #3. It blocks #17.
- T7 #10 is blocked by #4. It blocks #17.
- T8 #11 is blocked by #4. It blocks #17.
- T9 #12 is blocked by #4 and #3. It blocks #17 and #13.
- T10 #13 is blocked by #12. It blocks #17 and #16.
- T11 #14 is blocked by #4. It blocks #17 and #15.
- T12 #15 is blocked by #14 and #4. It blocks #17 and #16.
- T13 #16 is blocked by #15, #13, and #4. It blocks #17.
- T14 #17 is blocked by #5 through #16.

Because #3 is closed, the effective frontier on 2026-10-20 is #4 only. After #4 merges, the frontier is #5, #6, #7, #8, #9, #10, #11, #12, and #14.
