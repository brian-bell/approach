-- 0010_dead_letters: the §4.6 terminal landing. "Dead" means the
-- machine has given up and a HUMAN decides — entry is surfaced twice
-- (one owner-DM notification through the outbox, plus the heartbeat's
-- unacked re-surface), and draining is deliberately manual: one
-- requeue/discard decision per row. The spec sketch (§6) carries no
-- CHECKs; the repo convention (closed enums, fail loud) adds them.
CREATE TABLE dead_letters (
    event_id   INTEGER PRIMARY KEY REFERENCES events(id),
    reason     TEXT NOT NULL
        CHECK (reason IN ('retries-exhausted', 'malformed', 'unroutable', 'surface-failed')),
    entered    INTEGER NOT NULL,
    acked      INTEGER,                -- owner saw it (heartbeat re-surfaces unacked rows)
    resolution TEXT                    -- requeued | discarded — manual, per row
        CHECK (resolution IS NULL OR resolution IN ('requeued', 'discarded')),
    entries    INTEGER NOT NULL DEFAULT 1
                                       -- death-generation counter: a requeued event can die
                                       --   AGAIN — the row re-enters (fresh reason/entered,
                                       --   acked/resolution reset, entries+1) and the entry
                                       --   notice keys on it ("dead:<dedup_key>:<entries>"),
                                       --   so a later death is never silenced by an earlier
                                       --   notice. A counter, not a timestamp: seconds
                                       --   collide (same rule as events.parks, 0009).
);
