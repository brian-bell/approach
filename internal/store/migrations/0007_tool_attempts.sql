-- 0007_tool_attempts: the per-side-effecting-call journal (§4.1, §4.6,
-- §6). The PreToolUse gate writes a row in 'started' BEFORE the call
-- runs and PostToolUse flips it to done/failed after — so recovery
-- reasons from what provably STARTED, never from aggregate counts a
-- crash can lose. A 'started' row with no completion on restart is
-- AMBIGUOUS: never auto-retried unless its idempotency_key makes a
-- repeat provably safe (§4.6). The spec sketch (§6) carries no CHECKs
-- or FKs; the repo convention (closed enums, provenance must resolve,
-- fail loud) adds them, matching 0002–0006.
CREATE TABLE tool_attempts (
    id           INTEGER PRIMARY KEY,
    session_id   TEXT NOT NULL REFERENCES sessions(session_id),
                                       -- the turn's session; an attempt from a session the
                                       --   daemon never created is a bug, not a row
    event_id     INTEGER REFERENCES events(id),
                                       -- the queued event whose turn made the call; NULL for
                                       --   turns outside the queue. The §4.6 retry question
                                       --   ("any side-effecting attempt this turn?") keys on it.
    tool         TEXT NOT NULL,        -- canonical tool name
    args_digest  TEXT NOT NULL,        -- digest of normalized args (display strings are not
                                       --   identity — same rule as §4.4 approvals; the shared
                                       --   digest function lands with C9)
    idempotency_key TEXT,              -- present only when the verb supports it — the ONLY safe
                                       --   basis for auto-retrying an ambiguous attempt (§4.6);
                                       --   NULL is load-bearing: no key, no retry
    state        TEXT NOT NULL DEFAULT 'started'
        CHECK (state IN ('started', 'done', 'failed')),
                                       -- started (at PreToolUse) → done | failed (at PostToolUse)
    started_at   INTEGER NOT NULL,
    ended_at     INTEGER
);

-- The §4.6 retry question is asked per turn: all attempts for one
-- event, in id (call) order.
CREATE INDEX ta_by_event ON tool_attempts(event_id);
