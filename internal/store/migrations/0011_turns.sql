-- 0011_turns: C11 observability (§3, §6) — one row per engine turn,
-- populated from the CLI's stream-json result events. This table feeds
-- the §7 cost alarm (the heartbeat's daily-spend query), the §4.3
-- token-bloat check, and P4 tuning data. The spec sketch (§6) carries
-- no CHECKs or FKs; the repo convention (closed enums, provenance must
-- resolve, fail loud) adds them, matching 0002–0010.
CREATE TABLE turns (
    id            INTEGER PRIMARY KEY,
    session_id    TEXT NOT NULL REFERENCES sessions(session_id),
                                       -- the turn's session; a turn from a session the daemon
                                       --   never created is a bug, not observability data
    ts            INTEGER NOT NULL,    -- unix seconds when the turn ended — the daily-spend
                                       --   query's scan axis (§7)
    kind          TEXT                 -- the event kind that drove the turn (same closed enum
                                       --   as events); NULL for turns outside the queue
        CHECK (kind IS NULL OR kind IN ('message', 'heartbeat', 'webhook', 'cron', 'approval', 'task')),
    model         TEXT,                -- the served model from the stream's init event; NULL
                                       --   when the child died before reporting one
    input_tokens  INTEGER,             -- usage from the result event; NULL = the result event
    output_tokens INTEGER,             --   never arrived (timeout/kill) — unknown usage must
    cost_usd      REAL,                --   never read as a free turn in the spend query (§7)
    tool_calls    INTEGER NOT NULL,    -- tool_use blocks observed on the stream — known even
                                       --   for a killed turn (counted as they streamed)
    duration_ms   INTEGER NOT NULL,    -- result event's duration when it arrived, else the
                                       --   engine's own wall clock
    outcome       TEXT NOT NULL
        CHECK (outcome IN ('ok', 'error', 'denied', 'timeout'))
                                       -- ok | error | denied | timeout (§6); denied lands with
                                       --   the C9 policy gate — the schema already holds it
);

-- The §7 daily-spend query scans a time window; without this index it
-- would walk every turn ever recorded, forever growing.
CREATE INDEX turns_by_ts ON turns(ts);
