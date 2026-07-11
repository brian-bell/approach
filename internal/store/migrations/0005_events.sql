-- 0005_events: THE durable queue (§4.1, §6) — every inbound event is
-- written here on receipt, BEFORE any processing. Gateway channels
-- don't redeliver, so receipt is the last moment durability is free;
-- everything downstream is recoverable from this table. The spec
-- sketch (§6) carries no CHECKs; the repo convention (enums are
-- closed, ambiguity fails loud) adds them, matching 0002/0003.
CREATE TABLE events (
    id         INTEGER PRIMARY KEY,
    dedup_key  TEXT NOT NULL UNIQUE, -- event identity, per-kind contract (§6):
                                     --   message → (channel, native message id)
                                     --   webhook → provider delivery id
                                     --   heartbeat/cron → (schedule_id, occurrence_time)
                                     -- Duplicate delivery must collapse to one row
                                     -- (dup insert = no-op — the §4.1 "duplicate
                                     -- channel delivery → one turn" drill).
    thread_key TEXT NOT NULL,        -- per-channel contract (§6); the queue claim key (§4.1)
    kind       TEXT NOT NULL
        CHECK (kind IN ('message', 'heartbeat', 'webhook', 'cron', 'approval', 'task')),
    trust      TEXT NOT NULL         -- stamped at ingest (§6): participant levels from the
                                     --   identities lookup, plus the daemon-only system
                                     --   levels (§4.2 heartbeats, §4.5 workers). Stored on
                                     --   the event so the queue replays with the trust the
                                     --   adapter stamped, never re-derived later.
        CHECK (trust IN ('owner', 'known', 'untrusted', 'system', 'system-worker')),
    payload    TEXT NOT NULL,        -- full normalized event JSON (§6 contract)
    status     TEXT NOT NULL DEFAULT 'received'
        CHECK (status IN ('received', 'processing', 'completed', 'replied',
                          'interrupted',  -- parked, human decides (§4.6)
                          'skipped',      -- coalesced misfire (§4.2)
                          'dead')),
    attempts   INTEGER NOT NULL DEFAULT 0, -- side-effect-aware retry budget (§4.6): max 2,
                                           --   only if zero side-effecting calls this turn
    received   INTEGER NOT NULL,
    updated    INTEGER,
    correlation TEXT                 -- links retries / approval round-trips to the origin event
);

-- The per-thread queue scan (§4.1): unprocessed rows in arrival order,
-- claimed by thread_key. Partial — completed history never bloats it.
CREATE INDEX ev_queue ON events(thread_key, id)
    WHERE status IN ('received', 'processing');
