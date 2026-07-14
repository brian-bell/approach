-- 0006_deliveries: the generalized outbound outbox (§4.1, §6). EVERY
-- outbound message routes through here — turn replies AND proactive
-- scheduler/heartbeat notifies (§4.2) — written BEFORE the first send
-- attempt, so a crash after compose-but-before-ack re-sends from the
-- persisted payload (at-least-once: a rare duplicated chat message is
-- accepted; a silently eaten reply is not). The spec sketch (§6)
-- carries no CHECKs; the repo convention (enums are closed, ambiguity
-- fails loud) adds them, matching 0002/0003/0005.
CREATE TABLE deliveries (
    id           INTEGER PRIMARY KEY,
    delivery_key TEXT NOT NULL UNIQUE, -- delivery identity: a crash-retried compose collapses
                                       --   to one row (dup insert = no-op), so at-least-once
                                       --   duplication comes only from re-SENDING, never from
                                       --   re-composing two divergent payloads
    event_id     INTEGER REFERENCES events(id),
                                       -- originating turn; NULL for pure scheduler notifies
                                       --   (§4.2). foreign_keys=ON (store posture), so a bound
                                       --   id must exist.
    target       TEXT NOT NULL,        -- channel thread key to send to (§6 per-channel contract)
    payload      TEXT NOT NULL,        -- the rendered text — persisted BEFORE the send attempt
    status       TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'sent', 'failed')),
                                       -- pending → sent (on platform ack)
                                       --   | failed (terminal: retry budget exhausted — §4.6)
    attempts     INTEGER NOT NULL DEFAULT 0,
    last_attempt INTEGER,
    acked        INTEGER               -- set when the adapter confirms the platform accepted it;
                                       --   unacked non-failed rows re-send on restart (§4.6)
);

-- The §4.6 restart resend scan: unacked, non-failed rows still owed a
-- send. Partial — delivered and abandoned history never bloats it —
-- and the single schema definition of "owed a send", same convention
-- as ev_queue (0005).
CREATE INDEX deliveries_resend ON deliveries(id)
    WHERE acked IS NULL AND status <> 'failed';
