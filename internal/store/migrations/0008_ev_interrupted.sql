-- 0008_ev_interrupted: the §4.6 unsurfaced-park sweep rides this.
-- Interrupted rows are deliberately OUTSIDE the queue's rebuild scan
-- (never auto-rerun), so a park whose notice write failed has no other
-- path back to visibility — the outbox pump sweeps interrupted events
-- missing their notice row and composes it. Partial, same convention
-- as ev_queue (0005): live history never bloats it.
CREATE INDEX ev_interrupted ON events(id)
    WHERE status = 'interrupted';
