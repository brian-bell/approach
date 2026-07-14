-- 0009_park_generation: each park of an event is a distinct EPISODE
-- and its §4.6 notice must never be suppressed by an earlier one — a
-- failed retry that parks again silently would strand the event with
-- an owner who thinks it is running. Timestamps cannot key episodes
-- (two parks can share a second); a per-event monotonic counter,
-- incremented by every park, can. The notice key is
-- interrupted:<dedup_key>:<parks>.
ALTER TABLE events ADD COLUMN parks INTEGER NOT NULL DEFAULT 0;
