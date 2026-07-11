-- 0004_tainted: the sticky taint flag (§6, §7 rule 3). Set the moment a
-- session ingests externally-authored content — a non-owner prompt or
-- attachment, an untrusted MCP result, a web fetch, a Codex web read —
-- and read by mutating verbs as their one-lattice-step shift. Held to
-- 0|1 so "read one column right" can never see a third state. Clearing
-- belongs to rotation only (C4, M1); no in-place clear exists.
ALTER TABLE sessions ADD COLUMN tainted INTEGER NOT NULL DEFAULT 0
    CHECK (tainted IN (0, 1));

-- Historical taint cannot be reconstructed: a session that predates
-- this column may already hold non-owner prompts, MCP results, or web
-- content its flag never recorded, so pre-existing rows are marked
-- tainted rather than assumed clean — fail safe. Sessions created from
-- here on are born clean via the DEFAULT.
UPDATE sessions SET tainted = 1;
