-- 0002_identities: the hand-enrolled identity table (§6) — the root of
-- every §7 trust decision. (channel, native_id) maps to a trust level;
-- the ABSENCE of a row is what "untrusted" means, so only owner and
-- known are ever stored (deny-by-default). Seeded from approach.toml at
-- daemon startup; the config file is the source of truth and the seed
-- is a full sync, so revoking someone in the file revokes them here.
CREATE TABLE identities (
    channel   TEXT NOT NULL,           -- discord | slack | sms | …
    native_id TEXT NOT NULL,           -- discord user_id, slack user, e164 …
    trust     TEXT NOT NULL
        CHECK (trust IN ('owner', 'known')),
    owner_id  TEXT                     -- canonical principal, identical across ALL of the
                                       --   owner's surfaces — cross-surface approval (§4.4)
                                       --   matches on it, so exactly owner rows carry it.
                                       --   The two valid states are spelled out: owner rows
                                       --   need a real (non-empty) principal — '' is
                                       --   non-NULL, and empty principals would all match
                                       --   each other in approval — and known rows carry
                                       --   nothing at all.
        CHECK ((trust = 'owner' AND owner_id IS NOT NULL AND owner_id <> '')
            OR (trust = 'known' AND owner_id IS NULL)),
    label     TEXT,
    PRIMARY KEY (channel, native_id)
);
