-- 0001_baseline: reserves schema version 1. The runner (§6) needs at
-- least one embedded file to compile its go:embed pattern; real tables
-- arrive in later numbered migrations.
--
-- Rules for migration authors:
--   * names are NNNN_description.sql, numbered contiguously from 0001
--   * the whole pending batch applies in ONE transaction — do not
--     toggle pragmas here (foreign_keys cannot change mid-transaction)
--   * multi-statement files are fine
SELECT 1;
