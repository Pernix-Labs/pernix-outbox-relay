-- V002: drop the destination-only columns V001 added for the removed Database destination
--       (ADR 0010).
-- One-time exception to "later versions only add columns": this script is destructive.
-- Data in source_outbox, source_id, source_destination, dead_lettered_at, dead_letter_reason,
-- dead_letter_attempts is lost. The outbox_source_complete constraint and outbox_source_uq index go with the columns.
-- ALTER TABLE takes an ACCESS EXCLUSIVE lock on the table. Metadata-only (no rewrite), but it waits for every
-- open transaction on the table and blocks inserts while it waits. Set lock_timeout in the migration session.
-- The relay never reads or writes these columns and accepts the table before and after this script.
-- Exception: a table that is a Database destination target takes this script only after every relay delivering into it
-- runs this build or has dropped that destination. An older relay inserts into these columns and fails with 42703.

ALTER TABLE outbox
    DROP COLUMN IF EXISTS source_outbox,
    DROP COLUMN IF EXISTS source_id,
    DROP COLUMN IF EXISTS source_destination,
    DROP COLUMN IF EXISTS dead_lettered_at,
    DROP COLUMN IF EXISTS dead_letter_reason,
    DROP COLUMN IF EXISTS dead_letter_attempts;
