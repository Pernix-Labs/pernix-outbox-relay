-- V001: create the outbox table.
-- Applied by the domain's migration tool. The relay never runs DDL.
-- Later versions only add columns.
-- Requires PostgreSQL 13+ (xid8, pg_current_xact_id).
-- Index and constraint names are unique per schema. A domain that copies this script for a second outbox table
-- in one schema renames every outbox_* index and constraint name together with the table.

CREATE TABLE outbox (
    id                    bigint       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tx_id                 xid8         NOT NULL DEFAULT pg_current_xact_id(),   -- producing transaction
    message_key           bytea,
    payload               bytea        NOT NULL,
    headers               jsonb        NOT NULL DEFAULT '{}'::jsonb,
    route                 text,                                                 -- producer-written; NULL = no route
    created_at            timestamptz  NOT NULL DEFAULT now(),
    retry_count           integer      NOT NULL DEFAULT 0,                      -- relay-owned
    next_attempt_at       timestamptz  NOT NULL DEFAULT now(),                  -- relay-owned; 'infinity' = blocked
    last_error            text,                                                 -- relay-owned
    CONSTRAINT outbox_headers_is_object         CHECK (jsonb_typeof(headers) = 'object'),
    CONSTRAINT outbox_header_values_are_strings CHECK (NOT jsonb_path_exists(headers, '$.* ? (@.type() != "string")')),
    CONSTRAINT outbox_retry_count_non_negative  CHECK (retry_count >= 0),
    CONSTRAINT outbox_route_format              CHECK (route ~ '^[a-z0-9][a-z0-9._-]{0,62}$')
);
CREATE INDEX outbox_tx_id_id_idx ON outbox (tx_id, id);
CREATE INDEX outbox_failed_message_key_idx ON outbox (message_key, next_attempt_at) WHERE retry_count > 0;
ALTER TABLE outbox SET (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 1000);
