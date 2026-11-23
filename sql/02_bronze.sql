-- Bronze: the file exactly as received. Every source column is TEXT (schema-on-read),
-- unknown columns from drifted files land in _extra, and nothing is ever updated or deleted.
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.tickets_raw (
    _bronze_id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ticket_id           text,
    created_at          text,
    resolved_at         text,
    category            text,
    priority            text,
    status              text,
    building            text,
    description         text,
    submitted_by        text,
    assigned_to         text,
    resolution_notes    text,
    cost                text,
    sla_hours           text,
    _extra              jsonb,
    _source_file        text NOT NULL,
    _source_file_sha256 text NOT NULL,
    _row_number         int NOT NULL,
    _row_hash           text NOT NULL,
    _batch_id           uuid NOT NULL,
    _ingested_at        timestamptz NOT NULL,
    UNIQUE (_source_file_sha256, _row_number)
);

-- Not unique on purpose: identical lines are kept (no data loss); the hash tracks them.
CREATE INDEX IF NOT EXISTS tickets_raw_row_hash_idx ON bronze.tickets_raw (_row_hash);
CREATE INDEX IF NOT EXISTS tickets_raw_ingested_at_idx ON bronze.tickets_raw (_ingested_at);
-- normalised ticket number, as silver keys it (TKT-0123 = tkt-123): used by incremental silver
DROP INDEX IF EXISTS bronze.tickets_raw_ticket_id_idx;
CREATE INDEX IF NOT EXISTS tickets_raw_ticket_number_idx
    ON bronze.tickets_raw ((substring(upper(btrim(ticket_id)) FROM '^TKT-(\d+)$')::numeric));
