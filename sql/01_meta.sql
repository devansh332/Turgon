-- Control plane: run log, lineage, metrics, agent bookkeeping. Never read by business users.
CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS meta.pipeline_runs (
    run_id          uuid PRIMARY KEY,
    stage           text NOT NULL,
    status          text NOT NULL,            -- running | succeeded | failed | skipped
    started_at      timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz,
    git_sha         text,
    rules_version   text,                     -- hash of seeds/ (approved maps and rules)
    prompt_versions jsonb,
    rows_in         bigint,
    rows_out        bigint,
    rows_rejected   bigint,
    details         jsonb NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS meta.ingested_files (
    file_sha256  text PRIMARY KEY,
    source_file  text NOT NULL,
    batch_id     uuid NOT NULL,
    row_count    bigint NOT NULL,
    header       text[] NOT NULL,
    run_id       uuid NOT NULL,
    ingested_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS meta.watermarks (
    stage          text PRIMARY KEY,
    high_watermark timestamptz NOT NULL,
    rules_version  text NOT NULL,
    updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS meta.schema_registry (
    table_name    text NOT NULL,
    columns       text[] NOT NULL,
    fingerprint   text NOT NULL,
    registered_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (table_name, fingerprint)
);

CREATE TABLE IF NOT EXISTS meta.column_tags (
    table_name  text NOT NULL,
    column_name text NOT NULL,
    pii         boolean NOT NULL,
    pii_type    text,
    sensitivity text NOT NULL,                -- public | internal | confidential
    domain      text NOT NULL,
    source      text NOT NULL,                -- rule | llm (approved) | seed
    tagged_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (table_name, column_name)
);

CREATE TABLE IF NOT EXISTS meta.dq_metrics (
    run_id      uuid NOT NULL,
    layer       text NOT NULL,
    column_name text NOT NULL,
    null_rate   numeric NOT NULL,
    distinct_count bigint NOT NULL,
    measured_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, layer, column_name)
);

CREATE TABLE IF NOT EXISTS meta.dq_flag_counts (
    run_id   uuid NOT NULL,
    flag     text NOT NULL,                   -- silver dq_flags value, e.g. resolved_at:before_created
    tickets  bigint NOT NULL,
    rate     numeric NOT NULL,                -- share of silver tickets carrying the flag
    measured_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, flag)
);

CREATE TABLE IF NOT EXISTS meta.reconciliation (
    run_id     uuid NOT NULL,
    check_name text NOT NULL,
    expected   numeric,
    actual     numeric,
    passed     boolean NOT NULL,
    checked_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, check_name)
);

CREATE TABLE IF NOT EXISTS meta.alerts (
    alert_id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id     uuid,
    severity   text NOT NULL,                 -- warning | error
    check_name text NOT NULL,
    message    text NOT NULL,
    value      numeric,
    threshold  numeric,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS meta.llm_cache (
    cache_key      text PRIMARY KEY,
    agent          text NOT NULL,
    prompt_name    text NOT NULL,
    prompt_version text NOT NULL,
    model          text NOT NULL,
    response       jsonb NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now()
);

-- Per-item results of batched classification calls, so a new value in the vocabulary costs one small call
-- instead of shifting every batch and missing the batch-level cache above.
CREATE TABLE IF NOT EXISTS meta.llm_item_cache (
    prompt_name    text NOT NULL,
    prompt_version text NOT NULL,
    model          text NOT NULL,
    item_key       text NOT NULL,
    response       jsonb NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (prompt_name, prompt_version, model, item_key)
);

CREATE TABLE IF NOT EXISTS meta.llm_calls (
    call_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    agent          text NOT NULL,
    prompt_name    text NOT NULL,
    prompt_version text NOT NULL,
    model          text NOT NULL,
    cache_hit      boolean NOT NULL,
    ok             boolean NOT NULL,
    attempts       int NOT NULL,
    input_tokens   int NOT NULL DEFAULT 0,
    output_tokens  int NOT NULL DEFAULT 0,
    cost_usd       numeric(12, 6) NOT NULL DEFAULT 0,
    latency_ms     int NOT NULL DEFAULT 0,
    error          text,
    created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS meta.agent_proposals (
    proposal_id  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    agent        text NOT NULL,
    kind         text NOT NULL,               -- category_label | description_template | person_merge | dq_rule | column_tag | schema_migration
    item_key     text NOT NULL,
    payload      jsonb NOT NULL,
    payload_hash text NOT NULL,
    rationale    text,
    confidence   numeric,
    risk         text NOT NULL,               -- low | high
    status       text NOT NULL,               -- pending | auto_approved | approved | rejected
    prompt_version text,
    model        text,
    created_at   timestamptz NOT NULL DEFAULT now(),
    decided_at   timestamptz,
    decided_by   text,
    UNIQUE (kind, item_key, payload_hash)
);
