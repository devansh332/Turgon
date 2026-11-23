-- Silver: one row per ticket, typed, normalized, enriched, with DQ flags and lineage.
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS quarantine;

CREATE TABLE IF NOT EXISTS silver.tickets (
    ticket_key            text PRIMARY KEY,         -- TKT-1234, or UNK-<row hash> when the id is unusable
    ticket_number         int,
    ticket_id_raw         text,

    created_at            timestamp NOT NULL,       -- naive timestamps; source carries no time zone
    resolved_at           timestamp,
    resolution_hours      numeric(10, 2),           -- only when status is closed and dates are consistent

    category_raw          text,
    category              text NOT NULL,            -- controlled vocabulary (seeds/taxonomy.yaml) or 'Unclassified'
    category_source       text NOT NULL,            -- label | description | unmapped
    asset_type            text,
    issue_type            text,
    is_safety_issue       boolean,
    urgency_inferred      text,                     -- from description: critical | high | medium | low
    enrichment_source     text NOT NULL,            -- seed template | unmapped
    enrichment_confidence numeric(4, 3),
    enrichment_prompt_version text,

    priority_raw          text,
    priority              text,                     -- critical | high | medium | low | NULL
    priority_effective    text,                     -- priority, else urgency_inferred
    priority_source       text NOT NULL,            -- given | inferred | unknown

    status_raw            text,
    status                text,                     -- Open | In Progress | Escalated | Pending Vendor | Resolved | Closed
    is_open               boolean,

    building              text,
    location_floor        int,
    location_room         text,
    asset_id              text,
    description           text,
    description_template  text,

    submitted_by_raw      text,
    submitted_by          text,                     -- canonical person, NULL for system/test accounts
    submitter_resolution  text NOT NULL,            -- exact | initials | ambiguous | system | missing
    assignee_raw          text,
    assignee              text,
    assignee_type         text,                     -- vendor | in_house | NULL
    assignee_scope_match  boolean,                  -- NULL when the assignee is a generalist

    resolution_notes      text,
    resolution_outcome    text,
    root_cause            text,
    replaced_part         text,
    claimed_duplicate_of  text,                     -- from notes; recorded, never trusted for dedup

    cost_raw              text,
    cost                  numeric(12, 2),
    sla_hours_raw         text,
    sla_hours             int,
    sla_hours_effective   int,
    sla_source            text NOT NULL,            -- given | policy | unknown

    fields_swapped        boolean NOT NULL DEFAULT false,
    content_hash          text NOT NULL,
    duplicate_of_key      text,                     -- set when another key carries identical content
    dq_flags              text[] NOT NULL DEFAULT '{}',

    _bronze_id            bigint NOT NULL,
    _bronze_row_hash      text NOT NULL,
    _source_file          text NOT NULL,
    _source_versions      int NOT NULL,             -- non-quarantined bronze rows folded into this ticket
    _silver_run_id        uuid NOT NULL,
    _rules_version        text NOT NULL,
    _updated_at           timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS tickets_content_hash_idx ON silver.tickets (content_hash);
CREATE INDEX IF NOT EXISTS tickets_created_at_idx ON silver.tickets (created_at);
-- with created_at, lets gold.ticket_facts find as_of (max of both) by index instead of scanning silver
CREATE INDEX IF NOT EXISTS tickets_resolved_at_idx ON silver.tickets (resolved_at);

CREATE TABLE IF NOT EXISTS quarantine.tickets_rejected (
    _bronze_id     bigint PRIMARY KEY,
    ticket_key     text,
    reasons        text[] NOT NULL,
    raw            jsonb NOT NULL,
    _silver_run_id uuid NOT NULL,
    _rules_version text NOT NULL,
    rejected_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS tickets_rejected_ticket_key_idx ON quarantine.tickets_rejected (ticket_key);
