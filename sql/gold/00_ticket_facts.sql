-- Gold is fully derived from silver, so it is rebuilt in one transaction each run: readers never see a
-- half-built gold (they wait on the schema lock for the few seconds of the rebuild, then see the new
-- version), and a rerun produces identical output.
DROP SCHEMA IF EXISTS gold CASCADE;
CREATE SCHEMA gold;

-- One row per real ticket (content duplicates removed) with SLA outcome evaluated at a fixed as_of
-- (the latest event in silver, created or resolved), never now(), so results are reproducible.
CREATE VIEW gold.ticket_facts AS
WITH as_of AS (
    SELECT greatest(max(created_at), max(resolved_at)) AS as_of
    FROM silver.tickets WHERE duplicate_of_key IS NULL
)
SELECT
    t.ticket_key,
    t.created_at,
    t.resolved_at,
    t.resolution_hours,
    t.category,
    coalesce(t.priority_effective, 'unknown') AS priority,
    coalesce(t.building, 'unknown')           AS building,
    coalesce(t.assignee, 'Unassigned')        AS assignee,
    t.assignee_type,
    t.assignee_scope_match,
    t.status,
    t.is_open,
    t.cost,
    t.sla_hours_effective,
    t.sla_source,
    t.is_safety_issue,
    -- duration rests on a date-only value (midnight assumed) or a month/day-ambiguous date
    t.dq_flags && ARRAY['created_at:date_only', 'resolved_at:date_only',
                        'created_at:ambiguous_mdy', 'resolved_at:ambiguous_mdy'] AS timing_assumed,
    a.as_of,
    CASE
        WHEN t.sla_hours_effective IS NULL THEN NULL
        WHEN t.resolution_hours IS NOT NULL THEN t.resolution_hours > t.sla_hours_effective
        WHEN t.is_open THEN extract(epoch FROM a.as_of - t.created_at) / 3600 > t.sla_hours_effective
    END AS sla_breached
FROM silver.tickets t
CROSS JOIN as_of a
WHERE t.duplicate_of_key IS NULL;
