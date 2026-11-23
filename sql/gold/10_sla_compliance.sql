-- Q: Are we meeting SLAs, and where are we missing them? (ops leadership, monthly review)
-- Grain: month x category x priority. Building is left to backlog_daily: at this volume a
-- four-way grain leaves most cells with one or two tickets and the percentiles become noise.
CREATE MATERIALIZED VIEW gold.sla_compliance AS
SELECT
    date_trunc('month', created_at)::date                             AS month,
    category,
    priority,
    count(*)                                                          AS tickets,
    count(*) FILTER (WHERE sla_breached IS NOT NULL)                  AS sla_measurable,
    count(*) FILTER (WHERE sla_breached)                              AS sla_breached,
    count(*) FILTER (WHERE sla_breached AND timing_assumed)           AS breached_on_assumed_timing,
    round(avg(sla_breached::int)::numeric, 4)                         AS breach_rate,
    count(*) FILTER (WHERE is_open)                                   AS still_open,
    count(*) FILTER (WHERE is_safety_issue)                           AS safety_tickets,
    round((percentile_cont(0.5) WITHIN GROUP (ORDER BY resolution_hours))::numeric, 1) AS p50_resolution_hours,
    round((percentile_cont(0.9) WITHIN GROUP (ORDER BY resolution_hours))::numeric, 1) AS p90_resolution_hours,
    count(*) FILTER (WHERE sla_source = 'policy')                     AS sla_from_policy
FROM gold.ticket_facts
GROUP BY 1, 2, 3;

CREATE UNIQUE INDEX ON gold.sla_compliance (month, category, priority);
