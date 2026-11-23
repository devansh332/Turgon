-- Q: Which vendors and crews are fast, cheap and assigned to the right work? (procurement, vendor reviews)
-- Grain: assignee x category. scope_mismatch_rate only applies to specialists with a declared scope.
CREATE MATERIALIZED VIEW gold.vendor_performance AS
SELECT
    assignee,
    max(assignee_type)                                                AS assignee_type,
    category,
    count(*)                                                          AS tickets,
    count(*) FILTER (WHERE resolution_hours IS NOT NULL)              AS resolved_with_duration,
    round((percentile_cont(0.5) WITHIN GROUP (ORDER BY resolution_hours))::numeric, 1) AS p50_resolution_hours,
    round(avg(sla_breached::int)::numeric, 4)                         AS breach_rate,
    count(cost)                                                       AS tickets_with_cost,
    coalesce(sum(cost), 0)                                            AS total_cost,
    round(avg(cost), 2)                                               AS avg_cost,
    round(avg((NOT assignee_scope_match)::int)::numeric, 4)           AS scope_mismatch_rate
FROM gold.ticket_facts
GROUP BY assignee, category;

CREATE UNIQUE INDEX ON gold.vendor_performance (assignee, category);
