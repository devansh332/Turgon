-- Q: Is the open backlog growing, and in which buildings? (site managers, staffing)
-- Grain: day x building. Only tickets with a known lifecycle are counted: still open, or closed with a
-- valid resolved_at. Closed tickets without a usable close date cannot be placed on a day and are excluded.
CREATE MATERIALIZED VIEW gold.backlog_daily AS
WITH scoped AS (
    SELECT building, created_at::date AS opened_on,
           CASE WHEN is_open THEN NULL ELSE resolved_at::date END AS closed_on,
           (priority = 'critical')::int AS critical
    FROM gold.ticket_facts
    WHERE is_open OR resolution_hours IS NOT NULL
),
bounds AS (SELECT min(opened_on) AS d0, greatest(max(opened_on), max(closed_on)) AS d1 FROM scoped),
grid AS (
    SELECT d::date AS day, b.building
    FROM bounds, generate_series(bounds.d0, bounds.d1, interval '1 day') AS d,
         (SELECT DISTINCT building FROM scoped) AS b
),
opened AS (SELECT opened_on AS day, building, count(*) AS n, sum(critical) AS crit FROM scoped GROUP BY 1, 2),
closed AS (
    SELECT closed_on AS day, building, count(*) AS n, sum(critical) AS crit
    FROM scoped WHERE closed_on IS NOT NULL GROUP BY 1, 2
)
SELECT
    g.day,
    g.building,
    coalesce(o.n, 0) AS opened,
    coalesce(c.n, 0) AS closed,
    sum(coalesce(o.n, 0) - coalesce(c.n, 0)) OVER w AS open_backlog,
    sum(coalesce(o.crit, 0) - coalesce(c.crit, 0)) OVER w AS critical_open
FROM grid g
LEFT JOIN opened o USING (day, building)
LEFT JOIN closed c USING (day, building)
WINDOW w AS (PARTITION BY g.building ORDER BY g.day);

CREATE UNIQUE INDEX ON gold.backlog_daily (building, day);
