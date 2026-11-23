import uuid

import psycopg

from medallion.config import MAX_NULL_RATE_DELTA, MAX_QUARANTINE_RATE
from medallion.log import event


def alert(
    conn: psycopg.Connection,
    run_id: uuid.UUID | None,
    severity: str,
    check: str,
    message: str,
    value: float | None = None,
    threshold: float | None = None,
) -> None:
    conn.execute(
        """INSERT INTO meta.alerts (run_id, severity, check_name, message, value, threshold)
           VALUES (%s, %s, %s, %s, %s, %s)""",
        (run_id, severity, check, message, value, threshold),
    )
    event("alert", severity=severity, check=check, message=message, value=value, threshold=threshold)


SILVER_PROFILED = [
    "created_at",
    "resolved_at",
    "category",
    "priority",
    "priority_effective",
    "status",
    "building",
    "submitted_by",
    "assignee",
    "cost",
    "sla_hours",
    "sla_hours_effective",
    "asset_type",
    "location_floor",
]


def record_silver_quality(conn: psycopg.Connection, run_id: uuid.UUID, bronze_rows: int, quarantined: int) -> None:
    """Store per-column null rates and per-flag counts; alert on quarantine rate and on null-rate jumps vs the
    previous run."""
    exprs = ", ".join(
        f"avg(({c} IS NULL)::int) AS {c}_null, count(DISTINCT {c}) AS {c}_distinct" for c in SILVER_PROFILED
    )
    stats = conn.execute(f"SELECT {exprs} FROM silver.tickets").fetchone()
    previous = {
        r["column_name"]: float(r["null_rate"])
        for r in conn.execute(
            """SELECT column_name, null_rate FROM meta.dq_metrics
               WHERE layer = 'silver' AND run_id = (
                 SELECT run_id FROM meta.dq_metrics WHERE layer = 'silver' AND run_id <> %s
                 ORDER BY measured_at DESC LIMIT 1)""",
            (run_id,),
        ).fetchall()
    }
    for c in SILVER_PROFILED:
        null_rate = float(stats[f"{c}_null"] or 0)
        conn.execute(
            """INSERT INTO meta.dq_metrics (run_id, layer, column_name, null_rate, distinct_count)
               VALUES (%s, 'silver', %s, %s, %s) ON CONFLICT DO NOTHING""",
            (run_id, c, null_rate, stats[f"{c}_distinct"]),
        )
        if c in previous and abs(null_rate - previous[c]) > MAX_NULL_RATE_DELTA:
            alert(
                conn,
                run_id,
                "warning",
                "null_rate_delta",
                f"silver.{c} null rate moved {previous[c]:.1%} -> {null_rate:.1%}",
                null_rate - previous[c],
                MAX_NULL_RATE_DELTA,
            )

    conn.execute(
        """INSERT INTO meta.dq_flag_counts (run_id, flag, tickets, rate)
           SELECT %s, f, count(*), round(count(*)::numeric / (SELECT greatest(count(*), 1) FROM silver.tickets), 4)
           FROM silver.tickets, unnest(dq_flags) AS f GROUP BY f
           ON CONFLICT DO NOTHING""",
        (run_id,),
    )

    rate = quarantined / bronze_rows if bronze_rows else 0
    if rate > MAX_QUARANTINE_RATE:
        alert(
            conn,
            run_id,
            "warning",
            "quarantine_rate",
            f"{quarantined} of {bronze_rows} bronze rows quarantined",
            rate,
            MAX_QUARANTINE_RATE,
        )
