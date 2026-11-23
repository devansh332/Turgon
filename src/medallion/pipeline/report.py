"""Plain-text operational reports: what happened in the last runs, data quality, alerts, LLM spend, lineage."""

import json

import psycopg

from medallion import seeds


def _table(rows: list[dict], columns: list[str]) -> str:
    if not rows:
        return "  (none)\n"
    widths = {c: max(len(c), *(len(str(r[c])) for r in rows)) for c in columns}
    line = "  " + "  ".join(c.ljust(widths[c]) for c in columns) + "\n"
    for r in rows:
        line += "  " + "  ".join(str(r[c]).ljust(widths[c]) for c in columns) + "\n"
    return line


def metrics(conn: psycopg.Connection) -> str:
    out = "\n== Latest run per stage\n"
    out += _table(
        conn.execute(
            """SELECT DISTINCT ON (stage) stage, status, rows_in, rows_out, rows_rejected,
                  to_char(started_at, 'YYYY-MM-DD HH24:MI:SS') AS started, git_sha, rules_version
           FROM meta.pipeline_runs ORDER BY stage, started_at DESC"""
        ).fetchall(),
        ["stage", "status", "rows_in", "rows_out", "rows_rejected", "started", "git_sha", "rules_version"],
    )

    out += "\n== Layer row counts\n"
    out += _table(
        conn.execute(
            """SELECT 'bronze.tickets_raw' AS "table", count(*) AS rows FROM bronze.tickets_raw
           UNION ALL SELECT 'quarantine.tickets_rejected', count(*) FROM quarantine.tickets_rejected
           UNION ALL SELECT 'silver.tickets', count(*) FROM silver.tickets
           UNION ALL SELECT 'silver.tickets (content duplicates)', count(*) FROM silver.tickets
                     WHERE duplicate_of_key IS NOT NULL
           UNION ALL SELECT 'gold.ticket_facts', count(*) FROM gold.ticket_facts"""
        ).fetchall(),
        ["table", "rows"],
    )

    out += "\n== Quarantine reasons\n"
    out += _table(
        conn.execute(
            """SELECT reason, count(*) AS rows FROM quarantine.tickets_rejected, unnest(reasons) AS reason
           GROUP BY 1 ORDER BY 2 DESC"""
        ).fetchall(),
        ["reason", "rows"],
    )

    out += "\n== Top silver data-quality flags\n"
    out += _table(
        conn.execute(
            """SELECT flag, count(*) AS tickets,
                  round(100.0 * count(*) / (SELECT count(*) FROM silver.tickets), 1) AS pct
           FROM silver.tickets, unnest(dq_flags) AS flag GROUP BY 1 ORDER BY 2 DESC LIMIT 25"""
        ).fetchall(),
        ["flag", "tickets", "pct"],
    )

    out += "\n== Reconciliation (latest)\n"
    out += _table(
        conn.execute(
            """SELECT check_name, expected, actual, passed FROM meta.reconciliation
           WHERE run_id = (SELECT run_id FROM meta.reconciliation ORDER BY checked_at DESC LIMIT 1)
           ORDER BY check_name"""
        ).fetchall(),
        ["check_name", "expected", "actual", "passed"],
    )

    out += "\n== Alerts (last 10)\n"
    out += _table(
        conn.execute(
            """SELECT to_char(created_at, 'MM-DD HH24:MI') AS at, severity, check_name, left(message, 90) AS message
           FROM meta.alerts ORDER BY alert_id DESC LIMIT 10"""
        ).fetchall(),
        ["at", "severity", "check_name", "message"],
    )

    out += "\n== LLM usage by agent\n"
    out += _table(
        conn.execute(
            """SELECT agent, model, count(*) AS calls, count(*) FILTER (WHERE cache_hit) AS cache_hits,
                  count(*) FILTER (WHERE NOT ok) AS failures, sum(input_tokens) AS tokens_in,
                  sum(output_tokens) AS tokens_out, round(sum(cost_usd), 4) AS cost_usd,
                  round(avg(latency_ms) FILTER (WHERE NOT cache_hit)) AS avg_latency_ms
           FROM meta.llm_calls GROUP BY 1, 2 ORDER BY 1"""
        ).fetchall(),
        ["agent", "model", "calls", "cache_hits", "failures", "tokens_in", "tokens_out", "cost_usd", "avg_latency_ms"],
    )

    out += "\n== Agent proposals\n"
    out += _table(
        conn.execute(
            "SELECT kind, status, count(*) AS n FROM meta.agent_proposals GROUP BY 1, 2 ORDER BY 1, 2"
        ).fetchall(),
        ["kind", "status", "n"],
    )
    return out


def lineage(conn: psycopg.Connection, key: str) -> str:
    t = conn.execute("SELECT * FROM silver.tickets WHERE ticket_key = %s", (key,)).fetchone()
    q = conn.execute("SELECT * FROM quarantine.tickets_rejected WHERE ticket_key = %s", (key,)).fetchall()
    if not t and not q:
        return f"{key}: not found in silver or quarantine"
    out = f"\n== {key}\n"
    if t:
        shown = [
            "category_raw",
            "category",
            "category_source",
            "priority_raw",
            "priority_effective",
            "status",
            "created_at",
            "resolved_at",
            "resolution_hours",
            "sla_hours_effective",
            "sla_source",
            "cost",
            "duplicate_of_key",
            "dq_flags",
        ]
        out += "".join(f"  silver.{c:<22} {t[c]}\n" for c in shown)
        run = conn.execute("SELECT * FROM meta.pipeline_runs WHERE run_id = %s", (t["_silver_run_id"],)).fetchone()
        out += (
            f"\n  last changed by silver run {run['run_id']} at {run['started_at']:%Y-%m-%d %H:%M:%S}"
            f" (git {run['git_sha']}, rules {run['rules_version']})\n"
        )
        labels = seeds.load(seeds.AGENT_OWNED["category_label"])
        templates = seeds.load(seeds.AGENT_OWNED["description_template"])
        for name, entry in (
            ("category label", labels.get((t["category_raw"] or "").strip().lower())),
            ("description template", templates.get(t["description_template"])),
        ):
            if entry:
                out += f"  {name} rule: {json.dumps(entry.get('_provenance'), default=str)}\n"
    versions = conn.execute(
        """SELECT b._bronze_id, b._source_file, b._row_number, b._ingested_at, b._batch_id, b.status, b._row_hash
           FROM bronze.tickets_raw b WHERE upper(btrim(b.ticket_id)) = %s OR b._bronze_id = %s
           ORDER BY b._bronze_id""",
        (key, t["_bronze_id"] if t else -1),
    ).fetchall()
    out += "\n  bronze versions (winner marked *):\n"
    for v in versions:
        mark = "*" if t and v["_bronze_id"] == t["_bronze_id"] else " "
        out += (
            f"  {mark} bronze_id={v['_bronze_id']} {v['_source_file']}:{v['_row_number']} "
            f"ingested={v['_ingested_at']:%Y-%m-%d %H:%M:%S} batch={str(v['_batch_id'])[:8]} "
            f"status={v['status']!r} hash={v['_row_hash'][:10]}\n"
        )
    for r in q:
        out += f"  quarantined bronze_id={r['_bronze_id']} reasons={r['reasons']}\n"
    return out
