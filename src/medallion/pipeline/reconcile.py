"""Cross-layer reconciliation: every bronze row is accounted for, and gold totals tie back to figures
recomputed directly from silver (not from gold.ticket_facts, which the marts are built on, so a bug in that
view or in a mart shows up as a mismatch). Invariant checks expect zero violating rows."""

from medallion.config import Config
from medallion.db import connect
from medallion.pipeline.observability import alert
from medallion.pipeline.runs import stage_run
from medallion.seeds import rules_version

_REAL = "FROM silver.tickets WHERE duplicate_of_key IS NULL"
_AS_OF = f"(SELECT greatest(max(created_at), max(resolved_at)) {_REAL})"

CHECKS = {
    # every bronze row is either quarantined or folded into exactly one silver ticket
    "bronze_rows_accounted_for": (
        "SELECT count(*) FROM bronze.tickets_raw",
        "SELECT (SELECT count(*) FROM quarantine.tickets_rejected)"
        " + (SELECT coalesce(sum(_source_versions), 0) FROM silver.tickets)",
    ),
    "sla_mart_ticket_total": (f"SELECT count(*) {_REAL}", "SELECT sum(tickets) FROM gold.sla_compliance"),
    "sla_mart_breach_total": (
        f"""SELECT count(*) {_REAL} AND sla_hours_effective IS NOT NULL AND (
                (resolution_hours IS NOT NULL AND resolution_hours > sla_hours_effective)
             OR (is_open AND extract(epoch FROM {_AS_OF} - created_at) / 3600 > sla_hours_effective))""",
        "SELECT sum(sla_breached) FROM gold.sla_compliance",
    ),
    "vendor_mart_cost_total": (
        f"SELECT coalesce(sum(cost), 0) {_REAL}",
        "SELECT sum(total_cost) FROM gold.vendor_performance",
    ),
    "backlog_final_day_equals_open_tickets": (
        f"SELECT count(*) {_REAL} AND is_open",
        "SELECT sum(open_backlog) FROM gold.backlog_daily WHERE day = (SELECT max(day) FROM gold.backlog_daily)",
    ),
    "backlog_opened_equals_scoped_tickets": (
        f"SELECT count(*) {_REAL} AND (is_open OR resolution_hours IS NOT NULL)",
        "SELECT sum(opened) FROM gold.backlog_daily",
    ),
    "invariant_backlog_never_negative": ("SELECT 0", "SELECT count(*) FROM gold.backlog_daily WHERE open_backlog < 0"),
    "invariant_rates_between_0_and_1": (
        "SELECT 0",
        "SELECT (SELECT count(*) FROM gold.sla_compliance WHERE breach_rate NOT BETWEEN 0 AND 1)"
        " + (SELECT count(*) FROM gold.vendor_performance WHERE scope_mismatch_rate NOT BETWEEN 0 AND 1)",
    ),
    "invariant_no_negative_durations": (
        "SELECT 0",
        "SELECT count(*) FROM silver.tickets WHERE resolution_hours < 0 OR cost < 0",
    ),
}


def run_checks(cfg: Config) -> bool:
    with stage_run(cfg, "reconcile", rules_version()) as run, connect(cfg) as conn:
        failed = []
        for name, (expected_sql, actual_sql) in CHECKS.items():
            expected = conn.execute(f"SELECT ({expected_sql}) AS v").fetchone()["v"]
            actual = conn.execute(f"SELECT ({actual_sql}) AS v").fetchone()["v"]
            passed = expected == actual
            conn.execute(
                """INSERT INTO meta.reconciliation (run_id, check_name, expected, actual, passed)
                   VALUES (%s, %s, %s, %s, %s)""",
                (run.run_id, name, expected, actual, passed),
            )
            run.details[name] = {"expected": expected, "actual": actual, "passed": passed}
            if not passed:
                failed.append(name)
                alert(conn, run.run_id, "error", f"reconciliation:{name}", f"expected {expected}, got {actual}")
        conn.commit()
        run.rows_in, run.rows_rejected = len(CHECKS), len(failed)
        if failed:
            raise RuntimeError(f"reconciliation failed: {failed}")
        return True
