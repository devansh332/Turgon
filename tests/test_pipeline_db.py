"""End-to-end against a real Postgres (the compose `medallion_test` database). Skipped without TEST_DATABASE_URL."""

import csv
import os
from types import SimpleNamespace

import psycopg
import pytest

from medallion import proposals
from medallion.config import DEFAULT_SOURCE, ROOT, Config
from medallion.db import apply_ddl, connect, read_only
from medallion.pipeline import bronze, gold, reconcile, silver

pytestmark = pytest.mark.db
URL = os.environ.get("TEST_DATABASE_URL")
BATCH2 = ROOT / "tests" / "fixtures" / "batch2_drift.csv"


@pytest.fixture(scope="module")
def cfg():
    if not URL:
        pytest.skip("TEST_DATABASE_URL not set")
    cfg = Config(database_url=URL)
    with connect(cfg) as conn:
        conn.execute("DROP SCHEMA IF EXISTS bronze, silver, silver_proposed, gold, quarantine, meta CASCADE")
        conn.commit()
        apply_ddl(conn)
    return cfg


def q(cfg, sql):
    with connect(cfg) as conn:
        return conn.execute(sql).fetchone()


def snapshot(cfg):
    return q(
        cfg,
        """SELECT (SELECT count(*) FROM bronze.tickets_raw) AS bronze,
                            (SELECT count(*) FROM silver.tickets) AS silver,
                            (SELECT count(*) FROM quarantine.tickets_rejected) AS quarantined,
                            (SELECT md5(string_agg(
                                      (to_jsonb(t) - '_silver_run_id' - '_updated_at')::text, '|' ORDER BY ticket_key))
                               FROM silver.tickets t) AS silver_hash,
                            (SELECT md5(string_agg(s::text, '|' ORDER BY month, category, priority))
                               FROM gold.sla_compliance s) AS gold_hash""",
    )


def run_all(cfg, path):
    bronze.ingest(cfg, path)
    silver.build(cfg)
    gold.build(cfg)
    assert reconcile.run_checks(cfg)


def test_full_load_keeps_every_line(cfg):
    run_all(cfg, DEFAULT_SOURCE)
    with DEFAULT_SOURCE.open(newline="") as f:
        data_lines = sum(1 for _ in csv.reader(f)) - 1
    s = snapshot(cfg)
    assert s["bronze"] == data_lines == 10280
    assert s["silver"] + s["quarantined"] <= s["bronze"]


def test_rerun_is_a_no_op(cfg):
    before = snapshot(cfg)
    run_all(cfg, DEFAULT_SOURCE)
    assert snapshot(cfg) == before
    statuses = q(
        cfg,
        """SELECT string_agg(status, ',' ORDER BY stage) AS s FROM (
                           SELECT DISTINCT ON (stage) stage, status FROM meta.pipeline_runs
                           WHERE stage IN ('bronze', 'silver') ORDER BY stage, started_at DESC) x""",
    )["s"]
    assert statuses == "skipped,skipped"


def test_forced_full_rebuild_is_deterministic(cfg):
    before = snapshot(cfg)
    silver.build(cfg, full=True)
    gold.build(cfg)
    assert snapshot(cfg) == before


def test_incremental_batch_with_drift_late_update_and_duplicates(cfg):
    before = snapshot(cfg)
    run_all(cfg, BATCH2)
    after = snapshot(cfg)

    assert after["bronze"] == before["bronze"] + 5  # identical lines are kept in bronze
    assert after["silver"] == before["silver"] + 2  # only TKT-20001 and TKT-20002 are new tickets

    late = q(cfg, "SELECT status, cost, _source_versions FROM silver.tickets WHERE ticket_key = 'TKT-5528'")
    assert (late["status"], float(late["cost"]), late["_source_versions"]) == ("Closed", 250.0, 2)

    dup = q(cfg, "SELECT _source_versions FROM silver.tickets WHERE ticket_key = 'TKT-20001'")
    assert dup["_source_versions"] == 2  # the twice-sent line is counted once

    extra = q(cfg, "SELECT count(*) AS n FROM bronze.tickets_raw WHERE _extra ? 'channel'")
    assert extra["n"] == 5  # the new column is stored, not dropped
    drift = q(
        cfg,
        "SELECT count(*) AS n FROM meta.agent_proposals WHERE kind = 'schema_migration' "
        "AND item_key = 'silver.tickets.channel' AND status = 'pending'",
    )
    assert drift["n"] == 1  # and a reviewed migration is requested
    alerts = q(cfg, "SELECT count(*) AS n FROM meta.alerts WHERE check_name = 'schema_drift'")
    assert alerts["n"] == 1


def test_incremental_result_equals_a_full_rebuild(cfg):
    incremental = snapshot(cfg)
    silver.build(cfg, full=True)
    gold.build(cfg)
    assert snapshot(cfg) == incremental


def test_reconciliation_fails_when_gold_drifts_from_silver(cfg):
    with connect(cfg) as conn:
        conn.execute(
            "DELETE FROM silver.tickets WHERE ticket_key = "
            "(SELECT min(ticket_key) FROM silver.tickets WHERE is_open AND duplicate_of_key IS NULL)"
        )
        conn.commit()
    with pytest.raises(RuntimeError, match="sla_mart_ticket_total"):
        reconcile.run_checks(cfg)
    silver.build(cfg, full=True)
    gold.build(cfg)
    assert reconcile.run_checks(cfg)


def test_agent_sql_runs_without_access_to_anything_but_bronze(cfg):
    with connect(cfg) as conn:
        with read_only(conn):
            assert conn.execute("SELECT count(*) AS n FROM bronze.tickets_raw").fetchone()["n"] > 0
        for sql in ("SELECT * FROM meta.llm_cache", "SELECT * FROM silver.tickets", "SELECT pg_ls_dir('.')"):
            with pytest.raises(psycopg.errors.InsufficientPrivilege), read_only(conn):
                conn.execute(sql)


def test_bi_role_cannot_read_pii_tagged_columns_in_any_gold_object(cfg):
    with connect(cfg) as conn:
        conn.execute("SET ROLE medallion_bi")
        assert conn.execute("SELECT count(category) AS n FROM gold.vendor_performance").fetchone()["n"] > 0
        conn.rollback()
        for sql in ("SELECT assignee FROM gold.vendor_performance", "SELECT assignee FROM gold.ticket_facts"):
            conn.execute("SET ROLE medallion_bi")
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                conn.execute(sql)
            conn.rollback()


def test_approved_drift_migration_adds_and_fills_the_column(cfg):
    migration = {"silver_column": "channel", "pg_type": "text", "extract_sql": "nullif(btrim(_extra->>'channel'), '')"}
    stub = SimpleNamespace(schema_migrations={"silver.tickets.channel": migration})
    with connect(cfg) as conn:
        assert silver.apply_schema_migrations(conn, stub, None) == ["channel"]
        filled = conn.execute("SELECT count(channel) AS n FROM silver.tickets").fetchone()["n"]
        conn.rollback()
    assert filled == 4  # the four batch-2 tickets whose winning row carries the new column


def test_gate_ignores_confidence_changes_and_rejections_stick(cfg):
    common = dict(agent="test", kind="category_label", item_key="zz gate test", rationale="t", auto_threshold=0.9)
    with connect(cfg) as conn:
        a = {"is_label": True, "category": "HVAC"}
        first = proposals.propose(conn, payload=a, confidence=0.80, risk="low", **common)
        again = proposals.propose(conn, payload=a, confidence=0.99, risk="low", **common)
        assert first == again and first[1] == "pending"  # same decision, new confidence: same proposal
        proposals.decide(conn, [first[0]], "rejected", "tester")
        assert proposals.propose(conn, payload=a, confidence=0.99, risk="low", **common)[1] == "rejected"
        b = {"is_label": True, "category": "Plumbing"}
        assert proposals.propose(conn, payload=b, confidence=0.99, risk="low", **common)[1] == "pending"
        conn.execute("DELETE FROM meta.agent_proposals WHERE agent = 'test'")
        conn.commit()
