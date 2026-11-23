"""End-to-end against a real Postgres (the compose `medallion_test` database). Skipped without TEST_DATABASE_URL."""

import csv
import os
from types import SimpleNamespace
from uuid import uuid4

import psycopg
import pytest

from medallion import proposals, seeds
from medallion.agents import dq_agent, evals
from medallion.agents.dq_eval import score_predicates
from medallion.agents.llm import LLMClient
from medallion.config import DEFAULT_SOURCE, ROOT, Config
from medallion.db import apply_ddl, connect, read_only
from medallion.pipeline import bronze, gold, reconcile, report, silver

pytestmark = pytest.mark.db
URL = os.environ.get("TEST_DATABASE_URL")
BATCH2 = ROOT / "tests" / "fixtures" / "batch2_drift.csv"


@pytest.fixture(scope="module")
def cfg():
    if not URL:
        pytest.skip("TEST_DATABASE_URL not set")
    cfg = Config(database_url=URL)
    with connect(cfg) as conn:
        conn.execute("DROP SCHEMA IF EXISTS bronze, silver, gold, quarantine, meta CASCADE")
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


def test_lineage_traces_gold_marts_silver_and_bronze(cfg):
    key = q(cfg, "SELECT ticket_key FROM gold.ticket_facts WHERE resolution_hours IS NOT NULL LIMIT 1")["ticket_key"]
    with connect(cfg) as conn:
        out = report.lineage(conn, key)
    for layer in (
        "gold.ticket_facts",
        "gold.sla_compliance",
        "gold.vendor_performance",
        "gold.backlog_daily",
        "built by gold run",
        "last changed by silver run",
        "data/raw_tickets.csv:",
    ):
        assert layer in out


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
        b = {"is_label": True, "category": "Plumbing"}  # a different decision is a new proposal
        assert proposals.propose(conn, payload=b, confidence=0.99, risk="low", **common)[1] == "auto_approved"
        conn.execute("DELETE FROM meta.agent_proposals WHERE agent = 'test'")
        conn.commit()


def test_ingest_during_silver_is_processed_on_the_next_run(cfg, tmp_path, monkeypatch):
    with DEFAULT_SOURCE.open(newline="") as f:
        reader = csv.DictReader(f)
        sample, fields = next(reader), reader.fieldnames
    path = tmp_path / "overlapping.csv"
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fields)
        writer.writeheader()
        writer.writerow(sample | {"ticket_id": "TKT-91001"})
    original = silver._scope

    def ingest_after_scope(*args):
        scoped = original(*args)
        bronze.ingest(cfg, path)
        return scoped

    with monkeypatch.context() as m:
        m.setattr(silver, "_scope", ingest_after_scope)
        silver.build(cfg, full=True)
    assert q(cfg, "SELECT count(*) AS n FROM silver.tickets WHERE ticket_key='TKT-91001'")["n"] == 0
    silver.build(cfg)
    assert q(cfg, "SELECT count(*) AS n FROM silver.tickets WHERE ticket_key='TKT-91001'")["n"] == 1
    gold.build(cfg)
    assert reconcile.run_checks(cfg)


@pytest.mark.parametrize("initial_status", ["auto_approved", "approved"])
def test_revocation_removes_exported_seed_and_does_not_restore_older_decisions(
    cfg, tmp_path, monkeypatch, initial_status
):
    monkeypatch.setattr(seeds, "SEEDS_DIR", tmp_path)
    common = dict(agent="test", kind="category_label", item_key="review revocation", rationale="t", auto_threshold=0.9)
    with connect(cfg) as conn:
        older, _ = proposals.propose(
            conn, payload={"is_label": True, "category": "HVAC"}, confidence=0.8, risk="high", **common
        )
        proposals.decide(conn, [older], "approved", "tester")
        # Keep the older decision in the DB to check that export will never resurrect it after revocation.
        current, status = proposals.propose(
            conn,
            payload={"is_label": True, "category": "Plumbing"},
            confidence=0.99 if initial_status == "auto_approved" else 0.8,
            risk="low",
            **common,
        )
        if initial_status == "approved":
            proposals.decide(conn, [current], "approved", "tester")
        else:
            assert status == "auto_approved"
        seeds.save("category_labels.yaml", {"unrelated committed label": {"is_label": True, "category": "Electrical"}})
        proposals.export_seeds(conn)
        assert seeds.load("category_labels.yaml")[common["item_key"]]["category"] == "Plumbing"
        assert proposals.decide(conn, [current], "rejected", "reviewer") == 1
        proposals.export_seeds(conn)
        assert common["item_key"] not in seeds.load("category_labels.yaml")
        assert "unrelated committed label" in seeds.load("category_labels.yaml")
        proposals.export_seeds(conn)
        assert common["item_key"] not in seeds.load("category_labels.yaml")
        conn.execute("DELETE FROM meta.agent_proposals WHERE agent='test'")


def test_rejected_alternative_preserves_the_existing_seed(cfg, tmp_path, monkeypatch):
    monkeypatch.setattr(seeds, "SEEDS_DIR", tmp_path)
    seeds.save("category_labels.yaml", {"review alternate": {"is_label": True, "category": "HVAC"}})
    with connect(cfg) as conn:
        proposal, _ = proposals.propose(
            conn,
            agent="test",
            kind="category_label",
            item_key="review alternate",
            payload={"is_label": True, "category": "Plumbing"},
            rationale="t",
            confidence=0.99,
            risk="low",
            auto_threshold=0.9,
        )
        proposals.decide(conn, [proposal], "rejected", "tester")
        proposals.export_seeds(conn)
        assert seeds.load("category_labels.yaml")["review alternate"]["category"] == "HVAC"
        conn.execute("DELETE FROM meta.agent_proposals WHERE agent='test'")


def test_eval_bypasses_cache_and_metrics_ignore_other_runs(cfg):
    from pydantic import BaseModel

    class Output(BaseModel):
        value: str

    replies = iter(['{"value":"cached"}', '{"value":"fresh"}'])

    def create(**kwargs):
        return SimpleNamespace(
            usage=SimpleNamespace(prompt_tokens=100, completion_tokens=50),
            choices=[SimpleNamespace(message=SimpleNamespace(content=next(replies)))],
        )

    client = LLMClient.__new__(LLMClient)
    client.cfg, client.eval_run_id = cfg, None
    client.client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=create)))
    with connect(cfg, autocommit=True) as conn:
        client.log_conn = conn
        args = dict(
            agent="test",
            prompt_name="category_labels",
            model="fake-review",
            payload={"test": "eval scope"},
            schema=Output,
        )
        assert client.structured(**args).value == "cached"
        assert client.structured(**args).value == "cached"
        client.eval_run_id = uuid4()
        assert client.structured(**args).value == "fresh"
        metrics = evals.ops_metrics(conn, client.eval_run_id, 1000)
        assert metrics["calls"] == 1
        assert metrics["tokens_in"] == 100 and metrics["tokens_out"] == 50
        assert metrics["first_try_valid_rate"] == 1.0
        assert evals.ops_metrics(conn, uuid4(), 1000)["calls"] == 0
        conn.execute("DELETE FROM meta.llm_calls WHERE agent='test'")
        conn.execute("DELETE FROM meta.llm_cache WHERE model='fake-review'")


def test_dq_predicate_eval_penalizes_catch_all_rules_and_invalid_sql(cfg):
    exact = {"column": "cost", "issue_type": "sentinel_values", "predicate": "btrim(cost) = '-1'"}
    with connect(cfg) as conn:
        good = score_predicates(conn, [exact])
        broad = score_predicates(conn, [exact | {"predicate": "true"}])
        unsafe = score_predicates(conn, [exact | {"predicate": "true; DROP TABLE silver.tickets"}])
    assert good["true_positives"] == 1 and good["false_positives"] == 0
    assert broad["false_positives"] == broad["cases"] - 1
    assert good["precision"] > broad["precision"] and good["f1"] > broad["f1"]
    assert unsafe["errors"] and unsafe["true_positives"] == 0


def test_dq_payload_includes_value_level_and_cross_column_evidence(cfg):
    class FakeLLM:
        cfg = SimpleNamespace(llm_model_reasoning="fake")

        def structured(self, *, payload, **kwargs):
            self.payload = payload
            return dq_agent.Rules(rules=[])

    llm = FakeLLM()
    with connect(cfg) as conn:
        dq_agent.propose_rules(conn, llm)
    created = llm.payload["profile"]["columns"]["created_at"]
    assert created["shape_examples"]
    assert created["numeric_date_order"]["first_part_over_12"] == 0
    assert created["numeric_date_order"]["second_part_over_12"] > 0
    assert llm.payload["cross_column_checks"]["long_category_short_description"]["samples"]
