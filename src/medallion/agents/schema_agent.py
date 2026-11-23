"""Schema inference and evolution agent (option a).

1. Proposes a typed silver schema + DDL + transformation SQL from the bronze profile, then proves them: the
   DDL is executed in a rolled-back transaction and the SELECT is run read-only, and its parse rates are
   compared with the hand-built silver table.
2. For drift proposals opened by bronze (a new column), drafts the ALTER TABLE and extraction expression.
Every output is a high-risk proposal: schema changes always go through a human."""

import json

import psycopg
import sqlglot
from pydantic import BaseModel, Field
from sqlglot import exp

from medallion import proposals
from medallion.agents.llm import LLMClient
from medallion.agents.profiler import profile_bronze
from medallion.agents.sql_guard import UnsafeSQL, validate_select
from medallion.config import ARTIFACTS_DIR
from medallion.db import connect, read_only, sandbox


class ColumnSpec(BaseModel):
    name: str
    source_column: str | None
    pg_type: str
    nullable: bool
    parsing: str
    rationale: str


class SchemaProposal(BaseModel):
    columns: list[ColumnSpec]
    ddl: str
    transform_sql: str


class DriftMigration(BaseModel):
    silver_column: str = Field(pattern=r"^[a-z][a-z0-9_]{1,62}$")
    pg_type: str
    alter_sql: str
    extract_sql: str
    rationale: str
    confidence: float = Field(ge=0, le=1)


def validate_ddl(ddl: str) -> str:
    """Exactly one plain CREATE TABLE in silver_proposed: no CTAS, no second statement, nothing else."""
    try:
        statements = [s for s in sqlglot.parse(ddl, read="postgres") if s is not None]
    except sqlglot.errors.ParseError as exc:
        raise UnsafeSQL(f"does not parse: {exc}") from exc
    # tolerated: the model often prefixes "CREATE SCHEMA [IF NOT EXISTS] silver_proposed;" (it already exists)
    statements = [
        s
        for s in statements
        if not (
            isinstance(s, exp.Create)
            and s.kind == "SCHEMA"
            and s.this is not None
            and "silver_proposed" in (s.this.db, s.this.name)
        )
    ]
    if len(statements) != 1:
        raise UnsafeSQL("must be exactly one statement")
    create = statements[0]
    if not isinstance(create, exp.Create) or create.kind != "TABLE":
        raise UnsafeSQL("must be CREATE TABLE")
    table = create.find(exp.Table)
    if table is None or table.db != "silver_proposed":
        raise UnsafeSQL("table must be in schema silver_proposed")
    if create.expression is not None or any(isinstance(n, exp.Query) for n in create.walk()):
        raise UnsafeSQL("CREATE TABLE AS / subqueries are not allowed")
    return create.sql(dialect="postgres")


def verify(cfg, proposal: SchemaProposal) -> dict:
    """Validate the DDL, execute it as the sandbox role in a transaction that is always rolled back,
    and run the transform SELECT read-only."""
    report: dict = {}
    with connect(cfg) as conn:
        try:
            ddl = validate_ddl(proposal.ddl)
            with sandbox(conn):
                conn.execute(ddl)
            report["ddl_executes"] = True
        except (UnsafeSQL, psycopg.Error) as exc:
            report["ddl_executes"], report["ddl_error"] = False, str(exc).splitlines()[0]
    with connect(cfg) as conn:
        try:
            sql = validate_select(proposal.transform_sql.strip().rstrip(";"))
            names = [c.name for c in proposal.columns]
            probe = ", ".join(f"count({n}) AS {n}" for n in names if n.isidentifier())
            with read_only(conn, timeout_ms=30000):
                counts = conn.execute(f"SELECT count(*) AS _rows, {probe} FROM ({sql}) q").fetchone()
            report["transform_executes"] = True
            report["non_null_counts"] = counts
        except (UnsafeSQL, psycopg.Error, sqlglot.errors.SqlglotError) as exc:
            report["transform_executes"], report["transform_error"] = False, str(exc).splitlines()[0]
    return report


def compare_with_silver(conn: psycopg.Connection, report: dict) -> dict:
    """How many values did the agent's SQL recover vs the hand-written silver, for shared column names?"""
    counts = report.get("non_null_counts") or {}
    silver_cols = {
        r["column_name"]
        for r in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema='silver' AND table_name='tickets'"
        ).fetchall()
    }
    shared = [c for c in counts if c in silver_cols and c != "_rows"]
    if not shared:
        return {}
    ours = conn.execute(f"SELECT {', '.join(f'count({c}) AS {c}' for c in shared)} FROM silver.tickets").fetchone()
    return {c: {"agent": counts[c], "silver": ours[c]} for c in shared}


def run(conn: psycopg.Connection, llm: LLMClient, auto_threshold: float) -> dict:
    profile = profile_bronze(conn, top_n=10)
    payload = {
        "profile": {c: {k: v for k, v in p.items() if k != "distinct_values"} for c, p in profile["columns"].items()}
    }
    proposal = llm.structured(
        agent="schema",
        prompt_name="silver_schema",
        payload=payload,
        schema=SchemaProposal,
        model=llm.cfg.llm_model_reasoning,
    )
    report = verify(llm.cfg, proposal)
    report["vs_hand_built_silver"] = compare_with_silver(conn, report)

    out_dir = ARTIFACTS_DIR / "schema_agent"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "proposed_ddl.sql").write_text(
        sqlglot.transpile(proposal.ddl, read="postgres", pretty=True)[0] if report.get("ddl_executes") else proposal.ddl
    )
    (out_dir / "proposed_transform.sql").write_text(proposal.transform_sql)
    (out_dir / "verification.json").write_text(json.dumps(report, indent=2, default=str))
    (out_dir / "columns.json").write_text(json.dumps([c.model_dump() for c in proposal.columns], indent=2))

    proposals.propose(
        conn,
        agent="schema",
        kind="schema_migration",
        item_key="silver_proposed.tickets",
        payload={
            "ddl": proposal.ddl,
            "transform_sql": proposal.transform_sql,
            "verification": {k: v for k, v in report.items() if k != "non_null_counts"},
        },
        rationale=f"{len(proposal.columns)} typed columns inferred from the bronze profile",
        confidence=0.5,
        risk="high",
        auto_threshold=auto_threshold,
        prompt_version=llm.prompt_version("silver_schema"),
        model=llm.cfg.llm_model_reasoning,
    )

    drafted = 0
    silver_columns = [
        r["column_name"]
        for r in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema='silver' AND table_name='tickets'"
        ).fetchall()
    ]
    for p in proposals.list_proposals(conn):
        if p["agent"] != "drift_detector":
            continue
        column = p["payload"]["column"]
        samples = [
            r["v"]
            for r in conn.execute(
                "SELECT DISTINCT _extra->>%s AS v FROM bronze.tickets_raw WHERE _extra ? %s LIMIT 25", (column, column)
            ).fetchall()
        ]
        m = llm.structured(
            agent="schema",
            prompt_name="schema_drift",
            schema=DriftMigration,
            payload={"column": column, "samples": samples, "silver_columns": silver_columns},
        )
        try:
            parsed = sqlglot.parse_one(m.alter_sql, read="postgres")
            ok = isinstance(parsed, sqlglot.exp.Alter) and "silver.tickets" in m.alter_sql.lower()
        except sqlglot.errors.ParseError:
            ok = False
        proposals.propose(
            conn,
            agent="schema",
            kind="schema_migration",
            item_key=p["item_key"],
            payload=m.model_dump(exclude={"rationale", "confidence"}) | {"sql_valid": ok, "samples": samples[:10]},
            rationale=m.rationale,
            confidence=m.confidence,
            risk="high",
            auto_threshold=auto_threshold,
            prompt_version=llm.prompt_version("schema_drift"),
            model=llm.cfg.llm_model_bulk,
        )
        drafted += 1
    conn.commit()
    return {
        "columns_proposed": len(proposal.columns),
        "ddl_executes": report.get("ddl_executes"),
        "transform_executes": report.get("transform_executes"),
        "drift_migrations_drafted": drafted,
    }
