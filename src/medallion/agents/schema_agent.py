"""Schema evolution agent (option a, drift part).

For each drift proposal opened by bronze (a new column landed in `_extra`), drafts the silver column type and
a guarded extraction expression from sample values. Every output is a high-risk proposal: schema changes
always go through a human, and silver rebuilds the ALTER from the validated name and type
(silver.apply_schema_migrations) instead of executing the model's SQL.

A full silver-schema proposer (typed DDL plus transform SQL) was tried and removed: it failed verification
twice and the hand-built silver was better (proposals in artifacts/agent_run/proposals.csv)."""

import psycopg
import sqlglot
from pydantic import BaseModel, Field

from medallion import proposals
from medallion.agents.llm import LLMClient


class DriftMigration(BaseModel):
    silver_column: str = Field(pattern=r"^[a-z][a-z0-9_]{1,62}$")
    pg_type: str
    alter_sql: str
    extract_sql: str
    rationale: str
    confidence: float = Field(ge=0, le=1)


def run(conn: psycopg.Connection, llm: LLMClient, auto_threshold: float) -> dict:
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
    return {"drift_migrations_drafted": drafted}
