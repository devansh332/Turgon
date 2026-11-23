"""Data-quality agent (option b): profile -> rules in plain English with the business reason -> SQL predicate,
dry-run against bronze before anything is proposed. Approved rules run in silver as `rule:<id>` flags, or as
quarantine reasons for `quarantine` rules."""

from typing import Literal

import psycopg
from pydantic import BaseModel, Field

from medallion import proposals
from medallion.agents.llm import LLMClient
from medallion.agents.profiler import profile_bronze
from medallion.agents.sql_guard import UnsafeSQL, predicate_query
from medallion.config import SOURCE_COLUMNS
from medallion.db import connect, read_only

ISSUE_TYPES = [
    "placeholder_values",
    "sentinel_values",
    "mixed_formats",
    "synonyms_or_casing",
    "missing_values",
    "invalid_identifier",
    "out_of_range",
    "logical_inconsistency",
    "duplicates",
    "test_or_junk_data",
    "swapped_fields",
    "free_text_needs_parsing",
    "outliers",
]


class Rule(BaseModel):
    rule_id: str = Field(pattern=r"^[a-z][a-z0-9_]{2,60}$")
    column: Literal[tuple(SOURCE_COLUMNS)]
    issue_type: Literal[tuple(ISSUE_TYPES)]
    finding: str
    why_it_matters: str
    action: Literal["flag", "null_out", "normalize", "quarantine"]
    severity: Literal["low", "medium", "high"]
    predicate: str
    implementation: str


class Rules(BaseModel):
    rules: list[Rule]


def compact_profile(profile: dict) -> dict:
    return {
        c: {k: p[k] for k in ("blank_rate", "distinct_values", "top_values", "shapes", "numeric") if k in p}
        for c, p in profile["columns"].items()
    }


def dry_run(conn: psycopg.Connection, predicate: str) -> tuple[int | None, str | None]:
    try:
        sql = predicate_query(predicate)
        with read_only(conn):
            return conn.execute(f"SELECT count(*) AS n FROM ({sql}) q").fetchone()["n"], None
    except (UnsafeSQL, psycopg.Error) as exc:
        return None, f"{type(exc).__name__}: {str(exc).splitlines()[0]}"


def propose_rules(conn: psycopg.Connection, llm: LLMClient) -> tuple[list[Rule], dict]:
    profile = profile_bronze(conn, top_n=12)
    samples = conn.execute(
        f"SELECT {', '.join(SOURCE_COLUMNS)} FROM bronze.tickets_raw ORDER BY _row_hash LIMIT 12"
    ).fetchall()
    payload = {
        "profile": {"rows": profile["rows"], "columns": compact_profile(profile)},
        "sample_rows": samples,
        "issue_types": ISSUE_TYPES,
    }
    result = llm.structured(
        agent="dq", prompt_name="dq_rules", payload=payload, schema=Rules, model=llm.cfg.llm_model_reasoning
    )
    return result.rules, profile


def run(conn: psycopg.Connection, llm: LLMClient, auto_threshold: float) -> dict:
    rules, profile = propose_rules(conn, llm)
    valid = invalid = 0
    sandbox = connect(llm.cfg)  # dry runs get their own connection so READ ONLY never leaks into ours
    for rule in rules:
        rows, error = dry_run(sandbox, rule.predicate)
        payload = rule.model_dump(exclude={"rule_id"}) | {"affected_rows": rows, "dry_run_error": error}
        if rule.action in ("null_out", "normalize"):
            # silver applies predicates as flags or quarantine only; how to null or normalize a value is a
            # parsing rule a human writes in seeds/reference.yaml, so the request is kept but not executed
            payload["requested_action"], payload["action"] = rule.action, "flag"
        if error or not rows:
            invalid += 1
            payload["action"] = "flag"
        else:
            valid += 1
        # quarantine removes rows from silver, so agent-written quarantine SQL is always reviewed by a human
        risk = "low" if payload["action"] == "flag" and not error and rows else "high"
        proposals.propose(
            conn,
            agent="dq",
            kind="dq_rule",
            item_key=rule.rule_id,
            payload=payload,
            rationale=f"{rule.finding} | why: {rule.why_it_matters}",
            confidence=0.0 if error else 0.8,
            risk=risk,
            auto_threshold=auto_threshold,
            prompt_version=llm.prompt_version("dq_rules"),
            model=llm.cfg.llm_model_reasoning,
        )
    sandbox.close()
    conn.commit()
    return {
        "rules": len(rules),
        "dry_run_ok": valid,
        "dry_run_failed_or_empty": invalid,
        "bronze_rows_profiled": profile["rows"],
    }
