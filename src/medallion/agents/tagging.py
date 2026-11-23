"""Column tagging: deterministic rules at landing (always on, free), LLM review as an approved overlay.

Rules look only at values, never at column names, so a renamed column is still caught."""

import re
from typing import Literal

import psycopg
from pydantic import BaseModel, Field

from medallion import proposals
from medallion.agents.llm import LLMClient
from medallion.agents.profiler import profile_bronze

_PERSON = re.compile(r"^(?:[A-Z][a-z]+|[A-Z]\.)\s[A-Z][a-z]+$")  # case-sensitive: "maintenance team" is not a name
_EMAIL = re.compile(r"[^@\s]+@[^@\s]+\.[a-z]{2,}", re.IGNORECASE)
_PHONE = re.compile(r"^\+?(?:\(\d{3}\)|\d{3})[\s.-]?\d{3}[\s.-]?\d{4}$")
_MONEY = re.compile(r"^\$\d+(?:\.\d{1,2})?$|^\d+\.\d{2}$")
_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4}|\d{10}")


def _share(values: list[str], pattern: re.Pattern) -> float:
    return sum(bool(pattern.search(v)) for v in values) / len(values) if values else 0.0


def rule_tags(column: str, sample: list[str]) -> dict:
    values = [v.strip() for v in sample if v and v.strip()]
    if _share(values, _EMAIL) > 0.3:
        return {"pii": True, "pii_type": "email", "sensitivity": "confidential", "domain": "people"}
    if _share(values, _PHONE) > 0.3:
        return {"pii": True, "pii_type": "phone", "sensitivity": "confidential", "domain": "people"}
    if _share(values, _PERSON) > 0.5:
        return {"pii": True, "pii_type": "person_name", "sensitivity": "confidential", "domain": "people"}
    if _share(values, _MONEY) > 0.5:
        return {"pii": False, "pii_type": None, "sensitivity": "internal", "domain": "finance"}
    if _share(values, _TIMESTAMP) > 0.5:
        return {"pii": False, "pii_type": None, "sensitivity": "internal", "domain": "operations"}
    return {"pii": False, "pii_type": None, "sensitivity": "internal", "domain": "operations"}


class ColumnTag(BaseModel):
    column: str
    pii: bool
    pii_type: Literal["person_name", "email", "phone", "free_text_may_contain_pii"] | None
    sensitivity: Literal["public", "internal", "confidential"]
    domain: Literal["people", "finance", "operations", "location", "asset"]
    rationale: str = Field(description="one sentence, cite the evidence in the sample values")
    confidence: float = Field(ge=0, le=1)


class ColumnTags(BaseModel):
    tags: list[ColumnTag]


def sample_values(conn: psycopg.Connection, column: str, n: int = 500) -> list[str]:
    """Deterministic sample (ordered by row hash), the same rows the landing rules look at."""
    return [
        r["v"]
        for r in conn.execute(
            f"SELECT {column} AS v FROM bronze.tickets_raw ORDER BY _row_hash LIMIT %s", (n,)
        ).fetchall()
    ]


def run(conn: psycopg.Connection, llm: LLMClient, auto_threshold: float) -> dict:
    profile = profile_bronze(conn, top_n=8)
    rules = {c: rule_tags(c, sample_values(conn, c)) for c in profile["columns"]}
    payload = {
        "columns": [
            {"column": c, "top_values": p["top_values"], "rule_tags": rules[c]} for c, p in profile["columns"].items()
        ]
    }
    result = llm.structured(
        agent="tagging", prompt_name="column_tagging", payload=payload, schema=ColumnTags, model=llm.cfg.llm_model_bulk
    )
    changed = 0
    for tag in result.tags:
        rule = rules.get(tag.column, {})
        body = tag.model_dump(include={"pii", "pii_type", "sensitivity", "domain"})
        changed += body != rule
        proposals.propose(
            conn,
            agent="tagging",
            kind="column_tag",
            item_key=f"bronze.tickets_raw.{tag.column}",
            payload=body,
            rationale=tag.rationale,
            confidence=tag.confidence,
            risk="high" if rule.get("pii") and not tag.pii else "low",  # removing a PII flag needs a human
            auto_threshold=auto_threshold,
            prompt_version=llm.prompt_version("column_tagging"),
            model=llm.cfg.llm_model_bulk,
        )
    conn.commit()
    return {"columns": len(result.tags), "differs_from_rules": changed}
