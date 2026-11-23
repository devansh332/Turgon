"""Execute generated predicates against labelled synthetic rows; never modify bronze or send eval labels to an LLM."""

import json

import psycopg
import sqlglot
from sqlglot import exp

from medallion.agents.sql_guard import UnsafeSQL, predicate_query
from medallion.config import EVALS_DIR, SOURCE_COLUMNS
from medallion.db import read_only


def score_predicates(conn: psycopg.Connection, rules: list[dict]) -> dict:
    fixture = json.loads((EVALS_DIR / "golden" / "dq_predicates.json").read_text())
    cases = fixture["cases"]
    expected = {(i, issue) for i, case in enumerate(cases, 1) for issue in case["issues"]}
    scope = {issue for _, issue in expected}
    columns = ", ".join(f"{c} text" for c in SOURCE_COLUMNS)
    conn.execute(
        f"CREATE TEMP TABLE dq_eval_fixture (_bronze_id int PRIMARY KEY, {columns}, "
        "_extra jsonb DEFAULT '{}', _row_hash text DEFAULT '', _ingested_at timestamptz DEFAULT now())"
    )
    with conn.cursor() as cur:
        cur.executemany(
            f"INSERT INTO dq_eval_fixture (_bronze_id, {', '.join(SOURCE_COLUMNS)}) "
            f"VALUES ({', '.join(['%s'] * (len(SOURCE_COLUMNS) + 1))})",
            [
                [i] + [(fixture["defaults"] | case["overrides"]).get(c) for c in SOURCE_COLUMNS]
                for i, case in enumerate(cases, 1)
            ],
        )
    conn.execute("GRANT SELECT ON dq_eval_fixture TO medallion_agent")
    conn.commit()  # read_only must begin a fresh transaction; the temporary table survives the commit
    predicted, errors = set(), []
    for rule in rules:
        issue = f"{rule['column']}:{rule['issue_type']}"
        if issue not in scope:
            continue  # format diversity and other dataset-level findings have no per-row truth label here
        try:
            tree = sqlglot.parse_one(predicate_query(rule["predicate"]), read="postgres")
            for table in list(tree.find_all(exp.Table)):
                table.replace(exp.to_table("pg_temp.dq_eval_fixture"))
            with read_only(conn):
                predicted.update((r["_bronze_id"], issue) for r in conn.execute(tree.sql(dialect="postgres")))
        except (UnsafeSQL, psycopg.Error) as exc:
            errors.append({"issue": issue, "error": str(exc).splitlines()[0]})
    tp, fp, fn = len(predicted & expected), len(predicted - expected), len(expected - predicted)
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    conn.execute("DROP TABLE dq_eval_fixture")
    conn.commit()
    return {
        "cases": len(cases),
        "scope": sorted(scope),
        "true_positives": tp,
        "false_positives": fp,
        "false_negatives": fn,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(2 * precision * recall / (precision + recall), 4) if precision + recall else 0.0,
        "errors": errors,
    }
