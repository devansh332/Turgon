"""Deterministic bronze profile: the context every agent gets instead of raw rows.

All aggregation is pushed into Postgres, so the LLM sees a few KB of statistics whether the table
holds 10k rows or 10M. That is the main cost lever for the DQ and schema agents."""

import psycopg

from medallion.config import SOURCE_COLUMNS

_SHAPE = "regexp_replace(regexp_replace({c}, '[0-9]', '9', 'g'), '[A-Za-z]', 'a', 'g')"
_NUMERIC = r"'^\$?-?[0-9]+(\.[0-9]+)?$'"


def profile_bronze(conn: psycopg.Connection, top_n: int = 15, columns: list[str] | None = None) -> dict:
    total = conn.execute("SELECT count(*) AS n FROM bronze.tickets_raw").fetchone()["n"]
    out: dict = {"table": "bronze.tickets_raw", "rows": total, "columns": {}}
    for c in columns or SOURCE_COLUMNS:
        base = conn.execute(
            f"""SELECT count(*) FILTER (WHERE {c} IS NULL) AS absent,
                       count(*) FILTER (WHERE btrim({c}) = '') AS blank,
                       count(DISTINCT {c}) AS distinct_values,
                       count(*) FILTER (WHERE btrim({c}) ~ {_NUMERIC}) AS numeric_like
                FROM bronze.tickets_raw"""
        ).fetchone()
        top = conn.execute(
            f"SELECT {c} AS v, count(*) AS n FROM bronze.tickets_raw GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT %s",
            (top_n,),
        ).fetchall()
        shapes = conn.execute(
            f"SELECT {_SHAPE.format(c=c)} AS s, count(*) AS n FROM bronze.tickets_raw "
            "GROUP BY 1 ORDER BY 2 DESC LIMIT 12"
        ).fetchall()
        col = {
            "blank_rate": round((base["blank"] + base["absent"]) / total, 4) if total else 0,
            "distinct_values": base["distinct_values"],
            "top_values": [(r["v"], r["n"]) for r in top],
            "shapes": [(r["s"], r["n"]) for r in shapes],
        }
        if total and base["numeric_like"] / total > 0.3:
            stats = conn.execute(
                f"""WITH v AS (SELECT replace(btrim({c}), '$', '')::numeric AS x FROM bronze.tickets_raw
                               WHERE btrim({c}) ~ {_NUMERIC})
                    SELECT min(x), max(x), percentile_cont(0.5) WITHIN GROUP (ORDER BY x) AS p50,
                           percentile_cont(0.25) WITHIN GROUP (ORDER BY x) AS p25,
                           percentile_cont(0.75) WITHIN GROUP (ORDER BY x) AS p75,
                           count(*) FILTER (WHERE x < 0) AS negatives, count(*) AS n FROM v"""
            ).fetchone()
            iqr = float(stats["p75"]) - float(stats["p25"])
            hi, lo = float(stats["p75"]) + 3 * iqr, float(stats["p25"]) - 3 * iqr
            outliers = conn.execute(
                f"""SELECT count(*) AS n FROM bronze.tickets_raw WHERE btrim({c}) ~ {_NUMERIC}
                    AND (replace(btrim({c}), '$', '')::numeric > %s OR replace(btrim({c}), '$', '')::numeric < %s)""",
                (hi, lo),
            ).fetchone()["n"]
            col["numeric"] = {
                "parse_rate": round(stats["n"] / total, 4),
                "min": float(stats["min"]),
                "max": float(stats["max"]),
                "p50": float(stats["p50"]),
                "negatives": stats["negatives"],
                "iqr_outliers_3x": outliers,
            }
        out["columns"][c] = col
    return out
