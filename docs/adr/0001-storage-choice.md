# ADR 0001: PostgreSQL as the storage engine for all three layers

**Status:** accepted. **Context:** about 10k rows today; the brief also asks what changes at 100x (1M+ rows, daily loads).

## Decision

Use one PostgreSQL 16 instance. Each layer is a schema: `bronze`, `silver`, `gold`, plus `quarantine` for
rejected rows and `meta` for the control plane. Everything runs from `docker compose`.

## Why

Most of the work in this project is not analytical scanning. It is transactional bookkeeping:

- the run log;
- file-level rerun safety;
- watermarks;
- the LLM cache;
- agent proposals with approval states;
- the alert log;
- the reconciliation log.

Postgres is the best tool here for that work, and good enough for the analytics:

| Need | How Postgres covers it |
|---|---|
| Safe reruns | Each stage runs in one transaction. Bronze checks `meta.ingested_files` by file hash, and silver writes with `MERGE`. |
| Atomic gold rebuild | The gold schema is dropped and recreated inside one transaction. Readers see the old version until `COMMIT`. |
| Layer separation | Schemas give clear naming and per-layer `GRANT`s (for example, BI users get only `gold`). |
| Schema-on-read bronze | Every source column is `TEXT`, and unknown columns go into `_extra jsonb`. |
| Safe agent SQL | `SET TRANSACTION READ ONLY` plus `statement_timeout`. |
| Reviewer experience | A single image that every reviewer knows. |

## Alternatives considered

| Option | Why not here | When it is the right choice |
|---|---|---|
| DuckDB + Parquet | Very fast for scans. But it is embedded (no server for `docker compose`), allows one writer at a time, and is weak for the bookkeeping tables. | Local analysis, or as the engine reading Iceberg/Parquet at 100x. |
| ClickHouse | Excellent for gold aggregations. Updates and duplicate removal are eventual (ReplacingMergeTree), which makes silver rerun safety hard to prove. | Serving gold to dashboards at 100M+ rows. |
| Spark + Delta/Iceberg + MinIO, or Trino | The "real" lakehouse, but several containers for 10k rows. The brief grades against over-engineering. | 1000x and beyond, many sources, many teams. |
| Snowflake / Databricks / BigQuery | Hosted only, so they break the one-command local run, and trial accounts expire. | Production. The mapping is in [scale_100x.md](../scale_100x.md). |

## Consequences

- The SQL is plain ANSI-leaning Postgres (`MERGE`, window functions, `FILTER`), so the gold models port to
  dbt with Snowflake, Databricks or Trino adapters with minor changes.
- Postgres stores data by row, so the ceiling is roughly 100M silver rows with partitioning. Beyond that, see
  the 100x plan.
