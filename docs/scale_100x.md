# What changes at 100x (1M+ tickets, daily loads, more sources)

Today's numbers: 10,280 rows, a full pipeline run in a few seconds, LLM spend $0.17
for a complete cold-cache pass of all four agents. The design choices that matter at 100x were made up front, so this
is mostly a list of *what to swap*, not *what to redesign*.

## 1. What already scales, and why

| Design choice | Why it survives 100x |
|---|---|
| **The LLM labels distinct values, not rows.** Silver joins the approved seeds. | LLM cost grows with *new vocabulary*, not volume. 10k rows hold 99 labels, 98 templates and 33 people. At 1M rows from similar systems we would expect perhaps 2–5x the vocabulary, not 100x. Daily loads only pay for values never seen before (`classifier.run` skips values already in the seeds, and `meta.llm_cache` catches repeated calls). |
| **No LLM in the silver build.** | The heavy path is pure SQL plus Python transforms, fully repeatable and cheap to rebuild. The models never sit in the critical path; they propose, offline. |
| **Bronze is skipped by file hash; silver is incremental by watermark** (plus a 48h replay window), MERGE-based | A daily load touches only that day's rows. A full rebuild happens only when `rules_version` changes. |
| **Gold is rebuilt atomically from silver** | It is cheap while the marts are small aggregates. |
| **Reconciliation checks after every run** | Correctness checks are O(rows) SQL counts and stay cheap. |

## 2. What breaks first, and the swap

| Pressure point (≈ when) | Today | At 100x |
|---|---|---|
| Silver transform in Python (≈ 5–10M rows/day) | `psycopg` fetch, then Python, then MERGE, single process | Keep the rules in seeds, but compile the deterministic parts to SQL (dbt models), or run the same `transform()` as a Spark or Polars UDF over partitions. `transform()` is pure, which is why this is possible. |
| Row-store scans in Postgres (≈ 100M rows) | Single node, B-tree indexes | Partition `silver.tickets` by `created_month`, and put bronze in object storage as Parquet/Iceberg. Silver and gold move to Snowflake, Databricks or BigQuery. Mapping: bronze becomes an external stage or an Iceberg table; silver MERGE becomes `MERGE INTO` (identical semantics); gold materialized views become dbt incremental models or dynamic tables. |
| Gold full rebuild (≈ 10M+ silver rows) | `DROP SCHEMA gold CASCADE` and recreate in one transaction | Incremental by month partition. Only months touched by the run's watermark window are recomputed (`backlog_daily` from the first changed day onward). |
| Orchestration | `make run`, stages in sequence, a `pipeline_runs` row per stage | Dagster or Airflow with the same stage boundaries (each stage is already a function with run logging). Sensors on new files, SLA alerts on late data. |
| Alerts | `meta.alerts` table plus a log line | Route to PagerDuty or Slack. Thresholds per source, with anomaly detection on the `meta.dq_metrics` history (which already exists). |
| Multiple sources | One CSV contract | One bronze table per source, with drift detection per source (`meta.schema_registry` is keyed by source). Silver converges sources through the same seeds. People and asset resolution becomes a real entity-resolution step (blocking, then pairwise matching, then LLM only for the uncertain middle band). |

## 3. LLM economics at 100x

Measured with OpenAI `gpt-5-mini` at $0.25 / $2.00 per 1M input/output tokens. Output tokens (including
reasoning tokens) are about 60% of the token count but nearly all of the cost. The measured calls are in
[artifacts/agent_run/llm_calls.csv](../artifacts/agent_run/llm_calls.csv).

| | Today (10k rows) | 1M rows, first load | 1M rows, daily delta (~10k rows) |
|---|---|---|---|
| Distinct values to label | about 230 (99 labels, 98 templates, 33 people) | about 2–5x → 460–1,150 | new values only, typically < 1% |
| LLM calls | 24 live calls (labels 4, templates 14 including re-asks and a re-run, others 1–3 each) | about 50–120 | about 0–3 |
| Cost | $0.17 | about $0.35–$0.85 | a few cents |
| **Naive per-row classification**, for comparison | about $10 | about $1,000 | about $10 a day |

The naive estimate uses the measured classifier cost per item in the agent run (about $0.0008 per
template and $0.0002 per label, so about $0.001 per row for both). Evals are excluded, since they are a
deliberate extra cost. It is optimistic, because a call per row also repeats the
prompt for every row. Latency matters as much as cost: at a measured p50 of about 36 seconds per call, a
million per-row calls is not a daily job at any reasonable concurrency.

Controls already in the code:

- the cache, keyed by prompt name, prompt version, model and payload;
- fresh evals bypass that cache and tag calls with a UUID, keeping measurement independent of historical
  spend and other running agents;
- `LLM_DAILY_BUDGET_USD`, which raises an alert when crossed;
- a cost-per-1k-rows metric that the promotion gate checks, so a "better" model that is 20% more expensive
  fails promotion unless someone makes a deliberate decision.

What I would add:

- **Tiered models.** A small model for labels and templates (high agreement with rules), and a reasoning
  model only for DQ rules and schema design, which run rarely.
- **Batch APIs** (OpenAI or Anthropic batch, about 50% cheaper) for the first backfill.
- **Embedding pre-clustering** of new vocabulary, so near-duplicates ("H.V.A.C", "hvac ") are sent once.

## 4. Governance at 100x

- **The approval queue becomes the bottleneck**, not the model. Today: auto-approve when risk is low and
  confidence ≥ 0.9, everything else goes to a human. At scale:
  - route proposals to owners by kind (taxonomy owner, data steward for PII tags, platform team for
    schema migrations);
  - put an SLA on review time;
  - send seed changes through pull requests (they are already YAML files with `_provenance`), so review
    happens in normal code review with CI running the evals.
- **Promotion gate in CI.** Any prompt or model change runs `make eval` against the golden set, and merge
  is blocked if a gated metric drops more than 0.01. Grow the golden set from human review decisions:
  every rejected proposal becomes a labeled negative.
- **PII.** Column tags from the tagging agent drive masking policies (Snowflake masking policies or
  Postgres column grants), and `assigned_to` / `submitted_by` are hashed in any gold view exposed outside
  operations.
- **Drift.** The schema/drift agent is already wired to propose migrations for new columns (the
  batch-2 `channel` demo). At 100x, a migration auto-applies only when the change is additive and nullable.
  Type changes or removed columns page a human and stop silver for that source. Bronze keeps landing
  (schema-on-read), so no data is lost while a human decides.

## 5. What I would *not* change

- Bronze stays schema-on-read with all-text columns and `_extra`. It is the cheapest insurance against
  upstream drift at any scale.
- Agents stay out of the critical path. A model outage at 3am must not stop the daily load. It only means
  new vocabulary lands as `Unclassified` with `category_source = 'unmapped'` until the next agent pass, and that
  is visible in the `unmapped_descriptions` alert.
