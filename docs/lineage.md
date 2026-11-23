# Lineage: one ticket from a gold number back to the CSV line

Every layer records *what* produced a row, *when*, from *which input*, and under *which rules*. Agents
appear as a separate thread: the approved proposal, with its model, prompt version and approver. Below is a
real trace of **TKT-5528** after `make run` and `make demo-incremental`. The second batch carried a late
"Closed" update for this ticket.

`make lineage KEY=TKT-5528` prints all of it, from gold down to the CSV lines:

```
== TKT-5528
  gold.ticket_facts             Electrical | low | Annex West | CityWide Electric | sla_breached=False | as_of 2025-07-29 19:20:20
  gold.sla_compliance           2025-02-01 / Electrical / low: 13 tickets, 11 breached
  gold.vendor_performance       CityWide Electric / Electrical: 109 tickets, cost 400463.43
  gold.backlog_daily            Annex West: in open backlog 2025-02-17 to 2025-02-17
  built by gold run b86cc1dd-... at 2026-11-23 11:16:11 (git 852fe60f6c0e-dirty, rules f08e80ac0830) from sql/gold/*.sql

  silver.category_raw           power issue
  silver.category               Electrical
  silver.category_source        label
  silver.priority_raw           Low
  silver.priority_effective     low
  silver.status                 Closed
  silver.created_at             2025-02-17 12:41:38
  silver.resolved_at            2025-02-18 09:00:00
  silver.resolution_hours       20.31
  silver.sla_hours_effective    48
  silver.sla_source             given
  silver.cost                   250.00
  silver.duplicate_of_key       None
  silver.dq_flags               []

  last changed by silver run 08b76f90-... at 2026-11-23 11:16:10 (git 852fe60f6c0e-dirty, rules f08e80ac0830)
  category label rule: {"agent": "classifier", "approved_by": "auto", "confidence": 0.9, "model": "gpt-5-mini",
                        "prompt_version": "v1", "proposal_id": 337}
  description template rule: {"agent": "classifier", "approved_by": "auto", "confidence": 0.9, "model": "gpt-5-mini",
                        "prompt_version": "v1", "proposal_id": 601}

  bronze versions (winner marked *):
    bronze_id=14 data/raw_tickets.csv:15 ingested=2026-11-23 11:15:49 batch=1b26f682 status='Open' hash=9f838cda43
  * bronze_id=10281 tests/fixtures/batch2_drift.csv:2 ingested=2026-11-23 11:16:10 batch=170a0084 status='Closed' hash=c39c0c86c2
```

## 1. Gold: the number a stakeholder sees

In February 2025, Electrical, low priority, 13 tickets: 12 have a measurable SLA, 11 breached it, and
TKT-5528 is the one that met it. Each mart line above is the mart row this ticket is counted in. Gold is
rebuilt from silver on every run by the SQL in `sql/gold/`, so the gold run (with its git sha and rules
version) plus the silver lineage below fully explains a gold number.

## 2. Silver: which rules, which run, which agent decisions

- **Who and what:** the silver run id links to `meta.pipeline_runs`, which records the git sha,
  `rules_version` (a hash of every seed file and of the silver transform code), the row counts, and
  inserted/updated/unchanged counts.
- **Why "Electrical":** the raw label `power issue` maps through `seeds/category_labels.yaml`. Its
  `_provenance` names proposal #337: the classifier agent on gpt-5-mini with prompt v1, auto-approved
  because it was low risk at confidence 0.9. `meta.agent_proposals` keeps the rationale.
- **Why this version won:** two bronze rows share the id. The rule is most progressed status, then the
  most complete row, then the latest ingest, so the batch-2 "Closed" row replaces the original "Open" row.
  `_source_versions = 2`.
- **Agent-added column:** TKT-5528 also carries `channel = 'phone'`. That column exists because drift
  migration proposal #666 (schema agent) was approved by a human; see `seeds/schema_migrations.yaml`.

## 3. Bronze: the exact input

```
data/raw_tickets.csv line 15
TKT-5528,2025-02-17T12:41:38,,power issue,Low,Open,Annex West,Power outlet not working at desk 392. ...,
         David Kim,Joe (in-house),Temporary fix applied. Permanent repair scheduled.,5877,48

tests/fixtures/batch2_drift.csv line 2   (header has an extra column: channel)
TKT-5528,2025-02-17T12:41:38,2025-02-18T09:00:00,power issue,Low,Closed,Annex West,...,
         David Kim,CityWide Electric,Resolved. Root cause: faulty wiring.,250.00,48,phone
```

Each bronze row stores the following:

- `_source_file`, `_source_file_sha256` and `_row_number` (the CSV line);
- `_row_hash` (sha256 of the raw values);
- `_ingested_at` and `_batch_id`, which joins to `meta.ingested_files` (header, row count, run id).

Unknown columns land in `_extra jsonb`, so `channel` was stored before any migration existed.

## 4. Rows that never reach silver

Quarantined rows keep the same trail. `quarantine.tickets_rejected` holds the `_bronze_id`, `reasons[]`,
the raw row with file and line, and the silver run and rules version that rejected it. Reconciliation
check 1 proves that bronze rows equal quarantined rows plus the silver versions they were folded into.
