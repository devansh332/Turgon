# Agent assessment: full outputs, review decisions and time log

The README has the summary and my verdicts. This file has the detail behind them. Every proposal and
every LLM call from the live run is exported to:

- [artifacts/agent_run/proposals.csv](../artifacts/agent_run/proposals.csv): 260 proposals with payload,
  rationale, confidence, risk, status and approver;
- [artifacts/agent_run/llm_calls.csv](../artifacts/agent_run/llm_calls.csv): 68 calls with model, tokens,
  cost, latency, attempts and errors. The first 17 rows are the earlier local-model attempts.

## How the prompts are built

Every prompt is a versioned Markdown file in `src/medallion/agents/prompts/` with the same sections:

- **Context.** What the data is, how it was pre-processed (for example, templates with `{n}` and
  `{building}`), and why mistakes are expensive ("your answer is applied to every ticket that uses it").
- **Task.** Field-by-field definitions with decision rules, such as what counts as a safety issue and
  what separates `high` from `critical` urgency.
- **Rules.** Guardrails: taxonomy values only, judge the actual problem rather than words like "asap",
  and return every item exactly once.
- **Examples.** Three or four worked cases, including a negative one. Golden items quoted here are
  excluded from the held-out eval scores.

Output is constrained in code, not only in words:

- JSON mode with a pydantic schema whose enums come from `seeds/taxonomy.yaml`;
- consistency validators;
- the SQL guard for any SQL the model writes, which allows `SELECT` over `bronze.tickets_raw` only and an
  allowlist of functions;
- execution as the `medallion_agent` role, which can read only bronze and write only to
  `silver_proposed`.

Each agent receives evidence rather than raw rows: the profile (null rates, top values, shapes), counts,
and one real example per item.

## Classifier

### Category labels (99 proposals, $0.024)

| Input (raw `category`) | Output | Confidence | Gate |
|---|---|---|---|
| `power issue` | Electrical | 0.90 | auto-approved |
| `vertical transport` | Elevator | 0.95 | auto-approved |
| `Exterminator` | Pest Control | 0.95 | auto-approved |
| `???` | not a label | 0.99 | auto-approved |

Four labels were below 0.9 and went to review; I approved all four. The held-out score tied the rules
baseline (0.96 is-label, 1.00 category).

### Description templates (98 proposals, $0.077)

| Template | Output |
|---|---|
| `breaker keeps tripping in server room {n}. critical — affects production systems.` | Electrical / electrical_panel / outage / **critical** (high risk because of critical; approved) |
| `a/c leaking water onto the carpet near cubicle {n}. {building}.` | HVAC / hvac_unit / leak / high / safety true (approved) |
| `conference room {n} needs deep cleaning before client visit tomorrow.` | Cleaning / high. **Rejected:** a scheduling preference, not same-day urgency. It had been auto-approved at 0.90; I revoked it, so its 166 tickets are now unmapped |
| `.` | not a description (auto-approved) |

What went wrong and how it was fixed:

- **Contradictions.** About 20 templates came back as `is_description=false` while still carrying a
  category, and 3 of those had been auto-approved. The `model_validator`s on the output schema now reject
  this. The repair retry then fixed it; for example, "wifi password not working" was corrected on the
  second attempt.
- **Dropped items.** In 3 of 8 batches the model returned fewer items than it was sent. The client
  re-asks once for the missing ones.

### People (33 proposals, $0.009)

| Input | Output | Decision |
|---|---|---|
| `jane doe` | Jane Doe (exact) | auto-approved |
| `j. doe` | Jane Doe (initials) | approved: only one Doe in the data |
| `bob martinez` | Robert Martinez (nickname, 0.7) | **rejected**: no evidence they are the same person |
| `mike johnson` | Michael Johnson (nickname, 0.7) | **rejected**: same reason |
| `m. johnson` | ambiguous | approved as ambiguous: silver flags `submitted_by:ambiguous` |
| `admin`, `system`, `test` | system account (0.99) | approved |

## Data quality agent (13 rules, $0.017)

| Rule | Predicate | Rows | Decision |
|---|---|---|---|
| `assigned_to_missing` | `btrim(assigned_to) = ''` | 3,122 | **approved** |
| `resolution_notes_uninformative_placeholders` | `btrim(lower(resolution_notes)) IN ('fixed','done')` | 1,576 | **approved** |
| `ticket_id_invalid_or_placeholder` | blank, `N/A`, `NULL`, or not `AAA-1234` | 26 | rejected: silver already quarantines these |
| `category_synonyms_and_casing` | a list of variant spellings | 3,271 | rejected: the classifier handles it, and a fixed list does not generalise |
| `resolved_before_created` | both dates match `^\d{4}-\d{2}-\d{2}` and `resolved_at::timestamp < created_at::timestamp` | — | rejected: a good cross-column rule, but silver already flags it (and parses all eight formats, not only ISO) |
| `cost_sentinel_and_non_numeric_values` | blank, `N/A`, `TBD`, `error`, `NULL`, `-1`, `-999`, or not numeric | — | rejected: the cost parser already does this |
| 7 others | | | rejected: duplicates of silver rules, or flags with no downstream use |

Scored against my 25 golden issues, the agent-run rules give recall 0.64 (16 of 25), strict recall 0.28,
and every predicate executed. "Strict" needs the exact (column, issue type) pair. The default accepts
the equivalents listed per issue in `evals/golden/dq_issues.csv`, because the model's issue type is often a
fair synonym (`sentinel_values` for `-1`). The misses are: swapped fields, junk values in `building`,
`category` and `submitted_by`, mixed formats and placeholders in `resolved_at`, `$`-prefixed cost,
blank priority, and the free-text description.

Prompt v1 quoted this file's cost placeholders as its example, which leaked a golden answer. Prompt v2
uses an example from a different domain. On the eval, v2 scores recall 0.56 and strict recall 0.44
(`evals/results/latest.json`). That run was not promoted; see the gate note in the README §4.

## Schema and drift agent ($0.034)

- **Proposal 663 (rejected).** The DDL was valid. The transform failed with
  `date/time field value out of range: "03-20-2024 20:39:48"` because it parsed dash dates as day-first.
- **Proposal 665 (rejected).** The DDL was valid, but the transform SQL could not be parsed
  (`Error tokenizing '   END) AS cost'`).
- **Proposals 664 and 666 (approved).** Drift on `channel`. The detector raised 664 when the column
  appeared. The agent then proposed 666: a `text` column with a guarded regex extract from `_extra`,
  with confidence 0.8 and the samples `email`, `phone` and `portal`. Silver applied it and rebuilt the
  ALTER from the allowlisted type.

The schema agent's output for the full silver table is in `artifacts/schema_agent/`.

## Tagging agent (13 columns, $0.007)

| Column | Tag | Confidence | Decision |
|---|---|---|---|
| `submitted_by` | PII, person_name | 0.99 | auto-approved |
| `assigned_to` | PII, free_text_may_contain_pii ("Joe (in-house)") | 0.90 | auto-approved |
| `description` | PII, free_text_may_contain_pii | 0.75 | approved |
| `resolution_notes` | PII, free_text_may_contain_pii | 0.75 | approved |
| `building` | not PII, domain location | 0.95 | auto-approved |
| `cost` / `sla_hours` and 7 others | not PII, domain finance / operations | 0.90 | auto-approved |

The tags drive the `medallion_bi` column grants on every gold object.

## Time log

The work was done with an AI coding assistant (Cursor) writing most of the code. The phases, in order:

1. Profiling the CSV with SQL and writing the cleaning rules with evidence counts.
2. Bronze, silver and gold, reconciliation, and rerun safety.
3. The LLM client, the four agents and their prompts.
4. The approval gate, seeds with provenance, golden sets, evals and the promotion gate.
5. Live agent runs: first on a local model (too slow), then on gpt-5-mini; review of 260 proposals.
6. Hardening found by self-review: the SQL guard and sandbox role, validators, revoke, the item cache,
   drift migrations, and BI grants.
7. Documentation, with every number checked against the database.

Total: **about 3 hours** of my time.
