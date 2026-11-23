# Silver cleaning rules, and why each one exists

All counts are from the supplied `data/raw_tickets.csv` (10,280 rows), as measured by the pipeline's run
report (`make metrics`).

Three principles apply throughout:

1. **Never overwrite raw values.** Silver keeps `*_raw` columns next to the typed ones.
2. **Never drop silently.** A row is either in silver (with `dq_flags` explaining what was fixed or
   suspicious) or in `quarantine.tickets_rejected` (with a `reasons[]` array).
3. **Rules live in reviewed seed files**, not in prompts or code constants:
   - [seeds/reference.yaml](../seeds/reference.yaml) holds human-owned reference data.
   - The agent-owned seed files are written only through the approval gate.

## Identity and duplicates

| Rule | Evidence | Why |
|---|---|---|
| `ticket_id` must match `TKT-<n>`; otherwise the surrogate key is `UNK-<first 12 chars of row hash>`, flagged `ticket_id:invalid` | 22 rows hold `N/A`, `NULL` or blank, and 4 hold bare numbers (`15`, `90`). **All 26 also have a placeholder or missing `created_at`, so all of them end up in quarantine.** | They are clearly broken records. The surrogate-key path is still there so that a future row with a bad id but a good date still counts in gold. A hash-based surrogate is stable across reruns. |
| No real repeated ticket ids were found. If one appears, a single winner is kept: most progressed status, then most complete row, then latest ingest | Profiling found no repeated `TKT-` id. The batch-2 fixture shows the rule working. | A late "Closed" update must win over an earlier "Open" row. The trade-off is that a genuine reopen loses to an earlier close; the source has no `updated_at` to tell them apart. |
| Content duplicates (identical in every field except the id) are kept but linked through `duplicate_of_key` and excluded from gold | `TKT-11240` and `TKT-11251` are byte-identical apart from the id | It is the same request submitted twice. Counting it twice inflates volume and backlog. |
| **"Duplicate of ticket #N" notes are not used for duplicate removal.** They are recorded in `claimed_duplicate_of` only. | 823 rows carry such a note, but only 56 point at a ticket in the same building, and spot checks point at unrelated issues (a smoke detector "duplicates" a badge reader) | Removing duplicates on an unverified claim would delete about 8% of real tickets. |

## Timestamps

| Rule | Evidence | Why |
|---|---|---|
| Eight formats are parsed explicitly: ISO with `T`, ISO with a space, ISO date, `MM/DD/YYYY hh:mm AM`, `MM/DD/YYYY`, `MM-DD-YYYY HH:MM:SS`, `DD-Mon-YYYY HH:MM`, and Unix epoch seconds | Shape profile of `created_at` | Explicit formats are deterministic. A "smart" parser would guess differently for different rows. |
| **Slash and dash numeric dates are month-first** | Across `created_at` and `resolved_at`, 2,182 slash dates and 637 dash dates have a value above 12 in the second position. None has one in the first. | The data proves the convention. The 1,121 `created_at` values where both readings are valid are parsed month-first and flagged `ambiguous_mdy`. |
| Placeholders (`asap`, `TBD`, `not a date`, `00/00/0000`, `???`, `never`, `pending`) become NULL | | They are not dates. |
| Timestamps before 2023-01-01, or after the moment the file was ingested, become NULL and are flagged `out_of_range` | None occur in this file. The guard exists for future batches. | A ticket cannot be created after we received it. |
| **A row without a usable `created_at` is quarantined** | 30 rows | Every gold mart is time-based (SLA clock, monthly trend, daily backlog). A ticket that cannot be placed in time would be silently wrong everywhere. |
| Date-only values are flagged `date_only` | 1,956 `created_at` values | Midnight is an assumption, and it distorts resolution hours for those rows. |
| No time zone is applied; values are stored as naive `timestamp` | The source carries no zone, and epochs are converted from UTC | Being explicit beats a fake precision. |

## Lifecycle consistency

| Rule | Count | Why |
|---|---|---|
| `resolved_at` earlier than `created_at`: keep both values, flag `resolved_at:before_created`, set `resolution_hours` to NULL | 1,866 | Which value is wrong is unknowable, but a negative duration must not reach the SLA mart. |
| "Open" or "In Progress" with a `resolved_at`: treated as open, flagged | 2,127 | Status is the current state. The date may be stale from a reopen. |
| "Resolved" or "Closed" with no `resolved_at`: flagged, excluded from duration metrics and the backlog | 100 | The close date is unknown. |
| Notes say fixed or done while the status is open: flagged `notes:resolved_but_status_open` | 3,187 | This is a process-quality signal for operations, not something to "fix". |
| `resolution_hours` is computed only when status is closed and the dates are consistent | | There is one definition, used by every mart. |

## Categorical fields

| Field | Rule | Why |
|---|---|---|
| `category` | About 99 distinct lower-cased values map to 10 taxonomy categories through `seeds/category_labels.yaml` (proposed by the classifier agent, then reviewed). Values that are descriptions or junk are `is_label: false`. | There are 115 raw spellings. Grouping by raw value would split HVAC across 9 buckets. |
| Swapped fields | If `category` is not a known label but `description` is, the two are swapped back and flagged `fields_swapped` | 46 rows, for example category `Wasp nest forming on HQ Floor 3 exterior…` with description `Pest` |
| Category fallback | If no usable label exists, the category comes from the description template. If neither exists, it is `Unclassified`. `category_source` records which path was used. | Within each template, the raw labels agree with the description 100% of the time once normalized, so the template is a safe fallback. |
| `priority` | 22 spellings map to critical, high, medium or low. `Normal` becomes medium. `urgent!!!` and `ASAP` become high, flagged `priority:informal`. Blank stays NULL. | `priority_effective` falls back to the urgency inferred from the description. `priority_source` records given, inferred or unknown. |
| Understated priority | A description urgency at least two levels above the stated priority is flagged | For example, "Breaker keeps tripping in server room. Critical, affects production" filed as `low`. |
| `status` | 6 valid values. `NULL`, `unknown` and `???` become NULL, flagged | |
| `building` | Matched case-insensitively against the 17 known buildings. If the column is missing, the building is taken from the description and flagged `building:from_description`. `test building` is treated as junk. | |
| `submitted_by` | Case and spacing variants merge (rule). An initial plus surname merges only when exactly one full name matches (`J. Doe` becomes Jane Doe). `M. Johnson` (Mike or Michael?) stays unresolved and flagged `submitted_by:ambiguous`. `R. Martinez` merges into Robert Martinez, the only full name with that initial; `Bob Martinez` stays separate, because the nickname merge was rejected. `admin`, `system` and `test` become NULL. | Merging two people is destructive. Nickname merges are proposed by the agent as high-risk and wait for a human. |
| `assigned_to` | Seven canonical assignees, typed vendor or in-house. Specialists have a declared scope (PestPro: Pest Control), and work outside that scope is flagged `assignee:outside_scope`. | 2,483 of the 3,033 specialist tickets (82%) are flagged. Assignment is statistically independent of category in this extract, which is itself a finding worth raising. |

## Numbers

| Field | Rule | Count |
|---|---|---|
| `cost` | `$` and commas are stripped. `N/A`, `TBD`, `NULL` and `error` become NULL (placeholder). `-1` becomes NULL (sentinel). Other negatives become NULL. `0` is kept but flagged `cost:zero`, because in-house work legitimately costs nothing. | 978 placeholders, 192 sentinels, 295 zeros |
| `sla_hours` | Valid values are {4, 8, 24, 48, 72}. `999`, `0` and `-1` are sentinels. `N/A` and `NULL` are placeholders. | 2,073 nulled |
| `sla_hours_effective` | The given SLA, or else the policy SLA for `priority_effective` (critical 4h, high 8h, medium 24h, low 72h), with `sla_source` recording given, policy or unknown | The supplied SLA does not correlate with priority at all in this data, so the policy fill is a **business assumption** recorded per row and reported in gold (`sla_from_policy`), not a fact. |

## Free text, made into columns

| Output | Method | Why not an LLM per row |
|---|---|---|
| `location_floor`, `location_room`, `asset_id`, building fallback | Regex, with building names removed first so "HQ Floor 2" never reads as floor 2 | Exact, free and testable |
| `asset_type`, `issue_type`, `is_safety_issue`, `urgency_inferred` | The classifier agent labels each **template** once; silver joins on `description_template` | 10k rows reduce to 98 templates (30 of them junk or bare category words) |
| `resolution_outcome`, `root_cause`, `replaced_part`, `claimed_duplicate_of` | Nine regexes over notes | Notes are templated too. Regex is 100% here, so an LLM would add cost and no accuracy. |

## Junk records

A row is quarantined as `junk_record` when three or more fields hold test markers (`test`, `asdf`,
`DELETE ME`, `test building`, `admin`, `system`, …). That catches 8 rows. Requiring three fields rather than
one is deliberate: a real ticket submitted by the `system` account should not disappear.

## Agent-proposed rules

The data-quality agent proposes extra rules as SQL predicates over bronze. Once approved, they are applied
in silver as `rule:<rule_id>` flags, or as quarantine reasons when the action is `quarantine`. Every
predicate is checked twice:

- re-validated by `sql_guard`, which allows a single SELECT over bronze only, with no side-effecting
  functions;
- run in a READ ONLY transaction with a timeout.
