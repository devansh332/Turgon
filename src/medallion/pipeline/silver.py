"""Bronze -> silver: deterministic, rules-from-seeds only (silver never calls an LLM), incremental by
default, full rebuild when the approved rules change. See docs/cleaning_rules.md for the why of each rule."""

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import psycopg
from psycopg.types.json import Jsonb

from medallion.agents.sql_guard import UnsafeSQL, predicate_query, validate_select
from medallion.config import SOURCE_COLUMNS, Config
from medallion.db import connect, read_only
from medallion.log import event
from medallion.pipeline.observability import alert, record_silver_quality
from medallion.pipeline.parsers import (
    RESOLVED_OUTCOMES,
    description_template,
    extract_location,
    fold,
    parse_cost,
    parse_notes,
    parse_sla,
    parse_ticket_id,
    parse_ts,
    row_hash,
)
from medallion.pipeline.runs import stage_run
from medallion.seeds import Seeds, rules_version

TICKET_NUMBER_SQL = r"(substring(upper(btrim(ticket_id)) FROM '^TKT-(\d+)$')::numeric)"  # matches the bronze index

SILVER_COLUMNS = [
    "ticket_key",
    "ticket_number",
    "ticket_id_raw",
    "created_at",
    "resolved_at",
    "resolution_hours",
    "category_raw",
    "category",
    "category_source",
    "asset_type",
    "issue_type",
    "is_safety_issue",
    "urgency_inferred",
    "enrichment_source",
    "enrichment_confidence",
    "enrichment_prompt_version",
    "priority_raw",
    "priority",
    "priority_effective",
    "priority_source",
    "status_raw",
    "status",
    "is_open",
    "building",
    "location_floor",
    "location_room",
    "asset_id",
    "description",
    "description_template",
    "submitted_by_raw",
    "submitted_by",
    "submitter_resolution",
    "assignee_raw",
    "assignee",
    "assignee_type",
    "assignee_scope_match",
    "resolution_notes",
    "resolution_outcome",
    "root_cause",
    "replaced_part",
    "claimed_duplicate_of",
    "cost_raw",
    "cost",
    "sla_hours_raw",
    "sla_hours",
    "sla_hours_effective",
    "sla_source",
    "fields_swapped",
    "content_hash",
    "dq_flags",
    "_bronze_id",
    "_bronze_row_hash",
    "_source_file",
    "_source_versions",
    "_silver_run_id",
    "_rules_version",
]
_RANK = {"critical": 3, "high": 2, "medium": 1, "low": 0}
_COMPLETENESS_FIELDS = [
    "created_at",
    "resolved_at",
    "priority",
    "status",
    "building",
    "submitted_by",
    "assignee",
    "cost",
    "sla_hours",
    "resolution_notes",
]


@dataclass
class Context:
    seeds: Seeds

    def __post_init__(self):
        ref = self.seeds.ref
        self.null_tokens = set(ref["null_tokens"])
        self.date_null_tokens = self.null_tokens | set(ref["date_null_tokens"])
        self.cost_null_tokens = self.null_tokens | set(ref["cost_null_tokens"])
        self.junk_tokens = set(ref["junk_tokens"])
        self.buildings = ref["buildings"]
        self.building_by_fold = {fold(b): b for b in self.buildings}
        self.assignees = {fold(name): (name, spec) for name, spec in ref["assignees"].items()}
        self.open_statuses = set(ref["open_statuses"])
        self.min_date = datetime.fromisoformat(ref["min_plausible_date"])
        self.labels = self.seeds.category_labels
        self.templates = self.seeds.templates
        self.people = self.seeds.people

    def is_label(self, value: str | None) -> bool:
        entry = self.labels.get(fold(value))
        return bool(entry and entry.get("is_label") and entry.get("category"))


def _resolve_category(
    ctx: Context, category_raw: str | None, description: str | None, flags: list[str]
) -> tuple[str, str, dict | None, str | None]:
    """The label if it is a known label, else the description template, else Unclassified."""
    label = ctx.labels.get(fold(category_raw)) if ctx.is_label(category_raw) else None
    template_key = description_template(description, ctx.buildings) if (description or "").strip() else None
    template = ctx.templates.get(template_key) if template_key else None
    template_known = template is not None
    if template and not template.get("is_description"):
        template = None  # junk text or a bare category word: nothing to enrich
    if label:
        category, category_source = label["category"], "label"
        if template and template["category"] != category:
            flags.append("category:conflicts_with_description")
    elif template:
        category, category_source = template["category"], "description"
    else:
        category, category_source = "Unclassified", "unmapped"
    if (category_raw or "").strip() and not label:
        flags.append("category:unmapped_label")
    if template_key and not template_known:
        flags.append("description:unmapped_template")
    return category, category_source, template, template_key


def _resolve_building(ctx: Context, raw: str | None, described: str | None, flags: list[str]) -> str | None:
    b_fold = fold(raw)
    building = ctx.building_by_fold.get(b_fold)
    if building is None:
        if b_fold in ctx.junk_tokens:
            flags.append("building:junk")
        elif b_fold not in ctx.null_tokens:
            flags.append("building:unrecognized")
        if described:
            building = described
            flags.append("building:from_description")
    elif described and described != building:
        flags.append("building:conflicts_with_description")
    return building


def _resolve_priority(
    ctx: Context, raw: str | None, urgency: str | None, flags: list[str]
) -> tuple[str | None, str | None, str]:
    ref = ctx.seeds.ref
    p_fold = fold(raw)
    priority = ref["priority_map"].get(p_fold)
    if priority is None and p_fold not in ctx.null_tokens:
        flags.append("priority:unrecognized")
    if p_fold in ref["informal_priorities"]:
        flags.append("priority:informal")
    priority_effective = priority or urgency
    priority_source = "given" if priority else ("inferred" if urgency else "unknown")
    if priority and urgency and _RANK[urgency] - _RANK[priority] >= 2:
        flags.append("priority:understated_vs_description")
    return priority, priority_effective, priority_source


def _resolve_submitter(ctx: Context, raw: str | None, flags: list[str]) -> tuple[str | None, str]:
    sub_fold = fold(raw)
    person = ctx.people.get(sub_fold)
    if not sub_fold or sub_fold in ctx.null_tokens:
        submitted_by, resolution = None, "missing"
    elif sub_fold in ctx.junk_tokens:
        submitted_by, resolution = None, "system"
    elif person:
        submitted_by, resolution = person["canonical"], person["resolution"]
    else:
        submitted_by, resolution = raw.strip().title(), "unreviewed"
    if resolution == "ambiguous":
        flags.append("submitted_by:ambiguous")
    return submitted_by, resolution


def _resolve_assignee(
    ctx: Context, raw: str | None, category: str, flags: list[str]
) -> tuple[str | None, str | None, bool | None]:
    a_fold = fold(raw)
    assignee, assignee_type, scope_match = None, None, None
    if a_fold in ctx.assignees:
        assignee, spec = ctx.assignees[a_fold]
        assignee_type = spec["type"]
        if spec["scope"]:
            scope_match = category in spec["scope"]
            if not scope_match:
                flags.append("assignee:outside_scope")
    elif a_fold and a_fold not in ctx.null_tokens:
        flags.append("assignee:unrecognized")
    return assignee, assignee_type, scope_match


def _quarantine_reasons(
    ctx: Context, b: dict, created: datetime | None, flags: list[str], rule_flags: list[str]
) -> list[str]:
    reasons = []
    if sum(fold(b[c]) in ctx.junk_tokens for c in SOURCE_COLUMNS) >= ctx.seeds.ref["junk_threshold"]:
        reasons.append("junk_record")
    if created is None:
        reasons.append("created_at_" + next(f.split(":")[1] for f in flags if f.startswith("created_at:")))
    reasons += [f[len("quarantine:") :] for f in rule_flags if f.startswith("quarantine:")]
    return reasons


def transform(b: dict, ctx: Context, rule_flags: list[str] = ()) -> tuple[dict, list[str]]:
    """One bronze row -> (silver record, quarantine reasons). Pure apart from reading seeds."""
    ref = ctx.seeds.ref
    flags: list[str] = list(rule_flags)

    def add(column: str, new_flags, skip=("missing",)):
        flags.extend(f"{column}:{f}" for f in new_flags if f not in skip)

    key, number = parse_ticket_id(b["ticket_id"])
    if key is None:
        key = f"UNK-{b['_row_hash'][:12]}"
        flags.append("ticket_id:invalid")

    category_raw, description = b["category"], b["description"]
    swapped = bool((category_raw or "").strip()) and not ctx.is_label(category_raw) and ctx.is_label(description)
    if swapped:
        category_raw, description = description, category_raw
        flags.append("fields_swapped")

    max_date = b["_ingested_at"].astimezone(timezone.utc).replace(tzinfo=None)
    ts_kwargs = {"null_tokens": ctx.date_null_tokens, "min_date": ctx.min_date, "max_date": max_date}
    created, f = parse_ts(b["created_at"], **ts_kwargs)
    add("created_at", f, skip=())
    resolved, f = parse_ts(b["resolved_at"], **ts_kwargs)
    add("resolved_at", f)

    category, category_source, template, template_key = _resolve_category(ctx, category_raw, description, flags)
    loc = extract_location(description, ctx.buildings)
    building = _resolve_building(ctx, b["building"], loc["building"], flags)
    urgency = template.get("urgency") if template else None
    priority, priority_effective, priority_source = _resolve_priority(ctx, b["priority"], urgency, flags)

    s_fold = fold(b["status"])
    status = ref["status_map"].get(s_fold)
    if status is None:
        flags.append("status:missing" if s_fold in ctx.null_tokens else "status:unrecognized")
    is_open = (status in ctx.open_statuses) if status else None

    resolution_hours = None
    if created and resolved and resolved < created:
        flags.append("resolved_at:before_created")
    if is_open is False and resolved is None:
        flags.append("status:closed_without_resolved_at")
    if is_open and resolved:
        flags.append("status:open_with_resolved_at")
    if is_open is False and created and resolved and resolved >= created:
        resolution_hours = Decimal((resolved - created).total_seconds() / 3600).quantize(Decimal("0.01"))

    submitted_by, submitter_resolution = _resolve_submitter(ctx, b["submitted_by"], flags)
    assignee, assignee_type, scope_match = _resolve_assignee(ctx, b["assigned_to"], category, flags)

    notes = parse_notes(b["resolution_notes"])
    if notes["outcome"] in RESOLVED_OUTCOMES and is_open:
        flags.append("notes:resolved_but_status_open")

    cost, f = parse_cost(b["cost"], null_tokens=ctx.cost_null_tokens)
    add("cost", f, skip=("missing", "currency_symbol"))
    sla, f = parse_sla(b["sla_hours"], null_tokens=ctx.null_tokens, valid=set(ref["valid_sla_hours"]))
    add("sla_hours", f)
    policy_sla = ref["sla_policy_hours"].get(priority_effective)
    sla_effective = sla or policy_sla
    sla_source = "given" if sla else ("policy" if policy_sla else "unknown")

    reasons = _quarantine_reasons(ctx, b, created, flags, rule_flags)
    record = {
        "ticket_key": key,
        "ticket_number": number,
        "ticket_id_raw": b["ticket_id"],
        "created_at": created,
        "resolved_at": resolved,
        "resolution_hours": resolution_hours,
        "category_raw": category_raw,
        "category": category,
        "category_source": category_source,
        "asset_type": template.get("asset_type") if template else None,
        "issue_type": template.get("issue_type") if template else None,
        "is_safety_issue": template.get("is_safety_issue") if template else None,
        "urgency_inferred": urgency,
        "enrichment_source": "seed_template" if template else "unmapped",
        "enrichment_confidence": template.get("confidence") if template else None,
        "enrichment_prompt_version": (template.get("_provenance") or {}).get("prompt_version") if template else None,
        "priority_raw": b["priority"],
        "priority": priority,
        "priority_effective": priority_effective,
        "priority_source": priority_source,
        "status_raw": b["status"],
        "status": status,
        "is_open": is_open,
        "building": building,
        "location_floor": loc["floor"],
        "location_room": loc["room"],
        "asset_id": loc["asset_id"],
        "description": description,
        "description_template": template_key,
        "submitted_by_raw": b["submitted_by"],
        "submitted_by": submitted_by,
        "submitter_resolution": submitter_resolution,
        "assignee_raw": b["assigned_to"],
        "assignee": assignee,
        "assignee_type": assignee_type,
        "assignee_scope_match": scope_match,
        "resolution_notes": b["resolution_notes"],
        "resolution_outcome": notes["outcome"],
        "root_cause": notes["root_cause"],
        "replaced_part": notes["replaced_part"],
        "claimed_duplicate_of": notes["claimed_duplicate_of"],
        "cost_raw": b["cost"],
        "cost": cost,
        "sla_hours_raw": b["sla_hours"],
        "sla_hours": sla,
        "sla_hours_effective": sla_effective,
        "sla_source": sla_source,
        "fields_swapped": swapped,
        "content_hash": row_hash([b[c] for c in SOURCE_COLUMNS if c != "ticket_id"]),
        "dq_flags": sorted(set(f for f in flags if not f.startswith("quarantine:"))),
        "_bronze_id": b["_bronze_id"],
        "_bronze_row_hash": b["_row_hash"],
        "_source_file": b["_source_file"],
        "_ingested_at": b["_ingested_at"],
    }
    return record, reasons


def pick_winner(versions: list[dict], status_rank: dict) -> dict:
    """Same ticket id seen more than once: most progressed status, then most complete, then latest ingest."""
    return max(
        versions,
        key=lambda r: (
            status_rank.get(r["status"], 0),
            sum(r[f] is not None for f in _COMPLETENESS_FIELDS),
            r["_ingested_at"],
            r["_bronze_id"],
        ),
    )


def apply_dq_rules(cfg: Config, seeds: Seeds, run_id) -> dict[int, list[str]]:
    """Approved agent rules (seeds/dq_rules.yaml) run as validated, read-only predicates over bronze."""
    hits: dict[int, list[str]] = {}
    with connect(cfg) as ro:
        for rule_id, rule in sorted(seeds.dq_rules.items()):
            try:
                sql = predicate_query(rule["predicate"])
                with read_only(ro):
                    ids = [r["_bronze_id"] for r in ro.execute(sql).fetchall()]
            except (UnsafeSQL, psycopg.Error) as exc:
                event("dq_rule_skipped", rule_id=rule_id, error=str(exc), run_id=run_id)
                continue
            prefix = "quarantine:" if rule.get("action") == "quarantine" else ""
            for i in ids:
                hits.setdefault(i, []).append(f"{prefix}rule:{rule_id}")
    return hits


MIGRATION_TYPES = {"text", "integer", "bigint", "numeric", "boolean", "date", "timestamp", "timestamptz"}


def apply_schema_migrations(conn: psycopg.Connection, seeds: Seeds, run_id) -> list[str]:
    """Approved drift migrations (seeds/schema_migrations.yaml): add the column to silver and fill it from
    bronze `_extra`. The ALTER is rebuilt from the validated name and type rather than executed as written,
    and the extraction expression must pass the same SQL guard as DQ predicates."""
    applied = []
    for item_key, m in sorted(seeds.schema_migrations.items()):
        column, pg_type = m["silver_column"], str(m["pg_type"]).lower().strip()
        valid_name = re.fullmatch(r"[a-z][a-z0-9_]{1,62}", column) and column not in SILVER_COLUMNS
        if not valid_name or pg_type not in MIGRATION_TYPES:
            event("schema_migration_skipped", item=item_key, reason="invalid column or type", run_id=run_id)
            continue
        try:
            select = validate_select(f"SELECT _bronze_id, ({m['extract_sql']})::{pg_type} AS v FROM bronze.tickets_raw")
        except UnsafeSQL as exc:
            event("schema_migration_skipped", item=item_key, reason=str(exc), run_id=run_id)
            continue
        conn.execute(f"ALTER TABLE silver.tickets ADD COLUMN IF NOT EXISTS {column} {pg_type}")
        conn.execute(
            f"""UPDATE silver.tickets s SET {column} = x.v FROM ({select}) x
                WHERE x._bronze_id = s._bronze_id AND s.{column} IS DISTINCT FROM x.v"""
        )
        applied.append(column)
    return applied


def _scope(conn: psycopg.Connection, cfg: Config, rv: str, full: bool) -> tuple[list[dict], bool, datetime | None]:
    wm = conn.execute("SELECT * FROM meta.watermarks WHERE stage = 'silver'").fetchone()
    if full or wm is None or wm["rules_version"] != rv:
        return conn.execute("SELECT * FROM bronze.tickets_raw ORDER BY _bronze_id").fetchall(), True, None
    if not conn.execute(
        "SELECT 1 FROM bronze.tickets_raw WHERE _ingested_at > %s LIMIT 1", (wm["high_watermark"],)
    ).fetchone():
        return [], False, None
    # Late-arriving data: anything ingested inside the replay window is reprocessed with the new batch.
    since = wm["high_watermark"] - timedelta(hours=cfg.replay_window_hours)
    new = conn.execute("SELECT * FROM bronze.tickets_raw WHERE _ingested_at > %s", (since,)).fetchall()
    # match on the normalised number, as silver keys are (TKT-0123 and tkt-123 are the same ticket)
    numbers = sorted({n for r in new if (n := parse_ticket_id(r["ticket_id"])[1]) is not None})
    related = (
        conn.execute(
            f"SELECT * FROM bronze.tickets_raw WHERE {TICKET_NUMBER_SQL} = ANY(%s::numeric[])", (numbers,)
        ).fetchall()
        if numbers
        else []
    )
    rows = {r["_bronze_id"]: r for r in new + related}
    return [rows[k] for k in sorted(rows)], False, since


def _copy(conn: psycopg.Connection, table: str, columns: list[str], rows: list[list]) -> None:
    with conn.cursor().copy(f"COPY {table} ({', '.join(columns)}) FROM STDIN") as copy:
        for row in rows:
            copy.write_row(row)


def build(cfg: Config, full: bool = False) -> None:
    seeds, rv = Seeds(), rules_version()
    with stage_run(cfg, "silver", rv) as run, connect(cfg) as conn:
        # Keep the new-row and related-version queries on the same snapshot during overlapping ingestion.
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        bronze_rows, full, since = _scope(conn, cfg, rv, full)
        run.details.update(mode="full" if full else "incremental", replay_from=since)
        if not bronze_rows:
            run.skipped = True
            run.details["reason"] = "no new bronze rows"
            return

        ctx = Context(seeds)
        rule_hits = apply_dq_rules(cfg, seeds, run.run_id)
        by_key: dict[str, list[dict]] = {}
        quarantined: list[tuple[dict, str, list[str]]] = []
        for b in bronze_rows:
            record, reasons = transform(b, ctx, rule_hits.get(b["_bronze_id"], []))
            if reasons:
                quarantined.append((b, record["ticket_key"], reasons))
            else:
                by_key.setdefault(record["ticket_key"], []).append(record)

        stage = []
        for versions in by_key.values():
            winner = dict(pick_winner(versions, seeds.ref["status_rank"]))
            winner.update(_source_versions=len(versions), _silver_run_id=run.run_id, _rules_version=rv)
            stage.append([winner[c] for c in SILVER_COLUMNS])

        conn.execute("CREATE TEMP TABLE stage_silver (LIKE silver.tickets INCLUDING DEFAULTS) ON COMMIT DROP")
        _copy(conn, "stage_silver", SILVER_COLUMNS, stage)
        changed = (
            "(t._bronze_row_hash, t._rules_version, t._source_versions) IS DISTINCT FROM "
            "(s._bronze_row_hash, s._rules_version, s._source_versions)"
        )
        counts = conn.execute(
            f"""SELECT count(*) FILTER (WHERE t.ticket_key IS NULL) AS inserted,
                       count(*) FILTER (WHERE t.ticket_key IS NOT NULL AND {changed}) AS updated,
                       count(*) FILTER (WHERE t.ticket_key IS NOT NULL AND NOT {changed}) AS unchanged
                FROM stage_silver s LEFT JOIN silver.tickets t USING (ticket_key)"""
        ).fetchone()
        assignments = ", ".join(f"{c} = s.{c}" for c in SILVER_COLUMNS if c != "ticket_key")
        conn.execute(
            f"""MERGE INTO silver.tickets t USING stage_silver s ON t.ticket_key = s.ticket_key
                WHEN MATCHED AND {changed} THEN UPDATE SET {assignments}, _updated_at = now()
                WHEN NOT MATCHED THEN INSERT ({", ".join(SILVER_COLUMNS)})
                     VALUES ({", ".join("s." + c for c in SILVER_COLUMNS)})"""
        )

        scope_ids = [b["_bronze_id"] for b in bronze_rows]
        q_ids = [b["_bronze_id"] for b, _, _ in quarantined]
        quarantine_keys = {k for _, k, _ in quarantined}
        if full:
            removed = conn.execute(
                "DELETE FROM silver.tickets t WHERE NOT EXISTS (SELECT 1 FROM stage_silver s "
                "WHERE s.ticket_key = t.ticket_key)"
            ).rowcount
        else:
            removed = conn.execute(
                "DELETE FROM silver.tickets WHERE ticket_key = ANY(%s) AND ticket_key NOT IN "
                "(SELECT ticket_key FROM stage_silver)",
                (list(quarantine_keys),),
            ).rowcount
        conn.execute(
            "DELETE FROM quarantine.tickets_rejected WHERE _bronze_id = ANY(%s) AND NOT (_bronze_id = ANY(%s))",
            (scope_ids, q_ids),
        )
        for b, key, reasons in quarantined:
            raw = {c: b[c] for c in SOURCE_COLUMNS} | {
                "_source_file": b["_source_file"],
                "_row_number": b["_row_number"],
            }
            conn.execute(
                """INSERT INTO quarantine.tickets_rejected (_bronze_id, ticket_key, reasons, raw, _silver_run_id,
                     _rules_version) VALUES (%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (_bronze_id) DO UPDATE SET reasons = EXCLUDED.reasons,
                     _silver_run_id = EXCLUDED._silver_run_id, _rules_version = EXCLUDED._rules_version
                   WHERE quarantine.tickets_rejected.reasons IS DISTINCT FROM EXCLUDED.reasons
                      OR quarantine.tickets_rejected._rules_version IS DISTINCT FROM EXCLUDED._rules_version""",
                (b["_bronze_id"], key, reasons, Jsonb(raw), run.run_id, rv),
            )

        migrated = apply_schema_migrations(conn, seeds, run.run_id)

        # Identical content under different ids: keep the lowest ticket number, link the rest.
        linked = conn.execute(
            """WITH ranked AS (
                 SELECT ticket_key, first_value(ticket_key) OVER (
                   PARTITION BY content_hash ORDER BY ticket_number NULLS LAST, ticket_key) AS keep
                 FROM silver.tickets)
               UPDATE silver.tickets t SET duplicate_of_key = NULLIF(r.keep, t.ticket_key)
               FROM ranked r WHERE r.ticket_key = t.ticket_key
                 AND t.duplicate_of_key IS DISTINCT FROM NULLIF(r.keep, t.ticket_key)"""
        ).rowcount

        # A new batch may commit while we transform. Only acknowledge rows actually read by this build.
        high = max(b["_ingested_at"] for b in bronze_rows)
        conn.execute(
            """INSERT INTO meta.watermarks (stage, high_watermark, rules_version) VALUES ('silver', %s, %s)
               ON CONFLICT (stage) DO UPDATE SET high_watermark = EXCLUDED.high_watermark,
                 rules_version = EXCLUDED.rules_version, updated_at = now()""",
            (high, rv),
        )
        totals = conn.execute(
            """SELECT (SELECT count(*) FROM bronze.tickets_raw) AS bronze,
                      (SELECT count(*) FROM quarantine.tickets_rejected) AS quarantined"""
        ).fetchone()
        record_silver_quality(conn, run.run_id, totals["bronze"], totals["quarantined"])
        unmapped = conn.execute(
            "SELECT count(*) AS n FROM silver.tickets WHERE 'description:unmapped_template' = ANY(dq_flags)"
        ).fetchone()["n"]
        if unmapped:
            alert(
                conn,
                run.run_id,
                "warning",
                "unmapped_descriptions",
                f"{unmapped} tickets use description templates with no approved enrichment; run `make agents`",
                unmapped,
            )
        conn.commit()

        run.rows_in, run.rows_out, run.rows_rejected = len(bronze_rows), len(stage), len(quarantined)
        run.details.update(
            **counts,
            removed=removed,
            content_duplicates_relinked=linked,
            migrated_columns=migrated,
            dq_rule_hits=sum(len(v) for v in rule_hits.values()),
        )
