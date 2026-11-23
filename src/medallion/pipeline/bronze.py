import csv
import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import psycopg

from medallion import proposals
from medallion.agents import tagging
from medallion.config import ROOT, SOURCE_COLUMNS, Config
from medallion.db import connect
from medallion.log import event
from medallion.pipeline.observability import alert
from medallion.pipeline.parsers import row_hash
from medallion.pipeline.runs import stage_run
from medallion.seeds import Seeds, rules_version

LINEAGE_COLUMNS = [
    "_extra",
    "_source_file",
    "_source_file_sha256",
    "_row_number",
    "_row_hash",
    "_batch_id",
    "_ingested_at",
]


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_csv(path: Path) -> tuple[list[str], list[list[str]]]:
    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = [h.strip() for h in next(reader)]
        return header, list(reader)


def check_drift(conn: psycopg.Connection, run_id: uuid.UUID, header: list[str], source: str, cfg: Config) -> dict:
    """Deterministic drift detection against the registered header. Never blocks ingestion:
    unknown columns are kept in _extra and a high-risk migration proposal is opened for review."""
    fingerprint = hashlib.sha256("\x1f".join(header).encode()).hexdigest()[:16]
    new = [c for c in header if c not in SOURCE_COLUMNS]
    missing = [c for c in SOURCE_COLUMNS if c not in header]
    conn.execute(
        """INSERT INTO meta.schema_registry (table_name, columns, fingerprint)
           VALUES ('bronze.tickets_raw', %s, %s) ON CONFLICT DO NOTHING""",
        (header, fingerprint),
    )
    if new or missing:
        alert(conn, run_id, "warning", "schema_drift", f"{source}: new columns {new}, missing columns {missing}")
        for column in new:
            proposals.propose(
                conn,
                agent="drift_detector",
                kind="schema_migration",
                item_key=f"silver.tickets.{column}",
                payload={"column": column, "source_file": source, "status": "needs_type_and_mapping"},
                rationale=f"Column '{column}' appeared in {source}; stored in bronze._extra until mapped.",
                confidence=1.0,
                risk="high",
                auto_threshold=cfg.auto_approve_confidence,
            )
    return {"fingerprint": fingerprint, "new_columns": new, "missing_columns": missing}


def tag_columns(conn: psycopg.Connection, header: list[str], rows: list[list[str]]) -> None:
    """Tag every landed column for PII, sensitivity and domain. Approved agent tags win over rules."""
    approved = Seeds().column_tags
    for i, column in enumerate(header):
        if f"bronze.tickets_raw.{column}" in approved:
            tags, source = approved[f"bronze.tickets_raw.{column}"], "approved"
        else:
            tags, source = tagging.rule_tags(column, [r[i] for r in rows[:2000] if i < len(r)]), "rule"
        conn.execute(
            """INSERT INTO meta.column_tags (table_name, column_name, pii, pii_type, sensitivity, domain, source)
               VALUES ('bronze.tickets_raw', %s, %s, %s, %s, %s, %s)
               ON CONFLICT (table_name, column_name) DO UPDATE SET
                 pii=EXCLUDED.pii, pii_type=EXCLUDED.pii_type, sensitivity=EXCLUDED.sensitivity,
                 domain=EXCLUDED.domain, source=EXCLUDED.source, tagged_at=now()""",
            (column, tags["pii"], tags.get("pii_type"), tags["sensitivity"], tags["domain"], source),
        )


def ingest(cfg: Config, path: Path) -> None:
    path = path.resolve()
    source = str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else str(path)
    sha = file_sha256(path)
    with stage_run(cfg, "bronze", rules_version()) as run, connect(cfg) as conn:
        run.details.update(source_file=source, file_sha256=sha)
        # Serialize writers so _ingested_at follows commit order; silver's watermark relies on it.
        conn.execute("LOCK TABLE bronze.tickets_raw IN SHARE ROW EXCLUSIVE MODE")
        if conn.execute("SELECT 1 FROM meta.ingested_files WHERE file_sha256=%s", (sha,)).fetchone():
            run.skipped = True
            run.details["reason"] = "file content already ingested"
            return

        header, rows = read_csv(path)
        drift = check_drift(conn, run.run_id, header, source, cfg)
        batch_id, ingested_at = uuid.uuid4(), datetime.now(timezone.utc)
        positions = {c: header.index(c) for c in SOURCE_COLUMNS if c in header}
        extra_positions = {c: i for i, c in enumerate(header) if c not in SOURCE_COLUMNS}

        with conn.cursor().copy(
            f"COPY bronze.tickets_raw ({', '.join(SOURCE_COLUMNS + LINEAGE_COLUMNS)}) FROM STDIN"
        ) as copy:
            for line_no, values in enumerate(rows, start=2):  # line 1 is the header
                extra = {c: values[i] for c, i in extra_positions.items() if i < len(values)}
                if len(values) > len(header):
                    extra["_overflow"] = values[len(header) :]
                known = [
                    values[positions[c]] if c in positions and positions[c] < len(values) else None
                    for c in SOURCE_COLUMNS
                ]
                copy.write_row(
                    known
                    + [
                        json.dumps(extra) if extra else None,
                        source,
                        sha,
                        line_no,
                        row_hash(values),
                        batch_id,
                        ingested_at,
                    ]
                )

        conn.execute(
            """INSERT INTO meta.ingested_files (file_sha256, source_file, batch_id, row_count, header, run_id)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (sha, source, batch_id, len(rows), header, run.run_id),
        )
        tag_columns(conn, header, rows)
        conn.commit()

        run.rows_in = run.rows_out = len(rows)
        run.details.update(batch_id=str(batch_id), drift=drift)
        event("bronze_loaded", rows=len(rows), batch_id=batch_id, source_file=source)
