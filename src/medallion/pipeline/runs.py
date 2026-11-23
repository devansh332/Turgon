import json
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field

from psycopg.types.json import Jsonb

from medallion.config import ROOT, Config
from medallion.db import connect
from medallion.log import event


def git_sha() -> str:
    """Read HEAD without needing the git binary (the container doesn't ship one)."""
    head = ROOT / ".git" / "HEAD"
    if not head.exists():
        return "unknown"
    ref = head.read_text().strip()
    if not ref.startswith("ref: "):
        return ref[:12]
    ref_path = ROOT / ".git" / ref[5:]
    if ref_path.exists():
        return ref_path.read_text().strip()[:12]
    packed = ROOT / ".git" / "packed-refs"
    if packed.exists():
        for line in packed.read_text().splitlines():
            if line.endswith(ref[5:]):
                return line.split()[0][:12]
    return "uncommitted"


@dataclass
class Run:
    run_id: uuid.UUID
    stage: str
    rows_in: int = 0
    rows_out: int = 0
    rows_rejected: int = 0
    skipped: bool = False
    details: dict = field(default_factory=dict)


@contextmanager
def stage_run(cfg: Config, stage: str, rules_version: str | None = None, prompt_versions: dict | None = None):
    """Record one pipeline stage in meta.pipeline_runs on its own autocommit connection,
    so a failed stage still leaves a 'failed' row behind after its data transaction rolls back."""
    run = Run(uuid.uuid4(), stage)
    started = time.monotonic()
    with connect(cfg, autocommit=True) as log_conn:
        log_conn.execute(
            """INSERT INTO meta.pipeline_runs (run_id, stage, status, git_sha, rules_version, prompt_versions)
               VALUES (%s, %s, 'running', %s, %s, %s)""",
            (run.run_id, stage, git_sha(), rules_version, Jsonb(prompt_versions or {})),
        )
        event("stage_started", stage=stage, run_id=run.run_id)
        status, error = "succeeded", None
        try:
            yield run
            if run.skipped:
                status = "skipped"
        except Exception as exc:
            status, error = "failed", f"{type(exc).__name__}: {exc}"
            raise
        finally:
            details = {**run.details, **({"error": error} if error else {})}
            log_conn.execute(
                """UPDATE meta.pipeline_runs
                   SET status=%s, finished_at=now(), rows_in=%s, rows_out=%s, rows_rejected=%s, details=%s
                   WHERE run_id=%s""",
                (
                    status,
                    run.rows_in,
                    run.rows_out,
                    run.rows_rejected,
                    Jsonb(details, dumps=lambda o: json.dumps(o, default=str)),
                    run.run_id,
                ),
            )
            event(
                "stage_finished",
                stage=stage,
                run_id=run.run_id,
                status=status,
                seconds=round(time.monotonic() - started, 2),
                rows_in=run.rows_in,
                rows_out=run.rows_out,
                rows_rejected=run.rows_rejected,
                **({"error": error} if error else {}),
                details=json.loads(json.dumps(run.details, default=str)),
            )
