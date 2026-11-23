"""Human-in-the-loop gate between agents and silver.

Agents never write to silver. They write proposals; low-risk, high-confidence ones are auto-approved,
everything else waits for `proposals approve`. Approved proposals are exported to seeds/, which is
what silver reads, so every applied change is reviewable in git and traceable to a proposal id.
"""

import hashlib
import json

import psycopg
from psycopg.types.json import Jsonb

from medallion import seeds
from medallion.log import event

# Evidence that changes between runs without changing the decision itself.
EVIDENCE_KEYS = {"confidence", "affected_rows", "samples", "sql_valid", "safety_rule", "_provenance"}


def decision_of(payload: dict) -> dict:
    return {k: v for k, v in payload.items() if k not in EVIDENCE_KEYS}


def _hash(payload: dict) -> str:
    return hashlib.sha256(json.dumps(decision_of(payload), sort_keys=True, default=str).encode()).hexdigest()[:16]


def propose(
    conn: psycopg.Connection,
    *,
    agent: str,
    kind: str,
    item_key: str,
    payload: dict,
    rationale: str,
    confidence: float,
    risk: str,
    auto_threshold: float,
    prompt_version: str | None = None,
    model: str | None = None,
) -> tuple[int, str]:
    approved = seeds.load(seeds.AGENT_OWNED[kind]).get(item_key)
    decision = decision_of(payload)
    if approved and {k: approved.get(k) for k in decision} != decision:
        risk = "high"  # changing something already approved is a recategorization: always reviewed
    status = "auto_approved" if risk == "low" and confidence >= auto_threshold else "pending"
    row = conn.execute(
        """INSERT INTO meta.agent_proposals
             (agent, kind, item_key, payload, payload_hash, rationale, confidence, risk, status,
              prompt_version, model, decided_at, decided_by)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                   CASE WHEN %s = 'auto_approved' THEN now() END,
                   CASE WHEN %s = 'auto_approved' THEN 'auto' END)
           -- the same decision proposed again keeps its earlier status, so a rejection sticks
           ON CONFLICT (kind, item_key, payload_hash) DO UPDATE SET rationale = EXCLUDED.rationale
           RETURNING proposal_id, status""",
        (
            agent,
            kind,
            item_key,
            Jsonb(payload),
            _hash(payload),
            rationale,
            confidence,
            risk,
            status,
            prompt_version,
            model,
            status,
            status,
        ),
    ).fetchone()
    return row["proposal_id"], row["status"]


def list_proposals(conn: psycopg.Connection, status: str | None = "pending") -> list[dict]:
    sql = "SELECT * FROM meta.agent_proposals"
    params: tuple = ()
    if status:
        sql += " WHERE status = %s"
        params = (status,)
    return conn.execute(sql + " ORDER BY kind, item_key, proposal_id", params).fetchall()


def decide(conn: psycopg.Connection, ids: list[int], status: str, by: str) -> int:
    """Approve pending proposals, or reject/revoke pending and approved decisions."""
    if status not in {"approved", "rejected"}:
        raise ValueError("decision must be approved or rejected")
    eligible = ["pending", "approved", "auto_approved"] if status == "rejected" else ["pending"]
    rows = conn.execute(
        """UPDATE meta.agent_proposals SET status=%s, decided_at=now(), decided_by=%s
           WHERE proposal_id = ANY(%s) AND status = ANY(%s) RETURNING proposal_id""",
        (status, by, ids, eligible),
    ).fetchall()
    conn.commit()
    event("proposals_decided", status=status, by=by, ids=[r["proposal_id"] for r in rows])
    return len(rows)


def export_seeds(conn: psycopg.Connection) -> dict[str, int]:
    """Merge decisions into seeds; a revoked decision removes its matching seed, without restoring older ones.

    Merging rather than regenerating keeps seeds valid after a database reset: git is the source of truth."""
    rows = conn.execute(
        """SELECT DISTINCT ON (kind, item_key) *
           FROM meta.agent_proposals WHERE status IN ('approved', 'auto_approved', 'rejected')
           ORDER BY kind, item_key, decided_at DESC, proposal_id DESC"""
    ).fetchall()
    written: dict[str, int] = {}
    for kind, filename in seeds.AGENT_OWNED.items():
        items = [r for r in rows if r["kind"] == kind]
        if not items:
            continue
        data = seeds.load(filename)
        for r in items:
            if r["status"] == "rejected":
                existing = data.get(r["item_key"])
                # Match the decision, not a database-local id: ids can be reused after a database reset.
                if existing is not None and _hash(existing) == r["payload_hash"]:
                    del data[r["item_key"]]
                continue
            data[r["item_key"]] = {
                **r["payload"],
                "_provenance": {
                    "proposal_id": r["proposal_id"],
                    "agent": r["agent"],
                    "approved_by": r["decided_by"],
                    "model": r["model"],
                    "prompt_version": r["prompt_version"],
                    "confidence": float(r["confidence"]) if r["confidence"] is not None else None,
                },
            }
        seeds.save(filename, data)
        written[filename] = len(items)
    event("seeds_exported", files=written)
    return written
