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
EVIDENCE_KEYS = {"confidence", "affected_rows", "samples", "verification", "sql_valid"}


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
    previously_rejected = conn.execute(
        "SELECT 1 FROM meta.agent_proposals WHERE kind=%s AND item_key=%s AND status='rejected' LIMIT 1",
        (kind, item_key),
    ).fetchone()
    if previously_rejected:
        risk = "high"  # a human said no to this item once; never auto-approve a second attempt
    status = "auto_approved" if risk == "low" and confidence >= auto_threshold else "pending"
    row = conn.execute(
        """INSERT INTO meta.agent_proposals
             (agent, kind, item_key, payload, payload_hash, rationale, confidence, risk, status,
              prompt_version, model, decided_at, decided_by)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                   CASE WHEN %s = 'auto_approved' THEN now() END,
                   CASE WHEN %s = 'auto_approved' THEN 'auto' END)
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
    """Approve or reject pending proposals. A reviewer can also revoke an auto-approval by rejecting it."""
    decidable = ["pending", "auto_approved"] if status == "rejected" else ["pending"]
    rows = conn.execute(
        """UPDATE meta.agent_proposals SET status=%s, decided_at=now(), decided_by=%s
           WHERE proposal_id = ANY(%s) AND status = ANY(%s) RETURNING proposal_id""",
        (status, by, ids, decidable),
    ).fetchall()
    conn.commit()
    event("proposals_decided", status=status, by=by, ids=[r["proposal_id"] for r in rows])
    return len(rows)


def export_seeds(conn: psycopg.Connection) -> dict[str, int]:
    """Merge approved proposals into the agent-owned seed files (latest decision per item wins).

    Merging rather than regenerating keeps seeds valid after a database reset: git is the source of truth."""
    rows = conn.execute(
        """SELECT DISTINCT ON (kind, item_key) *
           FROM meta.agent_proposals WHERE status IN ('approved', 'auto_approved')
           ORDER BY kind, item_key, decided_at DESC, proposal_id DESC"""
    ).fetchall()
    revoked = conn.execute(
        "SELECT kind, item_key, proposal_id FROM meta.agent_proposals WHERE status = 'rejected'"
    ).fetchall()
    written: dict[str, int] = {}
    for kind, filename in seeds.AGENT_OWNED.items():
        items = [r for r in rows if r["kind"] == kind]
        data = seeds.load(filename)
        dropped = [
            r["item_key"]
            for r in revoked
            if r["kind"] == kind
            and data.get(r["item_key"], {}).get("_provenance", {}).get("proposal_id") == r["proposal_id"]
        ]
        for key in dropped:  # the applied decision was revoked by a reviewer
            del data[key]
        if not items and not dropped:
            continue
        for r in items:
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
