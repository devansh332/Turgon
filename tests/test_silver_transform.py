from datetime import datetime, timezone

import pytest

from medallion.config import SOURCE_COLUMNS
from medallion.pipeline.silver import Context, pick_winner, transform
from medallion.seeds import Seeds

INGESTED = datetime(2026, 11, 23, tzinfo=timezone.utc)


@pytest.fixture
def ctx():
    seeds = Seeds()
    # Pin the agent-owned seeds so these tests do not depend on what the agents last proposed.
    seeds.__dict__.update(
        category_labels={
            "power issue": {"is_label": True, "category": "Electrical"},
            "plumbing": {"is_label": True, "category": "Plumbing"},
            "elec": {"is_label": True, "category": "Electrical"},
        },
        templates={
            "power outlet not working at desk {n}. tried multiple devices.": {
                "is_description": True,
                "category": "Electrical",
                "asset_type": "power_outlet",
                "issue_type": "malfunction",
                "is_safety_issue": False,
                "urgency": "low",
                "confidence": 0.95,
            },
            "breaker keeps tripping in server room {n}. critical — affects production systems.": {
                "is_description": True,
                "category": "Electrical",
                "asset_type": "electrical_panel",
                "issue_type": "outage",
                "is_safety_issue": False,
                "urgency": "critical",
                "confidence": 0.95,
            },
        },
        people={
            "j. doe": {"canonical": "Jane Doe", "resolution": "initials"},
            "m. johnson": {"canonical": "M. Johnson", "resolution": "ambiguous"},
        },
    )
    return Context(seeds)


def bronze(**overrides) -> dict:
    row = dict.fromkeys(SOURCE_COLUMNS, "")
    row.update(
        ticket_id="TKT-5528",
        created_at="2025-02-17T12:41:38",
        category="power issue",
        priority="Low",
        status="Open",
        building="Annex West",
        description="Power outlet not working at desk 392. Tried multiple devices.",
        submitted_by="J. Doe",
        assigned_to="Joe (in-house)",
        cost="5877",
        sla_hours="48",
    )
    row.update(overrides)
    row.update(_bronze_id=1, _row_hash="ab" * 32, _source_file="data/raw_tickets.csv", _ingested_at=INGESTED)
    return row


def test_clean_row(ctx):
    rec, reasons = transform(bronze(), ctx)
    assert reasons == []
    assert rec["ticket_key"] == "TKT-5528"
    assert (rec["category"], rec["category_source"], rec["asset_type"]) == ("Electrical", "label", "power_outlet")
    assert (rec["priority"], rec["is_open"], rec["location_room"]) == ("low", True, "desk 392")
    assert (rec["submitted_by"], rec["submitter_resolution"]) == ("Jane Doe", "initials")
    assert (rec["sla_hours_effective"], rec["sla_source"]) == (48, "given")
    assert rec["dq_flags"] == []


def test_swapped_category_and_description_are_restored(ctx):
    rec, _ = transform(
        bronze(category="Power outlet not working at desk 12. Tried multiple devices.", description="Elec"), ctx
    )
    assert rec["fields_swapped"] and "fields_swapped" in rec["dq_flags"]
    assert (rec["category_raw"], rec["category"], rec["asset_type"]) == ("Elec", "Electrical", "power_outlet")


def test_placeholders_become_nulls_with_flags(ctx):
    rec, _ = transform(bronze(cost="TBD", sla_hours="999", priority="", status="???"), ctx)
    assert rec["cost"] is None and "cost:placeholder" in rec["dq_flags"]
    assert rec["sla_hours"] is None and "sla_hours:sentinel" in rec["dq_flags"]
    assert rec["status"] is None and "status:missing" in rec["dq_flags"]
    # priority falls back to the description's urgency, then the SLA to policy for that priority
    assert (rec["priority_effective"], rec["priority_source"]) == ("low", "inferred")
    assert (rec["sla_hours_effective"], rec["sla_source"]) == (72, "policy")


def test_understated_priority_is_flagged(ctx):
    rec, _ = transform(
        bronze(
            description="Breaker keeps tripping in server room 738. Critical — affects production systems.",
            priority="low",
        ),
        ctx,
    )
    assert "priority:understated_vs_description" in rec["dq_flags"]


def test_lifecycle_inconsistencies(ctx):
    rec, _ = transform(bronze(status="Resolved", resolved_at="2024-01-01"), ctx)
    assert "resolved_at:before_created" in rec["dq_flags"] and rec["resolution_hours"] is None
    rec, _ = transform(bronze(status="Open", resolved_at="2025-03-01"), ctx)
    assert "status:open_with_resolved_at" in rec["dq_flags"] and rec["resolution_hours"] is None
    rec, _ = transform(bronze(status="Closed", resolved_at="2025-02-18T12:41:38"), ctx)
    assert rec["resolution_hours"] == 24


def test_quarantine_rules(ctx):
    _, reasons = transform(bronze(created_at="asap"), ctx)
    assert reasons == ["created_at_placeholder"]
    _, reasons = transform(bronze(category="test", building="test building", submitted_by="admin"), ctx)
    assert "junk_record" in reasons


def test_invalid_ticket_id_gets_stable_surrogate(ctx):
    rec, _ = transform(bronze(ticket_id="N/A"), ctx)
    assert rec["ticket_key"] == "UNK-" + "ab" * 6 and "ticket_id:invalid" in rec["dq_flags"]


def test_ambiguous_initials_are_not_merged(ctx):
    rec, _ = transform(bronze(submitted_by="M. Johnson"), ctx)
    assert rec["submitted_by"] == "M. Johnson" and "submitted_by:ambiguous" in rec["dq_flags"]


def test_winner_prefers_progressed_status_then_latest():
    older = {
        "status": "Open",
        "_ingested_at": 1,
        "_bronze_id": 1,
        **dict.fromkeys(
            [
                "created_at",
                "resolved_at",
                "priority",
                "building",
                "submitted_by",
                "assignee",
                "cost",
                "sla_hours",
                "resolution_notes",
            ]
        ),
    }
    closed = {**older, "status": "Closed", "_bronze_id": 2}
    newer_open = {**older, "_ingested_at": 2, "_bronze_id": 3}
    assert pick_winner([older, closed, newer_open], {"Open": 1, "Closed": 3}) is closed
    assert pick_winner([older, newer_open], {"Open": 1}) is newer_open
