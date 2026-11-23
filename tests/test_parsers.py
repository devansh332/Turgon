from datetime import datetime
from decimal import Decimal

import pytest

from medallion.pipeline.parsers import (
    description_template,
    extract_location,
    parse_cost,
    parse_notes,
    parse_sla,
    parse_ticket_id,
    parse_ts,
)

NULLS = {"", "n/a", "null", "???", "unknown", "tbd", "asap", "not a date", "never", "pending", "00/00/0000"}
TS = {"null_tokens": NULLS, "min_date": datetime(2023, 1, 1), "max_date": datetime(2026, 11, 23)}
BUILDINGS = ["HQ Floor 1", "HQ Floor 2", "Tower A", "Annex West"]


@pytest.mark.parametrize(
    "raw, expected, flags",
    [
        ("2024-10-24 14:32:10", datetime(2024, 10, 24, 14, 32, 10), ()),
        ("2024-07-25T18:27:44", datetime(2024, 7, 25, 18, 27, 44), ()),
        ("2024-04-15", datetime(2024, 4, 15), ("date_only",)),
        ("02/14/2025 08:16 AM", datetime(2025, 2, 14, 8, 16), ()),
        ("04/26/2025 10:33 PM", datetime(2025, 4, 26, 22, 33), ()),
        ("02/03/2025", datetime(2025, 2, 3), ("ambiguous_mdy", "date_only")),
        ("03-20-2024 20:39:48", datetime(2024, 3, 20, 20, 39, 48), ()),
        ("12-Feb-2025 03:21", datetime(2025, 2, 12, 3, 21), ()),
        ("1704049383", datetime(2023, 12, 31, 19, 3, 3), ("epoch",)),
    ],
)
def test_parse_ts_formats(raw, expected, flags):
    assert parse_ts(raw, **TS) == (expected, flags)


@pytest.mark.parametrize(
    "raw, flag",
    [
        ("", "missing"),
        ("asap", "placeholder"),
        ("00/00/0000", "placeholder"),
        ("???", "placeholder"),
        ("13/45/2024", "unparseable"),
        ("yesterday", "unparseable"),
        ("2099-01-01", "out_of_range"),
        ("1999-05-01", "out_of_range"),
    ],
)
def test_parse_ts_rejects(raw, flag):
    assert parse_ts(raw, **TS) == (None, (flag,))


def test_same_day_and_month_is_not_ambiguous():
    assert parse_ts("05/05/2024", **TS) == (datetime(2024, 5, 5), ("date_only",))


@pytest.mark.parametrize(
    "raw, value, flags",
    [
        ("4569.86", Decimal("4569.86"), ()),
        ("$1,200.5", Decimal("1200.50"), ("currency_symbol",)),
        ("0", Decimal("0.00"), ("zero",)),
        ("-1", None, ("sentinel",)),
        ("-25", None, ("negative",)),
        ("N/A", None, ("placeholder",)),
        ("TBD", None, ("placeholder",)),
        ("error", None, ("placeholder",)),
        ("", None, ("missing",)),
        ("NaN", None, ("unparseable",)),
        ("Infinity", None, ("unparseable",)),
        ("1e999", None, ("unparseable",)),
    ],
)
def test_parse_cost(raw, value, flags):
    assert parse_cost(raw, null_tokens=NULLS | {"error"}) == (value, flags)


@pytest.mark.parametrize(
    "raw, value, flags",
    [
        ("48", 48, ()),
        ("999", None, ("sentinel",)),
        ("0", None, ("sentinel",)),
        ("-1", None, ("sentinel",)),
        ("N/A", None, ("placeholder",)),
        ("36", None, ("unexpected_value",)),
        ("", None, ("missing",)),
    ],
)
def test_parse_sla(raw, value, flags):
    assert parse_sla(raw, null_tokens=NULLS, valid={4, 8, 24, 48, 72}) == (value, flags)


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("TKT-3960", ("TKT-3960", 3960)),
        (" tkt-0042 ", ("TKT-42", 42)),
        ("N/A", (None, None)),
        ("15", (None, None)),
        ("", (None, None)),
        (None, (None, None)),
    ],
)
def test_parse_ticket_id(raw, expected):
    assert parse_ticket_id(raw) == expected


def test_template_collapses_buildings_numbers_assets_and_days():
    a = description_template("Elevator H-011 stuck between floors 1 and 9. No one inside (confirmed).", BUILDINGS)
    b = description_template("Elevator A-001 stuck between floors 4 and 12. No one inside (confirmed).", BUILDINGS)
    assert a == b == "elevator {asset} stuck between floors {n} and {n}. no one inside (confirmed)."
    assert (
        description_template("Need access card for Monday. HQ Floor 2 3 floor.", BUILDINGS)
        == "need access card for {day}. {building} {n} floor."
    )


def test_building_names_containing_floor_do_not_leak_into_floor():
    loc = extract_location("Restrooms in HQ Floor 2 need service", BUILDINGS)
    assert loc == {"building": "HQ Floor 2", "floor": None, "room": None, "asset_id": None}
    loc = extract_location("Printer on 3 floor not connecting. Model HP H-033.", BUILDINGS)
    assert (loc["floor"], loc["asset_id"]) == (3, "H-033")
    assert extract_location("Power outlet not working at desk 392.", BUILDINGS)["room"] == "desk 392"


@pytest.mark.parametrize(
    "raw, outcome, extra",
    [
        ("Duplicate of ticket #3098. Closing.", "duplicate_claim", {"claimed_duplicate_of": "TKT-3098"}),
        ("Resolved. Root cause: power surge.", "resolved", {"root_cause": "power surge"}),
        ("Dispatched technician. Fixed on site. Replaced valve.", "repaired", {"replaced_part": "valve"}),
        ("fixed", "resolved_unspecified", {}),
        ("Parts on order. ETA 3 business days.", "parts_on_order", {}),
        ("something else entirely", "other", {}),
        ("", None, {}),
    ],
)
def test_parse_notes(raw, outcome, extra):
    out = parse_notes(raw)
    assert out["outcome"] == outcome
    assert {k: out[k] for k in extra} == extra
