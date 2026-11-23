"""Pure, deterministic parsing and normalization. No I/O, so every rule here is unit-tested directly."""

import hashlib
import re
from datetime import datetime, timezone
from decimal import Decimal

Flags = tuple[str, ...]


def fold(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "").strip()).lower()


def row_hash(values: list[str | None]) -> str:
    return hashlib.sha256("\x1f".join("\x00" if v is None else v for v in values).encode()).hexdigest()


# ---------------------------------------------------------------- timestamps
# (pattern, strptime format, month-first?, date-only?)
_TS_FORMATS = [
    (re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"), "%Y-%m-%dT%H:%M:%S", False, False),
    (re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"), "%Y-%m-%d %H:%M:%S", False, False),
    (re.compile(r"\d{4}-\d{2}-\d{2}"), "%Y-%m-%d", False, True),
    (re.compile(r"\d{2}/\d{2}/\d{4} \d{2}:\d{2} [AaPp][Mm]"), "%m/%d/%Y %I:%M %p", True, False),
    (re.compile(r"\d{2}/\d{2}/\d{4}"), "%m/%d/%Y", True, True),
    (re.compile(r"\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}"), "%m-%d-%Y %H:%M:%S", True, False),
    (re.compile(r"\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}"), "%d-%b-%Y %H:%M", False, False),
]
_EPOCH = re.compile(r"\d{10}")


def parse_ts(
    raw: str | None, *, null_tokens: set[str], min_date: datetime, max_date: datetime
) -> tuple[datetime | None, Flags]:
    """Parse the eight timestamp shapes found in the source (seven text formats plus Unix epoch seconds).

    Slash and numeric-dash dates are month-first: in this file 2,182 slash dates have a day > 12 in
    the second position and none in the first. Values where both readings are valid are still parsed
    month-first but flagged `ambiguous_mdy`, so they can be excluded from time-sensitive analysis.
    """
    s = (raw or "").strip()
    if not s:
        return None, ("missing",)
    if s.lower() in null_tokens:
        return None, ("placeholder",)

    flags: list[str] = []
    value = None
    if _EPOCH.fullmatch(s):
        value = datetime.fromtimestamp(int(s), tz=timezone.utc).replace(tzinfo=None)
        flags.append("epoch")
    else:
        for pattern, fmt, month_first, date_only in _TS_FORMATS:
            if not pattern.fullmatch(s):
                continue
            try:
                value = datetime.strptime(s, fmt)
            except ValueError:
                return None, ("unparseable",)
            if month_first:
                first, second = int(s[0:2]), int(s[3:5])
                if first <= 12 and second <= 12 and first != second:
                    flags.append("ambiguous_mdy")
            if date_only:
                flags.append("date_only")
            break
    if value is None:
        return None, ("unparseable",)
    if not (min_date <= value <= max_date):
        return None, ("out_of_range",)
    return value, tuple(flags)


# ---------------------------------------------------------------- numbers
def parse_cost(raw: str | None, *, null_tokens: set[str]) -> tuple[Decimal | None, Flags]:
    s = (raw or "").strip()
    if not s:
        return None, ("missing",)
    if s.lower() in null_tokens:
        return None, ("placeholder",)
    number = s.replace("$", "").replace(",", "")
    if not re.fullmatch(r"-?\d+(?:\.\d+)?", number):
        return None, ("unparseable",)  # also keeps NaN / Infinity / 1e999 out of Decimal arithmetic
    value = Decimal(number)
    if value == -1:
        return None, ("sentinel",)
    if value < 0:
        return None, ("negative",)
    flags = ("currency_symbol",) if "$" in s else ()
    return value.quantize(Decimal("0.01")), flags + (("zero",) if value == 0 else ())


def parse_sla(raw: str | None, *, null_tokens: set[str], valid: set[int]) -> tuple[int | None, Flags]:
    s = (raw or "").strip()
    if not s:
        return None, ("missing",)
    if s.lower() in null_tokens:
        return None, ("placeholder",)
    if not re.fullmatch(r"-?\d+", s):
        return None, ("unparseable",)
    value = int(s)
    if value in (-1, 0, 999):
        return None, ("sentinel",)
    if value not in valid:
        return None, ("unexpected_value",)
    return value, ()


# ---------------------------------------------------------------- identifiers
_TICKET = re.compile(r"TKT-(\d+)")


def parse_ticket_id(raw: str | None) -> tuple[str | None, int | None]:
    m = _TICKET.fullmatch((raw or "").strip().upper())
    return (f"TKT-{int(m.group(1))}", int(m.group(1))) if m else (None, None)


# ---------------------------------------------------------------- free text
_ASSET = re.compile(r"\b[A-Z]-\d{3}\b")
_NUMBER = re.compile(r"\d+(?:\.\d+)?")
_WEEKDAY = re.compile(r"\b(?:mon|tues|wednes|thurs|fri|satur|sun)day\b", re.IGNORECASE)
_FLOOR = re.compile(r"\b(\d{1,3})(?:st|nd|rd|th)?\s+floor\b|\bfloor\s+(\d{1,3})\b", re.IGNORECASE)
_ROOM = re.compile(r"\b(room|desk|cubicle|spot)\s+(\d+)\b", re.IGNORECASE)


def _strip_buildings(text: str, buildings: list[str]) -> tuple[str, str | None]:
    """Replace building names (longest first, so 'HQ Floor 1' never leaves a stray 'Floor 1')."""
    found = None
    for name in sorted(buildings, key=len, reverse=True):
        pattern = re.compile(re.escape(name), re.IGNORECASE)
        if pattern.search(text):
            found = found or name
            text = pattern.sub("{building}", text)
    return text, found


def description_template(text: str | None, buildings: list[str]) -> str:
    """Reduce a description to its template, e.g. 'elevator {asset} stuck between floors {n} and {n}...'.

    The 10k descriptions collapse to ~100 templates, so the classifier labels templates, not rows."""
    t, _ = _strip_buildings(text or "", buildings)
    t = _ASSET.sub("{asset}", t)
    t = _NUMBER.sub("{n}", t)
    t = _WEEKDAY.sub("{day}", t)
    return re.sub(r"\s+", " ", t).strip().lower()


def extract_location(text: str | None, buildings: list[str]) -> dict:
    t, building = _strip_buildings(text or "", buildings)
    floor = _FLOOR.search(t)
    room = _ROOM.search(t)
    asset = _ASSET.search(t)
    return {
        "building": building,
        "floor": int(floor.group(1) or floor.group(2)) if floor else None,
        "room": f"{room.group(1).lower()} {room.group(2)}" if room else None,
        "asset_id": asset.group(0) if asset else None,
    }


_NOTES = [
    (re.compile(r"duplicate of ticket #(\d+)", re.I), "duplicate_claim"),
    (re.compile(r"temporary fix applied", re.I), "temporary_fix"),
    (re.compile(r"vendor called", re.I), "vendor_scheduled"),
    (re.compile(r"parts on order", re.I), "parts_on_order"),
    (re.compile(r"could not be reproduced", re.I), "not_reproduced"),
    (re.compile(r"resolved\. root cause: (.+?)\.?$", re.I), "resolved"),
    (re.compile(r"dispatched technician.*replaced (.+?)\.?$", re.I), "repaired"),
    (re.compile(r"^(?:completed|resolved by)\b", re.I), "resolved"),
    (re.compile(r"^(?:fixed|done)$", re.I), "resolved_unspecified"),
]
RESOLVED_OUTCOMES = {"resolved", "repaired", "resolved_unspecified"}


def parse_notes(raw: str | None) -> dict:
    """Notes are templated too; regexes beat an LLM here (exact, free, and nine patterns cover every note)."""
    s = (raw or "").strip()
    out = {"outcome": None, "root_cause": None, "replaced_part": None, "claimed_duplicate_of": None}
    if not s:
        return out
    for pattern, outcome in _NOTES:
        m = pattern.search(s)
        if not m:
            continue
        out["outcome"] = outcome
        if outcome == "duplicate_claim":
            out["claimed_duplicate_of"] = f"TKT-{int(m.group(1))}"
        elif outcome == "resolved" and m.groups():
            out["root_cause"] = m.group(1).lower()
        elif outcome == "repaired":
            out["replaced_part"] = m.group(1).lower()
        return out
    out["outcome"] = "other"
    return out
