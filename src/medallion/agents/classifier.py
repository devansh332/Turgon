"""Semantic classification agent (option c).

Cost model: the LLM never sees rows. It labels distinct values: ~100 category labels, ~100 description
templates and ~35 submitter names cover all 10k tickets, and anything already in seeds is skipped, so a
new daily batch only pays for values never seen before. Results are applied to rows deterministically in silver.
"""

import collections
import re
from typing import Literal

import psycopg
from pydantic import BaseModel, Field, create_model, model_validator

from medallion import proposals
from medallion.agents.llm import LLMClient, batched
from medallion.pipeline.parsers import description_template, fold
from medallion.seeds import Seeds

LABEL_BATCH, TEMPLATE_BATCH = 40, 12


def _literal(values: list[str]):
    return Literal[tuple(values)]


def _label_consistent(r):
    if r.is_label != (r.category is not None):
        raise ValueError(
            f"{r.label!r}: is_label={r.is_label} requires category to be {'set' if r.is_label else 'null'}"
        )
    return r


def _template_consistent(r):
    enrichment = (r.category, r.asset_type, r.issue_type, r.urgency)
    if r.is_description and r.category is None:
        raise ValueError(f"{r.template!r}: is_description=true requires a category")
    if not r.is_description and (any(v is not None for v in enrichment) or r.is_safety_issue):
        raise ValueError(
            f"{r.template!r}: is_description=false requires category, asset_type, issue_type, urgency null"
        )
    return r


def models(taxonomy: dict) -> dict[str, type[BaseModel]]:
    """Output schemas with the taxonomy baked in as enums, plus cross-field consistency: an invalid category or
    a self-contradicting item fails validation and the batch is retried with the error."""
    category = _literal(taxonomy["categories"])
    label = create_model(
        "LabelResult",
        __validators__={"consistent": model_validator(mode="after")(_label_consistent)},
        label=(str, ...),
        is_label=(bool, ...),
        category=(category | None, None),
        confidence=(float, Field(ge=0, le=1)),
        rationale=(str, ...),
    )
    template = create_model(
        "TemplateResult",
        __validators__={"consistent": model_validator(mode="after")(_template_consistent)},
        template=(str, ...),
        is_description=(bool, ...),
        category=(category | None, None),
        asset_type=(_literal(taxonomy["asset_types"]) | None, None),
        issue_type=(_literal(taxonomy["issue_types"]) | None, None),
        is_safety_issue=(bool, ...),
        urgency=(_literal(taxonomy["urgency_levels"]) | None, None),
        confidence=(float, Field(ge=0, le=1)),
        rationale=(str, ...),
    )
    person = create_model(
        "PersonResult",
        value=(str, ...),
        canonical=(str | None, None),
        resolution=(Literal["exact", "initials", "nickname", "ambiguous", "system"], ...),
        confidence=(float, Field(ge=0, le=1)),
        rationale=(str, ...),
    )
    return {
        "label": create_model("LabelResults", results=(list[label], ...)),
        "template": create_model("TemplateResults", results=(list[template], ...)),
        "person": create_model("PersonResults", results=(list[person], ...)),
    }


# ------------------------------------------------------------------ inputs
def distinct_labels(conn: psycopg.Connection) -> collections.Counter:
    rows = conn.execute("SELECT category, count(*) AS n FROM bronze.tickets_raw GROUP BY 1").fetchall()
    counts: collections.Counter = collections.Counter()
    for r in rows:
        counts[fold(r["category"])] += r["n"]
    return counts


def distinct_templates(conn: psycopg.Connection, buildings: list[str]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for r in conn.execute(
        "SELECT description FROM bronze.tickets_raw WHERE btrim(coalesce(description, '')) <> ''"
    ).fetchall():
        key = description_template(r["description"], buildings)
        entry = out.setdefault(key, {"template": key, "tickets": 0, "example": r["description"]})
        entry["tickets"] += 1
    return out


def distinct_people(conn: psycopg.Connection) -> collections.Counter:
    rows = conn.execute("SELECT submitted_by, count(*) AS n FROM bronze.tickets_raw GROUP BY 1").fetchall()
    counts: collections.Counter = collections.Counter()
    for r in rows:
        if fold(r["submitted_by"]):
            counts[fold(r["submitted_by"])] += r["n"]
    return counts


# ------------------------------------------------------------------ LLM calls
def _complete(llm: LLMClient, items: list, key_of, field: str, call) -> list:
    """Models sometimes drop or rewrite an item. Keep only results whose key matches an input exactly,
    and re-ask once for whatever is missing."""
    wanted = {key_of(i): i for i in items}
    results = {getattr(r, field): r for r in call(items) if getattr(r, field) in wanted}
    missing = [i for k, i in wanted.items() if k not in results]
    if missing:
        results |= {getattr(r, field): r for r in call(missing) if getattr(r, field) in wanted}
    return list(results.values())


def classify_labels(llm: LLMClient, labels: list[tuple[str, int]], taxonomy: dict) -> list:
    schema = models(taxonomy)["label"]

    def call(batch):
        payload = {"taxonomy": taxonomy["categories"], "labels": [{"label": v, "tickets": n} for v, n in batch]}
        return llm.structured(agent="classifier", prompt_name="category_labels", payload=payload, schema=schema).results

    return [r for b in batched(labels, LABEL_BATCH) for r in _complete(llm, b, lambda i: i[0], "label", call)]


def classify_templates(llm: LLMClient, templates: list[dict], taxonomy: dict) -> list:
    schema = models(taxonomy)["template"]
    allowed = {k: taxonomy[k] for k in ("categories", "asset_types", "issue_types", "urgency_levels")}

    def call(batch):
        return llm.structured(
            agent="classifier",
            prompt_name="description_templates",
            payload={"taxonomy": allowed, "templates": batch},
            schema=schema,
        ).results

    out = []
    for batch in batched(templates, TEMPLATE_BATCH):
        out += _complete(llm, batch, lambda i: i["template"], "template", call)
    return out


def resolve_people(llm: LLMClient, people: list[tuple[str, int]], taxonomy: dict) -> list:
    payload = {"submitters": [{"value": v, "tickets": n} for v, n in people]}
    return llm.structured(
        agent="classifier", prompt_name="people", payload=payload, schema=models(taxonomy)["person"]
    ).results


# ------------------------------------------------------------------ rules baseline (used by evals)
_KEYWORDS = [
    ("HVAC", r"hvac|a/?c\b|air|heat|cool|climate|thermostat|temperature|cold\b|hot\b(?! water)|humid"),
    ("Pest Control", r"pest|extermin|bug|roach|ant\b|ants\b|mice|rodent|wasp"),
    ("Fire Safety", r"fire|smoke|sprinkler|extinguisher|exit sign"),
    ("Elevator", r"elevator|lift|escalator|vertical"),
    ("Plumbing", r"plumb|water|pipe|toilet|faucet|drain|sink|leak"),
    ("Electrical", r"elec|power|outlet|breaker|light|spark"),
    ("Security & Access", r"secur|access|badge|camera|door.*lock|tailgat|alarm"),
    ("IT & Network", r"\bit\b|network|wifi|wi-fi|vpn|internet|printer|connect"),
    ("Cleaning", r"clean|janitor|housekeep|trash|spill|stain|restroom|dropping"),
    ("General Maintenance", r"general|maint|misc|other|carpet|paint|window|ceiling|pothole|furniture|door"),
]


def rule_category(text: str) -> str | None:
    for category, pattern in _KEYWORDS:
        if re.search(pattern, text, re.IGNORECASE):
            return category
    return None


def rule_label(label: str) -> dict:
    """Baseline: short values (<= 3 words) matched by keywords are labels."""
    category = rule_category(label) if len(label.split()) <= 3 else None
    return {"is_label": category is not None, "category": category}


def rule_template(template: str) -> dict:
    t = template.lower()
    category = rule_category(t) if len(t.split()) > 1 else None
    safety = bool(re.search(r"fire|smoke|sprinkler|exit sign|emergency|spark|hazard|falling|slip|trip", t))
    if re.search(r"critical|burst|production", t):
        urgency = "critical"
    elif re.search(r"urgent|hazard|down|out of order|can't get in|do not use|stuck|overflow|emergency", t):
        urgency = "high"
    elif re.search(r"request|need more|stain|peeling|looks", t):
        urgency = "low"
    else:
        urgency = "medium"
    return {
        "is_description": category is not None,
        "category": category,
        "is_safety_issue": safety,
        "urgency": urgency if category else None,
    }


def rule_people(values: list[str], junk: set[str]) -> dict[str, dict]:
    full = [v for v in values if not re.match(r"^[a-z]\. ", v) and v not in junk]
    out = {}
    for v in values:
        if v in junk:
            out[v] = {"canonical": None, "resolution": "system"}
        elif m := re.match(r"^([a-z])\. (.+)$", v):
            matches = [f for f in full if f.startswith(m.group(1)) and f.endswith(" " + m.group(2))]
            out[v] = (
                {"canonical": matches[0].title(), "resolution": "initials"}
                if len(matches) == 1
                else {"canonical": v.title(), "resolution": "ambiguous"}
            )
        else:
            out[v] = {"canonical": v.title(), "resolution": "exact"}
    return out


# ------------------------------------------------------------------ agent entry point
def run(conn: psycopg.Connection, llm: LLMClient, auto_threshold: float, refresh: bool = False) -> dict:
    s = Seeds()
    taxonomy, buildings = s.taxonomy, s.ref["buildings"]
    summary = collections.Counter()

    def propose(kind, key, payload, result, risk, prompt):
        _, status = proposals.propose(
            conn,
            agent="classifier",
            kind=kind,
            item_key=key,
            payload=payload,
            rationale=result.rationale,
            confidence=result.confidence,
            risk=risk,
            auto_threshold=auto_threshold,
            prompt_version=llm.prompt_version(prompt),
            model=llm.cfg.llm_model_bulk,
        )
        summary[f"{kind}:{status}"] += 1

    labels = [(v, n) for v, n in sorted(distinct_labels(conn).items()) if refresh or v not in s.category_labels]
    for r in classify_labels(llm, labels, taxonomy):
        propose(
            "category_label", r.label, {"is_label": r.is_label, "category": r.category}, r, "low", "category_labels"
        )

    templates = [t for k, t in sorted(distinct_templates(conn, buildings).items()) if refresh or k not in s.templates]
    for r in classify_templates(llm, templates, taxonomy):
        payload = r.model_dump(exclude={"template", "rationale"})
        safety_rule = rule_template(r.template)["is_safety_issue"]
        payload["safety_rule"] = safety_rule
        propose(
            "description_template",
            r.template,
            payload,
            r,
            "high" if r.urgency == "critical" or r.is_safety_issue or safety_rule != r.is_safety_issue else "low",
            "description_templates",
        )

    people = distinct_people(conn)
    if refresh or any(v not in s.people for v in people):
        for r in resolve_people(llm, sorted(people.items()), taxonomy):
            # only case/spacing variants are safe; any merge of distinct spellings, or nulling a submitter
            # as a system account, changes who a ticket is attributed to and needs a human
            same_name = bool(r.canonical) and r.canonical.lower().split() == r.value.lower().split()
            risk = "low" if r.resolution == "exact" and same_name else "high"
            propose("person_merge", r.value, {"canonical": r.canonical, "resolution": r.resolution}, r, risk, "people")

    conn.commit()
    summary["labels_sent"], summary["templates_sent"] = len(labels), len(templates)
    return dict(summary)
