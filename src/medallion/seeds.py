"""Load and save seeds: the versioned, reviewed reference data that silver is allowed to use.

Human-owned files (taxonomy, reference) are edited by hand. Agent-owned files are written only by
`proposals approve`, and every entry carries its provenance (proposal id, approver, model, prompt version).
"""

import hashlib
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path

import yaml

from medallion.config import SEEDS_DIR, SQL_DIR

AGENT_OWNED = {
    "category_label": "category_labels.yaml",
    "description_template": "description_templates.yaml",
    "person_merge": "people.yaml",
    "dq_rule": "dq_rules.yaml",
    "column_tag": "column_tags.yaml",
    "schema_migration": "schema_migrations.yaml",
}


def load(name: str) -> dict:
    path = SEEDS_DIR / name
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text()) or {}


def save(name: str, data: dict) -> None:
    header = "# Agent-owned seed. Edit through `medallion proposals approve`, not by hand.\n"
    body = yaml.safe_dump(data, sort_keys=True, allow_unicode=True, width=110)
    (SEEDS_DIR / name).write_text(header + body)


SILVER_CODE = [
    Path(__file__).parent / "pipeline" / "parsers.py",
    Path(__file__).parent / "pipeline" / "silver.py",
    SQL_DIR / "03_silver.sql",
]


def rules_version() -> str:
    """Hash of every seed file and of the silver transform code: any change to approved rules or to how
    they are applied changes this and forces a full silver rebuild on the next run."""
    digest = hashlib.sha256()
    for path in sorted(SEEDS_DIR.glob("*.yaml")) + SILVER_CODE:
        digest.update(path.name.encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()[:12]


@dataclass
class Seeds:
    """Everything silver needs, loaded once per run."""

    @cached_property
    def taxonomy(self) -> dict:
        return load("taxonomy.yaml")

    @cached_property
    def ref(self) -> dict:
        return load("reference.yaml")

    @cached_property
    def category_labels(self) -> dict:
        return load(AGENT_OWNED["category_label"])

    @cached_property
    def templates(self) -> dict:
        return load(AGENT_OWNED["description_template"])

    @cached_property
    def people(self) -> dict:
        return load(AGENT_OWNED["person_merge"])

    @cached_property
    def dq_rules(self) -> dict:
        return load(AGENT_OWNED["dq_rule"])

    @cached_property
    def column_tags(self) -> dict:
        return load(AGENT_OWNED["column_tag"])

    @cached_property
    def schema_migrations(self) -> dict:
        return load(AGENT_OWNED["schema_migration"])
