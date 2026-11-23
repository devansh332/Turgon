"""Completion of dropped items and output consistency, with a fake LLM (no network, no database)."""

from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from medallion.agents import classifier

TAXONOMY = {
    "categories": ["HVAC", "Plumbing"],
    "asset_types": ["hvac_unit", "pipe", "other"],
    "issue_types": ["leak", "malfunction"],
    "urgency_levels": ["low", "medium", "high", "critical"],
}


class FakeLLM:
    """Labels everything HVAC; drops the last item of the first call, like real models sometimes do."""

    def __init__(self):
        self.cfg = type("Cfg", (), {"llm_model_bulk": "fake"})()
        self.batches: list[list[str]] = []
        self.calls = 0

    def structured(self, *, payload, schema, **_):
        labels = [x["label"] for x in payload["labels"]]
        self.batches.append(labels)
        self.calls += 1
        if self.calls == 1:
            labels = labels[:-1]
        results = [
            {"label": v, "is_label": True, "category": "HVAC", "confidence": 0.9, "rationale": "r"} for v in labels
        ]
        return schema.model_validate({"results": results})


def test_dropped_items_are_re_asked_once():
    llm = FakeLLM()
    out = classifier.classify_labels(llm, [("a/c", 5), ("heat", 3), ("cool", 1)], TAXONOMY)
    assert sorted(r.label for r in out) == ["a/c", "cool", "heat"]
    assert llm.batches == [["a/c", "heat", "cool"], ["cool"]]


def test_self_contradicting_template_fails_validation():
    schema = classifier.models(TAXONOMY)["template"]
    bad = {
        "template": "smoke detector beeping {building}",
        "is_description": False,
        "category": "HVAC",
        "asset_type": None,
        "issue_type": None,
        "is_safety_issue": True,
        "urgency": "high",
        "confidence": 0.9,
        "rationale": "r",
    }
    with pytest.raises(ValidationError, match="is_description=false requires"):
        schema.model_validate({"results": [bad]})
    with pytest.raises(ValidationError, match="requires a category"):
        schema.model_validate({"results": [bad | {"is_description": True, "category": None}]})


def test_safety_rule_disagreement_requires_review(monkeypatch):
    template = "sparking power outlet"
    result = (
        classifier.models(TAXONOMY)["template"]
        .model_validate(
            {
                "results": [
                    {
                        "template": template,
                        "is_description": True,
                        "category": "HVAC",
                        "is_safety_issue": False,
                        "urgency": "low",
                        "confidence": 0.99,
                        "rationale": "model missed the hazard",
                    }
                ]
            }
        )
        .results
    )
    seeds = SimpleNamespace(taxonomy=TAXONOMY, ref={"buildings": []}, category_labels={}, templates={}, people={})
    monkeypatch.setattr(classifier, "Seeds", lambda: seeds)
    monkeypatch.setattr(classifier, "distinct_labels", lambda conn: {})
    monkeypatch.setattr(classifier, "distinct_people", lambda conn: {})
    monkeypatch.setattr(classifier, "distinct_templates", lambda conn, buildings: {template: {"template": template}})
    monkeypatch.setattr(classifier, "classify_templates", lambda *args: result)
    proposed = []

    def propose(conn, **kwargs):
        proposed.append(kwargs)
        return 1, "pending"

    monkeypatch.setattr(classifier.proposals, "propose", propose)
    llm = SimpleNamespace(cfg=SimpleNamespace(llm_model_bulk="fake"), prompt_version=lambda _: "v1")
    classifier.run(SimpleNamespace(commit=lambda: None), llm, 0.9)
    assert proposed[0]["risk"] == "high"
    assert proposed[0]["payload"]["safety_rule"] is True
