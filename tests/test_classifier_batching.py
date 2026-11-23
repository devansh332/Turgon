"""Batching, completion of dropped items and the item cache, with a fake LLM (no network, no database)."""

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
        self.item_cache: dict[str, object] = {}
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

    def cached_items(self, prompt_name, model, keys, schema):
        return {k: self.item_cache[k] for k in keys if k in self.item_cache}

    def cache_items(self, prompt_name, model, items):
        self.item_cache.update(items)


def test_dropped_items_are_re_asked_once():
    llm = FakeLLM()
    out = classifier.classify_labels(llm, [("a/c", 5), ("heat", 3), ("cool", 1)], TAXONOMY)
    assert sorted(r.label for r in out) == ["a/c", "cool", "heat"]
    assert llm.batches == [["a/c", "heat", "cool"], ["cool"]]


def test_item_cache_sends_only_new_values():
    llm = FakeLLM()
    classifier.classify_labels(llm, [("a/c", 5), ("heat", 3)], TAXONOMY)
    llm.batches.clear()
    out = classifier.classify_labels(llm, [("a/c", 9), ("heat", 3), ("chiller", 1)], TAXONOMY)
    assert sorted(r.label for r in out) == ["a/c", "chiller", "heat"]
    assert llm.batches == [["chiller"]]  # counts changed and a value was added: only the new value is paid for


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
