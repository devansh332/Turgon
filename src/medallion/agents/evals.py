"""Agent evaluation harness and promotion gate.

Scores every agent against a hand-labelled golden set AND a rules-only baseline, and reads latency, first-try
format compliance and cost from meta.llm_calls. A prompt or model change is promoted only if it does not
lose accuracy or format compliance, and does not raise cost per 1k rows by more than 20%.

Agents are called with the full distinct vocabulary, bypassing the cache. Each evaluation has its own UUID,
and operational metrics include only calls belonging to that run, even when other agents run concurrently.

Leakage: any golden item quoted in a prompt's "## Examples" section is excluded from the `held_out` scores,
and the promotion gate uses those. `agent` scores over every golden item are reported for completeness.
"""

import csv
import json
import re
import statistics
from datetime import datetime, timezone
from uuid import UUID, uuid4

from medallion.agents import classifier, dq_agent
from medallion.agents.dq_eval import score_predicates
from medallion.agents.llm import LLMClient, load_prompt
from medallion.config import EVALS_DIR, Config
from medallion.db import connect
from medallion.log import event
from medallion.seeds import Seeds

GOLDEN = EVALS_DIR / "golden"
RESULTS = EVALS_DIR / "results"
GATED = [
    "labels.held_out.category_accuracy",
    "templates.held_out.category_accuracy",
    "templates.held_out.urgency_within_one",
    "templates.held_out.safety_accuracy",
    "templates.held_out.safety_recall",
    "people.held_out.resolution_accuracy",
    "dq.agent.recall",
    "dq.predicates.f1",
    "ops.first_try_valid_rate",
]
_URGENCY = {"low": 0, "medium": 1, "high": 2, "critical": 3}


def _csv(name: str) -> list[dict]:
    with (GOLDEN / name).open(newline="") as f:
        return list(csv.DictReader(f))


def prompt_examples(prompt_name: str) -> set[str]:
    """Every quoted string in the prompt's Examples section, lower-cased."""
    text = load_prompt(prompt_name)[1]
    examples = text.split("## Examples", 1)[1] if "## Examples" in text else ""
    return {s.strip().lower() for s in re.findall(r'"([^"]*)"', examples)}


def held_out(gold: list[dict], key: str, prompt_name: str) -> list[dict]:
    seen = prompt_examples(prompt_name)
    return [g for g in gold if g[key].strip().lower() not in seen]


def _bool(v: str) -> bool:
    return v.strip().lower() == "true"


def _rate(hits: list[bool]) -> float | None:
    return round(sum(hits) / len(hits), 4) if hits else None


def score_labels(pred: dict, gold: list[dict]) -> dict:
    labels = [g for g in gold if _bool(g["is_label"])]
    return {
        "items": len(gold),
        "returned": sum(g["label"] in pred for g in gold),
        "is_label_accuracy": _rate(
            [g["label"] in pred and pred[g["label"]]["is_label"] == _bool(g["is_label"]) for g in gold]
        ),
        "category_accuracy": _rate([pred.get(g["label"], {}).get("category") == g["category"] for g in labels]),
    }


def score_templates(pred: dict, gold: list[dict]) -> dict:
    real = [g for g in gold if _bool(g["is_description"])]

    def urgency_gap(g):
        p = pred.get(g["template"], {}).get("urgency")
        return abs(_URGENCY[p] - _URGENCY[g["urgency"]]) if p in _URGENCY else 9

    return {
        "items": len(gold),
        "returned": sum(g["template"] in pred for g in gold),
        "is_description_accuracy": _rate(
            [pred.get(g["template"], {}).get("is_description") == _bool(g["is_description"]) for g in gold]
        ),
        "category_accuracy": _rate([pred.get(g["template"], {}).get("category") == g["category"] for g in real]),
        "safety_accuracy": _rate(
            [pred.get(g["template"], {}).get("is_safety_issue") == _bool(g["is_safety_issue"]) for g in real]
        ),
        "safety_recall": _rate(
            [pred.get(g["template"], {}).get("is_safety_issue") is True for g in real if _bool(g["is_safety_issue"])]
        ),
        "urgency_exact": _rate([urgency_gap(g) == 0 for g in real]),
        "urgency_within_one": _rate([urgency_gap(g) <= 1 for g in real]),
    }


def score_people(pred: dict, gold: list[dict]) -> dict:
    return {
        "items": len(gold),
        "canonical_accuracy": _rate(
            [(pred.get(g["value"], {}).get("canonical") or "") == g["canonical"] for g in gold]
        ),
        "resolution_accuracy": _rate([pred.get(g["value"], {}).get("resolution") == g["resolution"] for g in gold]),
    }


def score_dq(rules: list[dict], gold: list[dict]) -> dict:
    """`recall` accepts the equivalent (column, issue_type) pairs listed in the golden `also_accept` column,
    e.g. '-1' filed as out_of_range instead of sentinel_values; `strict_recall` needs the exact pair."""
    found = {(r["column"], r["issue_type"]) for r in rules}
    columns = {r["column"] for r in rules}

    def accepted(g: dict) -> set[tuple[str, str]]:
        alts = [tuple(a.split(":", 1)) for a in (g.get("also_accept") or "").split(";") if a]
        return {(g["column"], g["issue_type"]), *alts}

    hit = [bool(accepted(g) & found) for g in gold]
    return {
        "golden_issues": len(gold),
        "rules": len(rules),
        "recall": _rate(hit),
        "strict_recall": _rate([(g["column"], g["issue_type"]) in found for g in gold]),
        "column_recall": _rate([g["column"] in columns for g in gold]),
        "executable_rate": _rate([r.get("affected_rows") is not None for r in rules]),
        "non_empty_rate": _rate([bool(r.get("affected_rows")) for r in rules]),
        "missed": sorted(f"{g['column']}:{g['issue_type']}" for g, h in zip(gold, hit, strict=True) if not h),
    }


def ops_metrics(conn, eval_run_id: UUID, rows_covered: int) -> dict:
    calls = conn.execute(
        """SELECT prompt_name, attempts, ok, latency_ms, input_tokens, output_tokens, cost_usd FROM meta.llm_calls
           WHERE NOT cache_hit AND eval_run_id = %s""",
        (eval_run_id,),
    ).fetchall()
    latencies = sorted(c["latency_ms"] for c in calls if c["ok"])
    classifier_cost = sum(
        float(c["cost_usd"])
        for c in calls
        if c["prompt_name"] in ("category_labels", "description_templates", "people")
    )
    return {
        "calls": len(calls),
        "first_try_valid_rate": _rate([c["ok"] and c["attempts"] == 1 for c in calls]),
        "failure_rate": _rate([not c["ok"] for c in calls]),
        "latency_ms_p50": statistics.median(latencies) if latencies else None,
        "latency_ms_p95": latencies[int(0.95 * (len(latencies) - 1))] if latencies else None,
        "tokens_in": sum(c["input_tokens"] for c in calls),
        "tokens_out": sum(c["output_tokens"] for c in calls),
        "cost_usd": round(sum(float(c["cost_usd"]) for c in calls), 5),
        "classifier_cost_per_1k_rows": round(1000 * classifier_cost / rows_covered, 6) if rows_covered else None,
    }


def _get(d: dict, dotted: str):
    for part in dotted.split("."):
        d = (d or {}).get(part)
    return d


def gate(candidate: dict, promoted: dict | None) -> list[str]:
    if not promoted:
        return []
    failures = []
    for metric in GATED:
        new, old = _get(candidate, metric), _get(promoted, metric)
        if old is not None and (new is None or new < old - 0.01):
            failures.append(f"{metric}: {old} -> {new}")
    new_cost = _get(candidate, "ops.classifier_cost_per_1k_rows")
    old_cost = _get(promoted, "ops.classifier_cost_per_1k_rows")
    if old_cost and new_cost and new_cost > old_cost * 1.2:
        failures.append(f"classifier_cost_per_1k_rows: {old_cost} -> {new_cost} (> +20%)")
    return failures


def run(cfg: Config, promote: bool = False) -> bool:
    eval_run_id = uuid4()
    llm, s = LLMClient(cfg, eval_run_id=eval_run_id), Seeds()
    taxonomy, buildings = s.taxonomy, s.ref["buildings"]
    with connect(cfg) as conn:
        rows = conn.execute("SELECT count(*) AS n FROM bronze.tickets_raw").fetchone()["n"]

        labels = sorted(classifier.distinct_labels(conn).items())
        label_pred = {r.label: r.model_dump() for r in classifier.classify_labels(llm, labels, taxonomy)}
        templates = [t for _, t in sorted(classifier.distinct_templates(conn, buildings).items())]
        template_pred = {r.template: r.model_dump() for r in classifier.classify_templates(llm, templates, taxonomy)}
        people = sorted(classifier.distinct_people(conn).items())
        people_pred = {r.value: r.model_dump() for r in classifier.resolve_people(llm, people, taxonomy)}
        rules, _ = dq_agent.propose_rules(conn, llm)
        with connect(cfg) as sandbox:
            rule_rows = [r.model_dump() | {"affected_rows": dq_agent.dry_run(sandbox, r.predicate)[0]} for r in rules]
            predicate_scores = score_predicates(sandbox, rule_rows)

        g_labels, g_templates, g_people = _csv("category_labels.csv"), _csv("templates.csv"), _csv("people.csv")
        h_labels = held_out(g_labels, "label", "category_labels")
        h_templates = held_out(g_templates, "template", "description_templates")
        h_people = held_out(g_people, "value", "people")
        junk = set(s.ref["junk_tokens"])
        rule_labels = {g["label"]: classifier.rule_label(g["label"]) for g in g_labels}
        rule_templates = {g["template"]: classifier.rule_template(g["template"]) for g in g_templates}
        rule_people = classifier.rule_people([v for v, _ in people], junk)
        results = {
            "eval_run_id": str(eval_run_id),
            "evaluated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "model": {"bulk": cfg.llm_model_bulk, "reasoning": cfg.llm_model_reasoning, "base_url": cfg.llm_base_url},
            "prompt_versions": {
                p: llm.prompt_version(p) for p in ("category_labels", "description_templates", "people", "dq_rules")
            },
            "labels": {
                "agent": score_labels(label_pred, g_labels),
                "held_out": score_labels(label_pred, h_labels),
                "rules_baseline": score_labels(rule_labels, g_labels),
                "rules_baseline_held_out": score_labels(rule_labels, h_labels),
            },
            "templates": {
                "agent": score_templates(template_pred, g_templates),
                "held_out": score_templates(template_pred, h_templates),
                "rules_baseline": score_templates(rule_templates, g_templates),
                "rules_baseline_held_out": score_templates(rule_templates, h_templates),
            },
            "people": {
                "agent": score_people(people_pred, g_people),
                "held_out": score_people(people_pred, h_people),
                "rules_baseline": score_people(rule_people, g_people),
                "rules_baseline_held_out": score_people(rule_people, h_people),
            },
            "dq": {"agent": score_dq(rule_rows, _csv("dq_issues.csv")), "predicates": predicate_scores},
            "ops": ops_metrics(conn, eval_run_id, rows),
        }

    RESULTS.mkdir(parents=True, exist_ok=True)
    promoted_path = RESULTS / "promoted.json"
    promoted = json.loads(promoted_path.read_text()) if promoted_path.exists() else None
    failures = gate(results, promoted)
    results["gate"] = {
        "passed": not failures,
        "failures": failures,
        "compared_with": promoted.get("evaluated_at") if promoted else None,
    }
    (RESULTS / "latest.json").write_text(json.dumps(results, indent=2))
    if promote and not failures:
        promoted_path.write_text(json.dumps(results, indent=2))
    event("eval_finished", gate_passed=not failures, failures=failures, promoted=promote and not failures)
    print(json.dumps({k: results[k] for k in ("labels", "templates", "people", "dq", "ops", "gate")}, indent=2))
    return not failures
