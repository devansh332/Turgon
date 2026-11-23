from medallion.agents.evals import gate, score_templates


def test_gate_fails_on_ops_regression_and_cost_rise():
    old = {"ops": {"first_try_valid_rate": 1.0, "classifier_cost_per_1k_rows": 0.01}}
    new = {"ops": {"first_try_valid_rate": 0.9, "classifier_cost_per_1k_rows": 0.02}}
    assert len(gate(new, old)) == 2
    assert gate(old, old) == []


def test_gate_fails_on_accuracy_drop():
    assert gate({"dq": {"agent": {"recall": 0.5}}}, {"dq": {"agent": {"recall": 0.8}}})


def test_safety_recall_detects_missed_hazards_despite_high_accuracy():
    gold = [
        {
            "template": "hazard",
            "is_description": "true",
            "category": "Electrical",
            "is_safety_issue": "true",
            "urgency": "high",
        },
        {
            "template": "request",
            "is_description": "true",
            "category": "Electrical",
            "is_safety_issue": "false",
            "urgency": "low",
        },
    ]
    pred = {
        g["template"]: {
            "is_description": True,
            "category": "Electrical",
            "is_safety_issue": False,
            "urgency": g["urgency"],
        }
        for g in gold
    }
    scores = score_templates(pred, gold)
    assert scores["safety_accuracy"] == 0.5
    assert scores["safety_recall"] == 0
