from __future__ import annotations

from codex_orchestrator.planning_audit_issues import plan_planning_audit_issues


def test_plan_planning_audit_issues_filters_and_orders_deterministically() -> None:
    audit = {
        "run_id": "run-1",
        "repo_id": "demo",
        "findings": [
            {
                "category": "semantic_modeling",
                "title": "Informational finding",
                "severity": "info",
                "confidence": "high",
                "recommendation": "noop",
            },
            {
                "category": "semantic_registry",
                "title": "Missing semantics registry",
                "severity": "medium",
                "confidence": "high",
                "recommendation": "Add metadata/semantics/semantics.yml",
                "evidence_paths": ["b.py", "a.py", "a.py"],
            },
            {
                "category": "semantic_modeling_consistency",
                "title": "Multiple paradigms",
                "severity": "low",
                "confidence": "medium",
                "recommendation": "Prefer one primary approach",
                "evidence_paths": [],
            },
            {
                "category": "semantic_registry",
                "title": "High severity item",
                "severity": "high",
                "confidence": "low",
                "recommendation": "Do the thing",
            },
        ],
    }

    planned = plan_planning_audit_issues(audit)
    assert [p.title for p in planned] == [
        "planning-audit(semantic_registry): High severity item",
        "planning-audit(semantic_registry): Missing semantics registry",
        "planning-audit(semantic_modeling_consistency): Multiple paradigms",
    ]
    assert [p.priority for p in planned] == [1, 2, 3]
