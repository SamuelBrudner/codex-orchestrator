from __future__ import annotations

from pathlib import Path
from typing import Any


def _build_findings(
    *,
    repo_id: str,
    semantics_yml: Path | None,
    signals: dict[str, Any],
) -> list[dict[str, Any]]:
    del semantics_yml
    findings: list[dict[str, Any]] = []
    _append_if(findings, _finding_duplicate_model_shapes(signals))
    _append_if(findings, _finding_duplicate_model_modules(signals))
    _append_if(findings, _finding_multiple_model_paradigms(signals))
    _append_if(findings, _finding_repeated_config_parsing(signals))
    if not findings:
        findings.append(_finding_no_issues())
    for finding in findings:
        finding.setdefault("repo_id", repo_id)
    return findings


def _append_if(findings: list[dict[str, Any]], finding: dict[str, Any] | None) -> None:
    if finding is not None:
        findings.append(finding)


def _finding_duplicate_model_shapes(signals: dict[str, Any]) -> dict[str, Any] | None:
    block = signals.get("model_shape_duplicates")
    if not isinstance(block, dict):
        return None
    groups = block.get("groups")
    if not isinstance(groups, list) or not groups:
        return None

    paths: set[str] = set()
    has_cross_paradigm = False
    counts: list[int] = []
    for group in groups:
        if not isinstance(group, dict):
            continue
        counts.append(int(group.get("count", 0) or 0))
        kinds = group.get("kinds")
        if isinstance(kinds, list) and len(
            {k.strip() for k in kinds if isinstance(k, str) and k.strip()}
        ) >= 2:
            has_cross_paradigm = True
        occurrences = group.get("occurrences")
        if not isinstance(occurrences, list):
            continue
        for occ in occurrences:
            if isinstance(occ, dict):
                path = occ.get("path")
                if isinstance(path, str) and path.strip():
                    paths.add(path.strip())

    max_count = max(counts) if counts else 0
    severity = "medium" if max_count >= 3 or len(groups) >= 2 else "low"
    confidence = "high" if has_cross_paradigm else "medium"
    rationale = [
        f"trigger:model_shape_duplicates(groups={len(groups)})",
        "signal:exact_field_set_match(min_fields=3)",
    ]
    if has_cross_paradigm:
        rationale.append("signal:cross_paradigm_duplicates")

    return {
        "category": "semantic_modeling_dry",
        "title": "Duplicated DTO/model shapes detected",
        "severity": severity,
        "confidence": confidence,
        "confidence_rationale": rationale,
        "evidence_paths": sorted(paths),
        "details": {
            "duplicate_shape_groups": groups,
            "truncated": bool(block.get("truncated")),
        },
        "recommendation": (
            "Consider consolidating these representations into a single authoritative "
            "model (or clear layer boundaries + explicit conversion)."
        ),
    }


def _finding_duplicate_model_modules(signals: dict[str, Any]) -> dict[str, Any] | None:
    model_modules = signals.get("model_modules")
    if not isinstance(model_modules, list) or len(model_modules) < 2:
        return None
    return {
        "category": "semantic_modeling_dry",
        "title": "Potential duplicated domain-model modules",
        "severity": "low",
        "confidence": "low",
        "confidence_rationale": [
            f"trigger:multiple_modelish_modules(count={len(model_modules)})"
        ],
        "evidence_paths": _sorted_str_paths(model_modules),
        "recommendation": (
            "Consider consolidating domain models/schemas into a single authoritative "
            "module (or clearly separated layers)."
        ),
    }


def _sorted_str_paths(items: Any) -> list[str]:
    if not isinstance(items, list):
        return []
    return sorted({str(p) for p in items if isinstance(p, str) and p.strip()})


def _finding_multiple_model_paradigms(signals: dict[str, Any]) -> dict[str, Any] | None:
    pydantic = signals.get("pydantic_models")
    dataclasses = signals.get("dataclass_models")
    typed_dicts = signals.get("typed_dicts")
    if _nonempty_list_count(pydantic, dataclasses, typed_dicts) < 2:
        return None
    paradigms: list[str] = []
    if isinstance(pydantic, list) and pydantic:
        paradigms.append("pydantic")
    if isinstance(dataclasses, list) and dataclasses:
        paradigms.append("dataclass")
    if isinstance(typed_dicts, list) and typed_dicts:
        paradigms.append("typed_dict")
    return {
        "category": "semantic_modeling_consistency",
        "title": "Multiple model paradigms detected (Pydantic/dataclass/TypedDict)",
        "severity": "info",
        "confidence": "low",
        "confidence_rationale": [
            f"trigger:multiple_model_paradigms(paradigms={','.join(paradigms)})"
        ],
        "evidence_paths": _merge_paths(pydantic, dataclasses, typed_dicts),
        "recommendation": (
            "Prefer one primary modeling approach for core domain entities to reduce "
            "conceptual drift."
        ),
    }


def _finding_repeated_config_parsing(signals: dict[str, Any]) -> dict[str, Any] | None:
    patterns = signals.get("config_patterns")
    if not isinstance(patterns, dict):
        return None

    hot: list[tuple[str, list[str]]] = []
    for pattern in sorted(patterns):
        paths = patterns.get(pattern)
        if not isinstance(paths, list):
            continue
        uniq = sorted({p.strip() for p in paths if isinstance(p, str) and p.strip()})
        if len(uniq) >= 3:
            hot.append((str(pattern), uniq))

    if not hot:
        return None

    evidence: set[str] = set()
    for _, paths in hot:
        evidence.update(paths)

    hot_details = [
        {"pattern": pattern, "paths": paths, "count": len(paths)}
        for pattern, paths in hot[:10]
    ]

    return {
        "category": "config_parsing_dry",
        "title": "Repeated config parsing patterns detected",
        "severity": "info",
        "confidence": "low",
        "confidence_rationale": [
            f"trigger:repeated_config_parsing(patterns={len(hot)})",
            "signal:config_patterns_repeated_in_3+_files",
        ],
        "evidence_paths": sorted(evidence),
        "details": {
            "hot_patterns": hot_details,
            "truncated": len(hot) > len(hot_details),
        },
        "recommendation": (
            "Consider centralizing configuration loading/validation (env vars, config "
            "files, CLI args) into a single module or settings layer."
        ),
    }


def _nonempty_list_count(*groups: Any) -> int:
    return sum(1 for group in groups if isinstance(group, list) and group)


def _finding_no_issues() -> dict[str, Any]:
    return {
        "category": "semantic_modeling",
        "title": "No major semantic-modeling issues detected by heuristic scan",
        "severity": "info",
        "confidence": "low",
        "confidence_rationale": ["trigger:no_findings_emitted"],
        "evidence_paths": [],
        "recommendation": (
            "If this repo is expected to contain a domain model, consider adding a "
            "semantics registry and explicit modeling conventions."
        ),
    }


def _merge_paths(*groups: Any) -> list[str]:
    out: set[str] = set()
    for group in groups:
        if not isinstance(group, list):
            continue
        for item in group:
            if isinstance(item, str) and item.strip():
                out.add(item)
    return sorted(out)


def _score_summary(findings: list[dict[str, Any]]) -> dict[str, Any]:
    order = {"critical": 4, "high": 3, "medium": 2, "low": 1, "info": 0}
    max_sev = "info"
    for finding in findings:
        severity = str(finding.get("severity") or "info")
        if order.get(severity, 0) > order.get(max_sev, 0):
            max_sev = severity
    return {"overall_severity": max_sev, "findings_count": len(findings)}
