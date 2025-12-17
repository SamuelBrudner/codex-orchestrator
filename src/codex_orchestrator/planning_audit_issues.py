from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from codex_orchestrator.beads_subprocess import BdIssue, bd_create, bd_list_open_titles, bd_update


@dataclass(frozen=True, slots=True)
class PlannedAuditIssue:
    title: str
    notes: str
    issue_type: str
    priority: int


_SEVERITY_SCORE: dict[str, int] = {
    "critical": 4,
    "high": 3,
    "medium": 2,
    "low": 1,
    "info": 0,
}

_CONFIDENCE_SCORE: dict[str, int] = {
    "high": 3,
    "medium": 2,
    "low": 1,
}


def plan_planning_audit_issues(audit: Mapping[str, Any]) -> tuple[PlannedAuditIssue, ...]:
    findings = audit.get("findings")
    if not isinstance(findings, list):
        return ()

    run_id = str(audit.get("run_id") or "").strip()
    repo_id = str(audit.get("repo_id") or "").strip()

    candidates: list[tuple[tuple[Any, ...], PlannedAuditIssue]] = []
    for finding in findings:
        if not isinstance(finding, dict):
            continue

        finding_title = str(finding.get("title") or "").strip()
        if not finding_title:
            continue

        category = str(finding.get("category") or "").strip()
        severity = str(finding.get("severity") or "info").strip().lower()
        confidence = str(finding.get("confidence") or "").strip().lower()
        recommendation = str(finding.get("recommendation") or "").strip()
        if not recommendation:
            continue
        if severity == "info":
            continue

        evidence_raw = finding.get("evidence_paths") or []
        evidence: list[str] = []
        if isinstance(evidence_raw, list):
            evidence = sorted({p.strip() for p in evidence_raw if isinstance(p, str) and p.strip()})

        issue_title = _format_issue_title(category=category, finding_title=finding_title)
        notes = _format_issue_notes(
            run_id=run_id,
            repo_id=repo_id,
            category=category,
            severity=severity,
            confidence=confidence,
            recommendation=recommendation,
            evidence_paths=evidence,
        )
        issue_type = "task"
        priority = _priority_for_severity(severity)

        severity_score = _SEVERITY_SCORE.get(severity, 0)
        confidence_score = _CONFIDENCE_SCORE.get(confidence, 0)
        sort_key = (-severity_score, -confidence_score, category, finding_title, issue_title)
        candidates.append((sort_key, PlannedAuditIssue(issue_title, notes, issue_type, priority)))

    candidates.sort(key=lambda item: item[0])
    planned: list[PlannedAuditIssue] = []
    seen_titles: set[str] = set()
    for _, issue in candidates:
        if issue.title in seen_titles:
            continue
        seen_titles.add(issue.title)
        planned.append(issue)

    return tuple(planned)


def create_planning_audit_issues(
    *,
    repo_root: Path,
    audit: Mapping[str, Any],
    limit: int,
) -> list[BdIssue]:
    if limit <= 0:
        return []

    planned = plan_planning_audit_issues(audit)
    if not planned:
        return []

    open_titles = bd_list_open_titles(repo_root=repo_root)
    created: list[BdIssue] = []
    for issue in planned:
        if len(created) >= limit:
            break
        if issue.title in open_titles:
            continue
        created_issue = bd_create(
            repo_root=repo_root,
            title=issue.title,
            issue_type=issue.issue_type,
            priority=issue.priority,
        )
        created_issue = bd_update(
            repo_root=repo_root,
            issue_id=created_issue.issue_id,
            notes=issue.notes,
        )
        created.append(created_issue)
        open_titles.add(issue.title)

    return created


def _priority_for_severity(severity: str) -> int:
    sev = severity.strip().lower()
    if sev == "critical":
        return 0
    if sev == "high":
        return 1
    if sev == "medium":
        return 2
    if sev == "low":
        return 3
    return 4


def _format_issue_title(*, category: str, finding_title: str) -> str:
    prefix = "planning-audit"
    if category:
        return f"{prefix}({category}): {finding_title}"
    return f"{prefix}: {finding_title}"


def _format_issue_notes(
    *,
    run_id: str,
    repo_id: str,
    category: str,
    severity: str,
    confidence: str,
    recommendation: str,
    evidence_paths: list[str],
) -> str:
    header = "Planning audit finding"
    context_bits: list[str] = []
    if repo_id:
        context_bits.append(f"repo_id={repo_id}")
    if run_id:
        context_bits.append(f"run_id={run_id}")
    if context_bits:
        header += f" ({', '.join(context_bits)})"

    lines: list[str] = [header, ""]
    if category:
        lines.append(f"- Category: {category}")
    if severity:
        lines.append(f"- Severity: {severity}")
    if confidence:
        lines.append(f"- Confidence: {confidence}")
    lines.append(f"- Recommendation: {recommendation}")

    if evidence_paths:
        lines.append("")
        lines.append("Evidence:")
        for p in evidence_paths[:25]:
            lines.append(f"- {p}")

    return "\n".join(lines).rstrip() + "\n"

