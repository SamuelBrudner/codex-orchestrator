from __future__ import annotations

from pathlib import Path
from typing import Any

from codex_orchestrator.planning_audit_findings import _score_summary
from codex_orchestrator.planning_audit_types import _AssembleInputs, _FileCollection, _Inventory
from codex_orchestrator.repo_inventory import RepoPolicy


def _assemble_inputs(
    *,
    run_id: str,
    repo_policy: RepoPolicy,
    repo_root: Path,
    inv: _Inventory,
    collection: _FileCollection,
    signals: dict[str, Any],
    findings: list[dict[str, Any]],
    max_files: int,
    max_python_files_scanned: int,
) -> _AssembleInputs:
    status, notes, actions = _generation_status(
        collection=collection,
        inv=inv,
        scan=signals.get("scan"),
    )
    if inv.semantics_yml is None:
        notes.append(
            "No semantics registry detected; heuristic audit will rely on file structure only."
        )
        actions.append(
            "Consider adding metadata/semantics/semantics.yml if this repo has stable "
            "domain entities."
        )
    return _AssembleInputs(
        run_id,
        repo_policy,
        repo_root,
        inv,
        collection,
        status,
        notes,
        actions,
        max_files,
        max_python_files_scanned,
        signals,
        findings,
    )


def _generation_status(
    *,
    collection: _FileCollection,
    inv: _Inventory,
    scan: Any,
) -> tuple[str, list[str], list[str]]:
    return _audit_status(
        inventory_paths_count=len(collection.rel_paths),
        collection_truncated=collection.truncated,
        collection_errors=collection.errors,
        python_files_count=len(inv.python_files),
        scan=scan,
    )


def _assemble_audit(inputs: _AssembleInputs) -> dict[str, Any]:
    return _audit_base_from_inputs(inputs) | _audit_sections_from_inputs(inputs)


def _audit_base_from_inputs(inputs: _AssembleInputs) -> dict[str, Any]:
    return _audit_base(
        run_id=inputs.run_id,
        repo_id=inputs.repo_policy.repo_id,
        repo_root=inputs.repo_root,
        audit_status=inputs.audit_status,
        audit_notes=inputs.audit_notes,
        next_actions=inputs.next_actions,
    )


def _audit_sections_from_inputs(inputs: _AssembleInputs) -> dict[str, Any]:
    return _audit_sections(
        inv=inputs.inv,
        collection=inputs.collection,
        repo_policy=inputs.repo_policy,
        max_files=inputs.max_files,
        max_python_files_scanned=inputs.max_python_files_scanned,
        signals=inputs.signals,
        findings=inputs.findings,
    )


def _audit_sections_base(
    inv: _Inventory,
    collection: _FileCollection,
    repo_policy: RepoPolicy,
    max_files: int,
    max_python_files_scanned: int,
) -> dict[str, Any]:
    return {
        "limits": _audit_limits(max_files, max_python_files_scanned),
        "collection": _audit_collection(collection),
        "inputs": _audit_inputs(repo_policy),
        "inventory": _audit_inventory(inv),
    }


def _audit_sections(
    *,
    inv: _Inventory,
    collection: _FileCollection,
    repo_policy: RepoPolicy,
    max_files: int,
    max_python_files_scanned: int,
    signals: dict[str, Any],
    findings: list[dict[str, Any]],
) -> dict[str, Any]:
    base = _audit_sections_base(
        inv,
        collection,
        repo_policy,
        max_files,
        max_python_files_scanned,
    )
    return base | {"signals": signals, "findings": findings, "summary": _score_summary(findings)}


def _audit_base(
    *,
    run_id: str,
    repo_id: str,
    repo_root: Path,
    audit_status: str,
    audit_notes: list[str],
    next_actions: list[str],
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "run_id": run_id,
        "repo_id": repo_id,
        "repo_path": repo_root.as_posix(),
        "audit_status": audit_status,
        "audit_notes": audit_notes,
        "next_actions": next_actions,
    }


def _audit_limits(max_files: int, max_python_files_scanned: int) -> dict[str, int]:
    return {
        "max_files": int(max_files),
        "max_python_files_scanned": int(max_python_files_scanned),
    }


def _audit_collection(collection: _FileCollection) -> dict[str, Any]:
    return {"truncated": bool(collection.truncated), "errors": collection.errors}


def _audit_inputs(repo_policy: RepoPolicy) -> dict[str, list[str]]:
    return {
        "allowed_roots": [p.as_posix() for p in repo_policy.allowed_roots],
        "deny_roots": [p.as_posix() for p in repo_policy.deny_roots],
    }


def _audit_inventory(inv: _Inventory) -> dict[str, Any]:
    return {
        "python_files_count": len(inv.python_files),
        "notebooks_count": len(inv.notebook_files),
        "config_files_count": len(inv.config_files),
        "semantics_yml": inv.semantics_yml.as_posix() if inv.semantics_yml else None,
    }


def format_planning_audit_md(audit: dict[str, Any]) -> str:
    lines: list[str] = []
    _md_append_header(lines, audit)
    _md_append_generation(lines, audit)
    _md_append_inventory(lines, audit)
    _md_append_findings(lines, audit)
    return "\n".join(lines)


def _md_append_header(lines: list[str], audit: dict[str, Any]) -> None:
    repo_id = str(audit.get("repo_id") or "<unknown>")
    run_id = str(audit.get("run_id") or "<unknown>")
    severity = _md_severity(audit)
    lines.extend(
        [
            f"# Planning Audit ({repo_id})",
            "",
            "## Run",
            f"- RUN_ID: `{run_id}`",
            "",
            "## Summary",
            f"- Overall severity: `{severity}`",
            "",
        ]
    )


def _md_severity(audit: dict[str, Any]) -> str:
    summary = audit.get("summary")
    if not isinstance(summary, dict):
        return "unknown"
    return str(summary.get("overall_severity") or "unknown")


def _md_append_generation(lines: list[str], audit: dict[str, Any]) -> None:
    status = str(audit.get("audit_status") or "unknown")
    notes = audit.get("audit_notes")
    actions = audit.get("next_actions")
    if status == "unknown" and not notes and not actions:
        return
    lines.append("## Generation")
    lines.append(f"- Status: `{status}`")
    _md_append_items(lines, label="Note", items=notes)
    _md_append_items(lines, label="Next action", items=actions)
    lines.append("")


def _md_append_items(lines: list[str], *, label: str, items: Any) -> None:
    if not isinstance(items, list) or not items:
        return
    for item in items[:25]:
        if isinstance(item, str) and item.strip():
            lines.append(f"- {label}: {item}")


def _md_append_inventory(lines: list[str], audit: dict[str, Any]) -> None:
    inventory = audit.get("inventory")
    if not isinstance(inventory, dict):
        return
    lines.append("## Inventory")
    lines.append(f"- Python files: {int(inventory.get('python_files_count', 0) or 0)}")
    lines.append(f"- Notebooks: {int(inventory.get('notebooks_count', 0) or 0)}")
    lines.append(f"- Config files: {int(inventory.get('config_files_count', 0) or 0)}")
    lines.append(_md_semantics_line(inventory))
    lines.append("")


def _md_semantics_line(inventory: dict[str, Any]) -> str:
    semantics = inventory.get("semantics_yml")
    if isinstance(semantics, str) and semantics.strip():
        return f"- Semantics registry: `{semantics}`"
    return "- Semantics registry: <missing>"


def _md_append_findings(lines: list[str], audit: dict[str, Any]) -> None:
    findings = audit.get("findings")
    lines.append("## Findings")
    if not isinstance(findings, list) or not findings:
        lines.extend(["- None", ""])
        return
    for finding in findings:
        lines.extend(_md_finding_lines(finding))
    lines.append("")


def _md_finding_lines(finding: Any) -> list[str]:
    if not isinstance(finding, dict):
        return []
    title = str(finding.get("title") or "<untitled>")
    category = str(finding.get("category") or "<uncategorized>")
    severity_item = str(finding.get("severity") or "unknown")
    confidence_item = str(finding.get("confidence") or "unknown")
    lines = [f"- **{title}** (`{category}`, severity=`{severity_item}`)"]
    lines.append(f"  - Confidence: `{confidence_item}`")
    rationale = finding.get("confidence_rationale")
    if isinstance(rationale, list) and rationale:
        shown = [str(r) for r in rationale if isinstance(r, str) and r.strip()]
        if shown:
            lines.append(f"  - Confidence rationale: {', '.join(shown[:5])}")
    recommendation = str(finding.get("recommendation") or "").strip()
    if recommendation:
        lines.append(f"  - Recommendation: {recommendation}")
    lines.extend(_md_evidence_lines(finding.get("evidence_paths")))
    return lines


def _md_evidence_lines(evidence: Any) -> list[str]:
    if not isinstance(evidence, list) or not evidence:
        return []
    shown = [str(path) for path in evidence if isinstance(path, str) and path.strip()]
    return [f"  - `{path}`" for path in sorted(set(shown))[:25]]


def _audit_status(
    *,
    inventory_paths_count: int,
    collection_truncated: bool,
    collection_errors: list[dict[str, str]],
    python_files_count: int,
    scan: Any,
) -> tuple[str, list[str], list[str]]:
    if inventory_paths_count == 0:
        return _audit_skipped_no_files()
    status, notes, actions = _audit_status_base(
        collection_truncated=collection_truncated,
        collection_errors=collection_errors,
        python_files_count=python_files_count,
    )
    return _audit_status_with_scan(status=status, notes=notes, actions=actions, scan=scan)


def _audit_skipped_no_files() -> tuple[str, list[str], list[str]]:
    return (
        "skipped",
        ["No files found under allowed_roots (or all were denied)."],
        ["Verify allowed_roots/deny_roots include the intended code/config locations."],
    )


def _audit_status_base(
    *,
    collection_truncated: bool,
    collection_errors: list[dict[str, str]],
    python_files_count: int,
) -> tuple[str, list[str], list[str]]:
    notes, actions = _collection_notes_actions(
        collection_truncated=collection_truncated,
        collection_errors=collection_errors,
    )
    if python_files_count == 0:
        notes.append("No Python files detected; semantic scan is limited.")
    status = "partial" if collection_truncated or collection_errors else "ok"
    return status, notes, actions


def _collection_notes_actions(
    *,
    collection_truncated: bool,
    collection_errors: list[dict[str, str]],
) -> tuple[list[str], list[str]]:
    notes: list[str] = []
    actions: list[str] = []
    if collection_truncated:
        notes.append("Inventory truncated due to max_files limit.")
        actions.append("Increase max_files if you need a fuller audit for this repo.")
    if collection_errors:
        notes.append("Some paths could not be traversed during inventory.")
        actions.append("Fix filesystem permissions or adjust allowed_roots/deny_roots.")
    return notes, actions


def _audit_status_with_scan(
    *,
    status: str,
    notes: list[str],
    actions: list[str],
    scan: Any,
) -> tuple[str, list[str], list[str]]:
    if not isinstance(scan, dict):
        return status, notes, actions
    if bool(scan.get("truncated")):
        status = "partial"
        notes.append("Semantic scan truncated due to max_python_files_scanned limit.")
        actions.append(
            "Increase max_python_files_scanned if you need deeper scanning for this repo."
        )
    failures = scan.get("read_failures")
    if isinstance(failures, list) and failures:
        status = "partial"
        notes.append("Some Python files could not be read as UTF-8 during semantic scan.")
        actions.append("Inspect unreadable/binary files or adjust audit limits/permissions if needed.")
    return status, notes, actions
