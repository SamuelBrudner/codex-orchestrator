from __future__ import annotations

from typing import Any

from codex_orchestrator.planning_audit_collect import _collect_and_inventory
from codex_orchestrator.planning_audit_findings import _build_findings
from codex_orchestrator.planning_audit_render import (
    _assemble_audit,
    _assemble_inputs,
    format_planning_audit_md,
)
from codex_orchestrator.planning_audit_scan import _scan_semantic_signals
from codex_orchestrator.planning_audit_types import (
    _DEFAULT_MAX_FILES,
    _DEFAULT_MAX_PYTHON_FILES_SCANNED,
    PlanningAuditArtifacts,
    PlanningAuditError,
)
from codex_orchestrator.repo_inventory import RepoPolicy


def build_planning_audit(
    *,
    run_id: str,
    repo_policy: RepoPolicy,
    max_files: int = _DEFAULT_MAX_FILES,
    max_python_files_scanned: int = _DEFAULT_MAX_PYTHON_FILES_SCANNED,
) -> dict[str, Any]:
    repo_root = repo_policy.path
    collection, inv = _collect_and_inventory(
        repo_root,
        repo_policy=repo_policy,
        max_files=max_files,
    )
    signals = _scan_semantic_signals(
        repo_root,
        inv.python_files,
        max_python_files_scanned=max_python_files_scanned,
    )
    findings = _build_findings(
        repo_id=repo_policy.repo_id,
        semantics_yml=inv.semantics_yml,
        signals=signals,
    )
    inputs = _assemble_inputs(
        run_id=run_id,
        repo_policy=repo_policy,
        repo_root=repo_root,
        inv=inv,
        collection=collection,
        signals=signals,
        findings=findings,
        max_files=max_files,
        max_python_files_scanned=max_python_files_scanned,
    )
    return _assemble_audit(inputs)


__all__ = [
    "PlanningAuditArtifacts",
    "PlanningAuditError",
    "build_planning_audit",
    "format_planning_audit_md",
]
