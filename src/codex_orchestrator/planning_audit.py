from __future__ import annotations

import os
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from codex_orchestrator.repo_inventory import RepoPolicy


class PlanningAuditError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class PlanningAuditArtifacts:
    json_path: Path
    md_path: Path


_DEFAULT_MAX_FILES: int = 5_000
_DEFAULT_MAX_PYTHON_FILES_SCANNED: int = 500


@dataclass(frozen=True, slots=True)
class _FileCollection:
    rel_paths: list[Path]
    truncated: bool
    errors: list[dict[str, str]]


@dataclass(frozen=True, slots=True)
class _TextReadResult:
    text: str
    status: str
    truncated: bool


@dataclass(frozen=True, slots=True)
class _Inventory:
    python_files: list[Path]
    notebook_files: list[Path]
    config_files: list[Path]
    semantics_yml: Path | None


@dataclass(frozen=True, slots=True)
class _AssembleInputs:
    run_id: str
    repo_policy: RepoPolicy
    repo_root: Path
    inv: _Inventory
    collection: _FileCollection
    audit_status: str
    audit_notes: list[str]
    next_actions: list[str]
    max_files: int
    max_python_files_scanned: int
    signals: dict[str, Any]
    findings: list[dict[str, Any]]


def build_planning_audit(
    *,
    run_id: str,
    repo_policy: RepoPolicy,
    max_files: int = _DEFAULT_MAX_FILES,
    max_python_files_scanned: int = _DEFAULT_MAX_PYTHON_FILES_SCANNED,
) -> dict[str, Any]:
    repo_root = repo_policy.path
    collection, inv = _collect_and_inventory(repo_root, repo_policy=repo_policy, max_files=max_files)
    signals, findings = _signals_and_findings(repo_root, inv=inv, repo_id=repo_policy.repo_id, max_python_files_scanned=max_python_files_scanned)
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
    status, notes, actions = _generation_status(collection=collection, inv=inv, scan=signals.get("scan"))
    return _AssembleInputs(
        run_id=run_id,
        repo_policy=repo_policy,
        repo_root=repo_root,
        inv=inv,
        collection=collection,
        audit_status=status,
        audit_notes=notes,
        next_actions=actions,
        max_files=max_files,
        max_python_files_scanned=max_python_files_scanned,
        signals=signals,
        findings=findings,
    )


def _signals_and_findings(
    repo_root: Path,
    *,
    inv: _Inventory,
    repo_id: str,
    max_python_files_scanned: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    signals = _scan_semantic_signals(
        repo_root,
        inv.python_files,
        max_python_files_scanned=max_python_files_scanned,
    )
    findings = _build_findings(repo_id=repo_id, semantics_yml=inv.semantics_yml, signals=signals)
    return signals, findings


def _generation_status(
    *, collection: _FileCollection, inv: _Inventory, scan: Any
) -> tuple[str, list[str], list[str]]:
    return _audit_status(
        inventory_paths_count=len(collection.rel_paths),
        collection_truncated=collection.truncated,
        collection_errors=collection.errors,
        python_files_count=len(inv.python_files),
        scan=scan,
    )


def _collect_and_inventory(
    repo_root: Path,
    *,
    repo_policy: RepoPolicy,
    max_files: int,
) -> tuple[_FileCollection, _Inventory]:
    collection = _collect_repo_files(
        repo_root,
        allowed_roots=repo_policy.allowed_roots,
        deny_roots=repo_policy.deny_roots,
        max_files=max_files,
    )
    return collection, _inventory_from_paths(collection.rel_paths)


def _inventory_from_paths(rel_paths: list[Path]) -> _Inventory:
    python_files = [p for p in rel_paths if p.suffix == ".py"]
    notebook_files = [p for p in rel_paths if p.suffix == ".ipynb"]
    config_files = [p for p in rel_paths if p.suffix in {".yml", ".yaml", ".json", ".toml", ".ini", ".cfg"}]
    return _Inventory(
        python_files=python_files,
        notebook_files=notebook_files,
        config_files=config_files,
        semantics_yml=_find_semantics_file(rel_paths),
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
    return {
        "limits": _audit_limits(max_files, max_python_files_scanned),
        "collection": _audit_collection(collection),
        "inputs": _audit_inputs(repo_policy),
        "inventory": _audit_inventory(inv),
        "signals": signals,
        "findings": findings,
        "summary": _score_summary(findings),
    }


def _audit_base(*, run_id: str, repo_id: str, repo_root: Path, audit_status: str, audit_notes: list[str], next_actions: list[str]) -> dict[str, Any]:
    return dict(
        schema_version=1,
        run_id=run_id,
        repo_id=repo_id,
        repo_path=repo_root.as_posix(),
        audit_status=audit_status,
        audit_notes=audit_notes,
        next_actions=next_actions,
    )


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
    lines = [f"- **{title}** (`{category}`, severity=`{severity_item}`)"]
    rec = str(finding.get("recommendation") or "").strip()
    if rec:
        lines.append(f"  - Recommendation: {rec}")
    lines.extend(_md_evidence_lines(finding.get("evidence_paths")))
    return lines


def _md_evidence_lines(evidence: Any) -> list[str]:
    if not isinstance(evidence, list) or not evidence:
        return []
    shown = [str(p) for p in evidence if isinstance(p, str) and p.strip()]
    return [f"  - `{p}`" for p in sorted(set(shown))[:25]]


def _collect_repo_files(
    repo_root: Path,
    *,
    allowed_roots: tuple[Path, ...],
    deny_roots: tuple[Path, ...],
    max_files: int,
) -> _FileCollection:
    if max_files <= 0:
        raise PlanningAuditError(f"max_files must be > 0, got {max_files}")

    rel_paths: list[Path] = []
    errors: list[dict[str, str]] = []
    truncated = False

    allowed = [repo_root / p for p in allowed_roots]
    deny = [repo_root / p for p in deny_roots]

    for root in sorted({p for p in allowed}, key=lambda p: p.as_posix()):
        if truncated:
            break
        if not root.exists() or not root.is_dir():
            continue
        for rel in _iter_files_under_root(repo_root=repo_root, root=root, deny=deny, errors=errors):
            rel_paths.append(rel)
            if len(rel_paths) >= max_files:
                truncated = True
                break

    rel_paths.sort(key=lambda p: p.as_posix())
    return _FileCollection(rel_paths=rel_paths, truncated=truncated, errors=errors)


def _iter_files_under_root(
    *, repo_root: Path, root: Path, deny: list[Path], errors: list[dict[str, str]]
) -> Iterator[Path]:

    def _on_error(e: OSError) -> None:
        errors.append(
            {
                "kind": "walk_error",
                "path": str(getattr(e, "filename", "") or root.as_posix()),
                "error": f"{type(e).__name__}: {e}",
            }
        )

    for dirpath, dirnames, filenames in os.walk(root, topdown=True, onerror=_on_error):
        dir_path = Path(dirpath)
        if any(_is_within(dir_path, d) for d in deny):
            dirnames[:] = []
            continue
        dirnames[:] = sorted(dirnames)
        for name in sorted(filenames):
            abs_path = dir_path / name
            if any(_is_within(abs_path, d) for d in deny):
                continue
            try:
                rel = abs_path.relative_to(repo_root)
            except ValueError:
                continue
            if rel.is_absolute() or ".." in rel.parts:
                continue
            yield rel


def _is_within(path: Path, root: Path) -> bool:
    if root == Path(".") or root == Path():
        return True
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _find_semantics_file(paths: list[Path]) -> Path | None:
    candidate = Path("metadata") / "semantics" / "semantics.yml"
    if candidate in paths:
        return candidate
    alt = Path("metadata") / "semantics" / "semantics.yaml"
    if alt in paths:
        return alt
    return None


def _read_text_limited(path: Path, *, byte_limit: int = 200_000) -> str:
    return _read_text_limited_with_status(path, byte_limit=byte_limit).text


def _read_bytes(path: Path) -> bytes:
    return path.read_bytes()


def _read_text_limited_with_status(
    path: Path,
    *,
    byte_limit: int = 200_000,
) -> _TextReadResult:
    try:
        data = _read_bytes(path)
    except FileNotFoundError:
        return _TextReadResult(text="", status="missing", truncated=False)
    except PermissionError as e:
        return _TextReadResult(text=f"{type(e).__name__}: {e}", status="unreadable", truncated=False)
    except OSError as e:
        return _TextReadResult(text=f"{type(e).__name__}: {e}", status="unreadable", truncated=False)

    truncated = False
    if len(data) > byte_limit:
        truncated = True
        data = data[:byte_limit]
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return _TextReadResult(text="", status="binary", truncated=truncated)

    return _TextReadResult(text=text, status="ok", truncated=truncated)


def _scan_semantic_signals(
    repo_root: Path,
    python_files: list[Path],
    *,
    max_python_files_scanned: int,
) -> dict[str, Any]:
    if max_python_files_scanned <= 0:
        raise PlanningAuditError(
            f"max_python_files_scanned must be > 0, got {max_python_files_scanned}"
        )

    pydantic_files: set[str] = set()
    dataclass_files: set[str] = set()
    typed_dict_files: set[str] = set()
    sqlalchemy_files: set[str] = set()

    modelish_by_name: set[str] = set()

    read_failures: list[dict[str, str]] = []
    total = len(python_files)
    truncated = total > max_python_files_scanned
    scanned = python_files[:max_python_files_scanned]
    for rel in scanned:
        result = _read_text_limited_with_status(repo_root / rel)
        if result.status != "ok":
            read_failures.append(
                {
                    "path": rel.as_posix(),
                    "status": result.status,
                    "detail": result.text,
                }
            )
            continue
        text = result.text

        if "pydantic" in text or "BaseModel" in text:
            pydantic_files.add(rel.as_posix())
        if "@dataclass" in text or "dataclasses import dataclass" in text:
            dataclass_files.add(rel.as_posix())
        if "TypedDict" in text:
            typed_dict_files.add(rel.as_posix())
        if "sqlalchemy" in text:
            sqlalchemy_files.add(rel.as_posix())

        if _looks_like_model_module(rel):
            modelish_by_name.add(rel.as_posix())

    return {
        "scan": {
            "python_files_total": total,
            "python_files_scanned": len(scanned),
            "truncated": truncated,
            "read_failures": read_failures,
        },
        "model_modules": sorted(modelish_by_name),
        "pydantic_models": sorted(pydantic_files),
        "dataclass_models": sorted(dataclass_files),
        "typed_dicts": sorted(typed_dict_files),
        "sqlalchemy": sorted(sqlalchemy_files),
    }


def _audit_status(
    *,
    inventory_paths_count: int,
    collection_truncated: bool,
    collection_errors: list[dict[str, str]],
    python_files_count: int,
    scan: Any,
) -> tuple[str, list[str], list[str]]:
    notes: list[str] = []
    actions: list[str] = []
    status = "ok"

    if inventory_paths_count == 0:
        return (
            "skipped",
            ["No files found under allowed_roots (or all were denied)."],
            ["Verify allowed_roots/deny_roots include the intended code/config locations."],
        )

    if collection_truncated:
        status = "partial"
        notes.append("Inventory truncated due to max_files limit.")
        actions.append("Increase max_files if you need a fuller audit for this repo.")
    if collection_errors:
        status = "partial"
        notes.append("Some paths could not be traversed during inventory.")
        actions.append("Fix filesystem permissions or adjust allowed_roots/deny_roots.")

    if python_files_count == 0:
        notes.append("No Python files detected; semantic scan is limited.")

    if isinstance(scan, dict):
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
            actions.append(
                "Inspect unreadable/binary files or adjust audit limits/permissions if needed."
            )

    return status, notes, actions


def _looks_like_model_module(path: Path) -> bool:
    name = path.name.lower()
    return any(token in name for token in ("schema", "model", "types", "entities", "dto"))


def _build_findings(
    *,
    repo_id: str,
    semantics_yml: Path | None,
    signals: dict[str, Any],
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []

    if semantics_yml is None:
        findings.append(
            {
                "category": "semantic_registry",
                "title": "No semantics registry detected",
                "severity": "medium",
                "confidence": "high",
                "evidence_paths": [],
                "recommendation": (
                    "Add metadata/semantics/semantics.yml to register core entities and canonical "
                    "functions."
                ),
            }
        )

    model_modules = signals.get("model_modules")
    if isinstance(model_modules, list) and len(model_modules) >= 2:
        findings.append(
            {
                "category": "semantic_modeling_dry",
                "title": "Potential duplicated domain-model modules",
                "severity": "low",
                "confidence": "low",
                "evidence_paths": sorted(
                    {
                        str(p)
                        for p in model_modules
                        if isinstance(p, str) and p.strip()
                    }
                ),
                "recommendation": (
                    "Consider consolidating domain models/schemas into a single authoritative "
                    "module (or clearly separated layers)."
                ),
            }
        )

    pydantic = signals.get("pydantic_models")
    dataclasses = signals.get("dataclass_models")
    typed_dicts = signals.get("typed_dicts")

    model_kinds = 0
    for group in (pydantic, dataclasses, typed_dicts):
        if isinstance(group, list) and group:
            model_kinds += 1

    if model_kinds >= 2:
        findings.append(
            {
                "category": "semantic_modeling_consistency",
                "title": "Multiple model paradigms detected (Pydantic/dataclass/TypedDict)",
                "severity": "low",
                "confidence": "medium",
                "evidence_paths": _merge_paths(pydantic, dataclasses, typed_dicts),
                "recommendation": (
                    "Prefer one primary modeling approach for core domain entities "
                    "to reduce conceptual drift."
                ),
            }
        )

    if not findings:
        findings.append(
            {
                "category": "semantic_modeling",
                "title": "No major semantic-modeling issues detected by heuristic scan",
                "severity": "info",
                "confidence": "low",
                "evidence_paths": [],
                "recommendation": (
                    "If this repo is expected to contain a domain model, consider adding a "
                    "semantics registry and explicit modeling conventions."
                ),
            }
        )

    for f in findings:
        f.setdefault("repo_id", repo_id)

    return findings


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
    for f in findings:
        sev = str(f.get("severity") or "info")
        if order.get(sev, 0) > order.get(max_sev, 0):
            max_sev = sev
    return {"overall_severity": max_sev, "findings_count": len(findings)}
