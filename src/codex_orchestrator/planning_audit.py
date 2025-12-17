from __future__ import annotations

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


def build_planning_audit(*, run_id: str, repo_policy: RepoPolicy) -> dict[str, Any]:
    repo_root = repo_policy.path

    rel_paths = _collect_repo_files(
        repo_root,
        allowed_roots=repo_policy.allowed_roots,
        deny_roots=repo_policy.deny_roots,
    )

    python_files = [p for p in rel_paths if p.suffix == ".py"]
    notebook_files = [p for p in rel_paths if p.suffix == ".ipynb"]
    config_files = [
        p
        for p in rel_paths
        if p.suffix in {".yml", ".yaml", ".json", ".toml", ".ini", ".cfg"}
    ]

    semantics_yml = _find_semantics_file(rel_paths)

    signals = _scan_semantic_signals(repo_root, python_files)
    findings = _build_findings(
        repo_id=repo_policy.repo_id,
        semantics_yml=semantics_yml,
        signals=signals,
    )

    summary = _score_summary(findings)

    return {
        "schema_version": 1,
        "run_id": run_id,
        "repo_id": repo_policy.repo_id,
        "repo_path": repo_root.as_posix(),
        "inputs": {
            "allowed_roots": [p.as_posix() for p in repo_policy.allowed_roots],
            "deny_roots": [p.as_posix() for p in repo_policy.deny_roots],
        },
        "inventory": {
            "python_files_count": len(python_files),
            "notebooks_count": len(notebook_files),
            "config_files_count": len(config_files),
            "semantics_yml": semantics_yml.as_posix() if semantics_yml is not None else None,
        },
        "signals": signals,
        "findings": findings,
        "summary": summary,
    }


def format_planning_audit_md(audit: dict[str, Any]) -> str:
    repo_id = str(audit.get("repo_id") or "<unknown>")
    run_id = str(audit.get("run_id") or "<unknown>")

    summary = audit.get("summary")
    severity = "unknown"
    if isinstance(summary, dict):
        severity = str(summary.get("overall_severity") or "unknown")

    lines: list[str] = []
    lines.append(f"# Planning Audit ({repo_id})")
    lines.append("")
    lines.append("## Run")
    lines.append(f"- RUN_ID: `{run_id}`")
    lines.append("")
    lines.append("## Summary")
    lines.append(f"- Overall severity: `{severity}`")
    lines.append("")

    inventory = audit.get("inventory")
    if isinstance(inventory, dict):
        lines.append("## Inventory")
        lines.append(f"- Python files: {int(inventory.get('python_files_count', 0) or 0)}")
        lines.append(f"- Notebooks: {int(inventory.get('notebooks_count', 0) or 0)}")
        lines.append(
            f"- Config files: {int(inventory.get('config_files_count', 0) or 0)}"
        )
        semantics = inventory.get("semantics_yml")
        if isinstance(semantics, str) and semantics.strip():
            lines.append(f"- Semantics registry: `{semantics}`")
        else:
            lines.append("- Semantics registry: <missing>")
        lines.append("")

    findings = audit.get("findings")
    lines.append("## Findings")
    if not isinstance(findings, list) or not findings:
        lines.append("- None")
        lines.append("")
        return "\n".join(lines)

    for f in findings:
        if not isinstance(f, dict):
            continue
        title = str(f.get("title") or "<untitled>")
        category = str(f.get("category") or "<uncategorized>")
        severity_item = str(f.get("severity") or "unknown")
        recommendation = str(f.get("recommendation") or "").strip()
        evidence = f.get("evidence_paths")

        lines.append(f"- **{title}** (`{category}`, severity=`{severity_item}`)")
        if recommendation:
            lines.append(f"  - Recommendation: {recommendation}")
        if isinstance(evidence, list) and evidence:
            shown = [str(p) for p in evidence if isinstance(p, str) and p.strip()]
            if shown:
                for p in sorted(set(shown))[:25]:
                    lines.append(f"  - `{p}`")
    lines.append("")
    return "\n".join(lines)


def _collect_repo_files(
    repo_root: Path,
    *,
    allowed_roots: tuple[Path, ...],
    deny_roots: tuple[Path, ...],
) -> list[Path]:
    rel_paths: list[Path] = []

    allowed = [repo_root / p for p in allowed_roots]
    deny = [repo_root / p for p in deny_roots]

    for root in sorted({p for p in allowed}):
        if not root.exists():
            continue
        if not root.is_dir():
            continue
        for abs_path in root.rglob("*"):
            if not abs_path.is_file():
                continue
            if any(_is_within(abs_path, d) for d in deny):
                continue
            try:
                rel = abs_path.relative_to(repo_root)
            except ValueError:
                continue
            if rel.is_absolute():
                continue
            if ".." in rel.parts:
                continue
            rel_paths.append(rel)

    rel_paths.sort(key=lambda p: p.as_posix())
    return rel_paths


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
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return ""
    except OSError:
        return ""
    if len(data) > byte_limit:
        data = data[:byte_limit]
    try:
        return data.decode("utf-8", errors="ignore")
    except Exception:
        return ""


def _scan_semantic_signals(repo_root: Path, python_files: list[Path]) -> dict[str, Any]:
    pydantic_files: set[str] = set()
    dataclass_files: set[str] = set()
    typed_dict_files: set[str] = set()
    sqlalchemy_files: set[str] = set()

    modelish_by_name: set[str] = set()

    for rel in python_files:
        text = _read_text_limited(repo_root / rel)

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
        "model_modules": sorted(modelish_by_name),
        "pydantic_models": sorted(pydantic_files),
        "dataclass_models": sorted(dataclass_files),
        "typed_dicts": sorted(typed_dict_files),
        "sqlalchemy": sorted(sqlalchemy_files),
    }


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
