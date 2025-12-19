from __future__ import annotations

import ast
import os
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeAlias

from codex_orchestrator.repo_inventory import RepoPolicy


class PlanningAuditError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class PlanningAuditArtifacts:
    json_path: Path
    md_path: Path


_DEFAULT_MAX_FILES: int = 5_000
_DEFAULT_MAX_PYTHON_FILES_SCANNED: int = 500

_ScanCats: TypeAlias = dict[str, set[str]]
_ReadFailures: TypeAlias = list[dict[str, str]]
_CollectionErrors: TypeAlias = list[dict[str, str]]
_ParseFailures: TypeAlias = list[dict[str, str]]
_ConfigPatternUsage: TypeAlias = dict[str, set[str]]


@dataclass(frozen=True, slots=True)
class _ModelDef:
    kind: str
    name: str
    path: str
    fields: tuple[str, ...]


@dataclass(slots=True)
class _SemanticScanAcc:
    cats: _ScanCats
    modelish: set[str]
    read_failures: _ReadFailures
    parse_failures: _ParseFailures
    model_defs: list[_ModelDef]
    config_patterns: _ConfigPatternUsage


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
    *, run_id: str, repo_policy: RepoPolicy, max_files: int = _DEFAULT_MAX_FILES,
    max_python_files_scanned: int = _DEFAULT_MAX_PYTHON_FILES_SCANNED,
) -> dict[str, Any]:
    repo_root = repo_policy.path
    collection, inv = _collect_and_inventory(repo_root, repo_policy=repo_policy, max_files=max_files)
    signals, findings = _signals_and_findings(repo_root, inv=inv, repo_id=repo_policy.repo_id, max_python_files_scanned=max_python_files_scanned)
    inputs = _assemble_inputs(
        run_id=run_id, repo_policy=repo_policy, repo_root=repo_root, inv=inv,
        collection=collection, signals=signals, findings=findings,
        max_files=max_files, max_python_files_scanned=max_python_files_scanned,
    )
    return _assemble_audit(inputs)


def _assemble_inputs(
    *, run_id: str, repo_policy: RepoPolicy, repo_root: Path,
    inv: _Inventory, collection: _FileCollection,
    signals: dict[str, Any], findings: list[dict[str, Any]],
    max_files: int, max_python_files_scanned: int,
) -> _AssembleInputs:
    status, notes, actions = _generation_status(collection=collection, inv=inv, scan=signals.get("scan"))
    return _AssembleInputs(
        run_id, repo_policy, repo_root, inv, collection,
        status, notes, actions,
        max_files, max_python_files_scanned, signals, findings,
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
    base = _audit_sections_base(inv, collection, repo_policy, max_files, max_python_files_scanned)
    return base | {"signals": signals, "findings": findings, "summary": _score_summary(findings)}


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
    repo_id, run_id = (str(audit.get("repo_id") or "<unknown>"), str(audit.get("run_id") or "<unknown>"))
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


def _abs_roots(repo_root: Path, roots: tuple[Path, ...]) -> list[Path]:
    return sorted({repo_root / p for p in roots}, key=lambda p: p.as_posix())


def _require_positive(name: str, value: int) -> None:
    if value <= 0:
        raise PlanningAuditError(f"{name} must be > 0, got {value}")


def _collect_repo_files(
    repo_root: Path, *, allowed_roots: tuple[Path, ...], deny_roots: tuple[Path, ...], max_files: int
) -> _FileCollection:
    _require_positive("max_files", max_files)
    rel_paths: list[Path] = []
    errors: list[dict[str, str]] = []
    deny = [repo_root / p for p in deny_roots]
    truncated = _extend_rel_paths(
        rel_paths, repo_root=repo_root, allowed=_abs_roots(repo_root, allowed_roots),
        deny=deny, errors=errors, max_files=max_files,
    )
    rel_paths.sort(key=lambda p: p.as_posix())
    return _FileCollection(rel_paths=rel_paths, truncated=truncated, errors=errors)


def _extend_rel_paths(
    rel_paths: list[Path], *, repo_root: Path, allowed: list[Path], deny: list[Path],
    errors: list[dict[str, str]], max_files: int,
) -> bool:
    for root in allowed:
        if not root.exists() or not root.is_dir():
            continue
        for rel in _iter_files_under_root(repo_root=repo_root, root=root, deny=deny, errors=errors):
            rel_paths.append(rel)
            if len(rel_paths) >= max_files:
                return True
    return False


def _iter_files_under_root(*, repo_root: Path, root: Path, deny: list[Path], errors: list[dict[str, str]]) -> Iterator[Path]:
    on_error = _walk_on_error(errors=errors, root=root)
    for dirpath, dirnames, filenames in os.walk(root, topdown=True, onerror=on_error):
        dir_path = Path(dirpath)
        if _is_denied(dir_path, deny):
            dirnames[:] = []
            continue
        dirnames[:] = sorted(dirnames)
        for name in sorted(filenames):
            abs_path = dir_path / name
            if _is_denied(abs_path, deny):
                continue
            rel = _safe_relpath(repo_root, abs_path)
            if rel is not None:
                yield rel


def _walk_on_error(*, errors: list[dict[str, str]], root: Path) -> Callable[[OSError], None]:
    def on_error(e: OSError) -> None:
        errors.append(
            {
                "kind": "walk_error",
                "path": str(getattr(e, "filename", "") or root.as_posix()),
                "error": f"{type(e).__name__}: {e}",
            }
        )

    return on_error


def _is_denied(path: Path, deny: list[Path]) -> bool:
    return any(_is_within(path, d) for d in deny)


def _safe_relpath(repo_root: Path, abs_path: Path) -> Path | None:
    try:
        rel = abs_path.relative_to(repo_root)
    except ValueError:
        return None
    if rel.is_absolute() or ".." in rel.parts:
        return None
    return rel


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
    data = _read_bytes_or_failure(path)
    if isinstance(data, _TextReadResult):
        return data
    limited, truncated = _limit_bytes(data, byte_limit=byte_limit)
    return _decode_utf8_or_binary(limited, truncated=truncated)


def _read_bytes_or_failure(path: Path) -> bytes | _TextReadResult:
    try:
        return _read_bytes(path)
    except FileNotFoundError:
        return _TextReadResult(text="", status="missing", truncated=False)
    except (PermissionError, OSError) as e:
        return _TextReadResult(text=f"{type(e).__name__}: {e}", status="unreadable", truncated=False)


def _limit_bytes(data: bytes, *, byte_limit: int) -> tuple[bytes, bool]:
    return (data[:byte_limit], True) if len(data) > byte_limit else (data, False)


def _decode_utf8_or_binary(data: bytes, *, truncated: bool) -> _TextReadResult:
    try:
        return _TextReadResult(text=data.decode("utf-8"), status="ok", truncated=truncated)
    except UnicodeDecodeError:
        return _TextReadResult(text="", status="binary", truncated=truncated)


def _scan_sets() -> dict[str, set[str]]:
    return {
        "pydantic_models": set(),
        "dataclass_models": set(),
        "typed_dicts": set(),
        "sqlalchemy": set(),
    }


def _scan_update_sets(*, text: str, rel: str, cats: _ScanCats) -> None:
    if "pydantic" in text or "BaseModel" in text:
        cats["pydantic_models"].add(rel)
    if "@dataclass" in text or "dataclasses import dataclass" in text:
        cats["dataclass_models"].add(rel)
    if "TypedDict" in text:
        cats["typed_dicts"].add(rel)
    if "sqlalchemy" in text:
        cats["sqlalchemy"].add(rel)


def _scan_one_python_file(repo_root: Path, rel: Path, *, acc: _SemanticScanAcc) -> None:
    rel_s = rel.as_posix()
    result = _read_text_limited_with_status(repo_root / rel)
    if result.status != "ok":
        acc.read_failures.append({"path": rel_s, "status": result.status, "detail": result.text})
        return
    _scan_update_sets(text=result.text, rel=rel_s, cats=acc.cats)
    if _looks_like_model_module(rel):
        acc.modelish.add(rel_s)
    tree = _parse_python_ast(text=result.text, rel=rel_s, truncated=result.truncated, parse_failures=acc.parse_failures)
    if tree is None:
        return
    _scan_ast_semantics(tree=tree, rel=rel_s, model_defs=acc.model_defs, config_patterns=acc.config_patterns)


def _scan_output(
    *,
    total: int,
    scanned_count: int,
    truncated: bool,
    read_failures: _ReadFailures,
    parse_failures: _ParseFailures,
    modelish: set[str],
    cats: _ScanCats,
    model_defs: list[_ModelDef],
    config_patterns: _ConfigPatternUsage,
) -> dict[str, Any]:
    read_failures_sorted = sorted(read_failures, key=lambda item: str(item.get("path") or ""))
    parse_failures_sorted = sorted(parse_failures, key=lambda item: str(item.get("path") or ""))
    scan = _scan_block(
        total=total,
        scanned_count=scanned_count,
        truncated=truncated,
        read_failures=read_failures_sorted,
        parse_failures=parse_failures_sorted,
    )
    model_shapes = _model_shape_duplicates_block(model_defs)
    return (
        {"scan": scan, "model_modules": sorted(modelish)}
        | _scan_models_block(cats)
        | {"config_patterns": _sorted_config_pattern_usage(config_patterns), "model_shape_duplicates": model_shapes}
    )


def _scan_block(
    *,
    total: int,
    scanned_count: int,
    truncated: bool,
    read_failures: _ReadFailures,
    parse_failures: _ParseFailures,
) -> dict[str, Any]:
    return {
        "python_files_total": total,
        "python_files_scanned": scanned_count,
        "truncated": truncated,
        "read_failures": read_failures,
        "parse_failures": parse_failures,
    }


def _scan_models_block(cats: _ScanCats) -> dict[str, list[str]]:
    return {
        "pydantic_models": sorted(cats["pydantic_models"]),
        "dataclass_models": sorted(cats["dataclass_models"]),
        "typed_dicts": sorted(cats["typed_dicts"]),
        "sqlalchemy": sorted(cats["sqlalchemy"]),
    }


def _parse_python_ast(*, text: str, rel: str, truncated: bool, parse_failures: _ParseFailures) -> ast.AST | None:
    try:
        return ast.parse(text, filename=rel)
    except SyntaxError as e:
        status = "syntax_error_truncated" if truncated else "syntax_error"
        detail = f"SyntaxError: {e.msg}"
    except ValueError as e:
        status = "value_error_truncated" if truncated else "value_error"
        detail = f"ValueError: {e}"
    except Exception as e:  # pragma: no cover - defensive
        status = "parse_error_truncated" if truncated else "parse_error"
        detail = f"{type(e).__name__}: {e}"
    parse_failures.append({"path": rel, "status": status, "detail": detail})
    return None


def _scan_ast_semantics(
    *,
    tree: ast.AST,
    rel: str,
    model_defs: list[_ModelDef],
    config_patterns: _ConfigPatternUsage,
) -> None:
    for class_def in _top_level_classes(tree):
        base_names = {_ast_base_name(base) for base in class_def.bases}
        if "BaseSettings" in base_names:
            config_patterns.setdefault("pydantic.BaseSettings", set()).add(rel)
        kind = _model_kind(class_def, base_names=base_names)
        if kind is None:
            continue
        fields = _class_fields(class_def)
        if not fields:
            continue
        model_defs.append(_ModelDef(kind=kind, name=class_def.name, path=rel, fields=fields))

    for pattern in _config_patterns_in_tree(tree):
        config_patterns.setdefault(pattern, set()).add(rel)


def _top_level_classes(tree: ast.AST) -> Iterator[ast.ClassDef]:
    body = getattr(tree, "body", None)
    if not isinstance(body, list):
        return iter(())
    return (node for node in body if isinstance(node, ast.ClassDef))


def _ast_base_name(node: ast.expr) -> str:
    if isinstance(node, ast.Subscript):
        return _ast_base_name(node.value)
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return ""


def _model_kind(class_def: ast.ClassDef, *, base_names: set[str]) -> str | None:
    if _has_dataclass_decorator(class_def):
        return "dataclass"
    if "TypedDict" in base_names:
        return "typed_dict"
    if "BaseModel" in base_names or "BaseSettings" in base_names:
        return "pydantic"
    return None


def _has_dataclass_decorator(class_def: ast.ClassDef) -> bool:
    for deco in class_def.decorator_list:
        name = _decorator_name(deco)
        if name == "dataclass":
            return True
    return False


def _decorator_name(node: ast.expr) -> str | None:
    if isinstance(node, ast.Call):
        return _decorator_name(node.func)
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _class_fields(class_def: ast.ClassDef) -> tuple[str, ...]:
    fields: set[str] = set()
    for node in class_def.body:
        if not isinstance(node, ast.AnnAssign):
            continue
        target = node.target
        if isinstance(target, ast.Name):
            name = target.id
            if name and not name.startswith("_"):
                fields.add(name)
    return tuple(sorted(fields))


_CONFIG_CALL_PATTERNS: frozenset[str] = frozenset(
    {
        "argparse.ArgumentParser",
        "configparser.ConfigParser",
        "json.load",
        "load_dotenv",
        "os.environ.get",
        "os.getenv",
        "toml.load",
        "tomli.load",
        "tomli.loads",
        "tomllib.load",
        "tomllib.loads",
        "yaml.load",
        "yaml.safe_load",
    }
)


def _config_patterns_in_tree(tree: ast.AST) -> set[str]:
    patterns: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            dotted = _dotted_name(node.func)
            if dotted in _CONFIG_CALL_PATTERNS:
                patterns.add(dotted)
        if isinstance(node, ast.Subscript) and _dotted_name(node.value) == "os.environ":
            patterns.add("os.environ[...]")
    return patterns


def _dotted_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        base = _dotted_name(node.value)
        return f"{base}.{node.attr}" if base else node.attr
    return None


def _sorted_config_pattern_usage(usage: _ConfigPatternUsage) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for pattern, paths in usage.items():
        out[pattern] = sorted(paths)
    return out


def _model_shape_duplicates_block(model_defs: list[_ModelDef]) -> dict[str, Any]:
    groups, truncated = _shape_duplicate_groups(model_defs)
    return {"groups": groups, "truncated": truncated}


def _shape_duplicate_groups(
    model_defs: list[_ModelDef],
    *,
    min_fields: int = 3,
    max_groups: int = 10,
    max_occurrences_per_group: int = 10,
) -> tuple[list[dict[str, Any]], bool]:
    buckets: dict[tuple[str, ...], list[_ModelDef]] = {}
    for model_def in model_defs:
        if len(model_def.fields) < min_fields:
            continue
        buckets.setdefault(model_def.fields, []).append(model_def)

    duplicates: list[dict[str, Any]] = []
    for fields, occurrences in buckets.items():
        if len(occurrences) < 2:
            continue
        occ_sorted = sorted(occurrences, key=lambda m: (m.path, m.name, m.kind))
        limited = occ_sorted[:max_occurrences_per_group]
        duplicates.append(
            {
                "fields": list(fields),
                "count": len(occ_sorted),
                "kinds": sorted({m.kind for m in occ_sorted}),
                "occurrences": [{"path": m.path, "name": m.name, "kind": m.kind} for m in limited],
                "occurrences_truncated": len(occ_sorted) > len(limited),
            }
        )

    duplicates.sort(key=lambda g: (-int(g.get("count", 0) or 0), len(g.get("fields") or ()), "|".join(g.get("fields") or ())))
    return duplicates[:max_groups], len(duplicates) > max_groups


def _scan_window(python_files: list[Path], *, max_python_files_scanned: int) -> tuple[int, list[Path], bool]:
    total = len(python_files)
    return total, python_files[:max_python_files_scanned], total > max_python_files_scanned


def _scan_accumulators() -> _SemanticScanAcc:
    return _SemanticScanAcc(
        cats=_scan_sets(),
        modelish=set(),
        read_failures=[],
        parse_failures=[],
        model_defs=[],
        config_patterns={},
    )


def _scan_semantic_signals(
    repo_root: Path, python_files: list[Path], *, max_python_files_scanned: int
) -> dict[str, Any]:
    _require_positive("max_python_files_scanned", max_python_files_scanned)
    total, scanned, truncated = _scan_window(python_files, max_python_files_scanned=max_python_files_scanned)
    acc = _scan_accumulators()
    for rel in scanned:
        _scan_one_python_file(repo_root, rel, acc=acc)
    return _scan_output(
        total=total,
        scanned_count=len(scanned),
        truncated=truncated,
        read_failures=acc.read_failures,
        parse_failures=acc.parse_failures,
        modelish=acc.modelish,
        cats=acc.cats,
        model_defs=acc.model_defs,
        config_patterns=acc.config_patterns,
    )


def _audit_status(
    *, inventory_paths_count: int, collection_truncated: bool, collection_errors: _CollectionErrors,
    python_files_count: int, scan: Any,
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
    notes, actions = _collection_notes_actions(collection_truncated=collection_truncated, collection_errors=collection_errors)
    if python_files_count == 0:
        notes.append("No Python files detected; semantic scan is limited.")
    status = "partial" if collection_truncated or collection_errors else "ok"
    return status, notes, actions


def _collection_notes_actions(
    *, collection_truncated: bool, collection_errors: list[dict[str, str]]
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
    *, status: str, notes: list[str], actions: list[str], scan: Any
) -> tuple[str, list[str], list[str]]:
    if not isinstance(scan, dict):
        return status, notes, actions
    if bool(scan.get("truncated")):
        status = "partial"
        notes.append("Semantic scan truncated due to max_python_files_scanned limit.")
        actions.append("Increase max_python_files_scanned if you need deeper scanning for this repo.")
    failures = scan.get("read_failures")
    if isinstance(failures, list) and failures:
        status = "partial"
        notes.append("Some Python files could not be read as UTF-8 during semantic scan.")
        actions.append("Inspect unreadable/binary files or adjust audit limits/permissions if needed.")
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
    _append_if(findings, _finding_missing_semantics(semantics_yml))
    _append_if(findings, _finding_duplicate_model_shapes(signals))
    _append_if(findings, _finding_duplicate_model_modules(signals))
    _append_if(findings, _finding_multiple_model_paradigms(signals))
    _append_if(findings, _finding_repeated_config_parsing(signals))
    if not findings:
        findings.append(_finding_no_issues())
    for f in findings:
        f.setdefault("repo_id", repo_id)
    return findings


def _append_if(findings: list[dict[str, Any]], finding: dict[str, Any] | None) -> None:
    if finding is not None:
        findings.append(finding)


def _finding_missing_semantics(semantics_yml: Path | None) -> dict[str, Any] | None:
    if semantics_yml is not None:
        return None
    return {
        "category": "semantic_registry",
        "title": "No semantics registry detected",
        "severity": "medium",
        "confidence": "high",
        "confidence_rationale": ["trigger:missing_semantics_registry"],
        "evidence_paths": [],
        "recommendation": "Add metadata/semantics/semantics.yml to register core entities and canonical functions.",
    }


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
        if isinstance(kinds, list) and len({k.strip() for k in kinds if isinstance(k, str) and k.strip()}) >= 2:
            has_cross_paradigm = True
        occurrences = group.get("occurrences")
        if not isinstance(occurrences, list):
            continue
        for occ in occurrences:
            if isinstance(occ, dict):
                p = occ.get("path")
                if isinstance(p, str) and p.strip():
                    paths.add(p.strip())

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
        "details": {"duplicate_shape_groups": groups, "truncated": bool(block.get("truncated"))},
        "recommendation": "Consider consolidating these representations into a single authoritative model (or clear layer boundaries + explicit conversion).",
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
        "confidence_rationale": [f"trigger:multiple_modelish_modules(count={len(model_modules)})"],
        "evidence_paths": _sorted_str_paths(model_modules),
        "recommendation": "Consider consolidating domain models/schemas into a single authoritative module (or clearly separated layers).",
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
        "severity": "low",
        "confidence": "medium",
        "confidence_rationale": [f"trigger:multiple_model_paradigms(paradigms={','.join(paradigms)})"],
        "evidence_paths": _merge_paths(pydantic, dataclasses, typed_dicts),
        "recommendation": "Prefer one primary modeling approach for core domain entities to reduce conceptual drift.",
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

    max_files = max((len(paths) for _, paths in hot), default=0)
    severity = "medium" if max_files >= 5 or len(hot) >= 2 else "low"
    confidence = "medium"
    rationale = [
        f"trigger:repeated_config_parsing(patterns={len(hot)})",
        "signal:config_patterns_repeated_in_3+_files",
    ]

    hot_details = [{"pattern": pattern, "paths": paths, "count": len(paths)} for pattern, paths in hot[:10]]

    return {
        "category": "config_parsing_dry",
        "title": "Repeated config parsing patterns detected",
        "severity": severity,
        "confidence": confidence,
        "confidence_rationale": rationale,
        "evidence_paths": sorted(evidence),
        "details": {"hot_patterns": hot_details, "truncated": len(hot) > len(hot_details)},
        "recommendation": "Consider centralizing configuration loading/validation (env vars, config files, CLI args) into a single module or settings layer.",
    }


def _nonempty_list_count(*groups: Any) -> int:
    return sum(1 for g in groups if isinstance(g, list) and g)


def _finding_no_issues() -> dict[str, Any]:
    return {
        "category": "semantic_modeling",
        "title": "No major semantic-modeling issues detected by heuristic scan",
        "severity": "info",
        "confidence": "low",
        "confidence_rationale": ["trigger:no_findings_emitted"],
        "evidence_paths": [],
        "recommendation": "If this repo is expected to contain a domain model, consider adding a semantics registry and explicit modeling conventions.",
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
    for f in findings:
        sev = str(f.get("severity") or "info")
        if order.get(sev, 0) > order.get(max_sev, 0):
            max_sev = sev
    return {"overall_severity": max_sev, "findings_count": len(findings)}
