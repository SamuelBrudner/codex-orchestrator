from __future__ import annotations

import ast
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from codex_orchestrator.planning_audit_collect import _read_text_limited_with_status, _require_positive
from codex_orchestrator.planning_audit_types import (
    _ConfigPatternUsage,
    _ModelDef,
    _ParseFailures,
    _ReadFailures,
    _ScanCats,
    _SemanticScanAcc,
)


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
    tree = _parse_python_ast(
        text=result.text,
        rel=rel_s,
        truncated=result.truncated,
        parse_failures=acc.parse_failures,
    )
    if tree is None:
        return
    _scan_ast_semantics(
        tree=tree,
        rel=rel_s,
        model_defs=acc.model_defs,
        config_patterns=acc.config_patterns,
    )


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
        | {
            "config_patterns": _sorted_config_pattern_usage(config_patterns),
            "model_shape_duplicates": model_shapes,
        }
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


def _parse_python_ast(
    *,
    text: str,
    rel: str,
    truncated: bool,
    parse_failures: _ParseFailures,
) -> ast.AST | None:
    try:
        return ast.parse(text, filename=rel)
    except SyntaxError as e:
        status = "syntax_error_truncated" if truncated else "syntax_error"
        detail = f"SyntaxError: {e.msg}"
    except ValueError as e:
        status = "value_error_truncated" if truncated else "value_error"
        detail = f"ValueError: {e}"
    except Exception as e:  # pragma: no cover
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
                "occurrences": [
                    {"path": m.path, "name": m.name, "kind": m.kind}
                    for m in limited
                ],
                "occurrences_truncated": len(occ_sorted) > len(limited),
            }
        )

    duplicates.sort(
        key=lambda g: (
            -int(g.get("count", 0) or 0),
            len(g.get("fields") or ()),
            "|".join(g.get("fields") or ()),
        )
    )
    return duplicates[:max_groups], len(duplicates) > max_groups


def _scan_window(
    python_files: list[Path],
    *,
    max_python_files_scanned: int,
) -> tuple[int, list[Path], bool]:
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
    repo_root: Path,
    python_files: list[Path],
    *,
    max_python_files_scanned: int,
) -> dict[str, Any]:
    _require_positive("max_python_files_scanned", max_python_files_scanned)
    total, scanned, truncated = _scan_window(
        python_files,
        max_python_files_scanned=max_python_files_scanned,
    )
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


def _looks_like_model_module(path: Path) -> bool:
    name = path.name.lower()
    return any(token in name for token in ("schema", "model", "types", "entities", "dto"))
