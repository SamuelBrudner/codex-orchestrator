from __future__ import annotations

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
