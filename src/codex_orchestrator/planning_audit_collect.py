from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from pathlib import Path

from codex_orchestrator.planning_audit_types import (
    PlanningAuditError,
    _FileCollection,
    _Inventory,
    _TextReadResult,
)
from codex_orchestrator.repo_inventory import RepoPolicy


def _abs_roots(repo_root: Path, roots: tuple[Path, ...]) -> list[Path]:
    return sorted({repo_root / p for p in roots}, key=lambda p: p.as_posix())


def _require_positive(name: str, value: int) -> None:
    if value <= 0:
        raise PlanningAuditError(f"{name} must be > 0, got {value}")


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
    config_files = [
        p for p in rel_paths if p.suffix in {".yml", ".yaml", ".json", ".toml", ".ini", ".cfg"}
    ]
    return _Inventory(
        python_files=python_files,
        notebook_files=notebook_files,
        config_files=config_files,
        semantics_yml=_find_semantics_file(rel_paths),
    )


def _collect_repo_files(
    repo_root: Path,
    *,
    allowed_roots: tuple[Path, ...],
    deny_roots: tuple[Path, ...],
    max_files: int,
) -> _FileCollection:
    _require_positive("max_files", max_files)
    rel_paths: list[Path] = []
    errors: list[dict[str, str]] = []
    deny = [repo_root / p for p in deny_roots]
    truncated = _extend_rel_paths(
        rel_paths,
        repo_root=repo_root,
        allowed=_abs_roots(repo_root, allowed_roots),
        deny=deny,
        errors=errors,
        max_files=max_files,
    )
    rel_paths.sort(key=lambda p: p.as_posix())
    return _FileCollection(rel_paths=rel_paths, truncated=truncated, errors=errors)


def _extend_rel_paths(
    rel_paths: list[Path],
    *,
    repo_root: Path,
    allowed: list[Path],
    deny: list[Path],
    errors: list[dict[str, str]],
    max_files: int,
) -> bool:
    for root in allowed:
        if not root.exists() or not root.is_dir():
            continue
        for rel in _iter_files_under_root(repo_root=repo_root, root=root, deny=deny, errors=errors):
            rel_paths.append(rel)
            if len(rel_paths) >= max_files:
                return True
    return False


def _iter_files_under_root(
    *,
    repo_root: Path,
    root: Path,
    deny: list[Path],
    errors: list[dict[str, str]],
) -> Iterator[Path]:
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
        return _TextReadResult(
            text=f"{type(e).__name__}: {e}",
            status="unreadable",
            truncated=False,
        )


def _limit_bytes(data: bytes, *, byte_limit: int) -> tuple[bytes, bool]:
    return (data[:byte_limit], True) if len(data) > byte_limit else (data, False)


def _decode_utf8_or_binary(data: bytes, *, truncated: bool) -> _TextReadResult:
    try:
        return _TextReadResult(text=data.decode("utf-8"), status="ok", truncated=truncated)
    except UnicodeDecodeError:
        return _TextReadResult(text="", status="binary", truncated=truncated)
