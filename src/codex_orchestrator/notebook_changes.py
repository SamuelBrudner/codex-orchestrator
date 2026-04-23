from __future__ import annotations

from pathlib import Path

from codex_orchestrator.git_subprocess import git_status_porcelain


def detect_changed_notebooks(
    *,
    repo_root: Path,
    notebook_roots: tuple[Path, ...],
) -> tuple[str, ...]:
    entries = git_status_porcelain(repo_root=repo_root)
    if not entries:
        return ()

    changed: set[str] = set()
    for entry in entries:
        raw = str(entry.path or "").strip()
        if not raw:
            continue
        rel = Path(raw)
        if rel.is_absolute():
            continue
        if ".." in rel.parts:
            continue
        if rel.suffix != ".ipynb":
            continue
        if "D" in entry.xy:
            continue
        if not any(_within(rel, root) for root in notebook_roots):
            continue
        changed.add(rel.as_posix())

    return tuple(sorted(changed))


def _within(path: Path, root: Path) -> bool:
    if root == Path(".") or root == Path():
        return True
    return path == root or path.is_relative_to(root)
