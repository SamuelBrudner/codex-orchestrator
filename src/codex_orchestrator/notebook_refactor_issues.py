from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

from codex_orchestrator.beads_subprocess import (
    BdIssueSummary,
    bd_create,
    bd_dep_add,
    bd_list,
    bd_show,
    bd_update,
)
from codex_orchestrator.git_subprocess import git_status_porcelain
from codex_orchestrator.repo_inventory import NotebookOutputPolicy

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class NotebookRefactorIssue:
    issue_id: str
    title: str


@dataclass(frozen=True, slots=True)
class NotebookRefactorResult:
    notebook_paths: tuple[str, ...]
    issue_ids: tuple[str, ...]
    created_issues: tuple[NotebookRefactorIssue, ...]


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


def ensure_notebook_refactor_issues(
    *,
    repo_root: Path,
    notebook_paths: Sequence[str],
    limit: int,
    time_budget_minutes: int | None,
    validation_commands: Sequence[str],
    notebook_output_policy: NotebookOutputPolicy,
    block_bead_ids: Sequence[str],
    label: str = "notebook-refactor",
) -> NotebookRefactorResult:
    normalized = _normalize_paths(notebook_paths)
    if limit <= 0 or not normalized:
        return NotebookRefactorResult(notebook_paths=(), issue_ids=(), created_issues=())

    selected = normalized[:limit]
    open_issues = _open_issues_by_title(bd_list(repo_root=repo_root))

    created: list[NotebookRefactorIssue] = []
    issue_ids: list[str] = []
    for nb_path in selected:
        title = _format_issue_title(nb_path)
        existing_id = open_issues.get(title)
        if existing_id is not None:
            issue_ids.append(existing_id)
            continue

        notes = _format_notebook_refactor_notes(
            notebook_path=nb_path,
            time_budget_minutes=time_budget_minutes,
            validation_commands=_dedupe_preserve_order(validation_commands),
            notebook_output_policy=notebook_output_policy,
        )
        description = (
            f"Refactor `{nb_path}` by extracting reusable logic into `src/` (or the repo's "
            "preferred module layout), and add/extend tests to cover the extracted code.\n"
        )
        created_issue = bd_create(
            repo_root=repo_root,
            title=title,
            issue_type="task",
            priority=2,
            labels=(label,),
            description=description,
            estimate_minutes=time_budget_minutes,
        )
        created_issue = bd_update(
            repo_root=repo_root,
            issue_id=created_issue.issue_id,
            notes=notes,
        )
        created.append(
            NotebookRefactorIssue(issue_id=created_issue.issue_id, title=created_issue.title)
        )
        issue_ids.append(created_issue.issue_id)
        open_issues[title] = created_issue.issue_id

    blockers = tuple(issue_ids)
    _block_beads_on_issue_ids(
        repo_root=repo_root,
        bead_ids=block_bead_ids,
        blocker_issue_ids=blockers,
    )

    return NotebookRefactorResult(
        notebook_paths=tuple(selected),
        issue_ids=blockers,
        created_issues=tuple(created),
    )


def _format_issue_title(notebook_path: str) -> str:
    return f"notebook-refactor: {notebook_path}"


def _dedupe_preserve_order(items: Iterable[str]) -> tuple[str, ...]:
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        value = str(item or "").strip()
        if not value:
            continue
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return tuple(out)


def _normalize_paths(paths: Sequence[str]) -> tuple[str, ...]:
    out: set[str] = set()
    for raw in paths:
        value = str(raw or "").strip()
        if not value:
            continue
        rel = Path(value)
        if rel.is_absolute():
            continue
        if ".." in rel.parts:
            continue
        if rel.suffix != ".ipynb":
            continue
        out.add(rel.as_posix())
    return tuple(sorted(out))


def _open_issues_by_title(issues: tuple[BdIssueSummary, ...]) -> dict[str, str]:
    out: dict[str, str] = {}
    for issue in issues:
        if str(issue.status or "").strip().lower() == "closed":
            continue
        title = str(issue.title or "").strip()
        issue_id = str(issue.issue_id or "").strip()
        if not title or not issue_id:
            continue
        out[title] = issue_id
    return out


def _within(path: Path, root: Path) -> bool:
    if root == Path(".") or root == Path():
        return True
    return path == root or path.is_relative_to(root)


def _format_notebook_refactor_notes(
    *,
    notebook_path: str,
    time_budget_minutes: int | None,
    validation_commands: tuple[str, ...],
    notebook_output_policy: NotebookOutputPolicy,
) -> str:
    lines: list[str] = []
    lines.append("Notebook refactor candidate (auto-created by codex-orchestrator planning pass).")
    lines.append("")
    lines.append(f"- Notebook: `{notebook_path}`")
    lines.append("")
    lines.append("Work:")
    lines.append("- Extract reusable logic into `src/` (or the repo's preferred module layout).")
    lines.append("- Replace notebook cells with imports from extracted modules.")
    lines.append("- Add/extend tests to cover extracted behavior.")
    lines.append(f"- Respect notebook output policy: `{notebook_output_policy}`.")
    lines.append("")
    lines.append("Suggested execution contract:")
    if time_budget_minutes is not None and time_budget_minutes > 0:
        lines.append(f"- time_budget_minutes: {time_budget_minutes}")
    else:
        lines.append("- time_budget_minutes: <set in overlay defaults>")
    if validation_commands:
        lines.append("- validation_commands:")
        lines.extend([f"  - `{cmd}`" for cmd in validation_commands])
    else:
        lines.append("- validation_commands: <none configured>")
    lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _block_beads_on_issue_ids(
    *,
    repo_root: Path,
    bead_ids: Sequence[str],
    blocker_issue_ids: tuple[str, ...],
) -> None:
    if not blocker_issue_ids:
        return

    blocker_set = set(blocker_issue_ids)
    for bead_id in sorted({b for b in bead_ids if b}):
        if bead_id in blocker_set:
            continue
        issue = bd_show(repo_root=repo_root, issue_id=bead_id)
        existing_deps = set(issue.dependencies)
        for blocker_id in blocker_issue_ids:
            if blocker_id in existing_deps:
                continue
            bd_dep_add(
                repo_root=repo_root,
                issue_id=bead_id,
                depends_on_id=blocker_id,
                dep_type="blocks",
            )
            existing_deps.add(blocker_id)
        if blocker_issue_ids:
            logger.info(
                "Blocked bead %s on notebook-refactor issues: %s",
                bead_id,
                ", ".join(blocker_issue_ids),
            )
