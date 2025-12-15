from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from codex_orchestrator.planner import ReadyBead


class BdCliError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class BdIssue:
    issue_id: str
    title: str
    status: str
    notes: str
    dependencies: tuple[str, ...]
    dependents: tuple[str, ...]


def _run_bd(
    args: Sequence[str],
    *,
    cwd: Path,
    timeout_seconds: float = 60.0,
) -> str:
    try:
        completed = subprocess.run(
            ["bd", *args],
            cwd=cwd,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_seconds,
        )
    except FileNotFoundError as e:
        raise BdCliError("bd CLI not found (install beads/bd and ensure it's on PATH).") from e
    except subprocess.TimeoutExpired as e:
        raise BdCliError(f"bd {' '.join(args)} timed out after {timeout_seconds:.0f}s.") from e

    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        stdout = (completed.stdout or "").strip()
        details = stderr or stdout or "<no output>"
        raise BdCliError(f"bd {' '.join(args)} failed (exit={completed.returncode}): {details}")

    return completed.stdout or ""


def _parse_json_output(stdout: str) -> Any:
    payload = stdout.strip()
    if not payload:
        return None
    try:
        return json.loads(payload)
    except json.JSONDecodeError as e:
        raise BdCliError(f"Failed to parse bd --json output: {e}") from e


def _parse_issue(data: Any, *, context: str) -> BdIssue:
    if not isinstance(data, dict):
        raise BdCliError(f"{context}: expected object, got {type(data).__name__}")

    issue_id = data.get("id")
    title = data.get("title")
    status = data.get("status")
    notes = data.get("notes", "")
    if notes is None:
        notes = ""
    if not isinstance(issue_id, str) or not issue_id.strip():
        raise BdCliError(f"{context}: missing string id")
    if not isinstance(title, str) or not title.strip():
        raise BdCliError(f"{context}: missing string title")
    if not isinstance(status, str) or not status.strip():
        raise BdCliError(f"{context}: missing string status")
    if not isinstance(notes, str):
        raise BdCliError(f"{context}: notes must be a string")

    def _extract_ids(field: str) -> tuple[str, ...]:
        raw = data.get(field, [])
        if raw is None:
            raw = []
        if not isinstance(raw, list):
            raise BdCliError(f"{context}: {field} must be a list")
        ids: list[str] = []
        for idx, item in enumerate(raw):
            if not isinstance(item, dict):
                raise BdCliError(
                    f"{context}: {field}[{idx}] expected object, got {type(item).__name__}"
                )
            dep_id = item.get("id")
            if not isinstance(dep_id, str) or not dep_id.strip():
                raise BdCliError(f"{context}: {field}[{idx}].id missing string")
            ids.append(dep_id)
        return tuple(ids)

    dependencies = _extract_ids("dependencies")
    dependents = _extract_ids("dependents")

    return BdIssue(
        issue_id=issue_id,
        title=title,
        status=status,
        notes=notes,
        dependencies=dependencies,
        dependents=dependents,
    )


def bd_init(*, repo_root: Path) -> None:
    _run_bd(["init", "--quiet"], cwd=repo_root)


def bd_ready(*, repo_root: Path) -> list[ReadyBead]:
    data = _parse_json_output(_run_bd(["ready", "--json"], cwd=repo_root))
    if data is None:
        return []
    if not isinstance(data, list):
        raise BdCliError(f"bd ready --json: expected a list, got {type(data).__name__}")

    out: list[ReadyBead] = []
    for idx, item in enumerate(data):
        if not isinstance(item, dict):
            raise BdCliError(
                f"bd ready --json: expected objects, got {type(item).__name__} at index {idx}"
            )
        bead_id = item.get("id")
        title = item.get("title")
        if not isinstance(bead_id, str) or not bead_id.strip():
            raise BdCliError(f"bd ready --json: missing string id at index {idx}")
        if not isinstance(title, str) or not title.strip():
            raise BdCliError(f"bd ready --json: missing string title at index {idx}")
        out.append(ReadyBead(bead_id=bead_id, title=title))
    return out


def bd_list_ids(*, repo_root: Path) -> set[str]:
    data = _parse_json_output(_run_bd(["list", "--json"], cwd=repo_root))
    if data is None:
        return set()
    if not isinstance(data, list):
        raise BdCliError(f"bd list --json: expected a list, got {type(data).__name__}")

    out: set[str] = set()
    for idx, item in enumerate(data):
        if not isinstance(item, dict):
            raise BdCliError(
                f"bd list --json: expected objects, got {type(item).__name__} at index {idx}"
            )
        bead_id = item.get("id")
        if not isinstance(bead_id, str) or not bead_id.strip():
            raise BdCliError(f"bd list --json: missing string id at index {idx}")
        out.add(bead_id)
    return out


def bd_show(*, repo_root: Path, issue_id: str) -> BdIssue:
    data = _parse_json_output(_run_bd(["show", issue_id, "--json"], cwd=repo_root))
    return _parse_issue(data, context=f"bd show {issue_id} --json")


def bd_update(
    *,
    repo_root: Path,
    issue_id: str,
    status: str | None = None,
    notes: str | None = None,
) -> BdIssue:
    args: list[str] = ["update", issue_id]
    if status is not None:
        args.extend(["--status", status])
    if notes is not None:
        args.extend(["--notes", notes])
    args.append("--json")
    data = _parse_json_output(_run_bd(args, cwd=repo_root))
    return _parse_issue(data, context=f"bd update {issue_id} --json")


def bd_close(*, repo_root: Path, issue_id: str, reason: str) -> BdIssue:
    data = _parse_json_output(
        _run_bd(["close", issue_id, "--reason", reason, "--json"], cwd=repo_root)
    )
    return _parse_issue(data, context=f"bd close {issue_id} --json")
