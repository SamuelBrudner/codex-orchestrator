from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from codex_orchestrator.git_subprocess import (
    GitError,
    git_branch_exists,
    git_checkout,
    git_checkout_new_branch,
    git_fetch,
    git_head_is_detached,
    git_is_dirty,
    git_remote_branch_exists,
    git_remotes,
    git_remove_ignored_untracked,
    resolve_dirty_ignore_globs,
)


def _which(tool: str) -> str | None:
    for raw in os.environ.get("PATH", "").split(os.pathsep):
        if not raw:
            continue
        candidate = Path(raw) / tool
        if candidate.exists() and os.access(candidate, os.X_OK):
            return str(candidate)
    return None


def _require_tools(tools: tuple[str, ...] | list[str], *, error_type: type[Exception] = RuntimeError) -> None:
    missing = [tool for tool in tools if _which(tool) is None]
    if missing:
        raise error_type("Missing required tools on PATH: " + ", ".join(sorted(set(missing))))


def _ordered_remotes(remotes: tuple[str, ...] | list[str]) -> list[str]:
    preferred = ("origin", "upstream")
    ordered: list[str] = []
    seen: set[str] = set()
    for name in preferred:
        if name in remotes and name not in seen:
            ordered.append(name)
            seen.add(name)
    for name in remotes:
        if name not in seen:
            ordered.append(name)
            seen.add(name)
    return ordered


def _resolve_base_ref(*, repo_root: Path, base_branch: str) -> str:
    if git_branch_exists(repo_root=repo_root, branch=base_branch):
        return base_branch

    remotes = _ordered_remotes(git_remotes(repo_root=repo_root))
    for remote in remotes:
        if git_remote_branch_exists(repo_root=repo_root, remote=remote, branch=base_branch):
            return f"refs/remotes/{remote}/{base_branch}"
    return base_branch


def _ensure_run_branch(
    *,
    repo_root: Path,
    run_id: str,
    base_branch: str,
    dirty_ignore_globs: tuple[str, ...] | list[str],
    error_type: type[Exception] = RuntimeError,
) -> tuple[str, str | None]:
    try:
        if git_is_dirty(repo_root=repo_root, ignore_globs=dirty_ignore_globs):
            raise error_type("Repo is dirty; refusing to run unattended work.")
        if git_head_is_detached(repo_root=repo_root):
            raise error_type("Repo is in detached HEAD state; refusing to run unattended work.")
    except GitError as e:
        raise error_type(str(e)) from e

    fetch_error: str | None = None
    try:
        git_fetch(repo_root=repo_root)
    except GitError as e:
        fetch_error = str(e)

    run_branch = f"run/{run_id}"
    try:
        if git_branch_exists(repo_root=repo_root, branch=run_branch):
            git_checkout(repo_root=repo_root, ref=run_branch)
            return run_branch, fetch_error

        base_ref = _resolve_base_ref(repo_root=repo_root, base_branch=base_branch)
        git_checkout_new_branch(repo_root=repo_root, branch=run_branch, base_ref=base_ref)
    except GitError as e:
        raise error_type(f"git branch setup failed: {e}") from e
    return run_branch, fetch_error


@dataclass(frozen=True, slots=True)
class PreparedWorkspace:
    run_branch: str
    dirty_ignore_globs: tuple[str, ...]
    fetch_error: str | None


def prepare_repo_workspace(
    *,
    repo_policy: Any,
    run_id: str,
    emit: Any,
    exec_log_path: Path,
    now_fn: Any,
    append_log_fn: Any,
    error_type: type[Exception] = RuntimeError,
) -> PreparedWorkspace:
    _require_tools(["git", "bd", "codex"], error_type=error_type)

    dirty_resolution = resolve_dirty_ignore_globs(
        repo_root=repo_policy.path,
        configured=repo_policy.dirty_ignore_globs,
    )
    dirty_ignore_globs = tuple(dirty_resolution.resolved)
    if dirty_ignore_globs:
        emit("repo_dirty_ignore", globs=list(dirty_ignore_globs))
    if dirty_resolution.detected:
        emit("repo_dirty_ignore_detected", globs=list(dirty_resolution.detected))

    if repo_policy.dirty_cleanup and dirty_ignore_globs:
        try:
            removed_paths = git_remove_ignored_untracked(
                repo_root=repo_policy.path,
                ignore_globs=dirty_ignore_globs,
            )
        except GitError as e:
            append_log_fn(exec_log_path, f"{now_fn().isoformat()} dirty_cleanup_failed error={e}")
            emit("repo_dirty_cleanup_failed", error=str(e))
        else:
            if removed_paths:
                append_log_fn(
                    exec_log_path,
                    f"{now_fn().isoformat()} dirty_cleanup_removed count={len(removed_paths)}",
                )
                emit("repo_dirty_cleanup", removed=removed_paths)

    run_branch, fetch_error = _ensure_run_branch(
        repo_root=repo_policy.path,
        run_id=run_id,
        base_branch=repo_policy.base_branch,
        dirty_ignore_globs=dirty_ignore_globs,
        error_type=error_type,
    )
    return PreparedWorkspace(
        run_branch=run_branch,
        dirty_ignore_globs=dirty_ignore_globs,
        fetch_error=fetch_error,
    )
